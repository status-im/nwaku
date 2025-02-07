{.used.}

import
  std/[options, sets, random, math],
  testutils/unittests,
  chronos,
  libp2p/crypto/crypto,
  stew/byteutils

import
  ../../waku/[
    node/peer_manager,
    waku_core,
    waku_core/message,
    waku_core/message/digest,
    waku_store_sync/common,
    waku_store_sync/storage/range_processing,
    waku_store_sync/reconciliation,
    waku_store_sync/transfer,
    waku_archive/archive,
    waku_archive/driver,
    waku_archive/common,
  ],
  ../testlib/[wakucore, testasync],
  ../waku_archive/archive_utils,
  ./sync_utils

suite "Waku Sync: reconciliation":
  var serverSwitch {.threadvar.}: Switch
  var clientSwitch {.threadvar.}: Switch

  var
    idsChannel {.threadvar.}: AsyncQueue[(SyncID, uint16)]
    localWants {.threadvar.}: AsyncQueue[(PeerId, WakuMessageHash)]
    remoteNeeds {.threadvar.}: AsyncQueue[(PeerId, WakuMessageHash)]

  var server {.threadvar.}: SyncReconciliation
  var client {.threadvar.}: SyncReconciliation

  var serverPeerInfo {.threadvar.}: RemotePeerInfo
  var clientPeerInfo {.threadvar.}: RemotePeerInfo

  asyncSetup:
    serverSwitch = newTestSwitch()
    clientSwitch = newTestSwitch()

    await allFutures(serverSwitch.start(), clientSwitch.start())

    idsChannel = newAsyncQueue[(SyncID, uint16)]()
    localWants = newAsyncQueue[(PeerId, WakuMessageHash)]()
    remoteNeeds = newAsyncQueue[(PeerId, WakuMessageHash)]()

    serverPeerInfo = serverSwitch.peerInfo.toRemotePeerInfo()
    clientPeerInfo = clientSwitch.peerInfo.toRemotePeerInfo()

  asyncTeardown:
    server.stop()
    client.stop()

    await allFutures(serverSwitch.stop(), clientSwitch.stop())

  asyncTest "sync 2 nodes both empty":
    server = await newTestWakuRecon(serverSwitch, idsChannel, localWants, remoteNeeds)
    client = await newTestWakuRecon(clientSwitch, idsChannel, localWants, remoteNeeds)

    check:
      idsChannel.len == 0
      localWants.len == 0
      remoteNeeds.len == 0

    let res = await client.storeSynchronization(some(serverPeerInfo))
    assert res.isOk(), res.error

    check:
      idsChannel.len == 0
      localWants.len == 0
      remoteNeeds.len == 0

  asyncTest "sync 2 nodes empty client full server":
    server = await newTestWakuRecon(serverSwitch, idsChannel, localWants, remoteNeeds)
    client = await newTestWakuRecon(clientSwitch, idsChannel, localWants, remoteNeeds)

    let
      msg1 = fakeWakuMessage(ts = now(), contentTopic = DefaultContentTopic)
      msg2 = fakeWakuMessage(ts = now() + 1, contentTopic = DefaultContentTopic)
      msg3 = fakeWakuMessage(ts = now() + 2, contentTopic = DefaultContentTopic)
      hash1 = computeMessageHash(pubsubTopic = DefaultPubsubTopic, msg1)
      hash2 = computeMessageHash(pubsubTopic = DefaultPubsubTopic, msg2)
      hash3 = computeMessageHash(pubsubTopic = DefaultPubsubTopic, msg3)

    discard server.messageIngress(hash1, msg1, 1)
    discard server.messageIngress(hash2, msg2, 1)
    discard server.messageIngress(hash3, msg3, 1)

    check:
      remoteNeeds.len == 0
      localWants.len == 0
      remoteNeeds.contains((clientPeerInfo.peerId, hash1)) == false
      remoteNeeds.contains((clientPeerInfo.peerId, hash2)) == false
      remoteNeeds.contains((clientPeerInfo.peerId, hash3)) == false

    let res = await client.storeSynchronization(some(serverPeerInfo))
    assert res.isOk(), res.error

    check:
      remoteNeeds.len == 3
      localWants.len == 0
      remoteNeeds.contains((clientPeerInfo.peerId, hash1)) == true
      remoteNeeds.contains((clientPeerInfo.peerId, hash2)) == true
      remoteNeeds.contains((clientPeerInfo.peerId, hash3)) == true

  asyncTest "sync 2 nodes full client empty server":
    server = await newTestWakuRecon(serverSwitch, idsChannel, localWants, remoteNeeds)
    client = await newTestWakuRecon(clientSwitch, idsChannel, localWants, remoteNeeds)

    let
      msg1 = fakeWakuMessage(ts = now(), contentTopic = DefaultContentTopic)
      msg2 = fakeWakuMessage(ts = now() + 1, contentTopic = DefaultContentTopic)
      msg3 = fakeWakuMessage(ts = now() + 2, contentTopic = DefaultContentTopic)
      hash1 = computeMessageHash(pubsubTopic = DefaultPubsubTopic, msg1)
      hash2 = computeMessageHash(pubsubTopic = DefaultPubsubTopic, msg2)
      hash3 = computeMessageHash(pubsubTopic = DefaultPubsubTopic, msg3)

    discard client.messageIngress(hash1, msg1, 1)
    discard client.messageIngress(hash2, msg2, 1)
    discard client.messageIngress(hash3, msg3, 1)

    check:
      remoteNeeds.len == 0
      localWants.len == 0
      remoteNeeds.contains((serverPeerInfo.peerId, hash1)) == false
      remoteNeeds.contains((serverPeerInfo.peerId, hash2)) == false
      remoteNeeds.contains((serverPeerInfo.peerId, hash3)) == false

    let res = await client.storeSynchronization(some(serverPeerInfo))
    assert res.isOk(), res.error

    check:
      remoteNeeds.len == 3
      localWants.len == 0
      remoteNeeds.contains((serverPeerInfo.peerId, hash1)) == true
      remoteNeeds.contains((serverPeerInfo.peerId, hash2)) == true
      remoteNeeds.contains((serverPeerInfo.peerId, hash3)) == true

  asyncTest "sync 2 nodes different hashes":
    server = await newTestWakuRecon(serverSwitch, idsChannel, localWants, remoteNeeds)
    client = await newTestWakuRecon(clientSwitch, idsChannel, localWants, remoteNeeds)

    let
      msg1 = fakeWakuMessage(ts = now(), contentTopic = DefaultContentTopic)
      msg2 = fakeWakuMessage(ts = now() + 1, contentTopic = DefaultContentTopic)
      msg3 = fakeWakuMessage(ts = now() + 2, contentTopic = DefaultContentTopic)
      hash1 = computeMessageHash(DefaultPubsubTopic, msg1)
      hash2 = computeMessageHash(DefaultPubsubTopic, msg2)
      hash3 = computeMessageHash(DefaultPubsubTopic, msg3)

    discard server.messageIngress(hash1, msg1, 1)
    discard server.messageIngress(hash2, msg2, 1)
    discard client.messageIngress(hash1, msg1, 1)
    discard client.messageIngress(hash3, msg3, 1)

    check:
      remoteNeeds.len == 0
      localWants.len == 0
      remoteNeeds.contains((serverPeerInfo.peerId, hash3)) == false
      remoteNeeds.contains((clientPeerInfo.peerId, hash2)) == false
      localWants.contains((clientPeerInfo.peerId, hash3)) == false
      localWants.contains((serverPeerInfo.peerId, hash2)) == false

    var syncRes = await client.storeSynchronization(some(serverPeerInfo))
    assert syncRes.isOk(), $syncRes.error

    check:
      remoteNeeds.len == 2
      localWants.len == 2
      remoteNeeds.contains((serverPeerInfo.peerId, hash3)) == true
      remoteNeeds.contains((clientPeerInfo.peerId, hash2)) == true
      localWants.contains((clientPeerInfo.peerId, hash3)) == true
      localWants.contains((serverPeerInfo.peerId, hash2)) == true

  asyncTest "sync 2 nodes different shards":
    server = await newTestWakuRecon(
      serverSwitch, idsChannel, localWants, remoteNeeds, 2, @[0.uint16, 1, 2, 3, 4]
    )

    client = await newTestWakuRecon(
      clientSwitch, idsChannel, localWants, remoteNeeds, 2, @[3.uint16, 4, 5, 6, 7]
    )

    let
      msg1 = fakeWakuMessage(ts = now(), contentTopic = DefaultContentTopic)
      msg2 = fakeWakuMessage(ts = now() + 1, contentTopic = DefaultContentTopic)
      msg3 = fakeWakuMessage(ts = now() + 2, contentTopic = DefaultContentTopic)
      msg4 = fakeWakuMessage(ts = now() + 3, contentTopic = DefaultContentTopic)
      msg5 = fakeWakuMessage(ts = now() + 4, contentTopic = DefaultContentTopic)
      msg6 = fakeWakuMessage(ts = now() + 5, contentTopic = DefaultContentTopic)
      hash1 = computeMessageHash(DefaultPubsubTopic, msg1)
      hash2 = computeMessageHash(DefaultPubsubTopic, msg2)
      hash3 = computeMessageHash(DefaultPubsubTopic, msg3)
      hash4 = computeMessageHash(DefaultPubsubTopic, msg4)
      hash5 = computeMessageHash(DefaultPubsubTopic, msg5)
      hash6 = computeMessageHash(DefaultPubsubTopic, msg6)

    discard server.messageIngress(hash1, msg1, 1)
    discard server.messageIngress(hash2, msg2, 2)
    discard server.messageIngress(hash3, msg3, 3)

    discard client.messageIngress(hash4, msg4, 4)
    discard client.messageIngress(hash5, msg5, 5)
    discard client.messageIngress(hash6, msg6, 6)

    check:
      remoteNeeds.len == 0
      localWants.len == 0

    var syncRes = await client.storeSynchronization(some(serverPeerInfo))
    assert syncRes.isOk(), $syncRes.error

    check:
      remoteNeeds.len == 2
      localWants.len == 2
      remoteNeeds.contains((serverPeerInfo.peerId, hash4)) == true
      remoteNeeds.contains((clientPeerInfo.peerId, hash3)) == true
      localWants.contains((clientPeerInfo.peerId, hash4)) == true
      localWants.contains((serverPeerInfo.peerId, hash3)) == true

  asyncTest "sync 2 nodes same hashes":
    server = await newTestWakuRecon(serverSwitch, idsChannel, localWants, remoteNeeds)
    client = await newTestWakuRecon(clientSwitch, idsChannel, localWants, remoteNeeds)

    let
      msg1 = fakeWakuMessage(ts = now(), contentTopic = DefaultContentTopic)
      msg2 = fakeWakuMessage(ts = now() + 1, contentTopic = DefaultContentTopic)
      hash1 = computeMessageHash(pubsubTopic = DefaultPubsubTopic, msg1)
      hash2 = computeMessageHash(pubsubTopic = DefaultPubsubTopic, msg2)

    discard server.messageIngress(hash1, msg1, 1)
    discard client.messageIngress(hash1, msg1, 1)
    discard server.messageIngress(hash2, msg2, 1)
    discard client.messageIngress(hash2, msg2, 1)

    check:
      localWants.len == 0
      remoteNeeds.len == 0

    let res = await client.storeSynchronization(some(serverPeerInfo))
    assert res.isOk(), $res.error

    check:
      localWants.len == 0
      remoteNeeds.len == 0

  asyncTest "sync 2 nodes 100K msgs 1 diff":
    server = await newTestWakuRecon(serverSwitch, idsChannel, localWants, remoteNeeds)
    client = await newTestWakuRecon(clientSwitch, idsChannel, localWants, remoteNeeds)

    let msgCount = 100_000
    var diffIndex = rand(msgCount)
    var diff: WakuMessageHash

    # the sync window is 1 hour, spread msg equally in that time
    let timeSlice = calculateTimeRange()
    let timeWindow = int64(timeSlice.b) - int64(timeSlice.a)
    let (part, _) = divmod(timeWindow, 100_000)

    var timestamp = timeSlice.a

    for i in 0 ..< msgCount:
      let msg = fakeWakuMessage(ts = timestamp, contentTopic = DefaultContentTopic)
      let hash = computeMessageHash(DefaultPubsubTopic, msg)

      discard server.messageIngress(hash, msg, 1)

      if i != diffIndex:
        discard client.messageIngress(hash, msg, 1)
      else:
        diff = hash

      timestamp += Timestamp(part)

    check:
      remoteNeeds.len == 0
      localWants.len == 0
      localWants.contains((serverPeerInfo.peerId, WakuMessageHash(diff))) == false
      remoteNeeds.contains((clientPeerInfo.peerId, WakuMessageHash(diff))) == false

    let res = await client.storeSynchronization(some(serverPeerInfo))
    assert res.isOk(), $res.error

    check:
      remoteNeeds.len == 1
      localWants.len == 1
      localWants.contains((serverPeerInfo.peerId, WakuMessageHash(diff))) == true
      remoteNeeds.contains((clientPeerInfo.peerId, WakuMessageHash(diff))) == true

  asyncTest "sync 2 nodes 10K msgs 1K diffs":
    server = await newTestWakuRecon(serverSwitch, idsChannel, localWants, remoteNeeds)
    client = await newTestWakuRecon(clientSwitch, idsChannel, localWants, remoteNeeds)

    let msgCount = 10_000
    var diffCount = 1_000

    var diffMsgHashes: HashSet[WakuMessageHash]
    var randIndexes: HashSet[int]

    # Diffs
    for i in 0 ..< diffCount:
      var randInt = rand(0 ..< msgCount)

      #make sure we actually have the right number of diffs
      while randInt in randIndexes:
        randInt = rand(0 ..< msgCount)

      randIndexes.incl(randInt)

    # sync window is 1 hour, spread msg equally in that time
    let timeSlice = calculateTimeRange()
    let timeWindow = int64(timeSlice.b) - int64(timeSlice.a)
    let (part, _) = divmod(timeWindow, 100_000)

    var timestamp = timeSlice.a

    for i in 0 ..< msgCount:
      let
        msg = fakeWakuMessage(ts = timestamp, contentTopic = DefaultContentTopic)
        hash = computeMessageHash(DefaultPubsubTopic, msg)

      discard server.messageIngress(hash, msg, 1)

      if i in randIndexes:
        diffMsgHashes.incl(hash)
      else:
        discard client.messageIngress(hash, msg, 1)

      timestamp += Timestamp(part)
      continue

    check:
      localWants.len == 0
      remoteNeeds.len == 0

    let res = await client.storeSynchronization(some(serverPeerInfo))
    assert res.isOk(), $res.error

    # timimg issue make it hard to match exact numbers
    check:
      localWants.len > 900
      remoteNeeds.len > 900

suite "Waku Sync: transfer":
  var
    serverSwitch {.threadvar.}: Switch
    clientSwitch {.threadvar.}: Switch

  var
    serverDriver {.threadvar.}: ArchiveDriver
    clientDriver {.threadvar.}: ArchiveDriver
    serverArchive {.threadvar.}: WakuArchive
    clientArchive {.threadvar.}: WakuArchive

  var
    serverIds {.threadvar.}: AsyncQueue[(SyncID, uint16)]
    serverLocalWants {.threadvar.}: AsyncQueue[(PeerId, WakuMessageHash)]
    serverRemoteNeeds {.threadvar.}: AsyncQueue[(PeerId, WakuMessageHash)]
    clientIds {.threadvar.}: AsyncQueue[(SyncID, uint16)]
    clientLocalWants {.threadvar.}: AsyncQueue[(PeerId, WakuMessageHash)]
    clientRemoteNeeds {.threadvar.}: AsyncQueue[(PeerId, WakuMessageHash)]

  var
    server {.threadvar.}: SyncTransfer
    client {.threadvar.}: SyncTransfer

  var
    serverPeerInfo {.threadvar.}: RemotePeerInfo
    clientPeerInfo {.threadvar.}: RemotePeerInfo

  asyncSetup:
    serverSwitch = newTestSwitch()
    clientSwitch = newTestSwitch()

    await allFutures(serverSwitch.start(), clientSwitch.start())

    serverDriver = newSqliteArchiveDriver()
    clientDriver = newSqliteArchiveDriver()

    serverArchive = newWakuArchive(serverDriver)
    clientArchive = newWakuArchive(clientDriver)

    let
      serverPeerManager = PeerManager.new(serverSwitch)
      clientPeerManager = PeerManager.new(clientSwitch)

    serverIds = newAsyncQueue[(SyncID, uint16)]()
    serverLocalWants = newAsyncQueue[(PeerId, WakuMessageHash)]()
    serverRemoteNeeds = newAsyncQueue[(PeerId, WakuMessageHash)]()

    server = SyncTransfer.new(
      peerManager = serverPeerManager,
      wakuArchive = serverArchive,
      idsTx = serverIds,
      localWantsRx = serverLocalWants,
      remoteNeedsRx = serverRemoteNeeds,
    )

    clientIds = newAsyncQueue[(SyncID, uint16)]()
    clientLocalWants = newAsyncQueue[(PeerId, WakuMessageHash)]()
    clientRemoteNeeds = newAsyncQueue[(PeerId, WakuMessageHash)]()

    client = SyncTransfer.new(
      peerManager = clientPeerManager,
      wakuArchive = clientArchive,
      idsTx = clientIds,
      localWantsRx = clientLocalWants,
      remoteNeedsRx = clientRemoteNeeds,
    )

    server.start()
    client.start()

    serverSwitch.mount(server)
    clientSwitch.mount(client)

    serverPeerInfo = serverSwitch.peerInfo.toRemotePeerInfo()
    clientPeerInfo = clientSwitch.peerInfo.toRemotePeerInfo()

    serverPeerManager.addPeer(clientPeerInfo)
    clientPeermanager.addPeer(serverPeerInfo)

  asyncTeardown:
    server.stop()
    client.stop()

    await allFutures(serverSwitch.stop(), clientSwitch.stop())

  asyncTest "transfer 1 message":
    let msg = fakeWakuMessage()
    let hash = computeMessageHash(DefaultPubsubTopic, msg)
    let msgs = @[msg]

    serverDriver = serverDriver.put(DefaultPubsubTopic, msgs)

    # add server info and msg hash to client want channel
    let want = (serverPeerInfo.peerId, hash)
    await clientLocalWants.put(want)

    # add client info and msg hash to server need channel
    let need = (clientPeerInfo.peerId, hash)
    await serverRemoteNeeds.put(need)

    # give time for transfer to happen
    await sleepAsync(500.milliseconds)

    var query = ArchiveQuery()
    query.includeData = true
    query.hashes = @[hash]

    let res = await clientArchive.findMessages(query)
    assert res.isOk(), $res.error

    let response = res.get()

    check:
      response.messages.len > 0
