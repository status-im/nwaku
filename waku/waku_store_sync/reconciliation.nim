{.push raises: [].}

import
  std/[sequtils, options, packedsets],
  stew/byteutils,
  results,
  chronicles,
  chronos,
  metrics,
  libp2p/utility,
  libp2p/protocols/protocol,
  libp2p/stream/connection,
  libp2p/crypto/crypto,
  eth/p2p/discoveryv5/enr
import
  ../common/nimchronos,
  ../common/protobuf,
  ../common/paging,
  ../waku_enr,
  ../waku_core/codecs,
  ../waku_core/time,
  ../waku_core/topics/pubsub_topic,
  ../waku_core/message/digest,
  ../waku_core/message/message,
  ../node/peer_manager/peer_manager,
  ../waku_archive,
  ./common,
  ./codec,
  ./storage/storage,
  ./storage/seq_storage,
  ./storage/range_processing,
  ./protocols_metrics

logScope:
  topics = "waku reconciliation"

const DefaultStorageCap = 50_000

type SyncReconciliation* = ref object of LPProtocol
  cluster: uint16
  shards: PackedSet[uint16]

  peerManager: PeerManager

  wakuArchive: WakuArchive

  storage: SyncStorage

  # Receive IDs from transfer protocol for storage
  idsRx: AsyncQueue[(SyncID, uint16)]

  # Send Hashes to transfer protocol for reception
  localWantsTx: AsyncQueue[(PeerId, WakuMessageHash)]

  # Send Hashes to transfer protocol for transmission
  remoteNeedsTx: AsyncQueue[(PeerId, WakuMessageHash)]

  # params
  syncInterval: timer.Duration # Time between each synchronization attempt
  syncRange: timer.Duration # Amount of time in the past to sync
  relayJitter: Duration # Amount of time since the present to ignore when syncing

  # futures
  periodicSyncFut: Future[void]
  periodicPruneFut: Future[void]
  idsReceiverFut: Future[void]

proc messageIngress*(
    self: SyncReconciliation, pubsubTopic: PubsubTopic, msg: WakuMessage
): Result[void, string] =
  let msgHash = computeMessageHash(pubsubTopic, msg)

  let id = SyncID(time: msg.timestamp, hash: msgHash)

  let parseRes = RelayShard.parse(pubsubTopic)

  if parseRes.isErr():
    return err("failed to parse pubsub topic: " & $pubsubTopic)

  let shard = parseRes.get().shardId

  self.storage.insert(id, shard).isOkOr:
    return err(
      "failed to insert new message: msg_hash: " & $msgHash.toHex() & " error: " & $error
    )

proc messageIngress*(
    self: SyncReconciliation, msgHash: WakuMessageHash, msg: WakuMessage, shard: uint16
): Result[void, string] =
  let id = SyncID(time: msg.timestamp, hash: msgHash)

  self.storage.insert(id, shard).isOkOr:
    return err(
      "failed to insert new message: msg_hash: " & $id.hash.toHex() & " error: " & $error
    )

proc messageIngress*(
    self: SyncReconciliation, id: SyncID, shard: uint16
): Result[void, string] =
  self.storage.insert(id, shard).isOkOr:
    return err(
      "failed to insert new message: msg_hash: " & $id.hash.toHex() & " error: " & $error
    )

proc preProcessPayload(
    self: SyncReconciliation, payload: RangesData
): Option[RangesData] =
  ## Check the received payload for cluster, shards and/or time mismatch.

  var payload = payload

  if self.cluster != payload.cluster:
    return none(RangesData)

  let shardsIntersection = self.shards * payload.shards.toPackedSet()

  if shardsIntersection.len < 1:
    return none(RangesData)

  payload.shards = shardsIntersection.toSeq()

  let timeRange = calculateTimeRange(self.relayJitter, self.syncRange)
  let selfLowerBound = timeRange.a

  # for non skip ranges check if they happen before any of our ranges
  # convert to skip range before processing
  for i in 0 ..< payload.ranges.len:
    let rangeType = payload.ranges[i][1]
    if rangeType != RangeType.Skip:
      continue

    let upperBound = payload.ranges[i][0].b.time
    if selfLowerBound > upperBound:
      payload.ranges[i][1] = RangeType.Skip

      if rangeType == RangeType.Fingerprint:
        payload.fingerprints.delete(0)
      elif rangeType == RangeType.ItemSet:
        payload.itemSets.delete(0)
    else:
      break

  return some(payload)

proc processRequest(
    self: SyncReconciliation, conn: Connection
): Future[Result[void, string]] {.async.} =
  var roundTrips = 0

  while true:
    let readRes = catch:
      await conn.readLp(int.high)

    let buffer: seq[byte] = readRes.valueOr:
      return err("connection read error: " & error.msg)

    total_bytes_exchanged.observe(buffer.len, labelValues = [Reconciliation, Receiving])

    let recvPayload = RangesData.deltaDecode(buffer).valueOr:
      return err("payload decoding error: " & error)

    roundTrips.inc()

    trace "sync payload received",
      local = self.peerManager.switch.peerInfo.peerId,
      remote = conn.peerId,
      payload = recvPayload

    if recvPayload.ranges.len == 0 or recvPayload.ranges.allIt(it[1] == RangeType.Skip):
      break

    var
      hashToRecv: seq[WakuMessageHash]
      hashToSend: seq[WakuMessageHash]
      sendPayload: RangesData
      rawPayload: seq[byte]

    let preProcessedPayloadRes = self.preProcessPayload(recvPayload)
    if preProcessedPayloadRes.isSome():
      let preProcessedPayload = preProcessedPayloadRes.get()

      sendPayload =
        self.storage.processPayload(preProcessedPayload, hashToSend, hashToRecv)

      sendPayload.cluster = self.cluster
      sendPayload.shards = self.shards.toSeq()

      for hash in hashToSend:
        await self.remoteNeedsTx.addLast((conn.peerId, hash))

      for hash in hashToRecv:
        await self.localWantstx.addLast((conn.peerId, hash))

      rawPayload = sendPayload.deltaEncode()

    total_bytes_exchanged.observe(
      rawPayload.len, labelValues = [Reconciliation, Sending]
    )

    let writeRes = catch:
      await conn.writeLP(rawPayload)

    if writeRes.isErr():
      return err("connection write error: " & writeRes.error.msg)

    trace "sync payload sent",
      local = self.peerManager.switch.peerInfo.peerId,
      remote = conn.peerId,
      payload = sendPayload

    if sendPayload.ranges.len == 0 or sendPayload.ranges.allIt(it[1] == RangeType.Skip):
      break

    continue

  reconciliation_roundtrips.observe(roundTrips)

  await conn.close()

  return ok()

proc initiate(
    self: SyncReconciliation, connection: Connection
): Future[Result[void, string]] {.async.} =
  let
    timeRange = calculateTimeRange(self.relayJitter, self.syncRange)
    lower = SyncID(time: timeRange.a, hash: EmptyFingerprint)
    upper = SyncID(time: timeRange.b, hash: FullFingerprint)
    bounds = lower .. upper

    fingerprint = self.storage.computeFingerprint(bounds, self.shards)
    initPayload = RangesData(
      cluster: self.cluster,
      shards: self.shards.toSeq(),
      ranges: @[(bounds, RangeType.Fingerprint)],
      fingerprints: @[fingerprint],
      itemSets: @[],
    )

  let sendPayload = initPayload.deltaEncode()

  total_bytes_exchanged.observe(
    sendPayload.len, labelValues = [Reconciliation, Sending]
  )

  let writeRes = catch:
    await connection.writeLP(sendPayload)

  if writeRes.isErr():
    return err("connection write error: " & writeRes.error.msg)

  trace "sync payload sent",
    local = self.peerManager.switch.peerInfo.peerId,
    remote = connection.peerId,
    payload = sendPayload

  ?await self.processRequest(connection)

  return ok()

proc storeSynchronization*(
    self: SyncReconciliation, peerInfo: Option[RemotePeerInfo] = none(RemotePeerInfo)
): Future[Result[void, string]] {.async.} =
  let peer = peerInfo.valueOr:
    self.peerManager.selectPeer(WakuReconciliationCodec).valueOr:
      return err("no suitable peer found for sync")

  let connOpt = await self.peerManager.dialPeer(peer, WakuReconciliationCodec)

  let conn: Connection = connOpt.valueOr:
    return err("cannot establish sync connection")

  debug "sync session initialized",
    local = self.peerManager.switch.peerInfo.peerId, remote = conn.peerId

  (await self.initiate(conn)).isOkOr:
    error "sync session failed",
      local = self.peerManager.switch.peerInfo.peerId, remote = conn.peerId, err = error

    return err("sync request error: " & error)

  debug "sync session ended gracefully",
    local = self.peerManager.switch.peerInfo.peerId, remote = conn.peerId

  return ok()

proc initFillStorage(
    syncRange: timer.Duration, wakuArchive: WakuArchive
): Future[Result[(seq[SyncID], seq[uint16]), string]] {.async.} =
  if wakuArchive.isNil():
    return err("waku archive unavailable")

  let endTime = getNowInNanosecondTime()
  let starTime = endTime - syncRange.nanos

  #TODO special query for only timestap and hash ???

  var query = ArchiveQuery(
    includeData: true,
    cursor: none(ArchiveCursor),
    startTime: some(starTime),
    endTime: some(endTime),
    pageSize: 100,
    direction: PagingDirection.FORWARD,
  )

  debug "initial storage filling started"

  var ids = newSeq[SyncID](DefaultStorageCap)
  var shards = newSeq[uint16](DefaultStorageCap)

  # we assume IDs are in order

  while true:
    let response = (await wakuArchive.findMessages(query)).valueOr:
      return err("archive retrival failed: " & $error)

    for i in 0 ..< response.hashes.len:
      let hash = response.hashes[i]
      let msg = response.messages[i]
      let topic = response.topics[i]

      let parseRes = RelayShard.parse(topic)

      if parseRes.isErr():
        error "failed to parse pubsub topic", pubsubTopic = topic
        continue

      let shard = parseRes.get().shardId

      ids.add(SyncID(time: msg.timestamp, hash: hash))
      shards.add(shard)

    if response.cursor.isNone():
      break

    query.cursor = response.cursor

  debug "initial storage filling done", elements = ids.len

  return ok((ids, shards))

proc new*(
    T: type SyncReconciliation,
    cluster: uint16,
    shards: seq[uint16],
    peerManager: PeerManager,
    wakuArchive: WakuArchive,
    syncRange: timer.Duration = DefaultSyncRange,
    syncInterval: timer.Duration = DefaultSyncInterval,
    relayJitter: timer.Duration = DefaultGossipSubJitter,
    idsRx: AsyncQueue[(SyncID, uint16)],
    localWantsTx: AsyncQueue[(PeerId, WakuMessageHash)],
    remoteNeedsTx: AsyncQueue[(PeerId, WakuMessageHash)],
): Future[Result[T, string]] {.async.} =
  let res = await initFillStorage(syncRange, wakuArchive)
  let storage =
    if res.isErr():
      warn "will not sync messages before this point in time", error = res.error
      SeqStorage.new(DefaultStorageCap)
    else:
      let (ele, shar) = res.get()
      SeqStorage.new(ele, shar)

  var sync = SyncReconciliation(
    cluster: cluster,
    shards: shards.toPackedSet(),
    peerManager: peerManager,
    storage: storage,
    syncRange: syncRange,
    syncInterval: syncInterval,
    relayJitter: relayJitter,
    idsRx: idsRx,
    localWantsTx: localWantsTx,
    remoteNeedsTx: remoteNeedsTx,
  )

  let handler = proc(conn: Connection, proto: string) {.async, closure.} =
    (await sync.processRequest(conn)).isOkOr:
      error "request processing error", error = error

    return

  sync.handler = handler
  sync.codec = WakuReconciliationCodec

  info "Store Reconciliation protocol initialized"

  return ok(sync)

proc periodicSync(self: SyncReconciliation) {.async.} =
  debug "periodic sync initialized", interval = $self.syncInterval

  while true: # infinite loop
    await sleepAsync(self.syncInterval)

    debug "periodic sync started"

    (await self.storeSynchronization()).isOkOr:
      error "periodic sync failed", err = error
      continue

    debug "periodic sync done"

proc periodicPrune(self: SyncReconciliation) {.async.} =
  debug "periodic prune initialized", interval = $self.syncInterval

  # preventing sync and prune loops of happening at the same time.
  await sleepAsync((self.syncInterval div 2))

  while true: # infinite loop
    await sleepAsync(self.syncInterval)

    debug "periodic prune started"

    let time = getNowInNanosecondTime() - self.syncRange.nanos

    let count = self.storage.prune(time)

    debug "periodic prune done", elements_pruned = count

proc idsReceiverLoop(self: SyncReconciliation) {.async.} =
  while true: # infinite loop
    let (id, shard) = await self.idsRx.popfirst()

    self.messageIngress(id, shard).isOkOr:
      error "message ingress failed", error = error

proc start*(self: SyncReconciliation) =
  if self.started:
    return

  self.started = true

  if self.syncInterval > ZeroDuration:
    self.periodicSyncFut = self.periodicSync()

  if self.syncInterval > ZeroDuration:
    self.periodicPruneFut = self.periodicPrune()

  self.idsReceiverFut = self.idsReceiverLoop()

  info "Store Sync Reconciliation protocol started"

proc stop*(self: SyncReconciliation) =
  if self.syncInterval > ZeroDuration:
    self.periodicSyncFut.cancelSoon()

  if self.syncInterval > ZeroDuration:
    self.periodicPruneFut.cancelSoon()

  self.idsReceiverFut.cancelSoon()

  info "Store Sync Reconciliation protocol stopped"
