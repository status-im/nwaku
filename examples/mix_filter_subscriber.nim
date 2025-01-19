import
  chronicles, chronos, confutils,
  eth/[keys, p2p/discoveryv5/enr],
  libp2p/crypto/crypto,
  std/[tables, sequtils],
  stew/[byteutils, shims/net],
  waku/[
    common/logging,
    node/peer_manager,
    waku_core,
    waku_node,
    waku_enr,
    discovery/waku_discv5,
    factory/builder,
    waku_relay,
    waku_filter_v2/client,
  ]

# careful if running pub and sub in the same machine
const wakuPort = 50000

const clusterId = 16
const shardId = @[32'u16]

const
  FilterPeer =
    "/ip4/104.197.5.96/tcp/30303/p2p/16Uiu2HAmGAA54bBTE78MYidSy3P7Q9yAWFNTAEReJYD69VRvtL5r"
  FilterPubsubTopic = PubsubTopic("/waku/2/rs/16/32")
  FilterContentTopic = ContentTopic("/examples/1/pubsub-example/proto")

proc messagePushHandler(
    pubsubTopic: PubsubTopic, message: WakuMessage
) {.async, gcsafe.} =
  let payloadStr = string.fromBytes(message.payload)
  notice "message received",
    payload = payloadStr,
    pubsubTopic = pubsubTopic,
    contentTopic = message.contentTopic,
    timestamp = message.timestamp

proc setupAndSubscribe(rng: ref HmacDrbgContext) {.async.} =
  # use notice to filter all waku messaging
  setupLog(logging.LogLevel.NOTICE, logging.LogFormat.TEXT)

  notice "starting subscriber", wakuPort = wakuPort
  let
    nodeKey = crypto.PrivateKey.random(Secp256k1, rng[])[]
    ip = parseIpAddress("0.0.0.0")
    flags = CapabilitiesBitfield.init(relay = true)

  let relayShards = RelayShards.init(clusterId, shardId).valueOr:
    error "Relay shards initialization failed", error = error
    quit(QuitFailure)

  var enrBuilder = EnrBuilder.init(nodeKey)
  enrBuilder.withWakuRelaySharding(relayShards).expect(
    "Building ENR with relay sharding failed"
  )

  let recordRes = enrBuilder.build()
  let record =
    if recordRes.isErr():
      error "failed to create enr record", error = recordRes.error
      quit(QuitFailure)
    else:
      recordRes.get()

  var builder = WakuNodeBuilder.init()
  builder.withNodeKey(nodeKey)
  builder.withRecord(record)
  builder.withNetworkConfigurationDetails(ip, Port(wakuPort)).tryGet()
  let node = builder.build().tryGet()


  node.mountMetadata(clusterId).expect("failed to mount waku metadata protocol")
  waitFor node.mountFilterClient()


  await node.start()

  node.peerManager.start()

  node.wakuFilterClient.registerPushHandler(messagePushHandler)

  let filterPeer = parsePeerInfo(FilterPeer).get()

  while true:
    notice "maintaining subscription"
    # First use filter-ping to check if we have an active subscription
    let pingRes = await node.wakuFilterClient.ping(filterPeer)
    if pingRes.isErr():
      # No subscription found. Let's subscribe.
      notice "no subscription found. Sending subscribe request"

      let subscribeRes = await node.wakuFilterClient.subscribe(
        filterPeer, FilterPubsubTopic, @[FilterContentTopic]
      )

      if subscribeRes.isErr():
        notice "subscribe request failed. Quitting.", err = subscribeRes.error
        break
      else:
        notice "subscribe request successful."
    else:
      notice "subscription found."

    await sleepAsync(60.seconds) # Subscription maintenance interval

when isMainModule:
  let rng = crypto.newRng()
  asyncSpawn setupAndSubscribe(rng)
  runForever()