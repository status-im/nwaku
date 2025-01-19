import
  chronicles, chronos, confutils,
  eth/[keys, p2p/discoveryv5/enr],
  libp2p/crypto/crypto,
  std/[tables, times, sequtils],
  stew/byteutils,
  stew/shims/net,
  results,
  waku/[
    common/logging,
    node/peer_manager,
    waku_core,
    waku_node,
    waku_enr,
    discovery/waku_discv5,
    factory/builder,
  ]

proc now*(): Timestamp =
  getNanosecondTime(getTime().toUnixFloat())

# careful if running pub and sub in the same machine
const
  wakuPort = 60000
  clusterId = 16
  shardId = @[32'u16]
  LightpushPeer =
    "/ip4/143.198.250.233/tcp/30303/p2p/16Uiu2HAmQE7FXQc6iZHdBzYfw3qCSDa9dLc1wsBJKoP4aZvztq2d"

proc setupAndPublish(rng: ref HmacDrbgContext) {.async.} =
  # use notice to filter all waku messaging
  setupLog(logging.LogLevel.NOTICE, logging.LogFormat.TEXT)

  notice "starting publisher", wakuPort = wakuPort
  let
    nodeKey = crypto.PrivateKey.random(Secp256k1, rng[]).get()
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
  node.mountLightPushClient()

  await node.start()
  node.peerManager.start()

  # Make sure it matches the publisher. Use default value
  # see spec: https://rfc.vac.dev/spec/23/
  let pubSubTopic = PubsubTopic("/waku/2/rs/16/32")

  # any content topic can be chosen
  let contentTopic = ContentTopic("/examples/1/pubsub-example/proto")

  notice "publisher service started"
  while true:
    let text = "hi there i'm a publisher"
    let message = WakuMessage(
      payload: toBytes(text), # content of the message
      contentTopic: contentTopic, # content topic to publish to
      ephemeral: true, # tell store nodes to not store it
      timestamp: now(),
    ) # current timestamp

    let lightpushPeer = parsePeerInfo(LightpushPeer).get()

    let res = await node.lightpushPublish(some(pubSubTopic), message, lightpushPeer)

    if res.isOk:
      notice "published message",
        text = text,
        timestamp = message.timestamp,
        psTopic = pubSubTopic,
        contentTopic = contentTopic
    else:
      error "failed to publish message", error = res.error

    await sleepAsync(5000)

when isMainModule:
  let rng = crypto.newRng()
  asyncSpawn setupAndPublish(rng)
  runForever()
