import
  std/[tables, times, sequtils],
  stew/byteutils,
  stew/shims/net,
  chronicles,
  results,
  chronos,
  confutils,
  libp2p/crypto/crypto,
  eth/keys,
  eth/p2p/discoveryv5/enr

import ../vendor/mix/src/entry_connection, 
  ../vendor/mix/src/protocol

import
  waku/[
    common/logging,
    node/peer_manager,
    waku_core,
    waku_core/codecs,
    waku_node,
    waku_enr,
    discovery/waku_discv5,
    factory/builder,
    waku_lightpush/client
  ]

proc now*(): Timestamp =
  getNanosecondTime(getTime().toUnixFloat())

# careful if running pub and sub in the same machine
const wakuPort = 60000

const clusterId = 2
const shardId = @[0'u16]

const
  LightpushPeer =
    "/ip4/127.0.0.1/tcp/60005/p2p/16Uiu2HAmRhxmCHBYdXt1RibXrjAUNJbduAhzaTHwFCZT4qWnqZAu"
  LightpushPubsubTopic = PubsubTopic("/waku/2/rs/2/0")
  LightpushContentTopic = ContentTopic("/examples/1/light-pubsub-mix-example/proto")

proc setupAndPublish(rng: ref HmacDrbgContext) {.async.} =
  # use notice to filter all waku messaging
  setupLog(logging.LogLevel.DEBUG, logging.LogFormat.TEXT)

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
  (
    await node.mountMix("401dd1eb5582f6dc9488d424aa26ed1092becefcf8543172e6d92c17ed07265a")
  ).isOkOr:
    error "failed to mount waku mix protocol: ", error = $error
    return 

  let conn = MixEntryConnection.newConn(
    "/ip4/127.0.0.1/tcp/60001/p2p/16Uiu2HAmPiEs2ozjjJF2iN2Pe2FYeMC9w4caRHKYdLdAfjgbWM6o",
    ProtocolType.fromString(WakuLightPushCodec),
    node.mix)

  await node.start()
  node.peerManager.start()

  notice "publisher service started"
  while true:
    let text = "hi there i'm a publisher"
    let message = WakuMessage(
      payload: toBytes(text), # content of the message
      contentTopic: LightpushContentTopic, # content topic to publish to
      ephemeral: true, # tell store nodes to not store it
      timestamp: now(),
    ) # current timestamp

    let res =
      await node.wakuLightpushClient.publishWithConn(LightpushPubsubTopic, message, conn)

    if res.isOk:
      notice "published message",
        text = text,
        timestamp = message.timestamp,
        psTopic = LightpushPubsubTopic,
        contentTopic = LightpushContentTopic
    else:
      error "failed to publish message", error = res.error

    await sleepAsync(5000)
    break

when isMainModule:
  let rng = crypto.newRng()
  asyncSpawn setupAndPublish(rng)
  runForever()
