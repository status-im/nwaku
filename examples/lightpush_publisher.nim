## Example showing how a resource restricted client may
## use lightpush to publish messages without relay

import chronicles, chronos, stew/byteutils, results
import waku/[common/logging, node/peer_manager, waku_core, waku_lightpush_legacy/client]

const
  LightpushPeer =
    "/ip4/178.128.141.171/tcp/30303/p2p/16Uiu2HAkykgaECHswi3YKJ5dMLbq2kPVCo89fcyTd38UcQD6ej5W"
    # node-01.do-ams3.waku.test.status.im on waku.test
  LightpushPubsubTopic = PubsubTopic("/waku/2/rs/0/0")
  LightpushContentTopic = ContentTopic("/examples/1/light-pubsub-example/proto")

proc publishMessages(
    wlc: WakuLegacyLightpushClient,
    lightpushPeer: RemotePeerInfo,
    lightpushPubsubTopic: PubsubTopic,
    lightpushContentTopic: ContentTopic,
) {.async.} =
  while true:
    let text = "hi there i'm a lightpush publisher"
    let message = WakuMessage(
      payload: toBytes(text), # content of the message
      contentTopic: lightpushContentTopic, # content topic to publish to
      ephemeral: true, # tell store nodes to not store it
      timestamp: getNowInNanosecondTime(),
    ) # current timestamp

    let wlpRes = await wlc.publish(lightpushPubsubTopic, message, lightpushPeer)

    if wlpRes.isOk():
      notice "published message using lightpush", message = message
    else:
      notice "failed to publish message using lightpush", err = wlpRes.error()

    await sleepAsync(5000) # Publish every 5 seconds

proc setupAndPublish(rng: ref HmacDrbgContext) =
  let lightpushPeer = parsePeerInfo(LightpushPeer).get()

  setupLog(logging.LogLevel.NOTICE, logging.LogFormat.TEXT)
  notice "starting lightpush publisher"

  var
    switch = newStandardSwitch()
    pm = PeerManager.new(switch)
    wlc = WakuLegacyLightpushClient.new(pm, rng)

  # Start maintaining subscription
  asyncSpawn publishMessages(
    wlc, lightpushPeer, LightpushPubsubTopic, LightpushContentTopic
  )

when isMainModule:
  let rng = newRng()
  setupAndPublish(rng)
  runForever()
