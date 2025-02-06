{.used.}

import std/options, stew/shims/net as stewNet, testutils/unittests, chronos
import
  waku/[waku_core, waku_lightpush_legacy/common, node/peer_manager, waku_node],
  ./testlib/wakucore,
  ./testlib/wakunode

suite "WakuNode - Legacy Lightpush":
  asyncTest "Legacy lightpush message return success":
    ## Setup
    let
      lightNodeKey = generateSecp256k1Key()
      lightNode = newTestWakuNode(lightNodeKey, parseIpAddress("0.0.0.0"), Port(0))
      bridgeNodeKey = generateSecp256k1Key()
      bridgeNode = newTestWakuNode(bridgeNodeKey, parseIpAddress("0.0.0.0"), Port(0))
      destNodeKey = generateSecp256k1Key()
      destNode = newTestWakuNode(destNodeKey, parseIpAddress("0.0.0.0"), Port(0))

    await allFutures(destNode.start(), bridgeNode.start(), lightNode.start())

    await destNode.mountRelay(@[DefaultRelayShard])
    await bridgeNode.mountRelay(@[DefaultRelayShard])
    await bridgeNode.mountLegacyLightPush()
    lightNode.mountLegacyLightPushClient()

    discard await lightNode.peerManager.dialPeer(
      bridgeNode.peerInfo.toRemotePeerInfo(), WakuLegacyLightPushCodec
    )
    await sleepAsync(100.milliseconds)
    await destNode.connectToNodes(@[bridgeNode.peerInfo.toRemotePeerInfo()])

    ## Given
    let message = fakeWakuMessage()

    var completionFutRelay = newFuture[bool]()
    proc relayHandler(
        topic: PubsubTopic, msg: WakuMessage
    ): Future[void] {.async, gcsafe.} =
      check:
        topic == DefaultPubsubTopic
        msg == message
      completionFutRelay.complete(true)

    destNode.subscribe((kind: PubsubSub, topic: DefaultPubsubTopic), some(relayHandler))

    # Wait for subscription to take effect
    await sleepAsync(100.millis)

    ## When
    let res = await lightNode.legacyLightpushPublish(some(DefaultPubsubTopic), message)
    assert res.isOk(), $res.error

    ## Then
    check await completionFutRelay.withTimeout(5.seconds)

    ## Cleanup
    await allFutures(lightNode.stop(), bridgeNode.stop(), destNode.stop())
