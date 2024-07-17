{.used.}

import
  std/[options, strscans],
  testutils/unittests,
  chronicles,
  chronos,
  libp2p/crypto/crypto

import
  waku/[
    node/peer_manager,
    common/rate_limit/setting,
    waku_core,
    waku_lightpush_legacy,
    waku_lightpush_legacy/client,
    waku_lightpush_legacy/common,
    waku_lightpush_legacy/protocol_metrics,
    waku_lightpush_legacy/rpc,
    waku_lightpush_legacy/rpc_codec,
  ],
  ../testlib/[assertions, wakucore, testasync, futures, testutils],
  ./lightpush_utils,
  ../resources/[pubsub_topics, content_topics, payloads]

suite "Rate limited push service":
  asyncTest "push message with rate limit not violated":
    ## Setup
    let
      serverSwitch = newTestSwitch()
      clientSwitch = newTestSwitch()

    await allFutures(serverSwitch.start(), clientSwitch.start())

    ## Given
    var handlerFuture = newFuture[(string, WakuMessage)]()
    let handler: PushMessageHandler = proc(
        peer: PeerId, pubsubTopic: PubsubTopic, message: WakuMessage
    ): Future[WakuLightPushResult[void]] {.async.} =
      handlerFuture.complete((pubsubTopic, message))
      return ok()

    let
      tokenPeriod = 500.millis
      server = await newTestWakuLegacyLightpushNode(
        serverSwitch, handler, some((3, tokenPeriod))
      )
      client = newTestWakuLegacyLightpushClient(clientSwitch)

    let serverPeerId = serverSwitch.peerInfo.toRemotePeerInfo()

    let sendMsgProc = proc(): Future[void] {.async.} =
      let message = fakeWakuMessage()

      handlerFuture = newFuture[(string, WakuMessage)]()
      let requestRes =
        await client.publish(DefaultPubsubTopic, message, peer = serverPeerId)

      check await handlerFuture.withTimeout(50.millis)

      assert requestRes.isOk(), requestRes.error
      check handlerFuture.finished()

      let (handledMessagePubsubTopic, handledMessage) = handlerFuture.read()

      check:
        handledMessagePubsubTopic == DefaultPubsubTopic
        handledMessage == message

    let waitInBetweenFor = 20.millis

    # Test cannot be too explicit about the time when the TokenBucket resets
    # the internal timer, although in normal use there is no use case to care about it.
    var firstWaitExtend = 300.millis

    for runCnt in 0 ..< 3:
      let startTime = Moment.now()
      for testCnt in 0 ..< 3:
        await sendMsgProc()
        await sleepAsync(20.millis)

      var endTime = Moment.now()
      var elapsed: Duration = (endTime - startTime)
      await sleepAsync(tokenPeriod - elapsed + firstWaitExtend)
      firstWaitEXtend = 100.millis

    ## Cleanup
    await allFutures(clientSwitch.stop(), serverSwitch.stop())

  asyncTest "push message with rate limit reject":
    ## Setup
    let
      serverSwitch = newTestSwitch()
      clientSwitch = newTestSwitch()

    await allFutures(serverSwitch.start(), clientSwitch.start())

    ## Given
    var handlerFuture = newFuture[(string, WakuMessage)]()
    let handler = proc(
        peer: PeerId, pubsubTopic: PubsubTopic, message: WakuMessage
    ): Future[WakuLightPushResult[void]] {.async.} =
      handlerFuture.complete((pubsubTopic, message))
      return ok()

    let
      server = await newTestWakuLegacyLightpushNode(
        serverSwitch, handler, some((3, 500.millis))
      )
      client = newTestWakuLegacyLightpushClient(clientSwitch)

    let serverPeerId = serverSwitch.peerInfo.toRemotePeerInfo()
    let topic = DefaultPubsubTopic

    let successProc = proc(): Future[void] {.async.} =
      let message = fakeWakuMessage()
      handlerFuture = newFuture[(string, WakuMessage)]()
      let requestRes =
        await client.publish(DefaultPubsubTopic, message, peer = serverPeerId)
      discard await handlerFuture.withTimeout(10.millis)

      check:
        requestRes.isOk()
        handlerFuture.finished()
      let (handledMessagePubsubTopic, handledMessage) = handlerFuture.read()
      check:
        handledMessagePubsubTopic == DefaultPubsubTopic
        handledMessage == message

    let rejectProc = proc(): Future[void] {.async.} =
      let message = fakeWakuMessage()
      handlerFuture = newFuture[(string, WakuMessage)]()
      let requestRes =
        await client.publish(DefaultPubsubTopic, message, peer = serverPeerId)
      discard await handlerFuture.withTimeout(10.millis)

      check:
        requestRes.isErr()
        requestRes.error == "TOO_MANY_REQUESTS"

    for testCnt in 0 .. 2:
      await successProc()
      await sleepAsync(20.millis)

    await rejectProc()

    await sleepAsync(500.millis)

    ## next one shall succeed due to the rate limit time window has passed
    await successProc()

    ## Cleanup
    await allFutures(clientSwitch.stop(), serverSwitch.stop())
