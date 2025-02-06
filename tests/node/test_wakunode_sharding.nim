{.used.}

import
  std/[options, sequtils, tempfiles],
  testutils/unittests,
  chronos,
  chronicles,
  stew/shims/net as stewNet

import
  std/[sequtils, tempfiles],
  stew/byteutils,
  stew/shims/net as stewNet,
  testutils/unittests,
  chronos,
  libp2p/switch,
  libp2p/protocols/pubsub/pubsub

import
  waku/[
    waku_core/topics/pubsub_topic,
    waku_core/topics/sharding,
    waku_store_legacy/common,
    node/waku_node,
    common/paging,
    waku_core,
    waku_store/common,
    node/peer_manager,
    waku_filter_v2/client,
  ],
  ../waku_relay/utils,
  ../waku_archive/archive_utils,
  ../testlib/[assertions, common, wakucore, wakunode, testasync, futures, testutils]

import waku_relay/protocol

const
  listenIp = ValidIpAddress.init("0.0.0.0")
  listenPort = Port(0)

suite "Sharding":
  var
    server {.threadvar.}: WakuNode
    client {.threadvar.}: WakuNode

  asyncSetup:
    let
      serverKey = generateSecp256k1Key()
      clientKey = generateSecp256k1Key()

    server = newTestWakuNode(serverKey, listenIp, listenPort)
    client = newTestWakuNode(clientKey, listenIp, listenPort)

    await allFutures(server.mountRelay(), client.mountRelay())
    await allFutures(server.start(), client.start())

  asyncTeardown:
    await allFutures(server.stop(), client.stop())

  suite "Static Sharding Functionality":
    asyncTest "Shard Subscription and Peer Dialing":
      # Given a connected server and client subscribed to the same pubsub shard
      let
        topic = "/waku/2/rs/0/1"
        serverHandler = server.subscribeCompletionHandler(topic)
        clientHandler = client.subscribeCompletionHandler(topic)

      await client.connectToNodes(@[server.switch.peerInfo.toRemotePeerInfo()])

      # When the client publishes a message in the subscribed topic
      discard await client.publish(
        some(topic),
        WakuMessage(payload: "message1".toBytes(), contentTopic: "contentTopic"),
      )

      # Then the server receives the message
      let
        serverResult1 = await serverHandler.waitForResult(FUTURE_TIMEOUT)
        clientResult1 = await clientHandler.waitForResult(FUTURE_TIMEOUT)

      assertResultOk(serverResult1)
      assertResultOk(clientResult1)

      # When the server publishes a message in the subscribed topic
      serverHandler.reset()
      clientHandler.reset()
      discard await server.publish(
        some(topic),
        WakuMessage(payload: "message2".toBytes(), contentTopic: "contentTopic"),
      )

      # Then the client receives the message
      let
        serverResult2 = await serverHandler.waitForResult(FUTURE_TIMEOUT)
        clientResult2 = await clientHandler.waitForResult(FUTURE_TIMEOUT)

      assertResultOk(serverResult2)
      assertResultOk(clientResult2)

    asyncTest "Exclusion of Non-Subscribed Service Nodes":
      # When a connected server and client are subscribed to different pubsub shards
      let
        topic1 = "/waku/2/rs/0/1"
        topic2 = "/waku/2/rs/0/2"
        contentTopic = "myContentTopic"

      var
        serverHandler = server.subscribeCompletionHandler(topic1)
        clientHandler = client.subscribeCompletionHandler(topic2)

      # await sleepAsync(FUTURE_TIMEOUT)

      await client.connectToNodes(@[server.switch.peerInfo.toRemotePeerInfo()])

      # When a message is published in the server's subscribed topic
      discard await client.publish(
        some(topic1),
        WakuMessage(payload: "message1".toBytes(), contentTopic: contentTopic),
      )
      let
        serverResult1 = await serverHandler.waitForResult(FUTURE_TIMEOUT)
        clientResult1 = await clientHandler.waitForResult(FUTURE_TIMEOUT)

      # Then the server receives the message but the client does not
      assertResultOk(serverResult1)
      check clientResult1.isErr()

      # When the server publishes a message in the client's subscribed topic
      serverHandler.reset()
      clientHandler.reset()
      let wakuMessage2 =
        WakuMessage(payload: "message2".toBytes(), contentTopic: contentTopic)
      discard await server.publish(some(topic2), wakuMessage2)
      let
        serverResult2 = await serverHandler.waitForResult(FUTURE_TIMEOUT)
        clientResult2 = await clientHandler.waitForResult(FUTURE_TIMEOUT)

      # Then the client receives the message but the server does not
      check serverResult2.isErr()
      assertResultOk(clientResult2)

  suite "Automatic Sharding Mechanics":
    asyncTest "Content Topic-Based Shard Dialing":
      # Given a connected server and client subscribed to the same content topic (with two different formats)
      let
        contentTopicShort = "/toychat/2/huilong/proto"
        contentTopicFull = "/0/toychat/2/huilong/proto"
        pubsubTopic = "/waku/2/rs/0/58355"
          # Automatically generated from the contentTopic above

      let
        serverHandler = server.subscribeToContentTopicWithHandler(contentTopicShort)
        clientHandler = client.subscribeToContentTopicWithHandler(contentTopicFull)

      await client.connectToNodes(@[server.switch.peerInfo.toRemotePeerInfo()])

      # When the client publishes a message
      discard await client.publish(
        some(pubsubTopic),
        WakuMessage(payload: "message1".toBytes(), contentTopic: contentTopicShort),
      )
      let
        serverResult1 = await serverHandler.waitForResult(FUTURE_TIMEOUT)
        clientResult1 = await clientHandler.waitForResult(FUTURE_TIMEOUT)

      # Then the server and client receive the message
      assertResultOk(serverResult1)
      assertResultOk(clientResult1)

      # When the server publishes a message
      serverHandler.reset()
      clientHandler.reset()
      discard await server.publish(
        some(pubsubTopic),
        WakuMessage(payload: "message2".toBytes(), contentTopic: contentTopicFull),
      )
      let
        serverResult2 = await serverHandler.waitForResult(FUTURE_TIMEOUT)
        clientResult2 = await clientHandler.waitForResult(FUTURE_TIMEOUT)

      # Then the client and server receive the message
      assertResultOk(serverResult2)
      assertResultOk(clientResult2)

    asyncTest "Exclusion of Irrelevant Autosharded Topics":
      # Given a connected server and client subscribed to different content topics
      let
        contentTopic1 = "/toychat/2/huilong/proto"
        shard1 = "/waku/2/rs/0/58355"
        shard12 = RelayShard.parse(contentTopic1)
          # Automatically generated from the contentTopic above
        contentTopic2 = "/0/toychat2/2/huilong/proto"
        shard2 = "/waku/2/rs/0/23286"
          # Automatically generated from the contentTopic above

      let
        serverHandler = server.subscribeToContentTopicWithHandler(contentTopic1)
        clientHandler = client.subscribeToContentTopicWithHandler(contentTopic2)

      await client.connectToNodes(@[server.switch.peerInfo.toRemotePeerInfo()])

      # When the server publishes a message in the server's subscribed topic
      discard await server.publish(
        some(shard1),
        WakuMessage(payload: "message1".toBytes(), contentTopic: contentTopic1),
      )
      let
        serverResult1 = await serverHandler.waitForResult(FUTURE_TIMEOUT)
        clientResult1 = await clientHandler.waitForResult(FUTURE_TIMEOUT)

      # Then the server receives the message but the client does not
      assertResultOk(serverResult1)
      check clientResult1.isErr()

      # When the client publishes a message in the client's subscribed topic
      serverHandler.reset()
      clientHandler.reset()
      discard await client.publish(
        some(shard2),
        WakuMessage(payload: "message2".toBytes(), contentTopic: contentTopic2),
      )
      let
        serverResult2 = await serverHandler.waitForResult(FUTURE_TIMEOUT)
        clientResult2 = await clientHandler.waitForResult(FUTURE_TIMEOUT)

      # Then the client receives the message but the server does not
      assertResultOk(clientResult2)
      check serverResult2.isErr()

  suite "Application Layer Integration":
    suite "App Protocol Compatibility":
      asyncTest "relay":
        # Given a connected server and client subscribed to the same pubsub topic
        let
          pubsubTopic = "/waku/2/rs/0/1"
          serverHandler = server.subscribeCompletionHandler(pubsubTopic)
          clientHandler = client.subscribeCompletionHandler(pubsubTopic)

        await sleepAsync(FUTURE_TIMEOUT)
        await client.connectToNodes(@[server.switch.peerInfo.toRemotePeerInfo()])

        # When the client publishes a message
        discard await client.publish(
          some(pubsubTopic),
          WakuMessage(payload: "message1".toBytes(), contentTopic: "myContentTopic"),
        )
        let
          serverResult1 = await serverHandler.waitForResult(FUTURE_TIMEOUT)
          clientResult1 = await clientHandler.waitForResult(FUTURE_TIMEOUT)

        # Then the server and client receive the message
        assertResultOk(serverResult1)
        assertResultOk(clientResult1)

      asyncTest "filter":
        # Given a connected server and client using the same pubsub topic
        await client.mountFilterClient()
        await server.mountFilter()

        let pushHandlerFuture = newFuture[(string, WakuMessage)]()
        proc messagePushHandler(
            pubsubTopic: PubsubTopic, message: WakuMessage
        ): Future[void] {.async, closure, gcsafe.} =
          pushHandlerFuture.complete((pubsubTopic, message))

        client.wakuFilterClient.registerPushHandler(messagePushHandler)
        let
          pubsubTopic = "/waku/2/rs/0/1"
          contentTopic = "myContentTopic"
          subscribeResponse = await client.filterSubscribe(
            some(pubsubTopic),
            @[contentTopic],
            server.switch.peerInfo.toRemotePeerInfo(),
          )

        assertResultOk(subscribeResponse)
        await client.connectToNodes(@[server.switch.peerInfo.toRemotePeerInfo()])

        # When a peer publishes a message (the client, for testing easeness)
        let msg = WakuMessage(payload: "message".toBytes(), contentTopic: contentTopic)
        await server.filterHandleMessage(pubsubTopic, msg)

        # Then the client receives the message
        let pushHandlerResult = await pushHandlerFuture.waitForResult(FUTURE_TIMEOUT)
        assertResultOk(pushHandlerResult)

      asyncTest "lightpush":
        # Given a connected server and client subscribed to the same pubsub topic
        client.mountLegacyLightPushClient()
        await server.mountLightpush()

        let
          topic = "/waku/2/rs/0/1"
          clientHandler = client.subscribeCompletionHandler(topic)

        await client.connectToNodes(@[server.switch.peerInfo.toRemotePeerInfo()])

        # When a peer publishes a message (the client, for testing easeness)
        let
          msg =
            WakuMessage(payload: "message".toBytes(), contentTopic: "myContentTopic")
          lightpublishRespnse = await client.legacyLightpushPublish(
            some(topic), msg, server.switch.peerInfo.toRemotePeerInfo()
          )

        # Then the client receives the message
        let clientResult = await clientHandler.waitForResult(FUTURE_TIMEOUT)
        assertResultOk(clientResult)

    suite "Content Topic Filtering and Routing":
      asyncTest "relay (automatic sharding filtering)":
        # Given a connected server and client subscribed to the same content topic (with two different formats)
        let
          contentTopicShort = "/toychat/2/huilong/proto"
          contentTopicFull = "/0/toychat/2/huilong/proto"
          pubsubTopic = "/waku/2/rs/0/58355"
          serverHandler = server.subscribeToContentTopicWithHandler(contentTopicShort)
          clientHandler = client.subscribeToContentTopicWithHandler(contentTopicFull)

        await sleepAsync(FUTURE_TIMEOUT)
        await client.connectToNodes(@[server.switch.peerInfo.toRemotePeerInfo()])

        # When the client publishes a message
        discard await client.publish(
          some(pubsubTopic),
          WakuMessage(payload: "message1".toBytes(), contentTopic: contentTopicShort),
        )
        let
          serverResult1 = await serverHandler.waitForResult(FUTURE_TIMEOUT)
          clientResult1 = await clientHandler.waitForResult(FUTURE_TIMEOUT)

        # Then the server and client receive the message
        assertResultOk(serverResult1)
        assertResultOk(clientResult1)

        # When the server publishes a message
        serverHandler.reset()
        clientHandler.reset()
        discard await server.publish(
          some(pubsubTopic),
          WakuMessage(payload: "message2".toBytes(), contentTopic: contentTopicFull),
        )
        let
          serverResult2 = await serverHandler.waitForResult(FUTURE_TIMEOUT)
          clientResult2 = await clientHandler.waitForResult(FUTURE_TIMEOUT)

        # Then the server and client receive the message
        assertResultOk(serverResult2)
        assertResultOk(clientResult2)

      asyncTest "filter (automatic sharding filtering)":
        # Given a connected server and client using the same content topic (with two different formats)
        await client.mountFilterClient()
        await server.mountFilter()

        let pushHandlerFuture = newFuture[(string, WakuMessage)]()
        proc messagePushHandler(
            pubsubTopic: PubsubTopic, message: WakuMessage
        ): Future[void] {.async, closure, gcsafe.} =
          pushHandlerFuture.complete((pubsubTopic, message))

        client.wakuFilterClient.registerPushHandler(messagePushHandler)
        let
          contentTopicShort = "/toychat/2/huilong/proto"
          contentTopicFull = "/0/toychat/2/huilong/proto"
          pubsubTopic = "/waku/2/rs/0/58355"
          subscribeResponse1 = await client.filterSubscribe(
            some(pubsubTopic),
            @[contentTopicShort],
            server.switch.peerInfo.toRemotePeerInfo(),
          )

        assertResultOk(subscribeResponse1)
        await client.connectToNodes(@[server.switch.peerInfo.toRemotePeerInfo()])

        # When the client publishes a message
        let msg =
          WakuMessage(payload: "message1".toBytes(), contentTopic: contentTopicShort)
        await server.filterHandleMessage(pubsubTopic, msg)

        # Then the client receives the message
        let pushHandlerResult = await pushHandlerFuture.waitForResult(FUTURE_TIMEOUT)
        assertResultOk(pushHandlerResult)
        check pushHandlerResult.get() == (pubsubTopic, msg)

        # Given the subscription is cleared and a new subscription is made
        let
          unsubscribeResponse =
            await client.filterUnsubscribeAll(server.switch.peerInfo.toRemotePeerInfo())
          subscribeResponse2 = await client.filterSubscribe(
            some(pubsubTopic),
            @[contentTopicFull],
            server.switch.peerInfo.toRemotePeerInfo(),
          )

        assertResultOk(unsubscribeResponse)
        assertResultOk(subscribeResponse2)

        # When the client publishes a message
        pushHandlerFuture.reset()
        let msg2 =
          WakuMessage(payload: "message2".toBytes(), contentTopic: contentTopicFull)
        await server.filterHandleMessage(pubsubTopic, msg2)

        # Then the client receives the message
        let pushHandlerResult2 = await pushHandlerFuture.waitForResult(FUTURE_TIMEOUT)
        assertResultOk(pushHandlerResult2)
        check pushHandlerResult2.get() == (pubsubTopic, msg2)

      asyncTest "lightpush (automatic sharding filtering)":
        # Given a connected server and client using the same content topic (with two different formats)
        client.mountLegacyLightPushClient()
        await server.mountLightpush()

        let
          contentTopicShort = "/toychat/2/huilong/proto"
          contentTopicFull = "/0/toychat/2/huilong/proto"
          pubsubTopic = "/waku/2/rs/0/58355"
          clientHandler = client.subscribeToContentTopicWithHandler(contentTopicShort)

        await client.connectToNodes(@[server.switch.peerInfo.toRemotePeerInfo()])

        # When a peer publishes a message (the client, for testing easeness)
        let
          msg =
            WakuMessage(payload: "message".toBytes(), contentTopic: contentTopicFull)
          lightpublishRespnse = await client.legacyLightpushPublish(
            some(pubsubTopic), msg, server.switch.peerInfo.toRemotePeerInfo()
          )

        # Then the client receives the message
        let clientResult = await clientHandler.waitForResult(FUTURE_TIMEOUT)
        assertResultOk(clientResult)

      xasyncTest "store (automatic sharding filtering)":
        # Given one archive with two sets of messages using the same content topic (with two different formats)
        let
          timeOrigin = now()
          contentTopicShort = "/toychat/2/huilong/proto"
          contentTopicFull = "/0/toychat/2/huilong/proto"
          pubsubTopic = "/waku/2/rs/0/58355"
          archiveMessages1 =
            @[
              fakeWakuMessage(
                @[byte 00], ts = ts(00, timeOrigin), contentTopic = contentTopicShort
              )
            ]
          archiveMessages2 =
            @[
              fakeWakuMessage(
                @[byte 01], ts = ts(10, timeOrigin), contentTopic = contentTopicFull
              )
            ]
          archiveDriver = newArchiveDriverWithMessages(pubsubTopic, archiveMessages1)
        discard archiveDriver.put(pubsubTopic, archiveMessages2)
        let mountArchiveResult = server.mountArchive(archiveDriver)
        assertResultOk(mountArchiveResult)

        waitFor server.mountStore()
        client.mountStoreClient()

        # Given one query for each content topic format
        let
          historyQuery1 = HistoryQuery(
            contentTopics: @[contentTopicShort],
            direction: PagingDirection.Forward,
            pageSize: 3,
          )
          historyQuery2 = HistoryQuery(
            contentTopics: @[contentTopicFull],
            direction: PagingDirection.Forward,
            pageSize: 3,
          )

        # When the client queries the server for the messages
        let
          serverRemotePeerInfo = server.switch.peerInfo.toRemotePeerInfo()
          queryResponse1 = await client.query(historyQuery1, serverRemotePeerInfo)
          queryResponse2 = await client.query(historyQuery2, serverRemotePeerInfo)
        assertResultOk(queryResponse1)
        assertResultOk(queryResponse2)

        # Then the responses of both queries should contain all the messages
        check:
          queryResponse1.get().messages == archiveMessages1 & archiveMessages2
          queryResponse2.get().messages == archiveMessages1 & archiveMessages2

      asyncTest "relay - exclusion (automatic sharding filtering)":
        # Given a connected server and client subscribed to different content topics
        let
          contentTopic1 = "/toychat/2/huilong/proto"
          pubsubTopic1 = "/waku/2/rs/0/58355"
            # Automatically generated from the contentTopic above
          contentTopic2 = "/0/toychat2/2/huilong/proto"
          pubsubTopic2 = "/waku/2/rs/0/23286"
            # Automatically generated from the contentTopic above
          serverHandler = server.subscribeToContentTopicWithHandler(contentTopic1)
          clientHandler = client.subscribeToContentTopicWithHandler(contentTopic2)

        await sleepAsync(FUTURE_TIMEOUT)
        await client.connectToNodes(@[server.switch.peerInfo.toRemotePeerInfo()])

        # When the client publishes a message in the client's subscribed topic
        discard await client.publish(
          some(pubsubTopic2),
          WakuMessage(payload: "message1".toBytes(), contentTopic: contentTopic2),
        )
        let
          serverResult1 = await serverHandler.waitForResult(FUTURE_TIMEOUT)
          clientResult1 = await clientHandler.waitForResult(FUTURE_TIMEOUT)

        # Then the client receives the message but the server does not
        check serverResult1.isErr()
        assertResultOk(clientResult1)

        # When the server publishes a message in the server's subscribed topic
        serverHandler.reset()
        clientHandler.reset()
        discard await server.publish(
          some(pubsubTopic1),
          WakuMessage(payload: "message2".toBytes(), contentTopic: contentTopic1),
        )
        let
          serverResult2 = await serverHandler.waitForResult(FUTURE_TIMEOUT)
          clientResult2 = await clientHandler.waitForResult(FUTURE_TIMEOUT)

        # Then the server receives the message but the client does not
        assertResultOk(serverResult2)
        check clientResult2.isErr()

      asyncTest "filter - exclusion (automatic sharding filtering)":
        # Given a connected server and client using different content topics
        await client.mountFilterClient()
        await server.mountFilter()

        let pushHandlerFuture = newFuture[(string, WakuMessage)]()
        proc messagePushHandler(
            pubsubTopic: PubsubTopic, message: WakuMessage
        ): Future[void] {.async, closure, gcsafe.} =
          pushHandlerFuture.complete((pubsubTopic, message))

        client.wakuFilterClient.registerPushHandler(messagePushHandler)
        let
          contentTopic1 = "/toychat/2/huilong/proto"
          pubsubTopic1 = "/waku/2/rs/0/58355"
            # Automatically generated from the contentTopic above
          contentTopic2 = "/0/toychat2/2/huilong/proto"
          pubsubTopic2 = "/waku/2/rs/0/23286"
            # Automatically generated from the contentTopic above
          subscribeResponse1 = await client.filterSubscribe(
            some(pubsubTopic1),
            @[contentTopic1],
            server.switch.peerInfo.toRemotePeerInfo(),
          )

        assertResultOk(subscribeResponse1)
        await client.connectToNodes(@[server.switch.peerInfo.toRemotePeerInfo()])

        # When the server publishes a message in the server's subscribed topic
        let msg =
          WakuMessage(payload: "message1".toBytes(), contentTopic: contentTopic2)
        await server.filterHandleMessage(pubsubTopic2, msg)

        # Then the client does not receive the message
        let pushHandlerResult = await pushHandlerFuture.waitForResult(FUTURE_TIMEOUT)
        check pushHandlerResult.isErr()

      asyncTest "lightpush - exclusion (automatic sharding filtering)":
        # Given a connected server and client using different content topics
        client.mountLegacyLightPushClient()
        await server.mountLightpush()

        let
          contentTopic1 = "/toychat/2/huilong/proto"
          pubsubTopic1 = "/waku/2/rs/0/58355"
            # Automatically generated from the contentTopic above
          contentTopic2 = "/0/toychat2/2/huilong/proto"
          pubsubTopic2 = "/waku/2/rs/0/23286"
            # Automatically generated from the contentTopic above
          clientHandler = client.subscribeToContentTopicWithHandler(contentTopic1)

        await client.connectToNodes(@[server.switch.peerInfo.toRemotePeerInfo()])

        # When a peer publishes a message in the server's subscribed topic (the client, for testing easeness)
        let
          msg = WakuMessage(payload: "message".toBytes(), contentTopic: contentTopic2)
          lightpublishRespnse = await client.legacyLightpushPublish(
            some(pubsubTopic2), msg, server.switch.peerInfo.toRemotePeerInfo()
          )

        # Then the client does not receive the message
        let clientResult = await clientHandler.waitForResult(FUTURE_TIMEOUT)
        check clientResult.isErr()

      asyncTest "store - exclusion (automatic sharding filtering)":
        # Given one archive with two sets of messages using different content topics
        let
          timeOrigin = now()
          contentTopic1 = "/toychat/2/huilong/proto"
          pubsubTopic1 = "/waku/2/rs/0/58355"
            # Automatically generated from the contentTopic above
          contentTopic2 = "/0/toychat2/2/huilong/proto"
          pubsubTopic2 = "/waku/2/rs/0/23286"
            # Automatically generated from the contentTopic above
          archiveMessages1 =
            @[
              fakeWakuMessage(
                @[byte 00], ts = ts(00, timeOrigin), contentTopic = contentTopic1
              )
            ]
          archiveMessages2 =
            @[
              fakeWakuMessage(
                @[byte 01], ts = ts(10, timeOrigin), contentTopic = contentTopic2
              )
            ]
          archiveDriver = newArchiveDriverWithMessages(pubsubTopic1, archiveMessages1)
        discard archiveDriver.put(pubsubTopic2, archiveMessages2)
        let mountArchiveResult = server.mountArchive(archiveDriver)
        assertResultOk(mountArchiveResult)

        waitFor server.mountStore()
        client.mountStoreClient()

        # Given one query for each content topic
        let
          historyQuery1 = HistoryQuery(
            contentTopics: @[contentTopic1],
            direction: PagingDirection.Forward,
            pageSize: 2,
          )
          historyQuery2 = HistoryQuery(
            contentTopics: @[contentTopic2],
            direction: PagingDirection.Forward,
            pageSize: 2,
          )

        # When the client queries the server for the messages
        let
          serverRemotePeerInfo = server.switch.peerInfo.toRemotePeerInfo()
          queryResponse1 = await client.query(historyQuery1, serverRemotePeerInfo)
          queryResponse2 = await client.query(historyQuery2, serverRemotePeerInfo)
        assertResultOk(queryResponse1)
        assertResultOk(queryResponse2)

        # Then each response should contain only the messages of the corresponding content topic
        check:
          queryResponse1.get().messages == archiveMessages1
          queryResponse2.get().messages == archiveMessages2

  suite "Specific Tests":
    asyncTest "Configure Node with Multiple PubSub Topics":
      # Given a connected server and client subscribed to multiple pubsub topics
      let
        contentTopic = "myContentTopic"
        topic1 = "/waku/2/rs/0/1"
        topic2 = "/waku/2/rs/0/2"
        serverHandler1 = server.subscribeCompletionHandler(topic1)
        serverHandler2 = server.subscribeCompletionHandler(topic2)
        clientHandler1 = client.subscribeCompletionHandler(topic1)
        clientHandler2 = client.subscribeCompletionHandler(topic2)

      await client.connectToNodes(@[server.switch.peerInfo.toRemotePeerInfo()])

      # When the client publishes a message in the topic1
      discard await client.publish(
        some(topic1),
        WakuMessage(payload: "message1".toBytes(), contentTopic: contentTopic),
      )

      # Then the server and client receive the message in topic1's handlers, but not in topic2's
      assertResultOk(await serverHandler1.waitForResult(FUTURE_TIMEOUT))
      assertResultOk(await clientHandler1.waitForResult(FUTURE_TIMEOUT))
      check:
        (await serverHandler2.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await clientHandler2.waitForResult(FUTURE_TIMEOUT)).isErr()

      # When the client publishes a message in the topic2
      serverHandler1.reset()
      serverHandler2.reset()
      clientHandler1.reset()
      clientHandler2.reset()
      discard await client.publish(
        some(topic2),
        WakuMessage(payload: "message2".toBytes(), contentTopic: contentTopic),
      )

      # Then the server and client receive the message in topic2's handlers, but not in topic1's
      assertResultOk(await serverHandler2.waitForResult(FUTURE_TIMEOUT))
      assertResultOk(await clientHandler2.waitForResult(FUTURE_TIMEOUT))
      check:
        (await serverHandler1.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await clientHandler1.waitForResult(FUTURE_TIMEOUT)).isErr()

    asyncTest "Configure Node with Multiple Content Topics":
      # Given a connected server and client subscribed to multiple content topics
      let
        contentTopic1 = "/toychat/2/huilong/proto"
        pubsubTopic1 = "/waku/2/rs/0/58355"
          # Automatically generated from the contentTopic above
        contentTopic2 = "/0/toychat2/2/huilong/proto"
        pubsubTopic2 = "/waku/2/rs/0/23286"
          # Automatically generated from the contentTopic above
        serverHandler1 = server.subscribeToContentTopicWithHandler(contentTopic1)
        serverHandler2 = server.subscribeToContentTopicWithHandler(contentTopic2)
        clientHandler1 = client.subscribeToContentTopicWithHandler(contentTopic1)
        clientHandler2 = client.subscribeToContentTopicWithHandler(contentTopic2)

      await client.connectToNodes(@[server.switch.peerInfo.toRemotePeerInfo()])

      # When the client publishes a message in contentTopic1
      discard await client.publish(
        some(pubsubTopic1),
        WakuMessage(payload: "message1".toBytes(), contentTopic: contentTopic1),
      )

      # Then the server and client receive the message in contentTopic1's handlers, but not in contentTopic2's
      assertResultOk(await serverHandler1.waitForResult(FUTURE_TIMEOUT))
      assertResultOk(await clientHandler1.waitForResult(FUTURE_TIMEOUT))
      check:
        (await serverHandler2.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await clientHandler2.waitForResult(FUTURE_TIMEOUT)).isErr()

      # When the client publishes a message in contentTopic2
      serverHandler1.reset()
      serverHandler2.reset()
      clientHandler1.reset()
      clientHandler2.reset()
      discard await client.publish(
        some(pubsubTopic2),
        WakuMessage(payload: "message2".toBytes(), contentTopic: contentTopic2),
      )

      # Then the server and client receive the message in contentTopic2's handlers, but not in contentTopic1's
      assertResultOk(await serverHandler2.waitForResult(FUTURE_TIMEOUT))
      assertResultOk(await clientHandler2.waitForResult(FUTURE_TIMEOUT))
      check:
        (await serverHandler1.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await clientHandler1.waitForResult(FUTURE_TIMEOUT)).isErr()

    asyncTest "Configure Node combining Multiple Pubsub and Content Topics":
      # Given a connected server and client subscribed to multiple pubsub topics and content topics
      let
        contentTopic = "myContentTopic"
        pubsubTopic1 = "/waku/2/rs/0/1"
        pubsubTopic2 = "/waku/2/rs/0/2"
        serverHandler1 = server.subscribeCompletionHandler(pubsubTopic1)
        clientHandler1 = client.subscribeCompletionHandler(pubsubTopic1)
        serverHandler2 = server.subscribeCompletionHandler(pubsubTopic2)
        clientHandler2 = client.subscribeCompletionHandler(pubsubTopic2)
        contentTopic3 = "/toychat/2/huilong/proto"
        pubsubTopic3 = "/waku/2/rs/0/58355"
          # Automatically generated from the contentTopic above
        contentTopic4 = "/0/toychat2/2/huilong/proto"
        pubsubTopic4 = "/waku/2/rs/0/23286"
          # Automatically generated from the contentTopic above
        serverHandler3 = server.subscribeToContentTopicWithHandler(contentTopic3)
        clientHandler3 = client.subscribeToContentTopicWithHandler(contentTopic3)
        serverHandler4 = server.subscribeToContentTopicWithHandler(contentTopic4)
        clientHandler4 = client.subscribeToContentTopicWithHandler(contentTopic4)

      await client.connectToNodes(@[server.switch.peerInfo.toRemotePeerInfo()])

      # When the client publishes a message in the topic1
      discard await client.publish(
        some(pubsubTopic1),
        WakuMessage(payload: "message1".toBytes(), contentTopic: contentTopic),
      )

      # Then the server and client receive the message in topic1's handlers, but not in topic234's
      assertResultOk(await serverHandler1.waitForResult(FUTURE_TIMEOUT))
      assertResultOk(await clientHandler1.waitForResult(FUTURE_TIMEOUT))
      check:
        (await serverHandler2.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await clientHandler2.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await serverHandler3.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await clientHandler3.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await serverHandler4.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await clientHandler4.waitForResult(FUTURE_TIMEOUT)).isErr()

      # When the client publishes a message in the topic2
      serverHandler1.reset()
      clientHandler1.reset()
      serverHandler2.reset()
      clientHandler2.reset()
      serverHandler3.reset()
      clientHandler3.reset()
      serverHandler4.reset()
      clientHandler4.reset()
      discard await client.publish(
        some(pubsubTopic2),
        WakuMessage(payload: "message2".toBytes(), contentTopic: contentTopic),
      )

      # Then the server and client receive the message in topic2's handlers, but not in topic134's
      assertResultOk(await serverHandler2.waitForResult(FUTURE_TIMEOUT))
      assertResultOk(await clientHandler2.waitForResult(FUTURE_TIMEOUT))
      check:
        (await serverHandler1.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await clientHandler1.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await serverHandler3.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await clientHandler3.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await serverHandler4.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await clientHandler4.waitForResult(FUTURE_TIMEOUT)).isErr()

      # When the client publishes a message in the topic3
      serverHandler1.reset()
      clientHandler1.reset()
      serverHandler2.reset()
      clientHandler2.reset()
      serverHandler3.reset()
      clientHandler3.reset()
      serverHandler4.reset()
      clientHandler4.reset()
      discard await client.publish(
        some(pubsubTopic3),
        WakuMessage(payload: "message1".toBytes(), contentTopic: contentTopic3),
      )

      # Then the server and client receive the message in topic3's handlers, but not in topic124's
      assertResultOk(await serverHandler3.waitForResult(FUTURE_TIMEOUT))
      assertResultOk(await clientHandler3.waitForResult(FUTURE_TIMEOUT))
      check:
        (await serverHandler1.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await clientHandler1.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await serverHandler2.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await clientHandler2.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await serverHandler4.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await clientHandler4.waitForResult(FUTURE_TIMEOUT)).isErr()

      # When the client publishes a message in the topic4
      serverHandler1.reset()
      clientHandler1.reset()
      serverHandler2.reset()
      clientHandler2.reset()
      serverHandler3.reset()
      clientHandler3.reset()
      serverHandler4.reset()
      clientHandler4.reset()
      discard await client.publish(
        some(pubsubTopic4),
        WakuMessage(payload: "message2".toBytes(), contentTopic: contentTopic4),
      )

      # Then the server and client receive the message in topic4's handlers, but not in topic123's
      assertResultOk(await serverHandler4.waitForResult(FUTURE_TIMEOUT))
      assertResultOk(await clientHandler4.waitForResult(FUTURE_TIMEOUT))
      check:
        (await serverHandler1.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await clientHandler1.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await serverHandler2.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await clientHandler2.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await serverHandler3.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await clientHandler3.waitForResult(FUTURE_TIMEOUT)).isErr()

    asyncTest "Protocol with Unconfigured PubSub Topic Fails":
      # Given a
      let
        contentTopic = "myContentTopic"
        topic = "/waku/2/rs/0/1"
        # Using a different topic to simulate "unconfigured" pubsub topic
        # but to have a handler (and be able to assert the test)
        serverHandler = server.subscribeCompletionHandler("/waku/2/rs/0/0")
        clientHandler = client.subscribeCompletionHandler("/waku/2/rs/0/0")

      await client.connectToNodes(@[server.switch.peerInfo.toRemotePeerInfo()])

      # When the client publishes a message in the topic
      discard await client.publish(
        some(topic),
        WakuMessage(payload: "message1".toBytes(), contentTopic: contentTopic),
      )

      # Then the server and client don't receive the message
      check:
        (await serverHandler.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await clientHandler.waitForResult(FUTURE_TIMEOUT)).isErr()

    asyncTest "Waku LightPush Sharding (Static Sharding)":
      # Given a connected server and client using two different pubsub topics
      client.mountLegacyLightPushClient()
      await server.mountLightpush()

      # Given a connected server and client subscribed to multiple pubsub topics
      let
        contentTopic = "myContentTopic"
        topic1 = "/waku/2/rs/0/1"
        topic2 = "/waku/2/rs/0/2"
        serverHandler1 = server.subscribeCompletionHandler(topic1)
        serverHandler2 = server.subscribeCompletionHandler(topic2)
        clientHandler1 = client.subscribeCompletionHandler(topic1)
        clientHandler2 = client.subscribeCompletionHandler(topic2)

      await sleepAsync(FUTURE_TIMEOUT)

      await client.connectToNodes(@[server.switch.peerInfo.toRemotePeerInfo()])

      # When a peer publishes a message (the client, for testing easeness) in topic1
      let
        msg1 = WakuMessage(payload: "message1".toBytes(), contentTopic: contentTopic)
        lightpublishRespnse = await client.legacyLightpushPublish(
          some(topic1), msg1, server.switch.peerInfo.toRemotePeerInfo()
        )

      # Then the server and client receive the message in topic1's handlers, but not in topic2's
      assertResultOk(await clientHandler1.waitForResult(FUTURE_TIMEOUT))
      assertResultOk(await serverHandler1.waitForResult(FUTURE_TIMEOUT))
      check:
        (await clientHandler2.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await serverHandler2.waitForResult(FUTURE_TIMEOUT)).isErr()

      # When a peer publishes a message (the client, for testing easeness) in topic2
      serverHandler1.reset()
      serverHandler2.reset()
      clientHandler1.reset()
      clientHandler2.reset()
      let
        msg2 = WakuMessage(payload: "message2".toBytes(), contentTopic: contentTopic)
        lightpublishResponse2 = await client.legacyLightpushPublish(
          some(topic2), msg2, server.switch.peerInfo.toRemotePeerInfo()
        )

      # Then the server and client receive the message in topic2's handlers, but not in topic1's
      assertResultOk(await clientHandler2.waitForResult(FUTURE_TIMEOUT))
      assertResultOk(await serverHandler2.waitForResult(FUTURE_TIMEOUT))
      check:
        (await clientHandler1.waitForResult(FUTURE_TIMEOUT)).isErr()
        (await serverHandler1.waitForResult(FUTURE_TIMEOUT)).isErr()

    asyncTest "Waku Filter Sharding (Static Sharding)":
      # Given a connected server and client using two different pubsub topics
      await client.mountFilterClient()
      await server.mountFilter()

      let
        contentTopic = "myContentTopic"
        topic1 = "/waku/2/rs/0/1"
        topic2 = "/waku/2/rs/0/2"

      let
        pushHandlerFuture1 = newFuture[(string, WakuMessage)]()
        pushHandlerFuture2 = newFuture[(string, WakuMessage)]()

      proc messagePushHandler1(
          pubsubTopic: PubsubTopic, message: WakuMessage
      ): Future[void] {.async, closure, gcsafe.} =
        if topic1 == pubsubTopic:
          pushHandlerFuture1.complete((pubsubTopic, message))

      proc messagePushHandler2(
          pubsubTopic: PubsubTopic, message: WakuMessage
      ): Future[void] {.async, closure, gcsafe.} =
        if topic2 == pubsubTopic:
          pushHandlerFuture2.complete((pubsubTopic, message))

      client.wakuFilterClient.registerPushHandler(messagePushHandler1)
      client.wakuFilterClient.registerPushHandler(messagePushHandler2)

      let
        subscribeResponse1 = await client.filterSubscribe(
          some(topic1), @[contentTopic], server.switch.peerInfo.toRemotePeerInfo()
        )
        subscribeResponse2 = await client.filterSubscribe(
          some(topic2), @[contentTopic], server.switch.peerInfo.toRemotePeerInfo()
        )

      assertResultOk(subscribeResponse1)
      assertResultOk(subscribeResponse2)
      await client.connectToNodes(@[server.switch.peerInfo.toRemotePeerInfo()])

      # When the client publishes a message in topic1
      let msg = WakuMessage(payload: "message1".toBytes(), contentTopic: contentTopic)
      await server.filterHandleMessage(topic1, msg)

      # Then the client receives the message in topic1's handler, but not in topic2's
      let pushHandlerResult = await pushHandlerFuture1.waitForResult(FUTURE_TIMEOUT)
      assertResultOk(pushHandlerResult)
      check:
        pushHandlerResult.get() == (topic1, msg)
        (await pushHandlerFuture2.waitForResult(FUTURE_TIMEOUT)).isErr()

      # Given the futures are reset
      pushHandlerFuture1.reset()
      pushHandlerFuture2.reset()

      # When the client publishes a message in topic2
      let msg2 = WakuMessage(payload: "message2".toBytes(), contentTopic: contentTopic)
      await server.filterHandleMessage(topic2, msg2)

      # Then the client receives the message in topic2's handler, but not in topic1's
      let pushHandlerResult2 = await pushHandlerFuture2.waitForResult(FUTURE_TIMEOUT)
      assertResultOk(pushHandlerResult2)
      check:
        pushHandlerResult2.get() == (topic2, msg2)
        (await pushHandlerFuture1.waitForResult(FUTURE_TIMEOUT)).isErr()

    asyncTest "Waku Store Sharding (Static Sharding)":
      # Given one archive with two sets of messages using two different pubsub topics
      let
        timeOrigin = now()
        topic1 = "/waku/2/rs/0/1"
        topic2 = "/waku/2/rs/0/2"
        archiveMessages1 = @[fakeWakuMessage(@[byte 00], ts = ts(00, timeOrigin))]
        archiveMessages2 = @[fakeWakuMessage(@[byte 01], ts = ts(10, timeOrigin))]
        archiveDriver = newArchiveDriverWithMessages(topic1, archiveMessages1)
      discard archiveDriver.put(topic2, archiveMessages2)
      let mountArchiveResult = server.mountArchive(archiveDriver)
      assertResultOk(mountArchiveResult)

      waitFor server.mountStore()
      client.mountStoreClient()

      # Given one query for each pubsub topic
      let
        historyQuery1 = HistoryQuery(
          pubsubTopic: some(topic1), direction: PagingDirection.Forward, pageSize: 2
        )
        historyQuery2 = HistoryQuery(
          pubsubTopic: some(topic2), direction: PagingDirection.Forward, pageSize: 2
        )

      # When the client queries the server for the messages
      let
        serverRemotePeerInfo = server.switch.peerInfo.toRemotePeerInfo()
        queryResponse1 = await client.query(historyQuery1, serverRemotePeerInfo)
        queryResponse2 = await client.query(historyQuery2, serverRemotePeerInfo)
      assertResultOk(queryResponse1)
      assertResultOk(queryResponse2)

      # Then each response should contain only the messages of the corresponding pubsub topic
      check:
        queryResponse1.get().messages == archiveMessages1[0 ..< 1]
        queryResponse2.get().messages == archiveMessages2[0 ..< 1]
