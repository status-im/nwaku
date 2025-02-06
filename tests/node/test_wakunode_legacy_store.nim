{.used.}

import
  std/options,
  stew/shims/net as stewNet,
  testutils/unittests,
  chronos,
  libp2p/crypto/crypto

import
  waku/[
    common/paging,
    node/waku_node,
    node/peer_manager,
    waku_core,
    waku_store_legacy,
    waku_store_legacy/client,
    waku_archive_legacy,
    waku_archive_legacy/driver/sqlite_driver,
    common/databases/db_sqlite,
  ],
  ../waku_store_legacy/store_utils,
  ../waku_archive_legacy/archive_utils,
  ../testlib/[common, wakucore, wakunode, testasync, futures, testutils]

suite "Waku Store - End to End - Sorted Archive":
  var pubsubTopic {.threadvar.}: PubsubTopic
  var contentTopic {.threadvar.}: ContentTopic
  var contentTopicSeq {.threadvar.}: seq[ContentTopic]

  var archiveMessages {.threadvar.}: seq[WakuMessage]
  var historyQuery {.threadvar.}: HistoryQuery

  var server {.threadvar.}: WakuNode
  var client {.threadvar.}: WakuNode

  var archiveDriver {.threadvar.}: ArchiveDriver
  var serverRemotePeerInfo {.threadvar.}: RemotePeerInfo
  var clientPeerId {.threadvar.}: PeerId

  asyncSetup:
    pubsubTopic = DefaultPubsubTopic
    contentTopic = DefaultContentTopic
    contentTopicSeq = @[contentTopic]

    let timeOrigin = now()
    archiveMessages =
      @[
        fakeWakuMessage(@[byte 00], ts = ts(00, timeOrigin)),
        fakeWakuMessage(@[byte 01], ts = ts(10, timeOrigin)),
        fakeWakuMessage(@[byte 02], ts = ts(20, timeOrigin)),
        fakeWakuMessage(@[byte 03], ts = ts(30, timeOrigin)),
        fakeWakuMessage(@[byte 04], ts = ts(40, timeOrigin)),
        fakeWakuMessage(@[byte 05], ts = ts(50, timeOrigin)),
        fakeWakuMessage(@[byte 06], ts = ts(60, timeOrigin)),
        fakeWakuMessage(@[byte 07], ts = ts(70, timeOrigin)),
        fakeWakuMessage(@[byte 08], ts = ts(80, timeOrigin)),
        fakeWakuMessage(@[byte 09], ts = ts(90, timeOrigin)),
      ]

    historyQuery = HistoryQuery(
      pubsubTopic: some(pubsubTopic),
      contentTopics: contentTopicSeq,
      direction: PagingDirection.Forward,
      pageSize: 5,
    )

    let
      serverKey = generateSecp256k1Key()
      clientKey = generateSecp256k1Key()

    server = newTestWakuNode(serverKey, ValidIpAddress.init("0.0.0.0"), Port(0))
    client = newTestWakuNode(clientKey, ValidIpAddress.init("0.0.0.0"), Port(0))

    archiveDriver = newArchiveDriverWithMessages(pubsubTopic, archiveMessages)
    let mountArchiveResult = server.mountLegacyArchive(archiveDriver)
    assert mountArchiveResult.isOk()

    await server.mountLegacyStore()
    client.mountLegacyStoreClient()

    await allFutures(server.start(), client.start())

    serverRemotePeerInfo = server.peerInfo.toRemotePeerInfo()
    clientPeerId = client.peerInfo.toRemotePeerInfo().peerId

  asyncTeardown:
    await allFutures(client.stop(), server.stop())

  suite "Message Pagination":
    asyncTest "Forward Pagination":
      # When making a history query
      let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        queryResponse.get().messages == archiveMessages[0 ..< 5]

      # Given the next query
      var otherHistoryQuery = HistoryQuery(
        cursor: queryResponse.get().cursor,
        pubsubTopic: some(pubsubTopic),
        contentTopics: contentTopicSeq,
        direction: PagingDirection.FORWARD,
        pageSize: 5,
      )

      # When making the next history query
      let otherQueryResponse =
        await client.query(otherHistoryQuery, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        otherQueryResponse.get().messages == archiveMessages[5 ..< 10]

    asyncTest "Backward Pagination":
      # Given the history query is backward
      historyQuery.direction = PagingDirection.BACKWARD

      # When making a history query
      let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        queryResponse.get().messages == archiveMessages[5 ..< 10]

      # Given the next query
      var nextHistoryQuery = HistoryQuery(
        cursor: queryResponse.get().cursor,
        pubsubTopic: some(pubsubTopic),
        contentTopics: contentTopicSeq,
        direction: PagingDirection.BACKWARD,
        pageSize: 5,
      )

      # When making the next history query
      let otherQueryResponse =
        await client.query(nextHistoryQuery, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        otherQueryResponse.get().messages == archiveMessages[0 ..< 5]

    suite "Pagination with Differente Page Sizes":
      asyncTest "Pagination with Small Page Size":
        # Given the first query (1/5)
        historyQuery.pageSize = 2

        # When making a history query
        let queryResponse1 = await client.query(historyQuery, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse1.get().messages == archiveMessages[0 ..< 2]

        # Given the next query (2/5)
        let historyQuery2 = HistoryQuery(
          cursor: queryResponse1.get().cursor,
          pubsubTopic: some(pubsubTopic),
          contentTopics: contentTopicSeq,
          direction: PagingDirection.FORWARD,
          pageSize: 2,
        )

        # When making the next history query
        let queryResponse2 = await client.query(historyQuery2, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse2.get().messages == archiveMessages[2 ..< 4]

        # Given the next query (3/5)
        let historyQuery3 = HistoryQuery(
          cursor: queryResponse2.get().cursor,
          pubsubTopic: some(pubsubTopic),
          contentTopics: contentTopicSeq,
          direction: PagingDirection.FORWARD,
          pageSize: 2,
        )

        # When making the next history query
        let queryResponse3 = await client.query(historyQuery3, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse3.get().messages == archiveMessages[4 ..< 6]

        # Given the next query (4/5)
        let historyQuery4 = HistoryQuery(
          cursor: queryResponse3.get().cursor,
          pubsubTopic: some(pubsubTopic),
          contentTopics: contentTopicSeq,
          direction: PagingDirection.FORWARD,
          pageSize: 2,
        )

        # When making the next history query
        let queryResponse4 = await client.query(historyQuery4, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse4.get().messages == archiveMessages[6 ..< 8]

        # Given the next query (5/5)
        let historyQuery5 = HistoryQuery(
          cursor: queryResponse4.get().cursor,
          pubsubTopic: some(pubsubTopic),
          contentTopics: contentTopicSeq,
          direction: PagingDirection.FORWARD,
          pageSize: 2,
        )

        # When making the next history query
        let queryResponse5 = await client.query(historyQuery5, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse5.get().messages == archiveMessages[8 ..< 10]

      asyncTest "Pagination with Large Page Size":
        # Given the first query (1/2)
        historyQuery.pageSize = 8

        # When making a history query
        let queryResponse1 = await client.query(historyQuery, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse1.get().messages == archiveMessages[0 ..< 8]

        # Given the next query (2/2)
        let historyQuery2 = HistoryQuery(
          cursor: queryResponse1.get().cursor,
          pubsubTopic: some(pubsubTopic),
          contentTopics: contentTopicSeq,
          direction: PagingDirection.FORWARD,
          pageSize: 8,
        )

        # When making the next history query
        let queryResponse2 = await client.query(historyQuery2, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse2.get().messages == archiveMessages[8 ..< 10]

      asyncTest "Pagination with Excessive Page Size":
        # Given the first query (1/1)
        historyQuery.pageSize = 100

        # When making a history query
        let queryResponse1 = await client.query(historyQuery, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse1.get().messages == archiveMessages[0 ..< 10]

      asyncTest "Pagination with Mixed Page Size":
        # Given the first query (1/3)
        historyQuery.pageSize = 2

        # When making a history query
        let queryResponse1 = await client.query(historyQuery, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse1.get().messages == archiveMessages[0 ..< 2]

        # Given the next query (2/3)
        let historyQuery2 = HistoryQuery(
          cursor: queryResponse1.get().cursor,
          pubsubTopic: some(pubsubTopic),
          contentTopics: contentTopicSeq,
          direction: PagingDirection.FORWARD,
          pageSize: 4,
        )

        # When making the next history query
        let queryResponse2 = await client.query(historyQuery2, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse2.get().messages == archiveMessages[2 ..< 6]

        # Given the next query (3/3)
        let historyQuery3 = HistoryQuery(
          cursor: queryResponse2.get().cursor,
          pubsubTopic: some(pubsubTopic),
          contentTopics: contentTopicSeq,
          direction: PagingDirection.FORWARD,
          pageSize: 6,
        )

        # When making the next history query
        let queryResponse3 = await client.query(historyQuery3, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse3.get().messages == archiveMessages[6 ..< 10]

      asyncTest "Pagination with Zero Page Size (Behaves as DefaultPageSize)":
        # Given a message list of size higher than the default page size
        let currentStoreLen = uint((await archiveDriver.getMessagesCount()).get())
        assert archive.DefaultPageSize > currentStoreLen,
          "This test requires a store with more than (DefaultPageSize) messages"
        let missingMessagesAmount = archive.DefaultPageSize - currentStoreLen + 5

        let lastMessageTimestamp = archiveMessages[archiveMessages.len - 1].timestamp
        var extraMessages: seq[WakuMessage] = @[]
        for i in 0 ..< missingMessagesAmount:
          let
            timestampOffset = 10 * int(i + 1)
              # + 1 to avoid collision with existing messages
            message: WakuMessage =
              fakeWakuMessage(@[byte i], ts = ts(timestampOffset, lastMessageTimestamp))
          extraMessages.add(message)
        discard archiveDriver.put(pubsubTopic, extraMessages)

        let totalMessages = archiveMessages & extraMessages

        # Given the a query with zero page size (1/2)
        historyQuery.pageSize = 0

        # When making a history query
        let queryResponse1 = await client.query(historyQuery, serverRemotePeerInfo)

        # Then the response contains the archive.DefaultPageSize messages
        check:
          queryResponse1.get().messages == totalMessages[0 ..< archive.DefaultPageSize]

        # Given the next query (2/2)
        let historyQuery2 = HistoryQuery(
          cursor: queryResponse1.get().cursor,
          pubsubTopic: some(pubsubTopic),
          contentTopics: contentTopicSeq,
          direction: PagingDirection.FORWARD,
          pageSize: 0,
        )

        # When making the next history query
        let queryResponse2 = await client.query(historyQuery2, serverRemotePeerInfo)

        # Then the response contains the remaining messages
        check:
          queryResponse2.get().messages ==
            totalMessages[archive.DefaultPageSize ..< archive.DefaultPageSize + 5]

      asyncTest "Pagination with Default Page Size":
        # Given a message list of size higher than the default page size
        let currentStoreLen = uint((await archiveDriver.getMessagesCount()).get())
        assert archive.DefaultPageSize > currentStoreLen,
          "This test requires a store with more than (DefaultPageSize) messages"
        let missingMessagesAmount = archive.DefaultPageSize - currentStoreLen + 5

        let lastMessageTimestamp = archiveMessages[archiveMessages.len - 1].timestamp
        var extraMessages: seq[WakuMessage] = @[]
        for i in 0 ..< missingMessagesAmount:
          let
            timestampOffset = 10 * int(i + 1)
              # + 1 to avoid collision with existing messages
            message: WakuMessage =
              fakeWakuMessage(@[byte i], ts = ts(timestampOffset, lastMessageTimestamp))
          extraMessages.add(message)
        discard archiveDriver.put(pubsubTopic, extraMessages)

        let totalMessages = archiveMessages & extraMessages

        # Given a query with default page size (1/2)
        historyQuery = HistoryQuery(
          pubsubTopic: some(pubsubTopic),
          contentTopics: contentTopicSeq,
          direction: PagingDirection.FORWARD,
        )

        # When making a history query
        let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse.get().messages == totalMessages[0 ..< archive.DefaultPageSize]

        # Given the next query (2/2)
        let historyQuery2 = HistoryQuery(
          cursor: queryResponse.get().cursor,
          pubsubTopic: some(pubsubTopic),
          contentTopics: contentTopicSeq,
          direction: PagingDirection.FORWARD,
        )

        # When making the next history query
        let queryResponse2 = await client.query(historyQuery2, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse2.get().messages ==
            totalMessages[archive.DefaultPageSize ..< archive.DefaultPageSize + 5]

    suite "Pagination with Different Cursors":
      asyncTest "Starting Cursor":
        # Given a cursor pointing to the first message
        let cursor = computeHistoryCursor(pubsubTopic, archiveMessages[0])
        historyQuery.cursor = some(cursor)
        historyQuery.pageSize = 1

        # When making a history query
        let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

        # Then the response contains the message
        check:
          queryResponse.get().messages == archiveMessages[1 ..< 2]

      asyncTest "Middle Cursor":
        # Given a cursor pointing to the middle message1
        let cursor = computeHistoryCursor(pubsubTopic, archiveMessages[5])
        historyQuery.cursor = some(cursor)
        historyQuery.pageSize = 1

        # When making a history query
        let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

        # Then the response contains the message
        check:
          queryResponse.get().messages == archiveMessages[6 ..< 7]

      asyncTest "Ending Cursor":
        # Given a cursor pointing to the last message
        let cursor = computeHistoryCursor(pubsubTopic, archiveMessages[9])
        historyQuery.cursor = some(cursor)
        historyQuery.pageSize = 1

        # When making a history query
        let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

        # Then the response contains no messages
        check:
          queryResponse.get().messages.len == 0

    suite "Message Sorting":
      asyncTest "Cursor Reusability Across Nodes":
        # Given a different server node with the same archive
        let
          otherArchiveDriverWithMessages =
            newArchiveDriverWithMessages(pubsubTopic, archiveMessages)
          otherServerKey = generateSecp256k1Key()
          otherServer =
            newTestWakuNode(otherServerKey, ValidIpAddress.init("0.0.0.0"), Port(0))
          mountOtherArchiveResult =
            otherServer.mountLegacyArchive(otherArchiveDriverWithMessages)
        assert mountOtherArchiveResult.isOk()

        await otherServer.mountLegacyStore()

        await otherServer.start()
        let otherServerRemotePeerInfo = otherServer.peerInfo.toRemotePeerInfo()

        # When making a history query to the first server node      
        let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse.get().messages == archiveMessages[0 ..< 5]

        # Given the cursor from the first query
        let cursor = queryResponse.get().cursor

        # When making a history query to the second server node
        let otherHistoryQuery = HistoryQuery(
          cursor: cursor,
          pubsubTopic: some(pubsubTopic),
          contentTopics: contentTopicSeq,
          direction: PagingDirection.FORWARD,
          pageSize: 5,
        )
        let otherQueryResponse =
          await client.query(otherHistoryQuery, otherServerRemotePeerInfo)

        # Then the response contains the remaining messages
        check:
          otherQueryResponse.get().messages == archiveMessages[5 ..< 10]

        # Cleanup
        await otherServer.stop()

suite "Waku Store - End to End - Unsorted Archive":
  var pubsubTopic {.threadvar.}: PubsubTopic
  var contentTopic {.threadvar.}: ContentTopic
  var contentTopicSeq {.threadvar.}: seq[ContentTopic]

  var historyQuery {.threadvar.}: HistoryQuery
  var unsortedArchiveMessages {.threadvar.}: seq[WakuMessage]

  var server {.threadvar.}: WakuNode
  var client {.threadvar.}: WakuNode

  var serverRemotePeerInfo {.threadvar.}: RemotePeerInfo

  asyncSetup:
    pubsubTopic = DefaultPubsubTopic
    contentTopic = DefaultContentTopic
    contentTopicSeq = @[contentTopic]

    historyQuery = HistoryQuery(
      pubsubTopic: some(pubsubTopic),
      contentTopics: contentTopicSeq,
      direction: PagingDirection.FORWARD,
      pageSize: 5,
    )

    let timeOrigin = now()
    unsortedArchiveMessages =
      @[ # SortIndex (by timestamp and digest)
        fakeWakuMessage(@[byte 00], ts = ts(00, timeOrigin)), # 1
        fakeWakuMessage(@[byte 03], ts = ts(00, timeOrigin)), # 2
        fakeWakuMessage(@[byte 08], ts = ts(00, timeOrigin)), # 0
        fakeWakuMessage(@[byte 07], ts = ts(10, timeOrigin)), # 4
        fakeWakuMessage(@[byte 02], ts = ts(10, timeOrigin)), # 3
        fakeWakuMessage(@[byte 09], ts = ts(10, timeOrigin)), # 5
        fakeWakuMessage(@[byte 06], ts = ts(20, timeOrigin)), # 6
        fakeWakuMessage(@[byte 01], ts = ts(20, timeOrigin)), # 9
        fakeWakuMessage(@[byte 04], ts = ts(20, timeOrigin)), # 7
        fakeWakuMessage(@[byte 05], ts = ts(20, timeOrigin)), # 8
      ]

    let
      serverKey = generateSecp256k1Key()
      clientKey = generateSecp256k1Key()

    server = newTestWakuNode(serverKey, ValidIpAddress.init("0.0.0.0"), Port(0))
    client = newTestWakuNode(clientKey, ValidIpAddress.init("0.0.0.0"), Port(0))

    let
      unsortedArchiveDriverWithMessages =
        newArchiveDriverWithMessages(pubsubTopic, unsortedArchiveMessages)
      mountUnsortedArchiveResult =
        server.mountLegacyArchive(unsortedArchiveDriverWithMessages)

    assert mountUnsortedArchiveResult.isOk()

    await server.mountLegacyStore()
    client.mountLegacyStoreClient()

    await allFutures(server.start(), client.start())

    serverRemotePeerInfo = server.peerInfo.toRemotePeerInfo()

  asyncTeardown:
    await allFutures(client.stop(), server.stop())

  asyncTest "Basic (Timestamp and Digest) Sorting Validation":
    # When making a history query
    let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

    # Then the response contains the messages
    check:
      queryResponse.get().messages ==
        @[
          unsortedArchiveMessages[2],
          unsortedArchiveMessages[0],
          unsortedArchiveMessages[1],
          unsortedArchiveMessages[4],
          unsortedArchiveMessages[3],
        ]

    # Given the next query
    var historyQuery2 = HistoryQuery(
      cursor: queryResponse.get().cursor,
      pubsubTopic: some(pubsubTopic),
      contentTopics: contentTopicSeq,
      direction: PagingDirection.FORWARD,
      pageSize: 5,
    )

    # When making the next history query
    let queryResponse2 = await client.query(historyQuery2, serverRemotePeerInfo)

    # Then the response contains the messages
    check:
      queryResponse2.get().messages ==
        @[
          unsortedArchiveMessages[5],
          unsortedArchiveMessages[6],
          unsortedArchiveMessages[8],
          unsortedArchiveMessages[9],
          unsortedArchiveMessages[7],
        ]

  asyncTest "Backward pagination with Ascending Sorting":
    # Given a history query with backward pagination
    let cursor = computeHistoryCursor(pubsubTopic, unsortedArchiveMessages[4])
    historyQuery.direction = PagingDirection.BACKWARD
    historyQuery.cursor = some(cursor)

    # When making a history query
    let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

    # Then the response contains the messages
    check:
      queryResponse.get().messages ==
        @[
          unsortedArchiveMessages[2],
          unsortedArchiveMessages[0],
          unsortedArchiveMessages[1],
        ]

  asyncTest "Forward Pagination with Ascending Sorting":
    # Given a history query with forward pagination
    let cursor = computeHistoryCursor(pubsubTopic, unsortedArchiveMessages[4])
    historyQuery.direction = PagingDirection.FORWARD
    historyQuery.cursor = some(cursor)

    # When making a history query
    let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

    # Then the response contains the messages
    check:
      queryResponse.get().messages ==
        @[
          unsortedArchiveMessages[3],
          unsortedArchiveMessages[5],
          unsortedArchiveMessages[6],
          unsortedArchiveMessages[8],
          unsortedArchiveMessages[9],
        ]

suite "Waku Store - End to End - Archive with Multiple Topics":
  var pubsubTopic {.threadvar.}: PubsubTopic
  var pubsubTopicB {.threadvar.}: PubsubTopic
  var contentTopic {.threadvar.}: ContentTopic
  var contentTopicB {.threadvar.}: ContentTopic
  var contentTopicC {.threadvar.}: ContentTopic
  var contentTopicSpecials {.threadvar.}: ContentTopic
  var contentTopicSeq {.threadvar.}: seq[ContentTopic]

  var historyQuery {.threadvar.}: HistoryQuery
  var originTs {.threadvar.}: proc(offset: int): Timestamp {.gcsafe, raises: [].}
  var archiveMessages {.threadvar.}: seq[WakuMessage]

  var server {.threadvar.}: WakuNode
  var client {.threadvar.}: WakuNode

  var serverRemotePeerInfo {.threadvar.}: RemotePeerInfo

  asyncSetup:
    pubsubTopic = DefaultPubsubTopic
    pubsubTopicB = "topicB"
    contentTopic = DefaultContentTopic
    contentTopicB = "topicB"
    contentTopicC = "topicC"
    contentTopicSpecials = "!@#$%^&*()_+"
    contentTopicSeq =
      @[contentTopic, contentTopicB, contentTopicC, contentTopicSpecials]

    historyQuery = HistoryQuery(
      pubsubTopic: some(pubsubTopic),
      contentTopics: contentTopicSeq,
      direction: PagingDirection.FORWARD,
      pageSize: 5,
    )

    let timeOrigin = now()
    originTs = proc(offset = 0): Timestamp {.gcsafe, raises: [].} =
      ts(offset, timeOrigin)

    archiveMessages =
      @[
        fakeWakuMessage(@[byte 00], ts = originTs(00), contentTopic = contentTopic),
        fakeWakuMessage(@[byte 01], ts = originTs(10), contentTopic = contentTopicB),
        fakeWakuMessage(@[byte 02], ts = originTs(20), contentTopic = contentTopicC),
        fakeWakuMessage(@[byte 03], ts = originTs(30), contentTopic = contentTopic),
        fakeWakuMessage(@[byte 04], ts = originTs(40), contentTopic = contentTopicB),
        fakeWakuMessage(@[byte 05], ts = originTs(50), contentTopic = contentTopicC),
        fakeWakuMessage(@[byte 06], ts = originTs(60), contentTopic = contentTopic),
        fakeWakuMessage(@[byte 07], ts = originTs(70), contentTopic = contentTopicB),
        fakeWakuMessage(@[byte 08], ts = originTs(80), contentTopic = contentTopicC),
        fakeWakuMessage(
          @[byte 09], ts = originTs(90), contentTopic = contentTopicSpecials
        ),
      ]

    let
      serverKey = generateSecp256k1Key()
      clientKey = generateSecp256k1Key()

    server = newTestWakuNode(serverKey, ValidIpAddress.init("0.0.0.0"), Port(0))
    client = newTestWakuNode(clientKey, ValidIpAddress.init("0.0.0.0"), Port(0))

    let archiveDriver = newSqliteArchiveDriver()
      .put(pubsubTopic, archiveMessages[0 ..< 6])
      .put(pubsubTopicB, archiveMessages[6 ..< 10])
    let mountSortedArchiveResult = server.mountLegacyArchive(archiveDriver)

    assert mountSortedArchiveResult.isOk()

    await server.mountLegacyStore()
    client.mountLegacyStoreClient()

    await allFutures(server.start(), client.start())

    serverRemotePeerInfo = server.peerInfo.toRemotePeerInfo()

  asyncTeardown:
    await allFutures(client.stop(), server.stop())

  suite "Validation of Content Filtering":
    asyncTest "Basic Content Filtering":
      # Given a history query with content filtering
      historyQuery.contentTopics = @[contentTopic]

      # When making a history query
      let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        queryResponse.get().messages == @[archiveMessages[0], archiveMessages[3]]

    asyncTest "Multiple Content Filters":
      # Given a history query with multiple content filtering
      historyQuery.contentTopics = @[contentTopic, contentTopicB]

      # When making a history query
      let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        queryResponse.get().messages ==
          @[
            archiveMessages[0],
            archiveMessages[1],
            archiveMessages[3],
            archiveMessages[4],
          ]

    asyncTest "Empty Content Filtering":
      # Given a history query with empty content filtering
      historyQuery.contentTopics = @[]

      # When making a history query
      let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        queryResponse.get().messages == archiveMessages[0 ..< 5]

      # Given the next query
      let historyQuery2 = HistoryQuery(
        cursor: queryResponse.get().cursor,
        pubsubTopic: none(PubsubTopic),
        contentTopics: contentTopicSeq,
        direction: PagingDirection.FORWARD,
        pageSize: 5,
      )

      # When making the next history query
      let queryResponse2 = await client.query(historyQuery2, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        queryResponse2.get().messages == archiveMessages[5 ..< 10]

    asyncTest "Non-Existent Content Topic":
      # Given a history query with non-existent content filtering
      historyQuery.contentTopics = @["non-existent-topic"]

      # When making a history query
      let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

      # Then the response contains no messages
      check:
        queryResponse.get().messages.len == 0

    asyncTest "Special Characters in Content Filtering":
      # Given a history query with special characters in content filtering
      historyQuery.pubsubTopic = some(pubsubTopicB)
      historyQuery.contentTopics = @["!@#$%^&*()_+"]

      # When making a history query
      let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

      # Then the response contains no messages
      check:
        queryResponse.get().messages == @[archiveMessages[9]]

    asyncTest "PubsubTopic Specified":
      # Given a history query with pubsub topic specified
      historyQuery.pubsubTopic = some(pubsubTopicB)

      # When making a history query
      let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        queryResponse.get().messages ==
          @[
            archiveMessages[6],
            archiveMessages[7],
            archiveMessages[8],
            archiveMessages[9],
          ]

    asyncTest "PubsubTopic Left Empty":
      # Given a history query with pubsub topic left empty
      historyQuery.pubsubTopic = none(PubsubTopic)

      # When making a history query
      let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        queryResponse.get().messages == archiveMessages[0 ..< 5]

      # Given the next query
      let historyQuery2 = HistoryQuery(
        cursor: queryResponse.get().cursor,
        pubsubTopic: none(PubsubTopic),
        contentTopics: contentTopicSeq,
        direction: PagingDirection.FORWARD,
        pageSize: 5,
      )

      # When making the next history query
      let queryResponse2 = await client.query(historyQuery2, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        queryResponse2.get().messages == archiveMessages[5 ..< 10]

  suite "Validation of Time-based Filtering":
    asyncTest "Basic Time Filtering":
      # Given a history query with start and end time
      historyQuery.startTime = some(originTs(20))
      historyQuery.endTime = some(originTs(40))

      # When making a history query
      let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        queryResponse.get().messages ==
          @[archiveMessages[2], archiveMessages[3], archiveMessages[4]]

    asyncTest "Only Start Time Specified":
      # Given a history query with only start time
      historyQuery.startTime = some(originTs(20))
      historyQuery.endTime = none(Timestamp)

      # When making a history query
      let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        queryResponse.get().messages ==
          @[
            archiveMessages[2],
            archiveMessages[3],
            archiveMessages[4],
            archiveMessages[5],
          ]

    asyncTest "Only End Time Specified":
      # Given a history query with only end time
      historyQuery.startTime = none(Timestamp)
      historyQuery.endTime = some(originTs(40))

      # When making a history query
      let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

      # Then the response contains no messages
      check:
        queryResponse.get().messages ==
          @[
            archiveMessages[0],
            archiveMessages[1],
            archiveMessages[2],
            archiveMessages[3],
            archiveMessages[4],
          ]

    asyncTest "Invalid Time Range":
      # Given a history query with invalid time range
      historyQuery.startTime = some(originTs(60))
      historyQuery.endTime = some(originTs(40))

      # When making a history query
      let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

      # Then the response contains no messages
      check:
        queryResponse.get().messages.len == 0

    asyncTest "Time Filtering with Content Filtering":
      # Given a history query with time and content filtering
      historyQuery.startTime = some(originTs(20))
      historyQuery.endTime = some(originTs(60))
      historyQuery.contentTopics = @[contentTopicC]

      # When making a history query
      let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        queryResponse.get().messages == @[archiveMessages[2], archiveMessages[5]]

    asyncTest "Messages Outside of Time Range":
      # Given a history query with a valid time range which does not contain any messages
      historyQuery.startTime = some(originTs(100))
      historyQuery.endTime = some(originTs(200))

      # When making a history query
      let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

      # Then the response contains no messages
      check:
        queryResponse.get().messages.len == 0

  suite "Ephemeral":
    # TODO: Ephemeral value is not properly set for Sqlite
    xasyncTest "Only ephemeral Messages:":
      # Given an archive with only ephemeral messages
      let
        ephemeralMessages =
          @[
            fakeWakuMessage(@[byte 00], ts = ts(00), ephemeral = true),
            fakeWakuMessage(@[byte 01], ts = ts(10), ephemeral = true),
            fakeWakuMessage(@[byte 02], ts = ts(20), ephemeral = true),
          ]
        ephemeralArchiveDriver =
          newSqliteArchiveDriver().put(pubsubTopic, ephemeralMessages)

      # And a server node with the ephemeral archive
      let
        ephemeralServerKey = generateSecp256k1Key()
        ephemeralServer =
          newTestWakuNode(ephemeralServerKey, ValidIpAddress.init("0.0.0.0"), Port(0))
        mountEphemeralArchiveResult =
          ephemeralServer.mountLegacyArchive(ephemeralArchiveDriver)
      assert mountEphemeralArchiveResult.isOk()

      await ephemeralServer.mountLegacyStore()
      await ephemeralServer.start()
      let ephemeralServerRemotePeerInfo = ephemeralServer.peerInfo.toRemotePeerInfo()

      # When making a history query to the server with only ephemeral messages
      let queryResponse =
        await client.query(historyQuery, ephemeralServerRemotePeerInfo)

      # Then the response contains no messages
      check:
        queryResponse.get().messages.len == 0

      # Cleanup
      await ephemeralServer.stop()

    xasyncTest "Mixed messages":
      # Given an archive with both ephemeral and non-ephemeral messages
      let
        ephemeralMessages =
          @[
            fakeWakuMessage(@[byte 00], ts = ts(00), ephemeral = true),
            fakeWakuMessage(@[byte 01], ts = ts(10), ephemeral = true),
            fakeWakuMessage(@[byte 02], ts = ts(20), ephemeral = true),
          ]
        nonEphemeralMessages =
          @[
            fakeWakuMessage(@[byte 03], ts = ts(30), ephemeral = false),
            fakeWakuMessage(@[byte 04], ts = ts(40), ephemeral = false),
            fakeWakuMessage(@[byte 05], ts = ts(50), ephemeral = false),
          ]
        mixedArchiveDriver = newSqliteArchiveDriver()
          .put(pubsubTopic, ephemeralMessages)
          .put(pubsubTopic, nonEphemeralMessages)

      # And a server node with the mixed archive
      let
        mixedServerKey = generateSecp256k1Key()
        mixedServer =
          newTestWakuNode(mixedServerKey, ValidIpAddress.init("0.0.0.0"), Port(0))
        mountMixedArchiveResult = mixedServer.mountLegacyArchive(mixedArchiveDriver)
      assert mountMixedArchiveResult.isOk()

      await mixedServer.mountLegacyStore()
      await mixedServer.start()
      let mixedServerRemotePeerInfo = mixedServer.peerInfo.toRemotePeerInfo()

      # When making a history query to the server with mixed messages
      let queryResponse = await client.query(historyQuery, mixedServerRemotePeerInfo)

      # Then the response contains the non-ephemeral messages
      check:
        queryResponse.get().messages == nonEphemeralMessages

      # Cleanup
      await mixedServer.stop()

  suite "Edge Case Scenarios":
    asyncTest "Empty Message Store":
      # Given an empty archive
      let emptyArchiveDriver = newSqliteArchiveDriver()

      # And a server node with the empty archive
      let
        emptyServerKey = generateSecp256k1Key()
        emptyServer =
          newTestWakuNode(emptyServerKey, ValidIpAddress.init("0.0.0.0"), Port(0))
        mountEmptyArchiveResult = emptyServer.mountLegacyArchive(emptyArchiveDriver)
      assert mountEmptyArchiveResult.isOk()

      await emptyServer.mountLegacyStore()
      await emptyServer.start()
      let emptyServerRemotePeerInfo = emptyServer.peerInfo.toRemotePeerInfo()

      # When making a history query to the server with an empty archive
      let queryResponse = await client.query(historyQuery, emptyServerRemotePeerInfo)

      # Then the response contains no messages
      check:
        queryResponse.get().messages.len == 0

      # Cleanup
      await emptyServer.stop()

    asyncTest "Voluminous Message Store":
      # Given a voluminous archive (1M+ messages)
      var voluminousArchiveMessages: seq[WakuMessage] = @[]
      for i in 0 ..< 100000:
        let topic = "topic" & $i
        voluminousArchiveMessages.add(fakeWakuMessage(@[byte i], contentTopic = topic))
      let voluminousArchiveDriverWithMessages =
        newArchiveDriverWithMessages(pubsubTopic, voluminousArchiveMessages)

      # And a server node with the voluminous archive
      let
        voluminousServerKey = generateSecp256k1Key()
        voluminousServer =
          newTestWakuNode(voluminousServerKey, ValidIpAddress.init("0.0.0.0"), Port(0))
        mountVoluminousArchiveResult =
          voluminousServer.mountLegacyArchive(voluminousArchiveDriverWithMessages)
      assert mountVoluminousArchiveResult.isOk()

      await voluminousServer.mountLegacyStore()
      await voluminousServer.start()
      let voluminousServerRemotePeerInfo = voluminousServer.peerInfo.toRemotePeerInfo()

      # Given the following history query
      historyQuery.contentTopics =
        @["topic10000", "topic30000", "topic50000", "topic70000", "topic90000"]

      # When making a history query to the server with a voluminous archive
      let queryResponse =
        await client.query(historyQuery, voluminousServerRemotePeerInfo)

      # Then the response contains the messages
      check:
        queryResponse.get().messages ==
          @[
            voluminousArchiveMessages[10000],
            voluminousArchiveMessages[30000],
            voluminousArchiveMessages[50000],
            voluminousArchiveMessages[70000],
            voluminousArchiveMessages[90000],
          ]

      # Cleanup
      await voluminousServer.stop()

    asyncTest "Large contentFilters Array":
      # Given a history query with the max contentFilters len, 10
      historyQuery.contentTopics = @[contentTopic]
      for i in 0 ..< 9:
        let topic = "topic" & $i
        historyQuery.contentTopics.add(topic)

      # When making a history query
      let queryResponse = await client.query(historyQuery, serverRemotePeerInfo)

      # Then the response should trigger no errors
      check:
        queryResponse.get().messages == @[archiveMessages[0], archiveMessages[3]]
