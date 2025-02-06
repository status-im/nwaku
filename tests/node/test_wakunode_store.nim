{.used.}

import
  std/[options, sequtils, algorithm, sets],
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
    waku_core/message/digest,
    waku_store,
    waku_store/client,
    waku_archive,
    waku_archive/driver/sqlite_driver,
    common/databases/db_sqlite,
  ],
  ../waku_store/store_utils,
  ../waku_archive/archive_utils,
  ../testlib/[common, wakucore, wakunode, testasync, futures, testutils]

suite "Waku Store - End to End - Sorted Archive":
  var pubsubTopic {.threadvar.}: PubsubTopic
  var contentTopic {.threadvar.}: ContentTopic
  var contentTopicSeq {.threadvar.}: seq[ContentTopic]

  var archiveMessages {.threadvar.}: seq[WakuMessageKeyValue]
  var storeQuery {.threadvar.}: StoreQueryRequest

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
    let messages =
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
    archiveMessages = messages.mapIt(
      WakuMessageKeyValue(
        messageHash: computeMessageHash(pubsubTopic, it),
        message: some(it),
        pubsubTopic: some(pubsubTopic),
      )
    )

    storeQuery = StoreQueryRequest(
      includeData: true,
      pubsubTopic: some(pubsubTopic),
      contentTopics: contentTopicSeq,
      paginationForward: PagingDirection.Forward,
      paginationLimit: some(uint64(5)),
    )

    let
      serverKey = generateSecp256k1Key()
      clientKey = generateSecp256k1Key()

    server = newTestWakuNode(serverKey, ValidIpAddress.init("0.0.0.0"), Port(0))
    client = newTestWakuNode(clientKey, ValidIpAddress.init("0.0.0.0"), Port(0))

    archiveDriver = newArchiveDriverWithMessages(pubsubTopic, messages)
    let mountArchiveResult = server.mountArchive(archiveDriver)
    assert mountArchiveResult.isOk()

    await server.mountStore()
    client.mountStoreClient()

    await allFutures(server.start(), client.start())

    serverRemotePeerInfo = server.peerInfo.toRemotePeerInfo()
    clientPeerId = client.peerInfo.toRemotePeerInfo().peerId

  asyncTeardown:
    await allFutures(client.stop(), server.stop())

  suite "Message Pagination":
    asyncTest "Forward Pagination":
      # When making a history query
      let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        queryResponse.get().messages == archiveMessages[0 ..< 5]

      # Given the next query
      var otherHistoryQuery = StoreQueryRequest(
        includeData: true,
        pubsubTopic: some(pubsubTopic),
        contentTopics: contentTopicSeq,
        paginationCursor: queryResponse.get().paginationCursor,
        paginationForward: PagingDirection.FORWARD,
        paginationLimit: some(uint64(5)),
      )

      # When making the next history query
      let otherQueryResponse =
        await client.query(otherHistoryQuery, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        otherQueryResponse.get().messages == archiveMessages[5 ..< 10]

    asyncTest "Backward Pagination":
      # Given the history query is backward
      storeQuery.paginationForward = PagingDirection.BACKWARD

      # When making a history query
      let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        queryResponse.get().messages == archiveMessages[5 ..< 10]

      # Given the next query
      var nextHistoryQuery = StoreQueryRequest(
        includeData: true,
        paginationCursor: queryResponse.get().paginationCursor,
        pubsubTopic: some(pubsubTopic),
        contentTopics: contentTopicSeq,
        paginationForward: PagingDirection.BACKWARD,
        paginationLimit: some(uint64(5)),
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
        storeQuery.paginationLimit = some(uint64(2))

        # When making a history query
        let queryResponse1 = await client.query(storeQuery, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse1.get().messages == archiveMessages[0 ..< 2]

        # Given the next query (2/5)
        let historyQuery2 = StoreQueryRequest(
          includeData: true,
          paginationCursor: queryResponse1.get().paginationCursor,
          pubsubTopic: some(pubsubTopic),
          contentTopics: contentTopicSeq,
          paginationForward: PagingDirection.FORWARD,
          paginationLimit: some(uint64(2)),
        )

        # When making the next history query
        let queryResponse2 = await client.query(historyQuery2, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse2.get().messages == archiveMessages[2 ..< 4]

        # Given the next query (3/5)
        let historyQuery3 = StoreQueryRequest(
          includeData: true,
          paginationCursor: queryResponse2.get().paginationCursor,
          pubsubTopic: some(pubsubTopic),
          contentTopics: contentTopicSeq,
          paginationForward: PagingDirection.FORWARD,
          paginationLimit: some(uint64(2)),
        )

        # When making the next history query
        let queryResponse3 = await client.query(historyQuery3, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse3.get().messages == archiveMessages[4 ..< 6]

        # Given the next query (4/5)
        let historyQuery4 = StoreQueryRequest(
          includeData: true,
          paginationCursor: queryResponse3.get().paginationCursor,
          pubsubTopic: some(pubsubTopic),
          contentTopics: contentTopicSeq,
          paginationForward: PagingDirection.FORWARD,
          paginationLimit: some(uint64(2)),
        )

        # When making the next history query
        let queryResponse4 = await client.query(historyQuery4, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse4.get().messages == archiveMessages[6 ..< 8]

        # Given the next query (5/5)
        let historyQuery5 = StoreQueryRequest(
          includeData: true,
          paginationCursor: queryResponse4.get().paginationCursor,
          pubsubTopic: some(pubsubTopic),
          contentTopics: contentTopicSeq,
          paginationForward: PagingDirection.FORWARD,
          paginationLimit: some(uint64(2)),
        )

        # When making the next history query
        let queryResponse5 = await client.query(historyQuery5, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse5.get().messages == archiveMessages[8 ..< 10]

      asyncTest "Pagination with Large Page Size":
        # Given the first query (1/2)
        storeQuery.paginationLimit = some(uint64(8))

        # When making a history query
        let queryResponse1 = await client.query(storeQuery, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse1.get().messages == archiveMessages[0 ..< 8]

        # Given the next query (2/2)
        let historyQuery2 = StoreQueryRequest(
          includeData: true,
          paginationCursor: queryResponse1.get().paginationCursor,
          pubsubTopic: some(pubsubTopic),
          contentTopics: contentTopicSeq,
          paginationForward: PagingDirection.FORWARD,
          paginationLimit: some(uint64(8)),
        )

        # When making the next history query
        let queryResponse2 = await client.query(historyQuery2, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse2.get().messages == archiveMessages[8 ..< 10]

      asyncTest "Pagination with Excessive Page Size":
        # Given the first query (1/1)
        storeQuery.paginationLimit = some(uint64(100))

        # When making a history query
        let queryResponse1 = await client.query(storeQuery, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse1.get().messages == archiveMessages[0 ..< 10]

      asyncTest "Pagination with Mixed Page Size":
        # Given the first query (1/3)
        storeQuery.paginationLimit = some(uint64(2))

        # When making a history query
        let queryResponse1 = await client.query(storeQuery, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse1.get().messages == archiveMessages[0 ..< 2]

        # Given the next query (2/3)
        let historyQuery2 = StoreQueryRequest(
          includeData: true,
          paginationCursor: queryResponse1.get().paginationCursor,
          pubsubTopic: some(pubsubTopic),
          contentTopics: contentTopicSeq,
          paginationForward: PagingDirection.FORWARD,
          paginationLimit: some(uint64(4)),
        )

        # When making the next history query
        let queryResponse2 = await client.query(historyQuery2, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse2.get().messages == archiveMessages[2 ..< 6]

        # Given the next query (3/3)
        let historyQuery3 = StoreQueryRequest(
          includeData: true,
          paginationCursor: queryResponse2.get().paginationCursor,
          pubsubTopic: some(pubsubTopic),
          contentTopics: contentTopicSeq,
          paginationForward: PagingDirection.FORWARD,
          paginationLimit: some(uint64(6)),
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

        let lastMessageTimestamp =
          archiveMessages[archiveMessages.len - 1].message.get().timestamp
        var extraMessages: seq[WakuMessage] = @[]
        for i in 0 ..< missingMessagesAmount:
          let
            timestampOffset = 10 * int(i + 1)
              # + 1 to avoid collision with existing messages
            message: WakuMessage =
              fakeWakuMessage(@[byte i], ts = ts(timestampOffset, lastMessageTimestamp))
          extraMessages.add(message)
        discard archiveDriver.put(pubsubTopic, extraMessages)

        let totalMessages =
          archiveMessages &
          extraMessages.mapIt(
            WakuMessageKeyValue(
              messageHash: computeMessageHash(pubsubTopic, it),
              message: some(it),
              pubsubTopic: some(pubsubTopic),
            )
          )

        # Given the a query with zero page size (1/2)
        storeQuery.paginationLimit = none(uint64)

        # When making a history query
        let queryResponse1 = await client.query(storeQuery, serverRemotePeerInfo)

        # Then the response contains the archive.DefaultPageSize messages
        check:
          queryResponse1.get().messages == totalMessages[0 ..< archive.DefaultPageSize]

        # Given the next query (2/2)
        let historyQuery2 = StoreQueryRequest(
          includeData: true,
          paginationCursor: queryResponse1.get().paginationCursor,
          pubsubTopic: some(pubsubTopic),
          contentTopics: contentTopicSeq,
          paginationForward: PagingDirection.FORWARD,
          paginationLimit: none(uint64),
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

        let lastMessageTimestamp =
          archiveMessages[archiveMessages.len - 1].message.get().timestamp
        var extraMessages: seq[WakuMessage] = @[]
        for i in 0 ..< missingMessagesAmount:
          let
            timestampOffset = 10 * int(i + 1)
              # + 1 to avoid collision with existing messages
            message: WakuMessage =
              fakeWakuMessage(@[byte i], ts = ts(timestampOffset, lastMessageTimestamp))
          extraMessages.add(message)
        discard archiveDriver.put(pubsubTopic, extraMessages)

        let totalMessages =
          archiveMessages &
          extraMessages.mapIt(
            WakuMessageKeyValue(
              messageHash: computeMessageHash(pubsubTopic, it),
              message: some(it),
              pubsubTopic: some(pubsubTopic),
            )
          )

        # Given a query with default page size (1/2)
        storeQuery = StoreQueryRequest(
          includeData: true,
          pubsubTopic: some(pubsubTopic),
          contentTopics: contentTopicSeq,
          paginationForward: PagingDirection.FORWARD,
        )

        # When making a history query
        let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse.get().messages == totalMessages[0 ..< archive.DefaultPageSize]

        # Given the next query (2/2)
        let historyQuery2 = StoreQueryRequest(
          includeData: true,
          paginationCursor: queryResponse.get().paginationCursor,
          pubsubTopic: some(pubsubTopic),
          contentTopics: contentTopicSeq,
          paginationForward: PagingDirection.FORWARD,
        )

        # When making the next history query
        let queryResponse2 = await client.query(historyQuery2, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse2.get().messages ==
            totalMessages[archive.DefaultPageSize ..< archive.DefaultPageSize + 5]

    suite "Pagination with Different Cursors":
      asyncTest "Starting Cursor":
        # Given a paginationCursor pointing to the first message
        let paginationCursor = archiveMessages[0].messageHash
        storeQuery.paginationCursor = some(paginationCursor)
        storeQuery.paginationLimit = some(uint64(1))

        # When making a history query
        let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

        # Then the response contains the message
        check:
          queryResponse.get().messages == archiveMessages[1 ..< 2]

      asyncTest "Middle Cursor":
        # Given a paginationCursor pointing to the middle message1
        let paginationCursor = archiveMessages[5].messageHash
        storeQuery.paginationCursor = some(paginationCursor)
        storeQuery.paginationLimit = some(uint64(1))

        # When making a history query
        let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

        # Then the response contains the message
        check:
          queryResponse.get().messages == archiveMessages[6 ..< 7]

      asyncTest "Ending Cursor":
        # Given a paginationCursor pointing to the last message
        let paginationCursor = archiveMessages[9].messageHash
        storeQuery.paginationCursor = some(paginationCursor)
        storeQuery.paginationLimit = some(uint64(1))

        # When making a history query
        let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

        # Then the response contains no messages
        check:
          queryResponse.get().messages.len == 0

    suite "Message Sorting":
      asyncTest "Cursor Reusability Across Nodes":
        # Given a different server node with the same archive
        let
          otherArchiveDriverWithMessages = newArchiveDriverWithMessages(
            pubsubTopic, archiveMessages.mapIt(it.message.get())
          )
          otherServerKey = generateSecp256k1Key()
          otherServer =
            newTestWakuNode(otherServerKey, ValidIpAddress.init("0.0.0.0"), Port(0))
          mountOtherArchiveResult =
            otherServer.mountArchive(otherArchiveDriverWithMessages)
        assert mountOtherArchiveResult.isOk()

        await otherServer.mountStore()

        await otherServer.start()
        let otherServerRemotePeerInfo = otherServer.peerInfo.toRemotePeerInfo()

        # When making a history query to the first server node      
        let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

        # Then the response contains the messages
        check:
          queryResponse.get().messages == archiveMessages[0 ..< 5]

        # Given the paginationCursor from the first query
        let paginationCursor = queryResponse.get().paginationCursor

        # When making a history query to the second server node
        let otherHistoryQuery = StoreQueryRequest(
          includeData: true,
          paginationCursor: paginationCursor,
          pubsubTopic: some(pubsubTopic),
          contentTopics: contentTopicSeq,
          paginationForward: PagingDirection.FORWARD,
          paginationLimit: some(uint64(5)),
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

  var storeQuery {.threadvar.}: StoreQueryRequest
  var unsortedArchiveMessages {.threadvar.}: seq[WakuMessageKeyValue]

  var server {.threadvar.}: WakuNode
  var client {.threadvar.}: WakuNode

  var serverRemotePeerInfo {.threadvar.}: RemotePeerInfo

  asyncSetup:
    pubsubTopic = DefaultPubsubTopic
    contentTopic = DefaultContentTopic
    contentTopicSeq = @[contentTopic]

    storeQuery = StoreQueryRequest(
      includeData: true,
      pubsubTopic: some(pubsubTopic),
      contentTopics: contentTopicSeq,
      paginationForward: PagingDirection.FORWARD,
      paginationLimit: some(uint64(5)),
    )

    let timeOrigin = now()
    let messages =
      @[
        fakeWakuMessage(@[byte 00], ts = ts(00, timeOrigin)),
        fakeWakuMessage(@[byte 03], ts = ts(00, timeOrigin)),
        fakeWakuMessage(@[byte 08], ts = ts(00, timeOrigin)),
        fakeWakuMessage(@[byte 07], ts = ts(10, timeOrigin)),
        fakeWakuMessage(@[byte 02], ts = ts(10, timeOrigin)),
        fakeWakuMessage(@[byte 09], ts = ts(10, timeOrigin)),
        fakeWakuMessage(@[byte 06], ts = ts(20, timeOrigin)),
        fakeWakuMessage(@[byte 01], ts = ts(20, timeOrigin)),
        fakeWakuMessage(@[byte 04], ts = ts(20, timeOrigin)),
        fakeWakuMessage(@[byte 05], ts = ts(20, timeOrigin)),
      ]
    unsortedArchiveMessages = messages.mapIt(
      WakuMessageKeyValue(
        messageHash: computeMessageHash(pubsubTopic, it),
        message: some(it),
        pubsubTopic: some(pubsubTopic),
      )
    )

    let
      serverKey = generateSecp256k1Key()
      clientKey = generateSecp256k1Key()

    server = newTestWakuNode(serverKey, ValidIpAddress.init("0.0.0.0"), Port(0))
    client = newTestWakuNode(clientKey, ValidIpAddress.init("0.0.0.0"), Port(0))

    let
      unsortedArchiveDriverWithMessages =
        newArchiveDriverWithMessages(pubsubTopic, messages)
      mountUnsortedArchiveResult =
        server.mountArchive(unsortedArchiveDriverWithMessages)

    assert mountUnsortedArchiveResult.isOk()

    await server.mountStore()
    client.mountStoreClient()

    await allFutures(server.start(), client.start())

    serverRemotePeerInfo = server.peerInfo.toRemotePeerInfo()

  asyncTeardown:
    await allFutures(client.stop(), server.stop())

  asyncTest "Basic (Timestamp and Hash) Sorting Validation":
    # When making a history query
    let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

    # Check the ordering
    check:
      queryResponse.get().messages.len == 5

      queryResponse.get().messages[0].message.get().timestamp ==
        queryResponse.get().messages[1].message.get().timestamp

      queryResponse.get().messages[1].message.get().timestamp ==
        queryResponse.get().messages[2].message.get().timestamp

      queryResponse.get().messages[2].message.get().timestamp <
        queryResponse.get().messages[3].message.get().timestamp

      queryResponse.get().messages[3].message.get().timestamp ==
        queryResponse.get().messages[4].message.get().timestamp

      toHex(queryResponse.get().messages[0].messageHash) <
        toHex(queryResponse.get().messages[1].messageHash)

      toHex(queryResponse.get().messages[1].messageHash) <
        toHex(queryResponse.get().messages[2].messageHash)

      toHex(queryResponse.get().messages[3].messageHash) <
        toHex(queryResponse.get().messages[4].messageHash)

    # Given the next query
    var historyQuery2 = StoreQueryRequest(
      includeData: true,
      paginationCursor: queryResponse.get().paginationCursor,
      pubsubTopic: some(pubsubTopic),
      contentTopics: contentTopicSeq,
      paginationForward: PagingDirection.FORWARD,
      paginationLimit: some(uint64(5)),
    )

    # When making the next history query
    let queryResponse2 = await client.query(historyQuery2, serverRemotePeerInfo)

    # Check the ordering
    check:
      queryResponse2.get().messages[0].message.get().timestamp <
        queryResponse2.get().messages[1].message.get().timestamp

      queryResponse2.get().messages[1].message.get().timestamp ==
        queryResponse2.get().messages[2].message.get().timestamp

      queryResponse2.get().messages[2].message.get().timestamp ==
        queryResponse2.get().messages[3].message.get().timestamp

      queryResponse2.get().messages[3].message.get().timestamp ==
        queryResponse2.get().messages[4].message.get().timestamp

      toHex(queryResponse2.get().messages[1].messageHash) <
        toHex(queryResponse2.get().messages[2].messageHash)

      toHex(queryResponse2.get().messages[2].messageHash) <
        toHex(queryResponse2.get().messages[3].messageHash)

      toHex(queryResponse2.get().messages[3].messageHash) <
        toHex(queryResponse2.get().messages[4].messageHash)

  asyncTest "Backward pagination with Ascending Sorting":
    # Given a history query with backward pagination

    # Pick the right cursor based on the ordering
    var cursor = unsortedArchiveMessages[3].messageHash
    if toHex(cursor) > toHex(unsortedArchiveMessages[4].messageHash):
      cursor = unsortedArchiveMessages[4].messageHash
    if toHex(cursor) > toHex(unsortedArchiveMessages[5].messageHash):
      cursor = unsortedArchiveMessages[5].messageHash

    storeQuery.paginationForward = PagingDirection.BACKWARD
    storeQuery.paginationCursor = some(cursor)

    # When making a history query
    let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

    # Then check the response ordering
    check:
      queryResponse.get().messages.len == 3

      queryResponse.get().messages[0].message.get().timestamp ==
        queryResponse.get().messages[1].message.get().timestamp

      queryResponse.get().messages[1].message.get().timestamp ==
        queryResponse.get().messages[2].message.get().timestamp

      toHex(queryResponse.get().messages[0].messageHash) <
        toHex(queryResponse.get().messages[1].messageHash)

      toHex(queryResponse.get().messages[1].messageHash) <
        toHex(queryResponse.get().messages[2].messageHash)

  asyncTest "Forward Pagination with Ascending Sorting":
    # Given a history query with forward pagination

    # Pick the right cursor based on the ordering
    var cursor = unsortedArchiveMessages[3].messageHash
    if toHex(cursor) > toHex(unsortedArchiveMessages[4].messageHash):
      cursor = unsortedArchiveMessages[4].messageHash
    if toHex(cursor) > toHex(unsortedArchiveMessages[5].messageHash):
      cursor = unsortedArchiveMessages[5].messageHash

    storeQuery.paginationForward = PagingDirection.FORWARD
    storeQuery.paginationCursor = some(cursor)
    storeQuery.paginationLimit = some(uint64(6))

    # When making a history query
    let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

    # Then check the response ordering
    check:
      queryResponse.get().messages.len == 6

      queryResponse.get().messages[0].message.get().timestamp ==
        queryResponse.get().messages[1].message.get().timestamp

      queryResponse.get().messages[1].message.get().timestamp <
        queryResponse.get().messages[2].message.get().timestamp

      queryResponse.get().messages[2].message.get().timestamp ==
        queryResponse.get().messages[3].message.get().timestamp

      queryResponse.get().messages[3].message.get().timestamp ==
        queryResponse.get().messages[4].message.get().timestamp

      queryResponse.get().messages[4].message.get().timestamp ==
        queryResponse.get().messages[5].message.get().timestamp

      toHex(queryResponse.get().messages[0].messageHash) <
        toHex(queryResponse.get().messages[1].messageHash)

      toHex(queryResponse.get().messages[2].messageHash) <
        toHex(queryResponse.get().messages[3].messageHash)

      toHex(queryResponse.get().messages[3].messageHash) <
        toHex(queryResponse.get().messages[4].messageHash)

      toHex(queryResponse.get().messages[4].messageHash) <
        toHex(queryResponse.get().messages[5].messageHash)

suite "Waku Store - End to End - Unsorted Archive without provided Timestamp":
  var pubsubTopic {.threadvar.}: PubsubTopic
  var contentTopic {.threadvar.}: ContentTopic
  var contentTopicSeq {.threadvar.}: seq[ContentTopic]

  var storeQuery {.threadvar.}: StoreQueryRequest
  var unsortedArchiveMessages {.threadvar.}: seq[WakuMessageKeyValue]

  var server {.threadvar.}: WakuNode
  var client {.threadvar.}: WakuNode

  var serverRemotePeerInfo {.threadvar.}: RemotePeerInfo

  asyncSetup:
    pubsubTopic = DefaultPubsubTopic
    contentTopic = DefaultContentTopic
    contentTopicSeq = @[contentTopic]

    storeQuery = StoreQueryRequest(
      includeData: true,
      pubsubTopic: some(pubsubTopic),
      contentTopics: contentTopicSeq,
      paginationForward: PagingDirection.FORWARD,
      paginationLimit: some(uint64(5)),
    )

    let messages =
      @[ # Not providing explicit timestamp means it will be set in "arrive" order
        fakeWakuMessage(@[byte 09]),
        fakeWakuMessage(@[byte 07]),
        fakeWakuMessage(@[byte 05]),
        fakeWakuMessage(@[byte 03]),
        fakeWakuMessage(@[byte 01]),
        fakeWakuMessage(@[byte 00]),
        fakeWakuMessage(@[byte 02]),
        fakeWakuMessage(@[byte 04]),
        fakeWakuMessage(@[byte 06]),
        fakeWakuMessage(@[byte 08]),
      ]
    unsortedArchiveMessages = messages.mapIt(
      WakuMessageKeyValue(
        messageHash: computeMessageHash(pubsubTopic, it),
        message: some(it),
        pubsubTopic: some(pubsubTopic),
      )
    )

    let
      serverKey = generateSecp256k1Key()
      clientKey = generateSecp256k1Key()

    server = newTestWakuNode(serverKey, ValidIpAddress.init("0.0.0.0"), Port(0))
    client = newTestWakuNode(clientKey, ValidIpAddress.init("0.0.0.0"), Port(0))

    let
      unsortedArchiveDriverWithMessages =
        newArchiveDriverWithMessages(pubsubTopic, messages)
      mountUnsortedArchiveResult =
        server.mountArchive(unsortedArchiveDriverWithMessages)

    assert mountUnsortedArchiveResult.isOk()

    await server.mountStore()
    client.mountStoreClient()

    await allFutures(server.start(), client.start())

    serverRemotePeerInfo = server.peerInfo.toRemotePeerInfo()

  asyncTeardown:
    await allFutures(client.stop(), server.stop())

  asyncTest "Sorting using receiverTime":
    # When making a history query
    let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

    check:
      queryResponse.get().messages.len == 5

      queryResponse.get().messages[0].message.get().timestamp <=
        queryResponse.get().messages[1].message.get().timestamp

      queryResponse.get().messages[1].message.get().timestamp <=
        queryResponse.get().messages[2].message.get().timestamp

      queryResponse.get().messages[2].message.get().timestamp <=
        queryResponse.get().messages[3].message.get().timestamp

      queryResponse.get().messages[3].message.get().timestamp <=
        queryResponse.get().messages[4].message.get().timestamp

    # Given the next query
    var historyQuery2 = StoreQueryRequest(
      includeData: true,
      paginationCursor: queryResponse.get().paginationCursor,
      pubsubTopic: some(pubsubTopic),
      contentTopics: contentTopicSeq,
      paginationForward: PagingDirection.FORWARD,
      paginationLimit: some(uint64(5)),
    )

    # When making the next history query
    let queryResponse2 = await client.query(historyQuery2, serverRemotePeerInfo)

    # Timestamps are quite random in this case.
    # Those are the only assumptions we can make in ALL cases.
    let setA = toHashSet(queryResponse.get().messages)
    let setB = toHashSet(queryResponse2.get().messages)
    let setC = intersection(setA, setB)

    check:
      setC.len == 0

      queryResponse2.get().messages.len == 5

      queryResponse2.get().messages[0].message.get().timestamp <=
        queryResponse2.get().messages[1].message.get().timestamp

      queryResponse2.get().messages[1].message.get().timestamp <=
        queryResponse2.get().messages[2].message.get().timestamp

      queryResponse2.get().messages[2].message.get().timestamp <=
        queryResponse2.get().messages[3].message.get().timestamp

      queryResponse2.get().messages[3].message.get().timestamp <=
        queryResponse2.get().messages[4].message.get().timestamp

suite "Waku Store - End to End - Archive with Multiple Topics":
  var pubsubTopic {.threadvar.}: PubsubTopic
  var pubsubTopicB {.threadvar.}: PubsubTopic
  var contentTopic {.threadvar.}: ContentTopic
  var contentTopicB {.threadvar.}: ContentTopic
  var contentTopicC {.threadvar.}: ContentTopic
  var contentTopicSpecials {.threadvar.}: ContentTopic
  var contentTopicSeq {.threadvar.}: seq[ContentTopic]

  var storeQuery {.threadvar.}: StoreQueryRequest
  var originTs {.threadvar.}: proc(offset: int): Timestamp {.gcsafe, raises: [].}
  var archiveMessages {.threadvar.}: seq[WakuMessageKeyValue]

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

    storeQuery = StoreQueryRequest(
      includeData: true,
      pubsubTopic: some(pubsubTopic),
      contentTopics: contentTopicSeq,
      paginationForward: PagingDirection.FORWARD,
      paginationLimit: some(uint64(5)),
    )

    let timeOrigin = now()
    originTs = proc(offset = 0): Timestamp {.gcsafe, raises: [].} =
      ts(offset, timeOrigin)

    let messages =
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

    archiveMessages = messages.mapIt(
      WakuMessageKeyValue(
        messageHash: computeMessageHash(pubsubTopic, it),
        message: some(it),
        pubsubTopic: some(pubsubTopic),
      )
    )

    for i in 6 ..< 10:
      archiveMessages[i].messagehash =
        computeMessageHash(pubsubTopicB, archiveMessages[i].message.get())

      archiveMessages[i].pubsubTopic = some(pubsubTopicB)

    let
      serverKey = generateSecp256k1Key()
      clientKey = generateSecp256k1Key()

    server = newTestWakuNode(serverKey, ValidIpAddress.init("0.0.0.0"), Port(0))
    client = newTestWakuNode(clientKey, ValidIpAddress.init("0.0.0.0"), Port(0))

    let archiveDriver = newSqliteArchiveDriver().put(pubsubTopic, messages[0 ..< 6]).put(
        pubsubTopicB, messages[6 ..< 10]
      )
    let mountUnsortedArchiveResult = server.mountArchive(archiveDriver)

    assert mountUnsortedArchiveResult.isOk()

    await server.mountStore()
    client.mountStoreClient()

    await allFutures(server.start(), client.start())

    serverRemotePeerInfo = server.peerInfo.toRemotePeerInfo()

  asyncTeardown:
    await allFutures(client.stop(), server.stop())

  suite "Validation of Content Filtering":
    asyncTest "Basic Content Filtering":
      # Given a history query with content filtering
      storeQuery.contentTopics = @[contentTopic]

      # When making a history query
      let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        queryResponse.get().messages == @[archiveMessages[0], archiveMessages[3]]

    asyncTest "Multiple Content Filters":
      # Given a history query with multiple content filtering
      storeQuery.contentTopics = @[contentTopic, contentTopicB]

      # When making a history query
      let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

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
      storeQuery.contentTopics = @[]

      # When making a history query
      let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        queryResponse.get().messages == archiveMessages[0 ..< 5]

      # Given the next query
      let historyQuery2 = StoreQueryRequest(
        includeData: true,
        paginationCursor: queryResponse.get().paginationCursor,
        pubsubTopic: none(PubsubTopic),
        contentTopics: contentTopicSeq,
        paginationForward: PagingDirection.FORWARD,
        paginationLimit: some(uint64(5)),
      )

      # When making the next history query
      let queryResponse2 = await client.query(historyQuery2, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        queryResponse2.get().messages == archiveMessages[5 ..< 10]

    asyncTest "Non-Existent Content Topic":
      # Given a history query with non-existent content filtering
      storeQuery.contentTopics = @["non-existent-topic"]

      # When making a history query
      let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

      # Then the response contains no messages
      check:
        queryResponse.get().messages.len == 0

    asyncTest "Special Characters in Content Filtering":
      # Given a history query with special characters in content filtering
      storeQuery.pubsubTopic = some(pubsubTopicB)
      storeQuery.contentTopics = @["!@#$%^&*()_+"]

      # When making a history query
      let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

      # Then the response contains no messages
      check:
        queryResponse.get().messages == @[archiveMessages[9]]

    asyncTest "PubsubTopic Specified":
      # Given a history query with pubsub topic specified
      storeQuery.pubsubTopic = some(pubsubTopicB)

      # When making a history query
      let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

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
      storeQuery.pubsubTopic = none(PubsubTopic)

      # When making a history query
      let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        queryResponse.get().messages == archiveMessages[0 ..< 5]

      # Given the next query
      let historyQuery2 = StoreQueryRequest(
        includeData: true,
        paginationCursor: queryResponse.get().paginationCursor,
        pubsubTopic: none(PubsubTopic),
        contentTopics: contentTopicSeq,
        paginationForward: PagingDirection.FORWARD,
        paginationLimit: some(uint64(5)),
      )

      # When making the next history query
      let queryResponse2 = await client.query(historyQuery2, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        queryResponse2.get().messages == archiveMessages[5 ..< 10]

  suite "Validation of Time-based Filtering":
    asyncTest "Basic Time Filtering":
      # Given a history query with start and end time
      storeQuery.startTime = some(originTs(20))
      storeQuery.endTime = some(originTs(40))

      # When making a history query
      let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        queryResponse.get().messages ==
          @[archiveMessages[2], archiveMessages[3], archiveMessages[4]]

    asyncTest "Only Start Time Specified":
      # Given a history query with only start time
      storeQuery.startTime = some(originTs(20))
      storeQuery.endTime = none(Timestamp)

      # When making a history query
      let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

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
      storeQuery.startTime = none(Timestamp)
      storeQuery.endTime = some(originTs(40))

      # When making a history query
      let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

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
      storeQuery.startTime = some(originTs(60))
      storeQuery.endTime = some(originTs(40))

      # When making a history query
      let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

      # Then the response contains no messages
      check:
        queryResponse.get().messages.len == 0

    asyncTest "Time Filtering with Content Filtering":
      # Given a history query with time and content filtering
      storeQuery.startTime = some(originTs(20))
      storeQuery.endTime = some(originTs(60))
      storeQuery.contentTopics = @[contentTopicC]

      # When making a history query
      let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

      # Then the response contains the messages
      check:
        queryResponse.get().messages == @[archiveMessages[2], archiveMessages[5]]

    asyncTest "Messages Outside of Time Range":
      # Given a history query with a valid time range which does not contain any messages
      storeQuery.startTime = some(originTs(100))
      storeQuery.endTime = some(originTs(200))

      # When making a history query
      let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

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
          ephemeralServer.mountArchive(ephemeralArchiveDriver)
      assert mountEphemeralArchiveResult.isOk()

      await ephemeralServer.mountStore()
      await ephemeralServer.start()
      let ephemeralServerRemotePeerInfo = ephemeralServer.peerInfo.toRemotePeerInfo()

      # When making a history query to the server with only ephemeral messages
      let queryResponse = await client.query(storeQuery, ephemeralServerRemotePeerInfo)

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
        mountMixedArchiveResult = mixedServer.mountArchive(mixedArchiveDriver)
      assert mountMixedArchiveResult.isOk()

      await mixedServer.mountStore()
      await mixedServer.start()
      let mixedServerRemotePeerInfo = mixedServer.peerInfo.toRemotePeerInfo()

      # When making a history query to the server with mixed messages
      let queryResponse = await client.query(storeQuery, mixedServerRemotePeerInfo)

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
        mountEmptyArchiveResult = emptyServer.mountArchive(emptyArchiveDriver)
      assert mountEmptyArchiveResult.isOk()

      await emptyServer.mountStore()
      await emptyServer.start()
      let emptyServerRemotePeerInfo = emptyServer.peerInfo.toRemotePeerInfo()

      # When making a history query to the server with an empty archive
      let queryResponse = await client.query(storeQuery, emptyServerRemotePeerInfo)

      # Then the response contains no messages
      check:
        queryResponse.get().messages.len == 0

      # Cleanup
      await emptyServer.stop()

    asyncTest "Voluminous Message Store":
      # Given a voluminous archive (1M+ messages)
      var messages: seq[WakuMessage] = @[]
      for i in 0 ..< 100000:
        let topic = "topic" & $i
        messages.add(fakeWakuMessage(@[byte i], contentTopic = topic))

      let voluminousArchiveMessages = messages.mapIt(
        WakuMessageKeyValue(
          messageHash: computeMessageHash(pubsubTopic, it),
          message: some(it),
          pubsubTopic: some(pubsubTopic),
        )
      )

      let voluminousArchiveDriverWithMessages =
        newArchiveDriverWithMessages(pubsubTopic, messages)

      # And a server node with the voluminous archive
      let
        voluminousServerKey = generateSecp256k1Key()
        voluminousServer =
          newTestWakuNode(voluminousServerKey, ValidIpAddress.init("0.0.0.0"), Port(0))
        mountVoluminousArchiveResult =
          voluminousServer.mountArchive(voluminousArchiveDriverWithMessages)
      assert mountVoluminousArchiveResult.isOk()

      await voluminousServer.mountStore()
      await voluminousServer.start()
      let voluminousServerRemotePeerInfo = voluminousServer.peerInfo.toRemotePeerInfo()

      # Given the following history query
      storeQuery.contentTopics =
        @["topic10000", "topic30000", "topic50000", "topic70000", "topic90000"]

      # When making a history query to the server with a voluminous archive
      let queryResponse = await client.query(storeQuery, voluminousServerRemotePeerInfo)

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
      storeQuery.contentTopics = @[contentTopic]
      for i in 0 ..< 9:
        let topic = "topic" & $i
        storeQuery.contentTopics.add(topic)

      # When making a history query
      let queryResponse = await client.query(storeQuery, serverRemotePeerInfo)

      # Then the response should trigger no errors
      check:
        queryResponse.get().messages == @[archiveMessages[0], archiveMessages[3]]
