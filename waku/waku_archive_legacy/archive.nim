when (NimMajor, NimMinor) < (1, 4):
  {.push raises: [Defect].}
else:
  {.push raises: [].}

import
  std/[times, options, sequtils, algorithm],
  stew/byteutils,
  chronicles,
  chronos,
  metrics,
  results
import
  ../common/paging,
  ./driver,
  ../waku_core,
  ../waku_core/message/digest,
  ./common,
  ./archive_metrics

logScope:
  topics = "waku archive"

const
  DefaultPageSize*: uint = 20
  MaxPageSize*: uint = 100

  # Retention policy
  WakuArchiveDefaultRetentionPolicyInterval* = chronos.minutes(30)

  # Metrics reporting
  WakuArchiveDefaultMetricsReportInterval* = chronos.minutes(30)

  # Message validation
  # 20 seconds maximum allowable sender timestamp "drift"
  MaxMessageTimestampVariance* = getNanoSecondTime(20)

type MessageValidator* =
  proc(msg: WakuMessage): Result[void, string] {.closure, gcsafe, raises: [].}

## Archive

type WakuArchive* = ref object
  driver: ArchiveDriver

  validator: MessageValidator

proc validate*(msg: WakuMessage): Result[void, string] =
  if msg.ephemeral:
    # Ephemeral message, do not store
    return

  if msg.timestamp == 0:
    return ok()

  let
    now = getNanosecondTime(getTime().toUnixFloat())
    lowerBound = now - MaxMessageTimestampVariance
    upperBound = now + MaxMessageTimestampVariance

  if msg.timestamp < lowerBound:
    return err(invalidMessageOld)

  if upperBound < msg.timestamp:
    return err(invalidMessageFuture)

  return ok()

proc new*(
    T: type WakuArchive, driver: ArchiveDriver, validator: MessageValidator = validate
): Result[T, string] =
  if driver.isNil():
    return err("archive driver is Nil")

  let archive = WakuArchive(driver: driver, validator: validator)

  return ok(archive)

proc handleMessage*(
    self: WakuArchive, pubsubTopic: PubsubTopic, msg: WakuMessage
) {.async.} =
  let
    msgHash = computeMessageHash(pubsubTopic, msg)
    msgHashHex = msgHash.to0xHex()
    msgTimestamp =
      if msg.timestamp > 0:
        msg.timestamp
      else:
        getNanosecondTime(getTime().toUnixFloat())
    msgDigest = computeDigest(msg)
    msgDigestHex = msgDigest.data.to0xHex()

  self.validator(msg).isOkOr:
    error "failed validator message",
      msg_hash = msgHashHex,
      pubsubTopic = pubsubTopic,
      contentTopic = msg.contentTopic,
      msgTimestamp = msg.timestamp,
      usedTimestamp = msgTimestamp,
      digest = msgDigestHex,
      error = $error
    waku_legacy_archive_errors.inc(labelValues = [error])
    return

  trace "handling message",
    msg_hash = msgHashHex,
    pubsubTopic = pubsubTopic,
    contentTopic = msg.contentTopic,
    msgTimestamp = msg.timestamp,
    usedTimestamp = msgTimestamp,
    digest = msgDigestHex

  let insertStartTime = getTime().toUnixFloat()

  (await self.driver.put(pubsubTopic, msg, msgDigest, msgHash, msgTimestamp)).isOkOr:
    waku_legacy_archive_errors.inc(labelValues = [insertFailure])
    error "failed to insert message",
      msg_hash = msgHashHex,
      pubsubTopic = pubsubTopic,
      contentTopic = msg.contentTopic,
      msgTimestamp = msg.timestamp,
      digest = msgDigestHex,
      error = $error
    return

  debug "message archived",
    msg_hash = msgHashHex,
    pubsubTopic = pubsubTopic,
    contentTopic = msg.contentTopic,
    msgTimestamp = msg.timestamp,
    usedTimestamp = msgTimestamp,
    digest = msgDigestHex

  let insertDuration = getTime().toUnixFloat() - insertStartTime
  waku_legacy_archive_insert_duration_seconds.observe(insertDuration)

proc findMessages*(
    self: WakuArchive, query: ArchiveQuery
): Future[ArchiveResult] {.async, gcsafe.} =
  ## Search the archive to return a single page of messages matching the query criteria

  let maxPageSize =
    if query.pageSize <= 0:
      DefaultPageSize
    else:
      min(query.pageSize, MaxPageSize)

  let isAscendingOrder = query.direction.into()

  if query.contentTopics.len > 10:
    return err(ArchiveError.invalidQuery("too many content topics"))

  if query.cursor.isSome() and query.cursor.get().hash.len != 32:
    return err(ArchiveError.invalidQuery("invalid cursor hash length"))

  let queryStartTime = getTime().toUnixFloat()

  let rows = (
    await self.driver.getMessages(
      includeData = query.includeData,
      contentTopic = query.contentTopics,
      pubsubTopic = query.pubsubTopic,
      cursor = query.cursor,
      startTime = query.startTime,
      endTime = query.endTime,
      hashes = query.hashes,
      maxPageSize = maxPageSize + 1,
      ascendingOrder = isAscendingOrder,
      requestId = query.requestId,
    )
  ).valueOr:
    return err(ArchiveError(kind: ArchiveErrorKind.DRIVER_ERROR, cause: error))

  let queryDuration = getTime().toUnixFloat() - queryStartTime
  waku_legacy_archive_query_duration_seconds.observe(queryDuration)

  var hashes = newSeq[WakuMessageHash]()
  var messages = newSeq[WakuMessage]()
  var topics = newSeq[PubsubTopic]()
  var cursor = none(ArchiveCursor)

  if rows.len == 0:
    return ok(ArchiveResponse(hashes: hashes, messages: messages, cursor: cursor))

  ## Messages
  let pageSize = min(rows.len, int(maxPageSize))

  if query.includeData:
    topics = rows[0 ..< pageSize].mapIt(it[0])
    messages = rows[0 ..< pageSize].mapIt(it[1])

  hashes = rows[0 ..< pageSize].mapIt(it[4])

  ## Cursor
  if rows.len > int(maxPageSize):
    ## Build last message cursor
    ## The cursor is built from the last message INCLUDED in the response
    ## (i.e. the second last message in the rows list)

    let (pubsubTopic, message, digest, storeTimestamp, hash) = rows[^2]

    cursor = some(
      ArchiveCursor(
        digest: MessageDigest.fromBytes(digest),
        storeTime: storeTimestamp,
        sendertime: message.timestamp,
        pubsubTopic: pubsubTopic,
        hash: hash,
      )
    )

  # All messages MUST be returned in chronological order
  if not isAscendingOrder:
    reverse(hashes)
    reverse(messages)
    reverse(topics)

  return ok(
    ArchiveResponse(hashes: hashes, messages: messages, topics: topics, cursor: cursor)
  )

proc findMessagesV2*(
    self: WakuArchive, query: ArchiveQuery
): Future[ArchiveResult] {.async, deprecated, gcsafe.} =
  ## Search the archive to return a single page of messages matching the query criteria

  let maxPageSize =
    if query.pageSize <= 0:
      DefaultPageSize
    else:
      min(query.pageSize, MaxPageSize)

  let isAscendingOrder = query.direction.into()

  if query.contentTopics.len > 10:
    return err(ArchiveError.invalidQuery("too many content topics"))

  let queryStartTime = getTime().toUnixFloat()

  let rows = (
    await self.driver.getMessagesV2(
      contentTopic = query.contentTopics,
      pubsubTopic = query.pubsubTopic,
      cursor = query.cursor,
      startTime = query.startTime,
      endTime = query.endTime,
      maxPageSize = maxPageSize + 1,
      ascendingOrder = isAscendingOrder,
      requestId = query.requestId,
    )
  ).valueOr:
    return err(ArchiveError(kind: ArchiveErrorKind.DRIVER_ERROR, cause: error))

  let queryDuration = getTime().toUnixFloat() - queryStartTime
  waku_legacy_archive_query_duration_seconds.observe(queryDuration)

  var messages = newSeq[WakuMessage]()
  var cursor = none(ArchiveCursor)

  if rows.len == 0:
    return ok(ArchiveResponse(messages: messages, cursor: cursor))

  ## Messages
  let pageSize = min(rows.len, int(maxPageSize))

  messages = rows[0 ..< pageSize].mapIt(it[1])

  ## Cursor
  if rows.len > int(maxPageSize):
    ## Build last message cursor
    ## The cursor is built from the last message INCLUDED in the response
    ## (i.e. the second last message in the rows list)

    let (pubsubTopic, message, digest, storeTimestamp, _) = rows[^2]

    cursor = some(
      ArchiveCursor(
        digest: MessageDigest.fromBytes(digest),
        storeTime: storeTimestamp,
        sendertime: message.timestamp,
        pubsubTopic: pubsubTopic,
      )
    )

  # All messages MUST be returned in chronological order
  if not isAscendingOrder:
    reverse(messages)

  return ok(ArchiveResponse(messages: messages, cursor: cursor))
