{.push raises: [].}

import
  std/[times, options, sequtils, algorithm],
  stew/[byteutils],
  chronicles,
  chronos,
  results,
  metrics
import
  ../common/paging,
  ./driver,
  ./retention_policy,
  ../waku_core,
  ../waku_core/message/digest,
  ./common,
  ./archive_metrics

logScope:
  topics = "waku archive"

const
  DefaultPageSize*: uint = 20
  MaxPageSize*: uint = 100
  MaxContentTopicsPerQuery*: int = 10

  # Retention policy
  WakuArchiveDefaultRetentionPolicyInterval* = chronos.minutes(30)
  WakuArchiveDefaultRetentionPolicyIntervalWhenError* = chronos.minutes(1)

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

  retentionPolicy: Option[RetentionPolicy]

  retentionPolicyHandle: Future[void]
  metricsHandle: Future[void]

proc validate*(msg: WakuMessage): Result[void, string] =
  if msg.ephemeral:
    # Ephemeral message, do not store
    return

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
    T: type WakuArchive,
    driver: ArchiveDriver,
    validator: MessageValidator = validate,
    retentionPolicy = none(RetentionPolicy),
): Result[T, string] =
  if driver.isNil():
    return err("archive driver is Nil")

  let archive =
    WakuArchive(driver: driver, validator: validator, retentionPolicy: retentionPolicy)

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

  self.validator(msg).isOkOr:
    error "failed validator message",
      msg_hash = msgHashHex,
      pubsubTopic = pubsubTopic,
      contentTopic = msg.contentTopic,
      msgTimestamp = msg.timestamp,
      usedTimestamp = msgTimestamp,
      error = $error
    waku_archive_errors.inc(labelValues = [error])
    return

  let insertStartTime = getTime().toUnixFloat()

  (await self.driver.put(msgHash, pubsubTopic, msg)).isOkOr:
    waku_archive_errors.inc(labelValues = [insertFailure])
    error "failed to insert message",
      msg_hash = msgHash.to0xHex(),
      pubsubTopic = pubsubTopic,
      contentTopic = msg.contentTopic,
      timestamp = msg.timestamp,
      error = $error
    return

  debug "message archived",
    msg_hash = msgHashHex,
    pubsubTopic = pubsubTopic,
    contentTopic = msg.contentTopic,
    msgTimestamp = msg.timestamp,
    usedTimestamp = msgTimestamp

  let insertDuration = getTime().toUnixFloat() - insertStartTime
  waku_archive_insert_duration_seconds.observe(insertDuration)

proc syncMessageIngress*(
    self: WakuArchive,
    msgHash: WakuMessageHash,
    pubsubTopic: PubsubTopic,
    msg: WakuMessage,
) {.async.} =
  let insertStartTime = getTime().toUnixFloat()

  (await self.driver.put(msgHash, pubsubTopic, msg)).isOkOr:
    waku_archive_errors.inc(labelValues = [insertFailure])
    error "failed to insert message",
      msg_hash = msgHash.to0xHex(),
      pubsubTopic = pubsubTopic,
      contentTopic = msg.contentTopic,
      timestamp = msg.timestamp,
      error = error
    return

  trace "message archived",
    msg_hash = msgHash.to0xHex(),
    pubsubTopic = pubsubTopic,
    contentTopic = msg.contentTopic,
    timestamp = msg.timestamp

  let insertDuration = getTime().toUnixFloat() - insertStartTime
  waku_archive_insert_duration_seconds.observe(insertDuration)

proc findMessages*(
    self: WakuArchive, query: ArchiveQuery
): Future[ArchiveResult] {.async, gcsafe.} =
  ## Search the archive to return a single page of messages matching the query criteria

  if query.cursor.isSome():
    let cursor = query.cursor.get()

    if cursor.len != 32:
      return
        err(ArchiveError.invalidQuery("invalid cursor hash length: " & $cursor.len))

    if cursor == EmptyWakuMessageHash:
      return err(ArchiveError.invalidQuery("all zeroes cursor hash"))

    if query.contentTopics.len > MaxContentTopicsPerQuery:
      return err(ArchiveError.invalidQuery("too many content topics"))

  let maxPageSize =
    if query.pageSize <= 0:
      DefaultPageSize
    else:
      min(query.pageSize, MaxPageSize)

  let isAscendingOrder = query.direction.into()

  let queryStartTime = getTime().toUnixFloat()

  let rows = (
    await self.driver.getMessages(
      includeData = query.includeData,
      contentTopics = query.contentTopics,
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
  waku_archive_query_duration_seconds.observe(queryDuration)

  var hashes = newSeq[WakuMessageHash]()
  var messages = newSeq[WakuMessage]()
  var topics = newSeq[PubsubTopic]()
  var cursor = none(ArchiveCursor)

  if rows.len == 0:
    return ok(ArchiveResponse(hashes: hashes, messages: messages, cursor: cursor))

  let pageSize = min(rows.len, int(maxPageSize))

  hashes = rows[0 ..< pageSize].mapIt(it[0])

  if query.includeData:
    topics = rows[0 ..< pageSize].mapIt(it[1])
    messages = rows[0 ..< pageSize].mapIt(it[2])

  if rows.len > int(maxPageSize):
    ## Build last message cursor
    ## The cursor is built from the last message INCLUDED in the response
    ## (i.e. the second last message in the rows list)

    let (hash, _, _) = rows[^2]

    cursor = some(hash)

  # Messages MUST be returned in chronological order
  if not isAscendingOrder:
    reverse(hashes)
    reverse(topics)
    reverse(messages)

  return ok(
    ArchiveResponse(cursor: cursor, topics: topics, hashes: hashes, messages: messages)
  )

proc periodicRetentionPolicy(self: WakuArchive) {.async.} =
  let policy = self.retentionPolicy.get()

  while true:
    debug "executing message retention policy"
    (await policy.execute(self.driver)).isOkOr:
      waku_archive_errors.inc(labelValues = [retPolicyFailure])
      error "failed execution of retention policy", error = error
      await sleepAsync(WakuArchiveDefaultRetentionPolicyIntervalWhenError)
      ## in case of error, let's try again faster
      continue

    await sleepAsync(WakuArchiveDefaultRetentionPolicyInterval)

proc periodicMetricReport(self: WakuArchive) {.async.} =
  while true:
    let countRes = (await self.driver.getMessagesCount())
    if countRes.isErr():
      error "loopReportStoredMessagesMetric failed to get messages count",
        error = countRes.error
    else:
      let count = countRes.get()
      waku_archive_messages.set(count, labelValues = ["stored"])

    await sleepAsync(WakuArchiveDefaultMetricsReportInterval)

proc start*(self: WakuArchive) =
  if self.retentionPolicy.isSome():
    self.retentionPolicyHandle = self.periodicRetentionPolicy()

  self.metricsHandle = self.periodicMetricReport()

proc stopWait*(self: WakuArchive) {.async.} =
  var futures: seq[Future[void]]

  if self.retentionPolicy.isSome() and not self.retentionPolicyHandle.isNil():
    futures.add(self.retentionPolicyHandle.cancelAndWait())

  if not self.metricsHandle.isNil:
    futures.add(self.metricsHandle.cancelAndWait())

  await noCancel(allFutures(futures))
