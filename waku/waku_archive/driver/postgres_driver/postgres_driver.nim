{.push raises: [].}

import
  std/[options, sequtils, strutils, strformat, times, sugar],
  stew/[byteutils, arrayops],
  results,
  chronos,
  db_connector/[postgres, db_common],
  chronicles
import
  ../../../common/error_handling,
  ../../../waku_core,
  ../../common,
  ../../driver,
  ../../../common/databases/db_postgres as waku_postgres,
  ./postgres_healthcheck,
  ./partitions_manager

type PostgresDriver* = ref object of ArchiveDriver
  ## Establish a separate pools for read/write operations
  writeConnPool: PgAsyncPool
  readConnPool: PgAsyncPool

  ## Partition container
  partitionMngr: PartitionManager
  futLoopPartitionFactory: Future[void]

  futLoopAnalyzeTable: Future[void]

const InsertRowStmtName = "InsertRow"
const InsertRowStmtDefinition =
  """INSERT INTO messages (id, messageHash, pubsubTopic, contentTopic, payload,
  version, timestamp, meta) VALUES ($1, $2, $3, $4, $5, $6, $7, CASE WHEN $8 = '' THEN NULL ELSE $8 END) ON CONFLICT DO NOTHING;"""

const InsertRowInMessagesLookupStmtName = "InsertRowMessagesLookup"
const InsertRowInMessagesLookupStmtDefinition =
  """INSERT INTO messages_lookup (messageHash, timestamp) VALUES ($1, $2) ON CONFLICT DO NOTHING;"""

const SelectClause =
  """SELECT messageHash, pubsubTopic, contentTopic, payload, version, timestamp, meta FROM messages """

const SelectNoCursorAscStmtName = "SelectWithoutCursorAsc"
const SelectNoCursorAscStmtDef =
  SelectClause &
  """WHERE contentTopic IN ($1) AND
          messageHash IN ($2) AND
          pubsubTopic = $3 AND
          timestamp >= $4 AND
          timestamp <= $5
    ORDER BY timestamp ASC, messageHash ASC LIMIT $6;"""

const SelectNoCursorNoDataAscStmtName = "SelectWithoutCursorAndDataAsc"
const SelectNoCursorNoDataAscStmtDef =
  """SELECT messageHash FROM messages 
    WHERE contentTopic IN ($1) AND
          messageHash IN ($2) AND
          pubsubTopic = $3 AND
          timestamp >= $4 AND
          timestamp <= $5
    ORDER BY timestamp ASC, messageHash ASC LIMIT $6;"""

const SelectNoCursorDescStmtName = "SelectWithoutCursorDesc"
const SelectNoCursorDescStmtDef =
  SelectClause &
  """WHERE contentTopic IN ($1) AND
          messageHash IN ($2) AND
          pubsubTopic = $3 AND
          timestamp >= $4 AND
          timestamp <= $5
    ORDER BY timestamp DESC, messageHash DESC LIMIT $6;"""

const SelectNoCursorNoDataDescStmtName = "SelectWithoutCursorAndDataDesc"
const SelectNoCursorNoDataDescStmtDef =
  """SELECT messageHash FROM messages
    WHERE contentTopic IN ($1) AND
          messageHash IN ($2) AND
          pubsubTopic = $3 AND
          timestamp >= $4 AND
          timestamp <= $5
    ORDER BY timestamp DESC, messageHash DESC LIMIT $6;"""

const SelectWithCursorDescStmtName = "SelectWithCursorDesc"
const SelectWithCursorDescStmtDef =
  SelectClause &
  """WHERE contentTopic IN ($1) AND
          messageHash IN ($2) AND
          pubsubTopic = $3 AND
          (timestamp, messageHash) < ($4,$5) AND
          timestamp >= $6 AND
          timestamp <= $7
    ORDER BY timestamp DESC, messageHash DESC LIMIT $8;"""

const SelectWithCursorNoDataDescStmtName = "SelectWithCursorNoDataDesc"
const SelectWithCursorNoDataDescStmtDef =
  """SELECT messageHash FROM messages
    WHERE contentTopic IN ($1) AND
          messageHash IN ($2) AND
          pubsubTopic = $3 AND
          (timestamp, messageHash) < ($4,$5) AND
          timestamp >= $6 AND
          timestamp <= $7
    ORDER BY timestamp DESC, messageHash DESC LIMIT $8;"""

const SelectWithCursorAscStmtName = "SelectWithCursorAsc"
const SelectWithCursorAscStmtDef =
  SelectClause &
  """WHERE contentTopic IN ($1) AND
          messageHash IN ($2) AND
          pubsubTopic = $3 AND
          (timestamp, messageHash) > ($4,$5) AND
          timestamp >= $6 AND
          timestamp <= $7
    ORDER BY timestamp ASC, messageHash ASC LIMIT $8;"""

const SelectWithCursorNoDataAscStmtName = "SelectWithCursorNoDataAsc"
const SelectWithCursorNoDataAscStmtDef =
  """SELECT messageHash FROM messages
    WHERE contentTopic IN ($1) AND
          messageHash IN ($2) AND
          pubsubTopic = $3 AND
          (timestamp, messageHash) > ($4,$5) AND
          timestamp >= $6 AND
          timestamp <= $7
    ORDER BY timestamp ASC, messageHash ASC LIMIT $8;"""

const SelectCursorByHashName = "SelectMessageByHashInMessagesLookup"
const SelectCursorByHashDef =
  """SELECT timestamp FROM messages_lookup
    WHERE messageHash = $1"""

const
  DefaultMaxNumConns = 50
  MaxHashesPerQuery = 100

proc new*(
    T: type PostgresDriver,
    dbUrl: string,
    maxConnections = DefaultMaxNumConns,
    onFatalErrorAction: OnFatalErrorHandler = nil,
): ArchiveDriverResult[T] =
  ## Very simplistic split of max connections
  let maxNumConnOnEachPool = int(maxConnections / 2)

  let readConnPool = PgAsyncPool.new(dbUrl, maxNumConnOnEachPool).valueOr:
    return err("error creating read conn pool PgAsyncPool")

  let writeConnPool = PgAsyncPool.new(dbUrl, maxNumConnOnEachPool).valueOr:
    return err("error creating write conn pool PgAsyncPool")

  if not isNil(onFatalErrorAction):
    asyncSpawn checkConnectivity(readConnPool, onFatalErrorAction)

  if not isNil(onFatalErrorAction):
    asyncSpawn checkConnectivity(writeConnPool, onFatalErrorAction)

  let driver = PostgresDriver(
    writeConnPool: writeConnPool,
    readConnPool: readConnPool,
    partitionMngr: PartitionManager.new(),
  )
  return ok(driver)

proc reset*(s: PostgresDriver): Future[ArchiveDriverResult[void]] {.async.} =
  ## Clear the database partitions
  let targetSize = 0
  let forceRemoval = true
  let ret = await s.decreaseDatabaseSize(targetSize, forceRemoval)
  return ret

proc timeCursorCallbackImpl(pqResult: ptr PGresult, timeCursor: var Option[Timestamp]) =
  ## Callback to get a timestamp out of the DB.
  ## Used to get the cursor timestamp.

  let numFields = pqResult.pqnfields()
  if numFields != 1:
    error "Wrong number of fields"
    return

  let rawTimestamp = $(pqgetvalue(pqResult, 0, 0))

  trace "db output", rawTimestamp

  if rawTimestamp.len < 1:
    return

  let catchable = catch:
    parseBiggestInt(rawTimestamp)

  if catchable.isErr():
    error "could not parse correctly", error = catchable.error.msg
    return

  timeCursor = some(catchable.get())

proc hashCallbackImpl(
    pqResult: ptr PGresult, rows: var seq[(WakuMessageHash, PubsubTopic, WakuMessage)]
) =
  ## Callback to get a hash out of the DB.
  ## Used when queries only ask for hashes 

  let numFields = pqResult.pqnfields()
  if numFields != 1:
    error "Wrong number of fields"
    return

  for iRow in 0 ..< pqResult.pqNtuples():
    let rawHash = $(pqgetvalue(pqResult, iRow, 0))

    trace "db output", rawHash

    if rawHash.len < 1:
      return

    let catchable = catch:
      parseHexStr(rawHash)

    if catchable.isErr():
      error "could not parse correctly", error = catchable.error.msg
      return

    let hashHex = catchable.get()
    let msgHash = fromBytes(hashHex.toOpenArrayByte(0, 31))

    rows.add((msgHash, "", WakuMessage()))

proc rowCallbackImpl(
    pqResult: ptr PGresult,
    outRows: var seq[(WakuMessageHash, PubsubTopic, WakuMessage)],
) =
  ## Proc aimed to contain the logic of the callback passed to the `psasyncpool`.
  ## That callback is used in "SELECT" queries.
  ##
  ## pqResult - contains the query results
  ## outRows - seq of Store-rows. This is populated from the info contained in pqResult

  let numFields = pqResult.pqnfields()
  if numFields != 7:
    error "Wrong number of fields"
    return

  for iRow in 0 ..< pqResult.pqNtuples():
    var
      rawHash: string
      rawPayload: string
      rawVersion: string
      rawTimestamp: string
      rawMeta: string

      hashHex: string
      msgHash: WakuMessageHash

      pubSubTopic: string

      contentTopic: string
      payload: string
      version: uint
      timestamp: Timestamp
      meta: string
      wakuMessage: WakuMessage

    rawHash = $(pqgetvalue(pqResult, iRow, 0))
    pubSubTopic = $(pqgetvalue(pqResult, iRow, 1))
    contentTopic = $(pqgetvalue(pqResult, iRow, 2))
    rawPayload = $(pqgetvalue(pqResult, iRow, 3))
    rawVersion = $(pqgetvalue(pqResult, iRow, 4))
    rawTimestamp = $(pqgetvalue(pqResult, iRow, 5))
    rawMeta = $(pqgetvalue(pqResult, iRow, 6))

    trace "db output",
      rawHash, pubSubTopic, contentTopic, rawPayload, rawVersion, rawTimestamp, rawMeta

    try:
      hashHex = parseHexStr(rawHash)
      payload = parseHexStr(rawPayload)
      version = parseUInt(rawVersion)
      timestamp = parseInt(rawTimestamp)
      meta = parseHexStr(rawMeta)
    except ValueError:
      error "could not parse correctly", error = getCurrentExceptionMsg()

    msgHash = fromBytes(hashHex.toOpenArrayByte(0, 31))

    wakuMessage.contentTopic = contentTopic
    wakuMessage.payload = @(payload.toOpenArrayByte(0, payload.high))
    wakuMessage.version = uint32(version)
    wakuMessage.timestamp = timestamp
    wakuMessage.meta = @(meta.toOpenArrayByte(0, meta.high))

    outRows.add((msgHash, pubSubTopic, wakuMessage))

method put*(
    s: PostgresDriver,
    messageHash: WakuMessageHash,
    pubsubTopic: PubsubTopic,
    message: WakuMessage,
): Future[ArchiveDriverResult[void]] {.async.} =
  let messageHash = toHex(messageHash)

  let contentTopic = message.contentTopic
  let payload = toHex(message.payload)
  let version = $message.version
  let timestamp = $message.timestamp
  let meta = toHex(message.meta)

  trace "put PostgresDriver",
    messageHash, contentTopic, payload, version, timestamp, meta

  ## this is not needed for store-v3. Nevertheless, we will keep that temporarily
  ## until we completely remove the store/archive-v2 logic
  let fakeId = "0"

  (
    ## Add the row to the messages table
    await s.writeConnPool.runStmt(
      InsertRowStmtName,
      InsertRowStmtDefinition,
      @[
        fakeId, messageHash, pubsubTopic, contentTopic, payload, version, timestamp,
        meta,
      ],
      @[
        int32(fakeId.len),
        int32(messageHash.len),
        int32(pubsubTopic.len),
        int32(contentTopic.len),
        int32(payload.len),
        int32(version.len),
        int32(timestamp.len),
        int32(meta.len),
      ],
      @[int32(0), int32(0), int32(0), int32(0), int32(0), int32(0), int32(0), int32(0)],
    )
  ).isOkOr:
    return err("could not put msg in messages table: " & $error)

  ## Now add the row to messages_lookup
  return await s.writeConnPool.runStmt(
    InsertRowInMessagesLookupStmtName,
    InsertRowInMessagesLookupStmtDefinition,
    @[messageHash, timestamp],
    @[int32(messageHash.len), int32(timestamp.len)],
    @[int32(0), int32(0)],
  )

method getAllMessages*(
    s: PostgresDriver
): Future[ArchiveDriverResult[seq[ArchiveRow]]] {.async.} =
  ## Retrieve all messages from the store.
  var rows: seq[(WakuMessageHash, PubsubTopic, WakuMessage)]
  proc rowCallback(pqResult: ptr PGresult) =
    rowCallbackImpl(pqResult, rows)

  (
    await s.readConnPool.pgQuery(
      """SELECT messageHash, pubsubTopic, contentTopic, payload, version, timestamp, meta
        FROM messages
        ORDER BY timestamp ASC, messageHash ASC""",
      newSeq[string](0),
      rowCallback,
    )
  ).isOkOr:
    return err("failed in query: " & $error)

  return ok(rows)

proc getPartitionsList(
    s: PostgresDriver
): Future[ArchiveDriverResult[seq[string]]] {.async.} =
  ## Retrieves the seq of partition table names.
  ## e.g: @["messages_1708534333_1708534393", "messages_1708534273_1708534333"]
  var partitions: seq[string]
  proc rowCallback(pqResult: ptr PGresult) =
    for iRow in 0 ..< pqResult.pqNtuples():
      let partitionName = $(pqgetvalue(pqResult, iRow, 0))
      partitions.add(partitionName)

  (
    await s.readConnPool.pgQuery(
      """
                      SELECT child.relname       AS partition_name
                          FROM pg_inherits
                          JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
                          JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
                          JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
                          JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
                          WHERE parent.relname='messages'
                          ORDER BY partition_name ASC
                          """,
      newSeq[string](0),
      rowCallback,
    )
  ).isOkOr:
    return err("getPartitionsList failed in query: " & $error)

  return ok(partitions)

proc getTimeCursor(
    s: PostgresDriver, hashHex: string
): Future[ArchiveDriverResult[Option[Timestamp]]] {.async.} =
  var timeCursor: Option[Timestamp]

  proc cursorCallback(pqResult: ptr PGresult) =
    timeCursorCallbackImpl(pqResult, timeCursor)

  ?await s.readConnPool.runStmt(
    SelectCursorByHashName,
    SelectCursorByHashDef,
    @[hashHex],
    @[int32(hashHex.len)],
    @[int32(0)],
    cursorCallback,
  )

  return ok(timeCursor)

proc getMessagesArbitraryQuery(
    s: PostgresDriver,
    contentTopics: seq[ContentTopic] = @[],
    pubsubTopic = none(PubsubTopic),
    cursor = none(ArchiveCursor),
    startTime = none(Timestamp),
    endTime = none(Timestamp),
    hexHashes: seq[string] = @[],
    maxPageSize = DefaultPageSize,
    ascendingOrder = true,
    requestId: string,
): Future[ArchiveDriverResult[seq[ArchiveRow]]] {.async.} =
  ## This proc allows to handle atypical queries. We don't use prepared statements for those.

  var query = SelectClause
  var statements: seq[string]
  var args: seq[string]

  if cursor.isSome():
    let hashHex = toHex(cursor.get())

    let timeCursor = ?await s.getTimeCursor(hashHex)

    if timeCursor.isNone():
      return err("cursor not found")

    let comp = if ascendingOrder: ">" else: "<"
    statements.add("(timestamp, messageHash) " & comp & " (?,?)")

    args.add($timeCursor.get())
    args.add(hashHex)

  if contentTopics.len > 0:
    let cstmt = "contentTopic IN (" & "?".repeat(contentTopics.len).join(",") & ")"
    statements.add(cstmt)
    for t in contentTopics:
      args.add(t)

  if hexHashes.len > 0:
    let cstmt = "messageHash IN (" & "?".repeat(hexHashes.len).join(",") & ")"
    statements.add(cstmt)
    for t in hexHashes:
      args.add(t)

  if pubsubTopic.isSome():
    statements.add("pubsubTopic = ?")
    args.add(pubsubTopic.get())

  if startTime.isSome():
    statements.add("timestamp >= ?")
    args.add($startTime.get())

  if endTime.isSome():
    statements.add("timestamp <= ?")
    args.add($endTime.get())

  if statements.len > 0:
    query &= " WHERE " & statements.join(" AND ")

  var direction: string
  if ascendingOrder:
    direction = "ASC"
  else:
    direction = "DESC"

  query &= " ORDER BY timestamp " & direction & ", messageHash " & direction

  query &= " LIMIT ?"
  args.add($maxPageSize)

  var rows: seq[(WakuMessageHash, PubsubTopic, WakuMessage)]
  proc rowCallback(pqResult: ptr PGresult) =
    rowCallbackImpl(pqResult, rows)

  (await s.readConnPool.pgQuery(query, args, rowCallback, requestId)).isOkOr:
    return err("failed to run query: " & $error)

  return ok(rows)

proc getMessageHashesArbitraryQuery(
    s: PostgresDriver,
    contentTopics: seq[ContentTopic] = @[],
    pubsubTopic = none(PubsubTopic),
    cursor = none(ArchiveCursor),
    startTime = none(Timestamp),
    endTime = none(Timestamp),
    hexHashes: seq[string] = @[],
    maxPageSize = DefaultPageSize,
    ascendingOrder = true,
    requestId: string,
): Future[ArchiveDriverResult[seq[(WakuMessageHash, PubsubTopic, WakuMessage)]]] {.
    async
.} =
  ## This proc allows to handle atypical queries. We don't use prepared statements for those.
  var query = """SELECT messageHash FROM messages"""

  var statements: seq[string]
  var args: seq[string]

  if cursor.isSome():
    let hashHex = toHex(cursor.get())

    let timeCursor = ?await s.getTimeCursor(hashHex)

    if timeCursor.isNone():
      return err("cursor not found")

    let comp = if ascendingOrder: ">" else: "<"
    statements.add("(timestamp, messageHash) " & comp & " (?,?)")

    args.add($timeCursor.get())
    args.add(hashHex)

  if contentTopics.len > 0:
    let cstmt = "contentTopic IN (" & "?".repeat(contentTopics.len).join(",") & ")"
    statements.add(cstmt)
    for t in contentTopics:
      args.add(t)

  if hexHashes.len > 0:
    let cstmt = "messageHash IN (" & "?".repeat(hexHashes.len).join(",") & ")"
    statements.add(cstmt)
    for t in hexHashes:
      args.add(t)

  if pubsubTopic.isSome():
    statements.add("pubsubTopic = ?")
    args.add(pubsubTopic.get())

  if startTime.isSome():
    statements.add("timestamp >= ?")
    args.add($startTime.get())

  if endTime.isSome():
    statements.add("timestamp <= ?")
    args.add($endTime.get())

  if statements.len > 0:
    query &= " WHERE " & statements.join(" AND ")

  var direction: string
  if ascendingOrder:
    direction = "ASC"
  else:
    direction = "DESC"

  query &= " ORDER BY timestamp " & direction & ", messageHash " & direction

  query &= " LIMIT ?"
  args.add($maxPageSize)

  var rows: seq[(WakuMessageHash, PubsubTopic, WakuMessage)]
  proc rowCallback(pqResult: ptr PGresult) =
    hashCallbackImpl(pqResult, rows)

  (await s.readConnPool.pgQuery(query, args, rowCallback, requestId)).isOkOr:
    return err("failed to run query: " & $error)

  return ok(rows)

proc getMessagesPreparedStmt(
    s: PostgresDriver,
    contentTopic: string,
    pubsubTopic: PubsubTopic,
    cursor = none(ArchiveCursor),
    startTime: Timestamp,
    endTime: Timestamp,
    hashes: string,
    maxPageSize = DefaultPageSize,
    ascOrder = true,
    requestId: string,
): Future[ArchiveDriverResult[seq[ArchiveRow]]] {.async.} =
  ## This proc aims to run the most typical queries in a more performant way,
  ## i.e. by means of prepared statements.

  var rows: seq[(WakuMessageHash, PubsubTopic, WakuMessage)]

  proc rowCallback(pqResult: ptr PGresult) =
    rowCallbackImpl(pqResult, rows)

  let startTimeStr = $startTime
  let endTimeStr = $endTime
  let limit = $maxPageSize

  if cursor.isNone():
    var stmtName =
      if ascOrder: SelectNoCursorAscStmtName else: SelectNoCursorDescStmtName
    var stmtDef = if ascOrder: SelectNoCursorAscStmtDef else: SelectNoCursorDescStmtDef

    (
      await s.readConnPool.runStmt(
        stmtName,
        stmtDef,
        @[contentTopic, hashes, pubsubTopic, startTimeStr, endTimeStr, limit],
        @[
          int32(contentTopic.len),
          int32(pubsubTopic.len),
          int32(startTimeStr.len),
          int32(endTimeStr.len),
          int32(limit.len),
        ],
        @[int32(0), int32(0), int32(0), int32(0), int32(0)],
        rowCallback,
        requestId,
      )
    ).isOkOr:
      return err(stmtName & ": " & $error)

    return ok(rows)

  let hashHex = toHex(cursor.get())

  let timeCursor = ?await s.getTimeCursor(hashHex)

  if timeCursor.isNone():
    return err("cursor not found")

  let timeString = $timeCursor.get()

  var stmtName =
    if ascOrder: SelectWithCursorAscStmtName else: SelectWithCursorDescStmtName
  var stmtDef =
    if ascOrder: SelectWithCursorAscStmtDef else: SelectWithCursorDescStmtDef

  (
    await s.readConnPool.runStmt(
      stmtName,
      stmtDef,
      @[
        contentTopic, hashes, pubsubTopic, hashHex, timeString, startTimeStr,
        endTimeStr, limit,
      ],
      @[
        int32(contentTopic.len),
        int32(hashes.len),
        int32(pubsubTopic.len),
        int32(timeString.len),
        int32(hashHex.len),
        int32(startTimeStr.len),
        int32(endTimeStr.len),
        int32(limit.len),
      ],
      @[int32(0), int32(0), int32(0), int32(0), int32(0), int32(0), int32(0), int32(0)],
      rowCallback,
      requestId,
    )
  ).isOkOr:
    return err(stmtName & ": " & $error)

  return ok(rows)

proc getMessageHashesPreparedStmt(
    s: PostgresDriver,
    contentTopic: string,
    pubsubTopic: PubsubTopic,
    cursor = none(ArchiveCursor),
    startTime: Timestamp,
    endTime: Timestamp,
    hashes: string,
    maxPageSize = DefaultPageSize,
    ascOrder = true,
    requestId: string,
): Future[ArchiveDriverResult[seq[ArchiveRow]]] {.async.} =
  ## This proc aims to run the most typical queries in a more performant way,
  ## i.e. by means of prepared statements.

  var rows: seq[(WakuMessageHash, PubsubTopic, WakuMessage)]

  proc rowCallback(pqResult: ptr PGresult) =
    hashCallbackImpl(pqResult, rows)

  let startTimeStr = $startTime
  let endTimeStr = $endTime
  let limit = $maxPageSize

  if cursor.isNone():
    var stmtName =
      if ascOrder: SelectNoCursorNoDataAscStmtName else: SelectNoCursorNoDataDescStmtName
    var stmtDef =
      if ascOrder: SelectNoCursorNoDataAscStmtDef else: SelectNoCursorNoDataDescStmtDef

    (
      await s.readConnPool.runStmt(
        stmtName,
        stmtDef,
        @[contentTopic, hashes, pubsubTopic, startTimeStr, endTimeStr, limit],
        @[
          int32(contentTopic.len),
          int32(hashes.len),
          int32(pubsubTopic.len),
          int32(startTimeStr.len),
          int32(endTimeStr.len),
          int32(limit.len),
        ],
        @[int32(0), int32(0), int32(0), int32(0), int32(0), int32(0)],
        rowCallback,
        requestId,
      )
    ).isOkOr:
      return err(stmtName & ": " & $error)

    return ok(rows)

  let hashHex = toHex(cursor.get())

  let timeCursor = ?await s.getTimeCursor(hashHex)

  if timeCursor.isNone():
    return err("cursor not found")

  let timeString = $timeCursor.get()

  var stmtName =
    if ascOrder:
      SelectWithCursorNoDataAscStmtName
    else:
      SelectWithCursorNoDataDescStmtName
  var stmtDef =
    if ascOrder: SelectWithCursorNoDataAscStmtDef else: SelectWithCursorNoDataDescStmtDef

  (
    await s.readConnPool.runStmt(
      stmtName,
      stmtDef,
      @[
        contentTopic, hashes, pubsubTopic, hashHex, timeString, startTimeStr,
        endTimeStr, limit,
      ],
      @[
        int32(contentTopic.len),
        int32(hashes.len),
        int32(pubsubTopic.len),
        int32(timeString.len),
        int32(hashHex.len),
        int32(startTimeStr.len),
        int32(endTimeStr.len),
        int32(limit.len),
      ],
      @[int32(0), int32(0), int32(0), int32(0), int32(0), int32(0), int32(0), int32(0)],
      rowCallback,
      requestId,
    )
  ).isOkOr:
    return err(stmtName & ": " & $error)

  return ok(rows)

proc getMessagesByMessageHashes(
    s: PostgresDriver, hashes: string, maxPageSize: uint, requestId: string
): Future[ArchiveDriverResult[seq[ArchiveRow]]] {.async.} =
  ## Retrieves information only filtering by a given messageHashes list.
  ## This proc levarages on the messages_lookup table to have better query performance
  ## and only query the desired partitions in the partitioned messages table
  var query =
    fmt"""
  WITH min_timestamp AS (
    SELECT MIN(timestamp) AS min_ts
    FROM messages_lookup
    WHERE messagehash IN (
      {hashes}
    )
  )
  SELECT m.messageHash, pubsubTopic, contentTopic, payload, version, m.timestamp, meta
  FROM messages m
  INNER JOIN
    messages_lookup l
  ON
    m.timestamp = l.timestamp
    AND m.messagehash = l.messagehash
  WHERE
    l.timestamp >= (SELECT min_ts FROM min_timestamp)
    AND l.messagehash IN (
      {hashes}
    )
  ORDER BY
    m.timestamp DESC,
    m.messagehash DESC
  LIMIT {maxPageSize};
  """

  var rows: seq[(WakuMessageHash, PubsubTopic, WakuMessage)]
  proc rowCallback(pqResult: ptr PGresult) =
    rowCallbackImpl(pqResult, rows)

  (
    await s.readConnPool.pgQuery(
      query = query, rowCallback = rowCallback, requestId = requestId
    )
  ).isOkOr:
    return err("failed to run query: " & $error)

  return ok(rows)

proc getMessagesWithinLimits(
    self: PostgresDriver,
    includeData: bool,
    contentTopics: seq[ContentTopic],
    pubsubTopic: Option[PubsubTopic],
    cursor: Option[ArchiveCursor],
    startTime: Option[Timestamp],
    endTime: Option[Timestamp],
    hashes: seq[WakuMessageHash],
    maxPageSize: uint,
    ascendingOrder: bool,
    requestId: string,
): Future[ArchiveDriverResult[seq[ArchiveRow]]] {.async.} =
  if hashes.len > MaxHashesPerQuery:
    return err(fmt"can not attend queries with more than {MaxHashesPerQuery} hashes")

  let hexHashes = hashes.mapIt(toHex(it))

  if cursor.isNone() and pubsubTopic.isNone() and contentTopics.len == 0 and
      startTime.isNone() and endTime.isNone() and hexHashes.len > 0:
    return await self.getMessagesByMessageHashes(
      "'" & hexHashes.join("','") & "'", maxPageSize, requestId
    )

  if contentTopics.len > 0 and hexHashes.len > 0 and pubsubTopic.isSome() and
      startTime.isSome() and endTime.isSome():
    ## Considered the most common query. Therefore, we use prepared statements to optimize it.
    if includeData:
      return await self.getMessagesPreparedStmt(
        contentTopics.join(","),
        PubsubTopic(pubsubTopic.get()),
        cursor,
        startTime.get(),
        endTime.get(),
        hexHashes.join(","),
        maxPageSize,
        ascendingOrder,
        requestId,
      )
    else:
      return await self.getMessageHashesPreparedStmt(
        contentTopics.join(","),
        PubsubTopic(pubsubTopic.get()),
        cursor,
        startTime.get(),
        endTime.get(),
        hexHashes.join(","),
        maxPageSize,
        ascendingOrder,
        requestId,
      )
  else:
    if includeData:
      ## We will run atypical query. In this case we don't use prepared statemets
      return await self.getMessagesArbitraryQuery(
        contentTopics, pubsubTopic, cursor, startTime, endTime, hexHashes, maxPageSize,
        ascendingOrder, requestId,
      )
    else:
      return await self.getMessageHashesArbitraryQuery(
        contentTopics, pubsubTopic, cursor, startTime, endTime, hexHashes, maxPageSize,
        ascendingOrder, requestId,
      )

method getMessages*(
    s: PostgresDriver,
    includeData = true,
    contentTopics = newSeq[ContentTopic](0),
    pubsubTopic = none(PubsubTopic),
    cursor = none(ArchiveCursor),
    startTime = none(Timestamp),
    endTime = none(Timestamp),
    hashes = newSeq[WakuMessageHash](0),
    maxPageSize = DefaultPageSize,
    ascendingOrder = true,
    requestId = "",
): Future[ArchiveDriverResult[seq[ArchiveRow]]] {.async.} =
  let rows = collect(newSeq):
    for i in countup(0, hashes.len, MaxHashesPerQuery):
      let stop = min(i + MaxHashesPerQuery, hashes.len)

      let splittedHashes = hashes[i ..< stop]

      let subRows =
        ?await s.getMessagesWithinLimits(
          includeData, contentTopics, pubsubTopic, cursor, startTime, endTime,
          splittedHashes, maxPageSize, ascendingOrder, requestId,
        )

      for row in subRows:
        row

  return ok(rows)

proc getStr(
    s: PostgresDriver, query: string
): Future[ArchiveDriverResult[string]] {.async.} =
  # Performs a query that is expected to return a single string

  var ret: string
  proc rowCallback(pqResult: ptr PGresult) =
    if pqResult.pqnfields() != 1:
      error "Wrong number of fields in getStr"
      return

    if pqResult.pqNtuples() != 1:
      error "Wrong number of rows in getStr"
      return

    ret = $(pqgetvalue(pqResult, 0, 0))

  (await s.readConnPool.pgQuery(query, newSeq[string](0), rowCallback)).isOkOr:
    return err("failed in getRow: " & $error)

  return ok(ret)

proc getInt(
    s: PostgresDriver, query: string
): Future[ArchiveDriverResult[int64]] {.async.} =
  # Performs a query that is expected to return a single numeric value (int64)

  var retInt = 0'i64
  let str = (await s.getStr(query)).valueOr:
    return err("could not get str in getInt: " & $error)

  try:
    retInt = parseInt(str)
  except ValueError:
    return err(
      "exception in getInt, parseInt, str: " & str & " query: " & query & " exception: " &
        getCurrentExceptionMsg()
    )

  return ok(retInt)

method getDatabaseSize*(
    s: PostgresDriver
): Future[ArchiveDriverResult[int64]] {.async.} =
  let intRes = (await s.getInt("SELECT pg_database_size(current_database())")).valueOr:
    return err("error in getDatabaseSize: " & error)

  let databaseSize: int64 = int64(intRes)
  return ok(databaseSize)

method getMessagesCount*(
    s: PostgresDriver
): Future[ArchiveDriverResult[int64]] {.async.} =
  let intRes = await s.getInt("SELECT COUNT(1) FROM messages")
  if intRes.isErr():
    return err("error in getMessagesCount: " & intRes.error)

  return ok(intRes.get())

method getOldestMessageTimestamp*(
    s: PostgresDriver
): Future[ArchiveDriverResult[Timestamp]] {.async.} =
  ## In some cases it could happen that we have
  ## empty partitions which are older than the current stored rows.
  ## In those cases we want to consider those older partitions as the oldest considered timestamp.
  let oldestPartition = s.partitionMngr.getOldestPartition().valueOr:
    return err("could not get oldest partition: " & $error)

  let oldestPartitionTimeNanoSec = oldestPartition.getPartitionStartTimeInNanosec()

  let intRes = await s.getInt("SELECT MIN(timestamp) FROM messages")
  if intRes.isErr():
    ## Just return the oldest partition time considering the partitions set
    return ok(Timestamp(oldestPartitionTimeNanoSec))

  return ok(Timestamp(min(intRes.get(), oldestPartitionTimeNanoSec)))

method getNewestMessageTimestamp*(
    s: PostgresDriver
): Future[ArchiveDriverResult[Timestamp]] {.async.} =
  let intRes = await s.getInt("SELECT MAX(timestamp) FROM messages")

  if intRes.isErr():
    return err("error in getNewestMessageTimestamp: " & intRes.error)

  return ok(Timestamp(intRes.get()))

method deleteOldestMessagesNotWithinLimit*(
    s: PostgresDriver, limit: int
): Future[ArchiveDriverResult[void]] {.async.} =
  var execRes = await s.writeConnPool.pgQuery(
    """DELETE FROM messages WHERE messageHash NOT IN
                          (
                        SELECT messageHash FROM messages ORDER BY timestamp DESC LIMIT ?
                          );""",
    @[$limit],
  )
  if execRes.isErr():
    return err("error in deleteOldestMessagesNotWithinLimit: " & execRes.error)

  execRes = await s.writeConnPool.pgQuery(
    """DELETE FROM messages_lookup WHERE messageHash NOT IN
                          (
                        SELECT messageHash FROM messages ORDER BY timestamp DESC LIMIT ?
                          );""",
    @[$limit],
  )
  if execRes.isErr():
    return err(
      "error in deleteOldestMessagesNotWithinLimit messages_lookup: " & execRes.error
    )

  return ok()

method close*(s: PostgresDriver): Future[ArchiveDriverResult[void]] {.async.} =
  ## Cancel the partition factory loop
  s.futLoopPartitionFactory.cancelSoon()

  ## Cancel analyze table loop
  if not s.futLoopAnalyzeTable.isNil():
    s.futLoopAnalyzeTable.cancelSoon()

  ## Close the database connection
  let writeCloseRes = await s.writeConnPool.close()
  let readCloseRes = await s.readConnPool.close()

  writeCloseRes.isOkOr:
    return err("error closing write pool: " & $error)

  readCloseRes.isOkOr:
    return err("error closing read pool: " & $error)

  return ok()

proc sleep*(
    s: PostgresDriver, seconds: int
): Future[ArchiveDriverResult[void]] {.async.} =
  # This is for testing purposes only. It is aimed to test the proper
  # implementation of asynchronous requests. It merely triggers a sleep in the
  # database for the amount of seconds given as a parameter.

  proc rowCallback(result: ptr PGresult) =
    ## We are not interested in any value in this case
    discard

  try:
    let params = @[$seconds]
    (await s.writeConnPool.pgQuery("SELECT pg_sleep(?)", params, rowCallback)).isOkOr:
      return err("error in postgres_driver sleep: " & $error)
  except DbError:
    # This always raises an exception although the sleep works
    return err("exception sleeping: " & getCurrentExceptionMsg())

  return ok()

const EXPECTED_LOCK_ERROR* = "another waku instance is currently executing a migration"
proc acquireDatabaseLock*(
    s: PostgresDriver, lockId: int = 841886
): Future[ArchiveDriverResult[void]] {.async.} =
  ## Acquire an advisory lock (useful to avoid more than one application running migrations at the same time)
  ## This should only be used in the migrations module because this approach doesn't ensure
  ## that the lock is acquired/released by the same connection. The preferable "lock"
  ## approach is using the "performWriteQueryWithLock" proc. However, we can't use
  ## "performWriteQueryWithLock" in the migrations process because we can't nest two PL/SQL
  ## scripts.

  let locked = (
    await s.getStr(
      fmt"""
    SELECT pg_try_advisory_lock({lockId})
    """
    )
  ).valueOr:
    return err("error acquiring a lock: " & error)

  if locked == "f":
    return err(EXPECTED_LOCK_ERROR)

  return ok()

proc releaseDatabaseLock*(
    s: PostgresDriver, lockId: int = 841886
): Future[ArchiveDriverResult[void]] {.async.} =
  ## Release an advisory lock (useful to avoid more than one application running migrations at the same time)
  let unlocked = (
    await s.getStr(
      fmt"""
    SELECT pg_advisory_unlock({lockId})
    """
    )
  ).valueOr:
    return err("error releasing a lock: " & error)

  if unlocked == "f":
    return err("could not release advisory lock")

  return ok()

proc performWriteQuery*(
    s: PostgresDriver, query: string
): Future[ArchiveDriverResult[void]] {.async.} =
  ## Performs a query that somehow changes the state of the database

  (await s.writeConnPool.pgQuery(query)).isOkOr:
    return err("error in performWriteQuery: " & $error)

  return ok()

const COULD_NOT_ACQUIRE_ADVISORY_LOCK* = "could not acquire advisory lock"

proc performWriteQueryWithLock(
    self: PostgresDriver, queryToProtect: string
): Future[ArchiveDriverResult[void]] {.async.} =
  ## This wraps the original query in a script so that we make sure a pg_advisory lock protects it
  let query =
    fmt"""
          DO $$
          DECLARE
              lock_acquired boolean;
          BEGIN
              -- Try to acquire the advisory lock
              lock_acquired := pg_try_advisory_lock(123456789);

              IF NOT lock_acquired THEN
                  RAISE EXCEPTION '{COULD_NOT_ACQUIRE_ADVISORY_LOCK}';
              END IF;

              -- Perform the query
              BEGIN
                  {queryToProtect}
              EXCEPTION WHEN OTHERS THEN
                  -- Ensure the lock is released if an error occurs
                  PERFORM pg_advisory_unlock(123456789);
                  RAISE;
              END;

              -- Release the advisory lock after the query completes successfully
              PERFORM pg_advisory_unlock(123456789);
          END $$;
"""
  (await self.performWriteQuery(query)).isOkOr:
    if error.contains(COULD_NOT_ACQUIRE_ADVISORY_LOCK):
      ## We don't consider this as an error. Just someone else acquired the advisory lock
      debug "skip performWriteQuery because the advisory lock is acquired by other"
      return ok()

    if error.contains("already exists"):
      ## expected to happen when trying to add a partition table constraint that already exists
      ## e.g., constraint "constraint_name" for relation "messages_1720364735_1720364740" already exists
      debug "skip already exists error", error = error
      return ok()

    if error.contains("is already a partition"):
      ## expected to happen when a node tries to add a partition that is already attached,
      ## e.g., "messages_1720364735_1720364740" is already a partition
      debug "skip is already a partition error", error = error
      return ok()

    if error.contains("does not exist"):
      ## expected to happen when trying to drop a constraint that has already been dropped by other
      ## constraint "constraint_name" of relation "messages_1720364735_1720364740" does not exist
      debug "skip does not exist error", error = error
      return ok()

    debug "protected query ended with error", error = $error
    return err("protected query ended with error:" & $error)

  debug "protected query ended correctly"
  return ok()

proc addPartition(
    self: PostgresDriver, startTime: Timestamp
): Future[ArchiveDriverResult[void]] {.async.} =
  ## Creates a partition table that will store the messages that fall in the range
  ## `startTime` <= timestamp < `startTime + duration`.
  ## `startTime` is measured in seconds since epoch
  let beginning = startTime
  let `end` = partitions_manager.calcEndPartitionTime(startTime)

  let fromInSec: string = $beginning
  let untilInSec: string = $`end`

  let fromInNanoSec: string = fromInSec & "000000000"
  let untilInNanoSec: string = untilInSec & "000000000"

  let partitionName = "messages_" & fromInSec & "_" & untilInSec

  ## Create the partition table but not attach it yet to the main table
  let createPartitionQuery =
    "CREATE TABLE IF NOT EXISTS " & partitionName &
    " (LIKE messages INCLUDING DEFAULTS INCLUDING CONSTRAINTS);"

  (await self.performWriteQueryWithLock(createPartitionQuery)).isOkOr:
    return err(fmt"error adding partition [{partitionName}]: " & $error)

  ## Add constraint to the partition table so that EXCLUSIVE ACCESS is not performed when
  ## the partition is attached to the main table.
  let constraintName = partitionName & "_by_range_check"
  let addTimeConstraintQuery =
    "ALTER TABLE " & partitionName & " ADD CONSTRAINT " & constraintName &
    " CHECK ( timestamp >= " & fromInNanoSec & " AND timestamp < " & untilInNanoSec &
    " );"

  (await self.performWriteQueryWithLock(addTimeConstraintQuery)).isOkOr:
    return err(fmt"error creating constraint [{partitionName}]: " & $error)

  ## Attaching the new created table as a new partition. That does not require EXCLUSIVE ACCESS.
  let attachPartitionQuery =
    "ALTER TABLE messages ATTACH PARTITION " & partitionName & " FOR VALUES FROM (" &
    fromInNanoSec & ") TO (" & untilInNanoSec & ");"

  (await self.performWriteQueryWithLock(attachPartitionQuery)).isOkOr:
    return err(fmt"error attaching partition [{partitionName}]: " & $error)

  ## Dropping the check constraint as it was only necessary to prevent full scan,
  ## and EXCLUSIVE ACCESS, to the whole messages table, when the new partition was attached.
  let dropConstraint =
    "ALTER TABLE " & partitionName & " DROP CONSTRAINT " & constraintName & ";"

  (await self.performWriteQueryWithLock(dropConstraint)).isOkOr:
    return err(fmt"error dropping constraint [{partitionName}]: " & $error)

  debug "new partition added", query = createPartitionQuery

  self.partitionMngr.addPartitionInfo(partitionName, beginning, `end`)
  return ok()

proc refreshPartitionsInfo(
    self: PostgresDriver
): Future[ArchiveDriverResult[void]] {.async.} =
  debug "refreshPartitionsInfo"
  self.partitionMngr.clearPartitionInfo()

  let partitionNamesRes = await self.getPartitionsList()
  if not partitionNamesRes.isOk():
    return err("Could not retrieve partitions list: " & $partitionNamesRes.error)
  else:
    let partitionNames = partitionNamesRes.get()
    for partitionName in partitionNames:
      ## partitionName contains something like 'messages_1708449815_1708449875'
      let bothTimes = partitionName.replace("messages_", "")
      let times = bothTimes.split("_")
      if times.len != 2:
        return err(fmt"loopPartitionFactory wrong partition name {partitionName}")

      var beginning: int64
      try:
        beginning = parseInt(times[0])
      except ValueError:
        return err("Could not parse beginning time: " & getCurrentExceptionMsg())

      var `end`: int64
      try:
        `end` = parseInt(times[1])
      except ValueError:
        return err("Could not parse end time: " & getCurrentExceptionMsg())

      self.partitionMngr.addPartitionInfo(partitionName, beginning, `end`)

    return ok()

const DefaultDatabasePartitionCheckTimeInterval = timer.minutes(10)

proc loopPartitionFactory(
    self: PostgresDriver, onFatalError: OnFatalErrorHandler
) {.async.} =
  ## Loop proc that continuously checks whether we need to create a new partition.
  ## Notice that the deletion of partitions is handled by the retention policy modules.

  debug "starting loopPartitionFactory"

  while true:
    trace "Check if a new partition is needed"

    ## Let's make the 'partition_manager' aware of the current partitions
    (await self.refreshPartitionsInfo()).isOkOr:
      onFatalError("issue in loopPartitionFactory: " & $error)

    let now = times.now().toTime().toUnix()

    if self.partitionMngr.isEmpty():
      debug "adding partition because now there aren't more partitions"
      (await self.addPartition(now)).isOkOr:
        onFatalError("error when creating a new partition from empty state: " & $error)
    else:
      let newestPartitionRes = self.partitionMngr.getNewestPartition()
      if newestPartitionRes.isErr():
        onFatalError("could not get newest partition: " & $newestPartitionRes.error)

      let newestPartition = newestPartitionRes.get()
      if newestPartition.containsMoment(now):
        debug "creating a new partition for the future"
        ## The current used partition is the last one that was created.
        ## Thus, let's create another partition for the future.

        (await self.addPartition(newestPartition.getLastMoment())).isOkOr:
          onFatalError("could not add the next partition for 'now': " & $error)
      elif now >= newestPartition.getLastMoment():
        debug "creating a new partition to contain current messages"
        ## There is no partition to contain the current time.
        ## This happens if the node has been stopped for quite a long time.
        ## Then, let's create the needed partition to contain 'now'.
        (await self.addPartition(now)).isOkOr:
          onFatalError("could not add the next partition: " & $error)

    await sleepAsync(DefaultDatabasePartitionCheckTimeInterval)

proc startPartitionFactory*(
    self: PostgresDriver, onFatalError: OnFatalErrorHandler
) {.async.} =
  self.futLoopPartitionFactory = self.loopPartitionFactory(onFatalError)

proc getTableSize*(
    self: PostgresDriver, tableName: string
): Future[ArchiveDriverResult[string]] {.async.} =
  ## Returns a human-readable representation of the size for the requested table.
  ## tableName - table of interest.
  let tableSize = (
    await self.getStr(
      fmt"""
                SELECT pg_size_pretty(pg_total_relation_size(C.oid)) AS "total_size"
                FROM pg_class C
                where relname = '{tableName}'"""
    )
  ).valueOr:
    return err("error in getDatabaseSize: " & error)

  return ok(tableSize)

proc removePartition(
    self: PostgresDriver, partition: Partition
): Future[ArchiveDriverResult[void]] {.async.} =
  ## Removes the desired partition and also removes the rows from messages_lookup table
  ## whose rows belong to the partition time range

  let partitionName = partition.getName()
  debug "beginning of removePartition", partitionName

  var partSize = ""
  let partSizeRes = await self.getTableSize(partitionName)
  if partSizeRes.isOk():
    partSize = partSizeRes.get()

  ## Detach and remove the partition concurrently to not block the parent table (messages)
  let detachPartitionQuery =
    "ALTER TABLE messages DETACH PARTITION " & partitionName & " CONCURRENTLY;"
  debug "removeOldestPartition", query = detachPartitionQuery
  (await self.performWriteQuery(detachPartitionQuery)).isOkOr:
    if ($error).contains("FINALIZE"):
      ## We assume the database is suggesting to use FINALIZE when detaching a partition
      let detachPartitionFinalizeQuery =
        "ALTER TABLE messages DETACH PARTITION " & partitionName & " FINALIZE;"
      debug "removeOldestPartition detaching with FINALIZE",
        query = detachPartitionFinalizeQuery
      (await self.performWriteQuery(detachPartitionFinalizeQuery)).isOkOr:
        return err(fmt"error in FINALIZE {detachPartitionFinalizeQuery}: " & $error)
    else:
      return err(fmt"error in {detachPartitionQuery}: " & $error)

  ## Drop the partition
  let dropPartitionQuery = "DROP TABLE " & partitionName
  debug "removeOldestPartition drop partition", query = dropPartitionQuery
  (await self.performWriteQuery(dropPartitionQuery)).isOkOr:
    return err(fmt"error in {dropPartitionQuery}: " & $error)

  debug "removed partition", partition_name = partitionName, partition_size = partSize
  self.partitionMngr.removeOldestPartitionName()

  ## Now delete rows from the messages_lookup table
  let timeRange = partition.getTimeRange()
  let `end` = timeRange.`end` * 1_000_000_000
  let deleteRowsQuery = "DELETE FROM messages_lookup WHERE timestamp < " & $`end`
  (await self.performWriteQuery(deleteRowsQuery)).isOkOr:
    return err(fmt"error in {deleteRowsQuery}: " & $error)

  return ok()

proc removePartitionsOlderThan(
    self: PostgresDriver, tsInNanoSec: Timestamp
): Future[ArchiveDriverResult[void]] {.async.} =
  ## Removes old partitions that don't contain the specified timestamp

  let tsInSec = Timestamp(float(tsInNanoSec) / 1_000_000_000)
  debug "beginning of removePartitionsOlderThan", tsInSec

  var oldestPartition = self.partitionMngr.getOldestPartition().valueOr:
    return err("could not get oldest partition in removePartitionOlderThan: " & $error)

  while not oldestPartition.containsMoment(tsInSec):
    (await self.removePartition(oldestPartition)).isOkOr:
      return err("issue in removePartitionsOlderThan: " & $error)

    oldestPartition = self.partitionMngr.getOldestPartition().valueOr:
      return err(
        "could not get partition in removePartitionOlderThan in while loop: " & $error
      )

  ## We reached the partition that contains the target timestamp plus don't want to remove it
  return ok()

proc removeOldestPartition(
    self: PostgresDriver, forceRemoval: bool = false, ## To allow cleanup in tests
): Future[ArchiveDriverResult[void]] {.async.} =
  ## Indirectly called from the retention policy
  let oldestPartition = self.partitionMngr.getOldestPartition().valueOr:
    return err("could not remove oldest partition: " & $error)

  if not forceRemoval:
    let now = times.now().toTime().toUnix()
    let currentPartitionRes = self.partitionMngr.getPartitionFromDateTime(now)
    if currentPartitionRes.isOk():
      ## The database contains a partition that would store current messages.

      if currentPartitionRes.get() == oldestPartition:
        debug "Skipping to remove the current partition"
        return ok()

  return await self.removePartition(oldestPartition)

proc containsAnyPartition*(self: PostgresDriver): bool =
  return not self.partitionMngr.isEmpty()

method decreaseDatabaseSize*(
    driver: PostgresDriver, targetSizeInBytes: int64, forceRemoval: bool = false
): Future[ArchiveDriverResult[void]] {.async.} =
  var dbSize = (await driver.getDatabaseSize()).valueOr:
    return err("decreaseDatabaseSize failed to get database size: " & $error)

  ## database size in bytes
  var totalSizeOfDB: int64 = int64(dbSize)

  if totalSizeOfDB <= targetSizeInBytes:
    return ok()

  debug "start reducing database size",
    targetSize = $targetSizeInBytes, currentSize = $totalSizeOfDB

  while totalSizeOfDB > targetSizeInBytes and driver.containsAnyPartition():
    (await driver.removeOldestPartition(forceRemoval)).isOkOr:
      return err(
        "decreaseDatabaseSize inside loop failed to remove oldest partition: " & $error
      )

    dbSize = (await driver.getDatabaseSize()).valueOr:
      return
        err("decreaseDatabaseSize inside loop failed to get database size: " & $error)

    let newCurrentSize = int64(dbSize)
    if newCurrentSize == totalSizeOfDB:
      return err("the previous partition removal didn't clear database size")

    totalSizeOfDB = newCurrentSize

    debug "reducing database size",
      targetSize = $targetSizeInBytes, newCurrentSize = $totalSizeOfDB

  return ok()

method existsTable*(
    s: PostgresDriver, tableName: string
): Future[ArchiveDriverResult[bool]] {.async.} =
  let query: string =
    fmt"""
  SELECT EXISTS (
    SELECT FROM
        pg_tables
    WHERE
        tablename  = '{tableName}'
    );
    """

  var exists: string
  proc rowCallback(pqResult: ptr PGresult) =
    if pqResult.pqnfields() != 1:
      error "Wrong number of fields in existsTable"
      return

    if pqResult.pqNtuples() != 1:
      error "Wrong number of rows in existsTable"
      return

    exists = $(pqgetvalue(pqResult, 0, 0))

  (await s.readConnPool.pgQuery(query, newSeq[string](0), rowCallback)).isOkOr:
    return err("existsTable failed in getRow: " & $error)

  return ok(exists == "t")

proc getCurrentVersion*(
    s: PostgresDriver
): Future[ArchiveDriverResult[int64]] {.async.} =
  let existsVersionTable = (await s.existsTable("version")).valueOr:
    return err("error in getCurrentVersion-existsTable: " & $error)

  if not existsVersionTable:
    return ok(0)

  let res = (await s.getInt(fmt"SELECT version FROM version")).valueOr:
    return err("error in getMessagesCount: " & $error)

  return ok(res)

method deleteMessagesOlderThanTimestamp*(
    s: PostgresDriver, tsNanoSec: Timestamp
): Future[ArchiveDriverResult[void]] {.async.} =
  ## First of all, let's remove the older partitions so that we can reduce
  ## the database size.
  (await s.removePartitionsOlderThan(tsNanoSec)).isOkOr:
    return err("error while removing older partitions: " & $error)

  (
    await s.writeConnPool.pgQuery(
      "DELETE FROM messages WHERE timestamp < " & $tsNanoSec
    )
  ).isOkOr:
    return err("error in deleteMessagesOlderThanTimestamp: " & $error)

  return ok()

############################################
## TODO: start splitting code better

const AnalyzeQuery = "ANALYZE messages"
const AnalyzeTableLockId = 111111 ## An arbitrary and different lock id
const RunAnalyzeInterval = timer.days(1)

proc analyzeTableLoop(self: PostgresDriver) {.async.} =
  ## The database stats should be calculated regularly so that the planner
  ## picks up the proper indexes and we have better query performance.
  while true:
    debug "analyzeTableLoop lock db"
    (await self.acquireDatabaseLock(AnalyzeTableLockId)).isOkOr:
      if error != EXPECTED_LOCK_ERROR:
        error "failed to acquire lock in analyzeTableLoop", error = error
      await sleepAsync(RunAnalyzeInterval)
      continue

    debug "analyzeTableLoop start analysis"
    (await self.performWriteQuery(AnalyzeQuery)).isOkOr:
      error "failed to run ANALYZE messages", error = error

    debug "analyzeTableLoop unlock db"
    (await self.releaseDatabaseLock(AnalyzeTableLockId)).isOkOr:
      error "failed to release lock analyzeTableLoop", error = error

    debug "analyzeTableLoop analysis completed"

    await sleepAsync(RunAnalyzeInterval)

proc startAnalyzeTableLoop*(self: PostgresDriver) =
  self.futLoopAnalyzeTable = self.analyzeTableLoop
