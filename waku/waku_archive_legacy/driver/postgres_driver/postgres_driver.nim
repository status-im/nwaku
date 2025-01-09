when (NimMajor, NimMinor) < (1, 4):
  {.push raises: [Defect].}
else:
  {.push raises: [].}

import
  std/[options, sequtils, strutils, strformat, times],
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
  ./postgres_healthcheck,
  ../../../common/databases/db_postgres as waku_postgres

type PostgresDriver* = ref object of ArchiveDriver
  ## Establish a separate pools for read/write operations
  writeConnPool: PgAsyncPool
  readConnPool: PgAsyncPool

const InsertRowStmtName = "InsertRow"
const InsertRowStmtDefinition = # TODO: get the sql queries from a file
  """INSERT INTO messages (id, messageHash, contentTopic, payload, pubsubTopic,
  version, timestamp, meta) VALUES ($1, $2, $3, $4, $5, $6, $7, CASE WHEN $8 = '' THEN NULL ELSE $8 END) ON CONFLICT DO NOTHING;"""

const InsertRowInMessagesLookupStmtName = "InsertRowMessagesLookup"
const InsertRowInMessagesLookupStmtDefinition =
  """INSERT INTO messages_lookup (messageHash, timestamp) VALUES ($1, $2) ON CONFLICT DO NOTHING;"""

const SelectNoCursorAscStmtName = "SelectWithoutCursorAsc"
const SelectNoCursorAscStmtDef =
  """SELECT contentTopic, payload, pubsubTopic, version, timestamp, id, messageHash, meta FROM messages
    WHERE contentTopic IN ($1) AND
          messageHash IN ($2) AND
          pubsubTopic = $3 AND
          timestamp >= $4 AND
          timestamp <= $5
    ORDER BY timestamp ASC, messageHash ASC LIMIT $6;"""

const SelectNoCursorDescStmtName = "SelectWithoutCursorDesc"
const SelectNoCursorDescStmtDef =
  """SELECT contentTopic, payload, pubsubTopic, version, timestamp, id, messageHash, meta FROM messages
    WHERE contentTopic IN ($1) AND
          messageHash IN ($2) AND
          pubsubTopic = $3 AND
          timestamp >= $4 AND
          timestamp <= $5
    ORDER BY timestamp DESC, messageHash DESC LIMIT $6;"""

const SelectWithCursorDescStmtName = "SelectWithCursorDesc"
const SelectWithCursorDescStmtDef =
  """SELECT contentTopic, payload, pubsubTopic, version, timestamp, id, messageHash, meta FROM messages
    WHERE contentTopic IN ($1) AND
          messageHash IN ($2) AND
          pubsubTopic = $3 AND
          (timestamp, messageHash) < ($4,$5) AND
          timestamp >= $6 AND
          timestamp <= $7
    ORDER BY timestamp DESC, messageHash DESC LIMIT $8;"""

const SelectWithCursorAscStmtName = "SelectWithCursorAsc"
const SelectWithCursorAscStmtDef =
  """SELECT contentTopic, payload, pubsubTopic, version, timestamp, id, messageHash, meta FROM messages
    WHERE contentTopic IN ($1) AND
          messageHash IN ($2) AND
          pubsubTopic = $3 AND
          (timestamp, messageHash) > ($4,$5) AND
          timestamp >= $6 AND
          timestamp <= $7
    ORDER BY timestamp ASC, messageHash ASC LIMIT $8;"""

const SelectMessageByHashName = "SelectMessageByHash"
const SelectMessageByHashDef =
  """SELECT contentTopic, payload, pubsubTopic, version, timestamp, id, messageHash, meta FROM messages WHERE messageHash = $1"""

const SelectNoCursorV2AscStmtName = "SelectWithoutCursorV2Asc"
const SelectNoCursorV2AscStmtDef =
  """SELECT contentTopic, payload, pubsubTopic, version, timestamp, id, messageHash, meta FROM messages
    WHERE contentTopic IN ($1) AND
          pubsubTopic = $2 AND
          timestamp >= $3 AND
          timestamp <= $4
    ORDER BY timestamp ASC LIMIT $5;"""

const SelectNoCursorV2DescStmtName = "SelectWithoutCursorV2Desc"
const SelectNoCursorV2DescStmtDef =
  """SELECT contentTopic, payload, pubsubTopic, version, timestamp, id, messageHash, meta FROM messages
    WHERE contentTopic IN ($1) AND
          pubsubTopic = $2 AND
          timestamp >= $3 AND
          timestamp <= $4
    ORDER BY timestamp DESC LIMIT $5;"""

const SelectWithCursorV2DescStmtName = "SelectWithCursorV2Desc"
const SelectWithCursorV2DescStmtDef =
  """SELECT contentTopic, payload, pubsubTopic, version, timestamp, id, messageHash, meta FROM messages
    WHERE contentTopic IN ($1) AND
          pubsubTopic = $2 AND
          (timestamp, id) < ($3,$4) AND
          timestamp >= $5 AND
          timestamp <= $6
    ORDER BY timestamp DESC LIMIT $7;"""

const SelectWithCursorV2AscStmtName = "SelectWithCursorV2Asc"
const SelectWithCursorV2AscStmtDef =
  """SELECT contentTopic, payload, pubsubTopic, version, timestamp, id, messageHash, meta FROM messages
    WHERE contentTopic IN ($1) AND
          pubsubTopic = $2 AND
          (timestamp, id) > ($3,$4) AND
          timestamp >= $5 AND
          timestamp <= $6
    ORDER BY timestamp ASC LIMIT $7;"""

const DefaultMaxNumConns = 50

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

  let driver = PostgresDriver(writeConnPool: writeConnPool, readConnPool: readConnPool)
  return ok(driver)

proc reset*(s: PostgresDriver): Future[ArchiveDriverResult[void]] {.async.} =
  ## Clear the database partitions
  let targetSize = 0
  let forceRemoval = true
  let ret = await s.decreaseDatabaseSize(targetSize, forceRemoval)
  return ret

proc rowCallbackImpl(
    pqResult: ptr PGresult,
    outRows: var seq[(PubsubTopic, WakuMessage, seq[byte], Timestamp, WakuMessageHash)],
) =
  ## Proc aimed to contain the logic of the callback passed to the `psasyncpool`.
  ## That callback is used in "SELECT" queries.
  ##
  ## pqResult - contains the query results
  ## outRows - seq of Store-rows. This is populated from the info contained in pqResult

  let numFields = pqResult.pqnfields()
  if numFields != 8:
    error "Wrong number of fields, expected 8", numFields
    return

  for iRow in 0 ..< pqResult.pqNtuples():
    var wakuMessage: WakuMessage
    var timestamp: Timestamp
    var version: uint
    var pubSubTopic: string
    var contentTopic: string
    var digest: string
    var payload: string
    var hashHex: string
    var msgHash: WakuMessageHash
    var meta: string

    try:
      contentTopic = $(pqgetvalue(pqResult, iRow, 0))
      payload = parseHexStr($(pqgetvalue(pqResult, iRow, 1)))
      pubSubTopic = $(pqgetvalue(pqResult, iRow, 2))
      version = parseUInt($(pqgetvalue(pqResult, iRow, 3)))
      timestamp = parseInt($(pqgetvalue(pqResult, iRow, 4)))
      digest = parseHexStr($(pqgetvalue(pqResult, iRow, 5)))
      hashHex = parseHexStr($(pqgetvalue(pqResult, iRow, 6)))
      meta = parseHexStr($(pqgetvalue(pqResult, iRow, 7)))
      msgHash = fromBytes(hashHex.toOpenArrayByte(0, 31))
    except ValueError:
      error "could not parse correctly", error = getCurrentExceptionMsg()

    wakuMessage.timestamp = timestamp
    wakuMessage.version = uint32(version)
    wakuMessage.contentTopic = contentTopic
    wakuMessage.payload = @(payload.toOpenArrayByte(0, payload.high))
    wakuMessage.meta = @(meta.toOpenArrayByte(0, meta.high))

    outRows.add(
      (
        pubSubTopic,
        wakuMessage,
        @(digest.toOpenArrayByte(0, digest.high)),
        timestamp,
        msgHash,
      )
    )

method put*(
    s: PostgresDriver,
    pubsubTopic: PubsubTopic,
    message: WakuMessage,
    digest: MessageDigest,
    messageHash: WakuMessageHash,
    receivedTime: Timestamp,
): Future[ArchiveDriverResult[void]] {.async.} =
  let digest = toHex(digest.data)
  let messageHash = toHex(messageHash)
  let contentTopic = message.contentTopic
  let payload = toHex(message.payload)
  let version = $message.version
  let timestamp = $message.timestamp
  let meta = toHex(message.meta)

  trace "put PostgresDriver", timestamp = timestamp

  (
    await s.writeConnPool.runStmt(
      InsertRowStmtName,
      InsertRowStmtDefinition,
      @[
        digest, messageHash, contentTopic, payload, pubsubTopic, version, timestamp,
        meta,
      ],
      @[
        int32(digest.len),
        int32(messageHash.len),
        int32(contentTopic.len),
        int32(payload.len),
        int32(pubsubTopic.len),
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
  var rows: seq[(PubsubTopic, WakuMessage, seq[byte], Timestamp, WakuMessageHash)]
  proc rowCallback(pqResult: ptr PGresult) =
    rowCallbackImpl(pqResult, rows)

  (
    await s.readConnPool.pgQuery(
      """SELECT contentTopic,
                                       payload, pubsubTopic, version, timestamp,
                                       id, messageHash, meta FROM messages ORDER BY timestamp ASC""",
      newSeq[string](0),
      rowCallback,
    )
  ).isOkOr:
    return err("failed in query: " & $error)

  return ok(rows)

proc getMessagesArbitraryQuery(
    s: PostgresDriver,
    contentTopic: seq[ContentTopic] = @[],
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

  var query =
    """SELECT contentTopic, payload, pubsubTopic, version, timestamp, id, messageHash, meta FROM messages"""
  var statements: seq[string]
  var args: seq[string]

  if contentTopic.len > 0:
    let cstmt = "contentTopic IN (" & "?".repeat(contentTopic.len).join(",") & ")"
    statements.add(cstmt)
    for t in contentTopic:
      args.add(t)

  if hexHashes.len > 0:
    let cstmt = "messageHash IN (" & "?".repeat(hexHashes.len).join(",") & ")"
    statements.add(cstmt)
    for t in hexHashes:
      args.add(t)

  if pubsubTopic.isSome():
    statements.add("pubsubTopic = ?")
    args.add(pubsubTopic.get())

  if cursor.isSome():
    let hashHex = toHex(cursor.get().hash)

    var entree: seq[(PubsubTopic, WakuMessage, seq[byte], Timestamp, WakuMessageHash)]
    proc entreeCallback(pqResult: ptr PGresult) =
      rowCallbackImpl(pqResult, entree)

    (
      await s.readConnPool.runStmt(
        SelectMessageByHashName,
        SelectMessageByHashDef,
        @[hashHex],
        @[int32(hashHex.len)],
        @[int32(0)],
        entreeCallback,
        requestId,
      )
    ).isOkOr:
      return err("failed to run query with cursor: " & $error)

    if entree.len == 0:
      return ok(entree)

    let storetime = entree[0][3]

    let comp = if ascendingOrder: ">" else: "<"
    statements.add("(timestamp, messageHash) " & comp & " (?,?)")
    args.add($storetime)
    args.add(hashHex)

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

  var rows: seq[(PubsubTopic, WakuMessage, seq[byte], Timestamp, WakuMessageHash)]
  proc rowCallback(pqResult: ptr PGresult) =
    rowCallbackImpl(pqResult, rows)

  (await s.readConnPool.pgQuery(query, args, rowCallback, requestId)).isOkOr:
    return err("failed to run query: " & $error)

  return ok(rows)

proc getMessagesV2ArbitraryQuery(
    s: PostgresDriver,
    contentTopic: seq[ContentTopic] = @[],
    pubsubTopic = none(PubsubTopic),
    cursor = none(ArchiveCursor),
    startTime = none(Timestamp),
    endTime = none(Timestamp),
    maxPageSize = DefaultPageSize,
    ascendingOrder = true,
    requestId: string,
): Future[ArchiveDriverResult[seq[ArchiveRow]]] {.async, deprecated.} =
  ## This proc allows to handle atypical queries. We don't use prepared statements for those.

  var query =
    """SELECT contentTopic, payload, pubsubTopic, version, timestamp, id, messageHash, meta FROM messages"""
  var statements: seq[string]
  var args: seq[string]

  if contentTopic.len > 0:
    let cstmt = "contentTopic IN (" & "?".repeat(contentTopic.len).join(",") & ")"
    statements.add(cstmt)
    for t in contentTopic:
      args.add(t)

  if pubsubTopic.isSome():
    statements.add("pubsubTopic = ?")
    args.add(pubsubTopic.get())

  if cursor.isSome():
    let comp = if ascendingOrder: ">" else: "<"
    statements.add("(timestamp, id) " & comp & " (?,?)")
    args.add($cursor.get().storeTime)
    args.add(toHex(cursor.get().digest.data))

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

  query &= " ORDER BY timestamp " & direction & ", id " & direction

  query &= " LIMIT ?"
  args.add($maxPageSize)

  var rows: seq[(PubsubTopic, WakuMessage, seq[byte], Timestamp, WakuMessageHash)]
  proc rowCallback(pqResult: ptr PGresult) =
    rowCallbackImpl(pqResult, rows)

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
  ## This proc aims to run the most typical queries in a more performant way, i.e. by means of
  ## prepared statements.
  ##
  ## contentTopic - string with list of conten topics. e.g: "'ctopic1','ctopic2','ctopic3'"

  var rows: seq[(PubsubTopic, WakuMessage, seq[byte], Timestamp, WakuMessageHash)]
  proc rowCallback(pqResult: ptr PGresult) =
    rowCallbackImpl(pqResult, rows)

  let startTimeStr = $startTime
  let endTimeStr = $endTime
  let limit = $maxPageSize

  if cursor.isSome():
    let hash = toHex(cursor.get().hash)

    var entree: seq[(PubsubTopic, WakuMessage, seq[byte], Timestamp, WakuMessageHash)]

    proc entreeCallback(pqResult: ptr PGresult) =
      rowCallbackImpl(pqResult, entree)

    (
      await s.readConnPool.runStmt(
        SelectMessageByHashName,
        SelectMessageByHashDef,
        @[hash],
        @[int32(hash.len)],
        @[int32(0)],
        entreeCallback,
        requestId,
      )
    ).isOkOr:
      return err("failed to run query with cursor: " & $error)

    if entree.len == 0:
      return ok(entree)

    let timestamp = $entree[0][3]

    var stmtName =
      if ascOrder: SelectWithCursorAscStmtName else: SelectWithCursorDescStmtName
    var stmtDef =
      if ascOrder: SelectWithCursorAscStmtDef else: SelectWithCursorDescStmtDef

    (
      await s.readConnPool.runStmt(
        stmtName,
        stmtDef,
        @[
          contentTopic, hashes, pubsubTopic, timestamp, hash, startTimeStr, endTimeStr,
          limit,
        ],
        @[
          int32(contentTopic.len),
          int32(hashes.len),
          int32(pubsubTopic.len),
          int32(timestamp.len),
          int32(hash.len),
          int32(startTimeStr.len),
          int32(endTimeStr.len),
          int32(limit.len),
        ],
        @[
          int32(0), int32(0), int32(0), int32(0), int32(0), int32(0), int32(0), int32(0)
        ],
        rowCallback,
        requestId,
      )
    ).isOkOr:
      return err("failed to run query with cursor: " & $error)
  else:
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
      return err("failed to run query without cursor: " & $error)

  return ok(rows)

proc getMessagesV2PreparedStmt(
    s: PostgresDriver,
    contentTopic: string,
    pubsubTopic: PubsubTopic,
    cursor = none(ArchiveCursor),
    startTime: Timestamp,
    endTime: Timestamp,
    maxPageSize = DefaultPageSize,
    ascOrder = true,
    requestId: string,
): Future[ArchiveDriverResult[seq[ArchiveRow]]] {.async, deprecated.} =
  ## This proc aims to run the most typical queries in a more performant way, i.e. by means of
  ## prepared statements.
  ##
  ## contentTopic - string with list of conten topics. e.g: "'ctopic1','ctopic2','ctopic3'"

  var rows: seq[(PubsubTopic, WakuMessage, seq[byte], Timestamp, WakuMessageHash)]
  proc rowCallback(pqResult: ptr PGresult) =
    rowCallbackImpl(pqResult, rows)

  let startTimeStr = $startTime
  let endTimeStr = $endTime
  let limit = $maxPageSize

  if cursor.isSome():
    var stmtName =
      if ascOrder: SelectWithCursorV2AscStmtName else: SelectWithCursorV2DescStmtName
    var stmtDef =
      if ascOrder: SelectWithCursorV2AscStmtDef else: SelectWithCursorV2DescStmtDef

    let digest = toHex(cursor.get().digest.data)
    let timestamp = $cursor.get().storeTime

    (
      await s.readConnPool.runStmt(
        stmtName,
        stmtDef,
        @[contentTopic, pubsubTopic, timestamp, digest, startTimeStr, endTimeStr, limit],
        @[
          int32(contentTopic.len),
          int32(pubsubTopic.len),
          int32(timestamp.len),
          int32(digest.len),
          int32(startTimeStr.len),
          int32(endTimeStr.len),
          int32(limit.len),
        ],
        @[int32(0), int32(0), int32(0), int32(0), int32(0), int32(0), int32(0)],
        rowCallback,
        requestId,
      )
    ).isOkOr:
      return err("failed to run query with cursor: " & $error)
  else:
    var stmtName =
      if ascOrder: SelectNoCursorV2AscStmtName else: SelectNoCursorV2DescStmtName
    var stmtDef =
      if ascOrder: SelectNoCursorV2AscStmtDef else: SelectNoCursorV2DescStmtDef

    (
      await s.readConnPool.runStmt(
        stmtName,
        stmtDef,
        @[contentTopic, pubsubTopic, startTimeStr, endTimeStr, limit],
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
      return err("failed to run query without cursor: " & $error)

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
  SELECT contentTopic, payload, pubsubTopic, version, m.timestamp, id, m.messageHash, meta
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

  var rows: seq[(PubsubTopic, WakuMessage, seq[byte], Timestamp, WakuMessageHash)]
  proc rowCallback(pqResult: ptr PGresult) =
    rowCallbackImpl(pqResult, rows)

  (
    await s.readConnPool.pgQuery(
      query = query, rowCallback = rowCallback, requestId = requestId
    )
  ).isOkOr:
    return err("failed to run query: " & $error)

  return ok(rows)

method getMessages*(
    s: PostgresDriver,
    includeData = true,
    contentTopicSeq = newSeq[ContentTopic](0),
    pubsubTopic = none(PubsubTopic),
    cursor = none(ArchiveCursor),
    startTime = none(Timestamp),
    endTime = none(Timestamp),
    hashes = newSeq[WakuMessageHash](0),
    maxPageSize = DefaultPageSize,
    ascendingOrder = true,
    requestId = "",
): Future[ArchiveDriverResult[seq[ArchiveRow]]] {.async.} =
  let hexHashes = hashes.mapIt(toHex(it))

  if cursor.isNone() and pubsubTopic.isNone() and contentTopicSeq.len == 0 and
      startTime.isNone() and endTime.isNone() and hexHashes.len > 0:
    return await s.getMessagesByMessageHashes(
      "'" & hexHashes.join("','") & "'", maxPageSize, requestId
    )

  if contentTopicSeq.len == 1 and hexHashes.len == 1 and pubsubTopic.isSome() and
      startTime.isSome() and endTime.isSome():
    ## Considered the most common query. Therefore, we use prepared statements to optimize it.
    return await s.getMessagesPreparedStmt(
      contentTopicSeq.join(","),
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
    ## We will run atypical query. In this case we don't use prepared statemets
    return await s.getMessagesArbitraryQuery(
      contentTopicSeq, pubsubTopic, cursor, startTime, endTime, hexHashes, maxPageSize,
      ascendingOrder, requestId,
    )

method getMessagesV2*(
    s: PostgresDriver,
    contentTopicSeq = newSeq[ContentTopic](0),
    pubsubTopic = none(PubsubTopic),
    cursor = none(ArchiveCursor),
    startTime = none(Timestamp),
    endTime = none(Timestamp),
    maxPageSize = DefaultPageSize,
    ascendingOrder = true,
    requestId: string,
): Future[ArchiveDriverResult[seq[ArchiveRow]]] {.async, deprecated.} =
  if contentTopicSeq.len == 1 and pubsubTopic.isSome() and startTime.isSome() and
      endTime.isSome():
    ## Considered the most common query. Therefore, we use prepared statements to optimize it.
    return await s.getMessagesV2PreparedStmt(
      contentTopicSeq.join(","),
      PubsubTopic(pubsubTopic.get()),
      cursor,
      startTime.get(),
      endTime.get(),
      maxPageSize,
      ascendingOrder,
      requestId,
    )
  else:
    ## We will run atypical query. In this case we don't use prepared statemets
    return await s.getMessagesV2ArbitraryQuery(
      contentTopicSeq, pubsubTopic, cursor, startTime, endTime, maxPageSize,
      ascendingOrder, requestId,
    )

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
  return err("not implemented because legacy will get deprecated")

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
  ## Will be completely removed when deprecating store legacy
  # let execRes = await s.writeConnPool.pgQuery(
  #   """DELETE FROM messages WHERE id NOT IN
  #                         (
  #                       SELECT id FROM messages ORDER BY timestamp DESC LIMIT ?
  #                         );""",
  #   @[$limit],
  # )
  # if execRes.isErr():
  #   return err("error in deleteOldestMessagesNotWithinLimit: " & execRes.error)

  return ok()

method close*(s: PostgresDriver): Future[ArchiveDriverResult[void]] {.async.} =
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

proc performWriteQuery*(
    s: PostgresDriver, query: string
): Future[ArchiveDriverResult[void]] {.async.} =
  ## Performs a query that somehow changes the state of the database

  (await s.writeConnPool.pgQuery(query)).isOkOr:
    return err("error in performWriteQuery: " & $error)

  return ok()

method decreaseDatabaseSize*(
    driver: PostgresDriver, targetSizeInBytes: int64, forceRemoval: bool = false
): Future[ArchiveDriverResult[void]] {.async.} =
  ## This is completely disabled and only the non-legacy driver
  ## will take care of that
  # var dbSize = (await driver.getDatabaseSize()).valueOr:
  #   return err("decreaseDatabaseSize failed to get database size: " & $error)

  # ## database size in bytes
  # var totalSizeOfDB: int64 = int64(dbSize)

  # if totalSizeOfDB <= targetSizeInBytes:
  #   return ok()

  # debug "start reducing database size",
  #   targetSize = $targetSizeInBytes, currentSize = $totalSizeOfDB

  # while totalSizeOfDB > targetSizeInBytes and driver.containsAnyPartition():
  #   (await driver.removeOldestPartition(forceRemoval)).isOkOr:
  #     return err(
  #       "decreaseDatabaseSize inside loop failed to remove oldest partition: " & $error
  #     )

  #   dbSize = (await driver.getDatabaseSize()).valueOr:
  #     return
  #       err("decreaseDatabaseSize inside loop failed to get database size: " & $error)

  #   let newCurrentSize = int64(dbSize)
  #   if newCurrentSize == totalSizeOfDB:
  #     return err("the previous partition removal didn't clear database size")

  #   totalSizeOfDB = newCurrentSize

  #   debug "reducing database size",
  #     targetSize = $targetSizeInBytes, newCurrentSize = $totalSizeOfDB

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
  # (await s.removePartitionsOlderThan(tsNanoSec)).isOkOr:
  #   return err("error while removing older partitions: " & $error)

  # (
  #   await s.writeConnPool.pgQuery(
  #     "DELETE FROM messages WHERE timestamp < " & $tsNanoSec
  #   )
  # ).isOkOr:
  #   return err("error in deleteMessagesOlderThanTimestamp: " & $error)

  return ok()
