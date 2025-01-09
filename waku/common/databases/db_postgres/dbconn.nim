import
  std/[times, strutils, asyncnet, os, sequtils, sets, strformat],
  regex,
  results,
  chronos,
  chronos/threadsync,
  metrics,
  chronicles
import ./query_metrics

include db_connector/db_postgres

type DataProc* = proc(result: ptr PGresult) {.closure, gcsafe, raises: [].}

type DbConnWrapper* = ref object
  dbConn: DbConn
  open: bool
  preparedStmts: HashSet[string] ## [stmtName's]
  futBecomeFree*: Future[void]
    ## to notify the pgasyncpool that this conn is free, i.e. not busy

## Connection management

proc containsPreparedStmt*(dbConnWrapper: DbConnWrapper, preparedStmt: string): bool =
  return dbConnWrapper.preparedStmts.contains(preparedStmt)

proc inclPreparedStmt*(dbConnWrapper: DbConnWrapper, preparedStmt: string) =
  dbConnWrapper.preparedStmts.incl(preparedStmt)

proc getDbConn*(dbConnWrapper: DbConnWrapper): DbConn =
  return dbConnWrapper.dbConn

proc isPgDbConnBusy*(dbConnWrapper: DbConnWrapper): bool =
  if isNil(dbConnWrapper.futBecomeFree):
    return false
  return not dbConnWrapper.futBecomeFree.finished()

proc isPgDbConnOpen*(dbConnWrapper: DbConnWrapper): bool =
  return dbConnWrapper.open

proc setPgDbConnOpen*(dbConnWrapper: DbConnWrapper, newOpenState: bool) =
  dbConnWrapper.open = newOpenState

proc check(db: DbConn): Result[void, string] =
  var message: string
  try:
    message = $db.pqErrorMessage()
  except ValueError, DbError:
    return err("exception in check: " & getCurrentExceptionMsg())

  if message.len > 0:
    return err($message)

  return ok()

proc openDbConn(connString: string): Result[DbConn, string] =
  ## Opens a new connection.
  var conn: DbConn = nil
  try:
    conn = open("", "", "", connString) ## included from db_postgres module
  except DbError:
    return err("exception opening new connection: " & getCurrentExceptionMsg())

  if conn.status != CONNECTION_OK:
    let checkRes = conn.check()
    if checkRes.isErr():
      return err("failed to connect to database: " & checkRes.error)

    return err("unknown reason")

  ## registering the socket fd in chronos for better wait for data
  let asyncFd = cast[asyncengine.AsyncFD](pqsocket(conn))
  asyncengine.register(asyncFd)

  return ok(conn)

proc new*(T: type DbConnWrapper, connString: string): Result[T, string] =
  let dbConn = openDbConn(connString).valueOr:
    return err("failed to establish a new connection: " & $error)

  return ok(DbConnWrapper(dbConn: dbConn, open: true))

proc closeDbConn*(
    dbConnWrapper: DbConnWrapper
): Result[void, string] {.raises: [OSError].} =
  let fd = dbConnWrapper.dbConn.pqsocket()
  if fd == -1:
    return err("error file descriptor -1 in closeDbConn")

  asyncengine.unregister(cast[asyncengine.AsyncFD](fd))

  dbConnWrapper.dbConn.close()

  return ok()

proc `$`(self: SqlQuery): string =
  return cast[string](self)

proc sendQuery(
    dbConnWrapper: DbConnWrapper, query: SqlQuery, args: seq[string]
): Future[Result[void, string]] {.async.} =
  ## This proc can be used directly for queries that don't retrieve values back.

  if dbConnWrapper.dbConn.status != CONNECTION_OK:
    dbConnWrapper.dbConn.check().isOkOr:
      return err("failed to connect to database: " & $error)

    return err("unknown reason")

  var wellFormedQuery = ""
  try:
    wellFormedQuery = dbFormat(query, args)
  except DbError:
    return err("exception formatting the query: " & getCurrentExceptionMsg())

  let success = dbConnWrapper.dbConn.pqsendQuery(cstring(wellFormedQuery))
  if success != 1:
    dbConnWrapper.dbConn.check().isOkOr:
      return err("failed pqsendQuery: " & $error)
    return err("failed pqsendQuery: unknown reason")

  return ok()

proc sendQueryPrepared(
    dbConnWrapper: DbConnWrapper,
    stmtName: string,
    paramValues: openArray[string],
    paramLengths: openArray[int32],
    paramFormats: openArray[int32],
): Result[void, string] {.raises: [].} =
  ## This proc can be used directly for queries that don't retrieve values back.

  if paramValues.len != paramLengths.len or paramValues.len != paramFormats.len or
      paramLengths.len != paramFormats.len:
    let lengthsErrMsg =
      $paramValues.len & " " & $paramLengths.len & " " & $paramFormats.len
    return err("lengths discrepancies in sendQueryPrepared: " & $lengthsErrMsg)

  if dbConnWrapper.dbConn.status != CONNECTION_OK:
    dbConnWrapper.dbConn.check().isOkOr:
      return err("failed to connect to database: " & $error)

    return err("unknown reason")

  var cstrArrayParams = allocCStringArray(paramValues)
  defer:
    deallocCStringArray(cstrArrayParams)

  let nParams = cast[int32](paramValues.len)

  const ResultFormat = 0 ## 0 for text format, 1 for binary format.

  let success = dbConnWrapper.dbConn.pqsendQueryPrepared(
    stmtName,
    nParams,
    cstrArrayParams,
    unsafeAddr paramLengths[0],
    unsafeAddr paramFormats[0],
    ResultFormat,
  )
  if success != 1:
    dbConnWrapper.dbConn.check().isOkOr:
      return err("failed pqsendQueryPrepared: " & $error)

    return err("failed pqsendQueryPrepared: unknown reason")

  return ok()

proc waitQueryToFinish(
    dbConnWrapper: DbConnWrapper, rowCallback: DataProc = nil
): Future[Result[void, string]] {.async.} =
  ## The 'rowCallback' param is != nil when the underlying query wants to retrieve results (SELECT.)
  ## For other queries, like "INSERT", 'rowCallback' should be nil.

  let futDataAvailable = newFuture[void]("futDataAvailable")

  proc onDataAvailable(udata: pointer) {.gcsafe, raises: [].} =
    if not futDataAvailable.completed():
      futDataAvailable.complete()

  let asyncFd = cast[asyncengine.AsyncFD](pqsocket(dbConnWrapper.dbConn))

  asyncengine.addReader2(asyncFd, onDataAvailable).isOkOr:
    dbConnWrapper.futBecomeFree.fail(newException(ValueError, $error))
    return err("failed to add event reader in waitQueryToFinish: " & $error)
  defer:
    asyncengine.removeReader2(asyncFd).isOkOr:
      return err("failed to remove event reader in waitQueryToFinish: " & $error)

  await futDataAvailable

  ## Now retrieve the result from the database
  while true:
    let pqResult = dbConnWrapper.dbConn.pqgetResult()

    if pqResult == nil:
      dbConnWrapper.dbConn.check().isOkOr:
        if not dbConnWrapper.futBecomeFree.failed():
          dbConnWrapper.futBecomeFree.fail(newException(ValueError, $error))
        return err("error in query: " & $error)

      dbConnWrapper.futBecomeFree.complete()
      return ok() # reached the end of the results. The query is completed

    if not rowCallback.isNil():
      rowCallback(pqResult)

    pqclear(pqResult)

proc containsRiskyPatterns(input: string): bool =
  let riskyPatterns =
    @[
      " OR ", " AND ", " UNION ", " SELECT ", "INSERT ", "DELETE ", "UPDATE ", "DROP ",
      "EXEC ", "--", "/*", "*/",
    ]

  for pattern in riskyPatterns:
    if pattern.toLowerAscii() in input.toLowerAscii():
      return true

  return false

proc isSecureString(input: string): bool =
  ## Returns `false` if the string contains risky characters or patterns, `true` otherwise.
  let riskyChars = {'\'', '\"', ';', '#', '\\', '%', '_', '/', '*', '\0'}

  for ch in input:
    if ch in riskyChars:
      return false

  if containsRiskyPatterns(input):
    return false

  return true

proc dbConnQuery*(
    dbConnWrapper: DbConnWrapper,
    query: SqlQuery,
    args: seq[string],
    rowCallback: DataProc,
    requestId: string,
): Future[Result[void, string]] {.async, gcsafe.} =
  if not requestId.isSecureString():
    return err("the passed request id is not secure: " & requestId)

  dbConnWrapper.futBecomeFree = newFuture[void]("dbConnQuery")

  let cleanedQuery = ($query).replace(" ", "").replace("\n", "")
  ## remove everything between ' or " all possible sequence of numbers. e.g. rm partition partition
  var querySummary = cleanedQuery.replace(re2("""(['"]).*?\\1"""), "")
  querySummary = querySummary.replace(re2"\d+", "")
  querySummary = "query_tag_" & querySummary[0 ..< min(querySummary.len, 200)]

  var queryStartTime = getTime().toUnixFloat()

  let reqIdAndQuery = "/* requestId=" & requestId & " */ " & $query
  (await dbConnWrapper.sendQuery(SqlQuery(reqIdAndQuery), args)).isOkOr:
    error "error in dbConnQuery", error = $error
    dbConnWrapper.futBecomeFree.fail(newException(ValueError, $error))
    return err("error in dbConnQuery calling sendQuery: " & $error)

  let sendDuration = getTime().toUnixFloat() - queryStartTime
  query_time_secs.set(sendDuration, [querySummary, "sendToDBQuery"])

  queryStartTime = getTime().toUnixFloat()

  (await dbConnWrapper.waitQueryToFinish(rowCallback)).isOkOr:
    return err("error in dbConnQuery calling waitQueryToFinish: " & $error)

  let waitDuration = getTime().toUnixFloat() - queryStartTime
  query_time_secs.set(waitDuration, [querySummary, "waitFinish"])

  query_count.inc(labelValues = [querySummary])

  if "insert" notin ($query).toLower():
    debug "dbConnQuery",
      requestId,
      query = $query,
      args,
      querySummary,
      waitDbQueryDurationSecs = waitDuration,
      sendToDBDurationSecs = sendDuration

  return ok()

proc dbConnQueryPrepared*(
    dbConnWrapper: DbConnWrapper,
    stmtName: string,
    paramValues: seq[string],
    paramLengths: seq[int32],
    paramFormats: seq[int32],
    rowCallback: DataProc,
    requestId: string,
): Future[Result[void, string]] {.async, gcsafe.} =
  dbConnWrapper.futBecomeFree = newFuture[void]("dbConnQueryPrepared")
  var queryStartTime = getTime().toUnixFloat()

  dbConnWrapper.sendQueryPrepared(stmtName, paramValues, paramLengths, paramFormats).isOkOr:
    dbConnWrapper.futBecomeFree.fail(newException(ValueError, $error))
    error "error in dbConnQueryPrepared", error = $error
    return err("error in dbConnQueryPrepared calling sendQuery: " & $error)

  let sendDuration = getTime().toUnixFloat() - queryStartTime
  query_time_secs.set(sendDuration, [stmtName, "sendToDBQuery"])

  queryStartTime = getTime().toUnixFloat()

  (await dbConnWrapper.waitQueryToFinish(rowCallback)).isOkOr:
    return err("error in dbConnQueryPrepared calling waitQueryToFinish: " & $error)

  let waitDuration = getTime().toUnixFloat() - queryStartTime
  query_time_secs.set(waitDuration, [stmtName, "waitFinish"])

  query_count.inc(labelValues = [stmtName])

  if "insert" notin stmtName.toLower():
    debug "dbConnQueryPrepared",
      requestId,
      stmtName,
      paramValues,
      waitDbQueryDurationSecs = waitDuration,
      sendToDBDurationSecs = sendDuration

  return ok()
