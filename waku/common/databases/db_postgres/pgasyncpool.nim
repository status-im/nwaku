# Simple async pool driver for postgress.
# Inspired by: https://github.com/treeform/pg/
{.push raises: [].}

import
  std/[sequtils, strformat],
  regex,
  results,
  chronos,
  chronos/threadsync,
  chronicles,
  strutils
import ./dbconn, ../common, ../../../waku_core/time

type
  # Database connection pool
  PgAsyncPool* = ref object
    connString: string
    maxConnections: int
    conns: seq[DbConnWrapper]
    busySignal: ThreadSignalPtr ## signal to wait while the pool is busy

proc new*(T: type PgAsyncPool, dbUrl: string, maxConnections: int): DatabaseResult[T] =
  var connString: string

  try:
    let regex = re2("""^postgres:\/\/([^:]+):([^@]+)@([^:]+):(\d+)\/(.+)$""")
    var m: RegexMatch2
    if dbUrl.match(regex, m) == false:
      return err("could not properly parse dbUrl: " & dbUrl)

    let user = m.captures[0]
    let password = m.captures[1]
    let host = m.captures[2]
    let port = m.captures[3]
    let dbName = m.captures[4]

    connString =
      fmt"user={user} host={host} port={port} dbname={dbName} password={password}"
  except KeyError, ValueError:
    return err("could not parse postgres string: " & getCurrentExceptionMsg())

  let pool = PgAsyncPool(
    connString: connString,
    maxConnections: maxConnections,
    conns: newSeq[DbConnWrapper](0),
  )

  return ok(pool)

func isBusy(pool: PgAsyncPool): bool =
  return pool.conns.mapIt(it.isPgDbConnBusy()).allIt(it)

proc close*(pool: PgAsyncPool): Future[Result[void, string]] {.async.} =
  ## Gracefully wait and close all openned connections
  # wait for the connections to be released and close them, without
  # blocking the async runtime

  debug "close PgAsyncPool"
  await allFutures(pool.conns.mapIt(it.futBecomeFree))
  debug "closing all connection PgAsyncPool"

  for i in 0 ..< pool.conns.len:
    if pool.conns[i].isPgDbConnOpen():
      pool.conns[i].closeDbConn().isOkOr:
        return err("error in close PgAsyncPool: " & $error)
      pool.conns[i].setPgDbConnOpen(false)

  pool.conns.setLen(0)

  return ok()

proc getFirstFreeConnIndex(pool: PgAsyncPool): DatabaseResult[int] =
  for index in 0 ..< pool.conns.len:
    if pool.conns[index].isPgDbConnBusy():
      continue

    ## Pick up the first free connection and set it busy
    return ok(index)

proc getConnIndex(pool: PgAsyncPool): Future[DatabaseResult[int]] {.async.} =
  ## Waits for a free connection or create if max connections limits have not been reached.
  ## Returns the index of the free connection

  if not pool.isBusy():
    return pool.getFirstFreeConnIndex()

  ## Pool is busy then
  if pool.conns.len == pool.maxConnections:
    ## Can't create more connections. Wait for a free connection without blocking the async runtime.
    let busyFuts = pool.conns.mapIt(it.futBecomeFree)
    discard await one(busyFuts)

    return pool.getFirstFreeConnIndex()
  elif pool.conns.len < pool.maxConnections:
    ## stablish a new connection
    let dbConn = DbConnWrapper.new(pool.connString).valueOr:
      return err("error creating DbConnWrapper: " & $error)

    pool.conns.add(dbConn)
    return ok(pool.conns.len - 1)

proc resetConnPool*(pool: PgAsyncPool): Future[DatabaseResult[void]] {.async.} =
  ## Forces closing the connection pool.
  ## This proc is intended to be called when the connection with the database
  ## got interrupted from the database side or a connectivity problem happened.

  (await pool.close()).isOkOr:
    return err("error in resetConnPool: " & error)

  return ok()

const SlowQueryThreshold = 1.seconds

proc pgQuery*(
    pool: PgAsyncPool,
    query: string,
    args: seq[string] = newSeq[string](0),
    rowCallback: DataProc = nil,
    requestId: string = "",
): Future[DatabaseResult[void]] {.async.} =
  let connIndex = (await pool.getConnIndex()).valueOr:
    return err("connRes.isErr in query: " & $error)

  let queryStartTime = getNowInNanosecondTime()
  let dbConnWrapper = pool.conns[connIndex]
  defer:
    let queryDuration = getNowInNanosecondTime() - queryStartTime
    if queryDuration > SlowQueryThreshold.nanos:
      debug "pgQuery slow query",
        query_duration_secs = (queryDuration / 1_000_000_000), query, requestId

  (await dbConnWrapper.dbConnQuery(sql(query), args, rowCallback, requestId)).isOkOr:
    return err("error in asyncpool query: " & $error)

  return ok()

proc runStmt*(
    pool: PgAsyncPool,
    stmtName: string,
    stmtDefinition: string,
    paramValues: seq[string],
    paramLengths: seq[int32],
    paramFormats: seq[int32],
    rowCallback: DataProc = nil,
    requestId: string = "",
): Future[DatabaseResult[void]] {.async.} =
  ## Runs a stored statement, for performance purposes.
  ## The stored statements are connection specific and is a technique of caching a very common
  ## queries within the same connection.
  ##
  ## rowCallback != nil when it is expected to retrieve info from the database.
  ## rowCallback == nil for queries that change the database state.

  let connIndex = (await pool.getConnIndex()).valueOr:
    return err("Error in runStmt: " & $error)

  let dbConnWrapper = pool.conns[connIndex]
  let queryStartTime = getNowInNanosecondTime()

  defer:
    let queryDuration = getNowInNanosecondTime() - queryStartTime
    if queryDuration > SlowQueryThreshold.nanos:
      debug "runStmt slow query",
        query_duration = queryDuration / 1_000_000_000,
        query = stmtDefinition,
        requestId

  if not pool.conns[connIndex].containsPreparedStmt(stmtName):
    # The connection doesn't have that statement yet. Let's create it.
    # Each session/connection has its own prepared statements.
    let res = catch:
      let len = paramValues.len
      discard dbConnWrapper.getDbConn().prepare(stmtName, sql(stmtDefinition), len)

    if res.isErr():
      return err("failed prepare in runStmt: " & res.error.msg)

    pool.conns[connIndex].inclPreparedStmt(stmtName)

  (
    await dbConnWrapper.dbConnQueryPrepared(
      stmtName, paramValues, paramLengths, paramFormats, rowCallback, requestId
    )
  ).isOkOr:
    return err("error in runStmt: " & $error)

  return ok()
