import std/[json, sugar, strutils, options]
import chronos, chronicles, results
import
  ../../../../../waku/factory/waku,
  ../../../../alloc,
  ../../../../utils,
  ../../../../../waku/waku_core/peers,
  ../../../../../waku/waku_core/time,
  ../../../../../waku/waku_core/message/digest,
  ../../../../../waku/waku_store/common,
  ../../../../../waku/waku_store/client,
  ../../../../../waku/common/paging

type StoreReqType* = enum
  REMOTE_QUERY ## to perform a query to another Store node

type StoreRequest* = object
  operation: StoreReqType
  jsonQuery: cstring
  peerAddr: cstring
  timeoutMs: cint

func fromJsonNode(
    T: type StoreRequest, jsonContent: JsonNode
): Result[StoreQueryRequest, string] =
  let contentTopics = collect(newSeq):
    for cTopic in jsonContent["content_topics"].getElems():
      cTopic.getStr()

  let msgHashes = collect(newSeq):
    if jsonContent.contains("message_hashes"):
      for hashJsonObj in jsonContent["message_hashes"].getElems():
        var hash: WakuMessageHash
        var count: int = 0
        for byteValue in hashJsonObj.getElems():
          hash[count] = byteValue.getInt().byte
          count.inc()

        hash

  let pubsubTopic =
    if jsonContent.contains("pubsub_topic"):
      some(jsonContent["pubsub_topic"].getStr())
    else:
      none(string)

  let paginationCursor =
    if jsonContent.contains("pagination_cursor"):
      var hash: WakuMessageHash
      var count: int = 0
      for byteValue in jsonContent["pagination_cursor"].getElems():
        hash[count] = byteValue.getInt().byte
        count.inc()

      some(hash)
    else:
      none(WakuMessageHash)

  let paginationForwardBool = jsonContent["pagination_forward"].getBool()
  let paginationForward =
    if paginationForwardBool: PagingDirection.FORWARD else: PagingDirection.BACKWARD

  let paginationLimit =
    if jsonContent.contains("pagination_limit"):
      some(uint64(jsonContent["pagination_limit"].getInt()))
    else:
      none(uint64)

  let startTime = ?jsonContent.getProtoInt64("time_start")
  let endTime = ?jsonContent.getProtoInt64("time_end")

  return ok(
    StoreQueryRequest(
      requestId: jsonContent["request_id"].getStr(),
      includeData: jsonContent["include_data"].getBool(),
      pubsubTopic: pubsubTopic,
      contentTopics: contentTopics,
      startTime: startTime,
      endTime: endTime,
      messageHashes: msgHashes,
      paginationCursor: paginationCursor,
      paginationForward: paginationForward,
      paginationLimit: paginationLimit,
    )
  )

proc createShared*(
    T: type StoreRequest,
    op: StoreReqType,
    jsonQuery: cstring,
    peerAddr: cstring,
    timeoutMs: cint,
): ptr type T =
  var ret = createShared(T)
  ret[].operation = op
  ret[].timeoutMs = timeoutMs
  ret[].jsonQuery = jsonQuery.alloc()
  ret[].peerAddr = peerAddr.alloc()
  return ret

proc destroyShared(self: ptr StoreRequest) =
  deallocShared(self[].jsonQuery)
  deallocShared(self[].peerAddr)
  deallocShared(self)

proc process_remote_query(
    self: ptr StoreRequest, waku: ptr Waku
): Future[Result[string, string]] {.async.} =
  let jsonContentRes = catch:
    parseJson($self[].jsonQuery)

  if jsonContentRes.isErr():
    return err("StoreRequest failed parsing store request: " & jsonContentRes.error.msg)

  let storeQueryRequest = ?StoreRequest.fromJsonNode(jsonContentRes.get())

  let peer = peers.parsePeerInfo(($self[].peerAddr).split(",")).valueOr:
    return err("StoreRequest failed to parse peer addr: " & $error)

  let queryResponse = (await waku.node.wakuStoreClient.query(storeQueryRequest, peer)).valueOr:
    return err("StoreRequest failed store query: " & $error)

  return ok($(%*queryResponse)) ## returning the response in json format

proc process*(
    self: ptr StoreRequest, waku: ptr Waku
): Future[Result[string, string]] {.async.} =
  defer:
    deallocShared(self)

  case self.operation
  of REMOTE_QUERY:
    return await self.process_remote_query(waku)

  error "store request not handled at all"
  return err("store request not handled at all")
