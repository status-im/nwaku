{.push raises: [].}

import std/strformat, results, chronicles, uri, json_serialization, presto/route
import
  ../../../waku_core,
  ../../../waku_store/common,
  ../../../waku_store/self_req_handler,
  ../../../waku_node,
  ../../../node/peer_manager,
  ../../../common/paging,
  ../../handlers,
  ../responses,
  ../serdes,
  ./types

export types

logScope:
  topics = "waku node rest store_api"

const futTimeout* = 5.seconds # Max time to wait for futures

const NoPeerNoDiscError* =
  RestApiResponse.preconditionFailed("No suitable service peer & no discovery method")

# Queries the store-node with the query parameters and
# returns a RestApiResponse that is sent back to the api client.
proc performStoreQuery(
    selfNode: WakuNode, storeQuery: StoreQueryRequest, storePeer: RemotePeerInfo
): Future[RestApiResponse] {.async.} =
  let queryFut = selfNode.query(storeQuery, storePeer)

  if not await queryFut.withTimeout(futTimeout):
    const msg = "No history response received (timeout)"
    error msg
    return RestApiResponse.internalServerError(msg)

  let futRes = queryFut.read()

  if futRes.isErr():
    const msg = "Error occurred in queryFut.read()"
    error msg, error = futRes.error
    return RestApiResponse.internalServerError(fmt("{msg} [{futRes.error}]"))

  let res = futRes.get().toHex()

  if res.statusCode == uint32(ErrorCode.TOO_MANY_REQUESTS):
    debug "Request rate limit reached on peer ", storePeer
    return RestApiResponse.tooManyRequests("Request rate limit reached")

  let resp = RestApiResponse.jsonResponse(res, status = Http200).valueOr:
    const msg = "Error building the json respose"
    let e = $error
    error msg, error = e
    return RestApiResponse.internalServerError(fmt("{msg} [{e}]"))

  return resp

# Converts a string time representation into an Option[Timestamp].
# Only positive time is considered a valid Timestamp in the request
proc parseTime(input: Option[string]): Result[Option[Timestamp], string] =
  if input.isSome() and input.get() != "":
    try:
      let time = parseInt(input.get())
      if time > 0:
        return ok(some(Timestamp(time)))
    except ValueError:
      return err("time parsing error: " & getCurrentExceptionMsg())

  return ok(none(Timestamp))

proc parseIncludeData(input: Option[string]): Result[bool, string] =
  var includeData = false
  if input.isSome() and input.get() != "":
    try:
      includeData = parseBool(input.get())
    except ValueError:
      return err("include data parsing error: " & getCurrentExceptionMsg())

  return ok(includeData)

# Creates a HistoryQuery from the given params
proc createStoreQuery(
    includeData: Option[string],
    pubsubTopic: Option[string],
    contentTopics: Option[string],
    startTime: Option[string],
    endTime: Option[string],
    hashes: Option[string],
    cursor: Option[string],
    direction: Option[string],
    pageSize: Option[string],
): Result[StoreQueryRequest, string] =
  var parsedIncludeData = ?parseIncludeData(includeData)

  # Parse pubsubTopic parameter
  var parsedPubsubTopic = none(string)
  if pubsubTopic.isSome():
    let decodedPubsubTopic = decodeUrl(pubsubTopic.get())
    if decodedPubsubTopic != "":
      parsedPubsubTopic = some(decodedPubsubTopic)

  # Parse the content topics
  var parsedContentTopics = newSeq[ContentTopic](0)
  if contentTopics.isSome():
    let ctList = decodeUrl(contentTopics.get())
    if ctList != "":
      for ct in ctList.split(','):
        parsedContentTopics.add(ct)

  # Parse start time
  let parsedStartTime = ?parseTime(startTime)

  # Parse end time
  let parsedEndTime = ?parseTime(endTime)

  var parsedHashes = ?parseHashes(hashes)

  # Parse cursor information
  let parsedCursor = ?parseHash(cursor)

  # Parse ascending field
  var parsedDirection = default()
  if direction.isSome() and direction.get() != "":
    parsedDirection = direction.get().into()

  # Parse page size field
  var parsedPagedSize = none(uint64)
  if pageSize.isSome() and pageSize.get() != "":
    try:
      parsedPagedSize = some(uint64(parseInt(pageSize.get())))
    except CatchableError:
      return err("page size parsing error: " & getCurrentExceptionMsg())

  return ok(
    StoreQueryRequest(
      includeData: parsedIncludeData,
      pubsubTopic: parsedPubsubTopic,
      contentTopics: parsedContentTopics,
      startTime: parsedStartTime,
      endTime: parsedEndTime,
      messageHashes: parsedHashes,
      paginationCursor: parsedCursor,
      paginationForward: parsedDirection,
      paginationLimit: parsedPagedSize,
    )
  )

# Simple type conversion. The "Option[Result[string, cstring]]"
# type is used by the nim-presto library.
proc toOpt(self: Option[Result[string, cstring]]): Option[string] =
  if not self.isSome() or self.get().value == "":
    return none(string)
  if self.isSome() and self.get().value != "":
    return some(self.get().value)

proc retrieveMsgsFromSelfNode(
    self: WakuNode, storeQuery: StoreQueryRequest
): Future[RestApiResponse] {.async.} =
  ## Performs a "store" request to the local node (self node.)
  ## Notice that this doesn't follow the regular store libp2p channel because a node
  ## it is not allowed to libp2p-dial a node to itself, by default.
  ##

  let storeResp = (await self.wakuStore.handleSelfStoreRequest(storeQuery)).valueOr:
    return RestApiResponse.internalServerError($error)

  let resp = RestApiResponse.jsonResponse(storeResp.toHex(), status = Http200).valueOr:
    const msg = "Error building the json respose"
    let e = $error
    error msg, error = e
    return RestApiResponse.internalServerError(fmt("{msg} [{e}]"))

  return resp

# Subscribes the rest handler to attend "/store/v1/messages" requests
proc installStoreApiHandlers*(
    router: var RestRouter,
    node: WakuNode,
    discHandler: Option[DiscoveryHandler] = none(DiscoveryHandler),
) =
  # Handles the store-query request according to the passed parameters
  router.api(MethodGet, "/store/v3/messages") do(
    peerAddr: Option[string],
    includeData: Option[string],
    pubsubTopic: Option[string],
    contentTopics: Option[string],
    startTime: Option[string],
    endTime: Option[string],
    hashes: Option[string],
    cursor: Option[string],
    ascending: Option[string],
    pageSize: Option[string]
  ) -> RestApiResponse:
    let peer = peerAddr.toOpt()

    debug "REST-GET /store/v3/messages ", peer_addr = $peer

    # All the GET parameters are URL-encoded (https://en.wikipedia.org/wiki/URL_encoding)
    # Example:
    # /store/v1/messages?peerAddr=%2Fip4%2F127.0.0.1%2Ftcp%2F60001%2Fp2p%2F16Uiu2HAmVFXtAfSj4EiR7mL2KvL4EE2wztuQgUSBoj2Jx2KeXFLN\&pubsubTopic=my-waku-topic

    # Parse the rest of the parameters and create a HistoryQuery
    let storeQuery = createStoreQuery(
      includeData.toOpt(),
      pubsubTopic.toOpt(),
      contentTopics.toOpt(),
      startTime.toOpt(),
      endTime.toOpt(),
      hashes.toOpt(),
      cursor.toOpt(),
      ascending.toOpt(),
      pageSize.toOpt(),
    ).valueOr:
      return RestApiResponse.badRequest(error)

    if peer.isNone() and not node.wakuStore.isNil():
      ## The user didn't specify a peer address and self-node is configured as a store node.
      ## In this case we assume that the user is willing to retrieve the messages stored by
      ## the local/self store node.
      return await node.retrieveMsgsFromSelfNode(storeQuery)

    # Parse the peer address parameter
    let parsedPeerAddr = parseUrlPeerAddr(peer).valueOr:
      return RestApiResponse.badRequest(error)

    let peerInfo = parsedPeerAddr.valueOr:
      node.peerManager.selectPeer(WakuStoreCodec).valueOr:
        let handler = discHandler.valueOr:
          return NoPeerNoDiscError

        let peerOp = (await handler()).valueOr:
          return RestApiResponse.internalServerError($error)

        peerOp.valueOr:
          return RestApiResponse.preconditionFailed(
            "No suitable service peer & none discovered"
          )

    return await node.performStoreQuery(storeQuery, peerInfo)
