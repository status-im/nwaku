{.push raises: [].}

import std/options, results, chronicles, chronos, metrics, bearssl/rand
import
  ../node/peer_manager, ../utils/requests, ./protocol_metrics, ./common, ./rpc_codec

logScope:
  topics = "waku store client"

const DefaultPageSize*: uint = 20
  # A recommended default number of waku messages per page

type WakuStoreClient* = ref object
  peerManager: PeerManager
  rng: ref rand.HmacDrbgContext

proc new*(
    T: type WakuStoreClient, peerManager: PeerManager, rng: ref rand.HmacDrbgContext
): T {.gcsafe.} =
  WakuStoreClient(peerManager: peerManager, rng: rng)

proc sendStoreRequest(
    self: WakuStoreClient, request: StoreQueryRequest, connection: Connection
): Future[StoreQueryResult] {.async, gcsafe.} =
  var req = request

  if req.requestId == "":
    req.requestId = generateRequestId(self.rng)

  let writeRes = catch:
    await connection.writeLP(req.encode().buffer)
  if writeRes.isErr():
    return err(StoreError(kind: ErrorCode.BAD_REQUEST, cause: writeRes.error.msg))

  let readRes = catch:
    await connection.readLp(DefaultMaxRpcSize.int)

  let buf = readRes.valueOr:
    return err(StoreError(kind: ErrorCode.BAD_RESPONSE, cause: error.msg))

  let res = StoreQueryResponse.decode(buf).valueOr:
    waku_store_errors.inc(labelValues = [DecodeRpcFailure])
    return err(StoreError(kind: ErrorCode.BAD_RESPONSE, cause: DecodeRpcFailure))

  if res.statusCode != uint32(StatusCode.SUCCESS):
    waku_store_errors.inc(labelValues = [NoSuccessStatusCode])
    return err(StoreError.new(res.statusCode, res.statusDesc))

  return ok(res)

proc query*(
    self: WakuStoreClient, request: StoreQueryRequest, peer: RemotePeerInfo | PeerId
): Future[StoreQueryResult] {.async, gcsafe.} =
  if request.paginationCursor.isSome() and request.paginationCursor.get() == EmptyCursor:
    return err(StoreError(kind: ErrorCode.BAD_REQUEST, cause: "invalid cursor"))

  let connection = (await self.peerManager.dialPeer(peer, WakuStoreCodec)).valueOr:
    waku_store_errors.inc(labelValues = [DialFailure])

    return err(StoreError(kind: ErrorCode.PEER_DIAL_FAILURE, address: $peer))

  return await self.sendStoreRequest(request, connection)

proc queryToAny*(
    self: WakuStoreClient, request: StoreQueryRequest, peerId = none(PeerId)
): Future[StoreQueryResult] {.async.} =
  ## This proc is similar to the query one but in this case
  ## we don't specify a particular peer and instead we get it from peer manager

  if request.paginationCursor.isSome() and request.paginationCursor.get() == EmptyCursor:
    return err(StoreError(kind: ErrorCode.BAD_REQUEST, cause: "invalid cursor"))

  let peer = self.peerManager.selectPeer(WakuStoreCodec).valueOr:
    return err(StoreError(kind: BAD_RESPONSE, cause: "no service store peer connected"))

  let connection = (await self.peerManager.dialPeer(peer, WakuStoreCodec)).valueOr:
    waku_store_errors.inc(labelValues = [DialFailure])

    return err(StoreError(kind: ErrorCode.PEER_DIAL_FAILURE, address: $peer))

  return await self.sendStoreRequest(request, connection)
