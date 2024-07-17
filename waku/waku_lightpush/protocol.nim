{.push raises: [].}

import std/options, results, stew/byteutils, chronicles, chronos, metrics, bearssl/rand
import
  ../node/peer_manager/peer_manager,
  ../waku_core,
  ../waku_core/topics/sharding,
  ./common,
  ./rpc,
  ./rpc_codec,
  ./protocol_metrics,
  ../common/rate_limit/request_limiter

logScope:
  topics = "waku lightpush"

type WakuLightPush* = ref object of LPProtocol
  rng*: ref rand.HmacDrbgContext
  peerManager*: PeerManager
  pushHandler*: PushMessageHandler
  requestRateLimiter*: RequestRateLimiter
  sharding*: Sharding

proc handleRequest*(
    wl: WakuLightPush, peerId: PeerId, buffer: seq[byte]
): Future[LightPushResponse] {.async.} =
  let reqDecodeRes = LightpushRequest.decode(buffer)
  var isSuccess = false
  var pushResponse: LightpushResponse

  if reqDecodeRes.isErr():
    pushResponse = LightpushResponse(
      requestId: "N/A", # due to decode failure we don't know requestId
      statusCode: LightpushStatusCode.BAD_REQUEST.uint32,
      statusDesc: some(decodeRpcFailure & ": " & $reqDecodeRes.error),
    )
  else:
    let pushRequest = reqDecodeRes.get()

    let pubsubTopic = pushRequest.pubSubTopic.valueOr:
      wl.sharding.getShard(pushRequest.message.contentTopic).valueOr:
        let msg = "Autosharding error: " & error
        error "lightpush request handling error", msg = msg
        return LightpushResponse(
          requestId: pushRequest.requestId,
          statusCode: LightpushStatusCode.INTERNAL_SERVER_ERROR.uint32,
          statusDesc: some(msg),
        )

    waku_lightpush_messages.inc(labelValues = ["PushRequest"])

    notice "handling lightpush request",
      peer_id = peerId,
      requestId = pushRequest.requestId,
      pubsubTopic = pushRequest.pubsubTopic,
      msg_hash = pubsubTopic.computeMessageHash(pushRequest.message).to0xHex()

    let handleRes = await wl.pushHandler(peerId, pubsubTopic, pushRequest.message)

    isSuccess = handleRes.isOk()
    pushResponse = LightpushResponse(
      requestId: pushRequest.requestId,
      statusCode:
        if isSuccess:
          LightpushStatusCode.SUCCESS.uint32
        else:
          handleRes.error.code.uint32
      ,
      statusDesc:
        if isSuccess:
          none[string]()
        else:
          handleRes.error.desc
      ,
    )

  if not isSuccess:
    waku_lightpush_errors.inc(
      labelValues = [pushResponse.statusDesc.valueOr("unknown")]
    )
    error "failed to push message", error = pushResponse.statusDesc
  return pushResponse

proc initProtocolHandler(wl: WakuLightPush) =
  proc handle(conn: Connection, proto: string) {.async.} =
    var rpc: LightpushResponse
    wl.requestRateLimiter.checkUsageLimit(WakuLightPushCodec, conn):
      let buffer = await conn.readLp(DefaultMaxRpcSize)

      waku_service_network_bytes.inc(
        amount = buffer.len().int64, labelValues = [WakuLightPushCodec, "in"]
      )

      rpc = await handleRequest(wl, conn.peerId, buffer)
    do:
      debug "lightpush request rejected due rate limit exceeded",
        peerId = conn.peerId, limit = $wl.requestRateLimiter.setting

      rpc = static(
        LightpushResponse(
          ## We will not copy and decode RPC buffer from stream only for requestId
          ## in reject case as it is comparably too expensive and opens possible
          ## attack surface
          requestId: "N/A",
          statusCode: LightpushStatusCode.TOO_MANY_REQUESTS.uint32,
          statusDesc: some(TooManyRequestsMessage),
        )
      )

    await conn.writeLp(rpc.encode().buffer)

    ## For lightpush might not worth to measure outgoing trafic as it is only
    ## small respones about success/failure

  wl.handler = handle
  wl.codec = WakuLightPushCodec

proc new*(
    T: type WakuLightPush,
    peerManager: PeerManager,
    rng: ref rand.HmacDrbgContext,
    pushHandler: PushMessageHandler,
    sharding: Sharding,
    rateLimitSetting: Option[RateLimitSetting] = none[RateLimitSetting](),
): T =
  let wl = WakuLightPush(
    rng: rng,
    peerManager: peerManager,
    pushHandler: pushHandler,
    requestRateLimiter: newRequestRateLimiter(rateLimitSetting),
    sharding: sharding,
  )
  wl.initProtocolHandler()
  setServiceLimitMetric(WakuLightpushCodec, rateLimitSetting)
  return wl
