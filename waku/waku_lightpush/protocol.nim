{.push raises: [].}

import std/options, results, stew/byteutils, chronicles, chronos, metrics, bearssl/rand
import
  ../node/peer_manager/peer_manager,
  ../waku_core,
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

proc handleRequest*(
    wl: WakuLightPush, peerId: PeerId, buffer: seq[byte]
): Future[PushRPC] {.async.} =
  let reqDecodeRes = PushRPC.decode(buffer)
  var
    isSuccess = false
    pushResponseInfo = ""
    requestId = ""

  if reqDecodeRes.isErr():
    pushResponseInfo = decodeRpcFailure & ": " & $reqDecodeRes.error
  elif reqDecodeRes.get().request.isNone():
    pushResponseInfo = emptyRequestBodyFailure
  else:
    let pushRpcRequest = reqDecodeRes.get()

    requestId = pushRpcRequest.requestId

    let
      request = pushRpcRequest.request

      pubSubTopic = request.get().pubSubTopic
      message = request.get().message

    waku_lightpush_messages.inc(labelValues = ["PushRequest"])

    notice "handling lightpush request",
      my_peer_id = wl.peerManager.switch.peerInfo.peerId,
      peer_id = peerId,
      requestId = requestId,
      pubsubTopic = pubsubTopic,
      msg_hash = pubsubTopic.computeMessageHash(message).to0xHex(),
      receivedTime = getNowInNanosecondTime()

    let handleRes = await wl.pushHandler(peerId, pubsubTopic, message)
    isSuccess = handleRes.isOk()
    pushResponseInfo = (if isSuccess: "OK" else: handleRes.error)

  if not isSuccess:
    waku_lightpush_errors.inc(labelValues = [pushResponseInfo])
    error "failed to push message", error = pushResponseInfo
  let response = PushResponse(isSuccess: isSuccess, info: some(pushResponseInfo))
  let rpc = PushRPC(requestId: requestId, response: some(response))
  return rpc

proc initProtocolHandler(wl: WakuLightPush) =
  proc handle(conn: Connection, proto: string) {.async.} =
    var rpc: PushRPC
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
        PushRPC(
          ## We will not copy and decode RPC buffer from stream only for requestId
          ## in reject case as it is comparably too expensive and opens possible
          ## attack surface
          requestId: "N/A",
          response:
            some(PushResponse(isSuccess: false, info: some(TooManyRequestsMessage))),
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
    rateLimitSetting: Option[RateLimitSetting] = none[RateLimitSetting](),
): T =
  let wl = WakuLightPush(
    rng: rng,
    peerManager: peerManager,
    pushHandler: pushHandler,
    requestRateLimiter: newRequestRateLimiter(rateLimitSetting),
  )
  wl.initProtocolHandler()
  setServiceLimitMetric(WakuLightpushCodec, rateLimitSetting)
  return wl
