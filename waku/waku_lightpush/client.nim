{.push raises: [].}

import std/options, results, chronicles, chronos, metrics, bearssl/rand, stew/byteutils
import libp2p/peerid
import
  ../waku_core/peers,
  ../node/peer_manager,
  ../node/delivery_monitor/publish_observer,
  ../utils/requests,
  ../waku_core,
  ./common,
  ./protocol_metrics,
  ./rpc,
  ./rpc_codec

logScope:
  topics = "waku lightpush v2 client"

type WakuLightPushClient* = ref object
  peerManager*: PeerManager
  rng*: ref rand.HmacDrbgContext
  publishObservers: seq[PublishObserver]

proc new*(
    T: type WakuLightPushClient, peerManager: PeerManager, rng: ref rand.HmacDrbgContext
): T =
  WakuLightPushClient(peerManager: peerManager, rng: rng)

proc addPublishObserver*(wl: WakuLightPushClient, obs: PublishObserver) =
  wl.publishObservers.add(obs)

proc sendPushRequest(
    wl: WakuLightPushClient, req: LightPushRequest, peer: PeerId | RemotePeerInfo
): Future[WakuLightPushResult] {.async, gcsafe.} =
  let connection = (await wl.peerManager.dialPeer(peer, WakuLightPushCodec)).valueOr:
    waku_lightpush_errors.inc(labelValues = [dialFailure])
    return lighpushErrorResult(
      NO_PEERS_TO_RELAY, dialFailure & ": " & $peer & " is not accessible"
    )

  await connection.writeLP(req.encode().buffer)

  var buffer: seq[byte]
  try:
    buffer = await connection.readLp(DefaultMaxRpcSize.int)
  except LPStreamRemoteClosedError:
    error "Failed to read responose from peer", exception = getCurrentExceptionMsg()
    return
      lightpushResultInternalError("Exception reading: " & getCurrentExceptionMsg())

  let response = LightpushResponse.decode(buffer).valueOr:
    error "failed to decode response"
    waku_lightpush_errors.inc(labelValues = [decodeRpcFailure])
    return lightpushResultInternalError(decodeRpcFailure)

  if response.requestId != req.requestId and
      response.statusCode != TOO_MANY_REQUESTS.uint32:
    error "response failure, requestId mismatch",
      requestId = req.requestId, responseReqeustId = response.requestId
    return lightpushResultInternalError("response failure, requestId mismatch")

  return toPushResult(response)

proc publish*(
    wl: WakuLightPushClient,
    pubSubTopic: Option[PubsubTopic] = none(PubsubTopic),
    message: WakuMessage,
    peer: PeerId | RemotePeerInfo,
): Future[WakuLightPushResult] {.async, gcsafe.} =
  when peer is PeerId:
    info "publish",
      peerId = shortLog(peer),
      msg_hash = computeMessageHash(pubsubTopic.get(""), message).to0xHex
  else:
    info "publish",
      peerId = shortLog(peer.peerId),
      msg_hash = computeMessageHash(pubsubTopic.get(""), message).to0xHex

  let pushRequest = LightpushRequest(
    requestId: generateRequestId(wl.rng), pubSubTopic: pubSubTopic, message: message
  )
  let publishedCount = ?await wl.sendPushRequest(pushRequest, peer)

  for obs in wl.publishObservers:
    obs.onMessagePublished(pubSubTopic.get(""), message)

  return lightpushSuccessResult(publishedCount)

proc publishToAny*(
    wl: WakuLightPushClient, pubSubTopic: PubsubTopic, message: WakuMessage
): Future[WakuLightPushResult] {.async, gcsafe.} =
  ## This proc is similar to the publish one but in this case
  ## we don't specify a particular peer and instead we get it from peer manager

  info "publishToAny", msg_hash = computeMessageHash(pubsubTopic, message).to0xHex

  let peer = wl.peerManager.selectPeer(WakuLightPushCodec).valueOr:
    # TODO: check if it is matches the situation
    return lighpushErrorResult(NO_PEERS_TO_RELAY, "no suitable remote peers")

  let pushRequest = LightpushRequest(
    requestId: generateRequestId(wl.rng),
    pubSubTopic: some(pubSubTopic),
    message: message,
  )
  let publishedCount = ?await wl.sendPushRequest(pushRequest, peer)

  for obs in wl.publishObservers:
    obs.onMessagePublished(pubSubTopic, message)

  return lightpushSuccessResult(publishedCount)
