## Waku Filter protocol for subscribing and filtering messages

{.push raises: [].}

import
  std/[options, sequtils, sets, strutils, tables],
  stew/byteutils,
  chronicles,
  chronos,
  libp2p/peerid,
  libp2p/protocols/protocol,
  libp2p/protocols/pubsub/timedcache
import
  ../node/peer_manager,
  ../waku_core,
  ../common/rate_limit/per_peer_limiter,
  ./[common, protocol_metrics, rpc_codec, rpc, subscriptions]

logScope:
  topics = "waku filter"

const MaxContentTopicsPerRequest* = 100

type WakuFilter* = ref object of LPProtocol
  subscriptions*: FilterSubscriptions
    # a mapping of peer ids to a sequence of filter criteria
  peerManager: PeerManager
  messageCache: TimedCache[string]
  peerRequestRateLimiter*: PerPeerRateLimiter
  subscriptionsManagerFut: Future[void]
  peerConnections: Table[PeerId, Connection]

proc pingSubscriber(wf: WakuFilter, peerId: PeerID): FilterSubscribeResult =
  debug "pinging subscriber", peerId = peerId

  if not wf.subscriptions.isSubscribed(peerId):
    error "pinging peer has no subscriptions", peerId = peerId
    return err(FilterSubscribeError.notFound())

  wf.subscriptions.refreshSubscription(peerId)

  ok()

proc setSubscriptionTimeout*(wf: WakuFilter, newTimeout: Duration) =
  wf.subscriptions.setSubscriptionTimeout(newTimeout)

proc subscribe(
    wf: WakuFilter,
    peerId: PeerID,
    pubsubTopic: Option[PubsubTopic],
    contentTopics: seq[ContentTopic],
): Future[FilterSubscribeResult] {.async.} =
  # TODO: check if this condition is valid???
  if pubsubTopic.isNone() or contentTopics.len == 0:
    error "pubsubTopic and contentTopics must be specified", peerId = peerId
    return err(
      FilterSubscribeError.badRequest("pubsubTopic and contentTopics must be specified")
    )

  if contentTopics.len > MaxContentTopicsPerRequest:
    error "exceeds maximum content topics", peerId = peerId
    return err(
      FilterSubscribeError.badRequest(
        "exceeds maximum content topics: " & $MaxContentTopicsPerRequest
      )
    )

  let filterCriteria = toHashSet(contentTopics.mapIt((pubsubTopic.get(), it)))

  debug "subscribing peer to filter criteria",
    peerId = peerId, filterCriteria = filterCriteria

  (await wf.subscriptions.addSubscription(peerId, filterCriteria)).isOkOr:
    return err(FilterSubscribeError.serviceUnavailable(error))

  debug "correct subscription", peerId = peerId

  ok()

proc unsubscribe(
    wf: WakuFilter,
    peerId: PeerID,
    pubsubTopic: Option[PubsubTopic],
    contentTopics: seq[ContentTopic],
): FilterSubscribeResult =
  if pubsubTopic.isNone() or contentTopics.len == 0:
    error "pubsubTopic and contentTopics must be specified", peerId = peerId
    return err(
      FilterSubscribeError.badRequest("pubsubTopic and contentTopics must be specified")
    )

  if contentTopics.len > MaxContentTopicsPerRequest:
    error "exceeds maximum content topics", peerId = peerId
    return err(
      FilterSubscribeError.badRequest(
        "exceeds maximum content topics: " & $MaxContentTopicsPerRequest
      )
    )

  let filterCriteria = toHashSet(contentTopics.mapIt((pubsubTopic.get(), it)))

  debug "unsubscribing peer from filter criteria",
    peerId = peerId, filterCriteria = filterCriteria

  wf.subscriptions.removeSubscription(peerId, filterCriteria).isOkOr:
    error "failed to remove subscription", error = $error
    return err(FilterSubscribeError.notFound())

  ## Note: do not remove from peerRequestRateLimiter to prevent trick with subscribe/unsubscribe loop
  ## We remove only if peerManager removes the peer
  debug "correct unsubscription", peerId = peerId

  ok()

proc unsubscribeAll(
    wf: WakuFilter, peerId: PeerID
): Future[FilterSubscribeResult] {.async.} =
  if not wf.subscriptions.isSubscribed(peerId):
    debug "unsubscribing peer has no subscriptions", peerId = peerId
    return err(FilterSubscribeError.notFound())

  debug "removing peer subscription", peerId = peerId
  await wf.subscriptions.removePeer(peerId)
  wf.subscriptions.cleanUp()

  ok()

proc handleSubscribeRequest*(
    wf: WakuFilter, peerId: PeerId, request: FilterSubscribeRequest
): Future[FilterSubscribeResponse] {.async.} =
  info "received filter subscribe request", peerId = peerId, request = request
  waku_filter_requests.inc(labelValues = [$request.filterSubscribeType])

  var subscribeResult: FilterSubscribeResult

  let requestStartTime = Moment.now()

  block:
    ## Handle subscribe request
    case request.filterSubscribeType
    of FilterSubscribeType.SUBSCRIBER_PING:
      subscribeResult = wf.pingSubscriber(peerId)
    of FilterSubscribeType.SUBSCRIBE:
      subscribeResult =
        await wf.subscribe(peerId, request.pubsubTopic, request.contentTopics)
    of FilterSubscribeType.UNSUBSCRIBE:
      subscribeResult =
        wf.unsubscribe(peerId, request.pubsubTopic, request.contentTopics)
    of FilterSubscribeType.UNSUBSCRIBE_ALL:
      subscribeResult = await wf.unsubscribeAll(peerId)

  let
    requestDuration = Moment.now() - requestStartTime
    requestDurationSec = requestDuration.milliseconds.float / 1000
      # Duration in seconds with millisecond precision floating point
  waku_filter_request_duration_seconds.observe(
    requestDurationSec, labelValues = [$request.filterSubscribeType]
  )

  if subscribeResult.isErr():
    error "subscription request error", peerId = shortLog(peerId), request = request
    return FilterSubscribeResponse(
      requestId: request.requestId,
      statusCode: subscribeResult.error.kind.uint32,
      statusDesc: some($subscribeResult.error),
    )
  else:
    return FilterSubscribeResponse.ok(request.requestId)

proc pushToPeer(
    wf: WakuFilter, peerId: PeerId, buffer: seq[byte]
): Future[Result[void, string]] {.async.} =
  debug "pushing message to subscribed peer", peerId = shortLog(peerId)

  let stream = (
    await wf.peerManager.getStreamByPeerIdAndProtocol(peerId, WakuFilterPushCodec)
  ).valueOr:
    error "pushToPeer failed", error
    return err("pushToPeer failed: " & $error)

  await stream.writeLp(buffer)

  debug "published successful", peerId = shortLog(peerId), stream
  waku_service_network_bytes.inc(
    amount = buffer.len().int64, labelValues = [WakuFilterPushCodec, "out"]
  )

  return ok()

proc pushToPeers(
    wf: WakuFilter, peers: seq[PeerId], messagePush: MessagePush
) {.async.} =
  let targetPeerIds = peers.mapIt(shortLog(it))
  let msgHash =
    messagePush.pubsubTopic.computeMessageHash(messagePush.wakuMessage).to0xHex()

  ## it's also refresh expire of msghash, that's why update cache every time, even if it has a value.
  if wf.messageCache.put(msgHash, Moment.now()):
    error "duplicate message found, not-pushing message to subscribed peers",
      pubsubTopic = messagePush.pubsubTopic,
      contentTopic = messagePush.wakuMessage.contentTopic,
      payload = shortLog(messagePush.wakuMessage.payload),
      target_peer_ids = targetPeerIds,
      msg_hash = msgHash
  else:
    notice "pushing message to subscribed peers",
      pubsubTopic = messagePush.pubsubTopic,
      contentTopic = messagePush.wakuMessage.contentTopic,
      payload = shortLog(messagePush.wakuMessage.payload),
      target_peer_ids = targetPeerIds,
      msg_hash = msgHash

    let bufferToPublish = messagePush.encode().buffer
    var pushFuts: seq[Future[Result[void, string]]]

    for peerId in peers:
      let pushFut = wf.pushToPeer(peerId, bufferToPublish)
      pushFuts.add(pushFut)

    await allFutures(pushFuts)

proc maintainSubscriptions*(wf: WakuFilter) {.async.} =
  debug "maintaining subscriptions"

  ## Remove subscriptions for peers that have been removed from peer store
  var peersToRemove: seq[PeerId]
  for peerId in wf.subscriptions.peersSubscribed.keys:
    if not wf.peerManager.wakuPeerStore.hasPeer(peerId, WakuFilterPushCodec):
      debug "peer has been removed from peer store, we will remove subscription",
        peerId = peerId
      peersToRemove.add(peerId)

  if peersToRemove.len > 0:
    await wf.subscriptions.removePeers(peersToRemove)
    wf.peerRequestRateLimiter.unregister(peersToRemove)

  wf.subscriptions.cleanUp()

  ## Periodic report of number of subscriptions
  waku_filter_subscriptions.set(wf.subscriptions.peersSubscribed.len.float64)

const MessagePushTimeout = 20.seconds
proc handleMessage*(
    wf: WakuFilter, pubsubTopic: PubsubTopic, message: WakuMessage
) {.async.} =
  let msgHash = computeMessageHash(pubsubTopic, message).to0xHex()

  debug "handling message", pubsubTopic = pubsubTopic, msg_hash = msgHash

  let handleMessageStartTime = Moment.now()

  block:
    ## Find subscribers and push message to them
    let subscribedPeers =
      wf.subscriptions.findSubscribedPeers(pubsubTopic, message.contentTopic)
    if subscribedPeers.len == 0:
      error "no subscribed peers found",
        pubsubTopic = pubsubTopic,
        contentTopic = message.contentTopic,
        msg_hash = msgHash
      return

    let messagePush = MessagePush(pubsubTopic: pubsubTopic, wakuMessage: message)

    if not await wf.pushToPeers(subscribedPeers, messagePush).withTimeout(
      MessagePushTimeout
    ):
      error "timed out pushing message to peers",
        pubsubTopic = pubsubTopic,
        contentTopic = message.contentTopic,
        msg_hash = msgHash,
        numPeers = subscribedPeers.len,
        target_peer_ids = subscribedPeers.mapIt(shortLog(it))
      waku_filter_errors.inc(labelValues = [pushTimeoutFailure])
    else:
      notice "pushed message succesfully to all subscribers",
        pubsubTopic = pubsubTopic,
        contentTopic = message.contentTopic,
        msg_hash = msgHash,
        numPeers = subscribedPeers.len,
        target_peer_ids = subscribedPeers.mapIt(shortLog(it))

  let
    handleMessageDuration = Moment.now() - handleMessageStartTime
    handleMessageDurationSec = handleMessageDuration.milliseconds.float / 1000
      # Duration in seconds with millisecond precision floating point
  waku_filter_handle_message_duration_seconds.observe(handleMessageDurationSec)

proc initProtocolHandler(wf: WakuFilter) =
  proc handler(conn: Connection, proto: string) {.async.} =
    debug "filter subscribe request handler triggered",
      peerId = shortLog(conn.peerId), conn

    var response: FilterSubscribeResponse

    wf.peerRequestRateLimiter.checkUsageLimit(WakuFilterSubscribeCodec, conn):
      let buf = await conn.readLp(int(DefaultMaxSubscribeSize))

      waku_service_network_bytes.inc(
        amount = buf.len().int64, labelValues = [WakuFilterSubscribeCodec, "in"]
      )

      let decodeRes = FilterSubscribeRequest.decode(buf)
      if decodeRes.isErr():
        error "Failed to decode filter subscribe request",
          peer_id = conn.peerId, err = decodeRes.error
        waku_filter_errors.inc(labelValues = [decodeRpcFailure])
        return

      let request = decodeRes.value #TODO: toAPI() split here

      response = await wf.handleSubscribeRequest(conn.peerId, request)

      debug "sending filter subscribe response",
        peer_id = shortLog(conn.peerId), response = response
    do:
      debug "filter request rejected due rate limit exceeded",
        peerId = shortLog(conn.peerId), limit = $wf.peerRequestRateLimiter.setting
      response = FilterSubscribeResponse(
        requestId: "N/A",
        statusCode: FilterSubscribeErrorKind.TOO_MANY_REQUESTS.uint32,
        statusDesc: some("filter request rejected due rate limit exceeded"),
      )

    await conn.writeLp(response.encode().buffer) #TODO: toRPC() separation here
    return

  wf.handler = handler
  wf.codec = WakuFilterSubscribeCodec

proc onPeerEventHandler(wf: WakuFilter, peerId: PeerId, event: PeerEvent) {.async.} =
  ## These events are dispatched nim-libp2p, triggerPeerEvents proc
  case event.kind
  of Left:
    ## Drop the previous known connection reference
    wf.peerConnections.del(peerId)
  else:
    discard

proc new*(
    T: type WakuFilter,
    peerManager: PeerManager,
    subscriptionTimeout: Duration = DefaultSubscriptionTimeToLiveSec,
    maxFilterPeers: uint32 = MaxFilterPeers,
    maxFilterCriteriaPerPeer: uint32 = MaxFilterCriteriaPerPeer,
    messageCacheTTL: Duration = MessageCacheTTL,
    rateLimitSetting: Option[RateLimitSetting] = none[RateLimitSetting](),
): T =
  let wf = WakuFilter(
    subscriptions: FilterSubscriptions.new(
      subscriptionTimeout, maxFilterPeers, maxFilterCriteriaPerPeer
    ),
    peerManager: peerManager,
    messageCache: init(TimedCache[string], messageCacheTTL),
    peerRequestRateLimiter: PerPeerRateLimiter(setting: rateLimitSetting),
  )

  proc peerEventHandler(peerId: PeerId, event: PeerEvent): Future[void] {.gcsafe.} =
    wf.onPeerEventHandler(peerId, event)

  peerManager.addExtPeerEventHandler(peerEventHandler, PeerEventKind.Left)

  wf.initProtocolHandler()
  setServiceLimitMetric(WakuFilterSubscribeCodec, rateLimitSetting)
  return wf

proc periodicSubscriptionsMaintenance(wf: WakuFilter) {.async.} =
  const MaintainSubscriptionsInterval = 1.minutes
  debug "starting to maintain subscriptions"
  while true:
    await wf.maintainSubscriptions()
    await sleepAsync(MaintainSubscriptionsInterval)

proc start*(wf: WakuFilter) {.async.} =
  debug "starting filter protocol"
  await procCall LPProtocol(wf).start()
  wf.subscriptionsManagerFut = wf.periodicSubscriptionsMaintenance()

proc stop*(wf: WakuFilter) {.async.} =
  debug "stopping filter protocol"
  await wf.subscriptionsManagerFut.cancelAndWait()
  await procCall LPProtocol(wf).stop()
