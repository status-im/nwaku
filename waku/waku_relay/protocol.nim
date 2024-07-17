## Waku Relay module. Thin layer on top of GossipSub.
##
## See https://github.com/vacp2p/specs/blob/master/specs/waku/v2/waku-relay.md
## for spec.
{.push raises: [].}

import
  std/strformat,
  stew/byteutils,
  results,
  sequtils,
  chronos,
  chronicles,
  metrics,
  libp2p/multihash,
  libp2p/protocols/pubsub/pubsub,
  libp2p/protocols/pubsub/gossipsub,
  libp2p/protocols/pubsub/rpc/messages,
  libp2p/stream/connection,
  libp2p/switch
import
  ../waku_core, ./message_id, ./topic_health, ../node/delivery_monitor/publish_observer

from ../waku_core/codecs import WakuRelayCodec
export WakuRelayCodec

logScope:
  topics = "waku relay"

# see: https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md#overview-of-new-parameters
const TopicParameters = TopicParams(
  topicWeight: 1,

  # p1: favours peers already in the mesh
  timeInMeshWeight: 0.01,
  timeInMeshQuantum: 1.seconds,
  timeInMeshCap: 10.0,

  # p2: rewards fast peers
  firstMessageDeliveriesWeight: 1.0,
  firstMessageDeliveriesDecay: 0.5,
  firstMessageDeliveriesCap: 10.0,

  # p3: penalizes lazy peers. safe low value
  meshMessageDeliveriesWeight: 0.0,
  meshMessageDeliveriesDecay: 0.0,
  meshMessageDeliveriesCap: 0,
  meshMessageDeliveriesThreshold: 0,
  meshMessageDeliveriesWindow: 0.milliseconds,
  meshMessageDeliveriesActivation: 0.seconds,

  # p3b: tracks history of prunes
  meshFailurePenaltyWeight: 0.0,
  meshFailurePenaltyDecay: 0.0,

  # p4: penalizes invalid messages. highly penalize
  # peers sending wrong messages
  invalidMessageDeliveriesWeight: -100.0,
  invalidMessageDeliveriesDecay: 0.5,
)

declareCounter waku_relay_network_bytes,
  "total traffic per topic, distinct gross/net and direction",
  labels = ["topic", "type", "direction"]

# see: https://rfc.vac.dev/spec/29/#gossipsub-v10-parameters
const GossipsubParameters = GossipSubParams.init(
  pruneBackoff = chronos.minutes(1),
  unsubscribeBackoff = chronos.seconds(5),
  floodPublish = true,
  gossipFactor = 0.25,
  d = 6,
  dLow = 4,
  dHigh = 8,
  dScore = 6,
  dOut = 3,
  dLazy = 6,
  heartbeatInterval = chronos.seconds(1),
  historyLength = 6,
  historyGossip = 3,
  fanoutTTL = chronos.minutes(1),
  seenTTL = chronos.minutes(2),

  # no gossip is sent to peers below this score
  gossipThreshold = -100,

  # no self-published msgs are sent to peers below this score
  publishThreshold = -1000,

  # used to trigger disconnections + ignore peer if below this score
  graylistThreshold = -10000,

  # grafts better peers if the mesh median score drops below this. unset.
  opportunisticGraftThreshold = 0,

  # how often peer scoring is updated
  decayInterval = chronos.seconds(12),

  # below this we consider the parameter to be zero
  decayToZero = 0.01,

  # remember peer score during x after it disconnects
  retainScore = chronos.minutes(10),

  # p5: application specific, unset
  appSpecificWeight = 0.0,

  # p6: penalizes peers sharing more than threshold ips
  ipColocationFactorWeight = -50.0,
  ipColocationFactorThreshold = 5.0,

  # p7: penalizes bad behaviour (weight and decay)
  behaviourPenaltyWeight = -10.0,
  behaviourPenaltyDecay = 0.986,

  # triggers disconnections of bad peers aka score <graylistThreshold
  disconnectBadPeers = true,
)

type
  WakuRelayResult*[T] = Result[T, string]
  WakuRelayHandler* = proc(pubsubTopic: PubsubTopic, message: WakuMessage): Future[void] {.
    gcsafe, raises: [Defect]
  .}
  WakuValidatorHandler* = proc(
    pubsubTopic: PubsubTopic, message: WakuMessage
  ): Future[ValidationResult] {.gcsafe, raises: [Defect].}
  WakuRelay* = ref object of GossipSub
    # seq of tuples: the first entry in the tuple contains the validators are called for every topic
    # the second entry contains the error messages to be returned when the validator fails
    wakuValidators: seq[tuple[handler: WakuValidatorHandler, errorMessage: string]]
    # a map of validators to error messages to return when validation fails
    validatorInserted: Table[PubsubTopic, bool]
    publishObservers: seq[PublishObserver]
    topicsHealth*: Table[string, TopicHealth]
    onTopicHealthChange*: TopicHealthChangeHandler
    topicHealthLoopHandle*: Future[void]

proc initProtocolHandler(w: WakuRelay) =
  proc handler(conn: Connection, proto: string) {.async.} =
    ## main protocol handler that gets triggered on every
    ## connection for a protocol string
    ## e.g. ``/wakusub/0.0.1``, etc...
    debug "Incoming WakuRelay connection", connection = conn, protocol = proto

    try:
      await w.handleConn(conn, proto)
    except CancelledError:
      # This is top-level procedure which will work as separate task, so it
      # do not need to propogate CancelledError.
      error "Unexpected cancellation in relay handler",
        conn = conn, error = getCurrentExceptionMsg()
    except CatchableError:
      error "WakuRelay handler leaks an error",
        conn = conn, error = getCurrentExceptionMsg()

  # XXX: Handler hijack GossipSub here?
  w.handler = handler
  w.codec = WakuRelayCodec

proc logMessageInfo*(
    w: WakuRelay,
    remotePeerId: string,
    topic: string,
    msg_id_short: string,
    msg: WakuMessage,
    onRecv: bool,
) =
  let msg_hash = computeMessageHash(topic, msg).to0xHex()

  if onRecv:
    notice "received relay message",
      my_peer_id = w.switch.peerInfo.peerId,
      msg_hash = msg_hash,
      msg_id = msg_id_short,
      from_peer_id = remotePeerId,
      topic = topic,
      receivedTime = getNowInNanosecondTime(),
      payloadSizeBytes = msg.payload.len
  else:
    notice "sent relay message",
      my_peer_id = w.switch.peerInfo.peerId,
      msg_hash = msg_hash,
      msg_id = msg_id_short,
      to_peer_id = remotePeerId,
      topic = topic,
      sentTime = getNowInNanosecondTime(),
      payloadSizeBytes = msg.payload.len

proc initRelayObservers(w: WakuRelay) =
  proc decodeRpcMessageInfo(
      peer: PubSubPeer, msg: Message
  ): Result[
      tuple[msgId: string, topic: string, wakuMessage: WakuMessage, msgSize: int], void
  ] =
    let msg_id = w.msgIdProvider(msg).valueOr:
      warn "Error generating message id",
        my_peer_id = w.switch.peerInfo.peerId,
        from_peer_id = peer.peerId,
        pubsub_topic = msg.topic,
        error = $error
      return err()

    let msg_id_short = shortLog(msg_id)

    let wakuMessage = WakuMessage.decode(msg.data).valueOr:
      warn "Error decoding to Waku Message",
        my_peer_id = w.switch.peerInfo.peerId,
        msg_id = msg_id_short,
        from_peer_id = peer.peerId,
        pubsub_topic = msg.topic,
        error = $error
      return err()

    let msgSize = msg.data.len + msg.topic.len
    return ok((msg_id_short, msg.topic, wakuMessage, msgSize))

  proc updateMetrics(
      peer: PubSubPeer,
      pubsub_topic: string,
      msg: WakuMessage,
      msgSize: int,
      onRecv: bool,
  ) =
    if onRecv:
      waku_relay_network_bytes.inc(
        msgSize.int64, labelValues = [pubsub_topic, "gross", "in"]
      )
    else:
      # sent traffic can only be "net"
      # TODO: If we can measure unsuccessful sends would mean a possible distinction between gross/net
      waku_relay_network_bytes.inc(
        msgSize.int64, labelValues = [pubsub_topic, "net", "out"]
      )

  proc onRecv(peer: PubSubPeer, msgs: var RPCMsg) =
    for msg in msgs.messages:
      let (msg_id_short, topic, wakuMessage, msgSize) = decodeRpcMessageInfo(peer, msg).valueOr:
        continue
      # message receive log happens in onValidated observer as onRecv is called before checks
      updateMetrics(peer, topic, wakuMessage, msgSize, onRecv = true)
    discard

  proc onValidated(peer: PubSubPeer, msg: Message, msgId: MessageId) =
    let msg_id_short = shortLog(msgId)
    let wakuMessage = WakuMessage.decode(msg.data).valueOr:
      warn "onValidated: failed decoding to Waku Message",
        my_peer_id = w.switch.peerInfo.peerId,
        msg_id = msg_id_short,
        from_peer_id = peer.peerId,
        pubsub_topic = msg.topic,
        error = $error
      return

    logMessageInfo(
      w, shortLog(peer.peerId), msg.topic, msg_id_short, wakuMessage, onRecv = true
    )

  proc onAfterSent(peer: PubSubPeer, msgs: RPCMsg) =
    for msg in msgs.messages:
      let (msg_id_short, topic, wakuMessage, msgSize) = decodeRpcMessageInfo(peer, msg).valueOr:
        warn "onAfterSent: failed decoding RPC info",
          my_peer_id = w.switch.peerInfo.peerId, to_peer_id = peer.peerId
        continue
      logMessageInfo(
        w, shortLog(peer.peerId), topic, msg_id_short, wakuMessage, onRecv = false
      )
      updateMetrics(peer, topic, wakuMessage, msgSize, onRecv = false)

  let administrativeObserver =
    PubSubObserver(onRecv: onRecv, onAfterSent: onAfterSent, onValidated: onValidated)

  w.addObserver(administrativeObserver)

proc new*(
    T: type WakuRelay, switch: Switch, maxMessageSize = int(DefaultMaxWakuMessageSize)
): WakuRelayResult[T] =
  ## maxMessageSize: max num bytes that are allowed for the WakuMessage

  var w: WakuRelay
  try:
    w = WakuRelay.init(
      switch = switch,
      anonymize = true,
      verifySignature = false,
      sign = false,
      triggerSelf = true,
      msgIdProvider = defaultMessageIdProvider,
      maxMessageSize = maxMessageSize,
      parameters = GossipsubParameters,
    )

    procCall GossipSub(w).initPubSub()
    w.initProtocolHandler()
    w.initRelayObservers()
    w.topicsHealth = initTable[string, TopicHealth]()
  except InitializationError:
    return err("initialization error: " & getCurrentExceptionMsg())

  return ok(w)

proc addValidator*(
    w: WakuRelay, handler: WakuValidatorHandler, errorMessage: string = ""
) {.gcsafe.} =
  w.wakuValidators.add((handler, errorMessage))

proc addPublishObserver*(w: WakuRelay, obs: PublishObserver) =
  ## Observer when the api client performed a publish operation. This
  ## is initially aimed for bringing an additional layer of delivery reliability thanks
  ## to store
  w.publishObservers.add(obs)

proc addObserver*(w: WakuRelay, observer: PubSubObserver) {.gcsafe.} =
  ## Observes when a message is sent/received from the GossipSub PoV
  procCall GossipSub(w).addObserver(observer)

proc getDHigh*(T: type WakuRelay): int =
  return GossipsubParameters.dHigh

proc getNumPeersInMesh*(w: WakuRelay, pubsubTopic: PubsubTopic): Result[int, string] =
  ## Returns the number of peers in a mesh defined by the passed pubsub topic.
  ## The 'mesh' atribute is defined in the GossipSub ref object.

  if not w.mesh.hasKey(pubsubTopic):
    debug "getNumPeersInMesh - there is no mesh peer for the given pubsub topic",
      pubsubTopic = pubsubTopic
    return ok(0)

  let peersRes = catch:
    w.mesh[pubsubTopic]

  let peers: HashSet[PubSubPeer] = peersRes.valueOr:
    return
      err("getNumPeersInMesh - exception accessing " & pubsubTopic & ": " & error.msg)

  return ok(peers.len)

proc calculateTopicHealth(wakuRelay: WakuRelay, topic: string): TopicHealth =
  let numPeersInMesh = wakuRelay.getNumPeersInMesh(topic).valueOr:
    error "Could not calculate topic health", topic = topic, error = error
    return TopicHealth.UNHEALTHY

  if numPeersInMesh < 1:
    return TopicHealth.UNHEALTHY
  elif numPeersInMesh < wakuRelay.parameters.dLow:
    return TopicHealth.MINIMALLY_HEALTHY
  return TopicHealth.SUFFICIENTLY_HEALTHY

proc updateTopicsHealth(wakuRelay: WakuRelay) {.async.} =
  var futs = newSeq[Future[void]]()
  for topic in toSeq(wakuRelay.topics.keys):
    ## loop over all the topics I'm subscribed to
    let
      oldHealth = wakuRelay.topicsHealth.getOrDefault(topic)
      currentHealth = wakuRelay.calculateTopicHealth(topic)

    if oldHealth == currentHealth:
      continue

    wakuRelay.topicsHealth[topic] = currentHealth
    if not wakuRelay.onTopicHealthChange.isNil():
      let fut = wakuRelay.onTopicHealthChange(topic, currentHealth)
      if not fut.completed(): # Fast path for successful sync handlers
        futs.add(fut)

    if futs.len() > 0:
      # slow path - we have to wait for the handlers to complete
      try:
        futs = await allFinished(futs)
      except CancelledError:
        # check for errors in futures
        for fut in futs:
          if fut.failed:
            let err = fut.readError()
            warn "Error in health change handler", description = err.msg

proc topicsHealthLoop(wakuRelay: WakuRelay) {.async.} =
  while true:
    await wakuRelay.updateTopicsHealth()
    await sleepAsync(10.seconds)

method start*(w: WakuRelay) {.async, base.} =
  debug "start"
  await procCall GossipSub(w).start()
  w.topicHealthLoopHandle = w.topicsHealthLoop()

method stop*(w: WakuRelay) {.async, base.} =
  debug "stop"
  await procCall GossipSub(w).stop()
  if not w.topicHealthLoopHandle.isNil():
    await w.topicHealthLoopHandle.cancelAndWait()

proc isSubscribed*(w: WakuRelay, topic: PubsubTopic): bool =
  GossipSub(w).topics.hasKey(topic)

proc subscribedTopics*(w: WakuRelay): seq[PubsubTopic] =
  return toSeq(GossipSub(w).topics.keys())

proc generateOrderedValidator(w: WakuRelay): auto {.gcsafe.} =
  # rejects messages that are not WakuMessage
  let wrappedValidator = proc(
      pubsubTopic: string, message: messages.Message
  ): Future[ValidationResult] {.async.} =
    # can be optimized by checking if the message is a WakuMessage without allocating memory
    # see nim-libp2p protobuf library
    let msg = WakuMessage.decode(message.data).valueOr:
      error "protocol generateOrderedValidator reject decode error",
        pubsubTopic = pubsubTopic, error = $error
      return ValidationResult.Reject

    # now sequentially validate the message
    for (validator, errorMessage) in w.wakuValidators:
      let validatorRes = await validator(pubsubTopic, msg)

      if validatorRes != ValidationResult.Accept:
        let msgHash = computeMessageHash(pubsubTopic, msg).to0xHex()
        error "protocol generateOrderedValidator reject waku validator",
          msg_hash = msgHash,
          pubsubTopic = pubsubTopic,
          contentTopic = msg.contentTopic,
          validatorRes = validatorRes,
          error = errorMessage

        return validatorRes

    return ValidationResult.Accept

  return wrappedValidator

proc validateMessage*(
    w: WakuRelay, pubsubTopic: string, msg: WakuMessage
): Future[Result[void, string]] {.async.} =
  let messageSizeBytes = msg.encode().buffer.len
  let msgHash = computeMessageHash(pubsubTopic, msg).to0xHex()

  if messageSizeBytes > w.maxMessageSize:
    let message = fmt"Message size exceeded maximum of {w.maxMessageSize} bytes"
    error "too large Waku message",
      msg_hash = msgHash,
      error = message,
      messageSizeBytes = messageSizeBytes,
      maxMessageSize = w.maxMessageSize

    return err(message)

  for (validator, message) in w.wakuValidators:
    let validatorRes = await validator(pubsubTopic, msg)
    if validatorRes != ValidationResult.Accept:
      if message.len > 0:
        error "invalid Waku message", msg_hash = msgHash, error = message
        return err(message)
      else:
        ## This should never happen
        error "uncertain invalid Waku message", msg_hash = msgHash, error = message
        return err("validator failed")

  return ok()

proc subscribe*(
    w: WakuRelay, pubsubTopic: PubsubTopic, handler: WakuRelayHandler
): TopicHandler =
  debug "subscribe", pubsubTopic = pubsubTopic

  # We need to wrap the handler since gossipsub doesnt understand WakuMessage
  let wrappedHandler = proc(
      pubsubTopic: string, data: seq[byte]
  ): Future[void] {.gcsafe, raises: [].} =
    let decMsg = WakuMessage.decode(data)
    if decMsg.isErr():
      # fine if triggerSelf enabled, since validators are bypassed
      error "failed to decode WakuMessage, validator passed a wrong message",
        pubsubTopic = pubsubTopic, error = decMsg.error
      let fut = newFuture[void]()
      fut.complete()
      return fut
    else:
      # this subscription handler is called once for every validated message
      # that will be relayed, hence this is the place we can count net incoming traffic
      waku_relay_network_bytes.inc(
        data.len.int64 + pubsubTopic.len.int64, labelValues = [pubsubTopic, "net", "in"]
      )

      return handler(pubsubTopic, decMsg.get())

  # Add the ordered validator to the topic
  # This assumes that if `w.validatorInserted.hasKey(pubSubTopic) is true`, it contains the ordered validator.
  # Otherwise this might lead to unintended behaviour.
  if not w.validatorInserted.hasKey(pubSubTopic):
    procCall GossipSub(w).addValidator(pubSubTopic, w.generateOrderedValidator())
    w.validatorInserted[pubSubTopic] = true

  # set this topic parameters for scoring
  w.topicParams[pubsubTopic] = TopicParameters

  # subscribe to the topic with our wrapped handler
  procCall GossipSub(w).subscribe(pubsubTopic, wrappedHandler)

  return wrappedHandler

proc unsubscribeAll*(w: WakuRelay, pubsubTopic: PubsubTopic) =
  ## Unsubscribe all handlers on this pubsub topic

  debug "unsubscribe all", pubsubTopic = pubsubTopic

  procCall GossipSub(w).unsubscribeAll(pubsubTopic)
  w.validatorInserted.del(pubsubTopic)

proc unsubscribe*(w: WakuRelay, pubsubTopic: PubsubTopic, handler: TopicHandler) =
  ## Unsubscribe this handler on this pubsub topic

  debug "unsubscribe", pubsubTopic = pubsubTopic

  procCall GossipSub(w).unsubscribe(pubsubTopic, handler)

proc publish*(
    w: WakuRelay, pubsubTopic: PubsubTopic, message: WakuMessage
): Future[Result[int, ref PublishingError]] {.async.} =
  let data = message.encode().buffer

  let msgHash = computeMessageHash(pubsubTopic, message).to0xHex()
  notice "start publish Waku message", msg_hash = msgHash, pubsubTopic = pubsubTopic

  try:
    let relayedPeerCount = await procCall GossipSub(w).publish(pubsubTopic, data)

    if relayedPeerCount > 0:
      for obs in w.publishObservers:
        obs.onMessagePublished(pubSubTopic, message)

    return ok(relayedPeerCount)
  except PublishingError as ex:
    return err(ex)

proc getNumConnectedPeers*(
    w: WakuRelay, pubsubTopic: PubsubTopic
): Result[int, string] =
  ## Returns the number of connected peers and subscribed to the passed pubsub topic.
  ## The 'gossipsub' atribute is defined in the GossipSub ref object.

  if pubsubTopic == "":
    ## Return all the connected peers
    var numConnPeers = 0
    for k, v in w.gossipsub:
      numConnPeers.inc(v.len)
    return ok(numConnPeers)

  if not w.gossipsub.hasKey(pubsubTopic):
    return err(
      "getNumConnectedPeers - there is no gossipsub peer for the given pubsub topic: " &
        pubsubTopic
    )

  let peersRes = catch:
    w.gossipsub[pubsubTopic]

  let peers: HashSet[PubSubPeer] = peersRes.valueOr:
    return err(
      "getNumConnectedPeers - exception accessing " & pubsubTopic & ": " & error.msg
    )

  return ok(peers.len)

proc getSubscribedTopics*(w: WakuRelay): seq[PubsubTopic] =
  ## Returns a seq containing the current list of subscribed topics
  var topics: seq[PubsubTopic]
  for t in w.validatorInserted.keys():
    topics.add(t)
  return topics
