{.push raises: [].}

import
  std/[hashes, options, sugar, tables, strutils, sequtils, os, net],
  chronos,
  chronicles,
  metrics,
  results,
  stew/byteutils,
  eth/keys,
  nimcrypto,
  bearssl/rand,
  eth/p2p/discoveryv5/enr,
  libp2p/crypto/crypto,
  libp2p/protocols/ping,
  libp2p/protocols/pubsub/gossipsub,
  libp2p/protocols/pubsub/rpc/messages,
  libp2p/protocols/connectivity/autonat/client,
  libp2p/protocols/connectivity/autonat/service,
  libp2p/builders,
  libp2p/transports/transport,
  libp2p/transports/tcptransport,
  libp2p/transports/wstransport
import
  ../waku_core,
  ../waku_core/topics/sharding,
  ../waku_relay,
  ../waku_archive,
  ../waku_archive_legacy,
  ../waku_store_legacy/protocol as legacy_store,
  ../waku_store_legacy/client as legacy_store_client,
  ../waku_store_legacy/common as legacy_store_common,
  ../waku_store/protocol as store,
  ../waku_store/client as store_client,
  ../waku_store/common as store_common,
  ../waku_store/resume,
  ../waku_filter_v2,
  ../waku_filter_v2/client as filter_client,
  ../waku_filter_v2/subscriptions as filter_subscriptions,
  ../waku_metadata,
  ../waku_rendezvous/protocol,
  ../waku_lightpush/client as lightpush_client,
  ../waku_lightpush/common,
  ../waku_lightpush/protocol,
  ../waku_lightpush/self_req_handler,
  ../waku_lightpush/callbacks,
  ../waku_enr,
  ../waku_peer_exchange,
  ../waku_rln_relay,
  ./config,
  ./peer_manager,
  ../common/rate_limit/setting,
  ../discovery/autonat_service

declarePublicCounter waku_node_messages, "number of messages received", ["type"]
declarePublicHistogram waku_histogram_message_size,
  "message size histogram in kB",
  buckets = [
    0.0, 1.0, 3.0, 5.0, 15.0, 50.0, 75.0, 100.0, 125.0, 150.0, 500.0, 700.0, 1000.0, Inf
  ]

declarePublicGauge waku_version,
  "Waku version info (in git describe format)", ["version"]
declarePublicGauge waku_node_errors, "number of wakunode errors", ["type"]
declarePublicGauge waku_lightpush_peers, "number of lightpush peers"
declarePublicGauge waku_filter_peers, "number of filter peers"
declarePublicGauge waku_store_peers, "number of store peers"
declarePublicGauge waku_px_peers,
  "number of peers (in the node's peerManager) supporting the peer exchange protocol"

logScope:
  topics = "waku node"

# TODO: Move to application instance (e.g., `WakuNode2`)
# Git version in git describe format (defined compile time)
const git_version* {.strdefine.} = "n/a"

# Default clientId
const clientId* = "Nimbus Waku v2 node"

const WakuNodeVersionString* = "version / git commit hash: " & git_version

# key and crypto modules different
type
  # TODO: Move to application instance (e.g., `WakuNode2`)
  WakuInfo* = object # NOTE One for simplicity, can extend later as needed
    listenAddresses*: seq[string]
    enrUri*: string #multiaddrStrings*: seq[string]

  # NOTE based on Eth2Node in NBC eth2_network.nim
  WakuNode* = ref object
    peerManager*: PeerManager
    switch*: Switch
    wakuRelay*: WakuRelay
    wakuArchive*: waku_archive.WakuArchive
    wakuLegacyArchive*: waku_archive_legacy.WakuArchive
    wakuLegacyStore*: legacy_store.WakuStore
    wakuLegacyStoreClient*: legacy_store_client.WakuStoreClient
    wakuStore*: store.WakuStore
    wakuStoreClient*: store_client.WakuStoreClient
    wakuStoreResume*: StoreResume
    wakuFilter*: waku_filter_v2.WakuFilter
    wakuFilterClient*: filter_client.WakuFilterClient
    wakuRlnRelay*: WakuRLNRelay
    wakuLightPush*: WakuLightPush
    wakuLightpushClient*: WakuLightPushClient
    wakuPeerExchange*: WakuPeerExchange
    wakuMetadata*: WakuMetadata
    wakuSharding*: Sharding
    enr*: enr.Record
    libp2pPing*: Ping
    rng*: ref rand.HmacDrbgContext
    wakuRendezvous*: WakuRendezVous
    announcedAddresses*: seq[MultiAddress]
    started*: bool # Indicates that node has started listening
    topicSubscriptionQueue*: AsyncEventQueue[SubscriptionEvent]
    contentTopicHandlers: Table[ContentTopic, TopicHandler]
    rateLimitSettings*: ProtocolRateLimitSettings

proc new*(
    T: type WakuNode,
    netConfig: NetConfig,
    enr: enr.Record,
    switch: Switch,
    peerManager: PeerManager,
    # TODO: make this argument required after tests are updated
    rng: ref HmacDrbgContext = crypto.newRng(),
): T {.raises: [Defect, LPError, IOError, TLSStreamProtocolError].} =
  ## Creates a Waku Node instance.

  info "Initializing networking", addrs = $netConfig.announcedAddresses

  let queue = newAsyncEventQueue[SubscriptionEvent](0)

  let node = WakuNode(
    peerManager: peerManager,
    switch: switch,
    rng: rng,
    enr: enr,
    announcedAddresses: netConfig.announcedAddresses,
    topicSubscriptionQueue: queue,
    rateLimitSettings: DefaultProtocolRateLimit,
  )

  return node

proc peerInfo*(node: WakuNode): PeerInfo =
  node.switch.peerInfo

proc peerId*(node: WakuNode): PeerId =
  node.peerInfo.peerId

# TODO: Move to application instance (e.g., `WakuNode2`)
# TODO: Extend with more relevant info: topics, peers, memory usage, online time, etc
proc info*(node: WakuNode): WakuInfo =
  ## Returns information about the Node, such as what multiaddress it can be reached at.

  let peerInfo = node.switch.peerInfo

  var listenStr: seq[string]
  for address in node.announcedAddresses:
    var fulladdr = $address & "/p2p/" & $peerInfo.peerId
    listenStr &= fulladdr
  let enrUri = node.enr.toUri()
  let wakuInfo = WakuInfo(listenAddresses: listenStr, enrUri: enrUri)
  return wakuInfo

proc connectToNodes*(
    node: WakuNode, nodes: seq[RemotePeerInfo] | seq[string], source = "api"
) {.async.} =
  ## `source` indicates source of node addrs (static config, api call, discovery, etc)
  # NOTE Connects to the node without a give protocol, which automatically creates streams for relay
  await peer_manager.connectToNodes(node.peerManager, nodes, source = source)

proc disconnectNode*(node: WakuNode, remotePeer: RemotePeerInfo) {.async.} =
  await peer_manager.disconnectNode(node.peerManager, remotePeer)

## Waku Metadata

proc mountMetadata*(node: WakuNode, clusterId: uint32): Result[void, string] =
  if not node.wakuMetadata.isNil():
    return err("Waku metadata already mounted, skipping")

  let metadata = WakuMetadata.new(clusterId, node.enr, node.topicSubscriptionQueue)

  node.wakuMetadata = metadata
  node.peerManager.wakuMetadata = metadata

  let catchRes = catch:
    node.switch.mount(node.wakuMetadata, protocolMatcher(WakuMetadataCodec))
  if catchRes.isErr():
    return err(catchRes.error.msg)

  return ok()

## Waku Sharding
proc mountSharding*(
    node: WakuNode, clusterId: uint16, shardCount: uint32
): Result[void, string] =
  info "mounting sharding", clusterId = clusterId, shardCount = shardCount
  node.wakuSharding = Sharding(clusterId: clusterId, shardCountGenZero: shardCount)
  return ok()

## Waku relay

proc registerRelayDefaultHandler(node: WakuNode, topic: PubsubTopic) =
  if node.wakuRelay.isSubscribed(topic):
    return

  proc traceHandler(topic: PubsubTopic, msg: WakuMessage) {.async, gcsafe.} =
    let msg_hash = topic.computeMessageHash(msg).to0xHex()
    let msgSizeKB = msg.payload.len / 1000

    waku_node_messages.inc(labelValues = ["relay"])
    waku_histogram_message_size.observe(msgSizeKB)

  proc filterHandler(topic: PubsubTopic, msg: WakuMessage) {.async, gcsafe.} =
    if node.wakuFilter.isNil():
      return

    await node.wakuFilter.handleMessage(topic, msg)

  proc archiveHandler(topic: PubsubTopic, msg: WakuMessage) {.async, gcsafe.} =
    if not node.wakuLegacyArchive.isNil():
      ## we try to store with legacy archive
      await node.wakuLegacyArchive.handleMessage(topic, msg)
      return

    if node.wakuArchive.isNil():
      return

    await node.wakuArchive.handleMessage(topic, msg)

  let defaultHandler = proc(
      topic: PubsubTopic, msg: WakuMessage
  ): Future[void] {.async, gcsafe.} =
    await traceHandler(topic, msg)
    await filterHandler(topic, msg)
    await archiveHandler(topic, msg)

  discard node.wakuRelay.subscribe(topic, defaultHandler)

proc subscribe*(
    node: WakuNode, subscription: SubscriptionEvent, handler = none(WakuRelayHandler)
) =
  ## Subscribes to a PubSub or Content topic. Triggers handler when receiving messages on
  ## this topic. WakuRelayHandler is a method that takes a topic and a Waku message.

  if node.wakuRelay.isNil():
    error "Invalid API call to `subscribe`. WakuRelay not mounted."
    return

  let (pubsubTopic, contentTopicOp) =
    case subscription.kind
    of ContentSub:
      let shard = node.wakuSharding.getShard((subscription.topic)).valueOr:
        error "Autosharding error", error = error
        return

      ($shard, some(subscription.topic))
    of PubsubSub:
      (subscription.topic, none(ContentTopic))
    else:
      return

  if contentTopicOp.isSome() and node.contentTopicHandlers.hasKey(contentTopicOp.get()):
    error "Invalid API call to `subscribe`. Was already subscribed"
    return

  node.topicSubscriptionQueue.emit((kind: PubsubSub, topic: pubsubTopic))
  node.registerRelayDefaultHandler(pubsubTopic)

  if handler.isSome():
    let wrappedHandler = node.wakuRelay.subscribe(pubsubTopic, handler.get())

    if contentTopicOp.isSome():
      node.contentTopicHandlers[contentTopicOp.get()] = wrappedHandler

proc unsubscribe*(node: WakuNode, subscription: SubscriptionEvent) =
  ## Unsubscribes from a specific PubSub or Content topic.

  if node.wakuRelay.isNil():
    error "Invalid API call to `unsubscribe`. WakuRelay not mounted."
    return

  let (pubsubTopic, contentTopicOp) =
    case subscription.kind
    of ContentUnsub:
      let shard = node.wakuSharding.getShard((subscription.topic)).valueOr:
        error "Autosharding error", error = error
        return

      ($shard, some(subscription.topic))
    of PubsubUnsub:
      (subscription.topic, none(ContentTopic))
    else:
      return

  if not node.wakuRelay.isSubscribed(pubsubTopic):
    error "Invalid API call to `unsubscribe`. Was not subscribed"
    return

  if contentTopicOp.isSome():
    # Remove this handler only
    var handler: TopicHandler
    if node.contentTopicHandlers.pop(contentTopicOp.get(), handler):
      debug "unsubscribe", contentTopic = contentTopicOp.get()
      node.wakuRelay.unsubscribe(pubsubTopic, handler)

  if contentTopicOp.isNone() or node.wakuRelay.topics.getOrDefault(pubsubTopic).len == 1:
    # Remove all handlers
    debug "unsubscribe", pubsubTopic = pubsubTopic
    node.wakuRelay.unsubscribeAll(pubsubTopic)
    node.topicSubscriptionQueue.emit((kind: PubsubUnsub, topic: pubsubTopic))

proc publish*(
    node: WakuNode, pubsubTopicOp: Option[PubsubTopic], message: WakuMessage
): Future[Result[void, string]] {.async, gcsafe.} =
  ## Publish a `WakuMessage`. Pubsub topic contains; none, a named or static shard.
  ## `WakuMessage` should contain a `contentTopic` field for light node functionality.
  ## It is also used to determine the shard.

  if node.wakuRelay.isNil():
    let msg =
      "Invalid API call to `publish`. WakuRelay not mounted. Try `lightpush` instead."
    error "publish error", err = msg
    # TODO: Improve error handling
    return err(msg)

  let pubsubTopic = pubsubTopicOp.valueOr:
    node.wakuSharding.getShard(message.contentTopic).valueOr:
      let msg = "Autosharding error: " & error
      error "publish error", err = msg
      return err(msg)

  #TODO instead of discard return error when 0 peers received the message
  discard await node.wakuRelay.publish(pubsubTopic, message)

  notice "waku.relay published",
    peerId = node.peerId,
    pubsubTopic = pubsubTopic,
    msg_hash = pubsubTopic.computeMessageHash(message).to0xHex(),
    publishTime = getNowInNanosecondTime()

  return ok()

proc startRelay*(node: WakuNode) {.async.} =
  ## Setup and start relay protocol
  info "starting relay protocol"

  if node.wakuRelay.isNil():
    error "Failed to start relay. Not mounted."
    return

  ## Setup relay protocol

  # Resume previous relay connections
  if node.peerManager.wakuPeerStore.hasPeers(protocolMatcher(WakuRelayCodec)):
    info "Found previous WakuRelay peers. Reconnecting."

    # Reconnect to previous relay peers. This will respect a backoff period, if necessary
    let backoffPeriod =
      node.wakuRelay.parameters.pruneBackoff + chronos.seconds(BackoffSlackTime)

    await node.peerManager.reconnectPeers(WakuRelayCodec, backoffPeriod)

  # Start the WakuRelay protocol
  await node.wakuRelay.start()

  info "relay started successfully"

proc mountRelay*(
    node: WakuNode,
    shards: seq[RelayShard] = @[],
    peerExchangeHandler = none(RoutingRecordsHandler),
    maxMessageSize = int(DefaultMaxWakuMessageSize),
) {.async, gcsafe.} =
  if not node.wakuRelay.isNil():
    error "wakuRelay already mounted, skipping"
    return

  ## The default relay topics is the union of all configured topics plus default PubsubTopic(s)
  info "mounting relay protocol"

  let initRes = WakuRelay.new(node.switch, maxMessageSize)
  if initRes.isErr():
    error "failed mounting relay protocol", error = initRes.error
    return

  node.wakuRelay = initRes.value

  ## Add peer exchange handler
  if peerExchangeHandler.isSome():
    node.wakuRelay.parameters.enablePX = true
      # Feature flag for peer exchange in nim-libp2p
    node.wakuRelay.routingRecordsHandler.add(peerExchangeHandler.get())

  if node.started:
    await node.startRelay()

  node.switch.mount(node.wakuRelay, protocolMatcher(WakuRelayCodec))

  info "relay mounted successfully", shards = shards

  # Subscribe to shards
  for shard in shards:
    node.subscribe((kind: PubsubSub, topic: $shard))

## Waku filter

proc mountFilter*(
    node: WakuNode,
    subscriptionTimeout: Duration =
      filter_subscriptions.DefaultSubscriptionTimeToLiveSec,
    maxFilterPeers: uint32 = filter_subscriptions.MaxFilterPeers,
    maxFilterCriteriaPerPeer: uint32 = filter_subscriptions.MaxFilterCriteriaPerPeer,
    messageCacheTTL: Duration = filter_subscriptions.MessageCacheTTL,
    rateLimitSetting: RateLimitSetting = FilterDefaultPerPeerRateLimit,
) {.async: (raises: []).} =
  ## Mounting filter v2 protocol

  info "mounting filter protocol"
  node.wakuFilter = WakuFilter.new(
    node.peerManager,
    subscriptionTimeout,
    maxFilterPeers,
    maxFilterCriteriaPerPeer,
    messageCacheTTL,
    some(rateLimitSetting),
  )

  if node.started:
    try:
      await node.wakuFilter.start()
    except CatchableError:
      error "failed to start wakuFilter", error = getCurrentExceptionMsg()

  try:
    node.switch.mount(node.wakuFilter, protocolMatcher(WakuFilterSubscribeCodec))
  except LPError:
    error "failed to mount wakuFilter", error = getCurrentExceptionMsg()

proc filterHandleMessage*(
    node: WakuNode, pubsubTopic: PubsubTopic, message: WakuMessage
) {.async.} =
  if node.wakuFilter.isNil():
    error "cannot handle filter message", error = "waku filter is required"
    return

  await node.wakuFilter.handleMessage(pubsubTopic, message)

proc mountFilterClient*(node: WakuNode) {.async: (raises: []).} =
  ## Mounting both filter
  ## Giving option for application level to choose btw own push message handling or
  ## rely on node provided cache. - This only applies for v2 filter client
  info "mounting filter client"

  node.wakuFilterClient = WakuFilterClient.new(node.peerManager, node.rng)

  if node.started:
    try:
      await node.wakuFilterClient.start()
    except CatchableError:
      error "failed to start wakuFilterClient", error = getCurrentExceptionMsg()

  try:
    node.switch.mount(node.wakuFilterClient, protocolMatcher(WakuFilterSubscribeCodec))
  except LPError:
    error "failed to mount wakuFilterClient", error = getCurrentExceptionMsg()

proc filterSubscribe*(
    node: WakuNode,
    pubsubTopic: Option[PubsubTopic],
    contentTopics: ContentTopic | seq[ContentTopic],
    peer: RemotePeerInfo | string,
): Future[FilterSubscribeResult] {.async: (raises: []).} =
  ## Registers for messages that match a specific filter. Triggers the handler whenever a message is received.
  if node.wakuFilterClient.isNil():
    error "cannot register filter subscription to topic",
      error = "waku filter client is not set up"
    return err(FilterSubscribeError.serviceUnavailable())

  let remotePeerRes = parsePeerInfo(peer)
  if remotePeerRes.isErr():
    error "Couldn't parse the peer info properly", error = remotePeerRes.error
    return err(FilterSubscribeError.serviceUnavailable("No peers available"))

  let remotePeer = remotePeerRes.value

  if pubsubTopic.isSome():
    info "registering filter subscription to content",
      pubsubTopic = pubsubTopic.get(),
      contentTopics = contentTopics,
      peer = remotePeer.peerId

    when (contentTopics is ContentTopic):
      let contentTopics = @[contentTopics]
    let subRes = await node.wakuFilterClient.subscribe(
      remotePeer, pubsubTopic.get(), contentTopics
    )
    if subRes.isOk():
      info "v2 subscribed to topic",
        pubsubTopic = pubsubTopic, contentTopics = contentTopics

      # Purpose is to update Waku Metadata
      node.topicSubscriptionQueue.emit((kind: PubsubSub, topic: pubsubTopic.get()))
    else:
      error "failed filter v2 subscription", error = subRes.error
      waku_node_errors.inc(labelValues = ["subscribe_filter_failure"])

    return subRes
  else:
    let topicMapRes = node.wakuSharding.parseSharding(pubsubTopic, contentTopics)

    let topicMap =
      if topicMapRes.isErr():
        error "can't get shard", error = topicMapRes.error
        return err(FilterSubscribeError.badResponse("can't get shard"))
      else:
        topicMapRes.get()

    var futures = collect(newSeq):
      for pubsub, topics in topicMap.pairs:
        info "registering filter subscription to content",
          pubsubTopic = pubsub, contentTopics = topics, peer = remotePeer.peerId
        let content = topics.mapIt($it)
        node.wakuFilterClient.subscribe(remotePeer, $pubsub, content)

    var subRes: FilterSubscribeResult = FilterSubscribeResult.ok()
    try:
      let finished = await allFinished(futures)

      for fut in finished:
        let res = fut.read()

        if res.isErr():
          error "failed filter subscription", error = res.error
          waku_node_errors.inc(labelValues = ["subscribe_filter_failure"])
          subRes = FilterSubscribeResult.err(res.error)

      for pubsub, topics in topicMap.pairs:
        info "subscribed to topic", pubsubTopic = pubsub, contentTopics = topics

        # Purpose is to update Waku Metadata
        node.topicSubscriptionQueue.emit((kind: PubsubSub, topic: $pubsub))
    except CatchableError:
      let errMsg = "exception in filterSubscribe: " & getCurrentExceptionMsg()
      error "exception in filterSubscribe", error = getCurrentExceptionMsg()
      waku_node_errors.inc(labelValues = ["subscribe_filter_failure"])
      subRes =
        FilterSubscribeResult.err(FilterSubscribeError.serviceUnavailable(errMsg))

    # return the last error or ok
    return subRes

proc filterUnsubscribe*(
    node: WakuNode,
    pubsubTopic: Option[PubsubTopic],
    contentTopics: ContentTopic | seq[ContentTopic],
    peer: RemotePeerInfo | string,
): Future[FilterSubscribeResult] {.async: (raises: []).} =
  ## Unsubscribe from a content filter V2".

  let remotePeerRes = parsePeerInfo(peer)
  if remotePeerRes.isErr():
    error "couldn't parse remotePeerInfo", error = remotePeerRes.error
    return err(FilterSubscribeError.serviceUnavailable("No peers available"))

  let remotePeer = remotePeerRes.value

  if pubsubTopic.isSome():
    info "deregistering filter subscription to content",
      pubsubTopic = pubsubTopic.get(),
      contentTopics = contentTopics,
      peer = remotePeer.peerId

    let unsubRes = await node.wakuFilterClient.unsubscribe(
      remotePeer, pubsubTopic.get(), contentTopics
    )
    if unsubRes.isOk():
      info "unsubscribed from topic",
        pubsubTopic = pubsubTopic.get(), contentTopics = contentTopics

      # Purpose is to update Waku Metadata
      node.topicSubscriptionQueue.emit((kind: PubsubUnsub, topic: pubsubTopic.get()))
    else:
      error "failed filter unsubscription", error = unsubRes.error
      waku_node_errors.inc(labelValues = ["unsubscribe_filter_failure"])

    return unsubRes
  else: # pubsubTopic.isNone
    let topicMapRes = node.wakuSharding.parseSharding(pubsubTopic, contentTopics)

    let topicMap =
      if topicMapRes.isErr():
        error "can't get shard", error = topicMapRes.error
        return err(FilterSubscribeError.badResponse("can't get shard"))
      else:
        topicMapRes.get()

    var futures = collect(newSeq):
      for pubsub, topics in topicMap.pairs:
        info "deregistering filter subscription to content",
          pubsubTopic = pubsub, contentTopics = topics, peer = remotePeer.peerId
        let content = topics.mapIt($it)
        node.wakuFilterClient.unsubscribe(remotePeer, $pubsub, content)

    var unsubRes: FilterSubscribeResult = FilterSubscribeResult.ok()
    try:
      let finished = await allFinished(futures)

      for fut in finished:
        let res = fut.read()

        if res.isErr():
          error "failed filter unsubscription", error = res.error
          waku_node_errors.inc(labelValues = ["unsubscribe_filter_failure"])
          unsubRes = FilterSubscribeResult.err(res.error)

      for pubsub, topics in topicMap.pairs:
        info "unsubscribed from topic", pubsubTopic = pubsub, contentTopics = topics

        # Purpose is to update Waku Metadata
        node.topicSubscriptionQueue.emit((kind: PubsubUnsub, topic: $pubsub))
    except CatchableError:
      let errMsg = "exception in filterUnsubscribe: " & getCurrentExceptionMsg()
      error "exception in filterUnsubscribe", error = getCurrentExceptionMsg()
      waku_node_errors.inc(labelValues = ["unsubscribe_filter_failure"])
      unsubRes =
        FilterSubscribeResult.err(FilterSubscribeError.serviceUnavailable(errMsg))

    # return the last error or ok
    return unsubRes

proc filterUnsubscribeAll*(
    node: WakuNode, peer: RemotePeerInfo | string
): Future[FilterSubscribeResult] {.async: (raises: []).} =
  ## Unsubscribe from a content filter V2".

  let remotePeerRes = parsePeerInfo(peer)
  if remotePeerRes.isErr():
    error "couldn't parse remotePeerInfo", error = remotePeerRes.error
    return err(FilterSubscribeError.serviceUnavailable("No peers available"))

  let remotePeer = remotePeerRes.value

  info "deregistering all filter subscription to content", peer = remotePeer.peerId

  let unsubRes = await node.wakuFilterClient.unsubscribeAll(remotePeer)
  if unsubRes.isOk():
    info "unsubscribed from all content-topic", peerId = remotePeer.peerId
  else:
    error "failed filter unsubscription from all content-topic", error = unsubRes.error
    waku_node_errors.inc(labelValues = ["unsubscribe_filter_failure"])

  return unsubRes

# NOTICE: subscribe / unsubscribe methods are removed - they were already depricated
# yet incompatible to handle both type of filters - use specific filter registration instead

## Waku archive
proc mountArchive*(
    node: WakuNode,
    driver: waku_archive.ArchiveDriver,
    retentionPolicy = none(waku_archive.RetentionPolicy),
): Result[void, string] =
  node.wakuArchive = waku_archive.WakuArchive.new(
    driver = driver, retentionPolicy = retentionPolicy
  ).valueOr:
    return err("error in mountArchive: " & error)

  node.wakuArchive.start()

  return ok()

proc mountLegacyArchive*(
    node: WakuNode, driver: waku_archive_legacy.ArchiveDriver
): Result[void, string] =
  node.wakuLegacyArchive = waku_archive_legacy.WakuArchive.new(driver = driver).valueOr:
    return err("error in mountLegacyArchive: " & error)

  return ok()

## Legacy Waku Store

# TODO: Review this mapping logic. Maybe, move it to the appplication code
proc toArchiveQuery(
    request: legacy_store_common.HistoryQuery
): waku_archive_legacy.ArchiveQuery =
  waku_archive_legacy.ArchiveQuery(
    pubsubTopic: request.pubsubTopic,
    contentTopics: request.contentTopics,
    cursor: request.cursor.map(
      proc(cursor: HistoryCursor): waku_archive_legacy.ArchiveCursor =
        waku_archive_legacy.ArchiveCursor(
          pubsubTopic: cursor.pubsubTopic,
          senderTime: cursor.senderTime,
          storeTime: cursor.storeTime,
          digest: cursor.digest,
        )
    ),
    startTime: request.startTime,
    endTime: request.endTime,
    pageSize: request.pageSize.uint,
    direction: request.direction,
    requestId: request.requestId,
  )

# TODO: Review this mapping logic. Maybe, move it to the appplication code
proc toHistoryResult*(
    res: waku_archive_legacy.ArchiveResult
): legacy_store_common.HistoryResult =
  if res.isErr():
    let error = res.error
    case res.error.kind
    of waku_archive_legacy.ArchiveErrorKind.DRIVER_ERROR,
        waku_archive_legacy.ArchiveErrorKind.INVALID_QUERY:
      err(HistoryError(kind: HistoryErrorKind.BAD_REQUEST, cause: res.error.cause))
    else:
      err(HistoryError(kind: HistoryErrorKind.UNKNOWN))
  else:
    let response = res.get()
    ok(
      HistoryResponse(
        messages: response.messages,
        cursor: response.cursor.map(
          proc(cursor: waku_archive_legacy.ArchiveCursor): HistoryCursor =
            HistoryCursor(
              pubsubTopic: cursor.pubsubTopic,
              senderTime: cursor.senderTime,
              storeTime: cursor.storeTime,
              digest: cursor.digest,
            )
        ),
      )
    )

proc mountLegacyStore*(
    node: WakuNode, rateLimit: RateLimitSetting = DefaultGlobalNonRelayRateLimit
) {.async.} =
  info "mounting waku legacy store protocol"

  if node.wakuLegacyArchive.isNil():
    error "failed to mount waku legacy store protocol", error = "waku archive not set"
    return

  # TODO: Review this handler logic. Maybe, move it to the appplication code
  let queryHandler: HistoryQueryHandler = proc(
      request: HistoryQuery
  ): Future[legacy_store_common.HistoryResult] {.async.} =
    if request.cursor.isSome():
      request.cursor.get().checkHistCursor().isOkOr:
        return err(error)

    let request = request.toArchiveQuery()
    let response = await node.wakuLegacyArchive.findMessagesV2(request)
    return response.toHistoryResult()

  node.wakuLegacyStore = legacy_store.WakuStore.new(
    node.peerManager, node.rng, queryHandler, some(rateLimit)
  )

  if node.started:
    # Node has started already. Let's start store too.
    await node.wakuLegacyStore.start()

  node.switch.mount(
    node.wakuLegacyStore, protocolMatcher(legacy_store_common.WakuLegacyStoreCodec)
  )

proc mountLegacyStoreClient*(node: WakuNode) =
  info "mounting legacy store client"

  node.wakuLegacyStoreClient =
    legacy_store_client.WakuStoreClient.new(node.peerManager, node.rng)

proc query*(
    node: WakuNode, query: legacy_store_common.HistoryQuery, peer: RemotePeerInfo
): Future[legacy_store_common.WakuStoreResult[legacy_store_common.HistoryResponse]] {.
    async, gcsafe
.} =
  ## Queries known nodes for historical messages
  if node.wakuLegacyStoreClient.isNil():
    return err("waku legacy store client is nil")

  let queryRes = await node.wakuLegacyStoreClient.query(query, peer)
  if queryRes.isErr():
    return err("legacy store client query error: " & $queryRes.error)

  let response = queryRes.get()

  return ok(response)

# TODO: Move to application module (e.g., wakunode2.nim)
proc query*(
    node: WakuNode, query: legacy_store_common.HistoryQuery
): Future[legacy_store_common.WakuStoreResult[legacy_store_common.HistoryResponse]] {.
    async, gcsafe, deprecated: "Use 'node.query()' with peer destination instead"
.} =
  ## Queries known nodes for historical messages
  if node.wakuLegacyStoreClient.isNil():
    return err("waku legacy store client is nil")

  let peerOpt = node.peerManager.selectPeer(legacy_store_common.WakuLegacyStoreCodec)
  if peerOpt.isNone():
    error "no suitable remote peers"
    return err("peer_not_found_failure")

  return await node.query(query, peerOpt.get())

when defined(waku_exp_store_resume):
  # TODO: Move to application module (e.g., wakunode2.nim)
  proc resume*(
      node: WakuNode, peerList: Option[seq[RemotePeerInfo]] = none(seq[RemotePeerInfo])
  ) {.async, gcsafe.} =
    ## resume proc retrieves the history of waku messages published on the default waku pubsub topic since the last time the waku node has been online
    ## for resume to work properly the waku node must have the store protocol mounted in the full mode (i.e., persisting messages)
    ## messages are stored in the wakuStore's messages field and in the message db
    ## the offline time window is measured as the difference between the current time and the timestamp of the most recent persisted waku message
    ## an offset of 20 second is added to the time window to count for nodes asynchrony
    ## peerList indicates the list of peers to query from. The history is fetched from the first available peer in this list. Such candidates should be found through a discovery method (to be developed).
    ## if no peerList is passed, one of the peers in the underlying peer manager unit of the store protocol is picked randomly to fetch the history from.
    ## The history gets fetched successfully if the dialed peer has been online during the queried time window.
    if node.wakuLegacyStoreClient.isNil():
      return

    let retrievedMessages = await node.wakuLegacyStoreClient.resume(peerList)
    if retrievedMessages.isErr():
      error "failed to resume store", error = retrievedMessages.error
      return

    info "the number of retrieved messages since the last online time: ",
      number = retrievedMessages.value

## Waku Store

proc toArchiveQuery(request: StoreQueryRequest): waku_archive.ArchiveQuery =
  var query = waku_archive.ArchiveQuery()

  query.includeData = request.includeData
  query.pubsubTopic = request.pubsubTopic
  query.contentTopics = request.contentTopics
  query.startTime = request.startTime
  query.endTime = request.endTime
  query.hashes = request.messageHashes
  query.cursor = request.paginationCursor
  query.direction = request.paginationForward
  query.requestId = request.requestId

  if request.paginationLimit.isSome():
    query.pageSize = uint(request.paginationLimit.get())

  return query

proc toStoreResult(res: waku_archive.ArchiveResult): StoreQueryResult =
  let response = res.valueOr:
    return err(StoreError.new(300, "archive error: " & $error))

  var res = StoreQueryResponse()

  res.statusCode = 200
  res.statusDesc = "OK"

  for i in 0 ..< response.hashes.len:
    let hash = response.hashes[i]

    let kv = store_common.WakuMessageKeyValue(messageHash: hash)

    res.messages.add(kv)

  for i in 0 ..< response.messages.len:
    res.messages[i].message = some(response.messages[i])
    res.messages[i].pubsubTopic = some(response.topics[i])

  res.paginationCursor = response.cursor

  return ok(res)

proc mountStore*(
    node: WakuNode, rateLimit: RateLimitSetting = DefaultGlobalNonRelayRateLimit
) {.async.} =
  if node.wakuArchive.isNil():
    error "failed to mount waku store protocol", error = "waku archive not set"
    return

  info "mounting waku store protocol"

  let requestHandler: StoreQueryRequestHandler = proc(
      request: StoreQueryRequest
  ): Future[StoreQueryResult] {.async.} =
    let request = request.toArchiveQuery()
    let response = await node.wakuArchive.findMessages(request)

    return response.toStoreResult()

  node.wakuStore =
    store.WakuStore.new(node.peerManager, node.rng, requestHandler, some(rateLimit))

  if node.started:
    await node.wakuStore.start()

  node.switch.mount(node.wakuStore, protocolMatcher(store_common.WakuStoreCodec))

proc mountStoreClient*(node: WakuNode) =
  info "mounting store client"

  node.wakuStoreClient = store_client.WakuStoreClient.new(node.peerManager, node.rng)

proc query*(
    node: WakuNode, request: store_common.StoreQueryRequest, peer: RemotePeerInfo
): Future[store_common.WakuStoreResult[store_common.StoreQueryResponse]] {.
    async, gcsafe
.} =
  ## Queries known nodes for historical messages
  if node.wakuStoreClient.isNil():
    return err("waku store v3 client is nil")

  let response = (await node.wakuStoreClient.query(request, peer)).valueOr:
    var res = StoreQueryResponse()
    res.statusCode = uint32(error.kind)
    res.statusDesc = $error

    return ok(res)

  return ok(response)

proc setupStoreResume*(node: WakuNode) =
  node.wakuStoreResume = StoreResume.new(
    node.peerManager, node.wakuArchive, node.wakuStoreClient
  ).valueOr:
    error "Failed to setup Store Resume", error = $error
    return

## Waku lightpush

proc mountLightPush*(
    node: WakuNode, rateLimit: RateLimitSetting = DefaultGlobalNonRelayRateLimit
) {.async.} =
  info "mounting light push"

  var pushHandler =
    if node.wakuRelay.isNil:
      debug "mounting lightpush without relay (nil)"
      getNilPushHandler()
    else:
      debug "mounting lightpush with relay"
      let rlnPeer =
        if isNil(node.wakuRlnRelay):
          debug "mounting lightpush without rln-relay"
          none(WakuRLNRelay)
        else:
          debug "mounting lightpush with rln-relay"
          some(node.wakuRlnRelay)
      getRelayPushHandler(node.wakuRelay, rlnPeer)

  node.wakuLightPush =
    WakuLightPush.new(node.peerManager, node.rng, pushHandler, some(rateLimit))

  if node.started:
    # Node has started already. Let's start lightpush too.
    await node.wakuLightPush.start()

  node.switch.mount(node.wakuLightPush, protocolMatcher(WakuLightPushCodec))

proc mountLightPushClient*(node: WakuNode) =
  info "mounting light push client"

  node.wakuLightpushClient = WakuLightPushClient.new(node.peerManager, node.rng)

proc lightpushPublish*(
    node: WakuNode,
    pubsubTopic: Option[PubsubTopic],
    message: WakuMessage,
    peer: RemotePeerInfo,
): Future[WakuLightPushResult[string]] {.async, gcsafe.} =
  ## Pushes a `WakuMessage` to a node which relays it further on PubSub topic.
  ## Returns whether relaying was successful or not.
  ## `WakuMessage` should contain a `contentTopic` field for light node
  ## functionality.
  if node.wakuLightpushClient.isNil() and node.wakuLightPush.isNil():
    error "failed to publish message as lightpush not available"
    return err("Waku lightpush not available")

  let internalPublish = proc(
      node: WakuNode,
      pubsubTopic: PubsubTopic,
      message: WakuMessage,
      peer: RemotePeerInfo,
  ): Future[WakuLightPushResult[string]] {.async, gcsafe.} =
    let msgHash = pubsubTopic.computeMessageHash(message).to0xHex()
    if not node.wakuLightpushClient.isNil():
      notice "publishing message with lightpush",
        pubsubTopic = pubsubTopic,
        contentTopic = message.contentTopic,
        target_peer_id = peer.peerId,
        msg_hash = msgHash
      return await node.wakuLightpushClient.publish(pubsubTopic, message, peer)

    if not node.wakuLightPush.isNil():
      notice "publishing message with self hosted lightpush",
        pubsubTopic = pubsubTopic,
        contentTopic = message.contentTopic,
        target_peer_id = peer.peerId,
        msg_hash = msgHash
      return await node.wakuLightPush.handleSelfLightPushRequest(pubsubTopic, message)
  try:
    if pubsubTopic.isSome():
      return await internalPublish(node, pubsubTopic.get(), message, peer)

    let topicMapRes = node.wakuSharding.parseSharding(pubsubTopic, message.contentTopic)

    let topicMap =
      if topicMapRes.isErr():
        return err(topicMapRes.error)
      else:
        topicMapRes.get()

    for pubsub, _ in topicMap.pairs: # There's only one pair anyway
      return await internalPublish(node, $pubsub, message, peer)
  except CatchableError:
    return err(getCurrentExceptionMsg())

# TODO: Move to application module (e.g., wakunode2.nim)
proc lightpushPublish*(
    node: WakuNode, pubsubTopic: Option[PubsubTopic], message: WakuMessage
): Future[WakuLightPushResult[string]] {.
    async, gcsafe, deprecated: "Use 'node.lightpushPublish()' instead"
.} =
  if node.wakuLightpushClient.isNil() and node.wakuLightPush.isNil():
    error "failed to publish message as lightpush not available"
    return err("waku lightpush not available")

  var peerOpt: Option[RemotePeerInfo] = none(RemotePeerInfo)
  if not node.wakuLightpushClient.isNil():
    peerOpt = node.peerManager.selectPeer(WakuLightPushCodec)
    if peerOpt.isNone():
      let msg = "no suitable remote peers"
      error "failed to publish message", err = msg
      return err(msg)
  elif not node.wakuLightPush.isNil():
    peerOpt = some(RemotePeerInfo.init($node.switch.peerInfo.peerId))

  return await node.lightpushPublish(pubsubTopic, message, peer = peerOpt.get())

## Waku RLN Relay
proc mountRlnRelay*(
    node: WakuNode,
    rlnConf: WakuRlnConfig,
    spamHandler = none(SpamHandler),
    registrationHandler = none(RegistrationHandler),
) {.async.} =
  info "mounting rln relay"

  if node.wakuRelay.isNil():
    raise newException(
      CatchableError, "WakuRelay protocol is not mounted, cannot mount WakuRlnRelay"
    )

  let rlnRelayRes = waitFor WakuRlnRelay.new(rlnConf, registrationHandler)
  if rlnRelayRes.isErr():
    raise
      newException(CatchableError, "failed to mount WakuRlnRelay: " & rlnRelayRes.error)
  let rlnRelay = rlnRelayRes.get()
  if (rlnConf.rlnRelayUserMessageLimit > rlnRelay.groupManager.rlnRelayMaxMessageLimit):
    error "rln-relay-user-message-limit can't exceed the MAX_MESSAGE_LIMIT in the rln contract"
  let validator = generateRlnValidator(rlnRelay, spamHandler)

  # register rln validator as default validator
  debug "Registering RLN validator"
  node.wakuRelay.addValidator(validator, "RLN validation failed")

  node.wakuRlnRelay = rlnRelay

## Waku peer-exchange

proc mountPeerExchange*(
    node: WakuNode,
    cluster: Option[uint16] = none(uint16),
    rateLimit: RateLimitSetting = DefaultGlobalNonRelayRateLimit,
) {.async: (raises: []).} =
  info "mounting waku peer exchange"

  node.wakuPeerExchange =
    WakuPeerExchange.new(node.peerManager, cluster, some(rateLimit))

  if node.started:
    try:
      await node.wakuPeerExchange.start()
    except CatchableError:
      error "failed to start wakuPeerExchange", error = getCurrentExceptionMsg()

  try:
    node.switch.mount(node.wakuPeerExchange, protocolMatcher(WakuPeerExchangeCodec))
  except LPError:
    error "failed to mount wakuPeerExchange", error = getCurrentExceptionMsg()

proc fetchPeerExchangePeers*(
    node: Wakunode, amount = DefaultPXNumPeersReq
): Future[Result[int, PeerExchangeResponseStatus]] {.async: (raises: []).} =
  if node.wakuPeerExchange.isNil():
    error "could not get peers from px, waku peer-exchange is nil"
    return err(
      (
        status_code: PeerExchangeResponseStatusCode.SERVICE_UNAVAILABLE,
        status_desc: some("PeerExchange is not mounted"),
      )
    )

  info "Retrieving peer info via peer exchange protocol"
  let pxPeersRes = await node.wakuPeerExchange.request(amount)
  if pxPeersRes.isOk:
    var validPeers = 0
    let peers = pxPeersRes.get().peerInfos
    for pi in peers:
      var record: enr.Record
      if enr.fromBytes(record, pi.enr):
        node.peerManager.addPeer(record.toRemotePeerInfo().get, PeerExchange)
        validPeers += 1
    info "Retrieved peer info via peer exchange protocol",
      validPeers = validPeers, totalPeers = peers.len
    return ok(validPeers)
  else:
    warn "failed to retrieve peer info via peer exchange protocol",
      error = pxPeersRes.error
    return err(pxPeersRes.error)

proc peerExchangeLoop(node: WakuNode) {.async.} =
  while true:
    await sleepAsync(1.minutes)
    if not node.started:
      continue
    (await node.fetchPeerExchangePeers()).isOkOr:
      warn "Cannot fetch peers from peer exchange", cause = error

proc startPeerExchangeLoop*(node: WakuNode) =
  if node.wakuPeerExchange.isNil():
    error "startPeerExchangeLoop: Peer Exchange is not mounted"
    return
  node.wakuPeerExchange.pxLoopHandle = node.peerExchangeLoop()

# TODO: Move to application module (e.g., wakunode2.nim)
proc setPeerExchangePeer*(
    node: WakuNode, peer: RemotePeerInfo | MultiAddress | string
) =
  if node.wakuPeerExchange.isNil():
    error "could not set peer, waku peer-exchange is nil"
    return

  info "Set peer-exchange peer", peer = peer

  let remotePeerRes = parsePeerInfo(peer)
  if remotePeerRes.isErr():
    error "could not parse peer info", error = remotePeerRes.error
    return

  node.peerManager.addPeer(remotePeerRes.value, PeerExchange)
  waku_px_peers.inc()

## Other protocols

proc mountLibp2pPing*(node: WakuNode) {.async: (raises: []).} =
  info "mounting libp2p ping protocol"

  try:
    node.libp2pPing = Ping.new(rng = node.rng)
  except Exception as e:
    error "failed to create ping", error = getCurrentExceptionMsg()

  if node.started:
    # Node has started already. Let's start ping too.
    try:
      await node.libp2pPing.start()
    except CatchableError:
      error "failed to start libp2pPing", error = getCurrentExceptionMsg()

  try:
    node.switch.mount(node.libp2pPing)
  except LPError:
    error "failed to mount libp2pPing", error = getCurrentExceptionMsg()

# TODO: Move this logic to PeerManager
proc keepaliveLoop(node: WakuNode, keepalive: chronos.Duration) {.async.} =
  while true:
    await sleepAsync(keepalive)
    if not node.started:
      continue

    # Keep connected peers alive while running
    # Each node is responsible of keeping its outgoing connections alive
    trace "Running keepalive"

    # First get a list of connected peer infos
    let outPeers = node.peerManager.connectedPeers()[1]

    for peerId in outPeers:
      try:
        let conn = (await node.peerManager.dialPeer(peerId, PingCodec)).valueOr:
          warn "Failed dialing peer for keep alive", peerId = peerId
          continue
        let pingDelay = await node.libp2pPing.ping(conn)
        await conn.close()
      except CatchableError as exc:
        waku_node_errors.inc(labelValues = ["keep_alive_failure"])

# 2 minutes default - 20% of the default chronosstream timeout duration
proc startKeepalive*(node: WakuNode, keepalive = 2.minutes) =
  info "starting keepalive", keepalive = keepalive

  asyncSpawn node.keepaliveLoop(keepalive)

proc mountRendezvous*(node: WakuNode) {.async: (raises: []).} =
  info "mounting rendezvous discovery protocol"

  node.wakuRendezvous = WakuRendezVous.new(node.switch, node.peerManager, node.enr).valueOr:
    error "initializing waku rendezvous failed", error = error
    return

  # Always start discovering peers at startup
  (await node.wakuRendezvous.initialRequestAll()).isOkOr:
    error "rendezvous failed initial requests", error = error

  if node.started:
    await node.wakuRendezvous.start()

proc isBindIpWithZeroPort(inputMultiAdd: MultiAddress): bool =
  let inputStr = $inputMultiAdd
  if inputStr.contains("0.0.0.0/tcp/0") or inputStr.contains("127.0.0.1/tcp/0"):
    return true

  return false

proc updateAnnouncedAddrWithPrimaryIpAddr*(node: WakuNode): Result[void, string] =
  let peerInfo = node.switch.peerInfo
  var announcedStr = ""
  var listenStr = ""
  var localIp = "0.0.0.0"

  try:
    localIp = $getPrimaryIPAddr()
  except Exception as e:
    warn "Could not retrieve localIp", msg = e.msg

  info "PeerInfo", peerId = peerInfo.peerId, addrs = peerInfo.addrs

  ## Update the WakuNode addresses
  var newAnnouncedAddresses = newSeq[MultiAddress](0)
  for address in node.announcedAddresses:
    ## Replace "0.0.0.0" or "127.0.0.1" with the localIp
    let newAddr = ($address).replace("0.0.0.0", localIp).replace("127.0.0.1", localIp)
    let fulladdr = "[" & $newAddr & "/p2p/" & $peerInfo.peerId & "]"
    announcedStr &= fulladdr
    let newMultiAddr = MultiAddress.init(newAddr).valueOr:
      return err("error in updateAnnouncedAddrWithPrimaryIpAddr: " & $error)
    newAnnouncedAddresses.add(newMultiAddr)

  node.announcedAddresses = newAnnouncedAddresses

  ## Update the Switch addresses
  node.switch.peerInfo.addrs = newAnnouncedAddresses

  for transport in node.switch.transports:
    for address in transport.addrs:
      let fulladdr = "[" & $address & "/p2p/" & $peerInfo.peerId & "]"
      listenStr &= fulladdr

  info "Listening on",
    full = listenStr, localIp = localIp, switchAddress = $(node.switch.peerInfo.addrs)
  info "Announcing addresses", full = announcedStr
  info "DNS: discoverable ENR ", enr = node.enr.toUri()

  return ok()

proc start*(node: WakuNode) {.async.} =
  ## Starts a created Waku Node and
  ## all its mounted protocols.

  waku_version.set(1, labelValues = [git_version])
  info "Starting Waku node", version = git_version

  var zeroPortPresent = false
  for address in node.announcedAddresses:
    if isBindIpWithZeroPort(address):
      zeroPortPresent = true

  # Perform relay-specific startup tasks TODO: this should be rethought
  if not node.wakuRelay.isNil():
    await node.startRelay()

  if not node.wakuMetadata.isNil():
    node.wakuMetadata.start()

  if not node.wakuStoreResume.isNil():
    await node.wakuStoreResume.start()

  if not node.wakuRendezvous.isNil():
    await node.wakuRendezvous.start()

  ## The switch uses this mapper to update peer info addrs
  ## with announced addrs after start
  let addressMapper = proc(
      listenAddrs: seq[MultiAddress]
  ): Future[seq[MultiAddress]] {.async.} =
    return node.announcedAddresses
  node.switch.peerInfo.addressMappers.add(addressMapper)

  ## The switch will update addresses after start using the addressMapper
  await node.switch.start()

  node.started = true

  if not zeroPortPresent:
    updateAnnouncedAddrWithPrimaryIpAddr(node).isOkOr:
      error "failed update announced addr", error = $error
  else:
    info "Listening port is dynamically allocated, address and ENR generation postponed"

  info "Node started successfully"

proc stop*(node: WakuNode) {.async.} =
  ## By stopping the switch we are stopping all the underlying mounted protocols
  await node.switch.stop()

  node.peerManager.stop()

  if not node.wakuRlnRelay.isNil():
    try:
      await node.wakuRlnRelay.stop() ## this can raise an exception
    except Exception:
      error "exception stopping the node", error = getCurrentExceptionMsg()

  if not node.wakuArchive.isNil():
    await node.wakuArchive.stopWait()

  if not node.wakuStoreResume.isNil():
    await node.wakuStoreResume.stopWait()

  if not node.wakuPeerExchange.isNil() and not node.wakuPeerExchange.pxLoopHandle.isNil():
    await node.wakuPeerExchange.pxLoopHandle.cancelAndWait()

  if not node.wakuRendezvous.isNil():
    await node.wakuRendezvous.stopWait()

  node.started = false

proc isReady*(node: WakuNode): Future[bool] {.async: (raises: [Exception]).} =
  if node.wakuRlnRelay == nil:
    return true
  return await node.wakuRlnRelay.isReady()
  ## TODO: add other protocol `isReady` checks

proc setRateLimits*(node: WakuNode, limits: seq[string]): Result[void, string] =
  let rateLimitConfig = ProtocolRateLimitSettings.parse(limits)
  if rateLimitConfig.isErr():
    return err("invalid rate limit settings:" & rateLimitConfig.error)
  node.rateLimitSettings = rateLimitConfig.get()
  return ok()
