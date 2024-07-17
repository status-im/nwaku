import
  std/[options, sequtils],
  chronicles,
  chronos,
  libp2p/peerid,
  libp2p/protocols/pubsub/gossipsub,
  libp2p/protocols/connectivity/relay/relay,
  libp2p/nameresolving/dnsresolver,
  libp2p/crypto/crypto

import
  ./internal_config,
  ./external_config,
  ./builder,
  ./validator_signed,
  ../waku_enr/sharding,
  ../waku_node,
  ../waku_core,
  ../waku_core/codecs,
  ../waku_rln_relay,
  ../discovery/waku_dnsdisc,
  ../waku_archive/retention_policy as policy,
  ../waku_archive/retention_policy/builder as policy_builder,
  ../waku_archive/driver as driver,
  ../waku_archive/driver/builder as driver_builder,
  ../waku_archive_legacy/driver as legacy_driver,
  ../waku_archive_legacy/driver/builder as legacy_driver_builder,
  ../waku_store,
  ../waku_store/common as store_common,
  ../waku_store_legacy,
  ../waku_store_legacy/common as legacy_common,
  ../waku_filter_v2,
  ../waku_peer_exchange,
  ../node/peer_manager,
  ../node/peer_manager/peer_store/waku_peer_storage,
  ../node/peer_manager/peer_store/migrations as peer_store_sqlite_migrations,
  ../waku_lightpush_legacy/common,
  ../common/utils/parse_size_units,
  ../common/rate_limit/setting,
  ../common/databases/dburl

## Peer persistence

const PeerPersistenceDbUrl = "peers.db"
proc setupPeerStorage(): Result[Option[WakuPeerStorage], string] =
  let db = ?SqliteDatabase.new(PeerPersistenceDbUrl)

  ?peer_store_sqlite_migrations.migrate(db)

  let res = WakuPeerStorage.new(db)
  if res.isErr():
    return err("failed to init peer store" & res.error)

  ok(some(res.value))

## Init waku node instance

proc initNode(
    conf: WakuNodeConf,
    netConfig: NetConfig,
    rng: ref HmacDrbgContext,
    nodeKey: crypto.PrivateKey,
    record: enr.Record,
    peerStore: Option[WakuPeerStorage],
    relay: Relay,
    dynamicBootstrapNodes: openArray[RemotePeerInfo] = @[],
): Result[WakuNode, string] =
  ## Setup a basic Waku v2 node based on a supplied configuration
  ## file. Optionally include persistent peer storage.
  ## No protocols are mounted yet.

  var dnsResolver: DnsResolver
  if conf.dnsAddrs:
    # Support for DNS multiaddrs
    var nameServers: seq[TransportAddress]
    for ip in conf.dnsAddrsNameServers:
      nameServers.add(initTAddress(ip, Port(53))) # Assume all servers use port 53

    dnsResolver = DnsResolver.new(nameServers)

  var node: WakuNode

  let pStorage =
    if peerStore.isNone():
      nil
    else:
      peerStore.get()

  # Build waku node instance
  var builder = WakuNodeBuilder.init()
  builder.withRng(rng)
  builder.withNodeKey(nodekey)
  builder.withRecord(record)
  builder.withNetworkConfiguration(netConfig)
  builder.withPeerStorage(pStorage, capacity = conf.peerStoreCapacity)
  builder.withSwitchConfiguration(
    maxConnections = some(conf.maxConnections.int),
    secureKey = some(conf.websocketSecureKeyPath),
    secureCert = some(conf.websocketSecureCertPath),
    nameResolver = dnsResolver,
    sendSignedPeerRecord = conf.relayPeerExchange,
      # We send our own signed peer record when peer exchange enabled
    agentString = some(conf.agentString),
  )
  builder.withColocationLimit(conf.colocationLimit)

  if conf.maxRelayPeers.isSome():
    let
      maxRelayPeers = conf.maxRelayPeers.get()
      maxConnections = conf.maxConnections
      # Calculate the ratio as percentages
      relayRatio = (maxRelayPeers.float / maxConnections.float) * 100
      serviceRatio = 100 - relayRatio

    builder.withPeerManagerConfig(
      maxConnections = conf.maxConnections,
      relayServiceRatio = $relayRatio & ":" & $serviceRatio,
      shardAware = conf.relayShardedPeerManagement,
    )
    error "maxRelayPeers is deprecated. It is recommended to use relayServiceRatio instead. If relayServiceRatio is not set, it will be automatically calculated based on maxConnections and maxRelayPeers."
  else:
    builder.withPeerManagerConfig(
      maxConnections = conf.maxConnections,
      relayServiceRatio = conf.relayServiceRatio,
      shardAware = conf.relayShardedPeerManagement,
    )
  builder.withRateLimit(conf.rateLimits)
  builder.withCircuitRelay(relay)

  node =
    ?builder.build().mapErr(
      proc(err: string): string =
        "failed to create waku node instance: " & err
    )

  ok(node)

## Mount protocols

proc getNumShardsInNetwork*(conf: WakuNodeConf): uint32 =
  if conf.numShardsInNetwork != 0:
    return conf.numShardsInNetwork
  # If conf.numShardsInNetwork is not set, use 1024 - the maximum possible as per the static sharding spec
  # https://github.com/waku-org/specs/blob/master/standards/core/relay-sharding.md#static-sharding
  return uint32(MaxShardIndex + 1)

proc getAutoshards*(
    node: WakuNode, contentTopics: seq[string]
): Result[seq[RelayShard], string] =
  var autoShards: seq[RelayShard]
  for contentTopic in contentTopics:
    let shard = node.wakuSharding.getShard(contentTopic).valueOr:
      return err("Could not parse content topic: " & error)
    autoShards.add(shard)
  return ok(autoshards)

proc setupProtocols(
    node: WakuNode, conf: WakuNodeConf, nodeKey: crypto.PrivateKey
): Future[Result[void, string]] {.async.} =
  ## Setup configured protocols on an existing Waku v2 node.
  ## Optionally include persistent message storage.
  ## No protocols are started yet.

  if conf.discv5Only:
    notice "Running node only with Discv5, not mounting additional protocols"
    return ok()

  node.mountMetadata(conf.clusterId).isOkOr:
    return err("failed to mount waku metadata protocol: " & error)

  var onFatalErrorAction = proc(msg: string) {.gcsafe, closure.} =
    ## Action to be taken when an internal error occurs during the node run.
    ## e.g. the connection with the database is lost and not recovered.
    error "Unrecoverable error occurred", error = msg
    quit(QuitFailure)

  if conf.store:
    if conf.legacyStore:
      let archiveDriverRes = waitFor legacy_driver.ArchiveDriver.new(
        conf.storeMessageDbUrl, conf.storeMessageDbVacuum, conf.storeMessageDbMigration,
        conf.storeMaxNumDbConnections, onFatalErrorAction,
      )
      if archiveDriverRes.isErr():
        return err("failed to setup legacy archive driver: " & archiveDriverRes.error)

      let mountArcRes = node.mountLegacyArchive(archiveDriverRes.get())
      if mountArcRes.isErr():
        return err("failed to mount waku legacy archive protocol: " & mountArcRes.error)

    ## For now we always mount the future archive driver but if the legacy one is mounted,
    ## then the legacy will be in charge of performing the archiving.
    ## Regarding storage, the only diff between the current/future archive driver and the legacy
    ## one, is that the legacy stores an extra field: the id (message digest.)

    ## TODO: remove this "migrate" variable once legacy store is removed
    ## It is now necessary because sqlite's legacy store has an extra field: storedAt
    ## This breaks compatibility between store's and legacy store's schemas in sqlite
    ## So for now, we need to make sure that when legacy store is enabled and we use sqlite
    ## that we migrate our db according to legacy store's schema to have the extra field

    let engineRes = dburl.getDbEngine(conf.storeMessageDbUrl)
    if engineRes.isErr():
      return err("error getting db engine in setupProtocols: " & engineRes.error)

    let engine = engineRes.get()

    let migrate =
      if engine == "sqlite" and conf.legacyStore:
        false
      else:
        conf.storeMessageDbMigration

    let archiveDriverRes = waitFor driver.ArchiveDriver.new(
      conf.storeMessageDbUrl, conf.storeMessageDbVacuum, migrate,
      conf.storeMaxNumDbConnections, onFatalErrorAction,
    )
    if archiveDriverRes.isErr():
      return err("failed to setup archive driver: " & archiveDriverRes.error)

    let retPolicyRes = policy.RetentionPolicy.new(conf.storeMessageRetentionPolicy)
    if retPolicyRes.isErr():
      return err("failed to create retention policy: " & retPolicyRes.error)

    let mountArcRes = node.mountArchive(archiveDriverRes.get(), retPolicyRes.get())
    if mountArcRes.isErr():
      return err("failed to mount waku archive protocol: " & mountArcRes.error)

    if conf.legacyStore:
      # Store legacy setup
      try:
        await mountLegacyStore(node, node.rateLimitSettings.getSetting(STOREV2))
      except CatchableError:
        return
          err("failed to mount waku legacy store protocol: " & getCurrentExceptionMsg())

    # Store setup
    try:
      await mountStore(node, node.rateLimitSettings.getSetting(STOREV3))
    except CatchableError:
      return err("failed to mount waku store protocol: " & getCurrentExceptionMsg())

  mountStoreClient(node)
  if conf.storenode != "":
    let storeNode = parsePeerInfo(conf.storenode)
    if storeNode.isOk():
      node.peerManager.addServicePeer(storeNode.value, store_common.WakuStoreCodec)
    else:
      return err("failed to set node waku store peer: " & storeNode.error)

  mountLegacyStoreClient(node)
  if conf.storenode != "":
    let storeNode = parsePeerInfo(conf.storenode)
    if storeNode.isOk():
      node.peerManager.addServicePeer(
        storeNode.value, legacy_common.WakuLegacyStoreCodec
      )
    else:
      return err("failed to set node waku legacy store peer: " & storeNode.error)

  if conf.store and conf.storeResume:
    node.setupStoreResume()

  # If conf.numShardsInNetwork is not set, use the number of shards configured as numShardsInNetwork
  let numShardsInNetwork = getNumShardsInNetwork(conf)

  if conf.numShardsInNetwork == 0:
    warn "Number of shards in network not configured, setting it to",
      numShardsInNetwork = $numShardsInNetwork

  node.mountSharding(conf.clusterId, numShardsInNetwork).isOkOr:
    return err("failed to mount waku sharding: " & error)

  # Mount relay on all nodes
  var peerExchangeHandler = none(RoutingRecordsHandler)
  if conf.relayPeerExchange:
    proc handlePeerExchange(
        peer: PeerId, topic: string, peers: seq[RoutingRecordsPair]
    ) {.gcsafe.} =
      ## Handle peers received via gossipsub peer exchange
      # TODO: Only consider peers on pubsub topics we subscribe to
      let exchangedPeers = peers.filterIt(it.record.isSome())
        # only peers with populated records
        .mapIt(toRemotePeerInfo(it.record.get()))

      debug "adding exchanged peers",
        src = peer, topic = topic, numPeers = exchangedPeers.len

      for peer in exchangedPeers:
        # Peers added are filtered by the peer manager
        node.peerManager.addPeer(peer, PeerOrigin.PeerExchange)

    peerExchangeHandler = some(handlePeerExchange)

  let autoShards = node.getAutoshards(conf.contentTopics).valueOr:
    return err("Could not get autoshards: " & error)

  debug "Shards created from content topics",
    contentTopics = conf.contentTopics, shards = autoShards

  let confShards =
    conf.shards.mapIt(RelayShard(clusterId: conf.clusterId, shardId: uint16(it)))
  let shards = confShards & autoShards

  if conf.relay:
    let parsedMaxMsgSize = parseMsgSize(conf.maxMessageSize).valueOr:
      return err("failed to parse 'max-num-bytes-msg-size' param: " & $error)

    debug "Setting max message size", num_bytes = parsedMaxMsgSize

    try:
      await mountRelay(
        node, shards, peerExchangeHandler = peerExchangeHandler, int(parsedMaxMsgSize)
      )
    except CatchableError:
      return err("failed to mount waku relay protocol: " & getCurrentExceptionMsg())

    # Add validation keys to protected topics
    var subscribedProtectedShards: seq[ProtectedShard]
    for shardKey in conf.protectedShards:
      if shardKey.shard notin conf.shards:
        warn "protected shard not in subscribed shards, skipping adding validator",
          protectedShard = shardKey.shard, subscribedShards = shards
        continue
      subscribedProtectedShards.add(shardKey)
      notice "routing only signed traffic",
        protectedShard = shardKey.shard, publicKey = shardKey.key
    node.wakuRelay.addSignedShardsValidator(subscribedProtectedShards, conf.clusterId)

    # Only relay nodes should be rendezvous points.
    if conf.rendezvous:
      await node.mountRendezvous()

  # Keepalive mounted on all nodes
  try:
    await mountLibp2pPing(node)
  except CatchableError:
    return err("failed to mount libp2p ping protocol: " & getCurrentExceptionMsg())

  if conf.rlnRelay:
    let rlnConf = WakuRlnConfig(
      rlnRelayDynamic: conf.rlnRelayDynamic,
      rlnRelayCredIndex: conf.rlnRelayCredIndex,
      rlnRelayEthContractAddress: conf.rlnRelayEthContractAddress,
      rlnRelayChainId: conf.rlnRelayChainId,
      rlnRelayEthClientAddress: string(conf.rlnRelayethClientAddress),
      rlnRelayCredPath: conf.rlnRelayCredPath,
      rlnRelayCredPassword: conf.rlnRelayCredPassword,
      rlnRelayTreePath: conf.rlnRelayTreePath,
      rlnRelayUserMessageLimit: conf.rlnRelayUserMessageLimit,
      rlnEpochSizeSec: conf.rlnEpochSizeSec,
      onFatalErrorAction: onFatalErrorAction,
    )

    try:
      waitFor node.mountRlnRelay(rlnConf)
    except CatchableError:
      return err("failed to mount waku RLN relay protocol: " & getCurrentExceptionMsg())

  # NOTE Must be mounted after relay
  if conf.lightpush:
    try:
      await mountLightPush(node, node.rateLimitSettings.getSetting(LIGHTPUSH))
      await mountLegacyLightPush(node, node.rateLimitSettings.getSetting(LIGHTPUSH))
    except CatchableError:
      return err("failed to mount waku lightpush protocol: " & getCurrentExceptionMsg())

  mountLightPushClient(node)
  mountLegacyLightPushClient(node)
  if conf.lightpushnode != "":
    let lightPushNode = parsePeerInfo(conf.lightpushnode)
    if lightPushNode.isOk():
      node.peerManager.addServicePeer(lightPushNode.value, WakuLightPushCodec)
      node.peerManager.addServicePeer(lightPushNode.value, WakuLegacyLightPushCodec)
    else:
      return err("failed to set node waku lightpush peer: " & lightPushNode.error)

  # Filter setup. NOTE Must be mounted after relay
  if conf.filter:
    try:
      await mountFilter(
        node,
        subscriptionTimeout = chronos.seconds(conf.filterSubscriptionTimeout),
        maxFilterPeers = conf.filterMaxPeersToServe,
        maxFilterCriteriaPerPeer = conf.filterMaxCriteria,
        rateLimitSetting = node.rateLimitSettings.getSetting(FILTER),
      )
    except CatchableError:
      return err("failed to mount waku filter protocol: " & getCurrentExceptionMsg())

  await node.mountFilterClient()
  if conf.filternode != "":
    let filterNode = parsePeerInfo(conf.filternode)
    if filterNode.isOk():
      try:
        node.peerManager.addServicePeer(filterNode.value, WakuFilterSubscribeCodec)
      except CatchableError:
        return err(
          "failed to mount waku filter client protocol: " & getCurrentExceptionMsg()
        )
    else:
      return err("failed to set node waku filter peer: " & filterNode.error)

  if conf.storeSync:
    (
      await node.mountStoreSync(
        conf.storeSyncRange, conf.storeSyncInterval, conf.storeSyncRelayJitter
      )
    ).isOkOr:
      return err("failed to mount waku store sync protocol: " & $error)

  # waku peer exchange setup
  if conf.peerExchange:
    try:
      await mountPeerExchange(
        node, some(conf.clusterId), node.rateLimitSettings.getSetting(PEEREXCHG)
      )
    except CatchableError:
      return
        err("failed to mount waku peer-exchange protocol: " & getCurrentExceptionMsg())

  if conf.peerExchangeNode != "":
    let peerExchangeNode = parsePeerInfo(conf.peerExchangeNode)
    if peerExchangeNode.isOk():
      node.peerManager.addServicePeer(peerExchangeNode.value, WakuPeerExchangeCodec)
    else:
      return
        err("failed to set node waku peer-exchange peer: " & peerExchangeNode.error)

  return ok()

## Start node

proc startNode*(
    node: WakuNode, conf: WakuNodeConf, dynamicBootstrapNodes: seq[RemotePeerInfo] = @[]
): Future[Result[void, string]] {.async: (raises: []).} =
  ## Start a configured node and all mounted protocols.
  ## Connect to static nodes and start
  ## keep-alive, if configured.

  # Start Waku v2 node
  try:
    await node.start()
  except CatchableError:
    return err("failed to start waku node: " & getCurrentExceptionMsg())

  # Connect to configured static nodes
  if conf.staticnodes.len > 0:
    try:
      await connectToNodes(node, conf.staticnodes, "static")
    except CatchableError:
      return err("failed to connect to static nodes: " & getCurrentExceptionMsg())

  if dynamicBootstrapNodes.len > 0:
    info "Connecting to dynamic bootstrap peers"
    try:
      await connectToNodes(node, dynamicBootstrapNodes, "dynamic bootstrap")
    except CatchableError:
      return
        err("failed to connect to dynamic bootstrap nodes: " & getCurrentExceptionMsg())

  # retrieve px peers and add the to the peer store
  if conf.peerExchangeNode != "":
    var desiredOutDegree = DefaultPXNumPeersReq
    if not node.wakuRelay.isNil() and node.wakuRelay.parameters.d.uint64() > 0:
      desiredOutDegree = node.wakuRelay.parameters.d.uint64()
    (await node.fetchPeerExchangePeers(desiredOutDegree)).isOkOr:
      error "error while fetching peers from peer exchange", error = error

  # Use px to periodically get peers if discv5 is disabled, as discv5 nodes have their own
  # periodic loop to find peers and px returned peers actually come from discv5
  if conf.peerExchange and not conf.discv5Discovery:
    node.startPeerExchangeLoop()

  # Start keepalive, if enabled
  if conf.keepAlive:
    node.startKeepalive()

  # Maintain relay connections
  if conf.relay:
    node.peerManager.start()

  return ok()

proc setupNode*(
    conf: WakuNodeConf, rng: ref HmacDrbgContext = crypto.newRng(), relay: Relay
): Result[WakuNode, string] =
  # Use provided key only if corresponding rng is also provided
  let key =
    if conf.nodeKey.isSome():
      conf.nodeKey.get()
    else:
      warn "missing key, generating new"
      crypto.PrivateKey.random(Secp256k1, rng[]).valueOr:
        error "Failed to generate key", error = error
        return err("Failed to generate key: " & $error)

  let netConfig = networkConfiguration(conf, clientId).valueOr:
    error "failed to create internal config", error = error
    return err("failed to create internal config: " & error)

  let record = enrConfiguration(conf, netConfig, key).valueOr:
    error "failed to create record", error = error
    return err("failed to create record: " & error)

  if isClusterMismatched(record, conf.clusterId):
    error "cluster id mismatch configured shards"
    return err("cluster id mismatch configured shards")

  debug "Setting up storage"

  ## Peer persistence
  var peerStore: Option[WakuPeerStorage]
  if conf.peerPersistence:
    peerStore = setupPeerStorage().valueOr:
      error "Setting up storage failed", error = "failed to setup peer store " & error
      return err("Setting up storage failed: " & error)

  debug "Initializing node"

  let node = initNode(conf, netConfig, rng, key, record, peerStore, relay).valueOr:
    error "Initializing node failed", error = error
    return err("Initializing node failed: " & error)

  debug "Mounting protocols"

  try:
    (waitFor node.setupProtocols(conf, key)).isOkOr:
      error "Mounting protocols failed", error = error
      return err("Mounting protocols failed: " & error)
  except CatchableError:
    return err("Exception setting up protocols: " & getCurrentExceptionMsg())

  return ok(node)
