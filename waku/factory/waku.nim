{.push raises: [].}

import
  std/[options, sequtils],
  results,
  chronicles,
  chronos,
  libp2p/protocols/connectivity/relay/relay,
  libp2p/protocols/connectivity/relay/client,
  libp2p/wire,
  libp2p/crypto/crypto,
  libp2p/protocols/pubsub/gossipsub,
  libp2p/services/autorelayservice,
  libp2p/services/hpservice,
  libp2p/peerid,
  libp2p/discovery/discoverymngr,
  libp2p/discovery/rendezvousinterface,
  eth/keys,
  eth/p2p/discoveryv5/enr,
  presto,
  metrics,
  metrics/chronos_httpserver
import
  ../common/logging,
  ../waku_core,
  ../waku_node,
  ../node/peer_manager,
  ../node/health_monitor,
  ../node/delivery_monitor/delivery_monitor,
  ../waku_api/message_cache,
  ../waku_api/rest/server,
  ../waku_archive,
  ../waku_relay/protocol,
  ../discovery/waku_dnsdisc,
  ../discovery/waku_discv5,
  ../discovery/autonat_service,
  ../waku_enr/sharding,
  ../waku_rln_relay,
  ../waku_store,
  ../waku_filter_v2,
  ../factory/networks_config,
  ../factory/node_factory,
  ../factory/internal_config,
  ../factory/external_config,
  ../factory/app_callbacks,
  ../waku_enr/multiaddr

logScope:
  topics = "wakunode waku"

# Git version in git describe format (defined at compile time)
const git_version* {.strdefine.} = "n/a"

type Waku* = ref object
  version: string
  conf: WakuNodeConf
  rng: ref HmacDrbgContext
  key: crypto.PrivateKey

  wakuDiscv5*: WakuDiscoveryV5
  dynamicBootstrapNodes: seq[RemotePeerInfo]
  dnsRetryLoopHandle: Future[void]
  discoveryMngr: DiscoveryManager

  node*: WakuNode

  deliveryMonitor: DeliveryMonitor

  restServer*: WakuRestServerRef
  metricsServer*: MetricsHttpServerRef
  appCallbacks*: AppCallbacks

proc logConfig(conf: WakuNodeConf) =
  info "Configuration: Enabled protocols",
    relay = conf.relay,
    rlnRelay = conf.rlnRelay,
    store = conf.store,
    filter = conf.filter,
    lightpush = conf.lightpush,
    peerExchange = conf.peerExchange

  info "Configuration. Network", cluster = conf.clusterId, maxPeers = conf.maxRelayPeers

  for shard in conf.shards:
    info "Configuration. Shards", shard = shard

  for i in conf.discv5BootstrapNodes:
    info "Configuration. Bootstrap nodes", node = i

  if conf.rlnRelay and conf.rlnRelayDynamic:
    info "Configuration. Validation",
      mechanism = "onchain rln",
      contract = conf.rlnRelayEthContractAddress,
      maxMessageSize = conf.maxMessageSize,
      rlnEpochSizeSec = conf.rlnEpochSizeSec,
      rlnRelayUserMessageLimit = conf.rlnRelayUserMessageLimit,
      rlnRelayEthClientAddress = string(conf.rlnRelayEthClientAddress)

func version*(waku: Waku): string =
  waku.version

proc validateShards(conf: WakuNodeConf): Result[void, string] =
  let numShardsInNetwork = getNumShardsInNetwork(conf)

  for shard in conf.shards:
    if shard >= numShardsInNetwork:
      let msg =
        "validateShards invalid shard: " & $shard & " when numShardsInNetwork: " &
        $numShardsInNetwork # fmt doesn't work
      error "validateShards failed", error = msg
      return err(msg)

  return ok()

proc setupSwitchServices(
    waku: Waku, conf: WakuNodeConf, circuitRelay: Relay, rng: ref HmacDrbgContext
) =
  proc onReservation(addresses: seq[MultiAddress]) {.gcsafe, raises: [].} =
    debug "circuit relay handler new reserve event",
      addrs_before = $(waku.node.announcedAddresses), addrs = $addresses

    waku.node.announcedAddresses.setLen(0) ## remove previous addresses
    waku.node.announcedAddresses.add(addresses)
    debug "waku node announced addresses updated",
      announcedAddresses = waku.node.announcedAddresses

    if not isNil(waku.wakuDiscv5):
      waku.wakuDiscv5.updateAnnouncedMultiAddress(addresses).isOkOr:
        error "failed to update announced multiaddress", error = $error

  let autonatService = getAutonatService(rng)
  if conf.isRelayClient:
    ## The node is considered to be behind a NAT or firewall and then it
    ## should struggle to be reachable and establish connections to other nodes
    const MaxNumRelayServers = 2
    let autoRelayService = AutoRelayService.new(
      MaxNumRelayServers, RelayClient(circuitRelay), onReservation, rng
    )
    let holePunchService = HPService.new(autonatService, autoRelayService)
    waku.node.switch.services = @[Service(holePunchService)]
  else:
    waku.node.switch.services = @[Service(autonatService)]

## Initialisation

proc newCircuitRelay(isRelayClient: bool): Relay =
  if isRelayClient:
    return RelayClient.new()
  return Relay.new()

proc setupAppCallbacks(
    node: WakuNode, conf: WakuNodeConf, appCallbacks: AppCallbacks
): Result[void, string] =
  if appCallbacks.isNil():
    info "No external callbacks to be set"
    return ok()

  if not appCallbacks.relayHandler.isNil():
    if node.wakuRelay.isNil():
      return err("Cannot configure relayHandler callback without Relay mounted")

    let autoShards = node.getAutoshards(conf.contentTopics).valueOr:
      return err("Could not get autoshards: " & error)

    let confShards =
      conf.shards.mapIt(RelayShard(clusterId: conf.clusterId, shardId: uint16(it)))
    let shards = confShards & autoShards

    for shard in shards:
      discard node.wakuRelay.subscribe($shard, appCallbacks.relayHandler)

  if not appCallbacks.topicHealthChangeHandler.isNil():
    if node.wakuRelay.isNil():
      return
        err("Cannot configure topicHealthChangeHandler callback without Relay mounted")
    node.wakuRelay.onTopicHealthChange = appCallbacks.topicHealthChangeHandler

  if not appCallbacks.connectionChangeHandler.isNil():
    if node.peerManager.isNil():
      return
        err("Cannot configure connectionChangeHandler callback with empty peer manager")
    node.peerManager.onConnectionChange = appCallbacks.connectionChangeHandler

  return ok()

proc new*(
    T: type Waku, confCopy: var WakuNodeConf, appCallbacks: AppCallbacks = nil
): Result[Waku, string] =
  let rng = crypto.newRng()

  logging.setupLog(confCopy.logLevel, confCopy.logFormat)

  # TODO: remove after pubsubtopic config gets removed
  var shards = newSeq[uint16]()
  if confCopy.pubsubTopics.len > 0:
    let shardsRes = topicsToRelayShards(confCopy.pubsubTopics)
    if shardsRes.isErr():
      error "failed to parse pubsub topic, please format according to static shard specification",
        error = shardsRes.error
      return err("failed to parse pubsub topic: " & $shardsRes.error)

    let shardsOpt = shardsRes.get()

    if shardsOpt.isSome():
      let relayShards = shardsOpt.get()
      if relayShards.clusterId != confCopy.clusterId:
        error "clusterId of the pubsub topic should match the node's cluster. e.g. --pubsub-topic=/waku/2/rs/22/1 and --cluster-id=22",
          nodeCluster = confCopy.clusterId, pubsubCluster = relayShards.clusterId
        return err(
          "clusterId of the pubsub topic should match the node's cluster. e.g. --pubsub-topic=/waku/2/rs/22/1 and --cluster-id=22"
        )

      for shard in relayShards.shardIds:
        shards.add(shard)
      confCopy.shards = shards

  case confCopy.clusterId

  # cluster-id=1 (aka The Waku Network)
  of 1:
    let twnClusterConf = ClusterConf.TheWakuNetworkConf()

    # Override configuration
    confCopy.maxMessageSize = twnClusterConf.maxMessageSize
    confCopy.clusterId = twnClusterConf.clusterId
    confCopy.rlnRelayEthContractAddress = twnClusterConf.rlnRelayEthContractAddress
    confCopy.rlnRelayChainId = twnClusterConf.rlnRelayChainId
    confCopy.rlnRelayDynamic = twnClusterConf.rlnRelayDynamic
    confCopy.rlnRelayBandwidthThreshold = twnClusterConf.rlnRelayBandwidthThreshold
    confCopy.discv5Discovery = twnClusterConf.discv5Discovery
    confCopy.discv5BootstrapNodes =
      confCopy.discv5BootstrapNodes & twnClusterConf.discv5BootstrapNodes
    confCopy.rlnEpochSizeSec = twnClusterConf.rlnEpochSizeSec
    confCopy.rlnRelayUserMessageLimit = twnClusterConf.rlnRelayUserMessageLimit
    confCopy.numShardsInNetwork = twnClusterConf.numShardsInNetwork

    # Only set rlnRelay to true if relay is configured
    if confCopy.relay:
      confCopy.rlnRelay = twnClusterConf.rlnRelay
  else:
    discard

  info "Running nwaku node", version = git_version
  logConfig(confCopy)

  let validateShardsRes = validateShards(confCopy)
  if validateShardsRes.isErr():
    error "Failed validating shards", error = $validateShardsRes.error
    return err("Failed validating shards: " & $validateShardsRes.error)

  if not confCopy.nodekey.isSome():
    let keyRes = crypto.PrivateKey.random(Secp256k1, rng[])
    if keyRes.isErr():
      error "Failed to generate key", error = $keyRes.error
      return err("Failed to generate key: " & $keyRes.error)
    confCopy.nodekey = some(keyRes.get())

  var relay = newCircuitRelay(confCopy.isRelayClient)

  let nodeRes = setupNode(confCopy, rng, relay)
  if nodeRes.isErr():
    error "Failed setting up node", error = nodeRes.error
    return err("Failed setting up node: " & nodeRes.error)

  let node = nodeRes.get()

  node.setupAppCallbacks(confCopy, appCallbacks).isOkOr:
    error "Failed setting up app callbacks", error = error
    return err("Failed setting up app callbacks: " & $error)

  ## Delivery Monitor
  var deliveryMonitor: DeliveryMonitor
  if confCopy.reliabilityEnabled:
    if confCopy.storenode == "":
      return err("A storenode should be set when reliability mode is on")

    let deliveryMonitorRes = DeliveryMonitor.new(
      node.wakuStoreClient, node.wakuRelay, node.wakuLightpushClient,
      node.wakuFilterClient,
    )
    if deliveryMonitorRes.isErr():
      return err("could not create delivery monitor: " & $deliveryMonitorRes.error)
    deliveryMonitor = deliveryMonitorRes.get()

  var waku = Waku(
    version: git_version,
    conf: confCopy,
    rng: rng,
    key: confCopy.nodekey.get(),
    node: node,
    deliveryMonitor: deliveryMonitor,
    appCallbacks: appCallbacks,
  )

  waku.setupSwitchServices(confCopy, relay, rng)

  ok(waku)

proc getPorts(
    listenAddrs: seq[MultiAddress]
): Result[tuple[tcpPort, websocketPort: Option[Port]], string] =
  var tcpPort, websocketPort = none(Port)

  for a in listenAddrs:
    if a.isWsAddress():
      if websocketPort.isNone():
        let wsAddress = initTAddress(a).valueOr:
          return err("getPorts wsAddr error:" & $error)
        websocketPort = some(wsAddress.port)
    elif tcpPort.isNone():
      let tcpAddress = initTAddress(a).valueOr:
        return err("getPorts tcpAddr error:" & $error)
      tcpPort = some(tcpAddress.port)

  return ok((tcpPort: tcpPort, websocketPort: websocketPort))

proc getRunningNetConfig(waku: ptr Waku): Result[NetConfig, string] =
  var conf = waku[].conf
  let (tcpPort, websocketPort) = getPorts(waku[].node.switch.peerInfo.listenAddrs).valueOr:
    return err("Could not retrieve ports " & error)

  if tcpPort.isSome():
    conf.tcpPort = tcpPort.get()

  if websocketPort.isSome():
    conf.websocketPort = websocketPort.get()

  # Rebuild NetConfig with bound port values
  let netConf = networkConfiguration(conf, clientId).valueOr:
    return err("Could not update NetConfig: " & error)

  return ok(netConf)

proc updateEnr(waku: ptr Waku): Result[void, string] =
  let netConf: NetConfig = getRunningNetConfig(waku).valueOr:
    return err("error calling updateNetConfig: " & $error)

  let record = enrConfiguration(waku[].conf, netConf, waku[].key).valueOr:
    return err("ENR setup failed: " & error)

  if isClusterMismatched(record, waku[].conf.clusterId):
    return err("cluster id mismatch configured shards")

  waku[].node.enr = record

  return ok()

proc updateAddressInENR(waku: ptr Waku): Result[void, string] =
  let addresses: seq[MultiAddress] = waku[].node.announcedAddresses
  let encodedAddrs = multiaddr.encodeMultiaddrs(addresses)

  ## First update the enr info contained in WakuNode
  let keyBytes = waku[].key.getRawBytes().valueOr:
    return err("failed to retrieve raw bytes from waku key: " & $error)

  let parsedPk = keys.PrivateKey.fromHex(keyBytes.toHex()).valueOr:
    return err("failed to parse the private key: " & $error)

  let enrFields = @[toFieldPair(MultiaddrEnrField, encodedAddrs)]
  waku[].node.enr.update(parsedPk, enrFields).isOkOr:
    return err("failed to update multiaddress in ENR updateAddressInENR: " & $error)

  debug "Waku node ENR updated successfully with new multiaddress",
    enr = waku[].node.enr.toUri(), record = $(waku[].node.enr)

  ## Now update the ENR infor in discv5
  if not waku[].wakuDiscv5.isNil():
    waku[].wakuDiscv5.protocol.localNode.record = waku[].node.enr
    let enr = waku[].wakuDiscv5.protocol.localNode.record

    debug "Waku discv5 ENR updated successfully with new multiaddress",
      enr = enr.toUri(), record = $(enr)

  return ok()

proc updateWaku(waku: ptr Waku): Result[void, string] =
  if waku[].conf.tcpPort == Port(0) or waku[].conf.websocketPort == Port(0):
    updateEnr(waku).isOkOr:
      return err("error calling updateEnr: " & $error)

  ?updateAnnouncedAddrWithPrimaryIpAddr(waku[].node)

  ?updateAddressInENR(waku)

  return ok()

proc startDnsDiscoveryRetryLoop(waku: ptr Waku): Future[void] {.async.} =
  while true:
    await sleepAsync(30.seconds)
    let dynamicBootstrapNodesRes = await waku_dnsdisc.retrieveDynamicBootstrapNodes(
      waku.conf.dnsDiscovery, waku.conf.dnsDiscoveryUrl,
      waku.conf.dnsDiscoveryNameServers,
    )
    if dynamicBootstrapNodesRes.isErr():
      error "Retrieving dynamic bootstrap nodes failed",
        error = dynamicBootstrapNodesRes.error
      continue

    waku[].dynamicBootstrapNodes = dynamicBootstrapNodesRes.get()

    if not waku[].wakuDiscv5.isNil():
      let dynamicBootstrapEnrs = waku[].dynamicBootstrapNodes
        .filterIt(it.hasUdpPort())
        .mapIt(it.enr.get().toUri())
      var discv5BootstrapEnrs: seq[enr.Record]
      # parse enrURIs from the configuration and add the resulting ENRs to the discv5BootstrapEnrs seq
      for enrUri in dynamicBootstrapEnrs:
        addBootstrapNode(enrUri, discv5BootstrapEnrs)

      waku[].wakuDiscv5.updateBootstrapRecords(
        waku[].wakuDiscv5.protocol.bootstrapRecords & discv5BootstrapEnrs
      )

    info "Connecting to dynamic bootstrap peers"
    try:
      await connectToNodes(
        waku[].node, waku[].dynamicBootstrapNodes, "dynamic bootstrap"
      )
    except CatchableError:
      error "failed to connect to dynamic bootstrap nodes: " & getCurrentExceptionMsg()
    return

proc startWaku*(waku: ptr Waku): Future[Result[void, string]] {.async.} =
  debug "Retrieve dynamic bootstrap nodes"

  let dynamicBootstrapNodesRes = await waku_dnsdisc.retrieveDynamicBootstrapNodes(
    waku.conf.dnsDiscovery, waku.conf.dnsDiscoveryUrl, waku.conf.dnsDiscoveryNameServers
  )

  if dynamicBootstrapNodesRes.isErr():
    error "Retrieving dynamic bootstrap nodes failed",
      error = dynamicBootstrapNodesRes.error
    # Start Dns Discovery retry loop
    waku[].dnsRetryLoopHandle = waku.startDnsDiscoveryRetryLoop()
  else:
    waku[].dynamicBootstrapNodes = dynamicBootstrapNodesRes.get()

  if not waku[].conf.discv5Only:
    (await startNode(waku.node, waku.conf, waku.dynamicBootstrapNodes)).isOkOr:
      return err("error while calling startNode: " & $error)

    # Update waku data that is set dynamically on node start
    updateWaku(waku).isOkOr:
      return err("Error in updateApp: " & $error)

  ## Discv5
  if waku[].conf.discv5Discovery or waku[].conf.discv5Only:
    waku[].wakuDiscV5 = waku_discv5.setupDiscoveryV5(
      waku.node.enr, waku.node.peerManager, waku.node.topicSubscriptionQueue, waku.conf,
      waku.dynamicBootstrapNodes, waku.rng, waku.key,
    )

    (await waku.wakuDiscV5.start()).isOkOr:
      return err("failed to start waku discovery v5: " & $error)

  ## Reliability
  if not waku[].deliveryMonitor.isNil():
    waku[].deliveryMonitor.startDeliveryMonitor()

  return ok()

# Waku shutdown

proc stop*(waku: Waku): Future[void] {.async: (raises: [Exception]).} =
  if not waku.restServer.isNil():
    await waku.restServer.stop()

  if not waku.metricsServer.isNil():
    await waku.metricsServer.stop()

  if not waku.wakuDiscv5.isNil():
    await waku.wakuDiscv5.stop()

  if not waku.node.isNil():
    await waku.node.stop()

  if not waku.dnsRetryLoopHandle.isNil():
    await waku.dnsRetryLoopHandle.cancelAndWait()
