{.push raises: [].}

import
  std/[options, sets, sequtils, times, strformat, strutils, math, random],
  chronos,
  chronicles,
  metrics,
  libp2p/multistream,
  libp2p/muxers/muxer,
  libp2p/nameresolving/nameresolver,
  libp2p/peerstore

import
  ../../common/nimchronos,
  ../../common/enr,
  ../../common/utils/parse_size_units,
  ../../waku_core,
  ../../waku_relay,
  ../../waku_relay/protocol,
  ../../waku_enr/sharding,
  ../../waku_enr/capabilities,
  ../../waku_metadata,
  ./peer_store/peer_storage,
  ./waku_peer_store

export waku_peer_store, peer_storage, peers

declareCounter waku_peers_dials, "Number of peer dials", ["outcome"]
# TODO: Populate from PeerStore.Source when ready
declarePublicCounter waku_node_conns_initiated,
  "Number of connections initiated", ["source"]
declarePublicGauge waku_peers_errors, "Number of peer manager errors", ["type"]
declarePublicGauge waku_connected_peers,
  "Number of physical connections per direction and protocol",
  labels = ["direction", "protocol"]
declarePublicGauge waku_streams_peers,
  "Number of streams per direction and protocol", labels = ["direction", "protocol"]
declarePublicGauge waku_peer_store_size, "Number of peers managed by the peer store"
declarePublicGauge waku_service_peers,
  "Service peer protocol and multiaddress ", labels = ["protocol", "peerId"]
declarePublicGauge waku_total_unique_peers, "total number of unique peers"

logScope:
  topics = "waku node peer_manager"

randomize()

const
  # TODO: Make configurable
  DefaultDialTimeout* = chronos.seconds(10)

  # Max attempts before removing the peer
  MaxFailedAttempts = 5

  # Time to wait before attempting to dial again is calculated as:
  # initialBackoffInSec*(backoffFactor^(failedAttempts-1))
  # 120s, 480s, 1920, 7680s
  InitialBackoffInSec = 120
  BackoffFactor = 4

  # Limit the amount of paralel dials
  MaxParallelDials = 10

  # Delay between consecutive relayConnectivityLoop runs
  ConnectivityLoopInterval = chronos.seconds(30)

  # How often the peer store is pruned
  PrunePeerStoreInterval = chronos.minutes(10)

  # How often metrics and logs are shown/updated
  LogAndMetricsInterval = chronos.minutes(3)

  # Max peers that we allow from the same IP
  DefaultColocationLimit* = 5

type ConnectionChangeHandler* = proc(
  peerId: PeerId, peerEvent: PeerEventKind
): Future[void] {.gcsafe, raises: [Defect].}

type PeerManager* = ref object of RootObj
  switch*: Switch
  wakuPeerStore*: WakuPeerStore
  wakuMetadata*: WakuMetadata
  initialBackoffInSec*: int
  backoffFactor*: int
  maxFailedAttempts*: int
  storage*: PeerStorage
  serviceSlots*: Table[string, RemotePeerInfo]
  relayServiceRatio*: string
  maxRelayPeers*: int
  maxServicePeers*: int
  outRelayPeersTarget: int
  inRelayPeersTarget: int
  ipTable*: Table[string, seq[PeerId]]
  colocationLimit*: int
  started: bool
  shardedPeerManagement: bool # temp feature flag
  onConnectionChange*: ConnectionChangeHandler

#~~~~~~~~~~~~~~~~~~~#
# Helper Functions  #
#~~~~~~~~~~~~~~~~~~~#

proc calculateBackoff(
    initialBackoffInSec: int, backoffFactor: int, failedAttempts: int
): timer.Duration =
  if failedAttempts == 0:
    return chronos.seconds(0)
  return chronos.seconds(initialBackoffInSec * (backoffFactor ^ (failedAttempts - 1)))

proc protocolMatcher*(codec: string): Matcher =
  ## Returns a protocol matcher function for the provided codec
  proc match(proto: string): bool {.gcsafe.} =
    ## Matches a proto with any postfix to the provided codec.
    ## E.g. if the codec is `/vac/waku/filter/2.0.0` it matches the protos:
    ## `/vac/waku/filter/2.0.0`, `/vac/waku/filter/2.0.0-beta3`, `/vac/waku/filter/2.0.0-actualnonsense`
    return proto.startsWith(codec)

  return match

#~~~~~~~~~~~~~~~~~~~~~~~~~~#
# Peer Storage Management  #
#~~~~~~~~~~~~~~~~~~~~~~~~~~#

proc insertOrReplace(ps: PeerStorage, remotePeerInfo: RemotePeerInfo) {.gcsafe.} =
  ## Insert peer entry into persistent storage, or replace existing entry with updated info
  ps.put(remotePeerInfo).isOkOr:
    warn "failed to store peers", err = error
    waku_peers_errors.inc(labelValues = ["storage_failure"])
    return

proc addPeer*(
    pm: PeerManager, remotePeerInfo: RemotePeerInfo, origin = UnknownOrigin
) {.gcsafe.} =
  ## Adds peer to manager for the specified protocol

  if remotePeerInfo.peerId == pm.switch.peerInfo.peerId:
    trace "skipping to manage our unmanageable self"
    return

  if pm.wakuPeerStore[AddressBook][remotePeerInfo.peerId] == remotePeerInfo.addrs and
      pm.wakuPeerStore[KeyBook][remotePeerInfo.peerId] == remotePeerInfo.publicKey and
      pm.wakuPeerStore[ENRBook][remotePeerInfo.peerId].raw.len > 0:
    let incomingEnr = remotePeerInfo.enr.valueOr:
      trace "peer already managed and incoming ENR is empty",
        remote_peer_id = $remotePeerInfo.peerId
      return

    if pm.wakuPeerStore[ENRBook][remotePeerInfo.peerId].raw == incomingEnr.raw or
        pm.wakuPeerStore[ENRBook][remotePeerInfo.peerId].seqNum > incomingEnr.seqNum:
      trace "peer already managed and ENR info is already saved",
        remote_peer_id = $remotePeerInfo.peerId
      return

  trace "Adding peer to manager",
    peerId = remotePeerInfo.peerId, addresses = remotePeerInfo.addrs

  waku_total_unique_peers.inc()

  pm.wakuPeerStore[AddressBook][remotePeerInfo.peerId] = remotePeerInfo.addrs
  pm.wakuPeerStore[KeyBook][remotePeerInfo.peerId] = remotePeerInfo.publicKey
  pm.wakuPeerStore[SourceBook][remotePeerInfo.peerId] = origin
  pm.wakuPeerStore[ProtoVersionBook][remotePeerInfo.peerId] =
    remotePeerInfo.protoVersion
  pm.wakuPeerStore[AgentBook][remotePeerInfo.peerId] = remotePeerInfo.agent

  if remotePeerInfo.protocols.len > 0:
    pm.wakuPeerStore[ProtoBook][remotePeerInfo.peerId] = remotePeerInfo.protocols

  if remotePeerInfo.enr.isSome():
    pm.wakuPeerStore[ENRBook][remotePeerInfo.peerId] = remotePeerInfo.enr.get()

  # Add peer to storage. Entry will subsequently be updated with connectedness information
  if not pm.storage.isNil:
    # Reading from the db (pm.storage) is only done on startup, hence you need to connect to all saved peers.
    # `remotePeerInfo.connectedness` should already be `NotConnected`, but both we reset it to `NotConnected` just in case.
    # This reset is also done when reading from storage, I believe, to ensure the `connectedness` state is the correct one.
    # So many resets are likely redudant, but I haven't verified whether this is the case or not.
    remotePeerInfo.connectedness = NotConnected

    pm.storage.insertOrReplace(remotePeerInfo)

proc loadFromStorage(pm: PeerManager) {.gcsafe.} =
  ## Load peers from storage, if available

  trace "loading peers from storage"

  var amount = 0

  proc onData(remotePeerInfo: RemotePeerInfo) =
    let peerId = remotePeerInfo.peerId

    if pm.switch.peerInfo.peerId == peerId:
      # Do not manage self
      return

    trace "loading peer",
      peerId = peerId,
      address = remotePeerInfo.addrs,
      protocols = remotePeerInfo.protocols,
      agent = remotePeerInfo.agent,
      version = remotePeerInfo.protoVersion

    # nim-libp2p books
    pm.wakuPeerStore[AddressBook][peerId] = remotePeerInfo.addrs
    pm.wakuPeerStore[ProtoBook][peerId] = remotePeerInfo.protocols
    pm.wakuPeerStore[KeyBook][peerId] = remotePeerInfo.publicKey
    pm.wakuPeerStore[AgentBook][peerId] = remotePeerInfo.agent
    pm.wakuPeerStore[ProtoVersionBook][peerId] = remotePeerInfo.protoVersion

    # custom books
    pm.wakuPeerStore[ConnectionBook][peerId] = NotConnected # Reset connectedness state
    pm.wakuPeerStore[DisconnectBook][peerId] = remotePeerInfo.disconnectTime
    pm.wakuPeerStore[SourceBook][peerId] = remotePeerInfo.origin

    if remotePeerInfo.enr.isSome():
      pm.wakuPeerStore[ENRBook][peerId] = remotePeerInfo.enr.get()

    amount.inc()

  pm.storage.getAll(onData).isOkOr:
    warn "loading peers from storage failed", err = error
    waku_peers_errors.inc(labelValues = ["storage_load_failure"])
    return

  trace "recovered peers from storage", amount = amount

proc selectPeer*(
    pm: PeerManager, proto: string, shard: Option[PubsubTopic] = none(PubsubTopic)
): Option[RemotePeerInfo] =
  trace "Selecting peer from peerstore", protocol = proto

  # Selects the best peer for a given protocol
  var peers = pm.wakuPeerStore.getPeersByProtocol(proto)

  if shard.isSome():
    peers.keepItIf((it.enr.isSome() and it.enr.get().containsShard(shard.get())))

  shuffle(peers)

  # No criteria for selecting a peer for WakuRelay, random one
  if proto == WakuRelayCodec:
    # TODO: proper heuristic here that compares peer scores and selects "best" one. For now the first peer for the given protocol is returned
    if peers.len > 0:
      trace "Got peer from peerstore",
        peerId = peers[0].peerId, multi = peers[0].addrs[0], protocol = proto
      return some(peers[0])
    trace "No peer found for protocol", protocol = proto
    return none(RemotePeerInfo)

  # For other protocols, we select the peer that is slotted for the given protocol
  pm.serviceSlots.withValue(proto, serviceSlot):
    trace "Got peer from service slots",
      peerId = serviceSlot[].peerId, multi = serviceSlot[].addrs[0], protocol = proto
    return some(serviceSlot[])

  # If not slotted, we select a random peer for the given protocol
  if peers.len > 0:
    trace "Got peer from peerstore",
      peerId = peers[0].peerId, multi = peers[0].addrs[0], protocol = proto
    return some(peers[0])
  trace "No peer found for protocol", protocol = proto
  return none(RemotePeerInfo)

# Adds a peer to the service slots, which is a list of peers that are slotted for a given protocol
proc addServicePeer*(pm: PeerManager, remotePeerInfo: RemotePeerInfo, proto: string) =
  # Do not add relay peers
  if proto == WakuRelayCodec:
    warn "Can't add relay peer to service peers slots"
    return

  # Check if the number of service peers has reached the maximum limit
  if pm.serviceSlots.len >= pm.maxServicePeers:
    warn "Maximum number of service peers reached. Cannot add more.",
      peerId = remotePeerInfo.peerId, service = proto
    return

  info "Adding peer to service slots",
    peerId = remotePeerInfo.peerId, addr = remotePeerInfo.addrs[0], service = proto
  waku_service_peers.set(1, labelValues = [$proto, $remotePeerInfo.addrs[0]])

    # Set peer for service slot
  pm.serviceSlots[proto] = remotePeerInfo

  pm.addPeer(remotePeerInfo)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# Connection Lifecycle Management #
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

# require pre-connection
proc pruneInRelayConns(pm: PeerManager, amount: int) {.async.}

# Connects to a given node. Note that this function uses `connect` and
# does not provide a protocol. Streams for relay (gossipsub) are created
# automatically without the needing to dial.
proc connectPeer*(
    pm: PeerManager,
    peer: RemotePeerInfo,
    dialTimeout = DefaultDialTimeout,
    source = "api",
): Future[bool] {.async.} =
  let peerId = peer.peerId

  # Do not attempt to dial self
  if peerId == pm.switch.peerInfo.peerId:
    return false

  if not pm.wakuPeerStore.peerExists(peerId):
    pm.addPeer(peer)

  let failedAttempts = pm.wakuPeerStore[NumberFailedConnBook][peerId]
  trace "Connecting to peer",
    wireAddr = peer.addrs, peerId = peerId, failedAttempts = failedAttempts

  var deadline = sleepAsync(dialTimeout)
  let workfut = pm.switch.connect(peerId, peer.addrs)

  # Can't use catch: with .withTimeout() in this case
  let res = catch:
    await workfut or deadline

  let reasonFailed =
    if not workfut.finished():
      await workfut.cancelAndWait()
      "timed out"
    elif res.isErr():
      res.error.msg
    else:
      if not deadline.finished():
        await deadline.cancelAndWait()

      waku_peers_dials.inc(labelValues = ["successful"])
      waku_node_conns_initiated.inc(labelValues = [source])

      pm.wakuPeerStore[NumberFailedConnBook][peerId] = 0

      return true

  # Dial failed
  pm.wakuPeerStore[NumberFailedConnBook][peerId] =
    pm.wakuPeerStore[NumberFailedConnBook][peerId] + 1
  pm.wakuPeerStore[LastFailedConnBook][peerId] = Moment.init(getTime().toUnix, Second)
  pm.wakuPeerStore[ConnectionBook][peerId] = CannotConnect

  trace "Connecting peer failed",
    peerId = peerId,
    reason = reasonFailed,
    failedAttempts = pm.wakuPeerStore[NumberFailedConnBook][peerId]
  waku_peers_dials.inc(labelValues = [reasonFailed])

  return false

proc connectToNodes*(
    pm: PeerManager,
    nodes: seq[string] | seq[RemotePeerInfo],
    dialTimeout = DefaultDialTimeout,
    source = "api",
) {.async.} =
  if nodes.len == 0:
    return

  info "Dialing multiple peers", numOfPeers = nodes.len, nodes = $nodes

  var futConns: seq[Future[bool]]
  var connectedPeers: seq[RemotePeerInfo]
  for node in nodes:
    let node = parsePeerInfo(node)
    if node.isOk():
      futConns.add(pm.connectPeer(node.value))
      connectedPeers.add(node.value)
    else:
      error "Couldn't parse node info", error = node.error

  await allFutures(futConns)

  # Filtering successful connectedPeers based on futConns
  let combined = zip(connectedPeers, futConns)
  connectedPeers = combined.filterIt(it[1].read() == true).mapIt(it[0])

  when defined(debugDiscv5):
    let peerIds = connectedPeers.mapIt(it.peerId)
    let origin = connectedPeers.mapIt(it.origin)
    if peerIds.len > 0:
      notice "established connections with found peers",
        peerIds = peerIds.mapIt(shortLog(it)), origin = origin
    else:
      notice "could not connect to new peers", attempted = nodes.len

  info "Finished dialing multiple peers",
    successfulConns = connectedPeers.len, attempted = nodes.len

  # The issue seems to be around peers not being fully connected when
  # trying to subscribe. So what we do is sleep to guarantee nodes are
  # fully connected.
  #
  # This issue was known to Dmitiry on nim-libp2p and may be resolvable
  # later.
  await sleepAsync(chronos.seconds(5))

proc disconnectNode*(pm: PeerManager, peerId: PeerId) {.async.} =
  await pm.switch.disconnect(peerId)

proc disconnectNode*(pm: PeerManager, peer: RemotePeerInfo) {.async.} =
  let peerId = peer.peerId
  await pm.disconnectNode(peerId)

# Dialing should be used for just protocols that require a stream to write and read
# This shall not be used to dial Relay protocols, since that would create
# unneccesary unused streams.
proc dialPeer(
    pm: PeerManager,
    peerId: PeerID,
    addrs: seq[MultiAddress],
    proto: string,
    dialTimeout = DefaultDialTimeout,
    source = "api",
): Future[Option[Connection]] {.async.} =
  if peerId == pm.switch.peerInfo.peerId:
    error "could not dial self"
    return none(Connection)

  if proto == WakuRelayCodec:
    error "dial shall not be used to connect to relays"
    return none(Connection)

  trace "Dialing peer", wireAddr = addrs, peerId = peerId, proto = proto

  # Dial Peer
  let dialFut = pm.switch.dial(peerId, addrs, proto)

  let res = catch:
    if await dialFut.withTimeout(dialTimeout):
      return some(dialFut.read())
    else:
      await cancelAndWait(dialFut)

  let reasonFailed = if res.isOk: "timed out" else: res.error.msg

  trace "Dialing peer failed", peerId = peerId, reason = reasonFailed, proto = proto

  return none(Connection)

proc dialPeer*(
    pm: PeerManager,
    remotePeerInfo: RemotePeerInfo,
    proto: string,
    dialTimeout = DefaultDialTimeout,
    source = "api",
): Future[Option[Connection]] {.async.} =
  # Dial a given peer and add it to the list of known peers
  # TODO: check peer validity and score before continuing. Limit number of peers to be managed.

  # First add dialed peer info to peer store, if it does not exist yet..
  # TODO: nim libp2p peerstore already adds them
  if not pm.wakuPeerStore.hasPeer(remotePeerInfo.peerId, proto):
    trace "Adding newly dialed peer to manager",
      peerId = $remotePeerInfo.peerId, address = $remotePeerInfo.addrs[0], proto = proto
    pm.addPeer(remotePeerInfo)

  return await pm.dialPeer(
    remotePeerInfo.peerId, remotePeerInfo.addrs, proto, dialTimeout, source
  )

proc dialPeer*(
    pm: PeerManager,
    peerId: PeerID,
    proto: string,
    dialTimeout = DefaultDialTimeout,
    source = "api",
): Future[Option[Connection]] {.async.} =
  # Dial an existing peer by looking up it's existing addrs in the switch's peerStore
  # TODO: check peer validity and score before continuing. Limit number of peers to be managed.

  let addrs = pm.switch.peerStore[AddressBook][peerId]
  return await pm.dialPeer(peerId, addrs, proto, dialTimeout, source)

proc canBeConnected*(pm: PeerManager, peerId: PeerId): bool =
  # Returns if we can try to connect to this peer, based on past failed attempts
  # It uses an exponential backoff. Each connection attempt makes us
  # wait more before trying again.
  let failedAttempts = pm.wakuPeerStore[NumberFailedConnBook][peerId]

  # if it never errored, we can try to connect
  if failedAttempts == 0:
    return true

  # if there are too many failed attempts, do not reconnect
  if failedAttempts >= pm.maxFailedAttempts:
    return false

  # If it errored we wait an exponential backoff from last connection
  # the more failed attempts, the greater the backoff since last attempt
  let now = Moment.init(getTime().toUnix, Second)
  let lastFailed = pm.wakuPeerStore[LastFailedConnBook][peerId]
  let backoff =
    calculateBackoff(pm.initialBackoffInSec, pm.backoffFactor, failedAttempts)

  return now >= (lastFailed + backoff)

proc connectedPeers*(
    pm: PeerManager, protocol: string = ""
): (seq[PeerId], seq[PeerId]) =
  ## Returns the peerIds of physical connections (in and out)
  ## If a protocol is specified, only returns peers with at least one stream of that protocol

  var inPeers: seq[PeerId]
  var outPeers: seq[PeerId]

  for peerId, muxers in pm.switch.connManager.getConnections():
    for peerConn in muxers:
      let streams = peerConn.getStreams()
      if protocol.len == 0 or streams.anyIt(it.protocol == protocol):
        if peerConn.connection.transportDir == Direction.In:
          inPeers.add(peerId)
        elif peerConn.connection.transportDir == Direction.Out:
          outPeers.add(peerId)

  return (inPeers, outPeers)

proc connectToRelayPeers*(pm: PeerManager) {.async.} =
  var (inRelayPeers, outRelayPeers) = pm.connectedPeers(WakuRelayCodec)
  let totalRelayPeers = inRelayPeers.len + outRelayPeers.len

  if inRelayPeers.len > pm.inRelayPeersTarget:
    await pm.pruneInRelayConns(inRelayPeers.len - pm.inRelayPeersTarget)

  if outRelayPeers.len >= pm.outRelayPeersTarget:
    return

  let notConnectedPeers = pm.wakuPeerStore.getDisconnectedPeers()

  var outsideBackoffPeers = notConnectedPeers.filterIt(pm.canBeConnected(it.peerId))

  shuffle(outsideBackoffPeers)

  var index = 0
  var numPendingConnReqs =
    min(outsideBackoffPeers.len, pm.outRelayPeersTarget - outRelayPeers.len)
    ## number of outstanding connection requests

  while numPendingConnReqs > 0 and outRelayPeers.len < pm.outRelayPeersTarget:
    let numPeersToConnect = min(numPendingConnReqs, MaxParallelDials)
    await pm.connectToNodes(outsideBackoffPeers[index ..< (index + numPeersToConnect)])

    (inRelayPeers, outRelayPeers) = pm.connectedPeers(WakuRelayCodec)

    index += numPeersToConnect
    numPendingConnReqs -= numPeersToConnect

proc reconnectPeers*(
    pm: PeerManager, proto: string, backoffTime: chronos.Duration = chronos.seconds(0)
) {.async.} =
  ## Reconnect to peers registered for this protocol. This will update connectedness.
  ## Especially useful to resume connections from persistent storage after a restart.

  debug "Reconnecting peers", proto = proto

  # Proto is not persisted, we need to iterate over all peers.
  for peerInfo in pm.wakuPeerStore.peers(protocolMatcher(proto)):
    # Check that the peer can be connected
    if peerInfo.connectedness == CannotConnect:
      error "Not reconnecting to unreachable or non-existing peer",
        peerId = peerInfo.peerId
      continue

    if backoffTime > ZeroDuration:
      debug "Backing off before reconnect",
        peerId = peerInfo.peerId, backoffTime = backoffTime
      # We disconnected recently and still need to wait for a backoff period before connecting
      await sleepAsync(backoffTime)

    await pm.connectToNodes(@[peerInfo])

proc getNumStreams*(pm: PeerManager, protocol: string): (int, int) =
  var
    numStreamsIn = 0
    numStreamsOut = 0
  for peerId, muxers in pm.switch.connManager.getConnections():
    for peerConn in muxers:
      for stream in peerConn.getStreams():
        if stream.protocol == protocol:
          if stream.dir == Direction.In:
            numStreamsIn += 1
          elif stream.dir == Direction.Out:
            numStreamsOut += 1
  return (numStreamsIn, numStreamsOut)

proc getPeerIp(pm: PeerManager, peerId: PeerId): Option[string] =
  if not pm.switch.connManager.getConnections().hasKey(peerId):
    return none(string)

  let conns = pm.switch.connManager.getConnections().getOrDefault(peerId)
  if conns.len == 0:
    return none(string)

  let obAddr = conns[0].connection.observedAddr.valueOr:
    return none(string)

  # TODO: think if circuit relay ips should be handled differently

  return some(obAddr.getHostname())

#~~~~~~~~~~~~~~~~~#
# Event Handling  #
#~~~~~~~~~~~~~~~~~#

proc onPeerMetadata(pm: PeerManager, peerId: PeerId) {.async.} =
  let res = catch:
    await pm.switch.dial(peerId, WakuMetadataCodec)

  var reason: string
  block guardClauses:
    let conn = res.valueOr:
      reason = "dial failed: " & error.msg
      break guardClauses

    let metadata = (await pm.wakuMetadata.request(conn)).valueOr:
      reason = "waku metatdata request failed: " & error
      break guardClauses

    let clusterId = metadata.clusterId.valueOr:
      reason = "empty cluster-id reported"
      break guardClauses

    if pm.wakuMetadata.clusterId != clusterId:
      reason =
        "different clusterId reported: " & $pm.wakuMetadata.clusterId & " vs " &
        $clusterId
      break guardClauses

    if (
      pm.wakuPeerStore.hasPeer(peerId, WakuRelayCodec) and
      not metadata.shards.anyIt(pm.wakuMetadata.shards.contains(it))
    ):
      let myShardsString = "[ " & toSeq(pm.wakuMetadata.shards).join(", ") & " ]"
      let otherShardsString = "[ " & metadata.shards.join(", ") & " ]"
      reason =
        "no shards in common: my_shards = " & myShardsString & " others_shards = " &
        otherShardsString
      break guardClauses

    return

  info "disconnecting from peer", peerId = peerId, reason = reason
  asyncSpawn(pm.switch.disconnect(peerId))
  pm.wakuPeerStore.delete(peerId)

# called when a peer i) first connects to us ii) disconnects all connections from us
proc onPeerEvent(pm: PeerManager, peerId: PeerId, event: PeerEvent) {.async.} =
  if not pm.wakuMetadata.isNil() and event.kind == PeerEventKind.Joined:
    await pm.onPeerMetadata(peerId)

  var direction: PeerDirection
  var connectedness: Connectedness

  case event.kind
  of Joined:
    direction = if event.initiator: Outbound else: Inbound
    connectedness = Connected

    ## Check max allowed in-relay peers
    let inRelayPeers = pm.connectedPeers(WakuRelayCodec)[0]
    if inRelayPeers.len > pm.inRelayPeersTarget and
        pm.wakuPeerStore.hasPeer(peerId, WakuRelayCodec):
      debug "disconnecting relay peer because reached max num in-relay peers",
        peerId = peerId,
        inRelayPeers = inRelayPeers.len,
        inRelayPeersTarget = pm.inRelayPeersTarget
      await pm.switch.disconnect(peerId)

    ## Apply max ip colocation limit
    if (let ip = pm.getPeerIp(peerId); ip.isSome()):
      pm.ipTable.mgetOrPut(ip.get, newSeq[PeerId]()).add(peerId)

      # in theory this should always be one, but just in case
      let peersBehindIp = pm.ipTable[ip.get]

      # pm.colocationLimit == 0 disables the ip colocation limit
      if pm.colocationLimit != 0 and peersBehindIp.len > pm.colocationLimit:
        for peerId in peersBehindIp[0 ..< (peersBehindIp.len - pm.colocationLimit)]:
          debug "Pruning connection due to ip colocation", peerId = peerId, ip = ip
          asyncSpawn(pm.switch.disconnect(peerId))
          pm.wakuPeerStore.delete(peerId)
    if not pm.onConnectionChange.isNil():
      # we don't want to await for the callback to finish
      asyncSpawn pm.onConnectionChange(peerId, Joined)
  of Left:
    direction = UnknownDirection
    connectedness = CanConnect

    # note we cant access the peerId ip here as the connection was already closed
    for ip, peerIds in pm.ipTable.pairs:
      if peerIds.contains(peerId):
        pm.ipTable[ip] = pm.ipTable[ip].filterIt(it != peerId)
        if pm.ipTable[ip].len == 0:
          pm.ipTable.del(ip)
        break
    if not pm.onConnectionChange.isNil():
      # we don't want to await for the callback to finish
      asyncSpawn pm.onConnectionChange(peerId, Left)
  of Identified:
    debug "event identified", peerId = peerId

  pm.wakuPeerStore[ConnectionBook][peerId] = connectedness
  pm.wakuPeerStore[DirectionBook][peerId] = direction

  if not pm.storage.isNil:
    var remotePeerInfo = pm.wakuPeerStore.getPeer(peerId)

    if event.kind == PeerEventKind.Left:
      remotePeerInfo.disconnectTime = getTime().toUnix

    pm.storage.insertOrReplace(remotePeerInfo)

#~~~~~~~~~~~~~~~~~#
# Metrics Logging #
#~~~~~~~~~~~~~~~~~#

proc logAndMetrics(pm: PeerManager) {.async.} =
  heartbeat "Scheduling log and metrics run", LogAndMetricsInterval:
    # log metrics
    let (inRelayPeers, outRelayPeers) = pm.connectedPeers(WakuRelayCodec)
    let maxConnections = pm.switch.connManager.inSema.size
    let notConnectedPeers = pm.wakuPeerStore.getDisconnectedPeers().mapIt(
        RemotePeerInfo.init(it.peerId, it.addrs)
      )
    let outsideBackoffPeers = notConnectedPeers.filterIt(pm.canBeConnected(it.peerId))
    let totalConnections = pm.switch.connManager.getConnections().len

    info "Relay peer connections",
      inRelayConns = $inRelayPeers.len & "/" & $pm.inRelayPeersTarget,
      outRelayConns = $outRelayPeers.len & "/" & $pm.outRelayPeersTarget,
      totalConnections = $totalConnections & "/" & $maxConnections,
      notConnectedPeers = notConnectedPeers.len,
      outsideBackoffPeers = outsideBackoffPeers.len

    # update prometheus metrics
    for proto in pm.wakuPeerStore.getWakuProtos():
      let (protoConnsIn, protoConnsOut) = pm.connectedPeers(proto)
      let (protoStreamsIn, protoStreamsOut) = pm.getNumStreams(proto)
      waku_connected_peers.set(
        protoConnsIn.len.float64, labelValues = [$Direction.In, proto]
      )
      waku_connected_peers.set(
        protoConnsOut.len.float64, labelValues = [$Direction.Out, proto]
      )
      waku_streams_peers.set(
        protoStreamsIn.float64, labelValues = [$Direction.In, proto]
      )
      waku_streams_peers.set(
        protoStreamsOut.float64, labelValues = [$Direction.Out, proto]
      )

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# Pruning and Maintenance (Stale Peers Management)    #
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

proc manageRelayPeers*(pm: PeerManager) {.async.} =
  if pm.wakuMetadata.shards.len == 0:
    return

  var peersToConnect: HashSet[PeerId] # Can't use RemotePeerInfo as they are ref objects
  var peersToDisconnect: int

  # Get all connected peers for Waku Relay
  var (inPeers, outPeers) = pm.connectedPeers(WakuRelayCodec)

  # Calculate in/out target number of peers for each shards
  let inTarget = pm.inRelayPeersTarget div pm.wakuMetadata.shards.len
  let outTarget = pm.outRelayPeersTarget div pm.wakuMetadata.shards.len

  for shard in pm.wakuMetadata.shards.items:
    # Filter out peer not on this shard
    let connectedInPeers = inPeers.filterIt(
      pm.wakuPeerStore.hasShard(it, uint16(pm.wakuMetadata.clusterId), uint16(shard))
    )

    let connectedOutPeers = outPeers.filterIt(
      pm.wakuPeerStore.hasShard(it, uint16(pm.wakuMetadata.clusterId), uint16(shard))
    )

    # Calculate the difference between current values and targets
    let inPeerDiff = connectedInPeers.len - inTarget
    let outPeerDiff = outTarget - connectedOutPeers.len

    if inPeerDiff > 0:
      peersToDisconnect += inPeerDiff

    if outPeerDiff <= 0:
      continue

    # Get all peers for this shard
    var connectablePeers =
      pm.wakuPeerStore.getPeersByShard(uint16(pm.wakuMetadata.clusterId), uint16(shard))

    let shardCount = connectablePeers.len

    connectablePeers.keepItIf(
      not pm.wakuPeerStore.isConnected(it.peerId) and pm.canBeConnected(it.peerId)
    )

    let connectableCount = connectablePeers.len

    connectablePeers.keepItIf(pm.wakuPeerStore.hasCapability(it.peerId, Relay))

    let relayCount = connectablePeers.len

    debug "Sharded Peer Management",
      shard = shard,
      connectable = $connectableCount & "/" & $shardCount,
      relayConnectable = $relayCount & "/" & $shardCount,
      relayInboundTarget = $connectedInPeers.len & "/" & $inTarget,
      relayOutboundTarget = $connectedOutPeers.len & "/" & $outTarget

    # Always pick random connectable relay peers
    shuffle(connectablePeers)

    let length = min(outPeerDiff, connectablePeers.len)
    for peer in connectablePeers[0 ..< length]:
      trace "Peer To Connect To", peerId = $peer.peerId
      peersToConnect.incl(peer.peerId)

  await pm.pruneInRelayConns(peersToDisconnect)

  if peersToConnect.len == 0:
    return

  let uniquePeers = toSeq(peersToConnect).mapIt(pm.wakuPeerStore.getPeer(it))

  # Connect to all nodes
  for i in countup(0, uniquePeers.len, MaxParallelDials):
    let stop = min(i + MaxParallelDials, uniquePeers.len)
    trace "Connecting to Peers", peerIds = $uniquePeers[i ..< stop]
    await pm.connectToNodes(uniquePeers[i ..< stop])

proc prunePeerStore*(pm: PeerManager) =
  let numPeers = pm.wakuPeerStore[AddressBook].book.len
  let capacity = pm.wakuPeerStore.getCapacity()
  if numPeers <= capacity:
    return

  trace "Peer store capacity exceeded", numPeers = numPeers, capacity = capacity
  let pruningCount = numPeers - capacity
  var peersToPrune: HashSet[PeerId]

  # prune failed connections
  for peerId, count in pm.wakuPeerStore[NumberFailedConnBook].book.pairs:
    if count < pm.maxFailedAttempts:
      continue

    if peersToPrune.len >= pruningCount:
      break

    peersToPrune.incl(peerId)

  var notConnected = pm.wakuPeerStore.getDisconnectedPeers().mapIt(it.peerId)

  # Always pick random non-connected peers
  shuffle(notConnected)

  var shardlessPeers: seq[PeerId]
  var peersByShard = initTable[uint16, seq[PeerId]]()

  for peer in notConnected:
    if not pm.wakuPeerStore[ENRBook].contains(peer):
      shardlessPeers.add(peer)
      continue

    let record = pm.wakuPeerStore[ENRBook][peer]

    let rec = record.toTyped().valueOr:
      shardlessPeers.add(peer)
      continue

    let rs = rec.relaySharding().valueOr:
      shardlessPeers.add(peer)
      continue

    for shard in rs.shardIds:
      peersByShard.mgetOrPut(shard, @[]).add(peer)

  # prune not connected peers without shard
  for peer in shardlessPeers:
    if peersToPrune.len >= pruningCount:
      break

    peersToPrune.incl(peer)

  # calculate the avg peers per shard
  let total = sum(toSeq(peersByShard.values).mapIt(it.len))
  let avg = min(1, total div max(1, peersByShard.len))

  # prune peers from shard with higher than avg count
  for shard, peers in peersByShard.pairs:
    let count = max(peers.len - avg, 0)
    for peer in peers[0 .. count]:
      if peersToPrune.len >= pruningCount:
        break

      peersToPrune.incl(peer)

  for peer in peersToPrune:
    pm.wakuPeerStore.delete(peer)

  let afterNumPeers = pm.wakuPeerStore[AddressBook].book.len

  trace "Finished pruning peer store",
    beforeNumPeers = numPeers,
    afterNumPeers = afterNumPeers,
    capacity = capacity,
    pruned = peersToPrune.len

# Prunes peers from peerstore to remove old/stale ones
proc prunePeerStoreLoop(pm: PeerManager) {.async.} =
  trace "Starting prune peerstore loop"
  while pm.started:
    pm.prunePeerStore()
    await sleepAsync(PrunePeerStoreInterval)

# Ensures a healthy amount of connected relay peers
proc relayConnectivityLoop*(pm: PeerManager) {.async.} =
  trace "Starting relay connectivity loop"
  while pm.started:
    if pm.shardedPeerManagement:
      await pm.manageRelayPeers()
    else:
      await pm.connectToRelayPeers()
    let
      (inRelayPeers, outRelayPeers) = pm.connectedPeers(WakuRelayCodec)
      excessInConns = max(inRelayPeers.len - pm.inRelayPeersTarget, 0)

      # One minus the percentage of excess connections relative to the target, limited to 100%
      # We calculate one minus this percentage because we want the factor to be inversely proportional to the number of excess peers
      inFactor = 1 - min(excessInConns / pm.inRelayPeersTarget, 1)
      # Percentage of out relay peers relative to the target
      outFactor = min(outRelayPeers.len / pm.outRelayPeersTarget, 1)
      factor = min(outFactor, inFactor)
      dynamicSleepInterval =
        chronos.seconds(int(float(ConnectivityLoopInterval.seconds()) * factor))

    # Shorten the connectivity loop interval dynamically based on percentage of peers to fill or connections to prune
    await sleepAsync(max(dynamicSleepInterval, chronos.seconds(1)))

proc pruneInRelayConns(pm: PeerManager, amount: int) {.async.} =
  if amount <= 0:
    return

  let (inRelayPeers, _) = pm.connectedPeers(WakuRelayCodec)
  let connsToPrune = min(amount, inRelayPeers.len)

  for p in inRelayPeers[0 ..< connsToPrune]:
    trace "Pruning Peer", Peer = $p
    asyncSpawn(pm.switch.disconnect(p))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# Initialization and Constructor #
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

proc start*(pm: PeerManager) =
  pm.started = true
  asyncSpawn pm.relayConnectivityLoop()
  asyncSpawn pm.prunePeerStoreLoop()
  asyncSpawn pm.logAndMetrics()

proc stop*(pm: PeerManager) =
  pm.started = false

proc new*(
    T: type PeerManager,
    switch: Switch,
    wakuMetadata: WakuMetadata = nil,
    maxRelayPeers: Option[int] = none(int),
    maxServicePeers: Option[int] = none(int),
    relayServiceRatio: string = "60:40",
    storage: PeerStorage = nil,
    initialBackoffInSec = InitialBackoffInSec,
    backoffFactor = BackoffFactor,
    maxFailedAttempts = MaxFailedAttempts,
    colocationLimit = DefaultColocationLimit,
    shardedPeerManagement = false,
): PeerManager {.gcsafe.} =
  let capacity = switch.peerStore.capacity
  let maxConnections = switch.connManager.inSema.size
  if maxConnections > capacity:
    error "Max number of connections can't be greater than PeerManager capacity",
      capacity = capacity, maxConnections = maxConnections
    raise newException(
      Defect, "Max number of connections can't be greater than PeerManager capacity"
    )

  var relayRatio: float64
  var serviceRatio: float64
  (relayRatio, serviceRatio) = parseRelayServiceRatio(relayServiceRatio).get()

  var relayPeers = int(ceil(float(maxConnections) * relayRatio))
  var servicePeers = int(floor(float(maxConnections) * serviceRatio))

  let minRelayPeers = WakuRelay.getDHigh()

  if relayPeers < minRelayPeers:
    let errorMsg =
      fmt"""Doesn't fulfill minimum criteria for relay (which increases the chance of the node becoming isolated.)
    relayPeers: {relayPeers}, should be greater or equal than minRelayPeers: {minRelayPeers}
    relayServiceRatio: {relayServiceRatio}
    maxConnections: {maxConnections}"""
    error "Wrong relay peers config", error = errorMsg
    return

  let outRelayPeersTarget = relayPeers div 3
  let inRelayPeersTarget = relayPeers - outRelayPeersTarget

  # attempt to calculate max backoff to prevent potential overflows or unreasonably high values
  let backoff = calculateBackoff(initialBackoffInSec, backoffFactor, maxFailedAttempts)
  if backoff.weeks() > 1:
    error "Max backoff time can't be over 1 week", maxBackoff = backoff
    raise newException(Defect, "Max backoff time can't be over 1 week")

  let pm = PeerManager(
    switch: switch,
    wakuMetadata: wakuMetadata,
    wakuPeerStore: createWakuPeerStore(switch.peerStore),
    storage: storage,
    initialBackoffInSec: initialBackoffInSec,
    backoffFactor: backoffFactor,
    maxRelayPeers: relayPeers,
    maxServicePeers: servicePeers,
    outRelayPeersTarget: outRelayPeersTarget,
    inRelayPeersTarget: inRelayPeersTarget,
    maxFailedAttempts: maxFailedAttempts,
    colocationLimit: colocationLimit,
    shardedPeerManagement: shardedPeerManagement,
  )

  proc peerHook(peerId: PeerId, event: PeerEvent): Future[void] {.gcsafe.} =
    onPeerEvent(pm, peerId, event)

  proc peerStoreChanged(peerId: PeerId) {.gcsafe.} =
    waku_peer_store_size.set(toSeq(pm.wakuPeerStore[AddressBook].book.keys).len.int64)

  pm.switch.addPeerEventHandler(peerHook, PeerEventKind.Joined)
  pm.switch.addPeerEventHandler(peerHook, PeerEventKind.Left)

  # called every time the peerstore is updated
  pm.wakuPeerStore[AddressBook].addHandler(peerStoreChanged)

  pm.serviceSlots = initTable[string, RemotePeerInfo]()
  pm.ipTable = initTable[string, seq[PeerId]]()

  if not storage.isNil():
    trace "found persistent peer storage"
    pm.loadFromStorage() # Load previously managed peers.
  else:
    trace "no peer storage found"

  return pm
