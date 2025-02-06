{.push raises: [].}

import
  std/[strformat, sequtils, tables],
  chronicles,
  json_serialization,
  presto/route,
  libp2p/[peerinfo, switch]

import
  ../../../waku_core,
  ../../../waku_store_legacy/common,
  ../../../waku_store/common,
  ../../../waku_filter_v2,
  ../../../waku_lightpush_legacy/common,
  ../../../waku_relay,
  ../../../waku_peer_exchange,
  ../../../waku_node,
  ../../../node/peer_manager,
  ../responses,
  ../serdes,
  ../rest_serdes,
  ./types

export types

logScope:
  topics = "waku node rest admin api"

const ROUTE_ADMIN_V1_PEERS* = "/admin/v1/peers"
const ROUTE_ADMIN_V1_FILTER_SUBS* = "/admin/v1/filter/subscriptions"

type PeerProtocolTuple =
  tuple[multiaddr: string, protocol: string, connected: bool, origin: PeerOrigin]

proc tuplesToWakuPeers(peers: var WakuPeers, peersTup: seq[PeerProtocolTuple]) =
  for peer in peersTup:
    peers.add(peer.multiaddr, peer.protocol, peer.connected, peer.origin)

proc installAdminV1GetPeersHandler(router: var RestRouter, node: WakuNode) =
  router.api(MethodGet, ROUTE_ADMIN_V1_PEERS) do() -> RestApiResponse:
    var peers: WakuPeers = @[]

    let relayPeers = node.peerManager.wakuPeerStore.peers(WakuRelayCodec).mapIt(
        (
          multiaddr: constructMultiaddrStr(it),
          protocol: WakuRelayCodec,
          connected: it.connectedness == Connectedness.Connected,
          origin: it.origin,
        )
      )
    tuplesToWakuPeers(peers, relayPeers)

    let filterV2Peers = node.peerManager.wakuPeerStore
      .peers(WakuFilterSubscribeCodec)
      .mapIt(
        (
          multiaddr: constructMultiaddrStr(it),
          protocol: WakuFilterSubscribeCodec,
          connected: it.connectedness == Connectedness.Connected,
          origin: it.origin,
        )
      )
    tuplesToWakuPeers(peers, filterV2Peers)

    let storePeers = node.peerManager.wakuPeerStore.peers(WakuStoreCodec).mapIt(
        (
          multiaddr: constructMultiaddrStr(it),
          protocol: WakuStoreCodec,
          connected: it.connectedness == Connectedness.Connected,
          origin: it.origin,
        )
      )
    tuplesToWakuPeers(peers, storePeers)

    let legacyStorePeers = node.peerManager.wakuPeerStore
      .peers(WakuLegacyStoreCodec)
      .mapIt(
        (
          multiaddr: constructMultiaddrStr(it),
          protocol: WakuLegacyStoreCodec,
          connected: it.connectedness == Connectedness.Connected,
          origin: it.origin,
        )
      )
    tuplesToWakuPeers(peers, legacyStorePeers)

    let legacyLightpushPeers = node.peerManager.wakuPeerStore
      .peers(WakuLegacyLightPushCodec)
      .mapIt(
        (
          multiaddr: constructMultiaddrStr(it),
          protocol: WakuLegacyLightPushCodec,
          connected: it.connectedness == Connectedness.Connected,
          origin: it.origin,
        )
      )
    tuplesToWakuPeers(peers, legacyLightpushPeers)

    let lightpushPeers = node.peerManager.wakuPeerStore.peers(WakuLightPushCodec).mapIt(
        (
          multiaddr: constructMultiaddrStr(it),
          protocol: WakuLightPushCodec,
          connected: it.connectedness == Connectedness.Connected,
          origin: it.origin,
        )
      )
    tuplesToWakuPeers(peers, lightpushPeers)

    let pxPeers = node.peerManager.wakuPeerStore.peers(WakuPeerExchangeCodec).mapIt(
        (
          multiaddr: constructMultiaddrStr(it),
          protocol: WakuPeerExchangeCodec,
          connected: it.connectedness == Connectedness.Connected,
          origin: it.origin,
        )
      )
    tuplesToWakuPeers(peers, pxPeers)

    let resp = RestApiResponse.jsonResponse(peers, status = Http200)
    if resp.isErr():
      error "An error ocurred while building the json respose: ", error = resp.error
      return RestApiResponse.internalServerError(
        fmt("An error ocurred while building the json respose: {resp.error}")
      )

    return resp.get()

proc installAdminV1PostPeersHandler(router: var RestRouter, node: WakuNode) =
  router.api(MethodPost, ROUTE_ADMIN_V1_PEERS) do(
    contentBody: Option[ContentBody]
  ) -> RestApiResponse:
    let peers: seq[string] = decodeRequestBody[seq[string]](contentBody).valueOr:
      let e = $error
      return RestApiResponse.badRequest(fmt("Failed to decode request: {e}"))

    for i, peer in peers:
      let peerInfo = parsePeerInfo(peer).valueOr:
        let e = $error
        return RestApiResponse.badRequest(fmt("Couldn't parse remote peer info: {e}"))

      if not (await node.peerManager.connectPeer(peerInfo, source = "rest")):
        return RestApiResponse.badRequest(
          fmt("Failed to connect to peer at index: {i} - {peer}")
        )

    return RestApiResponse.ok()

proc installAdminV1GetFilterSubsHandler(router: var RestRouter, node: WakuNode) =
  router.api(MethodGet, ROUTE_ADMIN_V1_FILTER_SUBS) do() -> RestApiResponse:
    if node.wakuFilter.isNil():
      return
        RestApiResponse.badRequest("Error: Filter Protocol is not mounted to the node")

    var
      subscriptions: seq[FilterSubscription] = @[]
      filterCriteria: seq[FilterTopic]

    for peerId in node.wakuFilter.subscriptions.peersSubscribed.keys:
      filterCriteria = node.wakuFilter.subscriptions.getPeerSubscriptions(peerId).mapIt(
          FilterTopic(pubsubTopic: it[0], contentTopic: it[1])
        )

      subscriptions.add(
        FilterSubscription(peerId: $peerId, filterCriteria: filterCriteria)
      )

    let resp = RestApiResponse.jsonResponse(subscriptions, status = Http200)
    if resp.isErr():
      error "An error ocurred while building the json respose: ", error = resp.error
      return RestApiResponse.internalServerError(
        fmt("An error ocurred while building the json respose: {resp.error}")
      )

    return resp.get()

proc installAdminApiHandlers*(router: var RestRouter, node: WakuNode) =
  installAdminV1GetPeersHandler(router, node)
  installAdminV1PostPeersHandler(router, node)
  installAdminV1GetFilterSubsHandler(router, node)
