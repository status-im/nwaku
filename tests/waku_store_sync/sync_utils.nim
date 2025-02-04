import std/[options, random], chronos, chronicles

import
  waku/[
    node/peer_manager,
    waku_core,
    waku_store_sync/common,
    waku_store_sync/reconciliation,
    waku_store_sync/transfer,
  ],
  ../testlib/wakucore

randomize()

proc randomHash*(rng: var Rand): WakuMessageHash =
  var hash = EmptyWakuMessageHash

  for i in 0 ..< hash.len:
    hash[i] = rng.rand(uint8)

  return hash

proc newTestWakuRecon*(
    switch: Switch,
    idsRx: AsyncQueue[(SyncID, uint16)],
    wantsTx: AsyncQueue[(PeerId, Fingerprint)],
    needsTx: AsyncQueue[(PeerId, Fingerprint)],
    cluster: uint16 = 1,
    shards: seq[uint16] = @[0, 1, 2, 3, 4, 5, 6, 7],
): Future[SyncReconciliation] {.async.} =
  let peerManager = PeerManager.new(switch)

  let res = await SyncReconciliation.new(
    cluster = cluster,
    shards = shards,
    peerManager = peerManager,
    wakuArchive = nil,
    relayJitter = 0.seconds,
    idsRx = idsRx,
    localWantsTx = wantsTx,
    remoteNeedsTx = needsTx,
  )

  let proto = res.get()

  proto.start()
  switch.mount(proto)

  return proto

proc newTestWakuTransfer*(
    switch: Switch,
    idsTx: AsyncQueue[(SyncID, uint16)],
    wantsRx: AsyncQueue[(PeerId, Fingerprint)],
    needsRx: AsyncQueue[(PeerId, Fingerprint)],
): SyncTransfer =
  let peerManager = PeerManager.new(switch)

  let proto = SyncTransfer.new(
    peerManager = peerManager,
    wakuArchive = nil,
    idsTx = idsTx,
    localWantsRx = wantsRx,
    remoteNeedsRx = needsRx,
  )

  proto.start()
  switch.mount(proto)

  return proto
