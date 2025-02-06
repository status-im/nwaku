{.used.}

import std/options, chronicles, chronos, libp2p/crypto/crypto

import
  waku/node/peer_manager,
  waku/waku_core,
  waku/waku_lightpush_legacy,
  waku/waku_lightpush_legacy/[client, common],
  waku/common/rate_limit/setting,
  ../testlib/[common, wakucore]

proc newTestWakuLegacyLightpushNode*(
    switch: Switch,
    handler: PushMessageHandler,
    rateLimitSetting: Option[RateLimitSetting] = none[RateLimitSetting](),
): Future[WakuLegacyLightPush] {.async.} =
  let
    peerManager = PeerManager.new(switch)
    proto = WakuLegacyLightPush.new(peerManager, rng, handler, rateLimitSetting)

  await proto.start()
  switch.mount(proto)

  return proto

proc newTestWakuLegacyLightpushClient*(switch: Switch): WakuLegacyLightPushClient =
  let peerManager = PeerManager.new(switch)
  WakuLegacyLightPushClient.new(peerManager, rng)
