## RequestRateLimiter
##
## RequestRateLimiter is a general service protection mechanism.
## While applies an overall rate limit, it also ensure fair usage among peers.
##
## This is reached by reject peers that are constantly over using the service while allowing others to use it
## within the global limit set.
## Punished peers will also be recovered after a certain time period if not violating the limit.
##
## This is reached by calculating a ratio of the global limit and applying it to each peer.
## This ratio is applied to the allowed tokens within a ratio * the global time period.
## The allowed tokens for peers are limited to 75% of ratio * global token volume.
##
## This needs to be taken into account when setting the global limit for the specific service type and use cases.

{.push raises: [].}

import
  std/[options, math],
  chronicles,
  chronos/timer,
  libp2p/stream/connection,
  libp2p/utility

import ./[single_token_limiter, service_metrics, timed_map]

export token_bucket, setting, service_metrics

logScope:
  topics = "waku ratelimit"

const PER_PEER_ALLOWED_PERCENT_OF_VOLUME = 0.75
const UNLIMITED_RATIO = 0
const UNLIMITED_TIMEOUT = 0.seconds
const MILISECONDS_RATIO = 10
const SECONDS_RATIO = 3
const MINUTES_RATIO = 2

type RequestRateLimiter* = ref object of RootObj
  tokenBucket: Option[TokenBucket]
  setting*: Option[RateLimitSetting]
  peerBucketSetting*: RateLimitSetting
  peerUsage: TimedMap[PeerId, TokenBucket]

proc mgetOrPut(
    requestRateLimiter: var RequestRateLimiter, peerId: PeerId
): var TokenBucket =
  let bucketForNew = newTokenBucket(some(requestRateLimiter.peerBucketSetting)).valueOr:
    raiseAssert "This branch is not allowed to be reached as it will not be called if the setting is None."

  return requestRateLimiter.peerUsage.mgetOrPut(peerId, bucketForNew)

proc checkUsage*(
    t: var RequestRateLimiter, proto: string, conn: Connection, now = Moment.now()
): bool {.raises: [].} =
  if t.tokenBucket.isNone():
    return true

  let peerBucket = t.mgetOrPut(conn.peerId)
  ## check requesting peer's usage is not over the calculated ratio and let that peer go which not requested much/or this time...
  if not peerBucket.tryConsume(1, now):
    trace "peer usage limit reached", peer = conn.peerId
    return false

  # Ok if the peer can consume, check the overall budget we have left
  let tokenBucket = t.tokenBucket.get()
  if not tokenBucket.tryConsume(1, now):
    return false

  return true

template checkUsageLimit*(
    t: var RequestRateLimiter,
    proto: string,
    conn: Connection,
    bodyWithinLimit, bodyRejected: untyped,
) =
  if t.checkUsage(proto, conn):
    let requestStartTime = Moment.now()
    waku_service_requests.inc(labelValues = [proto, "served"])

    bodyWithinLimit

    let requestDurationSec = (Moment.now() - requestStartTime).milliseconds.float / 1000
    waku_service_request_handling_duration_seconds.observe(
      requestDurationSec, labelValues = [proto]
    )
  else:
    waku_service_requests.inc(labelValues = [proto, "rejected"])
    bodyRejected

# TODO: review these ratio assumptions! Debatable!
func calcPeriodRatio(settingOpt: Option[RateLimitSetting]): int =
  settingOpt.withValue(setting):
    if setting.isUnlimited():
      return UNLIMITED_RATIO

    if setting.period <= 1.seconds:
      return MILISECONDS_RATIO

    if setting.period <= 1.minutes:
      return SECONDS_RATIO

    return MINUTES_RATIO
  do:
    # when setting is none
    return UNLIMITED_RATIO

# calculates peer cache items timeout
# effectively if a peer does not issue any requests for this amount of time will be forgotten.
func calcCacheTimeout(settingOpt: Option[RateLimitSetting], ratio: int): Duration =
  settingOpt.withValue(setting):
    if setting.isUnlimited():
      return UNLIMITED_TIMEOUT

    # CacheTimout for peers is double the replensih period for peers
    return setting.period * ratio * 2
  do:
    # when setting is none
    return UNLIMITED_TIMEOUT

func calcPeerTokenSetting(
    setting: Option[RateLimitSetting], ratio: int
): RateLimitSetting =
  let s = setting.valueOr:
    return (0, 0.minutes)

  let peerVolume =
    trunc((s.volume * ratio).float * PER_PEER_ALLOWED_PERCENT_OF_VOLUME).int
  let peerPeriod = s.period * ratio

  return (peerVolume, peerPeriod)

proc newRequestRateLimiter*(setting: Option[RateLimitSetting]): RequestRateLimiter =
  let ratio = calcPeriodRatio(setting)
  return RequestRateLimiter(
    tokenBucket: newTokenBucket(setting),
    setting: setting,
    peerBucketSetting: calcPeerTokenSetting(setting, ratio),
    peerUsage: init(TimedMap[PeerId, TokenBucket], calcCacheTimeout(setting, ratio)),
  )
