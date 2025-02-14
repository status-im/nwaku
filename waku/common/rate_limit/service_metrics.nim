{.push raises: [].}

import std/options
import metrics, setting

declarePublicGauge waku_service_requests_limit,
  "Applied rate limit of non-relay service", ["service"]

declarePublicCounter waku_service_requests,
  "number of non-relay service requests received", ["service", "state"]

declarePublicCounter waku_service_network_bytes,
  "total incoming traffic of specific waku services", labels = ["service", "direction"]

proc setServiceLimitMetric*(service: string, limit: Option[RateLimitSetting]) =
  if limit.isSome() and not limit.get().isUnlimited():
    waku_service_requests_limit.set(
      limit.get().calculateLimitPerSecond(), labelValues = [service]
    )

declarePublicHistogram waku_service_request_handling_duration_seconds,
  "duration of non-relay service handling", ["service"]
