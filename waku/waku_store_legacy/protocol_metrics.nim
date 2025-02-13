{.push raises: [].}

import metrics

declarePublicGauge waku_legacy_store_errors,
  "number of legacy store protocol errors", ["type"]
declarePublicGauge waku_legacy_store_queries, "number of legacy store queries received"

## "query-db-time" phase considers the time when node performs the query to the database.
## "send-store-resp-time" phase is the time when node writes the store response to the store-client.
declarePublicGauge waku_legacy_store_time_seconds,
  "Time in seconds spent by each store phase", labels = ["phase"]

# Error types (metric label values)
const
  dialFailure* = "dial_failure_legacy"
  decodeRpcFailure* = "decode_rpc_failure_legacy"
  peerNotFoundFailure* = "peer_not_found_failure_legacy"
  emptyRpcQueryFailure* = "empty_rpc_query_failure_legacy"
  emptyRpcResponseFailure* = "empty_rpc_response_failure_legacy"
