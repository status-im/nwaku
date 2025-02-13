{.push raises: [].}

import metrics

declarePublicGauge waku_store_errors, "number of store protocol errors", ["type"]
declarePublicGauge waku_store_queries, "number of store queries received"

## "query-db-time" phase considers the time when node performs the query to the database.
## "send-store-resp-time" phase is the time when node writes the store response to the store-client.
declarePublicGauge waku_store_time_seconds,
  "Time in seconds spent by each store phase", labels = ["phase"]

# Error types (metric label values)
const
  DialFailure* = "dial_failure"
  DecodeRpcFailure* = "decode_rpc_failure"
  PeerNotFoundFailure* = "peer_not_found_failure"
  EmptyRpcQueryFailure* = "empty_rpc_query_failure"
  EmptyRpcResponseFailure* = "empty_rpc_response_failure"
  NoSuccessStatusCode* = "status_code_no_success"
