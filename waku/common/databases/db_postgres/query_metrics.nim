import metrics

declarePublicGauge query_time_secs,
  "query time measured in nanoseconds", labels = ["query", "phase"]

declarePublicCounter query_count,
  "number of times a query is being performed", labels = ["query"]

## Possible labels
## The above metrics can only use the following labels
const ContentTopicQuery* = "content_topic"
const SelectVersionQuery* = "select_version"
const MessagesLookupQuery* = "messages_lookup"
const MsgHashWithoutContentTopicQuery* = "msg_hash_no_ctopic"
const MsgHashWithCursorQuery* = "msg_hash_with_cursor"
const GetPartitionsListQuery* = "get_partitions_list"
const CountMsgsQuery* = "count_msgs"
const GetDatabaseSizeQuery* = "get_database_size"
const DeleteFromMessagesLookupQuery* = "delete_from_msgs_lookup"
const DropPartitionTableQuery* = "drop_partition_table"
const DetachPartitionQuery* = "detach_partition"
const GetPartitionSizeQuery* = "get_partition_size"
const TryAdvisoryLockQuery* = "try_advisory_lock"
const GetAllMsgHashQuery* = "get_all_msg_hash"
const AdvisoryUnlockQuery* = "advisory_unlock"
const AnalyzeMessagesQuery* = "analyze_messages"
const CheckVersionTableExistsQuery* = "check_version_table_exists"
