import ../waku_relay, ../node/peer_manager

type AppCallbacks* = ref object
  relayHandler*: WakuRelayHandler
  topicHealthChangeHandler*: TopicHealthChangeHandler
  connectionChangeHandler*: ConnectionChangeHandler
