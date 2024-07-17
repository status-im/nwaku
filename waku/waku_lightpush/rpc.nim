{.push raises: [].}

import std/options
import ../waku_core

type
  LightpushRequest* = object
    requestId*: string
    pubSubTopic*: Option[PubsubTopic]
    message*: WakuMessage

  LightPushResponse* = object
    requestId*: string
    statusCode*: uint32
    statusDesc*: Option[string]
    relayPeerCount*: Option[uint32]
