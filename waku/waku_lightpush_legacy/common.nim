{.push raises: [].}

import results, chronos, libp2p/peerid
import ../waku_core

from ../waku_core/codecs import WakuLegacyLightPushCodec
export WakuLegacyLightPushCodec

type WakuLightPushResult*[T] = Result[T, string]

type PushMessageHandler* = proc(
  peer: PeerId, pubsubTopic: PubsubTopic, message: WakuMessage
): Future[WakuLightPushResult[void]] {.async.}

const TooManyRequestsMessage* = "TOO_MANY_REQUESTS"
