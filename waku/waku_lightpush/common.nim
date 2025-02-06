{.push raises: [].}

import std/options, results, chronos, libp2p/peerid, libp2p/protocols/pubsub/errors
import ../waku_core, ./rpc

from ../waku_core/codecs import WakuLightPushCodec
export WakuLightPushCodec

type LightpushStatusCode* = enum
  SUCCESS = uint32(200)
  BAD_REQUEST = uint32(400)
  PAYLOAD_TOO_LARGE = uint32(413)
  INVALID_MESSAGE_ERROR = uint32(420) # TODO: check proper status code
  UNSUPPORTED_PUBSUB_TOPIC = uint32(421)
  TOO_MANY_REQUESTS = uint32(429)
  INTERNAL_SERVER_ERROR = uint32(500)
  NO_PEERS_TO_RELAY = uint32(503)
  OUT_OF_RLN_PROOF = uint32(504) # TODO: check proper status code
  SERVICE_NOT_AVAILABLE = uint32(505) # TODO: check proper status code

type ErrorStatus* = tuple[code: LightpushStatusCode, desc: Option[string]]
type WakuLightPushResult* = Result[uint32, ErrorStatus]

type PushMessageHandler* = proc(
  peer: PeerId, pubsubTopic: PubsubTopic, message: WakuMessage
): Future[WakuLightPushResult] {.async.}

const TooManyRequestsMessage* = "Request rejected due to too many requests"

func isSuccess*(response: LightPushResponse): bool =
  return response.statusCode == LightpushStatusCode.SUCCESS.uint32

func toPushResult*(response: LightPushResponse): WakuLightPushResult =
  if isSuccess(response):
    return ok(response.relayPeerCount.get(0))
  else:
    return err((response.statusCode.LightpushStatusCode, response.statusDesc))

func lightpushSuccessResult*(relayPeerCount: uint32): WakuLightPushResult =
  return ok(relayPeerCount)

func lightpushResultInternalError*(msg: string): WakuLightPushResult =
  return err((LightpushStatusCode.INTERNAL_SERVER_ERROR, some(msg)))

func lighpushErrorResult*(
    statusCode: LightpushStatusCode, desc: Option[string]
): WakuLightPushResult =
  return err((statusCode, desc))

func lighpushErrorResult*(
    statusCode: LightpushStatusCode, desc: string
): WakuLightPushResult =
  return err((statusCode, some(desc)))

func mapPubishingErrorToPushResult*(
    resultException: ref PublishingError
): WakuLightPushResult =
  if resultException of NoTopicSpecifiedError:
    return err((LightpushStatusCode.INVALID_MESSAGE_ERROR, some(resultException.msg)))
  elif resultException of PayloadIsEmptyError:
    return err((LightpushStatusCode.INVALID_MESSAGE_ERROR, some(resultException.msg)))
  elif resultException of DuplicateMessageError:
    return err((LightpushStatusCode.INVALID_MESSAGE_ERROR, some(resultException.msg)))
  elif resultException of NotSubscribedToTopicError:
    return
      err((LightpushStatusCode.UNSUPPORTED_PUBSUB_TOPIC, some(resultException.msg)))
  elif resultException of NoPeersToPublishError:
    return err((LightpushStatusCode.NO_PEERS_TO_RELAY, some(resultException.msg)))
  elif resultException of GeneratingMessageIdError:
    return err((LightpushStatusCode.INTERNAL_SERVER_ERROR, some(resultException.msg)))
  else:
    return err((LightpushStatusCode.INTERNAL_SERVER_ERROR, some(resultException.msg)))
