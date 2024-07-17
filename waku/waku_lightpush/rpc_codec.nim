{.push raises: [].}

import std/options
import ../common/protobuf, ../waku_core, ./rpc

const DefaultMaxRpcSize* = -1

proc encode*(rpc: LightpushRequest): ProtoBuffer =
  var pb = initProtoBuffer()

  pb.write3(1, rpc.requestId)
  pb.write3(20, rpc.pubSubTopic)
  pb.write3(21, rpc.message.encode())
  pb.finish3()

  return pb

proc decode*(T: type LightpushRequest, buffer: seq[byte]): ProtobufResult[T] =
  let pb = initProtoBuffer(buffer)
  var rpc = LightpushRequest()

  var requestId: string
  if not ?pb.getField(1, requestId):
    return err(ProtobufError.missingRequiredField("request_id"))
  else:
    rpc.requestId = requestId

  var pubSubTopic: PubsubTopic
  if not ?pb.getField(20, pubSubTopic):
    rpc.pubSubTopic = none(PubsubTopic)
  else:
    rpc.pubSubTopic = some(pubSubTopic)

  var messageBuf: seq[byte]
  if not ?pb.getField(21, messageBuf):
    return err(ProtobufError.missingRequiredField("message"))
  else:
    rpc.message = ?WakuMessage.decode(messageBuf)

  return ok(rpc)

proc encode*(rpc: LightPushResponse): ProtoBuffer =
  var pb = initProtoBuffer()

  pb.write3(1, rpc.requestId)
  pb.write3(10, rpc.statusCode)
  pb.write3(11, rpc.statusDesc)
  pb.write3(12, rpc.relayPeerCount)
  pb.finish3()

  return pb

proc decode*(T: type LightPushResponse, buffer: seq[byte]): ProtobufResult[T] =
  let pb = initProtoBuffer(buffer)
  var rpc = LightPushResponse()

  var requestId: string
  if not ?pb.getField(1, requestId):
    return err(ProtobufError.missingRequiredField("request_id"))
  else:
    rpc.requestId = requestId

  var statusCode: uint32
  if not ?pb.getField(10, statusCode):
    return err(ProtobufError.missingRequiredField("status_code"))
  else:
    rpc.statusCode = statusCode

  var statusDesc: string
  if not ?pb.getField(11, statusDesc):
    rpc.statusDesc = none(string)
  else:
    rpc.statusDesc = some(statusDesc)

  var relayPeerCount: uint32
  if not ?pb.getField(12, relayPeerCount):
    rpc.relayPeerCount = none(uint32)
  else:
    rpc.relayPeerCount = some(relayPeerCount)

  return ok(rpc)
