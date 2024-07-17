{.push raises: [].}

import
  json,
  std/sets,
  stew/byteutils,
  strformat,
  chronicles,
  json_serialization,
  json_serialization/std/options,
  presto/[route, client, common]
import ../../../waku_core, ../serdes, ../responses, ../rest_serdes, ./types

export types

logScope:
  topics = "waku node rest client v2"

proc encodeBytes*(value: PushRequest, contentType: string): RestResult[seq[byte]] =
  return encodeBytesOf(value, contentType)

proc sendPushRequest*(
  body: PushRequest
): RestResponse[string] {.
  rest, endpoint: "/lightpush/v1/message", meth: HttpMethod.MethodPost
.}
