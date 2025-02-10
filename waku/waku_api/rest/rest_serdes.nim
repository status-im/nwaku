{.push raises: [].}

import
  std/typetraits,
  std/os,
  results,
  chronicles,
  serialization,
  json_serialization,
  json_serialization/std/options,
  json_serialization/std/net,
  json_serialization/std/sets,
  presto/common

import ./serdes, ./responses

logScope:
  topics = "waku node rest"

proc encodeBytesOf*[T](value: T, contentType: string): RestResult[seq[byte]] =
  let reqContentType = MediaType.init(contentType)

  if reqContentType != MIMETYPE_JSON:
    error "Unsupported contentType value",
      contentType = contentType, typ = value.type.name
    return err("Unsupported contentType")

  let encoded = ?encodeIntoJsonBytes(value)
  return ok(encoded)

func decodeRequestBody*[T](
    contentBody: Option[ContentBody]
): Result[T, RestApiResponse] =
  if contentBody.isNone():
    return err(RestApiResponse.badRequest("Missing content body"))

  let reqBodyContentType = contentBody.get().contentType.mediaType

  if reqBodyContentType != MIMETYPE_JSON and reqBodyContentType != MIMETYPE_TEXT:
    return err(
      RestApiResponse.badRequest(
        "Wrong Content-Type, expected application/json or text/plain"
      )
    )

  let reqBodyData = contentBody.get().data

  let requestResult = decodeFromJsonBytes(T, reqBodyData)
  if requestResult.isErr():
    return err(
      RestApiResponse.badRequest(
        "Invalid content body, could not decode. " & $requestResult.error
      )
    )

  # Validate and enforce URL encoding for pubsubTopic and contentTopics
  if T.hasKey("pubsubTopic"):
    let pubsubTopic = T["pubsubTopic"]
    if pubsubTopic != encodeUrl(pubsubTopic):
      return err(RestApiResponse.badRequest("Invalid or non-URL-encoded pubsubTopic parameter"))

  if T.hasKey("contentTopics"):
    let contentTopics = T["contentTopics"]
    for topic in contentTopics:
      if topic != encodeUrl(topic):
        return err(RestApiResponse.badRequest("Invalid or non-URL-encoded content_topic parameter"))

  return ok(requestResult.get())

proc decodeBytes*(
    t: typedesc[string], value: openarray[byte], contentType: Opt[ContentTypeData]
): RestResult[string] =
  if MediaType.init($contentType) != MIMETYPE_TEXT:
    error "Unsupported contentType value", contentType = contentType
    return err("Unsupported contentType")

  var res: string
  if len(value) > 0:
    res = newString(len(value))
    copyMem(addr res[0], unsafeAddr value[0], len(value))
  return ok(res)

proc decodeBytes*[T](
    t: typedesc[T], data: openArray[byte], contentType: Opt[ContentTypeData]
): RestResult[T] =
  let reqContentType = contentType.valueOr:
    error "Unsupported response, missing contentType value"
    return err("Unsupported response, missing contentType")

  if reqContentType.mediaType != MIMETYPE_JSON and
      reqContentType.mediaType != MIMETYPE_TEXT:
    error "Unsupported response contentType value", contentType = contentType
    return err("Unsupported response contentType")

  let decoded = ?decodeFromJsonBytes(T, data)
  return ok(decoded)
