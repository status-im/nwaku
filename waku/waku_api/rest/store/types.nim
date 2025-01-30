{.push raises: [].}

import
  std/[sets, strformat, uri, options, sequtils],
  stew/byteutils,
  chronicles,
  json_serialization,
  json_serialization/std/options,
  presto/[route, client, common]
import ../../../waku_store/common, ../../../common/base64, ../../../waku_core, ../serdes

#### Types

createJsonFlavor RestJson

Json.setWriter JsonWriter, PreferredOutput = string

#### Type conversion

proc parseHash*(input: Option[string]): Result[Option[WakuMessageHash], string] =
  let hexUrlEncoded =
    if input.isSome():
      input.get()
    else:
      return ok(none(WakuMessageHash))

  if hexUrlEncoded == "":
    return ok(none(WakuMessageHash))

  let hexDecoded = decodeUrl(hexUrlEncoded, false)

  var decodedBytes: seq[byte]
  try:
    decodedBytes = hexToSeqByte(hexDecoded)
  except ValueError as e:
    return err("Exception converting hex string to bytes: " & e.msg)

  if decodedBytes.len != 32:
    return
      err("waku message hash parsing error: invalid hash length: " & $decodedBytes.len)

  let hash: WakuMessageHash = fromBytes(decodedBytes)

  return ok(some(hash))

proc parseHashes*(input: Option[string]): Result[seq[WakuMessageHash], string] =
  var hashes: seq[WakuMessageHash] = @[]

  if not input.isSome() or input.get() == "":
    return ok(hashes)

  let decodedUrl = decodeUrl(input.get(), false)

  if decodedUrl != "":
    for subString in decodedUrl.split(','):
      let hash = ?parseHash(some(subString))

      if hash.isSome():
        hashes.add(hash.get())

  return ok(hashes)

# Converts a given MessageDigest object into a suitable
# Hex-URL-encoded string suitable to be transmitted in a Rest
# request-response. The MessageDigest is first hex encoded
# and this result is URL-encoded.
proc toRestStringWakuMessageHash*(self: WakuMessageHash): string =
  let hexEncoded = self.to0xHex()
  encodeUrl(hexEncoded, false)

## WakuMessage serde

proc writeValue*(
    writer: var JsonWriter, msg: WakuMessage
) {.gcsafe, raises: [IOError].} =
  writer.beginRecord()

  writer.writeField("payload", base64.encode(msg.payload))
  writer.writeField("contentTopic", msg.contentTopic)

  if msg.meta.len > 0:
    writer.writeField("meta", base64.encode(msg.meta))

  writer.writeField("version", msg.version)
  writer.writeField("timestamp", msg.timestamp)
  writer.writeField("ephemeral", msg.ephemeral)

  if msg.proof.len > 0:
    writer.writeField("proof", base64.encode(msg.proof))

  writer.endRecord()

proc readValue*(
    reader: var JsonReader, value: var WakuMessage
) {.gcsafe, raises: [SerializationError, IOError].} =
  var
    payload: seq[byte]
    contentTopic: ContentTopic
    version: uint32
    timestamp: Timestamp
    ephemeral: bool
    meta: seq[byte]
    proof: seq[byte]

  var keys = initHashSet[string]()
  for fieldName in readObjectFields(reader):
    # Check for reapeated keys
    if keys.containsOrIncl(fieldName):
      let err =
        try:
          fmt"Multiple `{fieldName}` fields found"
        except CatchableError:
          "Multiple fields with the same name found"
      reader.raiseUnexpectedField(err, "WakuMessage")

    case fieldName
    of "payload":
      let base64String = reader.readValue(Base64String)
      payload = base64.decode(base64String).valueOr:
        reader.raiseUnexpectedField("Failed decoding data", "payload")
    of "contentTopic":
      contentTopic = reader.readValue(ContentTopic)
    of "version":
      version = reader.readValue(uint32)
    of "timestamp":
      timestamp = reader.readValue(Timestamp)
    of "ephemeral":
      ephemeral = reader.readValue(bool)
    of "meta":
      let base64String = reader.readValue(Base64String)
      meta = base64.decode(base64String).valueOr:
        reader.raiseUnexpectedField("Failed decoding data", "meta")
    of "proof":
      let base64String = reader.readValue(Base64String)
      proof = base64.decode(base64String).valueOr:
        reader.raiseUnexpectedField("Failed decoding data", "proof")
    else:
      reader.raiseUnexpectedField("Unrecognided field", cstring(fieldName))

  if payload.len == 0:
    reader.raiseUnexpectedValue("Field `payload` is missing")

  value = WakuMessage(
    payload: payload,
    contentTopic: contentTopic,
    version: version,
    timestamp: timestamp,
    ephemeral: ephemeral,
    meta: meta,
    proof: proof,
  )

## WakuMessageKeyValueHex serde

proc writeValue*(
    writer: var JsonWriter, value: WakuMessageKeyValueHex
) {.gcsafe, raises: [IOError].} =
  writer.beginRecord()

  writer.writeField("messageHash", value.messageHash)

  if value.message.isSome():
    writer.writeField("message", value.message.get())

  if value.pubsubTopic.isSome():
    writer.writeField("pubsubTopic", value.pubsubTopic.get())

  writer.endRecord()

proc readValue*(
    reader: var JsonReader, value: var WakuMessageKeyValueHex
) {.gcsafe, raises: [SerializationError, IOError].} =
  var
    messageHash = none(string)
    message = none(WakuMessage)
    pubsubTopic = none(PubsubTopic)

  for fieldName in readObjectFields(reader):
    case fieldName
    of "messageHash":
      if messageHash.isSome():
        reader.raiseUnexpectedField(
          "Multiple `messageHash` fields found", "WakuMessageKeyValueHex"
        )
      messageHash = some(reader.readValue(string))
    of "message":
      if message.isSome():
        reader.raiseUnexpectedField(
          "Multiple `message` fields found", "WakuMessageKeyValueHex"
        )
      message = some(reader.readValue(WakuMessage))
    of "pubsubTopic":
      if pubsubTopic.isSome():
        reader.raiseUnexpectedField(
          "Multiple `pubsubTopic` fields found", "WakuMessageKeyValueHex"
        )
      pubsubTopic = some(reader.readValue(string))
    else:
      reader.raiseUnexpectedField("Unrecognided field", cstring(fieldName))

  if messageHash.isNone():
    reader.raiseUnexpectedValue("Field `messageHash` is missing")

  value = WakuMessageKeyValueHex(
    messageHash: messageHash.get(), message: message, pubsubTopic: pubsubTopic
  )

## StoreQueryResponse serde

proc writeValue*(
    writer: var JsonWriter, value: StoreQueryResponseHex
) {.gcsafe, raises: [IOError].} =
  writer.beginRecord()

  writer.writeField("requestId", value.requestId)
  writer.writeField("statusCode", value.statusCode)
  writer.writeField("statusDesc", value.statusDesc)
  writer.writeField("messages", value.messages)

  if value.paginationCursor.isSome():
    writer.writeField("paginationCursor", value.paginationCursor.get())

  writer.endRecord()

proc readValue*(
    reader: var JsonReader, value: var StoreQueryResponseHex
) {.gcsafe, raises: [SerializationError, IOError].} =
  var
    requestId = none(string)
    code = none(uint32)
    desc = none(string)
    messages = none(seq[WakuMessageKeyValueHex])
    cursor = none(string)

  for fieldName in readObjectFields(reader):
    case fieldName
    of "requestId":
      if requestId.isSome():
        reader.raiseUnexpectedField(
          "Multiple `requestId` fields found", "StoreQueryResponseHex"
        )
      requestId = some(reader.readValue(string))
    of "statusCode":
      if code.isSome():
        reader.raiseUnexpectedField(
          "Multiple `statusCode` fields found", "StoreQueryResponseHex"
        )
      code = some(reader.readValue(uint32))
    of "statusDesc":
      if desc.isSome():
        reader.raiseUnexpectedField(
          "Multiple `statusDesc` fields found", "StoreQueryResponseHex"
        )
      desc = some(reader.readValue(string))
    of "messages":
      if messages.isSome():
        reader.raiseUnexpectedField(
          "Multiple `messages` fields found", "StoreQueryResponseHex"
        )
      messages = some(reader.readValue(seq[WakuMessageKeyValueHex]))
    of "paginationCursor":
      if cursor.isSome():
        reader.raiseUnexpectedField(
          "Multiple `paginationCursor` fields found", "StoreQueryResponseHex"
        )
      cursor = some(reader.readValue(string))
    else:
      reader.raiseUnexpectedField("Unrecognided field", cstring(fieldName))

  if requestId.isNone():
    reader.raiseUnexpectedValue("Field `requestId` is missing")

  if code.isNone():
    reader.raiseUnexpectedValue("Field `statusCode` is missing")

  if desc.isNone():
    reader.raiseUnexpectedValue("Field `statusDesc` is missing")

  if messages.isNone():
    reader.raiseUnexpectedValue("Field `messages` is missing")

  value = StoreQueryResponseHex(
    requestId: requestId.get(),
    statusCode: code.get(),
    statusDesc: desc.get(),
    messages: messages.get(),
    paginationCursor: cursor,
  )

## StoreRequestRest serde

proc writeValue*(
    writer: var JsonWriter, req: StoreQueryRequest
) {.gcsafe, raises: [IOError].} =
  writer.beginRecord()

  writer.writeField("requestId", req.requestId)
  writer.writeField("includeData", req.includeData)

  if req.pubsubTopic.isSome():
    writer.writeField("pubsubTopic", req.pubsubTopic.get())

  writer.writeField("contentTopics", req.contentTopics)

  if req.startTime.isSome():
    writer.writeField("startTime", req.startTime.get())

  if req.endTime.isSome():
    writer.writeField("endTime", req.endTime.get())

  writer.writeField("messageHashes", req.messageHashes.mapIt(base64.encode(it)))

  if req.paginationCursor.isSome():
    writer.writeField("paginationCursor", base64.encode(req.paginationCursor.get()))

  writer.writeField("paginationForward", req.paginationForward)

  if req.paginationLimit.isSome():
    writer.writeField("paginationLimit", req.paginationLimit.get())

  writer.endRecord()
