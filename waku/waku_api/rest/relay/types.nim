{.push raises: [].}

import
  std/[sets, strformat, times],
  chronicles,
  json_serialization,
  json_serialization/std/options,
  presto/[route, client, common]
import ../../../common/base64, ../../../waku_core, ../serdes

#### Types

type RelayWakuMessage* = object
  payload*: Base64String
  contentTopic*: Option[ContentTopic]
  version*: Option[Natural]
  timestamp*: Option[int64]
  meta*: Option[Base64String]
  ephemeral*: Option[bool]
  proof*: Option[Base64String]

type
  RelayGetMessagesResponse* = seq[RelayWakuMessage]
  RelayPostMessagesRequest* = RelayWakuMessage

#### Type conversion

proc toRelayWakuMessage*(msg: WakuMessage): RelayWakuMessage =
  RelayWakuMessage(
    payload: base64.encode(msg.payload),
    contentTopic: some(msg.contentTopic),
    version: some(Natural(msg.version)),
    timestamp: some(msg.timestamp),
    meta:
      if msg.meta.len > 0:
        some(base64.encode(msg.meta))
      else:
        none(Base64String),
    ephemeral: some(msg.ephemeral),
    proof: some(base64.encode(msg.proof)),
  )

proc toWakuMessage*(msg: RelayWakuMessage, version = 0): Result[WakuMessage, string] =
  let
    payload = ?msg.payload.decode()
    contentTopic = msg.contentTopic.get(DefaultContentTopic)
    version = uint32(msg.version.get(version))
    meta = ?msg.meta.get(Base64String("")).decode()
    ephemeral = msg.ephemeral.get(false)
    proof = ?msg.proof.get(Base64String("")).decode()

  var timestamp = msg.timestamp.get(0)

  if timestamp == 0:
    timestamp = getNanosecondTime(getTime().toUnixFloat())

  return ok(
    WakuMessage(
      payload: payload,
      contentTopic: contentTopic,
      version: version,
      timestamp: timestamp,
      meta: meta,
      ephemeral: ephemeral,
      proof: proof,
    )
  )

#### Serialization and deserialization

proc writeValue*(
    writer: var JsonWriter[RestJson], value: RelayWakuMessage
) {.raises: [IOError].} =
  writer.beginRecord()
  writer.writeField("payload", value.payload)
  if value.contentTopic.isSome():
    writer.writeField("contentTopic", value.contentTopic.get())
  if value.version.isSome():
    writer.writeField("version", value.version.get())
  if value.timestamp.isSome():
    writer.writeField("timestamp", value.timestamp.get())
  if value.meta.isSome():
    writer.writeField("meta", value.meta.get())
  if value.ephemeral.isSome():
    writer.writeField("ephemeral", value.ephemeral.get())
  if value.proof.isSome():
    writer.writeField("proof", value.proof.get())
  writer.endRecord()

proc readValue*(
    reader: var JsonReader[RestJson], value: var RelayWakuMessage
) {.raises: [SerializationError, IOError].} =
  var
    payload = none(Base64String)
    contentTopic = none(ContentTopic)
    version = none(Natural)
    timestamp = none(int64)
    meta = none(Base64String)
    ephemeral = none(bool)
    proof = none(Base64String)

  var keys = initHashSet[string]()
  for fieldName in readObjectFields(reader):
    # Check for reapeated keys
    if keys.containsOrIncl(fieldName):
      let err =
        try:
          fmt"Multiple `{fieldName}` fields found"
        except CatchableError:
          "Multiple fields with the same name found"
      reader.raiseUnexpectedField(err, "RelayWakuMessage")

    case fieldName
    of "payload":
      payload = some(reader.readValue(Base64String))
    of "contentTopic":
      contentTopic = some(reader.readValue(ContentTopic))
    of "version":
      version = some(reader.readValue(Natural))
    of "timestamp":
      timestamp = some(reader.readValue(int64))
    of "meta":
      meta = some(reader.readValue(Base64String))
    of "ephemeral":
      ephemeral = some(reader.readValue(bool))
    of "proof":
      proof = some(reader.readValue(Base64String))
    else:
      unrecognizedFieldWarning(value)

  if payload.isNone() or isEmptyOrWhitespace(string(payload.get())):
    reader.raiseUnexpectedValue("Field `payload` is missing or empty")

  if contentTopic.isNone() or contentTopic.get().isEmptyOrWhitespace():
    reader.raiseUnexpectedValue("Field `contentTopic` is missing or empty")

  value = RelayWakuMessage(
    payload: payload.get(),
    contentTopic: contentTopic,
    version: version,
    timestamp: timestamp,
    meta: meta,
    ephemeral: ephemeral,
    proof: proof,
  )
