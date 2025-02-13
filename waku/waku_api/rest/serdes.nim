{.push raises: [].}

import
  std/typetraits,
  results,
  stew/byteutils,
  chronicles,
  serialization,
  json_serialization,
  json_serialization/std/options,
  json_serialization/std/net,
  json_serialization/std/sets,
  presto/common
import ../../common/base64

logScope:
  topics = "waku node rest"

createJsonFlavor RestJson

Json.setWriter JsonWriter, PreferredOutput = string

template unrecognizedFieldWarning*(field: typed) =
  # TODO: There should be a different notification mechanism for informing the
  #       caller of a deserialization routine for unexpected fields.
  #       The chonicles import in this module should be removed.
  debug "JSON field not recognized by the current version of nwaku. Consider upgrading",
    fieldName, typeName = typetraits.name(typeof field)

type SerdesResult*[T] = Result[T, cstring]

proc writeValue*(
    writer: var JsonWriter, value: Base64String
) {.gcsafe, raises: [IOError].} =
  writer.writeValue(string(value))

proc readValue*(
    reader: var JsonReader, value: var Base64String
) {.gcsafe, raises: [SerializationError, IOError].} =
  value = Base64String(reader.readValue(string))

proc decodeFromJsonString*[T](
    t: typedesc[T], data: JsonString, requireAllFields = true
): SerdesResult[T] =
  try:
    ok(
      RestJson.decode(
        string(data), T, requireAllFields = requireAllFields, allowUnknownFields = true
      )
    )
  except SerializationError:
    # TODO: Do better error reporting here
    err("Unable to deserialize data")

proc decodeFromJsonBytes*[T](
    t: typedesc[T], data: openArray[byte], requireAllFields = true
): SerdesResult[T] =
  try:
    ok(
      RestJson.decode(
        string.fromBytes(data),
        T,
        requireAllFields = requireAllFields,
        allowUnknownFields = true,
      )
    )
  except SerializationError:
    err("Unable to deserialize data: " & getCurrentExceptionMsg())

proc encodeIntoJsonString*(value: auto): SerdesResult[string] =
  var encoded: string
  try:
    var stream = memoryOutput()
    var writer = JsonWriter[RestJson].init(stream)
    writer.writeValue(value)
    encoded = stream.getOutput(string)
  except SerializationError, IOError:
    # TODO: Do better error reporting here
    return err("unable to serialize data")

  ok(encoded)

proc encodeIntoJsonBytes*(value: auto): SerdesResult[seq[byte]] =
  var encoded: seq[byte]
  try:
    var stream = memoryOutput()
    var writer = JsonWriter[RestJson].init(stream)
    writer.writeValue(value)
    encoded = stream.getOutput(seq[byte])
  except SerializationError, IOError:
    # TODO: Do better error reporting here
    return err("unable to serialize data")

  ok(encoded)

#### helpers

proc encodeString*(value: string): RestResult[string] =
  ok(value)

proc decodeString*(t: typedesc[string], value: string): RestResult[string] =
  ok(value)
