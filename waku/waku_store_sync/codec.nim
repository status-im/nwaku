{.push raises: [].}

import std/sequtils, stew/[leb128, byteutils]

import ../common/protobuf, ../waku_core/message, ../waku_core/time, ./common

const
  HashLen = 32
  VarIntLen = 9
  AvgCapacity = 1000

proc encode*(value: WakuMessageAndTopic): ProtoBuffer =
  var pb = initProtoBuffer()

  pb.write3(1, value.pubsub)
  pb.write3(2, value.message.encode())

  return pb

proc deltaEncode*(itemSet: ItemSet): seq[byte] =
  # 1 byte for resolved bool and 32 bytes hash plus 9 bytes varint per elements 
  let capacity = 1 + (itemSet.elements.len * (VarIntLen + HashLen))

  var
    output = newSeqOfCap[byte](capacity)
    lastTime = Timestamp(0)
    buf = Leb128Buf[uint64]()

  for id in itemSet.elements:
    let timeDiff = uint64(id.time) - uint64(lastTime)
    lastTime = id.time

    # encode timestamp
    buf = timediff.toBytes(Leb128)
    output &= @buf

    output &= id.hash

  output &= byte(itemSet.reconciled)

  return output

proc deltaEncode*(value: RangesData): seq[byte] =
  if value.ranges.len == 0:
    return @[0]

  var
    output = newSeqOfCap[byte](AvgCapacity)
    buf = Leb128Buf[uint64]()
    lastTimestamp: Timestamp
    lastHash: Fingerprint
    i = 0
    j = 0

  # encode cluster
  buf = uint64(value.cluster).toBytes(Leb128)
  output &= @buf

  # encode pubsub topics
  buf = uint64(value.pubsubTopics.len).toBytes(Leb128)
  output &= @buf

  for topic in value.pubsubTopics:
    buf = uint64(topic.len).toBytes(Leb128)
    output &= @buf

    output &= topic.toBytes()

  # encode content topics
  buf = uint64(value.contentTopics.len).toBytes(Leb128)
  output &= @buf

  for topic in value.contentTopics:
    buf = uint64(topic.len).toBytes(Leb128)
    output &= @buf

    output &= topic.toBytes()

  # the first range is implicit but must be explicit when encoded
  let (bound, _) = value.ranges[0]

  lastTimestamp = bound.a.time
  lastHash = bound.a.hash

  # encode first timestamp
  buf = uint64(lastTimestamp).toBytes(Leb128)
  output &= @buf

  # implicit first hash is always 0 and range type is always skip

  for (bound, rangeType) in value.ranges:
    let timeDiff = uint64(bound.b.time) - uint64(lastTimestamp)
    lastTimestamp = bound.b.time

    # encode timestamp
    buf = timeDiff.toBytes(Leb128)
    output &= @buf

    if timeDiff == 0:
      var sameBytes = 0
      for (byte1, byte2) in zip(lastHash, bound.b.hash):
        sameBytes.inc()

        if byte1 != byte2:
          break

      # encode number of same bytes
      output &= byte(sameBytes)

      # encode hash bytes
      output &= bound.b.hash[0 ..< sameBytes]

    # encode rangeType
    output &= byte(rangeType)

    case rangeType
    of RangeType.Skip:
      continue
    of RangeType.Fingerprint:
      output &= value.fingerprints[i]
      i.inc()
    of RangeType.ItemSet:
      let itemSet = value.itemSets[j]
      j.inc()

      # encode how many elements are in the set
      buf = uint64(itemSet.elements.len).toBytes(Leb128)
      output &= @buf

      let encodedSet = itemSet.deltaEncode()

      output &= encodedSet

    continue

  return output

proc getItemSetLength(idx: var int, buffer: seq[byte]): int =
  let min = min(idx + VarIntLen, buffer.len)
  let slice = buffer[idx ..< min]
  let (val, len) = uint64.fromBytes(slice, Leb128)
  idx += len

  return int(val)

proc getFingerprint(idx: var int, buffer: seq[byte]): Result[Fingerprint, string] =
  if idx + HashLen > buffer.len:
    return err("Cannot decode fingerprint")

  let slice = buffer[idx ..< idx + HashLen]
  idx += HashLen
  var fingerprint = EmptyFingerprint
  for i, bytes in slice:
    fingerprint[i] = bytes

  return ok(fingerprint)

proc getRangeType(idx: var int, buffer: seq[byte]): Result[RangeType, string] =
  if idx >= buffer.len:
    return err("Cannot decode range type")

  let val = buffer[idx]

  if val > 2 or val < 0:
    return err("Cannot decode range type")

  let rangeType = RangeType(val)
  idx += 1

  return ok(rangeType)

proc updateHash(idx: var int, buffer: seq[byte], hash: var WakuMessageHash) =
  if idx >= buffer.len:
    return

  let sameBytes = int(buffer[idx])

  if sameBytes > 32:
    return

  idx += 1

  if idx + sameBytes > buffer.len:
    return

  let slice = buffer[idx ..< idx + sameBytes]
  idx += sameBytes

  for i, bytes in slice:
    hash[i] = bytes

proc getTimeDiff(idx: var int, buffer: seq[byte]): Timestamp =
  let min = min(idx + VarIntLen, buffer.len)
  let slice = buffer[idx ..< min]
  let (val, len) = uint64.fromBytes(slice, Leb128)
  idx += len

  return Timestamp(val)

proc getTimestamp(idx: var int, buffer: seq[byte]): Result[Timestamp, string] =
  if idx + VarIntLen > buffer.len:
    return err("Cannot decode timestamp")

  let slice = buffer[idx ..< idx + VarIntLen]
  let (val, len) = uint64.fromBytes(slice, Leb128)
  idx += len

  return ok(Timestamp(val))

proc getHash(idx: var int, buffer: seq[byte]): Result[WakuMessageHash, string] =
  if idx + HashLen > buffer.len:
    return err("Cannot decode hash")

  let slice = buffer[idx ..< idx + HashLen]
  idx += HashLen
  var hash = EmptyWakuMessageHash
  for i, bytes in slice:
    hash[i] = bytes

  return ok(hash)

proc getReconciled(idx: var int, buffer: seq[byte]): Result[bool, string] =
  if idx >= buffer.len:
    return err("Cannot decode reconciled")

  let val = buffer[idx]

  if val > 1 or val < 0:
    return err("Cannot decode reconciled")

  let recon = bool(val)
  idx += 1

  return ok(recon)

proc getCluster(idx: var int, buffer: seq[byte]): Result[uint16, string] =
  if idx + VarIntLen > buffer.len:
    return err("Cannot decode cluster")

  let slice = buffer[idx ..< idx + VarIntLen]
  let (val, len) = uint64.fromBytes(slice, Leb128)
  idx += len

  return ok(uint16(val))

proc getTopics(idx: var int, buffer: seq[byte]): Result[seq[string], string] =
  if idx + VarIntLen > buffer.len:
    return err("Cannot decode topic count")

  let slice = buffer[idx ..< idx + VarIntLen]
  let (val, len) = uint64.fromBytes(slice, Leb128)
  idx += len
  let topicCount = int(val)

  var topics: seq[string]
  for i in 0 ..< topicCount:
    if idx + VarIntLen > buffer.len:
      return err("Cannot decode length. Topic index: " & $i)

    let slice = buffer[idx ..< idx + VarIntLen]
    let (val, len) = uint64.fromBytes(slice, Leb128)
    idx += len
    let topicLen = int(val)

    if idx + topicLen > buffer.len:
      return err("Cannot decode bytes. Topic index: " & $i)

    let topic = string.fromBytes(buffer[idx ..< idx + topicLen])
    idx += topicLen

    topics.add(topic)

  return ok(topics)

proc deltaDecode*(
    itemSet: var ItemSet, buffer: seq[byte], setLength: int
): Result[int, string] =
  var
    lastTime = Timestamp(0)
    idx = 0

  while itemSet.elements.len < setLength:
    let timeDiff = ?getTimestamp(idx, buffer)
    let time = lastTime + timeDiff
    lastTime = time

    let hash = ?getHash(idx, buffer)

    let id = SyncID(time: time, hash: hash)

    itemSet.elements.add(id)

  itemSet.reconciled = ?getReconciled(idx, buffer)

  return ok(idx)

proc getItemSet(
    idx: var int, buffer: seq[byte], itemSetLength: int
): Result[ItemSet, string] =
  var itemSet = ItemSet()
  let slice = buffer[idx ..< buffer.len]
  let count = ?deltaDecode(itemSet, slice, itemSetLength)
  idx += count

  return ok(itemSet)

proc deltaDecode*(T: type RangesData, buffer: seq[byte]): Result[T, string] =
  if buffer.len <= 1:
    return ok(RangesData())

  var
    payload = RangesData()
    lastTime = Timestamp(0)
    idx = 0

  payload.cluster = ?getCluster(idx, buffer)
  payload.pubsubTopics = ?getTopics(idx, buffer)
  payload.contentTopics = ?getTopics(idx, buffer)

  lastTime = ?getTimestamp(idx, buffer)

  # implicit first hash is always 0 
  # implicit first range mode is alway skip

  while idx < buffer.len - 1:
    let lowerRangeBound = SyncID(time: lastTime, hash: EmptyWakuMessageHash)

    let timeDiff = getTimeDiff(idx, buffer)

    var hash = EmptyWakuMessageHash
    if timeDiff == 0:
      updateHash(idx, buffer, hash)

    let thisTime = lastTime + timeDiff
    lastTime = thisTime

    let upperRangeBound = SyncID(time: thisTime, hash: hash)
    let bounds = lowerRangeBound .. upperRangeBound

    let rangeType = ?getRangeType(idx, buffer)
    payload.ranges.add((bounds, rangeType))

    if rangeType == RangeType.Fingerprint:
      let fingerprint = ?getFingerprint(idx, buffer)
      payload.fingerprints.add(fingerprint)
    elif rangeType == RangeType.ItemSet:
      let itemSetLength = getItemSetLength(idx, buffer)
      let itemSet = ?getItemSet(idx, buffer, itemSetLength)
      payload.itemSets.add(itemSet)

  return ok(payload)

proc decode*(T: type WakuMessageAndTopic, buffer: seq[byte]): ProtobufResult[T] =
  let pb = initProtoBuffer(buffer)

  var pubsub: string
  if not ?pb.getField(1, pubsub):
    return err(ProtobufError.missingRequiredField("pubsub"))

  var proto: ProtoBuffer
  if not ?pb.getField(2, proto):
    return err(ProtobufError.missingRequiredField("msg"))

  let message = ?WakuMessage.decode(proto.buffer)

  return ok(WakuMessageAndTopic(pubsub: pubsub, message: message))
