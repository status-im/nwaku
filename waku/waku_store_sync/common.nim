{.push raises: [].}

import std/[options], chronos, stew/[byteutils]

import ../waku_core

const
  DefaultSyncInterval*: Duration = 5.minutes
  DefaultSyncRange*: Duration = 1.hours
  DefaultGossipSubJitter*: Duration = 20.seconds

type
  Fingerprint* = array[32, byte]

  SyncID* = object
    time*: Timestamp
    hash*: WakuMessageHash

  ItemSet* = object
    elements*: seq[SyncID]
    reconciled*: bool

  RangeType* {.pure.} = enum
    Skip = 0
    Fingerprint = 1
    ItemSet = 2

  RangesData* = object
    cluster*: uint16
    pubsubTopics*: seq[PubsubTopic]
    contentTopics*: seq[ContentTopic]

    ranges*: seq[(Slice[SyncID], RangeType)]
    fingerprints*: seq[Fingerprint] # Range type fingerprint stored here in order
    itemSets*: seq[ItemSet] # Range type itemset stored here in order

  WakuMessageAndTopic* = object
    pubsub*: PubSubTopic
    message*: WakuMessage

const EmptyFingerprint*: Fingerprint = [
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0,
]

const FullFingerprint*: Fingerprint = [
  255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
  255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
]

proc high*(T: type SyncID): T =
  ## Same as high(int) but for IDs

  return SyncID(time: Timestamp(high(int64)), fingerprint: FullFingerprint)

proc low*(T: type SyncID): T =
  ## Same as low(int) but for IDs

  return SyncID(time: Timestamp(low(int64)), fingerprint: EmptyFingerprint)

proc `$`*(value: SyncID): string =
  return '(' & $value.time & ", " & $value.hash & ')'

proc cmp(x, y: Fingerprint): int =
  if x < y:
    return -1
  elif x == y:
    return 0

  return 1

proc cmp*(x, y: SyncID): int =
  if x.time == y.time:
    return cmp(x.hash, y.hash)

  if x.time < y.time:
    return -1

  return 1

proc `<`*(x, y: SyncID): bool =
  cmp(x, y) == -1

proc `>`*(x, y: SyncID): bool =
  cmp(x, y) == 1
