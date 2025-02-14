import std/packedsets, results

import
  ../../waku_core/time,
  ../../waku_core/topics/content_topic,
  ../../waku_core/topics/pubsub_topic,
  ../common

type SyncStorage* = ref object of RootObj

method insert*(
    self: SyncStorage, element: SyncID, pubsubTopic: PubsubTopic, topic: ContentTopic
): Result[void, string] {.base, gcsafe, raises: [].} =
  return err("insert method not implemented for SyncStorage")

method batchInsert*(
    self: SyncStorage,
    elements: seq[SyncID],
    pubsubTopics: seq[PubsubTopic],
    contentTopics: seq[ContentTopic],
): Result[void, string] {.base, gcsafe, raises: [].} =
  return err("batchInsert method not implemented for SyncStorage")

method prune*(
    self: SyncStorage, timestamp: Timestamp
): int {.base, gcsafe, raises: [].} =
  -1

method computeFingerprint*(
    self: SyncStorage,
    bounds: Slice[SyncID],
    pubsubTopics: seq[PubsubTopic],
    contentTopics: seq[ContentTopic],
): Fingerprint {.base, gcsafe, raises: [].} =
  return EmptyFingerprint

method processPayload*(
    self: SyncStorage,
    cluster: uint16,
    pubsubTopics: seq[PubsubTopic],
    contentTopics: seq[ContentTopic],
    ranges: seq[(Slice[SyncID], RangeType)],
    fingerprints: seq[Fingerprint],
    itemSets: seq[ItemSet],
    hashToSend: var seq[Fingerprint],
    hashToRecv: var seq[Fingerprint],
): RangesData {.base, gcsafe, raises: [].} =
  return RangesData()

method length*(self: SyncStorage): int {.base, gcsafe, raises: [].} =
  -1
