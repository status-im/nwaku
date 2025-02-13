import
  std/[algorithm, sequtils, math, options, tables, packedsets, sugar],
  results,
  chronos,
  stew/arrayops

import
  ../../waku_core/time,
  ../../waku_core/message/digest,
  ../../waku_core/topics/pubsub_topic,
  ../../waku_core/topics/content_topic,
  ../common,
  ./range_processing,
  ./storage

type SeqStorage* = ref object of SyncStorage
  elements: seq[SyncID]

  pubsubTopicIndexes: seq[int]
  contentTopicIndexes: seq[int]

  pubsubTopics: seq[PubSubTopic]
  contentTopics: seq[ContentTopic]

  unusedPubsubTopicSet: PackedSet[int]
  unusedContentTopicSet: PackedSet[int]

  # Numer of parts a range will be splitted into.
  partitionCount: int

  # Number of element in a range for which item sets are used instead of fingerprints.
  lengthThreshold: int

method length*(self: SeqStorage): int {.raises: [].} =
  return self.elements.len

proc getPubsubTopicIndex(self: SeqStorage, pubsubTopic: PubSubTopic): int =
  for i, selfTopic in self.pubsubTopics:
    if pubsubTopic == selfTopic:
      return i

  if self.unusedPubsubTopicSet.len > 0:
    let unusedIdx = self.unusedPubsubTopicSet.toSeq()[0]
    self.unusedPubsubTopicSet.excl(unusedIdx)
    self.pubsubTopics[unusedIdx] = pubsubTopic
    return unusedIdx

  let newIdx = self.pubsubTopics.len
  self.pubsubTopics.add(pubsubTopic)
  return newidx

proc getContentTopicIndex(self: SeqStorage, contentTopic: ContentTopic): int =
  for i, selfTopic in self.contentTopics:
    if contentTopic == selfTopic:
      return i

  if self.unusedContentTopicSet.len > 0:
    let unusedIdx = self.unusedContentTopicSet.toSeq()[0]
    self.unusedContentTopicSet.excl(unusedIdx)
    self.contentTopics[unusedIdx] = contentTopic
    return unusedIdx

  let newIdx = self.contentTopics.len
  self.contentTopics.add(contentTopic)
  return newIdx

proc insertAt(
    self: SeqStorage,
    idx: int,
    element: SyncID,
    pubsubTopic: PubSubTopic,
    contentTopic: ContentTopic,
): Result[void, string] =
  if idx < self.elements.len and self.elements[idx] == element:
    return err("duplicate element")

  self.elements.insert(element, idx)

  let pubsubIndex = self.getPubsubTopicIndex(pubsubTopic)
  let contentIndex = self.getContentTopicIndex(contentTopic)

  self.pubsubTopicIndexes.insert(pubsubIndex, idx)
  self.contentTopicIndexes.insert(contentIndex, idx)

  return ok()

method insert*(
    self: SeqStorage,
    element: SyncID,
    pubsubTopic: PubSubTopic,
    contentTopic: ContentTopic,
): Result[void, string] {.raises: [].} =
  let idx = self.elements.lowerBound(element, common.cmp)
  return self.insertAt(idx, element, pubsubTopic, contentTopic)

method batchInsert*(
    self: SeqStorage,
    elements: seq[SyncID],
    pubsubTopics: seq[PubSubTopic],
    contentTopics: seq[ContentTopic],
): Result[void, string] {.raises: [].} =
  ## Insert the sorted seq of new elements.

  if elements.len == 1:
    return self.insert(elements[0], pubsubTopics[0], contentTopics[0])

  if not elements.isSorted(common.cmp):
    return err("seq not sorted")

  var idx = 0
  for i in 0 ..< elements.len:
    let element = elements[i]
    let pubsubTopic = pubsubTopics[i]
    let contentTopic = contentTopics[i]

    idx = self.elements[idx ..< self.elements.len].lowerBound(element, common.cmp)

    # We don't care about duplicates, discard result
    discard self.insertAt(idx, element, pubsubTopic, contentTopic)

  return ok()

method prune*(self: SeqStorage, timestamp: Timestamp): int {.raises: [].} =
  ## Remove all elements before the timestamp.
  ## Returns # of elements pruned.

  if self.elements.len == 0:
    return 0

  let bound = SyncID(time: timestamp, hash: EmptyWakuMessageHash)

  let idx = self.elements.lowerBound(bound, common.cmp)

  self.elements.delete(0 ..< idx)
  self.pubsubTopicIndexes.delete(0 ..< idx)
  self.contentTopicIndexes.delete(0 ..< idx)

  # Free unused content topics
  let contentIdxSet = self.contentTopicIndexes.toPackedSet()
  var contentTopicSet: PackedSet[int]
  for i in 0 ..< self.contentTopics.len:
    contentTopicSet.incl(i)

  self.unusedContentTopicSet = contentTopicSet - contentIdxSet

  # Free unused pubsub topics
  let pubsubIdxSet = self.pubsubTopicIndexes.toPackedSet()
  var pubsubTopicSet: PackedSet[int]
  for i in 0 ..< self.pubsubTopics.len:
    pubsubTopicSet.incl(i)

  self.unusedPubsubTopicSet = pubsubTopicSet - pubsubIdxSet

  return idx

proc computefingerprintFromSlice(
    self: SeqStorage,
    sliceOpt: Option[Slice[int]],
    pubsubTopicSet: PackedSet[int],
    contentTopicSet: PackedSet[int],
): Fingerprint =
  ## XOR all hashes of a slice of the storage.

  var fingerprint = EmptyFingerprint

  if sliceOpt.isNone():
    return fingerprint

  let idxSlice = sliceOpt.get()

  let elementSlice = self.elements[idxSlice]
  let pubsubSlice = self.pubsubTopicIndexes[idxSlice]
  let contentSlice = self.contentTopicIndexes[idxSlice]

  for i in 0 ..< elementSlice.len:
    let id = elementSlice[i]
    let pubsub = pubsubSlice[i]
    let content = contentSlice[i]

    if pubsubTopicSet.len > 0 and pubsub notin pubsubTopicSet:
      continue

    if contentTopicSet.len > 0 and content notin contentTopicSet:
      continue

    fingerprint = fingerprint xor id.hash

  return fingerprint

proc findIdxBounds(self: SeqStorage, slice: Slice[SyncID]): Option[Slice[int]] =
  ## Given bounds find the corresponding indices in this storage

  let lower = self.elements.lowerBound(slice.a, common.cmp)
  var upper = self.elements.upperBound(slice.b, common.cmp)

  if upper < 1:
    # entire range is before any of our elements
    return none(Slice[int])

  if lower >= self.elements.len:
    # entire range is after any of our elements
    return none(Slice[int])

  return some(lower ..< upper)

method computeFingerprint*(
    self: SeqStorage,
    bounds: Slice[SyncID],
    pubsubTopics: seq[PubsubTopic],
    contentTopics: seq[ContentTopic],
): Fingerprint {.raises: [].} =
  let idxSliceOpt = self.findIdxBounds(bounds)

  var pubsubTopicSet = initPackedSet[int]()
  for inputTopic in pubsubTopics:
    for i, localTopic in self.pubsubTopics:
      if inputTopic == localTopic:
        pubsubTopicSet.incl(i)

  var contentTopicSet = initPackedSet[int]()
  for inputTopic in contentTopics:
    for i, localTopic in self.contentTopics:
      if inputTopic == localTopic:
        contentTopicSet.incl(i)

  return self.computefingerprintFromSlice(idxSliceOpt, pubsubTopicSet, contentTopicSet)

proc getFilteredElements(
    self: SeqStorage,
    slice: Slice[int],
    pubsubTopicSet: PackedSet[int],
    contentTopicSet: PackedSet[int],
): seq[SyncID] =
  let elements = collect(newSeq):
    for i in slice:
      if pubsubTopicSet.len > 0 and self.pubsubTopicIndexes[i] notin pubsubTopicSet:
        continue

      if contentTopicSet.len > 0 and self.contentTopicIndexes[i] notin contentTopicSet:
        continue

      self.elements[i]

  elements

proc processFingerprintRange*(
    self: SeqStorage,
    inputBounds: Slice[SyncID],
    pubsubTopicSet: PackedSet[int],
    contentTopicSet: PackedSet[int],
    inputFingerprint: Fingerprint,
    output: var RangesData,
) {.raises: [].} =
  ## Compares fingerprints and partition new ranges.

  let idxSlice = self.findIdxBounds(inputBounds)
  let ourFingerprint =
    self.computeFingerprintFromSlice(idxSlice, pubsubTopicSet, contentTopicSet)

  if ourFingerprint == inputFingerprint:
    output.ranges.add((inputBounds, RangeType.Skip))
    return

  if idxSlice.isNone():
    output.ranges.add((inputBounds, RangeType.ItemSet))
    let state = ItemSet(elements: @[], reconciled: true)
    output.itemSets.add(state)
    return

  let slice = idxSlice.get()

  if slice.len <= self.lengthThreshold:
    output.ranges.add((inputBounds, RangeType.ItemSet))
    let elements = self.getFilteredElements(slice, pubsubTopicSet, contentTopicSet)
    let state = ItemSet(elements: elements, reconciled: false)
    output.itemSets.add(state)
    return

  let partitions = equalPartitioning(inputBounds, self.partitionCount)
  for partitionBounds in partitions:
    let sliceOpt = self.findIdxBounds(partitionBounds)

    if sliceOpt.isNone():
      output.ranges.add((partitionBounds, RangeType.ItemSet))
      let state = ItemSet(elements: @[], reconciled: true)
      output.itemSets.add(state)
      continue

    let slice = sliceOpt.get()

    if slice.len <= self.lengthThreshold:
      output.ranges.add((partitionBounds, RangeType.ItemSet))
      let elements = self.getFilteredElements(slice, pubsubTopicSet, contentTopicSet)
      let state = ItemSet(elements: elements, reconciled: false)
      output.itemSets.add(state)
      continue

    let fingerprint =
      self.computeFingerprintFromSlice(some(slice), pubsubTopicSet, contentTopicSet)
    output.ranges.add((partitionBounds, RangeType.Fingerprint))
    output.fingerprints.add(fingerprint)
    continue

proc processItemSetRange*(
    self: SeqStorage,
    inputBounds: Slice[SyncID],
    pubsubTopicSet: PackedSet[int],
    contentTopicSet: PackedSet[int],
    inputItemSet: ItemSet,
    hashToSend: var seq[Fingerprint],
    hashToRecv: var seq[Fingerprint],
    output: var RangesData,
) {.raises: [].} =
  ## Compares item sets and outputs differences

  let idxSlice = self.findIdxBounds(inputBounds)

  if idxSlice.isNone():
    if not inputItemSet.reconciled:
      output.ranges.add((inputBounds, RangeType.ItemSet))
      let state = ItemSet(elements: @[], reconciled: true)
      output.itemSets.add(state)
    else:
      output.ranges.add((inputBounds, RangeType.Skip))

    return

  let slice = idxSlice.get()

  var i = 0
  let n = inputItemSet.elements.len

  var j = slice.a
  let m = slice.b + 1

  while (j < m):
    let ourElement = self.elements[j]
    let pubsub = self.pubsubTopicIndexes[j]
    let content = self.contentTopicIndexes[j]

    if pubsubTopicSet.len > 0 and pubsub notin pubsubTopicSet:
      j.inc()
      continue

    if contentTopicSet.len > 0 and content notin contentTopicSet:
      j.inc()
      continue

    if i >= n:
      # in case we have more elements
      hashToSend.add(ourElement.hash)
      j.inc()
      continue

    let theirElement = inputItemSet.elements[i]

    if theirElement < ourElement:
      hashToRecv.add(theirElement.hash)
      i.inc()
    elif theirElement > ourElement:
      hashToSend.add(ourElement.hash)
      j.inc()
    else:
      i.inc()
      j.inc()

  while (i < n):
    # in case they have more elements
    let theirElement = inputItemSet.elements[i]
    i.inc()
    hashToRecv.add(theirElement.hash)

  if not inputItemSet.reconciled:
    output.ranges.add((inputBounds, RangeType.ItemSet))
    let elements = self.getFilteredElements(slice, pubsubTopicSet, contentTopicSet)
    let state = ItemSet(elements: elements, reconciled: true)
    output.itemSets.add(state)
  else:
    output.ranges.add((inputBounds, RangeType.Skip))

method processPayload*(
    self: SeqStorage,
    cluster: uint16,
    pubsubTopics: seq[PubsubTopic],
    contentTopics: seq[ContentTopic],
    ranges: seq[(Slice[SyncID], RangeType)],
    fingerprints: seq[Fingerprint],
    itemSets: seq[ItemSet],
    hashToSend: var seq[Fingerprint],
    hashToRecv: var seq[Fingerprint],
): RangesData {.raises: [].} =
  var output = RangesData()

  var
    i = 0
    j = 0

  var pubsubTopicSet = initPackedSet[int]()
  for inputTopic in pubsubTopics:
    for i, localTopic in self.pubsubTopics:
      if inputTopic == localTopic:
        pubsubTopicSet.incl(i)

  var contentTopicSet = initPackedSet[int]()
  for inputTopic in contentTopics:
    for i, localTopic in self.contentTopics:
      if inputTopic == localTopic:
        contentTopicSet.incl(i)

  for (bounds, rangeType) in ranges:
    case rangeType
    of RangeType.Skip:
      output.ranges.add((bounds, RangeType.Skip))

      continue
    of RangeType.Fingerprint:
      let fingerprint = fingerprints[i]
      i.inc()

      self.processFingerprintRange(
        bounds, pubsubTopicSet, contentTopicSet, fingerprint, output
      )

      continue
    of RangeType.ItemSet:
      let itemSet = itemsets[j]
      j.inc()

      self.processItemSetRange(
        bounds, pubsubTopicSet, contentTopicSet, itemSet, hashToSend, hashToRecv, output
      )

      continue

  # merge consecutive skip ranges
  var allSkip = true
  i = output.ranges.len - 1
  while i >= 0:
    let currRange = output.ranges[i]

    if allSkip and currRange[1] != RangeType.Skip:
      allSkip = false

    if i <= 0:
      break

    let prevRange = output.ranges[i - 1]

    if currRange[1] != RangeType.Skip or prevRange[1] != RangeType.Skip:
      i.dec()
      continue

    let lb = prevRange[0].a
    let ub = currRange[0].b
    let newRange = (lb .. ub, RangeType.Skip)

    output.ranges.delete(i)
    output.ranges[i - 1] = newRange

    i.dec()

  if allSkip:
    output = RangesData()

  return output

proc new*(T: type SeqStorage, capacity: int, threshold = 100, partitions = 8): T =
  return SeqStorage(
    elements: newSeqOfCap[SyncID](capacity),
    lengthThreshold: threshold,
    partitionCount: partitions,
  )

proc new*(
    T: type SeqStorage,
    elements: seq[SyncID],
    pubsubTopics: seq[PubsubTopic],
    contentTopics: seq[ContentTopic],
    threshold = 100,
    partitions = 8,
): T =
  var idx = 0
  var uniquePubsubTopics = initOrderedTable[PubsubTopic, int]()
  for pubsub in pubsubTopics:
    if pubsub notin uniquePubsubTopics:
      uniquePubsubTopics.add(pubsub, idx)
      idx.inc()

  let pubsubTopicIndexes = collect(newSeq):
    for pubsub in pubsubTopics:
      uniquePubsubTopics[pubsub]

  idx = 0
  var uniqueContentTopics = initOrderedTable[ContentTopic, int]()
  for content in contentTopics:
    if content notin uniqueContentTopics:
      uniqueContentTopics.add(content, idx)
      idx.inc()

  let contentTopicIndexes = collect(newSeq):
    for content in contentTopics:
      uniqueContentTopics[content]

  return SeqStorage(
    elements: elements,
    pubsubTopics: uniquePubsubTopics.keys.toSeq(),
    contentTopics: uniqueContentTopics.keys.toSeq(),
    pubsubTopicIndexes: pubsubTopicIndexes,
    contentTopicIndexes: contentTopicIndexes,
    lengthThreshold: threshold,
    partitionCount: partitions,
  )
