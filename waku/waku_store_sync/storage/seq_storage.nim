import
  std/[algorithm, sequtils, math, options, packedsets], results, chronos, stew/arrayops

import
  ../../waku_core/time,
  ../../waku_core/message/digest,
  ../common,
  ./range_processing,
  ./storage

type SeqStorage* = ref object of SyncStorage
  elements: seq[SyncID]
  shards: seq[uint16]

  # Numer of parts a range will be splitted into.
  partitionCount: int

  # Number of element in a range for which item sets are used instead of fingerprints.
  lengthThreshold: int

method length*(self: SeqStorage): int =
  return self.elements.len

method insert*(
    self: SeqStorage, element: SyncID, shard: uint16
): Result[void, string] {.raises: [].} =
  let idx = self.elements.lowerBound(element, common.cmp)

  if idx < self.elements.len and self.elements[idx] == element:
    return err("duplicate element")

  self.elements.insert(element, idx)
  self.shards.insert(shard, idx)

  return ok()

method batchInsert*(
    self: SeqStorage, elements: seq[SyncID], shards: seq[uint16]
): Result[void, string] {.raises: [].} =
  ## Insert the sorted seq of new elements.

  if elements.len == 1:
    return self.insert(elements[0], shards[0])

  if not elements.isSorted(common.cmp):
    return err("seq not sorted")

  var idx = 0
  for i in 0 ..< elements.len:
    let element = elements[i]
    let shard = shards[i]

    idx = self.elements[idx ..< self.elements.len].lowerBound(element, common.cmp)

    if self.elements[idx] == element:
      continue

    self.elements.insert(element, idx)
    self.shards.insert(shard, idx)

  return ok()

method prune*(self: SeqStorage, timestamp: Timestamp): int {.raises: [].} =
  ## Remove all elements before the timestamp.
  ## Returns # of elements pruned.

  if self.elements.len == 0:
    return 0

  let bound = SyncID(time: timestamp, hash: EmptyWakuMessageHash)

  let idx = self.elements.lowerBound(bound, common.cmp)

  self.elements.delete(0 ..< idx)
  self.shards.delete(0 ..< idx)

  return idx

proc computefingerprintFromSlice(
    self: SeqStorage, sliceOpt: Option[Slice[int]], shardSet: PackedSet[uint16]
): Fingerprint =
  ## XOR all hashes of a slice of the storage.

  var fingerprint = EmptyFingerprint

  if sliceOpt.isNone():
    return fingerprint

  let idxSlice = sliceOpt.get()

  let elementSlice = self.elements[idxSlice]
  let shardSlice = self.shards[idxSlice]

  for i in 0 ..< elementSlice.len:
    let id = elementSlice[i]
    let shard = shardSlice[i]

    if not shardSet.contains(shard):
      continue

    fingerprint = fingerprint xor id.hash

  return fingerprint

proc findIdxBounds(self: SeqStorage, slice: Slice[SyncID]): Option[Slice[int]] =
  ## Given bounds find the corresponding indices in this storage

  #TODO can thoses 2 binary search be combined for efficiency ???

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
    self: SeqStorage, bounds: Slice[SyncID], shardSet: PackedSet[uint16]
): Fingerprint {.raises: [].} =
  let idxSliceOpt = self.findIdxBounds(bounds)
  return self.computefingerprintFromSlice(idxSliceOpt, shardSet)

proc processFingerprintRange*(
    self: SeqStorage,
    inputBounds: Slice[SyncID],
    shardSet: PackedSet[uint16],
    inputFingerprint: Fingerprint,
    output: var RangesData,
) {.raises: [].} =
  ## Compares fingerprints and partition new ranges.

  let idxSlice = self.findIdxBounds(inputBounds)
  let ourFingerprint = self.computeFingerprintFromSlice(idxSlice, shardSet)

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
    let state = ItemSet(elements: self.elements[slice], reconciled: false)
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
      let state = ItemSet(elements: self.elements[slice], reconciled: false)
      output.itemSets.add(state)
      continue

    let fingerprint = self.computeFingerprintFromSlice(some(slice), shardSet)
    output.ranges.add((partitionBounds, RangeType.Fingerprint))
    output.fingerprints.add(fingerprint)
    continue

proc processItemSetRange*(
    self: SeqStorage,
    inputBounds: Slice[SyncID],
    shardSet: PackedSet[uint16],
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
    let shard = self.shards[j]

    if not shardSet.contains(shard):
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
    let state = ItemSet(elements: self.elements[slice], reconciled: true)
    output.itemSets.add(state)
  else:
    output.ranges.add((inputBounds, RangeType.Skip))

method processPayload*(
    self: SeqStorage,
    input: RangesData,
    hashToSend: var seq[Fingerprint],
    hashToRecv: var seq[Fingerprint],
): RangesData {.raises: [].} =
  var output = RangesData()

  var
    i = 0
    j = 0

  let shardSet = input.shards.toPackedSet()

  for (bounds, rangeType) in input.ranges:
    case rangeType
    of RangeType.Skip:
      output.ranges.add((bounds, RangeType.Skip))

      continue
    of RangeType.Fingerprint:
      let fingerprint = input.fingerprints[i]
      i.inc()

      self.processFingerprintRange(bounds, shardSet, fingerprint, output)

      continue
    of RangeType.ItemSet:
      let itemSet = input.itemsets[j]
      j.inc()

      self.processItemSetRange(
        bounds, shardSet, itemSet, hashToSend, hashToRecv, output
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
    shards: seq[uint16],
    threshold = 100,
    partitions = 8,
): T =
  return SeqStorage(
    elements: elements,
    shards: shards,
    lengthThreshold: threshold,
    partitionCount: partitions,
  )
