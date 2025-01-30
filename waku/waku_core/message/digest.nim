{.push raises: [].}

import std/sequtils, stew/[byteutils, endians2, arrayops], nimcrypto/sha2, results
import ../topics, ./message

## 14/WAKU2-MESSAGE: Deterministic message hashing
## https://rfc.vac.dev/spec/14/#deterministic-message-hashing

type WakuMessageHash* = array[32, byte]

func shortLog*(hash: WakuMessageHash): string =
  ## Returns compact string representation of ``WakuMessageHash``.
  var hexhash = newStringOfCap(13)
  hexhash &= hash.toOpenArray(0, 1).to0xHex()
  hexhash &= "..."
  hexhash &= hash.toOpenArray(hash.len - 2, hash.high).toHex()
  hexhash

func `$`*(hash: WakuMessageHash): string =
  shortLog(hash)

const EmptyWakuMessageHash*: WakuMessageHash = [
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0,
]

converter fromBytes*(array: openArray[byte]): WakuMessageHash =
  var hash: WakuMessageHash
  discard copyFrom(hash, array)
  hash

converter toBytesArray*(digest: MDigest[256]): WakuMessageHash =
  digest.data

converter toBytes*(digest: MDigest[256]): seq[byte] =
  toSeq(digest.data)

proc hexToHash*(hexString: string): Result[WakuMessageHash, string] =
  var hash: WakuMessageHash

  try:
    hash = hexString.hexToSeqByte().fromBytes()
  except ValueError as e:
    return err("Exception converting hex string to hash: " & e.msg)

  return ok(hash)

proc computeMessageHash*(pubsubTopic: PubsubTopic, msg: WakuMessage): WakuMessageHash =
  var ctx: sha256
  ctx.init()
  defer:
    ctx.clear()

  ctx.update(pubsubTopic.toBytes())
  ctx.update(msg.payload)
  ctx.update(msg.contentTopic.toBytes())
  ctx.update(msg.meta)
  ctx.update(toBytesBE(uint64(msg.timestamp)))

  return ctx.finish() # Computes the hash

proc cmp*(x, y: WakuMessageHash): int =
  if x < y:
    return -1
  elif x == y:
    return 0

  return 1
