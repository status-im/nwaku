import
  std/[options, times],
  stew/[results, byteutils],
  stew/shims/net,
  chronos,
  libp2p/switch,
  libp2p/builders,
  libp2p/crypto/crypto as libp2p_keys,
  eth/keys as eth_keys
import waku/waku_core, ./common

export switch

# Time

proc now*(): Timestamp =
  getNanosecondTime(getTime().toUnixFloat())

proc ts*(offset = 0, origin = now()): Timestamp =
  origin + getNanosecondTime(int64(offset))

# Switch

proc generateEcdsaKey*(): libp2p_keys.PrivateKey =
  libp2p_keys.PrivateKey.random(ECDSA, rng[]).get()

proc generateEcdsaKeyPair*(): libp2p_keys.KeyPair =
  libp2p_keys.KeyPair.random(ECDSA, rng[]).get()

proc generateSecp256k1Key*(): libp2p_keys.PrivateKey =
  libp2p_keys.PrivateKey.random(Secp256k1, rng[]).get()

proc ethSecp256k1Key*(hex: string): eth_keys.PrivateKey =
  eth_keys.PrivateKey.fromHex(hex).get()

proc newTestSwitch*(
    key = none(libp2p_keys.PrivateKey), address = none(MultiAddress)
): Switch =
  let peerKey = key.get(generateSecp256k1Key())
  let peerAddr = address.get(MultiAddress.init("/ip4/127.0.0.1/tcp/0").get())
  return newStandardSwitch(some(peerKey), addrs = peerAddr)

# Waku message

export waku_core.DefaultPubsubTopic, waku_core.DefaultContentTopic

proc fakeWakuMessage*(
    payload: string | seq[byte] = "TEST-PAYLOAD",
    contentTopic = DefaultContentTopic,
    meta: string | seq[byte] = newSeq[byte](),
    ts = now(),
    ephemeral = false,
    proof = newSeq[byte](),
): WakuMessage =
  var payloadBytes: seq[byte]
  var metaBytes: seq[byte]

  when payload is string:
    payloadBytes = toBytes(payload)
  else:
    payloadBytes = payload

  when meta is string:
    metaBytes = toBytes(meta)
  else:
    metaBytes = meta

  WakuMessage(
    payload: payloadBytes,
    contentTopic: contentTopic,
    meta: metaBytes,
    version: 2,
    timestamp: ts,
    ephemeral: ephemeral,
    proof: proof,
  )
