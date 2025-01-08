import std/net
import chronicles, chronos, stew/byteutils, results
import
  ../../../../../waku/waku_core/message/message,
  ../../../../../waku/factory/[external_config, validator_signed, waku],
  ../../../../../waku/waku_core/message,
  ../../../../../waku/waku_core/time, # Timestamp
  ../../../../../waku/waku_core/topics/pubsub_topic,
  ../../../../../waku/waku_relay/protocol,
  ../../../../alloc

type RelayMsgType* = enum
  SUBSCRIBE
  UNSUBSCRIBE
  PUBLISH
  LIST_CONNECTED_PEERS
    ## to return the list of all connected peers to an specific pubsub topic
  LIST_MESH_PEERS
    ## to return the list of only the peers that conform the mesh for a particular pubsub topic
  ADD_PROTECTED_SHARD ## Protects a shard with a public key

type ThreadSafeWakuMessage* = object
  payload: SharedSeq[byte]
  contentTopic: cstring
  meta: SharedSeq[byte]
  version: uint32
  timestamp: Timestamp
  ephemeral: bool
  when defined(rln):
    proof: SharedSeq[byte]

type RelayRequest* = object
  operation: RelayMsgType
  pubsubTopic: cstring
  relayEventCallback: WakuRelayHandler # not used in 'PUBLISH' requests
  message: ThreadSafeWakuMessage # only used in 'PUBLISH' requests
  clusterId: cint # only used in 'ADD_PROTECTED_SHARD' requests
  shardId: cint # only used in 'ADD_PROTECTED_SHARD' requests
  publicKey: cstring # only used in 'ADD_PROTECTED_SHARD' requests

proc createShared*(
    T: type RelayRequest,
    op: RelayMsgType,
    pubsubTopic: PubsubTopic = "",
    relayEventCallback: WakuRelayHandler = nil,
    m = WakuMessage(),
    clusterId: cint = 0,
    shardId: cint = 0,
    publicKey: string = "",
): ptr type T =
  var ret = createShared(T)
  ret[].operation = op
  ret[].pubsubTopic = pubsubTopic.alloc()
  ret[].clusterId = clusterId
  ret[].shardId = shardId
  ret[].publicKey = publicKey.alloc()
  ret[].relayEventCallback = relayEventCallback
  ret[].message = ThreadSafeWakuMessage(
    payload: allocSharedSeq(m.payload),
    contentTopic: m.contentTopic.alloc(),
    meta: allocSharedSeq(m.meta),
    version: m.version,
    timestamp: m.timestamp,
    ephemeral: m.ephemeral,
  )
  when defined(rln):
    ret[].message.proof = allocSharedSeq(m.proof)

  return ret

proc destroyShared(self: ptr RelayRequest) =
  deallocSharedSeq(self[].message.payload)
  deallocShared(self[].message.contentTopic)
  deallocSharedSeq(self[].message.meta)
  when defined(rln):
    deallocSharedSeq(self[].message.proof)
  deallocShared(self[].pubsubTopic)
  deallocShared(self[].publicKey)
  deallocShared(self)

proc toWakuMessage(m: ThreadSafeWakuMessage): WakuMessage =
  var wakuMessage = WakuMessage()

  wakuMessage.payload = m.payload.toSeq()
  wakuMessage.contentTopic = $m.contentTopic
  wakuMessage.meta = m.meta.toSeq()
  wakuMessage.version = m.version
  wakuMessage.timestamp = m.timestamp
  wakuMessage.ephemeral = m.ephemeral

  when defined(rln):
    wakuMessage.proof = m.proof

  return wakuMessage

proc process*(
    self: ptr RelayRequest, waku: ptr Waku
): Future[Result[string, string]] {.async.} =
  defer:
    destroyShared(self)

  if waku.node.wakuRelay.isNil():
    return err("Operation not supported without Waku Relay enabled.")

  case self.operation
  of SUBSCRIBE:
    # TO DO: properly perform 'subscribe'
    discard waku.node.wakuRelay.subscribe($self.pubsubTopic, self.relayEventCallback)
  of UNSUBSCRIBE:
    # TODO: properly perform 'unsubscribe'
    waku.node.wakuRelay.unsubscribeAll($self.pubsubTopic)
  of PUBLISH:
    let msg = self.message.toWakuMessage()
    let pubsubTopic = $self.pubsubTopic

    let numPeers = await waku.node.wakuRelay.publish(pubsubTopic, msg)
    if numPeers == 0:
      let errorMsg = "Message not sent because no peers found."
      error "PUBLISH failed", error = errorMsg
      return err(errorMsg)
    elif numPeers > 0:
      let msgHash = computeMessageHash(pubSubTopic, msg).to0xHex
      return ok(msgHash)
  of LIST_CONNECTED_PEERS:
    let numConnPeers = waku.node.wakuRelay.getNumConnectedPeers($self.pubsubTopic).valueOr:
      error "LIST_CONNECTED_PEERS failed", error = error
      return err($error)
    return ok($numConnPeers)
  of LIST_MESH_PEERS:
    let numPeersInMesh = waku.node.wakuRelay.getNumPeersInMesh($self.pubsubTopic).valueOr:
      error "LIST_MESH_PEERS failed", error = error
      return err($error)
    return ok($numPeersInMesh)
  of ADD_PROTECTED_SHARD:
    try:
      let relayShard =
        RelayShard(clusterId: uint16(self.clusterId), shardId: uint16(self.shardId))
      let protectedShard =
        ProtectedShard.parseCmdArg($relayShard & ":" & $self.publicKey)
      waku.node.wakuRelay.addSignedShardsValidator(
        @[protectedShard], uint16(self.clusterId)
      )
    except ValueError:
      return err("ADD_PROTECTED_SHARD exception: " & getCurrentExceptionMsg())
  return ok("")
