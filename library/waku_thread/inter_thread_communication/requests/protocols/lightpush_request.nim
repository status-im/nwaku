import options
import chronicles, chronos, results
import
  ../../../../../waku/waku_core/message/message,
  ../../../../../waku/waku_core/codecs,
  ../../../../../waku/factory/waku,
  ../../../../../waku/waku_core/message,
  ../../../../../waku/waku_core/time, # Timestamp
  ../../../../../waku/waku_core/topics/pubsub_topic,
  ../../../../../waku/waku_lightpush_legacy/client,
  ../../../../../waku/waku_lightpush_legacy/common,
  ../../../../../waku/node/peer_manager/peer_manager,
  ../../../../alloc

type LightpushMsgType* = enum
  PUBLISH

type ThreadSafeWakuMessage* = object
  payload: SharedSeq[byte]
  contentTopic: cstring
  meta: SharedSeq[byte]
  version: uint32
  timestamp: Timestamp
  ephemeral: bool
  when defined(rln):
    proof: SharedSeq[byte]

type LightpushRequest* = object
  operation: LightpushMsgType
  pubsubTopic: cstring
  message: ThreadSafeWakuMessage # only used in 'PUBLISH' requests

proc createShared*(
    T: type LightpushRequest,
    op: LightpushMsgType,
    pubsubTopic: cstring,
    m = WakuMessage(),
): ptr type T =
  var ret = createShared(T)
  ret[].operation = op
  ret[].pubsubTopic = pubsubTopic.alloc()
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

proc destroyShared(self: ptr LightpushRequest) =
  deallocSharedSeq(self[].message.payload)
  deallocShared(self[].message.contentTopic)
  deallocSharedSeq(self[].message.meta)
  when defined(rln):
    deallocSharedSeq(self[].message.proof)

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
    self: ptr LightpushRequest, waku: ptr Waku
): Future[Result[string, string]] {.async.} =
  defer:
    destroyShared(self)

  case self.operation
  of PUBLISH:
    let msg = self.message.toWakuMessage()
    let pubsubTopic = $self.pubsubTopic

    if waku.node.wakuLightpushClient.isNil():
      let errorMsg = "LightpushRequest waku.node.wakuLightpushClient is nil"
      error "PUBLISH failed", error = errorMsg
      return err(errorMsg)

    let peerOpt = waku.node.peerManager.selectPeer(WakuLightPushCodec)
    if peerOpt.isNone():
      let errorMsg = "failed to lightpublish message, no suitable remote peers"
      error "PUBLISH failed", error = errorMsg
      return err(errorMsg)

    let msgHashHex = (
      await waku.node.wakuLegacyLightpushClient.publish(
        pubsubTopic, msg, peer = peerOpt.get()
      )
    ).valueOr:
      error "PUBLISH failed", error = error
      return err("LightpushRequest error publishing: " & $error)

    return ok(msgHashHex)
