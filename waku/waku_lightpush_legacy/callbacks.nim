{.push raises: [].}

import
  ../waku_core,
  ../waku_relay,
  ./common,
  ./protocol_metrics,
  ../waku_rln_relay,
  ../waku_rln_relay/protocol_types

import std/times, libp2p/peerid, stew/byteutils

proc checkAndGenerateRLNProof*(
    rlnPeer: Option[WakuRLNRelay], message: WakuMessage
): Result[WakuMessage, string] =
  # check if the message already has RLN proof
  if message.proof.len > 0:
    return ok(message)

  if rlnPeer.isNone():
    notice "Publishing message without RLN proof"
    return ok(message)
  # generate and append RLN proof
  let
    time = getTime().toUnix()
    senderEpochTime = float64(time)
  var msgWithProof = message
  rlnPeer.get().appendRLNProof(msgWithProof, senderEpochTime).isOkOr:
    return err(error)
  return ok(msgWithProof)

proc getNilPushHandler*(): PushMessageHandler =
  return proc(
      peer: PeerId, pubsubTopic: string, message: WakuMessage
  ): Future[WakuLightPushResult[void]] {.async.} =
    return err("no waku relay found")

proc getRelayPushHandler*(
    wakuRelay: WakuRelay, rlnPeer: Option[WakuRLNRelay] = none[WakuRLNRelay]()
): PushMessageHandler =
  return proc(
      peer: PeerId, pubsubTopic: string, message: WakuMessage
  ): Future[WakuLightPushResult[void]] {.async.} =
    # append RLN proof
    let msgWithProof = checkAndGenerateRLNProof(rlnPeer, message)
    if msgWithProof.isErr():
      return err(msgWithProof.error)

    (await wakuRelay.validateMessage(pubSubTopic, msgWithProof.value)).isOkOr:
      return err(error)

    let publishResult = await wakuRelay.publish(pubsubTopic, msgWithProof.value)
    if publishResult.isErr():
      ## Agreed change expected to the lightpush protocol to better handle such case. https://github.com/waku-org/pm/issues/93
      let msgHash = computeMessageHash(pubsubTopic, message).to0xHex()
      notice "Lightpush request has not been published to any peers",
        msg_hash = msgHash, reason = publishResult.error.msg
      # for legacy lightpush we do not detail the reason towards clients. All error during publish result in not-published-to-any-peer
      # this let client of the legacy protocol to react as they did so far.
      return err(protocol_metrics.notPublishedAnyPeer)

    return ok()
