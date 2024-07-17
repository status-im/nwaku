{.push raises: [].}

## Notice that the REST /lightpush requests normally assume that the node
## is acting as a lightpush-client that will trigger the service provider node
## to relay the message.
## In this module, we allow that a lightpush service node (full node) can be
## triggered directly through the  REST /lightpush endpoint.
## The typical use case for that is when using `nwaku-compose`,
## which spawn a full service Waku node
## that could be used also as a lightpush client, helping testing and development.

import results, chronos, chronicles, std/options, metrics, stew/byteutils
import
  ../waku_core,
  ./protocol,
  ./common,
  ./rpc,
  ./rpc_codec,
  ./protocol_metrics,
  ../utils/requests

proc handleSelfLightPushRequest*(
    self: WakuLegacyLightPush, pubSubTopic: PubsubTopic, message: WakuMessage
): Future[WakuLightPushResult[string]] {.async.} =
  ## Handles the lightpush requests made by the node to itself.
  ## Normally used in REST-lightpush requests
  ## On success, returns the msg_hash of the published message.

  try:
    # provide self peerId as now this node is used directly, thus there is no light client sender peer.
    let selfPeerId = self.peerManager.switch.peerInfo.peerId

    let req = PushRequest(pubSubTopic: pubSubTopic, message: message)
    let rpc = PushRPC(requestId: generateRequestId(self.rng), request: some(req))

    let respRpc = await self.handleRequest(selfPeerId, rpc.encode().buffer)

    if respRpc.response.isNone():
      waku_legacy_lightpush_errors.inc(labelValues = [emptyResponseBodyFailure])
      return err(emptyResponseBodyFailure)

    let response = respRpc.response.get()
    if not response.isSuccess:
      if response.info.isSome():
        return err(response.info.get())
      else:
        return err("unknown failure")

    let msg_hash_hex_str = computeMessageHash(pubSubTopic, message).to0xHex()

    notice "publishing message with self hosted lightpush",
      pubsubTopic = pubsubTopic,
      contentTopic = message.contentTopic,
      self_peer_id = selfPeerId,
      msg_hash = msg_hash_hex_str

    return ok(msg_hash_hex_str)
  except Exception:
    return err("exception in handleSelfLightPushRequest: " & getCurrentExceptionMsg())
