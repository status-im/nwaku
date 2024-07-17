## This module helps to ensure the correct transmission and reception of messages

import results
import chronos
import
  ./recv_monitor,
  ./send_monitor,
  ./delivery_callback,
  ../../waku_core,
  ../../waku_store/client,
  ../../waku_relay/protocol,
  ../../waku_lightpush_legacy/client,
  ../../waku_filter_v2/client

type DeliveryMonitor* = ref object
  sendMonitor: SendMonitor
  recvMonitor: RecvMonitor

proc new*(
    T: type DeliveryMonitor,
    storeClient: WakuStoreClient,
    wakuRelay: protocol.WakuRelay,
    wakuLegacyLightpushClient: WakuLegacyLightpushClient,
    wakuFilterClient: WakuFilterClient,
): Result[T, string] =
  ## storeClient is needed to give store visitility to DeliveryMonitor
  ## wakuRelay and wakuLegacyLightpushClient are needed to give a mechanism to SendMonitor to re-publish
  let sendMonitor = ?SendMonitor.new(storeClient, wakuRelay, wakuLegacyLightpushClient)
  let recvMonitor = RecvMonitor.new(storeClient, wakuFilterClient)
  return ok(DeliveryMonitor(sendMonitor: sendMonitor, recvMonitor: recvMonitor))

proc startDeliveryMonitor*(self: DeliveryMonitor) =
  self.sendMonitor.startSendMonitor()
  self.recvMonitor.startRecvMonitor()

proc stopDeliveryMonitor*(self: DeliveryMonitor) {.async.} =
  self.sendMonitor.stopSendMonitor()
  await self.recvMonitor.stopRecvMonitor()

proc setDeliveryCallback*(self: DeliveryMonitor, deliveryCb: DeliveryFeedbackCallback) =
  ## The deliveryCb is a proc defined by the api client so that it can get delivery feedback
  self.sendMonitor.setDeliveryCallback(deliveryCb)
  self.recvMonitor.setDeliveryCallback(deliveryCb)
