## This module reinforces the publish operation with regular store-v3 requests.
##

import std/sequtils
import chronos, chronicles, libp2p/utility
import
  ./delivery_callback,
  ./publish_observer,
  ../../waku_core,
  ./not_delivered_storage/not_delivered_storage,
  ../../waku_store/[client, common],
  ../../waku_archive/archive,
  ../../waku_relay/protocol,
  ../../waku_lightpush_legacy/client

const MaxTimeInCache* = chronos.minutes(1)
  ## Messages older than this time will get completely forgotten on publication and a
  ## feedback will be given when that happens

const SendCheckInterval* = chronos.seconds(3)
  ## Interval at which we check that messages have been properly received by a store node

const MaxMessagesToCheckAtOnce = 100
  ## Max number of messages to check if they were properly archived by a store node

const ArchiveTime = chronos.seconds(3)
  ## Estimation of the time we wait until we start confirming that a message has been properly
  ## received and archived by a store node

type DeliveryInfo = object
  pubsubTopic: string
  msg: WakuMessage

type SendMonitor* = ref object of PublishObserver
  publishedMessages: Table[WakuMessageHash, DeliveryInfo]
    ## Cache that contains the delivery info per message hash.
    ## This is needed to make sure the published messages are properly published

  msgStoredCheckerHandle: Future[void] ## handle that allows to stop the async task

  notDeliveredStorage: NotDeliveredStorage
    ## NOTE: this is not fully used because that might be tackled by higher abstraction layers

  storeClient: WakuStoreClient
  deliveryCb: DeliveryFeedbackCallback

  wakuRelay: protocol.WakuRelay
  wakuLegacyLightpushClient: WakuLegacyLightPushClient

proc new*(
    T: type SendMonitor,
    storeClient: WakuStoreClient,
    wakuRelay: protocol.WakuRelay,
    wakuLegacyLightpushClient: WakuLegacyLightPushClient,
): Result[T, string] =
  if wakuRelay.isNil() and wakuLegacyLightpushClient.isNil():
    return err(
      "Could not create SendMonitor. wakuRelay or wakuLightpushClient should be set"
    )

  let notDeliveredStorage = ?NotDeliveredStorage.new()

  let sendMonitor = SendMonitor(
    notDeliveredStorage: notDeliveredStorage,
    storeClient: storeClient,
    wakuRelay: wakuRelay,
    wakuLegacyLightpushClient: wakuLegacyLightPushClient,
  )

  if not wakuRelay.isNil():
    wakuRelay.addPublishObserver(sendMonitor)

  if not wakuLegacyLightpushClient.isNil():
    wakuLegacyLightpushClient.addPublishObserver(sendMonitor)

  return ok(sendMonitor)

proc performFeedbackAndCleanup(
    self: SendMonitor,
    msgsToDiscard: Table[WakuMessageHash, DeliveryInfo],
    success: DeliverySuccess,
    dir: DeliveryDirection,
    comment: string,
) =
  ## This procs allows to bring delivery feedback to the API client
  ## It requires a 'deliveryCb' to be registered beforehand.
  if self.deliveryCb.isNil():
    error "deliveryCb is nil in performFeedbackAndCleanup",
      success, dir, comment, hashes = toSeq(msgsToDiscard.keys).mapIt(shortLog(it))
    return

  for hash, deliveryInfo in msgsToDiscard:
    debug "send monitor performFeedbackAndCleanup",
      success, dir, comment, msg_hash = shortLog(hash)

    self.deliveryCb(success, dir, comment, hash, deliveryInfo.msg)
    self.publishedMessages.del(hash)

proc checkMsgsInStore(
    self: SendMonitor, msgsToValidate: Table[WakuMessageHash, DeliveryInfo]
): Future[
    Result[
      tuple[
        publishedCorrectly: Table[WakuMessageHash, DeliveryInfo],
        notYetPublished: Table[WakuMessageHash, DeliveryInfo],
      ],
      void,
    ]
] {.async.} =
  let hashesToValidate = toSeq(msgsToValidate.keys)

  let storeResp: StoreQueryResponse = (
    await self.storeClient.queryToAny(
      StoreQueryRequest(includeData: false, messageHashes: hashesToValidate)
    )
  ).valueOr:
    error "checkMsgsInStore failed to get remote msgHashes",
      hashes = hashesToValidate.mapIt(shortLog(it)), error = $error
    return err()

  let publishedHashes = storeResp.messages.mapIt(it.messageHash)

  var notYetPublished: Table[WakuMessageHash, DeliveryInfo]
  var publishedCorrectly: Table[WakuMessageHash, DeliveryInfo]

  for msgHash, deliveryInfo in msgsToValidate.pairs:
    if publishedHashes.contains(msgHash):
      publishedCorrectly[msgHash] = deliveryInfo
      self.publishedMessages.del(msgHash) ## we will no longer track that message
    else:
      notYetPublished[msgHash] = deliveryInfo

  return ok((publishedCorrectly: publishedCorrectly, notYetPublished: notYetPublished))

proc processMessages(self: SendMonitor) {.async.} =
  var msgsToValidate: Table[WakuMessageHash, DeliveryInfo]
  var msgsToDiscard: Table[WakuMessageHash, DeliveryInfo]

  let now = getNowInNanosecondTime()
  let timeToCheckThreshold = now - ArchiveTime.nanos
  let maxLifeTime = now - MaxTimeInCache.nanos

  for hash, deliveryInfo in self.publishedMessages.pairs:
    if deliveryInfo.msg.timestamp < maxLifeTime:
      ## message is too old
      msgsToDiscard[hash] = deliveryInfo

    if deliveryInfo.msg.timestamp < timeToCheckThreshold:
      msgsToValidate[hash] = deliveryInfo

  ## Discard the messages that are too old
  self.performFeedbackAndCleanup(
    msgsToDiscard, DeliverySuccess.UNSUCCESSFUL, DeliveryDirection.PUBLISHING,
    "Could not publish messages. Please try again.",
  )

  let (publishedCorrectly, notYetPublished) = (
    await self.checkMsgsInStore(msgsToValidate)
  ).valueOr:
    return ## the error log is printed in checkMsgsInStore

  ## Give positive feedback for the correctly published messages
  self.performFeedbackAndCleanup(
    publishedCorrectly, DeliverySuccess.SUCCESSFUL, DeliveryDirection.PUBLISHING,
    "messages published correctly",
  )

  ## Try to publish again
  for msgHash, deliveryInfo in notYetPublished.pairs:
    let pubsubTopic = deliveryInfo.pubsubTopic
    let msg = deliveryInfo.msg
    if not self.wakuRelay.isNil():
      debug "trying to publish again with wakuRelay", msgHash, pubsubTopic
      let ret = await self.wakuRelay.publish(pubsubTopic, msg)
      if ret.isErr():
        error "could not publish with wakuRelay.publish",
          msgHash, pubsubTopic, reason = ret.error.msg
      continue

    if not self.wakuLegacyLightpushClient.isNil():
      debug "trying to publish again with wakuLightpushClient", msgHash, pubsubTopic
      (await self.wakuLegacyLightpushClient.publishToAny(pubsubTopic, msg)).isOkOr:
        error "could not publish with publishToAny", error = $error
      continue

proc checkIfMessagesStored(self: SendMonitor) {.async.} =
  ## Continuously monitors that the sent messages have been received by a store node
  while true:
    await self.processMessages()
    await sleepAsync(SendCheckInterval)

method onMessagePublished(
    self: SendMonitor, pubsubTopic: string, msg: WakuMessage
) {.gcsafe, raises: [].} =
  ## Implementation of the PublishObserver interface.
  ##
  ## When publishing a message either through relay or lightpush, we want to add some extra effort
  ## to make sure it is received to one store node. Hence, keep track of those published messages.

  debug "onMessagePublished"
  let msgHash = computeMessageHash(pubSubTopic, msg)

  if not self.publishedMessages.hasKey(msgHash):
    self.publishedMessages[msgHash] = DeliveryInfo(pubsubTopic: pubsubTopic, msg: msg)

proc startSendMonitor*(self: SendMonitor) =
  self.msgStoredCheckerHandle = self.checkIfMessagesStored()

proc stopSendMonitor*(self: SendMonitor) =
  discard self.msgStoredCheckerHandle.cancelAndWait()

proc setDeliveryCallback*(self: SendMonitor, deliveryCb: DeliveryFeedbackCallback) =
  self.deliveryCb = deliveryCb
