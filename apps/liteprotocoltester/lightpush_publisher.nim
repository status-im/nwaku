import
  std/[strformat, sysrand, random, strutils, sequtils],
  system/ansi_c,
  chronicles,
  chronos,
  chronos/timer as chtimer,
  stew/byteutils,
  results,
  json_serialization as js
import
  waku/[
    common/logging,
    waku_node,
    node/peer_manager,
    waku_core,
    waku_lightpush/client,
    waku_lightpush/common,
    common/utils/parse_size_units,
  ],
  ./tester_config,
  ./tester_message,
  ./lpt_metrics,
  ./diagnose_connections,
  ./service_peer_management

randomize()

type SizeRange* = tuple[min: uint64, max: uint64]

var RANDOM_PALYLOAD {.threadvar.}: seq[byte]
RANDOM_PALYLOAD = urandom(1024 * 1024)
  # 1MiB of random payload to be used to extend message

proc prepareMessage(
    sender: string,
    messageIndex, numMessages: uint32,
    startedAt: TimeStamp,
    prevMessageAt: var Timestamp,
    contentTopic: ContentTopic,
    size: SizeRange,
): (WakuMessage, uint64) =
  var renderSize = rand(size.min .. size.max)
  let current = getNowInNanosecondTime()
  let payload = ProtocolTesterMessage(
    sender: sender,
    index: messageIndex,
    count: numMessages,
    startedAt: startedAt,
    sinceStart: current - startedAt,
    sincePrev: current - prevMessageAt,
    size: renderSize,
  )

  prevMessageAt = current

  let text = js.Json.encode(payload)
  let contentPayload = toBytes(text & " \0")

  if renderSize < len(contentPayload).uint64:
    renderSize = len(contentPayload).uint64

  let finalPayload = concat(
    contentPayload, RANDOM_PALYLOAD[0 .. renderSize - len(contentPayload).uint64]
  )
  let message = WakuMessage(
    payload: finalPayload, # content of the message
    contentTopic: contentTopic, # content topic to publish to
    ephemeral: true, # tell store nodes to not store it
    timestamp: current, # current timestamp
  )

  return (message, renderSize)

var sentMessages {.threadvar.}: OrderedTable[uint32, tuple[hash: string, relayed: bool]]
var failedToSendCause {.threadvar.}: Table[string, uint32]
var failedToSendCount {.threadvar.}: uint32
var numMessagesToSend {.threadvar.}: uint32
var messagesSent {.threadvar.}: uint32
var noOfServicePeerSwitches {.threadvar.}: uint32

proc reportSentMessages() =
  let report = catch:
    """*----------------------------------------*
|  Service Peer Switches: {noOfServicePeerSwitches:>15} |
*----------------------------------------*
|  Expected  |    Sent    |   Failed   |
|{numMessagesToSend+failedToSendCount:>11} |{messagesSent:>11} |{failedToSendCount:>11} |
*----------------------------------------*""".fmt()

  if report.isErr:
    echo "Error while printing statistics"
  else:
    echo report.get()

  echo "*--------------------------------------------------------------------------------------------------*"
  echo "|  Failure cause                                                                         |  count   |"
  for (cause, count) in failedToSendCause.pairs:
    echo fmt"|{cause:<87}|{count:>10}|"
  echo "*--------------------------------------------------------------------------------------------------*"

  echo "*--------------------------------------------------------------------------------------------------*"
  echo "|  Index   | Relayed | Hash                                                                        |"
  for (index, info) in sentMessages.pairs:
    echo fmt"|{index+1:>10}|{info.relayed:<9}| {info.hash:<76}|"
  echo "*--------------------------------------------------------------------------------------------------*"
  # evere sent message hash should logged once
  sentMessages.clear()

proc publishMessages(
    wakuNode: WakuNode,
    servicePeer: RemotePeerInfo,
    lightpushPubsubTopic: PubsubTopic,
    lightpushContentTopic: ContentTopic,
    numMessages: uint32,
    messageSizeRange: SizeRange,
    messageInterval: Duration,
    preventPeerSwitch: bool,
) {.async.} =
  var actualServicePeer = servicePeer
  let startedAt = getNowInNanosecondTime()
  var prevMessageAt = startedAt
  var renderMsgSize = messageSizeRange
  # sets some default of min max message size to avoid conflict with meaningful payload size
  renderMsgSize.min = max(1024.uint64, renderMsgSize.min) # do not use less than 1KB
  renderMsgSize.max = max(2048.uint64, renderMsgSize.max) # minimum of max is 2KB
  renderMsgSize.min = min(renderMsgSize.min, renderMsgSize.max)
  renderMsgSize.max = max(renderMsgSize.min, renderMsgSize.max)

  const maxFailedPush = 3
  var noFailedPush = 0
  var noFailedServiceNodeSwitches = 0

  let selfPeerId = $wakuNode.switch.peerInfo.peerId
  failedToSendCount = 0
  numMessagesToSend = if numMessages == 0: uint32.high else: numMessages
  messagesSent = 0

  while messagesSent < numMessagesToSend:
    let (message, msgSize) = prepareMessage(
      selfPeerId,
      messagesSent + 1,
      numMessagesToSend,
      startedAt,
      prevMessageAt,
      lightpushContentTopic,
      renderMsgSize,
    )
    let wlpRes = await wakuNode.legacyLightpushPublish(
      some(lightpushPubsubTopic), message, actualServicePeer
    )

    let msgHash = computeMessageHash(lightpushPubsubTopic, message).to0xHex

    if wlpRes.isOk():
      sentMessages[messagesSent] = (hash: msgHash, relayed: true)
      notice "published message using lightpush",
        index = messagesSent + 1,
        count = numMessagesToSend,
        size = msgSize,
        pubsubTopic = lightpushPubsubTopic,
        hash = msgHash
      inc(messagesSent)
      lpt_publisher_sent_messages_count.inc()
      lpt_publisher_sent_bytes.inc(amount = msgSize.int64)
      if noFailedPush > 0:
        noFailedPush -= 1
    else:
      sentMessages[messagesSent] = (hash: msgHash, relayed: false)
      failedToSendCause.mgetOrPut(wlpRes.error, 1).inc()
      error "failed to publish message using lightpush",
        err = wlpRes.error, hash = msgHash
      inc(failedToSendCount)
      lpt_publisher_failed_messages_count.inc(labelValues = [wlpRes.error])
      if not wlpRes.error.toLower().contains("dial"):
        # retry sending after shorter wait
        await sleepAsync(2.seconds)
        continue
      else:
        noFailedPush += 1
        lpt_service_peer_failure_count.inc(
          labelValues = ["publisher", actualServicePeer.getAgent()]
        )
        if not preventPeerSwitch and noFailedPush > maxFailedPush:
          info "Max push failure limit reached, Try switching peer."
          let peerOpt = selectRandomServicePeer(
            wakuNode.peerManager, some(actualServicePeer), WakuLightPushCodec
          )
          if peerOpt.isOk():
            actualServicePeer = peerOpt.get()

            info "New service peer in use",
              codec = lightpushPubsubTopic,
              peer = constructMultiaddrStr(actualServicePeer)

            noFailedPush = 0
            noOfServicePeerSwitches += 1
            lpt_change_service_peer_count.inc(labelValues = ["publisher"])
            continue # try again with new peer without delay
          else:
            error "Failed to find new service peer. Exiting."
            noFailedServiceNodeSwitches += 1
            break

    await sleepAsync(messageInterval)

proc setupAndPublish*(
    wakuNode: WakuNode, conf: LiteProtocolTesterConf, servicePeer: RemotePeerInfo
) =
  if isNil(wakuNode.wakuLightpushClient):
    # if we have not yet initialized lightpush client, then do it as the only way we can get here is
    # by having a service peer discovered.
    wakuNode.mountLegacyLightPushClient()

  # give some time to receiver side to set up
  let waitTillStartTesting = conf.startPublishingAfter.seconds

  let parsedMinMsgSize = parseMsgSize(conf.minTestMessageSize).valueOr:
    error "failed to parse 'min-test-msg-size' param: ", error = error
    return

  let parsedMaxMsgSize = parseMsgSize(conf.maxTestMessageSize).valueOr:
    error "failed to parse 'max-test-msg-size' param: ", error = error
    return

  info "Sending test messages in", wait = waitTillStartTesting
  waitFor sleepAsync(waitTillStartTesting)

  info "Start sending messages to service node using lightpush"

  sentMessages.sort(system.cmp)

  let interval = secs(60)
  var printStats: CallbackFunc

  printStats = CallbackFunc(
    proc(udata: pointer) {.gcsafe.} =
      reportSentMessages()

      if messagesSent >= numMessagesToSend:
        info "All messages are sent. Exiting."

        ## for gracefull shutdown through signal hooks
        discard c_raise(ansi_c.SIGTERM)
      else:
        discard setTimer(Moment.fromNow(interval), printStats)
  )

  discard setTimer(Moment.fromNow(interval), printStats)

  # Start maintaining subscription
  asyncSpawn publishMessages(
    wakuNode,
    servicePeer,
    conf.pubsubTopics[0],
    conf.contentTopics[0],
    conf.numMessages,
    (min: parsedMinMsgSize, max: parsedMaxMsgSize),
    conf.messageInterval.milliseconds,
    conf.fixedServicePeer,
  )
