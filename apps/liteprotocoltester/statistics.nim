{.push raises: [].}

import
  std/[sets, tables, strutils, sequtils, options, strformat],
  chronos/timer as chtimer,
  chronicles,
  chronos,
  results

import ./tester_message

type
  ArrivalInfo = object
    arrivedAt: Moment
    prevArrivedAt: Moment
    prevIndex: uint32

  MessageInfo = tuple[msg: ProtocolTesterMessage, info: ArrivalInfo]

  StatHelper = object
    prevIndex: uint32
    prevArrivedAt: Moment
    lostIndices: HashSet[uint32]
    seenIndices: HashSet[uint32]
    maxIndex: uint32

  Statistics* = object
    received: Table[uint32, MessageInfo]
    firstReceivedIdx*: uint32
    allMessageCount*: uint32
    receivedMessages*: uint32
    misorderCount*: uint32
    lateCount*: uint32
    duplicateCount*: uint32
    helper: StatHelper

  PerPeerStatistics* = Table[string, Statistics]

func `$`*(a: Duration): string {.inline.} =
  ## Original stringify implementation from chronos/timer.nim is not capable of printing 0ns
  ## Returns string representation of Duration ``a`` as nanoseconds value.

  if a.isZero:
    return "0ns"

  return chtimer.`$`(a)

proc init*(T: type Statistics, expectedMessageCount: int = 1000): T =
  result.helper.prevIndex = 0
  result.helper.maxIndex = 0
  result.helper.seenIndices.init(expectedMessageCount)
  result.received = initTable[uint32, MessageInfo](expectedMessageCount)
  return result

proc addMessage*(self: var Statistics, msg: ProtocolTesterMessage) =
  if self.allMessageCount == 0:
    self.allMessageCount = msg.count
    self.firstReceivedIdx = msg.index
  elif self.allMessageCount != msg.count:
    error "Message count mismatch at message",
      index = msg.index, expected = self.allMessageCount, got = msg.count

  let currentArrived: MessageInfo = (
    msg: msg,
    info: ArrivalInfo(
      arrivedAt: Moment.now(),
      prevArrivedAt: self.helper.prevArrivedAt,
      prevIndex: self.helper.prevIndex,
    ),
  )

  if self.received.hasKeyOrPut(msg.index, currentArrived):
    warn "Duplicate message", index = msg.index
    inc(self.duplicateCount)
    return

  ## detect misorder arrival and possible lost messages
  if self.helper.prevIndex + 1 < msg.index:
    inc(self.misorderCount)
    warn "Misordered message arrival",
      index = msg.index, expected = self.helper.prevIndex + 1
  elif self.helper.prevIndex > msg.index:
    inc(self.lateCount)
    warn "Late message arrival", index = msg.index, expected = self.helper.prevIndex + 1

  self.helper.maxIndex = max(self.helper.maxIndex, msg.index)
  self.helper.prevIndex = msg.index
  self.helper.prevArrivedAt = currentArrived.info.arrivedAt
  inc(self.receivedMessages)

proc addMessage*(
    self: var PerPeerStatistics, peerId: string, msg: ProtocolTesterMessage
) =
  if not self.contains(peerId):
    self[peerId] = Statistics.init()

  discard catch:
    self[peerId].addMessage(msg)

proc lossCount*(self: Statistics): uint32 =
  self.helper.maxIndex - self.receivedMessages

proc calcLatency*(self: Statistics): tuple[min, max, avg: Duration] =
  var
    minLatency = nanos(0)
    maxLatency = nanos(0)
    avgLatency = nanos(0)

  if self.receivedMessages > 2:
    try:
      var prevArrivedAt = self.received[self.firstReceivedIdx].info.arrivedAt

      for idx, (msg, arrival) in self.received.pairs:
        if idx <= 1:
          continue
        let expectedDelay = nanos(msg.sincePrev)

        ## latency will be 0 if arrived in shorter time than expected
        var latency = arrival.arrivedAt - arrival.prevArrivedAt - expectedDelay

        if latency > nanos(0):
          if minLatency == nanos(0):
            minLatency = latency
          else:
            minLatency = min(minLatency, latency)

        maxLatency = max(maxLatency, latency)
        avgLatency += latency

      avgLatency = avgLatency div (self.receivedMessages - 1)
    except KeyError:
      error "Error while calculating latency"

  return (minLatency, maxLatency, avgLatency)

proc missingIndices*(self: Statistics): seq[uint32] =
  var missing: seq[uint32] = @[]
  for idx in 1 .. self.helper.maxIndex:
    if not self.received.hasKey(idx):
      missing.add(idx)
  return missing

proc echoStat*(self: Statistics) =
  let (minL, maxL, avgL) = self.calcLatency()

  let printable = catch:
    """*------------------------------------------------------------------------------------------*
|  Expected  |  Received  |   Target   |    Loss    |  Misorder  |    Late    |  Duplicate |
|{self.helper.maxIndex:>11} |{self.receivedMessages:>11} |{self.allMessageCount:>11} |{self.lossCount():>11} |{self.misorderCount:>11} |{self.lateCount:>11} |{self.duplicateCount:>11} |
*------------------------------------------------------------------------------------------*
| Latency stat:                                                                            |
|    min latency: {$minL:<73}|
|    avg latency: {$avgL:<73}|
|    max latency: {$maxL:<73}|
*------------------------------------------------------------------------------------------*
| Lost indices:                                                                            |
|  {self.missingIndices()} |
*------------------------------------------------------------------------------------------*""".fmt()

  if printable.isErr():
    echo "Error while printing statistics: " & printable.error().msg
  else:
    echo printable.get()

proc jsonStat*(self: Statistics): string =
  let minL, maxL, avgL = self.calcLatency()

  let json = catch:
    """{{"expected":{self.helper.maxIndex},
         "received": {self.receivedMessages},
         "target": {self.allMessageCount},
         "loss": {self.lossCount()},
         "misorder": {self.misorderCount},
         "late": {self.lateCount},
         "duplicate": {self.duplicateCount},
         "latency":
           {{"avg": "{avgL}",
             "min": "{minL}",
             "max": "{maxL}"
           }},
          "lostIndices": {self.missingIndices()}
     }}""".fmt()
  if json.isErr:
    return "{\"result:\": \"" & json.error.msg & "\"}"

  return json.get()

proc echoStats*(self: var PerPeerStatistics) =
  for peerId, stats in self.pairs:
    let peerLine = catch:
      "Receiver statistics from peer {peerId}".fmt()
    if peerLine.isErr:
      echo "Error while printing statistics"
    else:
      echo peerLine.get()
      stats.echoStat()

proc jsonStats*(self: PerPeerStatistics): string =
  try:
    #!fmt: off
    var json = "{\"statistics\": ["
    var first = true
    for peerId, stats in self.pairs:
      if first:
        first = false
      else:
        json.add(", ")
      json.add("{{\"sender\": \"{peerId}\", \"stat\":".fmt())
      json.add(stats.jsonStat())
      json.add("}")
    json.add("]}")
    return json
    #!fmt: on
  except CatchableError:
    return
      "{\"result:\": \"Error while generating json stats: " & getCurrentExceptionMsg() &
      "\"}"

proc checkIfAllMessagesReceived*(self: PerPeerStatistics): Future[bool] {.async.} =
  # if there are no peers have sent messages, assume we just have started.
  if self.len == 0:
    return false

  for stat in self.values:
    if (stat.allMessageCount == 0 and stat.receivedMessages == 0) or
        stat.helper.maxIndex < stat.allMessageCount:
      return false

  ## Ok, we see last message arrived from all peers,
  ## lets check if all messages are received
  ## and if not let's wait another 20 secs to give chance the system will send them.
  var shallWait = false
  for stat in self.values:
    if stat.receivedMessages < stat.allMessageCount:
      shallWait = true

  if shallWait:
    await sleepAsync(chtimer.seconds(20))

  return true
