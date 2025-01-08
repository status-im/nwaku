import system, std/json, libp2p/[connmanager, peerid]

import ../../waku/common/base64, ./json_base_event

type JsonConnectionChangeEvent* = ref object of JsonEvent
  peerId*: string
  peerEvent*: PeerEventKind

proc new*(
    T: type JsonConnectionChangeEvent, peerId: string, peerEvent: PeerEventKind
): T =
  # Returns a JsonConnectionChangeEvent event as indicated in
  # https://rfc.vac.dev/spec/36/#jsonmessageevent-type

  return JsonConnectionChangeEvent(
    eventType: "connection_change", peerId: peerId, peerEvent: peerEvent
  )

method `$`*(jsonConnectionChangeEvent: JsonConnectionChangeEvent): string =
  $(%*jsonConnectionChangeEvent)
