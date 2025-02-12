import tables

type
  PeerID = string
  ReputationScore = int

  ReputationManager* = ref object
    peerReputation*: Table[PeerID, ReputationScore]

proc init*(T: type ReputationManager): ReputationManager =
  return ReputationManager(peerReputation: initTable[PeerID, ReputationScore]())
