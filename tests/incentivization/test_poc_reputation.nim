import
  std/options,
  testutils/unittests,
  chronos,
  web3,
  stew/byteutils,
  stint,
  strutils,
  tests/testlib/testasync

import
  waku/[node/peer_manager, waku_core],
  waku/incentivization/[rpc, reputation_manager]

suite "Waku Incentivization PoC Reputation":
  ## Tests for a client-side reputation system as part of incentivization PoC.
  ## A client maintains a ReputationManager that tracks servers' reputation.
  ## A server's reputation depends on prior interactions with that server.
  ## TODO: think how to reuse existing peer scoring.
  ## TODO: think how to test reputation without integration with an actual protocol.

  var manager {.threadvar.}: ReputationManager

  setup:
    manager = ReputationManager.init()

  test "incentivization PoC: reputation: reputation table is empty after initialization":
    check manager.peerReputation.len == 0

  test "incentivization PoC: reputation: set and get reputation":
    manager.setReputation("peer1", true)
    check manager.getReputation("peer1") == true

  test "incentivization PoC: reputation: evaluate response":
    let response = DummyResponse(peerId: "peer1", responseQuality: true)
    check evaluateResponse(response) == true

  test "incentivization PoC: reputation: update reputation with response":
    let response = DummyResponse(peerId: "peer1", responseQuality: true)
    manager.updateReputation(response)
    check manager.getReputation("peer1") == true
