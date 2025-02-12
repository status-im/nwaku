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

  test "incentivization PoC: reputation: reputation table is empty after initialization":
    let manager = ReputationManager.init()
    check manager.peerReputation.len == 0
