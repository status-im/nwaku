{.used.}

import
  std/[options],
  testutils/unittests,
  chronos,
  web3,
  stew/byteutils,
  stint,
  strutils,
  tests/testlib/testasync

import
  waku/[node/peer_manager, waku_core],
  waku/incentivization/[rpc, eligibility_manager],
  ../waku_rln_relay/[utils_onchain, utils]

const TxHashNonExisting =
  TxHash.fromHex("0x0000000000000000000000000000000000000000000000000000000000000000")

# Anvil RPC URL
const EthClient = "ws://127.0.0.1:8540"

const TxValueExpectedWei = 1000.u256

## Storage.sol contract from https://remix.ethereum.org/
## Compiled with Solidity compiler version "0.8.26+commit.8a97fa7a"

const ExampleStorageContractBytecode =
  "6080604052348015600e575f80fd5b506101438061001c5f395ff3fe608060405234801561000f575f80fd5b5060043610610034575f3560e01c80632e64cec1146100385780636057361d14610056575b5f80fd5b610040610072565b60405161004d919061009b565b60405180910390f35b610070600480360381019061006b91906100e2565b61007a565b005b5f8054905090565b805f8190555050565b5f819050919050565b61009581610083565b82525050565b5f6020820190506100ae5f83018461008c565b92915050565b5f80fd5b6100c181610083565b81146100cb575f80fd5b50565b5f813590506100dc816100b8565b92915050565b5f602082840312156100f7576100f66100b4565b5b5f610104848285016100ce565b9150509291505056fea26469706673582212209a0dd35336aff1eb3eeb11db76aa60a1427a12c1b92f945ea8c8d1dfa337cf2264736f6c634300081a0033"

contract(ExampleStorageContract):
  proc number(): UInt256 {.view.}
  proc store(num: UInt256)
  proc retrieve(): UInt256 {.view.}

#[
// SPDX-License-Identifier: GPL-3.0

pragma solidity >=0.8.2 <0.9.0;

/**
 * @title Storage
 * @dev Store & retrieve value in a variable
 * @custom:dev-run-script ./scripts/deploy_with_ethers.ts
 */
contract Storage {

    uint256 number;

    /**
     * @dev Store value in variable
     * @param num value to store
     */
    function store(uint256 num) public {
        number = num;
    }

    /**
     * @dev Return value 
     * @return value of 'number'
     */
    function retrieve() public view returns (uint256){
        return number;
    }
}
]#

proc setup(
    manager: EligibilityManager
): Future[(TxHash, TxHash, TxHash, TxHash, TxHash, Address, Address)] {.async.} =
  ## Populate the local chain (connected to via manager)
  ## with txs required for eligibility testing.
  ## 
  ## 1. Depoly a dummy contract that has a publicly callable function.
  ##    (While doing so, we confirm a contract creation tx.)
  ## 2. Confirm these transactions:
  ## - a contract call tx (eligibility test must fail)
  ## - a simple transfer with the wrong receiver (must fail)
  ## - a simple transfer with the wrong amount (must fail)
  ## - a simple transfer with the right receiver and amount (must pass)

  let web3 = manager.web3

  let accounts = await web3.provider.eth_accounts()
  web3.defaultAccount = accounts[0]
  let sender = web3.defaultAccount
  let receiverExpected = accounts[1]
  let receiverNotExpected = accounts[2]

  let txValueEthExpected = TxValueExpectedWei
  let txValueEthNotExpected = txValueEthExpected + 1

  # wrong receiver, wrong amount
  let txHashWrongReceiverRightAmount =
    await web3.sendEthTransfer(sender, receiverNotExpected, txValueEthExpected)

  # right receiver, wrong amount
  let txHashRightReceiverWrongAmount =
    await web3.sendEthTransfer(sender, receiverExpected, txValueEthNotExpected)

  # right receiver, right amount
  let txHashRightReceiverRightAmount =
    await web3.sendEthTransfer(sender, receiverExpected, txValueEthExpected)

  let receipt = await web3.deployContract(ExampleStorageContractBytecode)
  let txHashContractCreation = receipt.transactionHash
  let exampleStorageContractAddress = receipt.contractAddress.get()
  let exampleStorageContract =
    web3.contractSender(ExampleStorageContract, exampleStorageContractAddress)

  let txHashContractCall = await exampleStorageContract.store(1.u256).send()

  return (
    txHashWrongReceiverRightAmount, txHashRightReceiverWrongAmount,
    txHashRightReceiverRightAmount, txHashContractCreation, txHashContractCall,
    receiverExpected, receiverNotExpected,
  )

suite "Waku Incentivization PoC Eligibility Proofs":
  ## Tests for service incentivization PoC.
  ## In a client-server interaction, a client submits an eligibility proof to the server.
  ## The server provides the service if and only if the proof is valid.
  ## In PoC, a txid serves as eligibility proof.
  ## The txid reflects the confirmed payment from the client to the server.
  ## The request is eligible if the tx is confirmed and pays the correct amount to the correct address.
  ## The tx must also be of a "simple transfer" type (not a contract creation, not a contract call).
  ## See spec: https://github.com/waku-org/specs/blob/master/standards/core/incentivization.md

  ## Start Anvil
  let runAnvil {.used.} = runAnvil()

  var txHashWrongReceiverRightAmount, txHashRightReceiverWrongAmount,
    txHashRightReceiverRightAmount, txHashContractCreation, txHashContractCall: TxHash

  var receiverExpected, receiverNotExpected: Address

  var manager {.threadvar.}: EligibilityManager

  asyncSetup:
    manager = await EligibilityManager.init(EthClient)

    (
      txHashWrongReceiverRightAmount, txHashRightReceiverWrongAmount,
      txHashRightReceiverRightAmount, txHashContractCreation, txHashContractCall,
      receiverExpected, receiverNotExpected,
    ) = await manager.setup()

  asyncTeardown:
    await manager.close()

  asyncTest "incentivization PoC: non-existent tx is not eligible":
    ## Test that an unconfirmed tx is not eligible.

    let eligibilityProof =
      EligibilityProof(proofOfPayment: some(@(TxHashNonExisting.bytes())))
    let isEligible = await manager.isEligibleTxId(
      eligibilityProof, receiverExpected, TxValueExpectedWei
    )
    check:
      isEligible.isErr()

  asyncTest "incentivization PoC: contract creation tx is not eligible":
    ## Test that a contract creation tx is not eligible.

    let eligibilityProof =
      EligibilityProof(proofOfPayment: some(@(txHashContractCreation.bytes())))
    let isEligible = await manager.isEligibleTxId(
      eligibilityProof, receiverExpected, TxValueExpectedWei
    )
    check:
      isEligible.isErr()

  asyncTest "incentivization PoC: contract call tx is not eligible":
    ## Test that a contract call tx is not eligible.
    ## This assumes a payment in native currency (ETH), not a token.

    let eligibilityProof =
      EligibilityProof(proofOfPayment: some(@(txHashContractCall.bytes())))
    let isEligible = await manager.isEligibleTxId(
      eligibilityProof, receiverExpected, TxValueExpectedWei
    )
    check:
      isEligible.isErr()

  asyncTest "incentivization PoC: simple transfer tx is eligible":
    ## Test that a simple transfer tx is eligible (if necessary conditions hold).

    let eligibilityProof =
      EligibilityProof(proofOfPayment: some(@(txHashRightReceiverRightAmount.bytes())))
    let isEligible = await manager.isEligibleTxId(
      eligibilityProof, receiverExpected, TxValueExpectedWei
    )

    assert isEligible.isOk(), isEligible.error

  # Stop Anvil daemon
  stopAnvil(runAnvil)
