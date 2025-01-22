import std/options, testutils/unittests, chronos, libp2p/crypto/crypto, web3

import waku/incentivization/[rpc, rpc_codec, common]

suite "Waku Incentivization Eligibility Codec":
  asyncTest "encode eligibility proof from txid":
    let txHash = TxHash.fromHex(
      "0x0000000000000000000000000000000000000000000000000000000000000000"
    )
    let txHashAsBytes = @(txHash.bytes())
    let eligibilityProof = EligibilityProof(proofOfPayment: some(txHashAsBytes))
    let encoded = encode(eligibilityProof)
    let decoded = EligibilityProof.decode(encoded.buffer).get()
    check:
      eligibilityProof == decoded

  asyncTest "encode eligibility status":
    let eligibilityStatus = init(EligibilityStatus, true)
    let encoded = encode(eligibilityStatus)
    let decoded = EligibilityStatus.decode(encoded.buffer).get()
    check:
      eligibilityStatus == decoded
