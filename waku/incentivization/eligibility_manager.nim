import std/options, chronos, web3, stew/byteutils, stint, results, chronicles

import waku/incentivization/rpc, tests/waku_rln_relay/[utils_onchain, utils]

const SimpleTransferGasUsed = Quantity(21000)
const TxReceiptQueryTimeout = 3.seconds

type EligibilityManager* = ref object # FIXME: make web3 private?
  web3*: Web3

# Initialize the eligibilityManager with a web3 instance
proc init*(
    T: type EligibilityManager, ethClient: string
): Future[EligibilityManager] {.async.} =
  result = EligibilityManager(web3: await newWeb3(ethClient))
  # TODO: handle error if web3 instance is not established

# Clean up the web3 instance
proc close*(eligibilityManager: EligibilityManager) {.async.} =
  await eligibilityManager.web3.close()

proc getTransactionByHash(
    eligibilityManager: EligibilityManager, txHash: TxHash
): Future[TransactionObject] {.async.} =
  await eligibilityManager.web3.provider.eth_getTransactionByHash(txHash)

proc getMinedTransactionReceipt(
    eligibilityManager: EligibilityManager, txHash: TxHash
): Future[Result[ReceiptObject, string]] {.async.} =
  let txReceipt = eligibilityManager.web3.getMinedTransactionReceipt(txHash)
  if (await txReceipt.withTimeout(TxReceiptQueryTimeout)):
    return ok(txReceipt.value())
  else:
    return err("Timeout on tx receipt query, tx hash: " & $txHash)

proc getTxAndTxReceipt(
    eligibilityManager: EligibilityManager, txHash: TxHash
): Future[Result[(TransactionObject, ReceiptObject), string]] {.async.} =
  let txFuture = eligibilityManager.getTransactionByHash(txHash)
  let receiptFuture = eligibilityManager.getMinedTransactionReceipt(txHash)
  await allFutures(txFuture, receiptFuture)
  let tx = txFuture.read()
  let txReceipt = receiptFuture.read()
  if txReceipt.isErr():
    return err("Cannot get tx receipt: " & txReceipt.error)
  return ok((tx, txReceipt.get()))

proc isEligibleTxId*(
    eligibilityManager: EligibilityManager,
    eligibilityProof: EligibilityProof,
    expectedToAddress: Address,
    expectedValueWei: UInt256,
): Future[Result[void, string]] {.async.} =
  ## We consider a tx eligible,
  ## in the context of service incentivization PoC,
  ## if it is confirmed and pays the expected amount to the server's address.
  ## See spec: https://github.com/waku-org/specs/blob/master/standards/core/incentivization.md
  if eligibilityProof.proofOfPayment.isNone():
    return err("Eligibility proof is empty")
  var tx: TransactionObject
  var txReceipt: ReceiptObject
  let txHash = TxHash.fromHex(byteutils.toHex(eligibilityProof.proofOfPayment.get()))
  try:
    let txAndTxReceipt = await eligibilityManager.getTxAndTxReceipt(txHash)
    txAndTxReceipt.isOkOr:
      return err("Failed to fetch tx or tx receipt")
    (tx, txReceipt) = txAndTxReceipt.value()
  except ValueError:
    let errorMsg = "Failed to fetch tx or tx receipt: " & getCurrentExceptionMsg()
    error "exception in isEligibleTxId", error = $errorMsg
    return err($errorMsg)
  # check that it is not a contract creation tx
  let toAddressOption = txReceipt.to
  if toAddressOption.isNone():
    # this is a contract creation tx
    return err("A contract creation tx is not eligible")
  # check that it is a simple transfer (not a contract call)
  # a simple transfer uses 21000 gas
  let gasUsed = txReceipt.gasUsed
  let isSimpleTransferTx = (gasUsed == SimpleTransferGasUsed)
  if not isSimpleTransferTx:
    return err("A contract call tx is not eligible")
  # check that the to address is "as expected"
  let toAddress = toAddressOption.get()
  if toAddress != expectedToAddress:
    return err("Wrong destination address: " & $toAddress)
  # check that the amount is "as expected"
  let txValueWei = tx.value
  if txValueWei != expectedValueWei:
    return err("Wrong tx value: got " & $txValueWei & ", expected " & $expectedValueWei)
  return ok()
