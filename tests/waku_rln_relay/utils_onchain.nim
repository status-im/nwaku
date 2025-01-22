{.used.}

{.push raises: [].}

import
  std/[options, os, osproc, sequtils, deques, streams, strutils, tempfiles, strformat],
  stew/[results, byteutils],
  testutils/unittests,
  chronos,
  chronicles,
  stint,
  web3,
  json,
  libp2p/crypto/crypto,
  eth/keys,
  results

import
  waku/[
    waku_rln_relay,
    waku_rln_relay/protocol_types,
    waku_rln_relay/constants,
    waku_rln_relay/contract,
    waku_rln_relay/rln,
  ],
  ../testlib/common,
  ./utils

const CHAIN_ID* = 1337

proc generateCredentials*(rlnInstance: ptr RLN): IdentityCredential =
  let credRes = membershipKeyGen(rlnInstance)
  return credRes.get()

proc getRateCommitment*(
    idCredential: IdentityCredential, userMessageLimit: UserMessageLimit
): RlnRelayResult[RawRateCommitment] =
  return RateCommitment(
    idCommitment: idCredential.idCommitment, userMessageLimit: userMessageLimit
  ).toLeaf()

proc generateCredentials*(rlnInstance: ptr RLN, n: int): seq[IdentityCredential] =
  var credentials: seq[IdentityCredential]
  for i in 0 ..< n:
    credentials.add(generateCredentials(rlnInstance))
  return credentials

#  a util function used for testing purposes
#  it deploys membership contract on Anvil (or any Eth client available on EthClient address)
#  must be edited if used for a different contract than membership contract
# <the difference between this and rln-v1 is that there is no need to deploy the poseidon hasher contract>
proc uploadRLNContract*(ethClientAddress: string): Future[Address] {.async.} =
  let web3 = await newWeb3(ethClientAddress)
  debug "web3 connected to", ethClientAddress

  # fetch the list of registered accounts
  let accounts = await web3.provider.eth_accounts()
  web3.defaultAccount = accounts[1]
  let add = web3.defaultAccount
  debug "contract deployer account address ", add

  let balance = await web3.provider.eth_getBalance(web3.defaultAccount, "latest")
  debug "Initial account balance: ", balance

  # deploy poseidon hasher bytecode
  let poseidonT3Receipt = await web3.deployContract(PoseidonT3)
  let poseidonT3Address = poseidonT3Receipt.contractAddress.get()
  let poseidonAddressStripped = strip0xPrefix($poseidonT3Address)

  # deploy lazy imt bytecode
  let lazyImtReceipt = await web3.deployContract(
    LazyIMT.replace("__$PoseidonT3$__", poseidonAddressStripped)
  )
  let lazyImtAddress = lazyImtReceipt.contractAddress.get()
  let lazyImtAddressStripped = strip0xPrefix($lazyImtAddress)

  # deploy waku rlnv2 contract
  let wakuRlnContractReceipt = await web3.deployContract(
    WakuRlnV2Contract.replace("__$PoseidonT3$__", poseidonAddressStripped).replace(
      "__$LazyIMT$__", lazyImtAddressStripped
    )
  )
  let wakuRlnContractAddress = wakuRlnContractReceipt.contractAddress.get()
  let wakuRlnAddressStripped = strip0xPrefix($wakuRlnContractAddress)

  debug "Address of the deployed rlnv2 contract: ", wakuRlnContractAddress

  # need to send concat: impl & init_bytes
  let contractInput = encode(wakuRlnContractAddress).data & Erc1967ProxyContractInput
  debug "contractInput", contractInput
  let proxyReceipt =
    await web3.deployContract(Erc1967Proxy, contractInput = contractInput)

  debug "proxy receipt", proxyReceipt
  let proxyAddress = proxyReceipt.contractAddress.get()

  let newBalance = await web3.provider.eth_getBalance(web3.defaultAccount, "latest")
  debug "Account balance after the contract deployment: ", newBalance

  await web3.close()
  debug "disconnected from ", ethClientAddress

  return proxyAddress

proc sendEthTransfer*(
    web3: Web3,
    accountFrom: Address,
    accountTo: Address,
    amountWei: UInt256,
    accountToBalanceBeforeExpectedWei: Option[UInt256] = none(UInt256),
): Future[TxHash] {.async.} =
  let doBalanceAssert = accountToBalanceBeforeExpectedWei.isSome()

  if doBalanceAssert:
    let balanceBeforeWei = await web3.provider.eth_getBalance(accountTo, "latest")
    let balanceBeforeExpectedWei = accountToBalanceBeforeExpectedWei.get()
    assert balanceBeforeWei == balanceBeforeExpectedWei,
      fmt"Balance is {balanceBeforeWei} but expected {balanceBeforeExpectedWei}"

  let gasPrice = int(await web3.provider.eth_gasPrice())

  var tx: EthSend
  tx.source = accountFrom
  tx.to = some(accountTo)
  tx.value = some(amountWei)
  tx.gasPrice = some(gasPrice)

  # TODO: handle the error if sending fails
  let txHash = await web3.send(tx)

  if doBalanceAssert:
    let balanceAfterWei = await web3.provider.eth_getBalance(accountTo, "latest")
    let balanceAfterExpectedWei = accountToBalanceBeforeExpectedWei.get() + amountWei
    assert balanceAfterWei == balanceAfterExpectedWei,
      fmt"Balance is {balanceAfterWei} but expected {balanceAfterExpectedWei}"

  return txHash

proc createEthAccount*(web3: Web3): (keys.PrivateKey, Address) =
  let pk = keys.PrivateKey.random(rng[])
  let acc = Address(toCanonicalAddress(pk.toPublicKey()))

  return (pk, acc)

proc getAnvilPath*(): string =
  var anvilPath = ""
  if existsEnv("XDG_CONFIG_HOME"):
    anvilPath = joinPath(anvilPath, os.getEnv("XDG_CONFIG_HOME", ""))
  else:
    anvilPath = joinPath(anvilPath, os.getEnv("HOME", ""))
  anvilPath = joinPath(anvilPath, ".foundry/bin/anvil")
  return $anvilPath

# Runs Anvil daemon
proc runAnvil*(port: int = 8540, chainId: string = "1337"): Process =
  # Passed options are
  # --port                            Port to listen on.
  # --gas-limit                       Sets the block gas limit in WEI.
  # --balance                         The default account balance, specified in ether.
  # --chain-id                        Chain ID of the network.
  # See anvil documentation https://book.getfoundry.sh/reference/anvil/ for more details
  try:
    let anvilPath = getAnvilPath()
    debug "Anvil path", anvilPath
    let runAnvil = startProcess(
      anvilPath,
      args = [
        "--port",
        "8540",
        "--gas-limit",
        "300000000000000",
        "--balance",
        "1000000000",
        "--chain-id",
        $CHAIN_ID,
      ],
      options = {poUsePath},
    )
    let anvilPID = runAnvil.processID

    # We read stdout from Anvil to see when daemon is ready
    var anvilStartLog: string
    var cmdline: string
    while true:
      try:
        if runAnvil.outputstream.readLine(cmdline):
          anvilStartLog.add(cmdline)
          if cmdline.contains("Listening on 127.0.0.1:" & $port):
            break
      except Exception, CatchableError:
        break
    debug "Anvil daemon is running and ready", pid = anvilPID, startLog = anvilStartLog
    return runAnvil
  except: # TODO: Fix "BareExcept" warning
    error "Anvil daemon run failed", err = getCurrentExceptionMsg()

# Stops Anvil daemon
proc stopAnvil*(runAnvil: Process) {.used.} =
  let anvilPID = runAnvil.processID
  # We wait the daemon to exit
  try:
    # We terminate Anvil daemon by sending a SIGTERM signal to the runAnvil PID to trigger RPC server termination and clean-up
    kill(runAnvil)
    debug "Sent SIGTERM to Anvil", anvilPID = anvilPID
  except:
    error "Anvil daemon termination failed: ", err = getCurrentExceptionMsg()

proc ethToWei(eth: UInt256): UInt256 =
  eth * 1000000000000000000.u256

proc setupOnchainGroupManager*(
    ethClientAddress: string = EthClient, amountEth: UInt256 = 10.u256
): Future[OnchainGroupManager] {.async.} =
  let rlnInstanceRes =
    createRlnInstance(tree_path = genTempPath("rln_tree", "group_manager_onchain"))
  check:
    rlnInstanceRes.isOk()

  let rlnInstance = rlnInstanceRes.get()

  let contractAddress = await uploadRLNContract(ethClientAddress)
  # connect to the eth client
  let web3 = await newWeb3(ethClientAddress)

  let accounts = await web3.provider.eth_accounts()
  web3.defaultAccount = accounts[0]

  let (privateKey, acc) = createEthAccount(web3)

  # we just need to fund the default account
  # the send procedure returns a tx hash that we don't use, hence discard
  discard await sendEthTransfer(
    web3, web3.defaultAccount, acc, ethToWei(1000.u256), some(0.u256)
  )

  let manager = OnchainGroupManager(
    ethClientUrl: ethClientAddress,
    ethContractAddress: $contractAddress,
    chainId: CHAIN_ID,
    ethPrivateKey: some($privateKey),
    rlnInstance: rlnInstance,
    onFatalErrorAction: proc(errStr: string) =
      raiseAssert errStr
    ,
  )

  return manager

{.pop.}
