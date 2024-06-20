when (NimMajor, NimMinor) < (1, 4):
  {.push raises: [Defect].}
else:
  {.push raises: [].}

import chronicles, stew/[results], std/tempfiles

import
  ../../waku/waku_keystore,
  ../../waku/waku_rln_relay/rln,
  ../../waku/waku_rln_relay/conversion_utils,
  ../../waku/waku_rln_relay/group_manager/on_chain,
  ../../waku/factory/external_config

logScope:
  topics = "rln_keystore_generator"

proc doRlnKeystoreGenerator*(conf: WakuNodeConf, quitOnSucces: bool = true) =
  # 1. load configuration
  trace "configuration", conf = $conf

  # 2. initialize rlnInstance
  let rlnInstanceRes = createRLNInstance(
    d = 20, tree_path = genTempPath("rln_tree", "rln_keystore_generator")
  )
  if rlnInstanceRes.isErr():
    error "failure while creating RLN instance", error = rlnInstanceRes.error
    quit(1)

  let rlnInstance = rlnInstanceRes.get()

  # 3. generate credentials
  let credentialRes = rlnInstance.membershipKeyGen()
  if credentialRes.isErr():
    error "failure while generating credentials", error = credentialRes.error
    quit(1)

  let credential = credentialRes.get()
  debug "credentials",
    idTrapdoor = credential.idTrapdoor.inHex(),
    idNullifier = credential.idNullifier.inHex(),
    idSecretHash = credential.idSecretHash.inHex(),
    idCommitment = credential.idCommitment.inHex()

  if quitOnSucces and not conf.execute:
    info "not executing, exiting"
    quit(0)

  # 4. initialize OnchainGroupManager
  let groupManager = OnchainGroupManager(
    ethClientUrl: string(conf.rlnRelayethClientAddress),
    ethContractAddress: conf.rlnRelayEthContractAddress,
    rlnInstance: rlnInstance,
    keystorePath: none(string),
    keystorePassword: none(string),
    ethPrivateKey: some(conf.rlnRelayEthPrivateKey),
  )
  try:
    (waitFor groupManager.init()).isOkOr:
      error "failure while initializing OnchainGroupManager", error = $error
      quit(1)
  # handling the exception is required since waitFor raises an exception
  except Exception, CatchableError:
    error "failure while initializing OnchainGroupManager",
      error = getCurrentExceptionMsg()
    quit(1)

  # 5. register on-chain
  try:
    waitFor groupManager.register(credential, conf.rlnRelayUserMessageLimit)
  except Exception, CatchableError:
    error "failure while registering credentials on-chain",
      error = getCurrentExceptionMsg()
    quit(1)

  debug "Transaction hash", txHash = groupManager.registrationTxHash.get()

  info "Your membership has been registered on-chain.",
    chainId = $groupManager.chainId.get(),
    contractAddress = conf.rlnRelayEthContractAddress,
    membershipIndex = groupManager.membershipIndex.get()
  info "Your user message limit is", userMessageLimit = conf.rlnRelayUserMessageLimit

  # 6. write to keystore
  let keystoreCred = KeystoreMembership(
    membershipContract: MembershipContract(
      chainId: $groupManager.chainId.get(), address: conf.rlnRelayEthContractAddress
    ),
    treeIndex: groupManager.membershipIndex.get(),
    identityCredential: credential,
    userMessageLimit: conf.rlnRelayUserMessageLimit,
  )

  let persistRes = addMembershipCredentials(
    conf.rlnRelayCredPath, keystoreCred, conf.rlnRelayCredPassword, RLNAppInfo
  )
  if persistRes.isErr():
    error "failed to persist credentials", error = persistRes.error
    quit(1)

  info "credentials persisted", path = conf.rlnRelayCredPath

  try:
    waitFor groupManager.stop()
  except CatchableError:
    error "failure while stopping OnchainGroupManager", error = getCurrentExceptionMsg()
    quit(0) # 0 because we already registered on-chain

  if quitOnSucces:
    quit(0)
