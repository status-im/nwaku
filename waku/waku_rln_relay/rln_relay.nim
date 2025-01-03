{.push raises: [].}

import
  std/[sequtils, tables, times, deques],
  chronicles,
  options,
  chronos,
  stint,
  web3,
  json,
  web3/ethtypes,
  eth/keys,
  libp2p/protocols/pubsub/rpc/messages,
  libp2p/protocols/pubsub/pubsub,
  results,
  stew/[byteutils, arrayops]
import
  ./group_manager,
  ./rln,
  ./conversion_utils,
  ./constants,
  ./protocol_types,
  ./protocol_metrics,
  ./nonce_manager

import
  ../common/error_handling,
  ../waku_relay, # for WakuRelayHandler
  ../waku_core,
  ../waku_keystore

logScope:
  topics = "waku rln_relay"

type WakuRlnConfig* = object
  rlnRelayDynamic*: bool
  rlnRelayCredIndex*: Option[uint]
  rlnRelayEthContractAddress*: string
  rlnRelayEthClientAddress*: string
  rlnRelayChainId*: uint
  rlnRelayCredPath*: string
  rlnRelayCredPassword*: string
  rlnRelayTreePath*: string
  rlnEpochSizeSec*: uint64
  onFatalErrorAction*: OnFatalErrorHandler
  rlnRelayUserMessageLimit*: uint64

proc createMembershipList*(
    rln: ptr RLN, n: int
): RlnRelayResult[(seq[RawMembershipCredentials], string)] =
  ## createMembershipList produces a sequence of identity credentials in the form of (identity trapdoor, identity nullifier, identity secret hash, id commitment) in the hexadecimal format
  ## this proc also returns the root of a Merkle tree constructed out of the identity commitment keys of the generated list
  ## the output of this proc is used to initialize a static group keys (to test waku-rln-relay in the off-chain mode)
  ## Returns an error if it cannot create the membership list

  var output = newSeq[RawMembershipCredentials]()
  var idCommitments = newSeq[IDCommitment]()

  for i in 0 .. n - 1:
    # generate an identity credential
    let idCredentialRes = rln.membershipKeyGen()
    if idCredentialRes.isErr():
      return
        err("could not generate an identity credential: " & idCredentialRes.error())
    let idCredential = idCredentialRes.get()
    let idTuple = (
      idCredential.idTrapdoor.inHex(),
      idCredential.idNullifier.inHex(),
      idCredential.idSecretHash.inHex(),
      idCredential.idCommitment.inHex(),
    )
    output.add(idTuple)
    idCommitments.add(idCredential.idCommitment)

  # Insert members into tree
  let membersAdded = rln.insertMembers(0, idCommitments)
  if not membersAdded:
    return err("could not insert members into the tree")

  let root = rln.getMerkleRoot().value().inHex()
  return ok((output, root))

type WakuRLNRelay* = ref object of RootObj
  # the log of nullifiers and Shamir shares of the past messages grouped per epoch
  nullifierLog*: OrderedTable[Epoch, Table[Nullifier, ProofMetadata]]
  lastEpoch*: Epoch # the epoch of the last published rln message
  rlnEpochSizeSec*: uint64
  rlnMaxEpochGap*: uint64
  groupManager*: GroupManager
  onFatalErrorAction*: OnFatalErrorHandler
  nonceManager*: NonceManager
  epochMonitorFuture*: Future[void]

proc calcEpoch*(rlnPeer: WakuRLNRelay, t: float64): Epoch =
  ## gets time `t` as `flaot64` with subseconds resolution in the fractional part
  ## and returns its corresponding rln `Epoch` value
  let e = uint64(t / rlnPeer.rlnEpochSizeSec.float64)
  return toEpoch(e)

proc nextEpoch*(rlnPeer: WakuRLNRelay, t: float64): float64 =
  # Calculates the next epoch time from the given time `t`.
  float64(uint64(t / rlnPeer.rlnEpochSizeSec.float64) + 1) * rlnPeer.rlnEpochSizeSec.float64

proc stop*(rlnPeer: WakuRLNRelay) {.async: (raises: [Exception]).} =
  ## stops the rln-relay protocol
  ## Throws an error if it cannot stop the rln-relay protocol

  # stop the group sync, and flush data to tree db
  info "stopping rln-relay"
  await rlnPeer.epochMonitorFuture.cancelAndWait()
  await rlnPeer.groupManager.stop()

proc hasDuplicate*(
    rlnPeer: WakuRLNRelay, epoch: Epoch, proofMetadata: ProofMetadata
): RlnRelayResult[bool] =
  ## returns true if there is another message in the  `nullifierLog` of the `rlnPeer` with the same
  ## epoch and nullifier as `proofMetadata`'s epoch and nullifier
  ## otherwise, returns false
  ## Returns an error if it cannot check for duplicates

  # check if the epoch exists
  let nullifier = proofMetadata.nullifier
  if not rlnPeer.nullifierLog.hasKey(epoch):
    return ok(false)
  try:
    if rlnPeer.nullifierLog[epoch].hasKey(nullifier):
      # there is an identical record, mark it as spam
      return ok(true)

    # there is no duplicate
    return ok(false)
  except KeyError:
    return err("the epoch was not found: " & getCurrentExceptionMsg())

proc updateLog*(
    rlnPeer: WakuRLNRelay, epoch: Epoch, proofMetadata: ProofMetadata
): RlnRelayResult[void] =
  ## saves supplied proofMetadata `proofMetadata`
  ## in the `nullifierLog` of the `rlnPeer`
  ## Returns an error if it cannot update the log

  # check if the epoch exists
  if not rlnPeer.nullifierLog.hasKeyOrPut(
    epoch, {proofMetadata.nullifier: proofMetadata}.toTable()
  ):
    return ok()

  try:
    # check if an identical record exists
    if rlnPeer.nullifierLog[epoch].hasKeyOrPut(proofMetadata.nullifier, proofMetadata):
      # the above condition could be `discarded` but it is kept for clarity, that slashing will
      # be implemented here
      # TODO: slashing logic
      return ok()
    return ok()
  except KeyError:
    return
      err("the epoch was not found: " & getCurrentExceptionMsg()) # should never happen

proc getCurrentEpoch*(rlnPeer: WakuRLNRelay): Epoch =
  ## gets the current rln Epoch time
  return rlnPeer.calcEpoch(epochTime())

proc absDiff*(e1, e2: Epoch): uint64 =
  ## returns the absolute difference between the two rln `Epoch`s `e1` and `e2`
  ## i.e., e1 - e2

  # convert epochs to their corresponding unsigned numerical values
  let
    epoch1 = fromEpoch(e1)
    epoch2 = fromEpoch(e2)

  # Manually perform an `abs` calculation
  if epoch1 > epoch2:
    return epoch1 - epoch2
  else:
    return epoch2 - epoch1

proc validateMessage*(
    rlnPeer: WakuRLNRelay, msg: WakuMessage, timeOption = none(float64)
): MessageValidationResult =
  ## validate the supplied `msg` based on the waku-rln-relay routing protocol i.e.,
  ## the `msg`'s epoch is within MaxEpochGap of the current epoch
  ## the `msg` has valid rate limit proof
  ## the `msg` does not violate the rate limit
  ## `timeOption` indicates Unix epoch time (fractional part holds sub-seconds)
  ## if `timeOption` is supplied, then the current epoch is calculated based on that

  let decodeRes = RateLimitProof.init(msg.proof)
  if decodeRes.isErr():
    return MessageValidationResult.Invalid

  let proof = decodeRes.get()

  # track message count for metrics
  waku_rln_messages_total.inc()

  # checks if the `msg`'s epoch is far from the current epoch
  # it corresponds to the validation of rln external nullifier
  var epoch: Epoch
  if timeOption.isSome():
    epoch = rlnPeer.calcEpoch(timeOption.get())
  else:
    # get current rln epoch
    epoch = rlnPeer.getCurrentEpoch()

  let
    msgEpoch = proof.epoch
    # calculate the gaps
    gap = absDiff(epoch, msgEpoch)

  trace "epoch info", currentEpoch = fromEpoch(epoch), msgEpoch = fromEpoch(msgEpoch)

  # validate the epoch
  if gap > rlnPeer.rlnMaxEpochGap:
    # message's epoch is too old or too ahead
    # accept messages whose epoch is within +-MaxEpochGap from the current epoch
    warn "invalid message: epoch gap exceeds a threshold",
      gap = gap, payloadLen = msg.payload.len, msgEpoch = fromEpoch(proof.epoch)
    waku_rln_invalid_messages_total.inc(labelValues = ["invalid_epoch"])
    return MessageValidationResult.Invalid

  let rootValidationRes = rlnPeer.groupManager.validateRoot(proof.merkleRoot)
  if not rootValidationRes:
    warn "invalid message: provided root does not belong to acceptable window of roots",
      provided = proof.merkleRoot.inHex(),
      validRoots = rlnPeer.groupManager.validRoots.mapIt(it.inHex())
    waku_rln_invalid_messages_total.inc(labelValues = ["invalid_root"])
    return MessageValidationResult.Invalid

  # verify the proof
  let
    contentTopicBytes = msg.contentTopic.toBytes
    input = concat(msg.payload, contentTopicBytes)

  waku_rln_proof_verification_total.inc()
  waku_rln_proof_verification_duration_seconds.nanosecondTime:
    let proofVerificationRes = rlnPeer.groupManager.verifyProof(input, proof)

  if proofVerificationRes.isErr():
    waku_rln_errors_total.inc(labelValues = ["proof_verification"])
    warn "invalid message: proof verification failed", payloadLen = msg.payload.len
    return MessageValidationResult.Invalid
  if not proofVerificationRes.value():
    # invalid proof
    warn "invalid message: invalid proof", payloadLen = msg.payload.len
    waku_rln_invalid_messages_total.inc(labelValues = ["invalid_proof"])
    return MessageValidationResult.Invalid

  # check if double messaging has happened
  let proofMetadataRes = proof.extractMetadata()
  if proofMetadataRes.isErr():
    waku_rln_errors_total.inc(labelValues = ["proof_metadata_extraction"])
    return MessageValidationResult.Invalid
  let hasDup = rlnPeer.hasDuplicate(msgEpoch, proofMetadataRes.get())
  if hasDup.isErr():
    waku_rln_errors_total.inc(labelValues = ["duplicate_check"])
  elif hasDup.value == true:
    trace "invalid message: message is spam", payloadLen = msg.payload.len
    waku_rln_spam_messages_total.inc()
    return MessageValidationResult.Spam

  trace "message is valid", payloadLen = msg.payload.len
  let rootIndex = rlnPeer.groupManager.indexOfRoot(proof.merkleRoot)
  waku_rln_valid_messages_total.observe(rootIndex.toFloat())
  return MessageValidationResult.Valid

proc validateMessageAndUpdateLog*(
    rlnPeer: WakuRLNRelay, msg: WakuMessage, timeOption = none(float64)
): MessageValidationResult =
  ## validates the message and updates the log to prevent double messaging
  ## in future messages

  let isValidMessage = rlnPeer.validateMessage(msg, timeOption)

  let decodeRes = RateLimitProof.init(msg.proof)
  if decodeRes.isErr():
    return MessageValidationResult.Invalid

  let msgProof = decodeRes.get()
  let proofMetadataRes = msgProof.extractMetadata()

  if proofMetadataRes.isErr():
    return MessageValidationResult.Invalid

  # insert the message to the log (never errors) only if the
  # message is valid.
  if isValidMessage == MessageValidationResult.Valid:
    discard rlnPeer.updateLog(msgProof.epoch, proofMetadataRes.get())

  return isValidMessage

proc toRLNSignal*(wakumessage: WakuMessage): seq[byte] =
  ## it is a utility proc that prepares the `data` parameter of the proof generation procedure i.e., `proofGen`  that resides in the current module
  ## it extracts the `contentTopic` and the `payload` of the supplied `wakumessage` and serializes them into a byte sequence
  let
    contentTopicBytes = wakumessage.contentTopic.toBytes()
    output = concat(wakumessage.payload, contentTopicBytes)
  return output

proc appendRLNProof*(
    rlnPeer: WakuRLNRelay, msg: var WakuMessage, senderEpochTime: float64
): RlnRelayResult[void] =
  ## returns true if it can create and append a `RateLimitProof` to the supplied `msg`
  ## returns false otherwise
  ## `senderEpochTime` indicates the number of seconds passed since Unix epoch. The fractional part holds sub-seconds.
  ## The `epoch` field of `RateLimitProof` is derived from the provided `senderEpochTime` (using `calcEpoch()`)

  let input = msg.toRLNSignal()
  let epoch = rlnPeer.calcEpoch(senderEpochTime)

  let nonce = rlnPeer.nonceManager.getNonce().valueOr:
    return err("could not get new message id to generate an rln proof: " & $error)
  let proof = rlnPeer.groupManager.generateProof(input, epoch, nonce).valueOr:
    return err("could not generate rln-v2 proof: " & $error)

  msg.proof = proof.encode().buffer
  return ok()

proc clearNullifierLog*(rlnPeer: WakuRlnRelay) =
  # clear the first MaxEpochGap epochs of the nullifer log
  # if more than MaxEpochGap epochs are in the log
  let currentEpoch = fromEpoch(rlnPeer.getCurrentEpoch())

  var epochsToRemove: seq[Epoch] = @[]
  for epoch in rlnPeer.nullifierLog.keys():
    let epochInt = fromEpoch(epoch)

    # clean all epochs that are +- rlnMaxEpochGap from the current epoch
    if (currentEpoch + rlnPeer.rlnMaxEpochGap) <= epochInt or
        epochInt <= (currentEpoch - rlnPeer.rlnMaxEpochGap):
      epochsToRemove.add(epoch)

  for epochRemove in epochsToRemove:
    trace "clearing epochs from the nullifier log",
      currentEpoch = currentEpoch, cleanedEpoch = fromEpoch(epochRemove)
    rlnPeer.nullifierLog.del(epochRemove)

proc generateRlnValidator*(
    wakuRlnRelay: WakuRLNRelay, spamHandler = none(SpamHandler)
): WakuValidatorHandler =
  ## this procedure is a thin wrapper for the pubsub addValidator method
  ## it sets a validator for waku messages, acting in the registered pubsub topic
  ## the message validation logic is according to https://rfc.vac.dev/spec/17/
  proc validator(
      topic: string, message: WakuMessage
  ): Future[pubsub.ValidationResult] {.async.} =
    trace "rln-relay topic validator is called"
    wakuRlnRelay.clearNullifierLog()

    let decodeRes = RateLimitProof.init(message.proof)

    if decodeRes.isErr():
      trace "generateRlnValidator reject", error = decodeRes.error
      return pubsub.ValidationResult.Reject

    let msgProof = decodeRes.get()

    # validate the message and update log
    let validationRes = wakuRlnRelay.validateMessageAndUpdateLog(message)

    let
      proof = toHex(msgProof.proof)
      epoch = fromEpoch(msgProof.epoch)
      root = inHex(msgProof.merkleRoot)
      shareX = inHex(msgProof.shareX)
      shareY = inHex(msgProof.shareY)
      nullifier = inHex(msgProof.nullifier)
      payload = string.fromBytes(message.payload)
    case validationRes
    of Valid:
      trace "message validity is verified, relaying:",
        proof = proof,
        root = root,
        shareX = shareX,
        shareY = shareY,
        nullifier = nullifier
      return pubsub.ValidationResult.Accept
    of Invalid:
      trace "message validity could not be verified, discarding:",
        proof = proof,
        root = root,
        shareX = shareX,
        shareY = shareY,
        nullifier = nullifier
      return pubsub.ValidationResult.Reject
    of Spam:
      trace "A spam message is found! yay! discarding:",
        proof = proof,
        root = root,
        shareX = shareX,
        shareY = shareY,
        nullifier = nullifier
      if spamHandler.isSome():
        let handler = spamHandler.get()
        handler(message)
      return pubsub.ValidationResult.Reject

  return validator

proc monitorEpochs(wakuRlnRelay: WakuRLNRelay): Future[void] {.async.} =
  while true:
    try:
      waku_rln_remaining_proofs_per_epoch.set(
        wakuRlnRelay.groupManager.userMessageLimit.get().float64
      )
    except CatchableError:
      error "Error in epoch monitoring", error = getCurrentExceptionMsg()

    let nextEpochTime = wakuRlnRelay.nextEpoch(epochTime())
    await sleepAsync(int(wakuRlnRelay.rlnEpochSizeSec * 1000))

proc mount(
    conf: WakuRlnConfig, registrationHandler = none(RegistrationHandler)
): Future[RlnRelayResult[WakuRlnRelay]] {.async.} =
  var
    groupManager: GroupManager
    wakuRlnRelay: WakuRLNRelay
  # create an RLN instance
  let rlnInstance = createRLNInstance(tree_path = conf.rlnRelayTreePath).valueOr:
    return err("could not create RLN instance: " & $error)

  if not conf.rlnRelayDynamic:
    # static setup
    let parsedGroupKeys = StaticGroupKeys.toIdentityCredentials().valueOr:
      return err("could not parse static group keys: " & $error)

    groupManager = StaticGroupManager(
      groupSize: StaticGroupSize,
      groupKeys: parsedGroupKeys,
      membershipIndex: conf.rlnRelayCredIndex,
      rlnInstance: rlnInstance,
      onFatalErrorAction: conf.onFatalErrorAction,
    )
    # we don't persist credentials in static mode since they exist in ./constants.nim
  else:
    # dynamic setup
    proc useValueOrNone(s: string): Option[string] =
      if s == "":
        none(string)
      else:
        some(s)

    let
      rlnRelayCredPath = useValueOrNone(conf.rlnRelayCredPath)
      rlnRelayCredPassword = useValueOrNone(conf.rlnRelayCredPassword)
    groupManager = OnchainGroupManager(
      ethClientUrl: string(conf.rlnRelayethClientAddress),
      ethContractAddress: $conf.rlnRelayEthContractAddress,
      chainId: conf.rlnRelayChainId,
      rlnInstance: rlnInstance,
      registrationHandler: registrationHandler,
      keystorePath: rlnRelayCredPath,
      keystorePassword: rlnRelayCredPassword,
      membershipIndex: conf.rlnRelayCredIndex,
      onFatalErrorAction: conf.onFatalErrorAction,
    )

  # Initialize the groupManager
  (await groupManager.init()).isOkOr:
    return err("could not initialize the group manager: " & $error)
  # Start the group sync
  (await groupManager.startGroupSync()).isOkOr:
    return err("could not start the group sync: " & $error)

  wakuRlnRelay = WakuRLNRelay(
    groupManager: groupManager,
    nonceManager:
      NonceManager.init(conf.rlnRelayUserMessageLimit, conf.rlnEpochSizeSec.float),
    rlnEpochSizeSec: conf.rlnEpochSizeSec,
    rlnMaxEpochGap: max(uint64(MaxClockGapSeconds / float64(conf.rlnEpochSizeSec)), 1),
    onFatalErrorAction: conf.onFatalErrorAction,
  )

  # Start epoch monitoring in the background
  wakuRlnRelay.epochMonitorFuture = monitorEpochs(wakuRlnRelay)
  return ok(wakuRlnRelay)

proc isReady*(rlnPeer: WakuRLNRelay): Future[bool] {.async: (raises: [Exception]).} =
  ## returns true if the rln-relay protocol is ready to relay messages
  ## returns false otherwise

  # could be nil during startup
  if rlnPeer.groupManager == nil:
    return false
  try:
    return await rlnPeer.groupManager.isReady()
  except CatchableError:
    error "could not check if the rln-relay protocol is ready",
      err = getCurrentExceptionMsg()
    return false

proc new*(
    T: type WakuRlnRelay,
    conf: WakuRlnConfig,
    registrationHandler = none(RegistrationHandler),
): Future[RlnRelayResult[WakuRlnRelay]] {.async.} =
  ## Mounts the rln-relay protocol on the node.
  ## The rln-relay protocol can be mounted in two modes: on-chain and off-chain.
  ## Returns an error if the rln-relay protocol could not be mounted.
  try:
    return await mount(conf, registrationHandler)
  except CatchableError:
    return err("could not mount the rln-relay protocol: " & getCurrentExceptionMsg())
