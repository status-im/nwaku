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
  libp2p/crypto/crypto,
  eth/keys,
  tests/testlib/testasync

import
  waku/[
    waku_node,
    node/waku_node,
    waku_rln_relay,
    waku_rln_relay/protocol_types,
    waku_rln_relay/constants,
    waku_rln_relay/contract,
    waku_rln_relay/rln,
    waku_rln_relay/conversion_utils,
    waku_rln_relay/group_manager/on_chain/group_manager,
  ],
  ../testlib/[wakucore, wakunode, common],
  ./utils_onchain,
  ./utils

suite "Onchain group manager":
  # We run Anvil
  let runAnvil {.used.} = runAnvil()

  var manager {.threadvar.}: OnchainGroupManager

  asyncSetup:
    manager = await setupOnchainGroupManager()

  asyncTeardown:
    await manager.stop()

  asyncTest "should initialize successfully":
    (await manager.init()).isOkOr:
      raiseAssert $error

    check:
      manager.ethRpc.isSome()
      manager.wakuRlnContract.isSome()
      manager.initialized
      manager.rlnContractDeployedBlockNumber > 0
      manager.rlnRelayMaxMessageLimit == 100

  asyncTest "should error on initialization when chainId does not match":
    manager.chainId = CHAIN_ID + 1

    (await manager.init()).isErrOr:
      raiseAssert "Expected error when chainId does not match"

  asyncTest "should initialize when chainId is set to 0":
    manager.chainId = 0

    (await manager.init()).isOkOr:
      raiseAssert $error

  asyncTest "should error on initialization when loaded metadata does not match":
    (await manager.init()).isOkOr:
      raiseAssert $error

    let metadataSetRes = manager.setMetadata()
    assert metadataSetRes.isOk(), metadataSetRes.error
    let metadataOpt = manager.rlnInstance.getMetadata().valueOr:
      raiseAssert $error
    assert metadataOpt.isSome(), "metadata is not set"
    let metadata = metadataOpt.get()

    assert metadata.chainId == 1337, "chainId is not equal to 1337"
    assert metadata.contractAddress == manager.ethContractAddress,
      "contractAddress is not equal to " & manager.ethContractAddress

    let differentContractAddress = await uploadRLNContract(manager.ethClientUrl)
    # simulating a change in the contractAddress
    let manager2 = OnchainGroupManager(
      ethClientUrl: EthClient,
      ethContractAddress: $differentContractAddress,
      rlnInstance: manager.rlnInstance,
      onFatalErrorAction: proc(errStr: string) =
        raiseAssert errStr
      ,
    )
    let e = await manager2.init()
    (e).isErrOr:
      raiseAssert "Expected error when contract address doesn't match"

    echo "---"
    discard "persisted data: contract address mismatch"
    echo e.error
    echo "---"

  asyncTest "should error if contract does not exist":
    var triggeredError = false

    manager.ethContractAddress = "0x0000000000000000000000000000000000000000"
    manager.onFatalErrorAction = proc(msg: string) {.gcsafe, closure.} =
      echo "---"
      discard
        "Failed to get the deployed block number. Have you set the correct contract address?: No response from the Web3 provider"
      echo msg
      echo "---"
      triggeredError = true

    discard await manager.init()

    check triggeredError

  asyncTest "should error when keystore path and password are provided but file doesn't exist":
    manager.keystorePath = some("/inexistent/file")
    manager.keystorePassword = some("password")

    (await manager.init()).isErrOr:
      raiseAssert "Expected error when keystore file doesn't exist"

  asyncTest "startGroupSync: should start group sync":
    (await manager.init()).isOkOr:
      raiseAssert $error
    (await manager.startGroupSync()).isOkOr:
      raiseAssert $error


  asyncTest "startGroupSync: should guard against uninitialized state":
    (await manager.startGroupSync()).isErrOr:
      raiseAssert "Expected error when not initialized"

  asyncTest "startGroupSync: should sync to the state of the group":
    let credentials = generateCredentials(manager.rlnInstance)
    let rateCommitment = getRateCommitment(credentials, UserMessageLimit(1)).valueOr:
      raiseAssert $error
    (await manager.init()).isOkOr:
      raiseAssert $error

    let merkleRootBefore = manager.rlnInstance.getMerkleRoot().valueOr:
      raiseAssert $error

    let fut = newFuture[void]("startGroupSync")

    proc generateCallback(fut: Future[void]): OnRegisterCallback =
      proc callback(registrations: seq[Membership]): Future[void] {.async.} =
        check:
          registrations.len == 1
          registrations[0].index == 0
          registrations[0].rateCommitment == rateCommitment
        fut.complete()

      return callback

    try:
      manager.onRegister(generateCallback(fut))
      await manager.register(credentials, UserMessageLimit(1))
      (await manager.startGroupSync()).isOkOr:
        raiseAssert $error
    except Exception, CatchableError:
      assert false, "exception raised: " & getCurrentExceptionMsg()

    await fut

    let merkleRootAfter = manager.rlnInstance.getMerkleRoot().valueOr:
      raiseAssert $error

    let metadataOpt = manager.rlnInstance.getMetadata().valueOr:
      raiseAssert $error
    check:
      metadataOpt.get().validRoots == manager.validRoots.toSeq()
      merkleRootBefore != merkleRootAfter

  asyncTest "startGroupSync: should fetch history correctly":
    const credentialCount = 6
    let credentials = generateCredentials(manager.rlnInstance, credentialCount)
    (await manager.init()).isOkOr:
      raiseAssert $error

    let merkleRootBefore = manager.rlnInstance.getMerkleRoot().valueOr:
      raiseAssert $error

    type TestGroupSyncFuts = array[0 .. credentialCount - 1, Future[void]]
    var futures: TestGroupSyncFuts
    for i in 0 ..< futures.len():
      futures[i] = newFuture[void]()
    proc generateCallback(
        futs: TestGroupSyncFuts, credentials: seq[IdentityCredential]
    ): OnRegisterCallback =
      var futureIndex = 0
      proc callback(registrations: seq[Membership]): Future[void] {.async.} =
        let rateCommitment =
          getRateCommitment(credentials[futureIndex], UserMessageLimit(1))
        if registrations.len == 1 and
            registrations[0].rateCommitment == rateCommitment.get() and
            registrations[0].index == MembershipIndex(futureIndex):
          futs[futureIndex].complete()
          futureIndex += 1

      return callback

    try:
      manager.onRegister(generateCallback(futures, credentials))
      (await manager.startGroupSync()).isOkOr:
        raiseAssert $error

      for i in 0 ..< credentials.len():
        await manager.register(credentials[i], UserMessageLimit(1))
    except Exception, CatchableError:
      assert false, "exception raised: " & getCurrentExceptionMsg()

    await allFutures(futures)

    let merkleRootAfter = manager.rlnInstance.getMerkleRoot().valueOr:
      raiseAssert $error

    check:
      merkleRootBefore != merkleRootAfter
      manager.validRootBuffer.len() == credentialCount - AcceptableRootWindowSize

  asyncTest "register: should guard against uninitialized state":
    let dummyCommitment = default(IDCommitment)

    try:
      await manager.register(
        RateCommitment(
          idCommitment: dummyCommitment, userMessageLimit: UserMessageLimit(1)
        )
      )
    except CatchableError:
      assert true
    except Exception:
      assert false, "exception raised: " & getCurrentExceptionMsg()

  asyncTest "register: should register successfully":
    (await manager.init()).isOkOr:
      raiseAssert $error
    (await manager.startGroupSync()).isOkOr:
      raiseAssert $error

    let idCommitment = generateCredentials(manager.rlnInstance).idCommitment
    let merkleRootBefore = manager.rlnInstance.getMerkleRoot().valueOr:
      raiseAssert $error

    try:
      await manager.register(
        RateCommitment(
          idCommitment: idCommitment, userMessageLimit: UserMessageLimit(1)
        )
      )
    except Exception, CatchableError:
      assert false,
        "exception raised when calling register: " & getCurrentExceptionMsg()

    let merkleRootAfter = manager.rlnInstance.getMerkleRoot().valueOr:
      raiseAssert $error
    check:
      merkleRootAfter.inHex() != merkleRootBefore.inHex()
      manager.latestIndex == 1

  asyncTest "register: callback is called":
    let idCredentials = generateCredentials(manager.rlnInstance)
    let idCommitment = idCredentials.idCommitment

    let fut = newFuture[void]()

    proc callback(registrations: seq[Membership]): Future[void] {.async.} =
      let rateCommitment = getRateCommitment(idCredentials, UserMessageLimit(1))
      check:
        registrations.len == 1
        registrations[0].rateCommitment == rateCommitment.get()
        registrations[0].index == 0
      fut.complete()

    manager.onRegister(callback)
    (await manager.init()).isOkOr:
      raiseAssert $error
    try:
      (await manager.startGroupSync()).isOkOr:
        raiseAssert $error
      await manager.register(
        RateCommitment(
          idCommitment: idCommitment, userMessageLimit: UserMessageLimit(1)
        )
      )
    except Exception, CatchableError:
      assert false, "exception raised: " & getCurrentExceptionMsg()

    await fut

  asyncTest "withdraw: should guard against uninitialized state":
    let idSecretHash = generateCredentials(manager.rlnInstance).idSecretHash

    try:
      await manager.withdraw(idSecretHash)
    except CatchableError:
      assert true
    except Exception:
      assert false, "exception raised: " & getCurrentExceptionMsg()

  asyncTest "validateRoot: should validate good root":
    let credentials = generateCredentials(manager.rlnInstance)
    (await manager.init()).isOkOr:
      raiseAssert $error

    let fut = newFuture[void]()

    proc callback(registrations: seq[Membership]): Future[void] {.async.} =
      if registrations.len == 1 and
          registrations[0].rateCommitment ==
          getRateCommitment(credentials, UserMessageLimit(1)).get() and
          registrations[0].index == 0:
        manager.idCredentials = some(credentials)
        fut.complete()

    manager.onRegister(callback)

    try:
      (await manager.startGroupSync()).isOkOr:
        raiseAssert $error
      await manager.register(credentials, UserMessageLimit(1))
    except Exception, CatchableError:
      assert false, "exception raised: " & getCurrentExceptionMsg()

    await fut

    let messageBytes = "Hello".toBytes()

    # prepare the epoch
    let epoch = default(Epoch)
    debug "epoch in bytes", epochHex = epoch.inHex()

    # generate proof
    let validProofRes = manager.generateProof(
      data = messageBytes, epoch = epoch, messageId = MessageId(1)
    )

    check:
      validProofRes.isOk()
    let validProof = validProofRes.get()

    # validate the root (should be true)
    let validated = manager.validateRoot(validProof.merkleRoot)

    check:
      validated

  asyncTest "validateRoot: should reject bad root":
    (await manager.init()).isOkOr:
      raiseAssert $error
    (await manager.startGroupSync()).isOkOr:
      raiseAssert $error

    let credentials = generateCredentials(manager.rlnInstance)

    ## Assume the registration occured out of band
    manager.idCredentials = some(credentials)
    manager.membershipIndex = some(MembershipIndex(0))
    manager.userMessageLimit = some(UserMessageLimit(1))

    let messageBytes = "Hello".toBytes()

    # prepare the epoch
    let epoch = default(Epoch)
    debug "epoch in bytes", epochHex = epoch.inHex()

    # generate proof
    let validProof = manager.generateProof(
      data = messageBytes, epoch = epoch, messageId = MessageId(0)
    ).valueOr:
      raiseAssert $error

    # validate the root (should be false)
    let validated = manager.validateRoot(validProof.merkleRoot)

    check:
      validated == false

  asyncTest "verifyProof: should verify valid proof":
    let credentials = generateCredentials(manager.rlnInstance)
    (await manager.init()).isOkOr:
      raiseAssert $error

    let fut = newFuture[void]()

    proc callback(registrations: seq[Membership]): Future[void] {.async.} =
      if registrations.len == 1 and
          registrations[0].rateCommitment ==
          getRateCommitment(credentials, UserMessageLimit(1)).get() and
          registrations[0].index == 0:
        manager.idCredentials = some(credentials)
        fut.complete()

    manager.onRegister(callback)

    try:
      (await manager.startGroupSync()).isOkOr:
        raiseAssert $error
      await manager.register(credentials, UserMessageLimit(1))
    except Exception, CatchableError:
      assert false, "exception raised: " & getCurrentExceptionMsg()
    await fut

    let messageBytes = "Hello".toBytes()

    # prepare the epoch
    let epoch = default(Epoch)
    debug "epoch in bytes", epochHex = epoch.inHex()

    # generate proof
    let validProof = manager.generateProof(
      data = messageBytes, epoch = epoch, messageId = MessageId(0)
    ).valueOr:
      raiseAssert $error

    # verify the proof (should be true)
    let verified = manager.verifyProof(messageBytes, validProof).valueOr:
      raiseAssert $error

    check:
      verified

  asyncTest "verifyProof: should reject invalid proof":
    (await manager.init()).isOkOr:
      raiseAssert $error
    (await manager.startGroupSync()).isOkOr:
      raiseAssert $error

    let idCredential = generateCredentials(manager.rlnInstance)

    try:
      await manager.register(
        RateCommitment(
          idCommitment: idCredential.idCommitment, userMessageLimit: UserMessageLimit(1)
        )
      )
    except Exception, CatchableError:
      assert false,
        "exception raised when calling startGroupSync: " & getCurrentExceptionMsg()

    let idCredential2 = generateCredentials(manager.rlnInstance)

    ## Assume the registration occured out of band
    manager.idCredentials = some(idCredential2)
    manager.membershipIndex = some(MembershipIndex(0))
    manager.userMessageLimit = some(UserMessageLimit(1))

    let messageBytes = "Hello".toBytes()

    # prepare the epoch
    let epoch = default(Epoch)
    debug "epoch in bytes", epochHex = epoch.inHex()

    # generate proof
    let invalidProofRes = manager.generateProof(
      data = messageBytes, epoch = epoch, messageId = MessageId(0)
    )

    check:
      invalidProofRes.isOk()
    let invalidProof = invalidProofRes.get()

    # verify the proof (should be false)
    let verified = manager.verifyProof(messageBytes, invalidProof).valueOr:
      raiseAssert $error

    check:
      verified == false

  asyncTest "backfillRootQueue: should backfill roots in event of chain reorg":
    const credentialCount = 6
    let credentials = generateCredentials(manager.rlnInstance, credentialCount)
    (await manager.init()).isOkOr:
      raiseAssert $error

    type TestBackfillFuts = array[0 .. credentialCount - 1, Future[void]]
    var futures: TestBackfillFuts
    for i in 0 ..< futures.len():
      futures[i] = newFuture[void]()

    proc generateCallback(
        futs: TestBackfillFuts, credentials: seq[IdentityCredential]
    ): OnRegisterCallback =
      var futureIndex = 0
      proc callback(registrations: seq[Membership]): Future[void] {.async.} =
        if registrations.len == 1 and
            registrations[0].rateCommitment ==
            getRateCommitment(credentials[futureIndex], UserMessageLimit(1)).get() and
            registrations[0].index == MembershipIndex(futureIndex):
          futs[futureIndex].complete()
          futureIndex += 1

      return callback

    try:
      manager.onRegister(generateCallback(futures, credentials))
      (await manager.startGroupSync()).isOkOr:
        raiseAssert $error

      for i in 0 ..< credentials.len():
        await manager.register(credentials[i], UserMessageLimit(1))
    except Exception, CatchableError:
      assert false, "exception raised: " & getCurrentExceptionMsg()

    await allFutures(futures)

    # At this point, we should have a full root queue, 5 roots, and partial buffer of 1 root
    check:
      manager.validRoots.len() == credentialCount - 1
      manager.validRootBuffer.len() == 1

    # We can now simulate a chain reorg by calling backfillRootQueue
    let expectedLastRoot = manager.validRootBuffer[0]
    try:
      await manager.backfillRootQueue(1)
    except Exception, CatchableError:
      assert false, "exception raised: " & getCurrentExceptionMsg()

    # We should now have 5 roots in the queue, and no partial buffer
    check:
      manager.validRoots.len() == credentialCount - 1
      manager.validRootBuffer.len() == 0
      manager.validRoots[credentialCount - 2] == expectedLastRoot

  asyncTest "isReady should return false if ethRpc is none":
    (await manager.init()).isOkOr:
      raiseAssert $error

    manager.ethRpc = none(Web3)

    var isReady = true
    try:
      isReady = await manager.isReady()
    except Exception, CatchableError:
      assert false, "exception raised: " & getCurrentExceptionMsg()

    check:
      isReady == false

  asyncTest "isReady should return false if lastSeenBlockHead > lastProcessed":
    (await manager.init()).isOkOr:
      raiseAssert $error

    var isReady = true
    try:
      isReady = await manager.isReady()
    except Exception, CatchableError:
      assert false, "exception raised: " & getCurrentExceptionMsg()

    check:
      isReady == false

  asyncTest "isReady should return true if ethRpc is ready":
    (await manager.init()).isOkOr:
      raiseAssert $error
    # node can only be ready after group sync is done
    (await manager.startGroupSync()).isOkOr:
      raiseAssert $error

    var isReady = false
    try:
      isReady = await manager.isReady()
    except Exception, CatchableError:
      assert false, "exception raised: " & getCurrentExceptionMsg()

    check:
      isReady == true

  ################################
  ## Terminating/removing Anvil
  ################################

  # We stop Anvil daemon
  stopAnvil(runAnvil)
