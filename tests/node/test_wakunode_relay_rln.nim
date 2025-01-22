{.used.}

import
  std/[tempfiles, strutils, options],
  stew/shims/net as stewNet,
  stew/results,
  testutils/unittests,
  chronos,
  libp2p/switch,
  libp2p/protocols/pubsub/pubsub,
  eth/keys

from std/times import epochTime

import
  ../../../waku/[
    node/waku_node,
    node/peer_manager,
    waku_core,
    waku_node,
    common/error_handling,
    waku_rln_relay,
    waku_rln_relay/rln,
    waku_rln_relay/protocol_types,
    waku_keystore/keystore,
  ],
  ../waku_store/store_utils,
  ../waku_archive/archive_utils,
  ../testlib/[wakucore, wakunode, testasync, futures, common, assertions],
  ../resources/payloads,
  ../waku_rln_relay/[utils_static, utils_onchain]

from ../../waku/waku_noise/noise_utils import randomSeqByte

proc buildRandomIdentityCredentials(): IdentityCredential =
  # We generate a random identity credential (inter-value constrains are not enforced, otherwise we need to load e.g. zerokit RLN keygen)
  let
    idTrapdoor = randomSeqByte(rng[], 32)
    idNullifier = randomSeqByte(rng[], 32)
    idSecretHash = randomSeqByte(rng[], 32)
    idCommitment = randomSeqByte(rng[], 32)

  IdentityCredential(
    idTrapdoor: idTrapdoor,
    idNullifier: idNullifier,
    idSecretHash: idSecretHash,
    idCommitment: idCommitment,
  )

proc addMembershipCredentialsToKeystore(
    credentials: IdentityCredential,
    keystorePath: string,
    appInfo: AppInfo,
    rlnRelayEthContractAddress: string,
    password: string,
    membershipIndex: uint,
): KeystoreResult[void] =
  let
    contract = MembershipContract(chainId: "0x539", address: rlnRelayEthContractAddress)
    # contract = MembershipContract(chainId: "1337", address: rlnRelayEthContractAddress)
    index = MembershipIndex(membershipIndex)
    membershipCredential = KeystoreMembership(
      membershipContract: contract, treeIndex: index, identityCredential: credentials
    )

  addMembershipCredentials(
    path = keystorePath,
    membership = membershipCredential,
    password = password,
    appInfo = appInfo,
  )

proc fatalErrorVoidHandler(errMsg: string) {.gcsafe, raises: [].} =
  discard

proc getWakuRlnConfigOnChain*(
    keystorePath: string,
    appInfo: AppInfo,
    rlnRelayEthContractAddress: string,
    password: string,
    credIndex: uint,
    fatalErrorHandler: Option[OnFatalErrorHandler] = none(OnFatalErrorHandler),
    ethClientAddress: Option[string] = none(string),
): WakuRlnConfig =
  return WakuRlnConfig(
    rlnRelayDynamic: true,
    rlnRelayCredIndex: some(credIndex),
    rlnRelayEthContractAddress: rlnRelayEthContractAddress,
    rlnRelayEthClientAddress: ethClientAddress.get(EthClient),
    rlnRelayTreePath: genTempPath("rln_tree", "wakunode_" & $credIndex),
    rlnEpochSizeSec: 1,
    onFatalErrorAction: fatalErrorHandler.get(fatalErrorVoidHandler),
    # If these are used, initialisation fails with "failed to mount WakuRlnRelay: could not initialize the group manager: the commitment does not have a membership"
    rlnRelayCredPath: keystorePath,
    rlnRelayCredPassword: password,
  )

proc setupRelayWithOnChainRln*(
    node: WakuNode, shards: seq[RelayShard], wakuRlnConfig: WakuRlnConfig
) {.async.} =
  await node.mountRelay(shards)
  await node.mountRlnRelay(wakuRlnConfig)

suite "Waku RlnRelay - End to End - Static":
  var
    pubsubTopic {.threadvar.}: PubsubTopic
    contentTopic {.threadvar.}: ContentTopic

  var
    server {.threadvar.}: WakuNode
    client {.threadvar.}: WakuNode

  var
    serverRemotePeerInfo {.threadvar.}: RemotePeerInfo
    clientPeerId {.threadvar.}: PeerId

  asyncSetup:
    pubsubTopic = DefaultPubsubTopic
    contentTopic = DefaultContentTopic

    let
      serverKey = generateSecp256k1Key()
      clientKey = generateSecp256k1Key()

    server = newTestWakuNode(serverKey, ValidIpAddress.init("0.0.0.0"), Port(0))
    client = newTestWakuNode(clientKey, ValidIpAddress.init("0.0.0.0"), Port(0))

    await allFutures(server.start(), client.start())

    serverRemotePeerInfo = server.switch.peerInfo.toRemotePeerInfo()
    clientPeerId = client.switch.peerInfo.toRemotePeerInfo().peerId

  asyncTeardown:
    await allFutures(client.stop(), server.stop())

  suite "Mount":
    asyncTest "Can't mount if relay is not mounted":
      # Given Relay and RLN are not mounted
      check:
        server.wakuRelay == nil
        server.wakuRlnRelay == nil

      # When RlnRelay is mounted
      let catchRes = catch:
        await server.setupStaticRln(1)

      # Then Relay and RLN are not mounted,and the process fails
      check:
        server.wakuRelay == nil
        server.wakuRlnRelay == nil
        catchRes.error()[].msg ==
          "WakuRelay protocol is not mounted, cannot mount WakuRlnRelay"

    asyncTest "Pubsub topics subscribed before mounting RlnRelay are added to it":
      # Given the node enables Relay and Rln while subscribing to a pubsub topic
      await server.setupRelayWithStaticRln(1.uint, @[pubsubTopic])
      await client.setupRelayWithStaticRln(2.uint, @[pubsubTopic])
      check:
        server.wakuRelay != nil
        server.wakuRlnRelay != nil
        client.wakuRelay != nil
        client.wakuRlnRelay != nil

      # And the nodes are connected
      await client.connectToNodes(@[serverRemotePeerInfo])

      # And the node registers the completion handler
      var completionFuture = subscribeCompletionHandler(server, pubsubTopic)

      # When the client sends a valid RLN message
      let isCompleted1 =
        await sendRlnMessage(client, pubsubTopic, contentTopic, completionFuture)

      # Then the valid RLN message is relayed
      check:
        isCompleted1
        completionFuture.read()

      # When the client sends an invalid RLN message
      completionFuture = newBoolFuture()
      let isCompleted2 = await sendRlnMessageWithInvalidProof(
        client, pubsubTopic, contentTopic, completionFuture
      )

      # Then the invalid RLN message is not relayed
      check:
        not isCompleted2

    asyncTest "Pubsub topics subscribed after mounting RlnRelay are added to it":
      # Given the node enables Relay and Rln without subscribing to a pubsub topic
      await server.setupRelayWithStaticRln(1.uint, @[])
      await client.setupRelayWithStaticRln(2.uint, @[])

      # And the nodes are connected
      await client.connectToNodes(@[serverRemotePeerInfo])

      # await sleepAsync(FUTURE_TIMEOUT)
      # And the node registers the completion handler
      var completionFuture = subscribeCompletionHandler(server, pubsubTopic)

      await sleepAsync(FUTURE_TIMEOUT)
      # When the client sends a valid RLN message
      let isCompleted1 =
        await sendRlnMessage(client, pubsubTopic, contentTopic, completionFuture)

      # Then the valid RLN message is relayed
      check:
        isCompleted1
        completionFuture.read()

      # When the client sends an invalid RLN message
      completionFuture = newBoolFuture()
      let isCompleted2 = await sendRlnMessageWithInvalidProof(
        client, pubsubTopic, contentTopic, completionFuture
      )

      # Then the invalid RLN message is not relayed
      check:
        not isCompleted2

    asyncTest "rln-relay-max-message-limit testing":
      let
        nodekey = generateSecp256k1Key()
        node = newTestWakuNode(nodekey, parseIpAddress("0.0.0.0"), Port(0))

      await node.mountRelay(@[DefaultRelayShard])

      let contractAddress = await uploadRLNContract(EthClient)
      let wakuRlnConfig = WakuRlnConfig(
        rlnRelayDynamic: true,
        rlnRelayCredIndex: some(0.uint),
        rlnRelayUserMessageLimit: 111,
        rlnRelayTreepath: genTempPath("rln_tree", "wakunode_0"),
        rlnRelayEthClientAddress: EthClient,
        rlnRelayEthContractAddress: $contractAddress,
        rlnRelayChainId: 1337,
        onFatalErrorAction: proc(errStr: string) =
          raiseAssert errStr
        ,
      )

      try:
        await node.mountRlnRelay(wakuRlnConfig)
      except CatchableError as e:
        check e.msg ==
          "failed to mount WakuRlnRelay: rln-relay-user-message-limit can't exceed the MAX_MESSAGE_LIMIT in the rln contract"

  suite "Analysis of Bandwith Limitations":
    asyncTest "Valid Payload Sizes":
      # Given the node enables Relay and Rln while subscribing to a pubsub topic
      await server.setupRelayWithStaticRln(1.uint, @[pubsubTopic])
      await client.setupRelayWithStaticRln(2.uint, @[pubsubTopic])

      # And the nodes are connected
      await client.connectToNodes(@[serverRemotePeerInfo])

      # Register Relay Handler
      var completionFut = newPushHandlerFuture()
      proc relayHandler(
          topic: PubsubTopic, msg: WakuMessage
      ): Future[void] {.async, gcsafe.} =
        if topic == pubsubTopic:
          completionFut.complete((topic, msg))

      let subscriptionEvent = (kind: PubsubSub, topic: pubsubTopic)
      server.subscribe(subscriptionEvent, some(relayHandler))
      await sleepAsync(FUTURE_TIMEOUT)

      # Generate Messages
      let
        epoch = epochTime()
        payload1b = getByteSequence(1)
        payload1kib = getByteSequence(1024)
        overhead: uint64 = 419
        payload150kib = getByteSequence((150 * 1024) - overhead)
        payload150kibPlus = getByteSequence((150 * 1024) - overhead + 1)

      var
        message1b = WakuMessage(payload: @payload1b, contentTopic: contentTopic)
        message1kib = WakuMessage(payload: @payload1kib, contentTopic: contentTopic)
        message150kib = WakuMessage(payload: @payload150kib, contentTopic: contentTopic)
        message151kibPlus =
          WakuMessage(payload: @payload150kibPlus, contentTopic: contentTopic)

      doAssert(
        client.wakuRlnRelay
        .appendRLNProof(
          message1b, epoch + float64(client.wakuRlnRelay.rlnEpochSizeSec * 0)
        )
        .isOk()
      )
      doAssert(
        client.wakuRlnRelay
        .appendRLNProof(
          message1kib, epoch + float64(client.wakuRlnRelay.rlnEpochSizeSec * 1)
        )
        .isOk()
      )
      doAssert(
        client.wakuRlnRelay
        .appendRLNProof(
          message150kib, epoch + float64(client.wakuRlnRelay.rlnEpochSizeSec * 2)
        )
        .isOk()
      )
      doAssert(
        client.wakuRlnRelay
        .appendRLNProof(
          message151kibPlus, epoch + float64(client.wakuRlnRelay.rlnEpochSizeSec * 3)
        )
        .isOk()
      )

      # When sending the 1B message
      discard await client.publish(some(pubsubTopic), message1b)
      discard await completionFut.withTimeout(FUTURE_TIMEOUT_LONG)

      # Then the message is relayed
      check completionFut.read() == (pubsubTopic, message1b)
      # When sending the 1KiB message
      completionFut = newPushHandlerFuture() # Reset Future
      discard await client.publish(some(pubsubTopic), message1kib)
      discard await completionFut.withTimeout(FUTURE_TIMEOUT_LONG)

      # Then the message is relayed
      check completionFut.read() == (pubsubTopic, message1kib)

      # When sending the 150KiB message
      completionFut = newPushHandlerFuture() # Reset Future
      discard await client.publish(some(pubsubTopic), message150kib)
      discard await completionFut.withTimeout(FUTURE_TIMEOUT_LONG)

      # Then the message is relayed
      check completionFut.read() == (pubsubTopic, message150kib)

      # When sending the 150KiB plus message
      completionFut = newPushHandlerFuture() # Reset Future
      discard await client.publish(some(pubsubTopic), message151kibPlus)

      # Then the message is not relayed
      check not await completionFut.withTimeout(FUTURE_TIMEOUT_LONG)

    asyncTest "Invalid Payload Sizes":
      # Given the node enables Relay and Rln while subscribing to a pubsub topic
      await server.setupRelayWithStaticRln(1.uint, @[pubsubTopic])
      await client.setupRelayWithStaticRln(2.uint, @[pubsubTopic])

      # And the nodes are connected
      await client.connectToNodes(@[serverRemotePeerInfo])

      # Register Relay Handler
      var completionFut = newPushHandlerFuture()
      proc relayHandler(
          topic: PubsubTopic, msg: WakuMessage
      ): Future[void] {.async, gcsafe.} =
        if topic == pubsubTopic:
          completionFut.complete((topic, msg))

      let subscriptionEvent = (kind: PubsubSub, topic: pubsubTopic)
      server.subscribe(subscriptionEvent, some(relayHandler))
      await sleepAsync(FUTURE_TIMEOUT)

      # Generate Messages
      let
        epoch = epochTime()
        overhead: uint64 = 419
        payload150kibPlus = getByteSequence((150 * 1024) - overhead + 1)

      var message151kibPlus =
        WakuMessage(payload: @payload150kibPlus, contentTopic: contentTopic)

      doAssert(
        client.wakuRlnRelay
        .appendRLNProof(
          message151kibPlus, epoch + float64(client.wakuRlnRelay.rlnEpochSizeSec * 3)
        )
        .isOk()
      )

      # When sending the 150KiB plus message
      completionFut = newPushHandlerFuture() # Reset Future
      discard await client.publish(some(pubsubTopic), message151kibPlus)

      # Then the message is not relayed
      check not await completionFut.withTimeout(FUTURE_TIMEOUT_LONG)

suite "Waku RlnRelay - End to End - OnChain":
  let runAnvil {.used.} = runAnvil()

  var
    pubsubTopic {.threadvar.}: PubsubTopic
    contentTopic {.threadvar.}: ContentTopic

  var
    server {.threadvar.}: WakuNode
    client {.threadvar.}: WakuNode

  var
    serverRemotePeerInfo {.threadvar.}: RemotePeerInfo
    clientPeerId {.threadvar.}: PeerId

  asyncSetup:
    pubsubTopic = DefaultPubsubTopic
    contentTopic = DefaultContentTopic

    let
      serverKey = generateSecp256k1Key()
      clientKey = generateSecp256k1Key()

    server = newTestWakuNode(serverKey, ValidIpAddress.init("0.0.0.0"), Port(0))
    client = newTestWakuNode(clientKey, ValidIpAddress.init("0.0.0.0"), Port(0))

    await allFutures(server.start(), client.start())

    serverRemotePeerInfo = server.switch.peerInfo.toRemotePeerInfo()
    clientPeerId = client.switch.peerInfo.toRemotePeerInfo().peerId

  asyncTeardown:
    await allFutures(client.stop(), server.stop())

  suite "Smart Contract Availability and Interaction":
    asyncTest "Invalid format contract":
      let
        # One character missing
        invalidContractAddress = "0x000000000000000000000000000000000000000"
        keystorePath =
          genTempPath("rln_keystore", "test_wakunode_relay_rln-no_valid_contract")
        appInfo = RlnAppInfo
        password = "1234"
        wakuRlnConfig1 = getWakuRlnConfigOnChain(
          keystorePath, appInfo, invalidContractAddress, password, 0
        )
        wakuRlnConfig2 = getWakuRlnConfigOnChain(
          keystorePath, appInfo, invalidContractAddress, password, 1
        )
        idCredential = buildRandomIdentityCredentials()
        persistRes = addMembershipCredentialsToKeystore(
          idCredential, keystorePath, appInfo, invalidContractAddress, password, 1
        )
      assertResultOk(persistRes)

      # Given the node enables Relay and Rln while subscribing to a pubsub topic
      try:
        await server.setupRelayWithOnChainRln(@[pubsubTopic], wakuRlnConfig1)
        assert false, "Relay should fail mounting when using an invalid contract"
      except CatchableError:
        assert true

      try:
        await client.setupRelayWithOnChainRln(@[pubsubTopic], wakuRlnConfig2)
        assert false, "Relay should fail mounting when using an invalid contract"
      except CatchableError:
        assert true

    asyncTest "Unregistered contract":
      # This is a very slow test due to the retries RLN does. Might take upwards of 1m-2m to finish.
      let
        invalidContractAddress = "0x0000000000000000000000000000000000000000"
        keystorePath =
          genTempPath("rln_keystore", "test_wakunode_relay_rln-no_valid_contract")
        appInfo = RlnAppInfo
        password = "1234"

      # Connect to the eth client
      discard await newWeb3(EthClient)

      var serverErrorFuture = Future[string].new()
      proc serverFatalErrorHandler(errMsg: string) {.gcsafe, closure, raises: [].} =
        serverErrorFuture.complete(errMsg)

      var clientErrorFuture = Future[string].new()
      proc clientFatalErrorHandler(errMsg: string) {.gcsafe, closure, raises: [].} =
        clientErrorFuture.complete(errMsg)

      let
        wakuRlnConfig1 = getWakuRlnConfigOnChain(
          keystorePath,
          appInfo,
          invalidContractAddress,
          password,
          0,
          some(serverFatalErrorHandler),
        )
        wakuRlnConfig2 = getWakuRlnConfigOnChain(
          keystorePath,
          appInfo,
          invalidContractAddress,
          password,
          1,
          some(clientFatalErrorHandler),
        )

      # Given the node enable Relay and Rln while subscribing to a pubsub topic.
      # The withTimeout call is a workaround for the test not to terminate with an exception.
      # However, it doesn't reduce the retries against the blockchain that the mounting rln process attempts (until it accepts failure).
      # Note: These retries might be an unintended library issue.
      discard await server
      .setupRelayWithOnChainRln(@[pubsubTopic], wakuRlnConfig1)
      .withTimeout(FUTURE_TIMEOUT)
      discard await client
      .setupRelayWithOnChainRln(@[pubsubTopic], wakuRlnConfig2)
      .withTimeout(FUTURE_TIMEOUT)

      check:
        (await serverErrorFuture.waitForResult()).get() ==
          "Failed to get the storage index: No response from the Web3 provider"
        (await clientErrorFuture.waitForResult()).get() ==
          "Failed to get the storage index: No response from the Web3 provider"

    asyncTest "Valid contract":
      #[
        # Notes
        ## Issues
        ### TreeIndex
        For some reason the calls to `getWakuRlnConfigOnChain` need to be made with `treeIndex` = 0 and 1, in that order.
        But the registration needs to be made with 1 and 2. 
        #### Solutions
        Requires investigation
        ### Monkeypatching
        Instead of running the idCredentials monkeypatch, passing the correct membershipIndex and keystorePath and keystorePassword should work.
        #### Solutions
        A) Using the register callback to fetch the correct membership
        B) Using two different keystores, one for each rlnconfig. If there's only one key, it will fetch it regardless of membershipIndex.
        ##### A
        - Register is not calling callback even though register is happening, this should happen.
        - This command should be working, but it doesn't on the current HEAD of the branch, it does work on master, which suggest there's something wrong with the branch.
        - nim c -r --out:build/onchain -d:chronicles_log_level=NOTICE --verbosity:0 --hints:off  -d:git_version="v0.27.0-rc.0-3-gaa9c30" -d:release --passL:librln_v0.3.7.a --passL:-lm tests/waku_rln_relay/test_rln_group_manager_onchain.nim && onchain_group_test
        - All modified files are tests/*, which is a bit weird. Might be interesting re-creating the branch slowly, and checking out why this is happening.
        ##### B
        Untested
      ]#

      let
        onChainGroupManager = await setup()
        contractAddress = onChainGroupManager.ethContractAddress
        keystorePath =
          genTempPath("rln_keystore", "test_wakunode_relay_rln-valid_contract")
        appInfo = RlnAppInfo
        password = "1234"
        rlnInstance = onChainGroupManager.rlnInstance
      assertResultOk(createAppKeystore(keystorePath, appInfo))

      # Generate configs before registering the credentials. Otherwise the file gets cleared up.
      let
        wakuRlnConfig1 =
          getWakuRlnConfigOnChain(keystorePath, appInfo, contractAddress, password, 0)
        wakuRlnConfig2 =
          getWakuRlnConfigOnChain(keystorePath, appInfo, contractAddress, password, 1)

      # Generate credentials
      let
        idCredential1 = rlnInstance.membershipKeyGen().get()
        idCredential2 = rlnInstance.membershipKeyGen().get()

      discard await onChainGroupManager.init()
      try:
        # Register credentials in the chain
        waitFor onChainGroupManager.register(idCredential1)
        waitFor onChainGroupManager.register(idCredential2)
      except Exception:
        assert false, "Failed to register credentials: " & getCurrentExceptionMsg()

      # Add credentials to keystore
      let
        persistRes1 = addMembershipCredentialsToKeystore(
          idCredential1, keystorePath, appInfo, contractAddress, password, 0
        )
        persistRes2 = addMembershipCredentialsToKeystore(
          idCredential2, keystorePath, appInfo, contractAddress, password, 1
        )

      assertResultOk(persistRes1)
      assertResultOk(persistRes2)

      await onChainGroupManager.stop()

      # Given the node enables Relay and Rln while subscribing to a pubsub topic
      await server.setupRelayWithOnChainRln(@[pubsubTopic], wakuRlnConfig1)
      await client.setupRelayWithOnChainRln(@[pubsubTopic], wakuRlnConfig2)

      try:
        (await server.wakuRlnRelay.groupManager.startGroupSync()).isOkOr:
          raiseAssert $error
        (await client.wakuRlnRelay.groupManager.startGroupSync()).isOkOr:
          raiseAssert $error

        # Test Hack: Monkeypatch the idCredentials into the groupManager
        server.wakuRlnRelay.groupManager.idCredentials = some(idCredential1)
        client.wakuRlnRelay.groupManager.idCredentials = some(idCredential2)
      except Exception, CatchableError:
        assert false, "exception raised: " & getCurrentExceptionMsg()

      # And the nodes are connected
      let serverRemotePeerInfo = server.switch.peerInfo.toRemotePeerInfo()
      await client.connectToNodes(@[serverRemotePeerInfo])

      # And the node registers the completion handler
      var completionFuture = subscribeCompletionHandler(server, pubsubTopic)

      # When the client sends a valid RLN message
      let isCompleted =
        await sendRlnMessage(client, pubsubTopic, contentTopic, completionFuture)

      # Then the valid RLN message is relayed
      check isCompleted
      assertResultOk(await completionFuture.waitForResult())

    asyncTest "Not enough gas":
      let
        onChainGroupManager = await setupOnchainGroupManager(amountWei = 0.u256)
        contractAddress = onChainGroupManager.ethContractAddress
        keystorePath =
          genTempPath("rln_keystore", "test_wakunode_relay_rln-valid_contract")
        appInfo = RlnAppInfo
        password = "1234"
        rlnInstance = onChainGroupManager.rlnInstance
      assertResultOk(createAppKeystore(keystorePath, appInfo))

      # Generate credentials
      let idCredential = rlnInstance.membershipKeyGen().get()

      discard await onChainGroupManager.init()
      var errorFuture = Future[string].new()
      onChainGroupManager.onFatalErrorAction = proc(
          errMsg: string
      ) {.gcsafe, closure.} =
        errorFuture.complete(errMsg)
      try:
        # Register credentials in the chain
        waitFor onChainGroupManager.register(idCredential)
        assert false, "Should have failed to register credentials given there is 0 gas"
      except Exception:
        assert true

      check (await errorFuture.waitForResult()).get() ==
        "Failed to register the member: {\"code\":-32003,\"message\":\"Insufficient funds for gas * price + value\"}"
      await onChainGroupManager.stop()

  suite "RLN Relay Configuration and Parameters":
    asyncTest "RLN Relay Credential Path":
      let
        onChainGroupManager = await setup()
        contractAddress = onChainGroupManager.ethContractAddress
        keystorePath =
          genTempPath("rln_keystore", "test_wakunode_relay_rln-valid_contract")
        appInfo = RlnAppInfo
        password = "1234"
        rlnInstance = onChainGroupManager.rlnInstance
      assertResultOk(createAppKeystore(keystorePath, appInfo))

      # Generate configs before registering the credentials. Otherwise the file gets cleared up.
      let
        wakuRlnConfig1 =
          getWakuRlnConfigOnChain(keystorePath, appInfo, contractAddress, password, 0)
        wakuRlnConfig2 =
          getWakuRlnConfigOnChain(keystorePath, appInfo, contractAddress, password, 1)

      # Given the node enables Relay and Rln while subscribing to a pubsub topic
      await server.setupRelayWithOnChainRln(@[pubsubTopic], wakuRlnConfig1)
      await client.setupRelayWithOnChainRln(@[pubsubTopic], wakuRlnConfig2)

      try:
        (await server.wakuRlnRelay.groupManager.startGroupSync()).isOkOr:
          raiseAssert $error
        (await client.wakuRlnRelay.groupManager.startGroupSync()).isOkOr:
          raiseAssert $error

        # Test Hack: Monkeypatch the idCredentials into the groupManager
        echo server.wakuRlnRelay.groupManager.idCredentials
        echo client.wakuRlnRelay.groupManager.idCredentials
      except Exception, CatchableError:
        assert false, "exception raised: " & getCurrentExceptionMsg()

      # And the nodes are connected
      let serverRemotePeerInfo = server.switch.peerInfo.toRemotePeerInfo()
      await client.connectToNodes(@[serverRemotePeerInfo])

      # And the node registers the completion handler
      var completionFuture = subscribeCompletionHandler(server, pubsubTopic)

      # When the client attempts to send a message
      try:
        let isCompleted =
          await sendRlnMessage(client, pubsubTopic, contentTopic, completionFuture)
        assert false, "Should have failed to send a message"
      except AssertionDefect as e:
        # Then the message is not relayed
        assert e.msg.endsWith("identity credentials are not set")

  suite "RLN Relay Resilience, Security and Compatibility":
    asyncTest "Key Management and Integrity":
      let
        onChainGroupManager = await setup()
        contractAddress = onChainGroupManager.ethContractAddress
        keystorePath =
          genTempPath("rln_keystore", "test_wakunode_relay_rln-valid_contract")
        appInfo = RlnAppInfo
        password = "1234"
        rlnInstance = onChainGroupManager.rlnInstance
      assertResultOk(createAppKeystore(keystorePath, appInfo))

      # Generate configs before registering the credentials. Otherwise the file gets cleared up.
      let
        wakuRlnConfig1 =
          getWakuRlnConfigOnChain(keystorePath, appInfo, contractAddress, password, 0)
        wakuRlnConfig2 =
          getWakuRlnConfigOnChain(keystorePath, appInfo, contractAddress, password, 1)

      # Generate credentials
      let
        idCredential1 = rlnInstance.membershipKeyGen().get()
        idCredential2 = rlnInstance.membershipKeyGen().get()

      discard await onChainGroupManager.init()
      try:
        # Register credentials in the chain
        waitFor onChainGroupManager.register(idCredential1)
        waitFor onChainGroupManager.register(idCredential2)
      except Exception:
        assert false, "Failed to register credentials: " & getCurrentExceptionMsg()

      # Add credentials to keystore
      let
        persistRes1 = addMembershipCredentialsToKeystore(
          idCredential1, keystorePath, appInfo, contractAddress, password, 0
        )
        persistRes2 = addMembershipCredentialsToKeystore(
          idCredential2, keystorePath, appInfo, contractAddress, password, 1
        )

      assertResultOk(persistRes1)
      assertResultOk(persistRes2)

      # await onChainGroupManager.stop()

      let
        registryContract = onChainGroupManager.registryContract.get()
        storageIndex = (await registryContract.usingStorageIndex().call())
        rlnContractAddress = await registryContract.storages(storageIndex).call()
        contract = onChainGroupManager.ethRpc.get().contractSender(
            RlnStorage, rlnContractAddress
          )
        contract2 = onChainGroupManager.rlnContract.get()

      echo "###"
      echo await (contract.memberExists(idCredential1.idCommitment.toUInt256()).call())
      echo await (contract.memberExists(idCredential2.idCommitment.toUInt256()).call())
      echo await (contract2.memberExists(idCredential1.idCommitment.toUInt256()).call())
      echo await (contract2.memberExists(idCredential2.idCommitment.toUInt256()).call())
      echo "###"

  ################################
  ## Terminating/removing Anvil
  ################################

  # We stop Anvil daemon
  stopAnvil(runAnvil)
