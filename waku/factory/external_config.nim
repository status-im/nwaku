import
  std/[strutils, strformat],
  results,
  chronos,
  regex,
  confutils,
  confutils/defs,
  confutils/std/net,
  confutils/toml/defs as confTomlDefs,
  confutils/toml/std/net as confTomlNet,
  libp2p/crypto/crypto,
  libp2p/crypto/secp,
  libp2p/multiaddress,
  nimcrypto/utils,
  secp256k1,
  json
import
  ../common/confutils/envvar/defs as confEnvvarDefs,
  ../common/confutils/envvar/std/net as confEnvvarNet,
  ../common/logging,
  ../waku_enr,
  ../node/peer_manager,
  ../waku_core/topics/pubsub_topic

include ../waku_core/message/default_values

export confTomlDefs, confTomlNet, confEnvvarDefs, confEnvvarNet

# Git version in git describe format (defined at compile time)
const git_version* {.strdefine.} = "n/a"

type ConfResult*[T] = Result[T, string]

type EthRpcUrl* = distinct string

type ProtectedShard* = object
  shard*: uint16
  key*: secp256k1.SkPublicKey

type StartUpCommand* = enum
  noCommand # default, runs waku
  generateRlnKeystore # generates a new RLN keystore
  inspectRlnDb # Inspects a given RLN tree db, providing essential db stats

type WakuNodeConf* = object
  configFile* {.
    desc: "Loads configuration from a TOML file (cmd-line parameters take precedence)",
    name: "config-file"
  .}: Option[InputFile]

  ## Log configuration
  logLevel* {.
    desc:
      "Sets the log level for process. Supported levels: TRACE, DEBUG, INFO, NOTICE, WARN, ERROR or FATAL",
    defaultValue: logging.LogLevel.INFO,
    name: "log-level"
  .}: logging.LogLevel

  logFormat* {.
    desc:
      "Specifies what kind of logs should be written to stdout. Supported formats: TEXT, JSON",
    defaultValue: logging.LogFormat.TEXT,
    name: "log-format"
  .}: logging.LogFormat

  rlnRelayCredPath* {.
    desc: "The path for peristing rln-relay credential",
    defaultValue: "",
    name: "rln-relay-cred-path"
  .}: string

  rlnRelayEthClientAddress* {.
    desc: "HTTP address of an Ethereum testnet client e.g., http://localhost:8540/",
    defaultValue: "http://localhost:8540/",
    name: "rln-relay-eth-client-address"
  .}: EthRpcUrl

  rlnRelayEthContractAddress* {.
    desc: "Address of membership contract on an Ethereum testnet",
    defaultValue: "",
    name: "rln-relay-eth-contract-address"
  .}: string

  rlnRelayChainId* {.
    desc:
      "Chain ID of the provided contract (optional, will fetch from RPC provider if not used)",
    defaultValue: 0,
    name: "rln-relay-chain-id"
  .}: uint

  rlnRelayCredPassword* {.
    desc: "Password for encrypting RLN credentials",
    defaultValue: "",
    name: "rln-relay-cred-password"
  .}: string

  rlnRelayEthPrivateKey* {.
    desc: "Private key for broadcasting transactions",
    defaultValue: "",
    name: "rln-relay-eth-private-key"
  .}: string

  rlnRelayUserMessageLimit* {.
    desc:
      "Set a user message limit for the rln membership registration. Must be a positive integer. Default is 1.",
    defaultValue: 1,
    name: "rln-relay-user-message-limit"
  .}: uint64

  rlnEpochSizeSec* {.
    desc:
      "Epoch size in seconds used to rate limit RLN memberships. Default is 1 second.",
    defaultValue: 1,
    name: "rln-relay-epoch-sec"
  .}: uint64

  maxMessageSize* {.
    desc:
      "Maximum message size. Accepted units: KiB, KB, and B. e.g. 1024KiB; 1500 B; etc.",
    defaultValue: DefaultMaxWakuMessageSizeStr,
    name: "max-msg-size"
  .}: string

  case cmd* {.command, defaultValue: noCommand.}: StartUpCommand
  of inspectRlnDb:
    # have to change the name here since it counts as a duplicate, within noCommand
    treePath* {.
      desc: "Path to the RLN merkle tree sled db (https://github.com/spacejam/sled)",
      defaultValue: "",
      name: "rln-relay-tree-path"
    .}: string
  of generateRlnKeystore:
    execute* {.
      desc: "Runs the registration function on-chain. By default, a dry-run will occur",
      defaultValue: false,
      name: "execute"
    .}: bool
  of noCommand:
    ##  Application-level configuration
    protectedShards* {.
      desc:
        "Shards and its public keys to be used for message validation, shard:pubkey. Argument may be repeated.",
      defaultValue: newSeq[ProtectedShard](0),
      name: "protected-shard"
    .}: seq[ProtectedShard]

    ## General node config
    clusterId* {.
      desc:
        "Cluster id that the node is running in. Node in a different cluster id is disconnected.",
      defaultValue: 0,
      name: "cluster-id"
    .}: uint16

    agentString* {.
      defaultValue: "nwaku-" & external_config.git_version,
      desc: "Node agent string which is used as identifier in network",
      name: "agent-string"
    .}: string

    nodekey* {.desc: "P2P node private key as 64 char hex string.", name: "nodekey".}:
      Option[PrivateKey]

    listenAddress* {.
      defaultValue: defaultListenAddress(),
      desc: "Listening address for LibP2P (and Discovery v5, if enabled) traffic.",
      name: "listen-address"
    .}: IpAddress

    tcpPort* {.desc: "TCP listening port.", defaultValue: 60000, name: "tcp-port".}:
      Port

    portsShift* {.
      desc: "Add a shift to all port numbers.", defaultValue: 0, name: "ports-shift"
    .}: uint16

    nat* {.
      desc:
        "Specify method to use for determining public address. " &
        "Must be one of: any, none, upnp, pmp, extip:<IP>.",
      defaultValue: "any"
    .}: string

    extMultiAddrs* {.
      desc:
        "External multiaddresses to advertise to the network. Argument may be repeated.",
      name: "ext-multiaddr"
    .}: seq[string]

    extMultiAddrsOnly* {.
      desc: "Only announce external multiaddresses",
      defaultValue: false,
      name: "ext-multiaddr-only"
    .}: bool

    maxConnections* {.
      desc: "Maximum allowed number of libp2p connections.",
      defaultValue: 50,
      name: "max-connections"
    .}: int

    maxRelayPeers* {.
      desc:
        "Deprecated. Use relay-service-ratio instead. It represents the maximum allowed number of relay peers.",
      name: "max-relay-peers"
    .}: Option[int]

    relayServiceRatio* {.
      desc:
        "This percentage ratio represents the relay peers to service peers. For example, 60:40, tells that 60% of the max-connections will be used for relay protocol and the other 40% of max-connections will be reserved for other service protocols (e.g., filter, lightpush, store, metadata, etc.)",
      name: "relay-service-ratio",
      defaultValue: "60:40" # 60:40 ratio of relay to service peers
    .}: string

    colocationLimit* {.
      desc:
        "Max num allowed peers from the same IP. Set it to 0 to remove the limitation.",
      defaultValue: defaultColocationLimit(),
      name: "ip-colocation-limit"
    .}: int

    peerStoreCapacity* {.
      desc: "Maximum stored peers in the peerstore.", name: "peer-store-capacity"
    .}: Option[int]

    peerPersistence* {.
      desc: "Enable peer persistence.", defaultValue: false, name: "peer-persistence"
    .}: bool

    ## DNS addrs config
    dnsAddrs* {.
      desc: "Enable resolution of `dnsaddr`, `dns4` or `dns6` multiaddrs",
      defaultValue: true,
      name: "dns-addrs"
    .}: bool

    dnsAddrsNameServers* {.
      desc:
        "DNS name server IPs to query for DNS multiaddrs resolution. Argument may be repeated.",
      defaultValue: @[parseIpAddress("1.1.1.1"), parseIpAddress("1.0.0.1")],
      name: "dns-addrs-name-server"
    .}: seq[IpAddress]

    dns4DomainName* {.
      desc: "The domain name resolving to the node's public IPv4 address",
      defaultValue: "",
      name: "dns4-domain-name"
    .}: string

    ## Circuit-relay config
    isRelayClient* {.
      desc:
        """Set the node as a relay-client.
Set it to true for nodes that run behind a NAT or firewall and
hence would have reachability issues.""",
      defaultValue: false,
      name: "relay-client"
    .}: bool

    ## Relay config
    relay* {.
      desc: "Enable relay protocol: true|false", defaultValue: true, name: "relay"
    .}: bool

    relayPeerExchange* {.
      desc: "Enable gossipsub peer exchange in relay protocol: true|false",
      defaultValue: false,
      name: "relay-peer-exchange"
    .}: bool

    relayShardedPeerManagement* {.
      desc:
        "Enable experimental shard aware peer manager for relay protocol: true|false",
      defaultValue: false,
      name: "relay-shard-manager"
    .}: bool

    rlnRelay* {.
      desc: "Enable spam protection through rln-relay: true|false",
      defaultValue: false,
      name: "rln-relay"
    .}: bool

    rlnRelayCredIndex* {.
      desc: "the index of the onchain commitment to use",
      name: "rln-relay-membership-index"
    .}: Option[uint]

    rlnRelayDynamic* {.
      desc: "Enable  waku-rln-relay with on-chain dynamic group management: true|false",
      defaultValue: false,
      name: "rln-relay-dynamic"
    .}: bool

    rlnRelayIdKey* {.
      desc: "Rln relay identity secret key as a Hex string",
      defaultValue: "",
      name: "rln-relay-id-key"
    .}: string

    rlnRelayIdCommitmentKey* {.
      desc: "Rln relay identity commitment key as a Hex string",
      defaultValue: "",
      name: "rln-relay-id-commitment-key"
    .}: string

    rlnRelayTreePath* {.
      desc: "Path to the RLN merkle tree sled db (https://github.com/spacejam/sled)",
      defaultValue: "",
      name: "rln-relay-tree-path"
    .}: string

    rlnRelayBandwidthThreshold* {.
      desc: "Message rate in bytes/sec after which verification of proofs should happen",
      defaultValue: 0, # to maintain backwards compatibility
      name: "rln-relay-bandwidth-threshold"
    .}: int

    staticnodes* {.
      desc: "Peer multiaddr to directly connect with. Argument may be repeated.",
      name: "staticnode"
    .}: seq[string]

    keepAlive* {.
      desc: "Enable keep-alive for idle connections: true|false",
      defaultValue: false,
      name: "keep-alive"
    .}: bool

    # If numShardsInNetwork is not set, we use the number of shards configured as numShardsInNetwork
    numShardsInNetwork* {.
      desc: "Number of shards in the network",
      defaultValue: 0,
      name: "num-shards-in-network"
    .}: uint32

    shards* {.
      desc:
        "Shards index to subscribe to [0..NUM_SHARDS_IN_NETWORK-1]. Argument may be repeated.",
      defaultValue:
        @[
          uint16(0),
          uint16(1),
          uint16(2),
          uint16(3),
          uint16(4),
          uint16(5),
          uint16(6),
          uint16(7),
        ],
      name: "shard"
    .}: seq[uint16]

    contentTopics* {.
      desc: "Default content topic to subscribe to. Argument may be repeated.",
      name: "content-topic"
    .}: seq[string]

    ## Store and message store config
    store* {.
      desc: "Enable/disable waku store protocol", defaultValue: false, name: "store"
    .}: bool

    legacyStore* {.
      desc: "Enable/disable waku store legacy mode",
      defaultValue: true,
      name: "legacy-store"
    .}: bool

    storenode* {.
      desc: "Peer multiaddress to query for storage",
      defaultValue: "",
      name: "storenode"
    .}: string

    storeMessageRetentionPolicy* {.
      desc:
        "Message store retention policy. Time retention policy: 'time:<seconds>'. Capacity retention policy: 'capacity:<count>'. Size retention policy: 'size:<xMB/xGB>'. Set to 'none' to disable.",
      defaultValue: "time:" & $2.days.seconds,
      name: "store-message-retention-policy"
    .}: string

    storeMessageDbUrl* {.
      desc: "The database connection URL for peristent storage.",
      defaultValue: "sqlite://store.sqlite3",
      name: "store-message-db-url"
    .}: string

    storeMessageDbVacuum* {.
      desc:
        "Enable database vacuuming at start. Only supported by SQLite database engine.",
      defaultValue: false,
      name: "store-message-db-vacuum"
    .}: bool

    storeMessageDbMigration* {.
      desc: "Enable database migration at start.",
      defaultValue: true,
      name: "store-message-db-migration"
    .}: bool

    storeMaxNumDbConnections* {.
      desc: "Maximum number of simultaneous Postgres connections.",
      defaultValue: 50,
      name: "store-max-num-db-connections"
    .}: int

    storeResume* {.
      desc: "Enable store resume functionality",
      defaultValue: false,
      name: "store-resume"
    .}: bool

    ## Sync config
    storeSync* {.
      desc: "Enable store sync protocol: true|false",
      defaultValue: false,
      name: "store-sync"
    .}: bool

    storeSyncInterval* {.
      desc: "Interval between store sync attempts. In seconds.",
      defaultValue: 300, # 5 minutes
      name: "store-sync-interval"
    .}: int64

    storeSyncRange* {.
      desc: "Amount of time to sync. In seconds.",
      defaultValue: 3600, # 1 hours
      name: "store-sync-range"
    .}: int64

    storeSyncRelayJitter* {.
      hidden,
      desc: "Time offset to account for message propagation jitter. In seconds.",
      defaultValue: 20,
      name: "store-sync-relay-jitter"
    .}: int64

    storeSyncMaxPayloadSize* {.
      hidden,
      desc:
        "Max size in bytes of the inner negentropy payload. Cannot be less than 5K, 0 is unlimited.",
      defaultValue: 0,
      name: "store-sync-max-payload-size"
    .}: int64

    ## Filter config
    filter* {.
      desc: "Enable filter protocol: true|false", defaultValue: false, name: "filter"
    .}: bool

    filternode* {.
      desc: "Peer multiaddr to request content filtering of messages.",
      defaultValue: "",
      name: "filternode"
    .}: string

    filterSubscriptionTimeout* {.
      desc:
        "Timeout for filter subscription without ping or refresh it, in seconds. Only for v2 filter protocol.",
      defaultValue: 300, # 5 minutes
      name: "filter-subscription-timeout"
    .}: int64

    filterMaxPeersToServe* {.
      desc: "Maximum number of peers to serve at a time. Only for v2 filter protocol.",
      defaultValue: 1000,
      name: "filter-max-peers-to-serve"
    .}: uint32

    filterMaxCriteria* {.
      desc:
        "Maximum number of pubsub- and content topic combination per peers at a time. Only for v2 filter protocol.",
      defaultValue: 1000,
      name: "filter-max-criteria"
    .}: uint32

    ## Lightpush config
    lightpush* {.
      desc: "Enable lightpush protocol: true|false",
      defaultValue: false,
      name: "lightpush"
    .}: bool

    lightpushnode* {.
      desc: "Peer multiaddr to request lightpush of published messages.",
      defaultValue: "",
      name: "lightpushnode"
    .}: string

    ## Reliability config
    reliabilityEnabled* {.
      desc:
        """Adds an extra effort in the delivery/reception of messages by leveraging store-v3 requests.
with the drawback of consuming some more bandwidth.""",
      defaultValue: false,
      name: "reliability"
    .}: bool

    ## REST HTTP config
    rest* {.
      desc: "Enable Waku REST HTTP server: true|false", defaultValue: true, name: "rest"
    .}: bool

    restAddress* {.
      desc: "Listening address of the REST HTTP server.",
      defaultValue: parseIpAddress("127.0.0.1"),
      name: "rest-address"
    .}: IpAddress

    restPort* {.
      desc: "Listening port of the REST HTTP server.",
      defaultValue: 8645,
      name: "rest-port"
    .}: uint16

    restRelayCacheCapacity* {.
      desc: "Capacity of the Relay REST API message cache.",
      defaultValue: 30,
      name: "rest-relay-cache-capacity"
    .}: uint32

    restAdmin* {.
      desc: "Enable access to REST HTTP Admin API: true|false",
      defaultValue: false,
      name: "rest-admin"
    .}: bool

    restAllowOrigin* {.
      desc:
        "Allow cross-origin requests from the specified origin." &
        "Argument may be repeated." & "Wildcards: * or ? allowed." &
        "Ex.: \"localhost:*\" or \"127.0.0.1:8080\"",
      defaultValue: newSeq[string](),
      name: "rest-allow-origin"
    .}: seq[string]

    ## Metrics config
    metricsServer* {.
      desc: "Enable the metrics server: true|false",
      defaultValue: false,
      name: "metrics-server"
    .}: bool

    metricsServerAddress* {.
      desc: "Listening address of the metrics server.",
      defaultValue: parseIpAddress("127.0.0.1"),
      name: "metrics-server-address"
    .}: IpAddress

    metricsServerPort* {.
      desc: "Listening HTTP port of the metrics server.",
      defaultValue: 8008,
      name: "metrics-server-port"
    .}: uint16

    metricsLogging* {.
      desc: "Enable metrics logging: true|false",
      defaultValue: true,
      name: "metrics-logging"
    .}: bool

    ## DNS discovery config
    dnsDiscovery* {.
      desc: "Enable discovering nodes via DNS",
      defaultValue: false,
      name: "dns-discovery"
    .}: bool

    dnsDiscoveryUrl* {.
      desc: "URL for DNS node list in format 'enrtree://<key>@<fqdn>'",
      defaultValue: "",
      name: "dns-discovery-url"
    .}: string

    dnsDiscoveryNameServers* {.
      desc: "DNS name server IPs to query. Argument may be repeated.",
      defaultValue: @[parseIpAddress("1.1.1.1"), parseIpAddress("1.0.0.1")],
      name: "dns-discovery-name-server"
    .}: seq[IpAddress]

    ## Discovery v5 config
    discv5Discovery* {.
      desc: "Enable discovering nodes via Node Discovery v5",
      defaultValue: false,
      name: "discv5-discovery"
    .}: bool

    discv5UdpPort* {.
      desc: "Listening UDP port for Node Discovery v5.",
      defaultValue: 9000,
      name: "discv5-udp-port"
    .}: Port

    discv5BootstrapNodes* {.
      desc:
        "Text-encoded ENR for bootstrap node. Used when connecting to the network. Argument may be repeated.",
      name: "discv5-bootstrap-node"
    .}: seq[string]

    discv5EnrAutoUpdate* {.
      desc:
        "Discovery can automatically update its ENR with the IP address " &
        "and UDP port as seen by other nodes it communicates with. " &
        "This option allows to enable/disable this functionality",
      defaultValue: false,
      name: "discv5-enr-auto-update"
    .}: bool

    discv5TableIpLimit* {.
      hidden,
      desc: "Maximum amount of nodes with the same IP in discv5 routing tables",
      defaultValue: 10,
      name: "discv5-table-ip-limit"
    .}: uint

    discv5BucketIpLimit* {.
      hidden,
      desc: "Maximum amount of nodes with the same IP in discv5 routing table buckets",
      defaultValue: 2,
      name: "discv5-bucket-ip-limit"
    .}: uint

    discv5BitsPerHop* {.
      hidden,
      desc: "Kademlia's b variable, increase for less hops per lookup",
      defaultValue: 1,
      name: "discv5-bits-per-hop"
    .}: int

    discv5Only* {.
      desc: "Disable all protocols other than discv5",
      defaultValue: false,
      name: "discv5-only"
    .}: bool

    ## waku peer exchange config
    peerExchange* {.
      desc: "Enable waku peer exchange protocol (responder side): true|false",
      defaultValue: false,
      name: "peer-exchange"
    .}: bool

    peerExchangeNode* {.
      desc:
        "Peer multiaddr to send peer exchange requests to. (enables peer exchange protocol requester side)",
      defaultValue: "",
      name: "peer-exchange-node"
    .}: string

    ## Rendez vous
    rendezvous* {.
      desc: "Enable waku rendezvous discovery server",
      defaultValue: true,
      name: "rendezvous"
    .}: bool

    ## websocket config
    websocketSupport* {.
      desc: "Enable websocket:  true|false",
      defaultValue: false,
      name: "websocket-support"
    .}: bool

    websocketPort* {.
      desc: "WebSocket listening port.", defaultValue: 8000, name: "websocket-port"
    .}: Port

    websocketSecureSupport* {.
      desc: "Enable secure websocket:  true|false",
      defaultValue: false,
      name: "websocket-secure-support"
    .}: bool

    websocketSecureKeyPath* {.
      desc: "Secure websocket key path:   '/path/to/key.txt' ",
      defaultValue: "",
      name: "websocket-secure-key-path"
    .}: string

    websocketSecureCertPath* {.
      desc: "Secure websocket Certificate path:   '/path/to/cert.txt' ",
      defaultValue: "",
      name: "websocket-secure-cert-path"
    .}: string

    ## Rate limitation config, if not set, rate limit checks will not be performed
    rateLimits* {.
      desc:
        "Rate limit settings for different protocols." &
        "Format: protocol:volume/period<unit>" &
        " Where 'protocol' can be one of: <store|storev2|storev3|lightpush|px|filter> if not defined it means a global setting" &
        " 'volume' and period must be an integer value. " &
        " 'unit' must be one of <h|m|s|ms> - hours, minutes, seconds, milliseconds respectively. " &
        "Argument may be repeated.",
      defaultValue: newSeq[string](0),
      name: "rate-limit"
    .}: seq[string]

## Parsing

# NOTE: Keys are different in nim-libp2p
proc parseCmdArg*(T: type crypto.PrivateKey, p: string): T =
  try:
    let key = SkPrivateKey.init(utils.fromHex(p)).tryGet()
    crypto.PrivateKey(scheme: Secp256k1, skkey: key)
  except CatchableError:
    raise newException(ValueError, "Invalid private key")

proc parseCmdArg*[T](_: type seq[T], s: string): seq[T] {.raises: [ValueError].} =
  var
    inputSeq: JsonNode
    res: seq[T] = @[]

  try:
    inputSeq = s.parseJson()
  except Exception:
    raise newException(ValueError, fmt"Could not parse sequence: {s}")

  for entry in inputSeq:
    let formattedString = ($entry).strip(chars = {'\"'})
    res.add(parseCmdArg(T, formattedString))

  return res

proc completeCmdArg*(T: type crypto.PrivateKey, val: string): seq[string] =
  return @[]

# TODO: Remove when removing protected-topic configuration
proc isNumber(x: string): bool =
  try:
    discard parseInt(x)
    result = true
  except ValueError:
    result = false

proc parseCmdArg*(T: type ProtectedShard, p: string): T =
  let elements = p.split(":")
  if elements.len != 2:
    raise newException(
      ValueError, "Invalid format for protected shard expected shard:publickey"
    )
  let publicKey = secp256k1.SkPublicKey.fromHex(elements[1])
  if publicKey.isErr:
    raise newException(ValueError, "Invalid public key")

  if isNumber(elements[0]):
    return ProtectedShard(shard: uint16.parseCmdArg(elements[0]), key: publicKey.get())

  # TODO: Remove when removing protected-topic configuration
  let shard = RelayShard.parse(elements[0]).valueOr:
    raise newException(
      ValueError,
      "Invalid pubsub topic. Pubsub topics must be in the format /waku/2/rs/<cluster-id>/<shard-id>",
    )
  return ProtectedShard(shard: shard.shardId, key: publicKey.get())

proc completeCmdArg*(T: type ProtectedShard, val: string): seq[string] =
  return @[]

proc completeCmdArg*(T: type IpAddress, val: string): seq[string] =
  return @[]

proc defaultListenAddress*(): IpAddress =
  # TODO: How should we select between IPv4 and IPv6
  # Maybe there should be a config option for this.
  (static parseIpAddress("0.0.0.0"))

proc defaultColocationLimit*(): int =
  return DefaultColocationLimit

proc completeCmdArg*(T: type Port, val: string): seq[string] =
  return @[]

proc completeCmdArg*(T: type EthRpcUrl, val: string): seq[string] =
  return @[]

proc parseCmdArg*(T: type EthRpcUrl, s: string): T =
  ## allowed patterns:
  ## http://url:port
  ## https://url:port
  ## http://url:port/path
  ## https://url:port/path
  ## http://url/with/path
  ## http://url:port/path?query
  ## https://url:port/path?query
  ## disallowed patterns:
  ## any valid/invalid ws or wss url
  var httpPattern =
    re2"^(https?):\/\/([\w-]+(\.[\w-]+)*)(:[0-9]{1,5})?(\/[\w.,@?^=%&:\/~+#-]*)?$"
  var wsPattern =
    re2"^(wss?):\/\/([\w-]+(\.[\w-]+)+)(:[0-9]{1,5})?(\/[\w.,@?^=%&:\/~+#-]*)?$"
  if regex.match(s, wsPattern):
    raise newException(
      ValueError, "Websocket RPC URL is not supported, Please use an HTTP URL"
    )
  if not regex.match(s, httpPattern):
    raise newException(ValueError, "Invalid HTTP RPC URL")
  return EthRpcUrl(s)

## Load

proc readValue*(
    r: var TomlReader, value: var crypto.PrivateKey
) {.raises: [SerializationError].} =
  try:
    value = parseCmdArg(crypto.PrivateKey, r.readValue(string))
  except CatchableError:
    raise newException(SerializationError, getCurrentExceptionMsg())

proc readValue*(
    r: var EnvvarReader, value: var crypto.PrivateKey
) {.raises: [SerializationError].} =
  try:
    value = parseCmdArg(crypto.PrivateKey, r.readValue(string))
  except CatchableError:
    raise newException(SerializationError, getCurrentExceptionMsg())

proc readValue*(
    r: var TomlReader, value: var ProtectedShard
) {.raises: [SerializationError].} =
  try:
    value = parseCmdArg(ProtectedShard, r.readValue(string))
  except CatchableError:
    raise newException(SerializationError, getCurrentExceptionMsg())

proc readValue*(
    r: var EnvvarReader, value: var ProtectedShard
) {.raises: [SerializationError].} =
  try:
    value = parseCmdArg(ProtectedShard, r.readValue(string))
  except CatchableError:
    raise newException(SerializationError, getCurrentExceptionMsg())

proc readValue*(
    r: var TomlReader, value: var EthRpcUrl
) {.raises: [SerializationError].} =
  try:
    value = parseCmdArg(EthRpcUrl, r.readValue(string))
  except CatchableError:
    raise newException(SerializationError, getCurrentExceptionMsg())

proc readValue*(
    r: var EnvvarReader, value: var EthRpcUrl
) {.raises: [SerializationError].} =
  try:
    value = parseCmdArg(EthRpcUrl, r.readValue(string))
  except CatchableError:
    raise newException(SerializationError, getCurrentExceptionMsg())

proc load*(T: type WakuNodeConf, version = ""): ConfResult[T] =
  try:
    let conf = WakuNodeConf.load(
      version = version,
      secondarySources = proc(
          conf: WakuNodeConf, sources: auto
      ) {.gcsafe, raises: [ConfigurationError].} =
        sources.addConfigFile(Envvar, InputFile("wakunode2"))

        if conf.configFile.isSome():
          sources.addConfigFile(Toml, conf.configFile.get())
      ,
    )

    ok(conf)
  except CatchableError:
    err(getCurrentExceptionMsg())

proc defaultWakuNodeConf*(): ConfResult[WakuNodeConf] =
  try:
    let conf = WakuNodeConf.load(version = "", cmdLine = @[])
    return ok(conf)
  except CatchableError:
    return err("exception in defaultWakuNodeConf: " & getCurrentExceptionMsg())
