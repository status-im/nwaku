{.push raises: [].}

import waku/[common/logging, factory/[waku, networks_config, external_config]]
import
  std/[options, strutils, os, sequtils],
  stew/shims/net as stewNet,
  chronicles,
  chronos,
  metrics,
  libbacktrace,
  libp2p/crypto/crypto

export
  networks_config, waku, logging, options, strutils, os, sequtils, stewNet, chronicles,
  chronos, metrics, libbacktrace, crypto

proc setup*(): Waku =
  const versionString = "version / git commit hash: " & waku.git_version
  let rng = crypto.newRng()

  let confRes = WakuNodeConf.load(version = versionString)
  if confRes.isErr():
    error "failure while loading the configuration", error = $confRes.error
    quit(QuitFailure)

  var conf = confRes.get()

  let twnClusterConf = ClusterConf.TheWakuNetworkConf()

  # Override configuration
  conf.maxMessageSize = twnClusterConf.maxMessageSize
  conf.clusterId = twnClusterConf.clusterId
  conf.rlnRelayEthContractAddress = twnClusterConf.rlnRelayEthContractAddress
  conf.rlnRelayDynamic = twnClusterConf.rlnRelayDynamic
  conf.rlnRelayBandwidthThreshold = twnClusterConf.rlnRelayBandwidthThreshold
  conf.discv5Discovery = twnClusterConf.discv5Discovery
  conf.discv5BootstrapNodes =
    conf.discv5BootstrapNodes & twnClusterConf.discv5BootstrapNodes
  conf.rlnEpochSizeSec = twnClusterConf.rlnEpochSizeSec
  conf.rlnRelayUserMessageLimit = twnClusterConf.rlnRelayUserMessageLimit

  # Only set rlnRelay to true if relay is configured
  if conf.relay:
    conf.rlnRelay = twnClusterConf.rlnRelay

  debug "Starting node"
  var waku = Waku.new(conf).valueOr:
    error "Waku initialization failed", error = error
    quit(QuitFailure)

  (waitFor startWaku(addr waku)).isOkOr:
    error "Starting waku failed", error = error
    quit(QuitFailure)

  # set triggerSelf to false, we don't want to process our own stealthCommitments
  waku.node.wakuRelay.triggerSelf = false

  info "Node setup complete"
  return waku
