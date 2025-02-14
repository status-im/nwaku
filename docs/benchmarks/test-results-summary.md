---
title: Performance Benchmarks and Test Reports
---


## Introduction
This page summarises key performance metrics for nwaku and provides links to detailed test reports.

> ## TL;DR
>
> - Average Waku bandwidth usage: ~**10 KB/s** (minus discv5 Discovery) for 1KB message size and message injection rate of 1msg/s.  
Confirmed for topologies of up to 2000 Relay nodes.
> - Average time for a message to propagate to 100% of nodes: **0.4s** for topologies of up to 2000 Relay nodes.
> - Average per-node bandwidth usage of the discv5 protocol: **8 KB/s** for incoming traffic and **7.4 KB/s** for outgoing traffic,   
 in a network with 100 continuously online nodes.
> - Relevancy to Status App: **TODO**


## Insights

### Relay Bandwidth Usage: nwaku v0.34.0
The average per-node `libp2p` bandwidth usage in a 1000-node Relay network with 1KB messages at varying injection rates.


| Message Injection Rate | Average libp2p incoming bandwidth (KB/s) | Average libp2p outgoing bandwidth (KB/s) |
|------------------------|------------------------------------------|------------------------------------------|
| 1 msg/s                | ~10.1                                    | ~10.3                                    |
| 1 msg/10s              | ~1.8                                     | ~1.9                                     |

### Message Propagation Latency: nwaku v0.34.0-rc1
The message propagation latency is measured as the total time for a message to reach all nodes.  
We compare the latency in different network configurations for the following simulation parameters:
- Total messages published: 600
- Message size: 1KB
- Message injection rate: 1msg/s

The different network configurations tested are:
- Relay Config: 1000 nodes with relay enabled
- Mixed Config: 210 nodes, consisting of bootstrap nodes, filter clients and servers, lightpush clients and servers, store nodes
- Non-persistent Relay Config: 500 persistent relay nodes, 10 store nodes and 100 non-persistent relay nodes

Click on a specific config to see the detailed test report.

| Config                                                                                                                       | Average Message Propagation Latency (s) | Max Message Propagation Latency (s)|
|------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|------------------------------------|
| [Relay](https://www.notion.so/Waku-regression-testing-v0-34-1618f96fb65c803bb7bad6ecd6bafff9) (1000 nodes)                   | 0.05                                    | 1.6                                |
| [Mixed](https://www.notion.so/Mixed-environment-analysis-1688f96fb65c809eb235c59b97d6e15b) (210 nodes)                       | 0.0125                                  | 0.007                              |
| [Non-persistent Relay](https://www.notion.so/High-Churn-Relay-Store-Reliability-16c8f96fb65c8008bacaf5e86881160c) (510 nodes)| 0.0125                                  | 0.25                               |  

### Discv5 Bandwidth Usage: nwaku v0.34.0
The average bandwidth usage of discv5 for a network of 100 nodes and message injection rate of 0 or 1msg/s.  
The measurements are based on a stable network where all nodes have already connected to peers to form a healthy mesh.

|Message size         |Average discv5 incoming bandwidth (KB/s)|Average discv5 outgoing bandwidth (KB/s)|
|-------------------- |----------------------------------------|----------------------------------------|
| no message injection| 7.88                                   | 6.70                                   |
| 1KB                 | 8.04                                   | 7.40                                   |
| 10KB                | 8.03                                   | 7.45                                   |

## Testing
### DST
The VAC DST team performs regression testing on all new **nwaku** releases, comparing performance with previous versions.  
They simulate large Waku networks with a variety of network and protocol configurations that are representative of real-world usage.

**Test Reports**: [DST Reports](https://www.notion.so/DST-Reports-1228f96fb65c80729cd1d98a7496fe6f)  


### QA
The VAC QA team performs interoperability tests for **nwaku** and **go-waku** using the latest main branch builds.  
These tests run daily and verify protocol functionality by targeting specific features of each protocol.  

**Test Reports**: [QA Reports](https://discord.com/channels/1110799176264056863/1196933819614363678)  

### nwaku
The **nwaku** team follows a structured release procedure for all release candidates.  
This involves deploying RCs to `status.staging` fleet for validation and performing sanity checks.  

**Release Process**: [nwaku Release Procedure](https://github.com/waku-org/nwaku/blob/master/.github/ISSUE_TEMPLATE/prepare_release.md)  


### Research
The Waku Research team conducts a variety of benchmarking, performance testing, proof-of-concept validations and debugging efforts.  
They also maintain a Waku simulator designed for small-scale, single-purpose, on-demand testing.


**Test Reports**: [Waku Research Reports](https://www.notion.so/Miscellaneous-2c02516248db4a28ba8cb2797a40d1bb)

**Waku Simulator**: [Waku Simulator Book](https://waku-org.github.io/waku-simulator/)
