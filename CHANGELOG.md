## v0.35.0 (2025-01-31)

### Features

- waku sync shard matching check ([#3259](https://github.com/waku-org/nwaku/issues/3259)) ([42fd6b827](https://github.com/waku-org/nwaku/commit/42fd6b827))
- waku store sync 2.0 config & setup ([#3217](https://github.com/waku-org/nwaku/issues/3217)) ([7f64dc03a](https://github.com/waku-org/nwaku/commit/7f64dc03a))
- waku store sync 2.0 protocols & tests ([#3216](https://github.com/waku-org/nwaku/issues/3216)) ([6ee494d90](https://github.com/waku-org/nwaku/commit/6ee494d90))
- waku store sync 2.0 storage & tests ([#3215](https://github.com/waku-org/nwaku/issues/3215)) ([54a7a6875](https://github.com/waku-org/nwaku/commit/54a7a6875))
- waku store sync 2.0 common types & codec ([#3213](https://github.com/waku-org/nwaku/issues/3213)) ([29fda2dab](https://github.com/waku-org/nwaku/commit/29fda2dab))
- add txhash-based eligibility checks for incentivization PoC ([#3166](https://github.com/waku-org/nwaku/issues/3166)) ([505ec84ce](https://github.com/waku-org/nwaku/commit/505ec84ce))
- connection change event ([#3225](https://github.com/waku-org/nwaku/issues/3225)) ([e81a5517b](https://github.com/waku-org/nwaku/commit/e81a5517b))
- libwaku add protected topic ([#3211](https://github.com/waku-org/nwaku/issues/3211)) ([d932dd10c](https://github.com/waku-org/nwaku/commit/d932dd10c))
- topic health tracking ([#3212](https://github.com/waku-org/nwaku/issues/3212)) ([6020a673b](https://github.com/waku-org/nwaku/commit/6020a673b))
- allowing configuration of application level callbacks ([#3206](https://github.com/waku-org/nwaku/issues/3206)) ([049fbeabb](https://github.com/waku-org/nwaku/commit/049fbeabb))
- waku rendezvous wrapper ([#2962](https://github.com/waku-org/nwaku/issues/2962)) ([650a9487e](https://github.com/waku-org/nwaku/commit/650a9487e))
- making dns discovery async ([#3175](https://github.com/waku-org/nwaku/issues/3175)) ([d7d00bfd7](https://github.com/waku-org/nwaku/commit/d7d00bfd7))
- remove Waku Sync 1.0 & Negentropy ([#3185](https://github.com/waku-org/nwaku/issues/3185)) ([2ab9c3d36](https://github.com/waku-org/nwaku/commit/2ab9c3d36))
- add waku_dial_peer and get_connected_peers to libwaku ([#3149](https://github.com/waku-org/nwaku/issues/3149)) ([507b1fc4d](https://github.com/waku-org/nwaku/commit/507b1fc4d))
- running periodicaly peer exchange if discv5 is disabled ([#3150](https://github.com/waku-org/nwaku/issues/3150)) ([400d7a54f](https://github.com/waku-org/nwaku/commit/400d7a54f))

### Bug Fixes

- filter enhancements in subscription management ([#3198](https://github.com/waku-org/nwaku/issues/3198)) ([287e9b12c](https://github.com/waku-org/nwaku/commit/287e9b12c))
- avoid double db migration for sqlite ([#3244](https://github.com/waku-org/nwaku/issues/3244)) ([2ce245354](https://github.com/waku-org/nwaku/commit/2ce245354))
- libwaku waku_relay_unsubscribe ([#3207](https://github.com/waku-org/nwaku/issues/3207)) ([ab0c1d4aa](https://github.com/waku-org/nwaku/commit/ab0c1d4aa))
- libwaku support string and int64 for timestamps ([#3205](https://github.com/waku-org/nwaku/issues/3205)) ([2022f54f5](https://github.com/waku-org/nwaku/commit/2022f54f5))
- lite-protocol-tester receiver exit check ([#3187](https://github.com/waku-org/nwaku/issues/3187)) ([beb21c78f](https://github.com/waku-org/nwaku/commit/beb21c78f))
- linting error ([#3156](https://github.com/waku-org/nwaku/issues/3156)) ([99ac68447](https://github.com/waku-org/nwaku/commit/99ac68447))

### Changes

- filter remove all subscription from a peer that is leaving ([#3267](https://github.com/waku-org/nwaku/issues/3267)) ([46747fd49](https://github.com/waku-org/nwaku/commit/46747fd49))
- send msg hash as string on libwaku message event ([#3234](https://github.com/waku-org/nwaku/issues/3234)) ([9c209b4c3](https://github.com/waku-org/nwaku/commit/9c209b4c3))
- compile with skipParentCfg ([#3262](https://github.com/waku-org/nwaku/issues/3262)) ([aaf3c0070](https://github.com/waku-org/nwaku/commit/aaf3c0070))
- bump dependencies for v0.35 ([#3255](https://github.com/waku-org/nwaku/issues/3255)) ([f8a233381](https://github.com/waku-org/nwaku/commit/f8a233381))
- waku_archive add protection against queries longer than 24h ([#3256](https://github.com/waku-org/nwaku/issues/3256)) ([a4d71718a](https://github.com/waku-org/nwaku/commit/a4d71718a))
- separate heaptrack from debug build ([#3249](https://github.com/waku-org/nwaku/issues/3249)) ([81f24cc25](https://github.com/waku-org/nwaku/commit/81f24cc25))
- adding debug flag to makefile ([#3248](https://github.com/waku-org/nwaku/issues/3248)) ([dfd06fa72](https://github.com/waku-org/nwaku/commit/dfd06fa72))
- capping mechanism for relay and service connections ([#3184](https://github.com/waku-org/nwaku/issues/3184)) ([2942782f9](https://github.com/waku-org/nwaku/commit/2942782f9))
- adding new job in the CI.yml file ([#3193](https://github.com/waku-org/nwaku/issues/3193)) ([6d167dd2e](https://github.com/waku-org/nwaku/commit/6d167dd2e))
- add extra migration to sqlite and improving error message ([#3240](https://github.com/waku-org/nwaku/issues/3240)) ([bfd60ceab](https://github.com/waku-org/nwaku/commit/bfd60ceab))
- optimize libwaku size ([#3242](https://github.com/waku-org/nwaku/issues/3242)) ([9c0ad8517](https://github.com/waku-org/nwaku/commit/9c0ad8517))
- golang example end using negentropy dependency plus simple readme.md ([#3235](https://github.com/waku-org/nwaku/issues/3235)) ([0e0fcfb1a](https://github.com/waku-org/nwaku/commit/0e0fcfb1a))
- libwaku tweaks ([#3233](https://github.com/waku-org/nwaku/issues/3233)) ([625c8ee51](https://github.com/waku-org/nwaku/commit/625c8ee51))
- add comment to clarify where contract.nim comes from ([#3226](https://github.com/waku-org/nwaku/issues/3226)) ([b956d0b40](https://github.com/waku-org/nwaku/commit/b956d0b40))
- enhance libwaku store protocol and more ([#3223](https://github.com/waku-org/nwaku/issues/3223)) ([22ce9ee87](https://github.com/waku-org/nwaku/commit/22ce9ee87))
- Bump nimbus and nim to latest available - nim-2.0.12 ([#3188](https://github.com/waku-org/nwaku/issues/3188)) ([e2b7149f8](https://github.com/waku-org/nwaku/commit/e2b7149f8))
- add two metrics and panel ([#3181](https://github.com/waku-org/nwaku/issues/3181)) ([1b532e8ab](https://github.com/waku-org/nwaku/commit/1b532e8ab))
- libwaku async ([#3180](https://github.com/waku-org/nwaku/issues/3180)) ([47a623541](https://github.com/waku-org/nwaku/commit/47a623541))
- Filter in libwaku ([#3177](https://github.com/waku-org/nwaku/issues/3177)) ([f856298ca](https://github.com/waku-org/nwaku/commit/f856298ca))
- add supervisor for lite-protocol-tester infra ([#3176](https://github.com/waku-org/nwaku/issues/3176)) ([a7264d68c](https://github.com/waku-org/nwaku/commit/a7264d68c))
- flaky rln test ([#3173](https://github.com/waku-org/nwaku/issues/3173)) ([bebd3dd62](https://github.com/waku-org/nwaku/commit/bebd3dd62))
- Partial version bumps for v0.34.0-rc.1 ([#3172](https://github.com/waku-org/nwaku/issues/3172)) ([8368ff006](https://github.com/waku-org/nwaku/commit/8368ff006))
- libwaku better error handling and better waku thread destroy handling ([#3167](https://github.com/waku-org/nwaku/issues/3167)) ([294dd03c4](https://github.com/waku-org/nwaku/commit/294dd03c4))
- libwaku allow several multiaddresses for a single peer in store queries ([#3171](https://github.com/waku-org/nwaku/issues/3171)) ([3cb8ebdd8](https://github.com/waku-org/nwaku/commit/3cb8ebdd8))
- remove protected-topic cli flg ([#3160](https://github.com/waku-org/nwaku/issues/3160)) ([8fde70937](https://github.com/waku-org/nwaku/commit/8fde70937))
- naming connectPeer procedure ([#3157](https://github.com/waku-org/nwaku/issues/3157)) ([b3656d6ee](https://github.com/waku-org/nwaku/commit/b3656d6ee))

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |
| [`66/WAKU2-METADATA`](https://github.com/waku-org/specs/blob/master/standards/core/metadata.md) | `raw` | `/vac/waku/metadata/1.0.0` |
| [`WAKU-SYNC`](https://github.com/waku-org/specs/blob/master/standards/core/sync.md) | `draft` | `/vac/waku/reconciliation/1.0.0` & `/vac/waku/transfer/1.0.0` |

## v0.34.0 (2024-10-29)

### Notes:

* The `--protected-topic` CLI configuration has been removed. Equivalent flag, `--protected-shard`, shall be used instead.

### Features

- change latency buckets ([#3153](https://github.com/waku-org/nwaku/issues/3153)) ([956fde6e](https://github.com/waku-org/nwaku/commit/956fde6e))
- libwaku: ping peer ([#3144](https://github.com/waku-org/nwaku/issues/3144)) ([de11e576](https://github.com/waku-org/nwaku/commit/de11e576))
- initial windows support ([#3107](https://github.com/waku-org/nwaku/issues/3107)) ([ff21c01e](https://github.com/waku-org/nwaku/commit/ff21c01e))
- circuit relay support ([#3112](https://github.com/waku-org/nwaku/issues/3112)) ([cfde7eea](https://github.com/waku-org/nwaku/commit/cfde7eea))

### Bug Fixes

- peer exchange libwaku response handling ([#3141](https://github.com/waku-org/nwaku/issues/3141)) ([76606421](https://github.com/waku-org/nwaku/commit/76606421))
- add more logs, stagger intervals & set prune offset to 10% for waku sync ([#3142](https://github.com/waku-org/nwaku/issues/3142)) ([a386880b](https://github.com/waku-org/nwaku/commit/a386880b))
- add log and archive message ingress for sync ([#3133](https://github.com/waku-org/nwaku/issues/3133)) ([80c7581a](https://github.com/waku-org/nwaku/commit/80c7581a))
- add a limit of max 10 content topics per query ([#3117](https://github.com/waku-org/nwaku/issues/3117)) ([c35dc549](https://github.com/waku-org/nwaku/commit/c35dc549))
- avoid segfault by setting a default num peers requested in Peer eXchange ([#3122](https://github.com/waku-org/nwaku/issues/3122)) ([82fd5dde](https://github.com/waku-org/nwaku/commit/82fd5dde))
- returning peerIds in base 64 ([#3105](https://github.com/waku-org/nwaku/issues/3105)) ([37edaf62](https://github.com/waku-org/nwaku/commit/37edaf62))
- changing libwaku's error handling format ([#3093](https://github.com/waku-org/nwaku/issues/3093)) ([2e6c299d](https://github.com/waku-org/nwaku/commit/2e6c299d))
- remove spammy log ([#3091](https://github.com/waku-org/nwaku/issues/3091)) ([1d2b910f](https://github.com/waku-org/nwaku/commit/1d2b910f))
- avoid out connections leak ([#3077](https://github.com/waku-org/nwaku/issues/3077)) ([eb2bbae6](https://github.com/waku-org/nwaku/commit/eb2bbae6))
- rejecting excess relay connections ([#3065](https://github.com/waku-org/nwaku/issues/3065)) ([8b0884c7](https://github.com/waku-org/nwaku/commit/8b0884c7))
- static linking negentropy in ARM based mac ([#3046](https://github.com/waku-org/nwaku/issues/3046)) ([256b7853](https://github.com/waku-org/nwaku/commit/256b7853))

### Changes

- support ping with multiple multiaddresses and close stream ([#3154](https://github.com/waku-org/nwaku/issues/3154)) ([3665991a](https://github.com/waku-org/nwaku/commit/3665991a))
- liteprotocoltester: easy setup fleets ([#3125](https://github.com/waku-org/nwaku/issues/3125)) ([268e7e66](https://github.com/waku-org/nwaku/commit/268e7e66))
- saving peers enr capabilities ([#3127](https://github.com/waku-org/nwaku/issues/3127)) ([69d9524f](https://github.com/waku-org/nwaku/commit/69d9524f))
- networkmonitor: add missing field on RlnRelay init, set default for num of shard ([#3136](https://github.com/waku-org/nwaku/issues/3136)) ([edcb0e15](https://github.com/waku-org/nwaku/commit/edcb0e15))
- add to libwaku peer id retrieval proc ([#3124](https://github.com/waku-org/nwaku/issues/3124)) ([c5a825e2](https://github.com/waku-org/nwaku/commit/c5a825e2))
- adding to libwaku dial and disconnect by peerIds ([#3111](https://github.com/waku-org/nwaku/issues/3111)) ([25da8102](https://github.com/waku-org/nwaku/commit/25da8102))
- dbconn: add requestId info as a comment in the database logs ([#3110](https://github.com/waku-org/nwaku/issues/3110)) ([30c072a4](https://github.com/waku-org/nwaku/commit/30c072a4))
- improving get_peer_ids_by_protocol by returning the available protocols of connected peers ([#3109](https://github.com/waku-org/nwaku/issues/3109)) ([ed0ee5be](https://github.com/waku-org/nwaku/commit/ed0ee5be))
- remove warnings ([#3106](https://github.com/waku-org/nwaku/issues/3106)) ([c861fa9f](https://github.com/waku-org/nwaku/commit/c861fa9f))
- better store logs ([#3103](https://github.com/waku-org/nwaku/issues/3103)) ([21b03551](https://github.com/waku-org/nwaku/commit/21b03551))
- Improve binding for waku_sync ([#3102](https://github.com/waku-org/nwaku/issues/3102)) ([c3756e3a](https://github.com/waku-org/nwaku/commit/c3756e3a))
- improving and temporarily skipping flaky rln test ([#3094](https://github.com/waku-org/nwaku/issues/3094)) ([a6ed80a5](https://github.com/waku-org/nwaku/commit/a6ed80a5))
- update master after release v0.33.1 ([#3089](https://github.com/waku-org/nwaku/issues/3089)) ([54c3083d](https://github.com/waku-org/nwaku/commit/54c3083d))
- re-arrange function based on responsibility of peer-manager ([#3086](https://github.com/waku-org/nwaku/issues/3086)) ([0f8e8740](https://github.com/waku-org/nwaku/commit/0f8e8740))
- waku_keystore: give some more context in case of error ([#3064](https://github.com/waku-org/nwaku/issues/3064)) ([3ad613ca](https://github.com/waku-org/nwaku/commit/3ad613ca))
- bump negentropy ([#3078](https://github.com/waku-org/nwaku/issues/3078)) ([643ab20f](https://github.com/waku-org/nwaku/commit/643ab20f))
- Optimize store ([#3061](https://github.com/waku-org/nwaku/issues/3061)) ([5875ed63](https://github.com/waku-org/nwaku/commit/5875ed63))
- wrap peer store  ([#3051](https://github.com/waku-org/nwaku/issues/3051)) ([729e63f5](https://github.com/waku-org/nwaku/commit/729e63f5))
- disabling metrics for libwaku ([#3058](https://github.com/waku-org/nwaku/issues/3058)) ([b358c90f](https://github.com/waku-org/nwaku/commit/b358c90f))
- test peer connection management ([#3049](https://github.com/waku-org/nwaku/issues/3049)) ([711e7db1](https://github.com/waku-org/nwaku/commit/711e7db1))
- updating upload and download artifact actions to v4 ([#3047](https://github.com/waku-org/nwaku/issues/3047)) ([7c4a9717](https://github.com/waku-org/nwaku/commit/7c4a9717))
- Better database query logs and logarithmic scale in grafana store panels ([#3048](https://github.com/waku-org/nwaku/issues/3048)) ([d68b06f1](https://github.com/waku-org/nwaku/commit/d68b06f1))
- extending store metrics ([#3042](https://github.com/waku-org/nwaku/issues/3042)) ([fd83b42f](https://github.com/waku-org/nwaku/commit/fd83b42f))

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |
| [`66/WAKU2-METADATA`](https://github.com/waku-org/specs/blob/master/standards/core/metadata.md) | `raw` | `/vac/waku/metadata/1.0.0` |
| [`WAKU-SYNC`](https://github.com/waku-org/specs/blob/master/standards/core/sync.md) | `draft` | `/vac/waku/sync/1.0.0` |

## v0.33.1 (2024-10-03)

### Bug fixes

* Fix out connections leak ([3077](https://github.com/waku-org/nwaku/pull/3077)) ([eb2bbae6](https://github.com/waku-org/nwaku/commit/eb2bbae6))

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |
| [`66/WAKU2-METADATA`](https://github.com/waku-org/specs/blob/master/standards/core/metadata.md) | `raw` | `/vac/waku/metadata/1.0.0` |
| [`WAKU-SYNC`](https://github.com/waku-org/specs/blob/feat--waku-sync/standards/core/sync.md) | `draft` | `/vac/waku/sync/1.0.0` |

## v0.33.0 (2024-09-30)

#### Notes:

* The `--pubsub-topic` CLI configuration has been deprecated and support for it will be removed on release v0.35.0. In order to migrate, please use the `--shard` configuration instead. For example, instead of `--pubsub-topic=/waku/2/rs/<CLUSTER_ID>/<SHARD_ID>`, use `--cluster-id=<CLUSTER_ID>` once and `--shard=<SHARD_ID>` for each subscribed shard
* The `--rest-private` CLI configuration has been removed. Please delete any reference to it when running your nodes
* Introduced the `--reliability` CLI configuration, activating the new experimental StoreV3 message confirmation protocol
* DOS protection configurations of non-relay, req/resp protocols are changed
  * `--request-rate-limit` and `--request-rate-period` options are no longer supported.
  * `--rate-limit` CLI configuration is now available.
    - The new flag can describe various rate-limit requirements for each protocol supported. The setting can be repeated, each instance can define exactly one rate-limit option.
    - Format is `<protocol>:volume/period<time-unit>`
    - If protocol is not given, settings will be taken as default for un-set protocols. Ex: 80/2s
    - Supported protocols are: lightpush|filter|px|store|storev2|storev3
    - `volume` must be an integer value, representing number of requests over the period of time allowed.
    - `period <time-unit>` must be an integer with defined unit as one of h|m|s|ms
    - If not set, no rate limit will be applied to request/response protocols, except for the filter protocol.


### Release highlights

* a new experimental reliability protocol has been implemented, leveraging StoreV3 to confirm message delivery
* Peer Exchange protocol can now be protected by rate-limit boundary checks.
* Fine-grained configuration of DOS protection is available with this release. See, "Notes" above.

### Bug Fixes

- rejecting excess relay connections ([#3063](https://github.com/waku-org/nwaku/issues/3063)) ([8b0884c7](https://github.com/waku-org/nwaku/commit/8b0884c7))
- make Peer Exchange's rpc status_code optional for backward compatibility ([#3059](https://github.com/waku-org/nwaku/pull/3059)) ([5afa9b13](https://github.com/waku-org/nwaku/commit/5afa9b13))
- px protocol decode - do not treat missing response field as error ([#3054](https://github.com/waku-org/nwaku/issues/3054)) ([9b445ac4](https://github.com/waku-org/nwaku/commit/9b445ac4))
- setting up node with modified config ([#3036](https://github.com/waku-org/nwaku/issues/3036)) ([8f289925](https://github.com/waku-org/nwaku/commit/8f289925))
- get back health check for postgres legacy ([#3010](https://github.com/waku-org/nwaku/issues/3010)) ([5a0edff7](https://github.com/waku-org/nwaku/commit/5a0edff7))
- libnegentropy integration ([#2996](https://github.com/waku-org/nwaku/issues/2996)) ([c3cb06ac](https://github.com/waku-org/nwaku/commit/c3cb06ac))
- peer-exchange issue ([#2889](https://github.com/waku-org/nwaku/issues/2889)) ([43157102](https://github.com/waku-org/nwaku/commit/43157102))

### Changes

- append current version in agentString which is used by the identify protocol ([#3057](https://github.com/waku-org/nwaku/pull/3057)) ([368bb3c1](https://github.com/waku-org/nwaku/commit/368bb3c1))
- rate limit peer exchange protocol, enhanced response status in RPC ([#3035](https://github.com/waku-org/nwaku/issues/3035)) ([0a7f16a3](https://github.com/waku-org/nwaku/commit/0a7f16a3))
- Switch libnegentropy library build from shared to static linkage ([#3041](https://github.com/waku-org/nwaku/issues/3041)) ([83f25c3e](https://github.com/waku-org/nwaku/commit/83f25c3e))
- libwaku reduce repetitive code by adding a template handling resp returns ([#3032](https://github.com/waku-org/nwaku/issues/3032)) ([1713f562](https://github.com/waku-org/nwaku/commit/1713f562))
- libwaku - extending the library with peer_manager and peer_exchange features ([#3026](https://github.com/waku-org/nwaku/issues/3026)) ([5ea1cf0c](https://github.com/waku-org/nwaku/commit/5ea1cf0c))
- use submodule nph in CI to check lint ([#3027](https://github.com/waku-org/nwaku/issues/3027)) ([ce9a8c46](https://github.com/waku-org/nwaku/commit/ce9a8c46))
- deprecating pubsub topic ([#2997](https://github.com/waku-org/nwaku/issues/2997)) ([a3cd2a1a](https://github.com/waku-org/nwaku/commit/a3cd2a1a))
- lightpush - error metric less variable by only setting a fixed string ([#3020](https://github.com/waku-org/nwaku/issues/3020)) ([d3e6717a](https://github.com/waku-org/nwaku/commit/d3e6717a))
- enhance libpq management ([#3015](https://github.com/waku-org/nwaku/issues/3015)) ([45319f09](https://github.com/waku-org/nwaku/commit/45319f09))
- per limit split of PostgreSQL queries ([#3008](https://github.com/waku-org/nwaku/issues/3008)) ([e1e05afb](https://github.com/waku-org/nwaku/commit/e1e05afb))
- Added metrics to liteprotocoltester ([#3002](https://github.com/waku-org/nwaku/issues/3002)) ([8baf627f](https://github.com/waku-org/nwaku/commit/8baf627f))
- extending store metrics ([#2995](https://github.com/waku-org/nwaku/issues/2995)) ([fd83b42f](https://github.com/waku-org/nwaku/commit/fd83b42f))
- Better timing and requestId detail for slower store db queries  ([#2994](https://github.com/waku-org/nwaku/issues/2994)) ([e8a49b76](https://github.com/waku-org/nwaku/commit/e8a49b76))
- remove unused setting from external_config.nim ([#3004](https://github.com/waku-org/nwaku/issues/3004)) ([fd84363e](https://github.com/waku-org/nwaku/commit/fd84363e))
- delivery monitor for store v3 reliability protocol ([#2977](https://github.com/waku-org/nwaku/issues/2977)) ([0f68274c](https://github.com/waku-org/nwaku/commit/0f68274c))

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |
| [`66/WAKU2-METADATA`](https://github.com/waku-org/specs/blob/master/standards/core/metadata.md) | `raw` | `/vac/waku/metadata/1.0.0` |
| [`WAKU-SYNC`](https://github.com/waku-org/specs/blob/feat--waku-sync/standards/core/sync.md) | `draft` | `/vac/waku/sync/1.0.0` |

## v0.32.0 (2024-08-30)

#### Notes:

* A new `discv5-only` CLI flag was introduced, which if set to true will perform optimizations for nodes that only run the DiscV5 service
* The `protected-topic` CLI config item has been deprecated in favor of the new `protected-shard` configuration. Protected topics are still supported and will be completely removed in two releases time for `v0.34.0`

### Release highlights

* Merged Nwaku Sync protocol for synchronizing store nodes
* Added Store Resume mechanism to retrieve messages sent when the node was offline

### Features

- Nwaku Sync ([#2403](https://github.com/waku-org/nwaku/issues/2403)) ([2cc86c51](https://github.com/waku-org/nwaku/commit/2cc86c51))
- misc. updates for discovery network analysis ([#2930](https://github.com/waku-org/nwaku/issues/2930)) ([4340eb75](https://github.com/waku-org/nwaku/commit/4340eb75))
- store resume ([#2919](https://github.com/waku-org/nwaku/issues/2919)) ([aed2a113](https://github.com/waku-org/nwaku/commit/aed2a113))

### Bug Fixes

- return on insert error ([#2956](https://github.com/waku-org/nwaku/issues/2956)) ([5f0fbd78](https://github.com/waku-org/nwaku/commit/5f0fbd78))
- network monitor improvements ([#2939](https://github.com/waku-org/nwaku/issues/2939)) ([80583237](https://github.com/waku-org/nwaku/commit/80583237))
- add back waku discv5 metrics ([#2927](https://github.com/waku-org/nwaku/issues/2927)) ([e4e01fab](https://github.com/waku-org/nwaku/commit/e4e01fab))
- update and shift unittest ([#2934](https://github.com/waku-org/nwaku/issues/2934)) ([08973add](https://github.com/waku-org/nwaku/commit/08973add))
- handle rln-relay-message-limit  ([#2867](https://github.com/waku-org/nwaku/issues/2867)) ([8d107b0d](https://github.com/waku-org/nwaku/commit/8d107b0d))

### Changes

- libwaku retrieve my enr and adapt golang example ([#2987](https://github.com/waku-org/nwaku/issues/2987)) ([1ff9f1dd](https://github.com/waku-org/nwaku/commit/1ff9f1dd))
- run `ANALYZE messages` regularly for better db performance ([#2986](https://github.com/waku-org/nwaku/issues/2986)) ([32f2d85d](https://github.com/waku-org/nwaku/commit/32f2d85d))
- liteprotocoltester for simulation and for fleets ([#2813](https://github.com/waku-org/nwaku/issues/2813)) ([f4fa73e9](https://github.com/waku-org/nwaku/commit/f4fa73e9))
- lock in nph version and add pre-commit hook ([#2938](https://github.com/waku-org/nwaku/issues/2938)) ([d63e3430](https://github.com/waku-org/nwaku/commit/d63e3430))
- logging received message info via onValidated observer ([#2973](https://github.com/waku-org/nwaku/issues/2973)) ([e8bce67d](https://github.com/waku-org/nwaku/commit/e8bce67d))
- deprecating protected topics in favor of protected shards ([#2983](https://github.com/waku-org/nwaku/issues/2983)) ([e51ffe07](https://github.com/waku-org/nwaku/commit/e51ffe07))
- rename NsPubsubTopic ([#2974](https://github.com/waku-org/nwaku/issues/2974)) ([67439057](https://github.com/waku-org/nwaku/commit/67439057))
- install dig ([#2975](https://github.com/waku-org/nwaku/issues/2975)) ([d24b56b9](https://github.com/waku-org/nwaku/commit/d24b56b9))
- print WakuMessageHash as hex strings ([#2969](https://github.com/waku-org/nwaku/issues/2969)) ([2fd4eb62](https://github.com/waku-org/nwaku/commit/2fd4eb62))
- updating dependencies for release 0.32.0 ([#2971](https://github.com/waku-org/nwaku/issues/2971)) ([dfd42a7c](https://github.com/waku-org/nwaku/commit/dfd42a7c))
- bump negentropy to latest master ([#2968](https://github.com/waku-org/nwaku/issues/2968)) ([b36cb075](https://github.com/waku-org/nwaku/commit/b36cb075))
- keystore: verbose error message when credential is not found ([#2943](https://github.com/waku-org/nwaku/issues/2943)) ([0f11ee14](https://github.com/waku-org/nwaku/commit/0f11ee14))
- upgrade peer exchange mounting  ([#2953](https://github.com/waku-org/nwaku/issues/2953)) ([42f1bed0](https://github.com/waku-org/nwaku/commit/42f1bed0))
- replace statusim.net instances with status.im ([#2941](https://github.com/waku-org/nwaku/issues/2941)) ([f534549a](https://github.com/waku-org/nwaku/commit/f534549a))
- updating doc reference to https rpc ([#2937](https://github.com/waku-org/nwaku/issues/2937)) ([bb7bba35](https://github.com/waku-org/nwaku/commit/bb7bba35))
- Simplification of store legacy code ([#2931](https://github.com/waku-org/nwaku/issues/2931)) ([d4e8a0da](https://github.com/waku-org/nwaku/commit/d4e8a0da))
- add peer filtering by cluster for waku peer exchange ([#2932](https://github.com/waku-org/nwaku/issues/2932)) ([b4618f98](https://github.com/waku-org/nwaku/commit/b4618f98))
- return all connected peers from REST API ([#2923](https://github.com/waku-org/nwaku/issues/2923)) ([a29eca77](https://github.com/waku-org/nwaku/commit/a29eca77))
- adding lint job to the CI ([#2925](https://github.com/waku-org/nwaku/issues/2925)) ([086cc8ed](https://github.com/waku-org/nwaku/commit/086cc8ed))
- improve sonda dashboard ([#2918](https://github.com/waku-org/nwaku/issues/2918)) ([6d385cef](https://github.com/waku-org/nwaku/commit/6d385cef))
- Add new custom built and test target to make in order to enable easy build or test single nim modules ([#2913](https://github.com/waku-org/nwaku/issues/2913)) ([ad25f437](https://github.com/waku-org/nwaku/commit/ad25f437))

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |
| [`66/WAKU2-METADATA`](https://github.com/waku-org/specs/blob/master/standards/core/metadata.md) | `raw` | `/vac/waku/metadata/1.0.0` |
| [`WAKU-SYNC`](https://github.com/waku-org/specs/blob/feat--waku-sync/standards/core/sync.md) | `draft` | `/vac/waku/sync/1.0.0` |

## v0.31.1 (2024-08-02)

### Changes

- Optimize hash queries with lookup table ([#2933](https://github.com/waku-org/nwaku/issues/2933)) ([6463885bf](https://github.com/waku-org/nwaku/commit/6463885bf))

### Bug fixes

* Use of detach finalize when needed [2966](https://github.com/waku-org/nwaku/pull/2966)
* Prevent legacy store from creating new partitions as that approach blocked the database.
[2931](https://github.com/waku-org/nwaku/pull/2931)

* lightpush better feedback in case the lightpush service node does not have peers [2951](https://github.com/waku-org/nwaku/pull/2951)

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`WAKU2-STORE`](https://github.com/waku-org/specs/blob/master/standards/core/store.md) | `draft` | `/vac/waku/store-query/3.0.0` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |
| [`66/WAKU2-METADATA`](https://github.com/waku-org/specs/blob/master/standards/core/metadata.md) | `raw` | `/vac/waku/metadata/1.0.0` |

## v0.31.0 (2024-07-16)
### Notes

* Named sharding has been deprecated in favor of static sharding. Topics in formats other than `/waku/2/rs/<cluster>/<shard>` are no longer supported

### Features

- DOS protection of non relay protocols - rate limit phase3 (#2897) ([ba418ab5b](https://github.com/waku-org/nwaku/commit/ba418ab5b))
- sonda tool (#2893) ([e269dca9c](https://github.com/waku-org/nwaku/commit/e269dca9c))
- add proper per shard bandwidth metric calculation (#2851) ([8f14c0473](https://github.com/waku-org/nwaku/commit/8f14c0473))

### Bug Fixes

- bug(storev3): can't advance cursor [#2745](https://github.com/waku-org/nwaku/issues/2745)
- chore(storev3): only select the messageHash column when using a store query with include_data: false [#2637](https://github.com/waku-org/nwaku/issues/2637)
- rln_keystore_generator improve error handling for unrecoverable failure (#2881) ([1c9eb2741](https://github.com/waku-org/nwaku/commit/1c9eb2741))
- duplicate message forwarding in filter service (#2842) ([99149ea9d](https://github.com/waku-org/nwaku/commit/99149ea9d))
- only set disconnect time on left event (#2831) ([01050138c](https://github.com/waku-org/nwaku/commit/01050138c))
- adding peer exchange peers to the peerStore (#2824) ([325e13169](https://github.com/waku-org/nwaku/commit/325e13169))
- ci use --tags to match non-annotated tags (#2814) ([317c83dc1](https://github.com/waku-org/nwaku/commit/317c83dc1))
- update peers ENRs in peer store in case they are updated (#2818) ([cda18f96c](https://github.com/waku-org/nwaku/commit/cda18f96c))
- mount metadata in wakucanary (#2793) ([3b27aee82](https://github.com/waku-org/nwaku/commit/3b27aee82))

### Changes

- setting filter handling logs to trace (#2914) ([5c539fe13](https://github.com/waku-org/nwaku/commit/5c539fe13))
- enhance postgres and retention policy logs (#2884) ([71ee42de5](https://github.com/waku-org/nwaku/commit/71ee42de5))
- improving logging under debugDiscv5 flag (#2899) ([8578fb0c3](https://github.com/waku-org/nwaku/commit/8578fb0c3))
- archive and drivers refactor (#2761) ([f54ba10bc](https://github.com/waku-org/nwaku/commit/f54ba10bc))
- new release process to include Status fleets (#2825) ([4264666a3](https://github.com/waku-org/nwaku/commit/4264666a3))
- sqlite make sure code is always run (#2891) ([4ac4ab2a4](https://github.com/waku-org/nwaku/commit/4ac4ab2a4))
- deprecating named sharding (#2723) ([e1518cf9f](https://github.com/waku-org/nwaku/commit/e1518cf9f))
- bump dependencies for v0.31.0 (#2885) ([fd6a71cdd](https://github.com/waku-org/nwaku/commit/fd6a71cdd))
- refactor relative path to better absolute (#2861) ([8bfad3ab4](https://github.com/waku-org/nwaku/commit/8bfad3ab4))
- saving agent and protoVersion in peerStore (#2860) ([cae0c7e37](https://github.com/waku-org/nwaku/commit/cae0c7e37))
- unit test for duplicate message push (#2852) ([31c632e42](https://github.com/waku-org/nwaku/commit/31c632e42))
- remove all pre-nim-1.6 deadcode from codebase (#2857) ([9bd8c33ae](https://github.com/waku-org/nwaku/commit/9bd8c33ae))
- nim-chronos bump submodule (#2850) ([092add1ca](https://github.com/waku-org/nwaku/commit/092add1ca))
- ignore arbitrary data stored in `multiaddrs` enr key (#2853) ([76d5b2642](https://github.com/waku-org/nwaku/commit/76d5b2642))
- add origin to peers admin endpoint (#2848) ([7205f95cf](https://github.com/waku-org/nwaku/commit/7205f95cf))
- add discv5 logs (#2811) ([974b8a39a](https://github.com/waku-org/nwaku/commit/974b8a39a))
- archive.nim - increase the max limit of content topics per query to 100 (#2846) ([a05fa0691](https://github.com/waku-org/nwaku/commit/a05fa0691))
- update content-topic parsing for filter (#2835) ([733edae43](https://github.com/waku-org/nwaku/commit/733edae43))
- better descriptive log (#2826) ([94947a850](https://github.com/waku-org/nwaku/commit/94947a850))
- zerokit: bump submodule (#2830) ([c483acee3](https://github.com/waku-org/nwaku/commit/c483acee3))

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`WAKU2-STORE`](https://github.com/waku-org/specs/blob/master/standards/core/store.md) | `draft` | `/vac/waku/store-query/3.0.0` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |
| [`66/WAKU2-METADATA`](https://github.com/waku-org/specs/blob/master/standards/core/metadata.md) | `raw` | `/vac/waku/metadata/1.0.0` |


## v0.30.2 (2024-07-12)

### Release highlights

* RLN message limit to 100 messages per epoch.
* Avoid exclusive access when creating new partitions in the PostgreSQL messages table.

### Changes

- chore(rln): rln message limit to 100 ([#2883](https://github.com/waku-org/nwaku/pull/2883))
- fix: postgres_driver better partition creation without exclusive access [28bdb70b](https://github.com/waku-org/nwaku/commit/28bdb70be46d3fb3a6f992b3f9f2de1defd85a30)

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |
| [`66/WAKU2-METADATA`](https://github.com/waku-org/specs/blob/master/standards/core/metadata.md) | `raw` | `/vac/waku/metadata/1.0.0` |

## v0.30.1 (2024-07-03)

### Notes

* Before upgrading to this version, if you are currently using RLN, make sure to remove your existing `keystore` folder and `rln_tree`
and start your installation from scratch, as
explained in [nwaku-compose](https://github.com/waku-org/nwaku-compose/blob/1b56575df9ddb904af0941a19ea1df3d36bfddfa/README.md).

### Release highlights

* RLN_v2 is used. The maximum rate can be set to `N` messages per epoch, instead of just one message per epoch. See [this](https://github.com/waku-org/nwaku/issues/2345) for more details. Notice that we established an epoch of 10 minutes.


### Changes

- rln-relay: add chain-id flag to wakunode and restrict usage if mismatches rpc provider ([#2858](https://github.com/waku-org/nwaku/pull/2858))
- rln: fix nullifierlog vulnerability ([#2855](https://github.com/waku-org/nwaku/pull/2855))
- chore: add TWN parameters for RLNv2 ([#2843](https://github.com/waku-org/nwaku/pull/2843))
- fix(rln-relay): clear nullifier log only if length is over max epoch gap ([#2836](https://github.com/waku-org/nwaku/pull/2836))
- rlnv2: clean fork of rlnv2 ([#2828](https://github.com/waku-org/nwaku/issues/2828)) ([a02832fe](https://github.com/waku-org/nwaku/commit/a02832fe))
- zerokit: bump submodule ([#2830](https://github.com/waku-org/nwaku/issues/2830)) ([bd064882](https://github.com/waku-org/nwaku/commit/bd064882))

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |
| [`66/WAKU2-METADATA`](https://github.com/waku-org/specs/blob/master/standards/core/metadata.md) | `raw` | `/vac/waku/metadata/1.0.0` |

## v0.29.0 (2024-06-19)

## What's Changed

Notes:

* Named sharding will be deprecated in favor of static sharding. Topics in formats other than `/waku/2/rs/<cluster>/<shard>` will stop being supported starting from `v0.31.0`

Release highlights:

* Android support in libwaku
* Discovery is available in libwaku
* New LiteProcotolTester tool
* RLN proofs as a lightpush service

### Features

- RLN proofs as a lightpush service ([#2768](https://github.com/waku-org/nwaku/issues/2768)) ([0561e5bd](https://github.com/waku-org/nwaku/commit/0561e5bd))
- Push newly released nwaku image with latest-release tag ([#2732](https://github.com/waku-org/nwaku/issues/2732)) ([736ce1cb](https://github.com/waku-org/nwaku/commit/736ce1cb))
- Rln-relay: use arkzkey variant of zerokit ([#2681](https://github.com/waku-org/nwaku/issues/2681)) ([e7b0777d](https://github.com/waku-org/nwaku/commit/e7b0777d))

### Bug Fixes

- Better sync lock in partition creation ([#2783](https://github.com/waku-org/nwaku/issues/2783)) ([8d3bbb1b](https://github.com/waku-org/nwaku/pull/2809/commits/8d3bbb1b4e79b15c8cf18bb91d366e9ec1153301))
- Multi nat initialization causing dead lock in waku tests + serialize test runs to avoid timing and port occupied issues ([#2799](https://github.com/waku-org/nwaku/issues/2799)) ([5989de88](https://github.com/waku-org/nwaku/commit/5989de88))
- Increase on chain group manager starting balance ([#2795](https://github.com/waku-org/nwaku/issues/2795)) ([e72bb7e7](https://github.com/waku-org/nwaku/commit/e72bb7e7))
- More detailed logs to differentiate shards with peers ([#2794](https://github.com/waku-org/nwaku/issues/2794)) ([55a87d21](https://github.com/waku-org/nwaku/commit/55a87d21))
- waku_archive: only allow a single instance to execute migrations ([#2736](https://github.com/waku-org/nwaku/issues/2736)) ([88b8e186](https://github.com/waku-org/nwaku/commit/88b8e186))
- Move postgres related tests under linux conditional ([57ecb3e0](https://github.com/waku-org/nwaku/commit/57ecb3e0))
- Invalid cursor returning messages ([#2724](https://github.com/waku-org/nwaku/issues/2724)) ([a65b13fc](https://github.com/waku-org/nwaku/commit/a65b13fc))
- Do not print the db url on error ([#2725](https://github.com/waku-org/nwaku/issues/2725)) ([40296f9d](https://github.com/waku-org/nwaku/commit/40296f9d))
- Use `when` instead of `if` for adding soname on linux ([#2721](https://github.com/waku-org/nwaku/issues/2721)) ([cbaefeb3](https://github.com/waku-org/nwaku/commit/cbaefeb3))
- Store v3 bug fixes ([#2718](https://github.com/waku-org/nwaku/issues/2718)) ([4a6ec468](https://github.com/waku-org/nwaku/commit/4a6ec468))


### Changes

- Set msg_hash logs to notice level ([#2737](https://github.com/waku-org/nwaku/issues/2737)) ([f5d87c5b](https://github.com/waku-org/nwaku/commit/f5d87c5b))
- Minor enhancements ([#2789](https://github.com/waku-org/nwaku/issues/2789)) ([31bd6d71](https://github.com/waku-org/nwaku/commit/31bd6d71))
- postgres_driver - acquire/release advisory lock when creating partitions ([#2784](https://github.com/waku-org/nwaku/issues/2784)) ([c5d19c44](https://github.com/waku-org/nwaku/commit/c5d19c44))
- Setting fail-fast to false in matrixed github actions ([#2787](https://github.com/waku-org/nwaku/issues/2787)) ([005349cc](https://github.com/waku-org/nwaku/commit/005349cc))
- Simple link refactor ([#2781](https://github.com/waku-org/nwaku/issues/2781)) ([77adfccd](https://github.com/waku-org/nwaku/commit/77adfccd))
- Improving liteprotocolteseter stats ([#2750](https://github.com/waku-org/nwaku/issues/2750)) ([4c7c8a15](https://github.com/waku-org/nwaku/commit/4c7c8a15))
- Extract common prefixes into a constant for multiple query ([#2747](https://github.com/waku-org/nwaku/issues/2747)) ([dfc979a8](https://github.com/waku-org/nwaku/commit/dfc979a8))
- wakucanary: fix fitler protocol, add storev3 ([#2735](https://github.com/waku-org/nwaku/issues/2735)) ([e0079cd0](https://github.com/waku-org/nwaku/commit/e0079cd0))
- Bump nim-libp2p version ([#2661](https://github.com/waku-org/nwaku/issues/2661)) ([6fbab633](https://github.com/waku-org/nwaku/commit/6fbab633))
- Link validation process docs to the release process file ([#2714](https://github.com/waku-org/nwaku/issues/2714)) ([ebe69be8](https://github.com/waku-org/nwaku/commit/ebe69be8))
- Android support ([#2554](https://github.com/waku-org/nwaku/issues/2554)) ([1e2aa57a](https://github.com/waku-org/nwaku/commit/1e2aa57a))
- Discovery in libwaku ([#2711](https://github.com/waku-org/nwaku/issues/2711)) ([74646848](https://github.com/waku-org/nwaku/commit/74646848))
- libwaku - allow to properly set the log level in libwaku and unify a little ([#2708](https://github.com/waku-org/nwaku/issues/2708)) ([3faffdbc](https://github.com/waku-org/nwaku/commit/3faffdbc))
- waku_discv5, peer_manager - add more logs help debug discovery issues ([#2705](https://github.com/waku-org/nwaku/issues/2705)) ([401630ee](https://github.com/waku-org/nwaku/commit/401630ee))
- Generic change to reduce the number of compilation warnings ([#2696](https://github.com/waku-org/nwaku/issues/2696)) ([78132dc1](https://github.com/waku-org/nwaku/commit/78132dc1))


This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |
| [`66/WAKU2-METADATA`](https://github.com/waku-org/specs/blob/master/standards/core/metadata.md) | `raw` | `/vac/waku/metadata/1.0.0` |

## v0.28.1 (2024-05-29)

This patch release fixes the following bug:
- Store node does not retrieve messages because the meta field is missing in queries.

### Bug Fix

- Commit that fixes the bug [8b42f199](https://github.com/waku-org/nwaku/commit/8b42f199baf4e00794c4cec4d8601c3f6c330a20)

This is a patch release that is fully backwards-compatible with release `v0.28.0`.

It supports the same [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |
| [`66/WAKU2-METADATA`](https://github.com/waku-org/specs/blob/master/standards/core/metadata.md) | `raw` | `/vac/waku/metadata/1.0.0` |


## v0.28.0 (2024-05-22)

## What's Changed

Release highlights:

* Store V3 has been merged
* Implemented an enhanced and more robust node health check mechanism
* Introduced the Waku object to libwaku in order to setup a node and its protocols

### Features

- Added message size check before relay for lightpush ([#2695](https://github.com/waku-org/nwaku/issues/2695)) ([9dfdfa27](https://github.com/waku-org/nwaku/commit/9dfdfa27))
- adding json string support to bindings config ([#2685](https://github.com/waku-org/nwaku/issues/2685)) ([be5471c6](https://github.com/waku-org/nwaku/commit/be5471c6))
- Added flexible rate limit checks for store, legacy store and lightpush ([#2668](https://github.com/waku-org/nwaku/issues/2668)) ([026d804a](https://github.com/waku-org/nwaku/commit/026d804a))
- store v3 return pubsub topics ([#2676](https://github.com/waku-org/nwaku/issues/2676)) ([d700006a](https://github.com/waku-org/nwaku/commit/d700006a))
- supporting meta field in store ([#2609](https://github.com/waku-org/nwaku/issues/2609)) ([a46d4451](https://github.com/waku-org/nwaku/commit/a46d4451))
- store v3 ([#2431](https://github.com/waku-org/nwaku/issues/2431)) ([0b0fbfad](https://github.com/waku-org/nwaku/commit/0b0fbfad))

### Bug Fixes

- use await instead of waitFor in async tests ([#2690](https://github.com/waku-org/nwaku/issues/2690)) ([a37c9ba9](https://github.com/waku-org/nwaku/commit/a37c9ba9))
- message cache removal crash ([#2682](https://github.com/waku-org/nwaku/issues/2682)) ([fa26d05f](https://github.com/waku-org/nwaku/commit/fa26d05f))
- add `meta` to sqlite migration scripts ([#2675](https://github.com/waku-org/nwaku/issues/2675)) ([82f95999](https://github.com/waku-org/nwaku/commit/82f95999))
- content_script_version_4.nim: migration failed when dropping unexisting constraing ([#2672](https://github.com/waku-org/nwaku/issues/2672)) ([38f8b08c](https://github.com/waku-org/nwaku/commit/38f8b08c))
- **filter:** log is too large ([#2665](https://github.com/waku-org/nwaku/issues/2665)) ([cee020f2](https://github.com/waku-org/nwaku/commit/cee020f2))
- issue [#2644](https://github.com/waku-org/nwaku/issues/2644) properly ([#2663](https://github.com/waku-org/nwaku/issues/2663)) ([853ec186](https://github.com/waku-org/nwaku/commit/853ec186))
- store v3 validate cursor & remove messages  ([#2636](https://github.com/waku-org/nwaku/issues/2636)) ([e03d1165](https://github.com/waku-org/nwaku/commit/e03d1165))
- **waku_keystore:** sigsegv on different appInfo ([#2654](https://github.com/waku-org/nwaku/issues/2654)) ([5dd645cf](https://github.com/waku-org/nwaku/commit/5dd645cf))
- **rln-relay:** persist metadata every batch during initial sync ([#2649](https://github.com/waku-org/nwaku/issues/2649)) ([a9e19efd](https://github.com/waku-org/nwaku/commit/a9e19efd))
- handle named sharding in enr ([#2647](https://github.com/waku-org/nwaku/issues/2647)) ([8d1b0834](https://github.com/waku-org/nwaku/commit/8d1b0834))
- parse shards properly in enr config for non twn ([#2633](https://github.com/waku-org/nwaku/issues/2633)) ([6e6cb298](https://github.com/waku-org/nwaku/commit/6e6cb298))
- proto field numbers & status desc ([#2632](https://github.com/waku-org/nwaku/issues/2632)) ([843fe217](https://github.com/waku-org/nwaku/commit/843fe217))
- missing rate limit setting for legacy store protocol ([#2631](https://github.com/waku-org/nwaku/issues/2631)) ([5f65565c](https://github.com/waku-org/nwaku/commit/5f65565c))
- **rln-relay:** enforce error callback to remove exception raised from retryWrapper ([#2622](https://github.com/waku-org/nwaku/issues/2622)) ([9c9883a6](https://github.com/waku-org/nwaku/commit/9c9883a6))
- **rln-relay:** increase retries for 1 minute recovery time ([#2614](https://github.com/waku-org/nwaku/issues/2614)) ([1a23700d](https://github.com/waku-org/nwaku/commit/1a23700d))
- **ci:** unique comment_tag to reference rln version ([#2613](https://github.com/waku-org/nwaku/issues/2613)) ([2c01fa0f](https://github.com/waku-org/nwaku/commit/2c01fa0f))
- don't use WakuMessageSize in req/resp protocols ([#2601](https://github.com/waku-org/nwaku/issues/2601)) ([e61e4ff9](https://github.com/waku-org/nwaku/commit/e61e4ff9))
- create options api for cors preflight request ([#2598](https://github.com/waku-org/nwaku/issues/2598)) ([768c61b1](https://github.com/waku-org/nwaku/commit/768c61b1))
- node restart test issue ([#2576](https://github.com/waku-org/nwaku/issues/2576)) ([4a8e62ac](https://github.com/waku-org/nwaku/commit/4a8e62ac))
- **doc:** update REST API docs ([#2581](https://github.com/waku-org/nwaku/issues/2581)) ([006d43ae](https://github.com/waku-org/nwaku/commit/006d43ae))

### Changes

- move code from wakunode2 to a more generic place, waku ([#2670](https://github.com/waku-org/nwaku/issues/2670)) ([840e0122](https://github.com/waku-org/nwaku/commit/840e0122))
- closing ping streams ([#2692](https://github.com/waku-org/nwaku/issues/2692)) ([7d4857ea](https://github.com/waku-org/nwaku/commit/7d4857ea))
- Postgres enhance get oldest timestamp ([#2687](https://github.com/waku-org/nwaku/issues/2687)) ([8451cf8e](https://github.com/waku-org/nwaku/commit/8451cf8e))
- **rln-relay:** health check should account for window of roots ([#2664](https://github.com/waku-org/nwaku/issues/2664)) ([6a1af922](https://github.com/waku-org/nwaku/commit/6a1af922))
- updating TWN bootstrap fleet to waku.sandbox ([#2638](https://github.com/waku-org/nwaku/issues/2638)) ([22f64bbd](https://github.com/waku-org/nwaku/commit/22f64bbd))
- simplify migration script postgres version_4 ([#2674](https://github.com/waku-org/nwaku/issues/2674)) ([91c85738](https://github.com/waku-org/nwaku/commit/91c85738))
- big refactor to add waku component in libwaku instead of only waku node ([#2658](https://github.com/waku-org/nwaku/issues/2658)) ([2463527b](https://github.com/waku-org/nwaku/commit/2463527b))
- simplify app.nim and move discovery items to appropriate modules ([#2657](https://github.com/waku-org/nwaku/issues/2657)) ([404810aa](https://github.com/waku-org/nwaku/commit/404810aa))
- log enhancement for message reliability analysis ([#2640](https://github.com/waku-org/nwaku/issues/2640)) ([d5e0e4a9](https://github.com/waku-org/nwaku/commit/d5e0e4a9))
- metrics server. Simplify app.nim module ([#2650](https://github.com/waku-org/nwaku/issues/2650)) ([4a110f65](https://github.com/waku-org/nwaku/commit/4a110f65))
- change nim-libp2p branch from unstable to master ([#2648](https://github.com/waku-org/nwaku/issues/2648)) ([d09c9c91](https://github.com/waku-org/nwaku/commit/d09c9c91))
- Enabling to use a full node for lightpush via rest api without lightpush client configured ([#2626](https://github.com/waku-org/nwaku/issues/2626)) ([2a4c0f15](https://github.com/waku-org/nwaku/commit/2a4c0f15))
- **rln-relay:** resultify rln-relay 1/n ([#2607](https://github.com/waku-org/nwaku/issues/2607)) ([1d7ff288](https://github.com/waku-org/nwaku/commit/1d7ff288))
- ci.yml - avoid calling brew link libpq --force on macos ([#2627](https://github.com/waku-org/nwaku/issues/2627)) ([05f332ed](https://github.com/waku-org/nwaku/commit/05f332ed))
- an enhanced version of convenient node health check script ([#2624](https://github.com/waku-org/nwaku/issues/2624)) ([7f8d8e80](https://github.com/waku-org/nwaku/commit/7f8d8e80))
- **rln-db-inspector:** add more logging to find zero leaf indices ([#2617](https://github.com/waku-org/nwaku/issues/2617)) ([40752b1e](https://github.com/waku-org/nwaku/commit/40752b1e))
- addition of waku_api/rest/builder.nim and reduce app.nim ([#2623](https://github.com/waku-org/nwaku/issues/2623)) ([b28207ab](https://github.com/waku-org/nwaku/commit/b28207ab))
- Separation of node health and initialization state from rln_relay ([#2612](https://github.com/waku-org/nwaku/issues/2612)) ([6d135b0d](https://github.com/waku-org/nwaku/commit/6d135b0d))
- enabling rest api as default ([#2600](https://github.com/waku-org/nwaku/issues/2600)) ([6bc79bc7](https://github.com/waku-org/nwaku/commit/6bc79bc7))
- move app.nim and networks_config.nim to waku/factory ([#2608](https://github.com/waku-org/nwaku/issues/2608)) ([1ba9df4b](https://github.com/waku-org/nwaku/commit/1ba9df4b))
- workflow to autoassign PR ([#2604](https://github.com/waku-org/nwaku/issues/2604)) ([10d36c39](https://github.com/waku-org/nwaku/commit/10d36c39))
- start moving discovery modules to waku/discovery ([#2587](https://github.com/waku-org/nwaku/issues/2587)) ([828583ad](https://github.com/waku-org/nwaku/commit/828583ad))
- don't create docker images for users without org's secrets ([#2585](https://github.com/waku-org/nwaku/issues/2585)) ([51ec12be](https://github.com/waku-org/nwaku/commit/51ec12be))

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |
| [`66/WAKU2-METADATA`](https://github.com/waku-org/specs/blob/master/standards/core/metadata.md) | `raw` | `/vac/waku/metadata/1.0.0` |

The Waku v1 implementation has been removed from this repository and can be found in a separate [Waku Legacy](https://github.com/waku-org/waku-legacy) repository.

## v0.27.0 (2024-04-19)

>  **Note:**

>  - Filter v1 protocol and its REST-API access have been deprecated.
>  - A new field of the `WakuMetadataRequest` protobuf for shards was introduced. The old shards field (2) will be deprecated in 2 releases time
>  - CLI flags `--requestRateLimit` and `--requestRatePeriod` have been added for rate limiting configuration. Period is measured in seconds. Limits are measured per protocol per period of time. Over limit will result in TOO_MANY_REQUEST (429) response.

## What's Changed

Release highlights:

* Introduced configurable rate limiting for lightpush and store requests
* Sync time has been considerably reduced for node initialization
* Significant refactors were made to node initialization and `WakuArchive` logic as work towards C-bindings and Store V3 features

### Features

- Added simple, configurable rate limit for lightpush and store-query ([#2390](https://github.com/waku-org/nwaku/issues/2390)) ([a00f350c](https://github.com/waku-org/nwaku/commit/a00f350c))
- examples/golang/waku.go add new example ([#2559](https://github.com/waku-org/nwaku/issues/2559)) ([8d66a548](https://github.com/waku-org/nwaku/commit/8d66a548))
- **c-bindings:** rln relay ([#2544](https://github.com/waku-org/nwaku/issues/2544)) ([2aa835e3](https://github.com/waku-org/nwaku/commit/2aa835e3))
- **incentivization:** add codec for eligibility proof and status ([#2419](https://github.com/waku-org/nwaku/issues/2419)) ([65530264](https://github.com/waku-org/nwaku/commit/65530264))
- **rest:** add support to ephemeral field ([#2525](https://github.com/waku-org/nwaku/issues/2525)) ([c734f60d](https://github.com/waku-org/nwaku/commit/c734f60d))
- archive update for store v3 ([#2451](https://github.com/waku-org/nwaku/issues/2451)) ([505479b8](https://github.com/waku-org/nwaku/commit/505479b8))
- **c-bindings:** add function to dealloc nodes ([#2499](https://github.com/waku-org/nwaku/issues/2499)) ([8341864d](https://github.com/waku-org/nwaku/commit/8341864d))

### Bug Fixes

- **rln-relay:** reduce sync time ([#2577](https://github.com/waku-org/nwaku/issues/2577)) ([480a62fa](https://github.com/waku-org/nwaku/commit/480a62fa))
- rest store: content_topic -> contentTopic in the response ([#2584](https://github.com/waku-org/nwaku/issues/2584)) ([d2578553](https://github.com/waku-org/nwaku/commit/d2578553))
- **c-bindings:** rln credential path key ([#2564](https://github.com/waku-org/nwaku/issues/2564)) ([3d752b11](https://github.com/waku-org/nwaku/commit/3d752b11))
- cluster-id 0 disc5 issue ([#2562](https://github.com/waku-org/nwaku/issues/2562)) ([a76c9587](https://github.com/waku-org/nwaku/commit/a76c9587))
- regex for rpc endpoint ([#2563](https://github.com/waku-org/nwaku/issues/2563)) ([c87545d5](https://github.com/waku-org/nwaku/commit/c87545d5))
- **rln:** set a minimum epoch gap ([#2555](https://github.com/waku-org/nwaku/issues/2555)) ([b5e4795f](https://github.com/waku-org/nwaku/commit/b5e4795f))
- fix regresion + remove deprecated flag ([#2556](https://github.com/waku-org/nwaku/issues/2556)) ([47ad0fb0](https://github.com/waku-org/nwaku/commit/47ad0fb0))
- **networkmanager:** regularly disconnect from random peers ([#2553](https://github.com/waku-org/nwaku/issues/2553)) ([70c53fc0](https://github.com/waku-org/nwaku/commit/70c53fc0))
- remove subscription queue limit ([#2551](https://github.com/waku-org/nwaku/issues/2551)) ([94ff5eab](https://github.com/waku-org/nwaku/commit/94ff5eab))
- peer_manager - extend the number of connection requests to known peers ([#2534](https://github.com/waku-org/nwaku/issues/2534)) ([2173fe22](https://github.com/waku-org/nwaku/commit/2173fe22))
- **2491:** Fix metadata protocol disconnecting light nodes ([#2533](https://github.com/waku-org/nwaku/issues/2533)) ([33774fad](https://github.com/waku-org/nwaku/commit/33774fad))
- **rest:** filter/v2/subscriptions response ([#2529](https://github.com/waku-org/nwaku/issues/2529)) ([7aea2d4f](https://github.com/waku-org/nwaku/commit/7aea2d4f))
- **store:** retention policy regex ([#2532](https://github.com/waku-org/nwaku/issues/2532)) ([23a291b3](https://github.com/waku-org/nwaku/commit/23a291b3))
- enable autosharding in any cluster ([#2505](https://github.com/waku-org/nwaku/issues/2505)) ([5a225809](https://github.com/waku-org/nwaku/commit/5a225809))
- introduce new field for shards in metadata protocol ([#2511](https://github.com/waku-org/nwaku/issues/2511)) ([f9f92b7d](https://github.com/waku-org/nwaku/commit/f9f92b7d))
- **rln-relay:** handle empty metadata returned by getMetadata proc ([#2516](https://github.com/waku-org/nwaku/issues/2516)) ([1274b15d](https://github.com/waku-org/nwaku/commit/1274b15d))

### Changes

- adding migration script adding i_query index ([#2578](https://github.com/waku-org/nwaku/issues/2578)) ([4117fe65](https://github.com/waku-org/nwaku/commit/4117fe65))
- bumping chronicles version ([#2583](https://github.com/waku-org/nwaku/issues/2583)) ([a04e0d99](https://github.com/waku-org/nwaku/commit/a04e0d99))
- add ARM64 support for Linux/MacOS ([#2580](https://github.com/waku-org/nwaku/issues/2580)) ([269139cf](https://github.com/waku-org/nwaku/commit/269139cf))
- **rln:** update submodule + rln patch version ([#2574](https://github.com/waku-org/nwaku/issues/2574)) ([24f6fed8](https://github.com/waku-org/nwaku/commit/24f6fed8))
- bumping dependencies for 0.27.0 ([#2572](https://github.com/waku-org/nwaku/issues/2572)) ([f68ac792](https://github.com/waku-org/nwaku/commit/f68ac792))
- **c-bindings:** node initialization ([#2547](https://github.com/waku-org/nwaku/issues/2547)) ([6d0f6d82](https://github.com/waku-org/nwaku/commit/6d0f6d82))
- remove deprecated legacy filter protocol ([#2507](https://github.com/waku-org/nwaku/issues/2507)) ([e8613172](https://github.com/waku-org/nwaku/commit/e8613172))
- switch wakuv2 to waku fleet ([#2519](https://github.com/waku-org/nwaku/issues/2519)) ([18a05359](https://github.com/waku-org/nwaku/commit/18a05359))
- create nph.md ([#2536](https://github.com/waku-org/nwaku/issues/2536)) ([a576e624](https://github.com/waku-org/nwaku/commit/a576e624))
- Better postgres duplicate insert ([#2535](https://github.com/waku-org/nwaku/issues/2535)) ([693a1778](https://github.com/waku-org/nwaku/commit/693a1778))
- add 150 kB to msg size histogram metric ([#2430](https://github.com/waku-org/nwaku/issues/2430)) ([2c1391d3](https://github.com/waku-org/nwaku/commit/2c1391d3))
- content_script_version_2: add simple protection and rename messages_backup if exists ([#2531](https://github.com/waku-org/nwaku/issues/2531)) ([c6c376b5](https://github.com/waku-org/nwaku/commit/c6c376b5))
- **vendor:** update nim-libp2p path ([#2527](https://github.com/waku-org/nwaku/issues/2527)) ([3c823756](https://github.com/waku-org/nwaku/commit/3c823756))
- adding node factory tests ([#2524](https://github.com/waku-org/nwaku/issues/2524)) ([a1b3e090](https://github.com/waku-org/nwaku/commit/a1b3e090))
- factory cleanup ([#2523](https://github.com/waku-org/nwaku/issues/2523)) ([8d7eb3a6](https://github.com/waku-org/nwaku/commit/8d7eb3a6))
- **rln-relay-v2:** wakunode testing + improvements ([#2501](https://github.com/waku-org/nwaku/issues/2501)) ([059cb975](https://github.com/waku-org/nwaku/commit/059cb975))
- update CHANGELOG for v0.26.0 release ([#2518](https://github.com/waku-org/nwaku/issues/2518)) ([097cb362](https://github.com/waku-org/nwaku/commit/097cb362))
- migrating logic from wakunode2.nim to node_factory.nim ([#2504](https://github.com/waku-org/nwaku/issues/2504)) ([dcc88ee0](https://github.com/waku-org/nwaku/commit/dcc88ee0))

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |
| [`66/WAKU2-METADATA`](https://github.com/waku-org/specs/blob/master/standards/core/metadata.md) | `raw` | `/vac/waku/metadata/1.0.0` |

The Waku v1 implementation has been removed from this repository and can be found in a separate [Waku Legacy](https://github.com/waku-org/waku-legacy) repository.

## v0.26.0 (2024-03-07)

> **Note:**
> - JSON-RPC API has been removed completely. Instead we recommend you to utilize REST API endpoints that have same and extended functionality.
> Please have a look at Waku's REST-API reference: https://waku-org.github.io/waku-rest-api
> - Support for Cross-Origin-Resource-Sharing (CORS headers) is added for our REST-API services. This allows you to access our REST-API from a browser.
> New repeatable CLI flag is added by this feature:
> `--rest-allow-origin="example.com"` or `--rest-allow-origin="127.0.0.0:*"`
> Flag allows using wildcards (`*` and `?`) in the origin string.
> - Store protocol now has a better support for controlling DB size of Postgres store. This feature needs no user action.

> **Announcement:**
>
> Please notice that from the next release (0.27.0) we will deprecate features.
>
> - We will decomission the Filter v1 protocol and its REST-API access.

### Features

- Postgres partition implementation ([#2506](https://github.com/waku-org/nwaku/issues/2506)) ([161a10ec](https://github.com/waku-org/nwaku/commit/161a10ec))
- **waku-stealth-commitments:** waku stealth commitment protocol ([#2490](https://github.com/waku-org/nwaku/issues/2490)) ([0def4904](https://github.com/waku-org/nwaku/commit/0def4904))
- **bindings:** generate a random private key ([#2446](https://github.com/waku-org/nwaku/issues/2446)) ([56ff30ca](https://github.com/waku-org/nwaku/commit/56ff30ca))
- prioritise yamux above mplex ([#2417](https://github.com/waku-org/nwaku/issues/2417)) ([ce151efc](https://github.com/waku-org/nwaku/commit/ce151efc))
- supporting meta field in WakuMessage ([#2384](https://github.com/waku-org/nwaku/issues/2384)) ([3903f130](https://github.com/waku-org/nwaku/commit/3903f130))
- `eventCallback` per wakunode and `userData`  ([#2418](https://github.com/waku-org/nwaku/issues/2418)) ([707f3e8b](https://github.com/waku-org/nwaku/commit/707f3e8b))
- **rln-relay-v2:** nonce/messageId manager ([#2413](https://github.com/waku-org/nwaku/issues/2413)) ([50308eda](https://github.com/waku-org/nwaku/commit/50308eda))
- **networkmonitor:** add support for rln ([#2401](https://github.com/waku-org/nwaku/issues/2401)) ([9c0e9431](https://github.com/waku-org/nwaku/commit/9c0e9431))
- **rln-relay-v2:** rln-keystore-generator updates ([#2392](https://github.com/waku-org/nwaku/issues/2392)) ([2d46c351](https://github.com/waku-org/nwaku/commit/2d46c351))
- add yamux support ([#2397](https://github.com/waku-org/nwaku/issues/2397)) ([1b402667](https://github.com/waku-org/nwaku/commit/1b402667))

### Bug Fixes

- **rln-relay:** make nullifier log abide by epoch ordering ([#2508](https://github.com/waku-org/nwaku/issues/2508)) ([beba14dc](https://github.com/waku-org/nwaku/commit/beba14dc))
- **postgres:** import under feature flag ([#2500](https://github.com/waku-org/nwaku/issues/2500)) ([e692edf6](https://github.com/waku-org/nwaku/commit/e692edf6))
- notify Waku Metadata when Waku Filter subscribe to a topic ([#2493](https://github.com/waku-org/nwaku/issues/2493)) ([91e3f8cd](https://github.com/waku-org/nwaku/commit/91e3f8cd))
- time on 32 bits architecture ([#2492](https://github.com/waku-org/nwaku/issues/2492)) ([0a751228](https://github.com/waku-org/nwaku/commit/0a751228))
- return message id on `waku_relay_publish` ([#2485](https://github.com/waku-org/nwaku/issues/2485)) ([045091a9](https://github.com/waku-org/nwaku/commit/045091a9))
- **bindings:** base64 payload and key for content topic ([#2435](https://github.com/waku-org/nwaku/issues/2435)) ([d01585e9](https://github.com/waku-org/nwaku/commit/d01585e9))
- **rln-relay:** regex pattern match for extended domains ([#2444](https://github.com/waku-org/nwaku/issues/2444)) ([29b0c0b8](https://github.com/waku-org/nwaku/commit/29b0c0b8))
- checking for keystore file existence ([#2427](https://github.com/waku-org/nwaku/issues/2427)) ([8f487a21](https://github.com/waku-org/nwaku/commit/8f487a21))
- **rln-relay:** graceful shutdown with non-zero exit code ([#2429](https://github.com/waku-org/nwaku/issues/2429)) ([22026b7e](https://github.com/waku-org/nwaku/commit/22026b7e))
- check max message size in validator according to configured value ([#2424](https://github.com/waku-org/nwaku/issues/2424)) ([731dfcbd](https://github.com/waku-org/nwaku/commit/731dfcbd))
- **wakunode2:** move node config inside app init branch ([#2423](https://github.com/waku-org/nwaku/issues/2423)) ([0dac9f9d](https://github.com/waku-org/nwaku/commit/0dac9f9d))

### Changes

- **rln_db_inspector:** include in wakunode2 binary ([#2292](https://github.com/waku-org/nwaku/issues/2292)) ([a9d0e481](https://github.com/waku-org/nwaku/commit/a9d0e481))
- Update link to DNS discovery tutorial ([#2496](https://github.com/waku-org/nwaku/issues/2496)) ([9ef2eccb](https://github.com/waku-org/nwaku/commit/9ef2eccb))
- **rln-relay-v2:** added tests for static rln-relay-v2 ([#2484](https://github.com/waku-org/nwaku/issues/2484)) ([5b174fb3](https://github.com/waku-org/nwaku/commit/5b174fb3))
- moving node initialization code to node_factory.nim ([#2479](https://github.com/waku-org/nwaku/issues/2479)) ([361fe2cd](https://github.com/waku-org/nwaku/commit/361fe2cd))
- Postgres migrations ([#2477](https://github.com/waku-org/nwaku/issues/2477)) ([560f949a](https://github.com/waku-org/nwaku/commit/560f949a))
- **rln-relay-v2:** added tests for onchain rln-relay-v2 ([#2482](https://github.com/waku-org/nwaku/issues/2482)) ([88ff9282](https://github.com/waku-org/nwaku/commit/88ff9282))
- remove json rpc ([#2416](https://github.com/waku-org/nwaku/issues/2416)) ([c994ee04](https://github.com/waku-org/nwaku/commit/c994ee04))
- **ci:** use git describe for image version ([55ff6674](https://github.com/waku-org/nwaku/commit/55ff6674))
- Implemented CORS handling for nwaku REST server ([#2470](https://github.com/waku-org/nwaku/issues/2470)) ([d832f92a](https://github.com/waku-org/nwaku/commit/d832f92a))
- remove rln epoch hardcoding ([#2483](https://github.com/waku-org/nwaku/issues/2483)) ([3f4f6d7e](https://github.com/waku-org/nwaku/commit/3f4f6d7e))
- **cbindings:** cbindings rust simple libwaku integration example ([#2089](https://github.com/waku-org/nwaku/issues/2089)) ([a4993005](https://github.com/waku-org/nwaku/commit/a4993005))
- adding NIMFLAGS usage to readme ([#2469](https://github.com/waku-org/nwaku/issues/2469)) ([a1d5cbd9](https://github.com/waku-org/nwaku/commit/a1d5cbd9))
- bumping nim-libp2p after yamux timeout fix ([#2468](https://github.com/waku-org/nwaku/issues/2468)) ([216531b0](https://github.com/waku-org/nwaku/commit/216531b0))
- new proc to foster different size retention policy implementations ([#2463](https://github.com/waku-org/nwaku/issues/2463)) ([d5305282](https://github.com/waku-org/nwaku/commit/d5305282))
- **rln-relay:** use anvil instead of ganache in onchain tests ([#2449](https://github.com/waku-org/nwaku/issues/2449)) ([f6332ac6](https://github.com/waku-org/nwaku/commit/f6332ac6))
- bindings return multiaddress array ([#2461](https://github.com/waku-org/nwaku/issues/2461)) ([7aea145e](https://github.com/waku-org/nwaku/commit/7aea145e))
- **ci:** fix IMAGE_NAME to use harbor.status.im ([b700d046](https://github.com/waku-org/nwaku/commit/b700d046))
- **rln-relay:** remove wss support from node config ([#2442](https://github.com/waku-org/nwaku/issues/2442)) ([2060cfab](https://github.com/waku-org/nwaku/commit/2060cfab))
- **ci:** reuse discord send function from library ([1151d50f](https://github.com/waku-org/nwaku/commit/1151d50f))
- **rln-relay-v2:** add tests for serde ([#2421](https://github.com/waku-org/nwaku/issues/2421)) ([d0377056](https://github.com/waku-org/nwaku/commit/d0377056))
- add stdef.h to libwaku.h ([#2409](https://github.com/waku-org/nwaku/issues/2409)) ([d58aca01](https://github.com/waku-org/nwaku/commit/d58aca01))
- automatically generating certs if not provided (Waku Canary) ([#2408](https://github.com/waku-org/nwaku/issues/2408)) ([849d76d6](https://github.com/waku-org/nwaku/commit/849d76d6))
- Simplify configuration for the waku network ([#2404](https://github.com/waku-org/nwaku/issues/2404)) ([985d092f](https://github.com/waku-org/nwaku/commit/985d092f))

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |
| [`66/WAKU2-METADATA`](https://github.com/waku-org/specs/blob/master/standards/core/metadata.md) | `raw` | `/vac/waku/metadata/1.0.0` |

The Waku v1 implementation has been removed from this repository and can be found in a separate [Waku Legacy](https://github.com/waku-org/waku-legacy) repository.

## v0.25.0 (2024-02-06)

> **Note:**
>  Waku Filter v2 now has three additional configuration options
> `--filter-max-peers-to-serve=1000` drives how many peers can subscribe at once and
> `--filter-max-criteria=1000` defines what is the maximum criterion stored per each peers
>
> This release introduces a major change in Filter v2 protocol subscription management.
> From now each subscribed peer needs to refresh its living subscriptions by sending a SUBSCRIBER_PING message every 5 minutes by default, otherwise the peer's subscription will be removed.
> `--filter-subscription-timeout=300` defines configurable timeout for the subscriptions (*in seconds*).
>
> New experimental feature, shard aware peer manager for relay protocol can be activated by the flag:
> `--relay-shard-manager=true|false`
> It is disabled by default.

> **Announcement:**
>
> Please notice that from the next release (0.26.0) we will deprecate features.
>
> - JSON-RPC API will be removed completely. Instead we recommend you to utilize REST API endpoints that have same and extended functionality.
> - We will retire websockets support for RLN on-chain group management. You are expected to use HTTP version of ETH_CLIENT_ADDRESS

### Features

- running validators in /relay/v1/auto/messages/{topic} ([#2394](https://github.com/waku-org/nwaku/issues/2394)) ([e4e147bc](https://github.com/waku-org/nwaku/commit/e4e147bc))
- **rln-relay-v2:** update C FFI api's and serde ([#2385](https://github.com/waku-org/nwaku/issues/2385)) ([b88facd0](https://github.com/waku-org/nwaku/commit/b88facd0))
- running validators in /relay/v1/messages/{pubsubTopic} ([#2373](https://github.com/waku-org/nwaku/issues/2373)) ([59d8b620](https://github.com/waku-org/nwaku/commit/59d8b620))
- shard aware relay peer management ([#2332](https://github.com/waku-org/nwaku/issues/2332)) ([edca1df1](https://github.com/waku-org/nwaku/commit/edca1df1))

### Bug Fixes

- adding rln validator as default ([#2367](https://github.com/waku-org/nwaku/issues/2367)) ([bb58a63a](https://github.com/waku-org/nwaku/commit/bb58a63a))
- Fix test for filter client receiving messages after restart ([#2360](https://github.com/waku-org/nwaku/issues/2360)) ([7de91d92](https://github.com/waku-org/nwaku/commit/7de91d92))
- making filter admin data test order independent ([#2355](https://github.com/waku-org/nwaku/issues/2355)) ([8a9fad29](https://github.com/waku-org/nwaku/commit/8a9fad29))

### Changes

- **rln-relay-v2:** use rln-v2 contract code ([#2381](https://github.com/waku-org/nwaku/issues/2381)) ([c55ca067](https://github.com/waku-org/nwaku/commit/c55ca067))
- v0.25 vendor bump and associated fixes ([#2352](https://github.com/waku-org/nwaku/issues/2352)) ([761ce7b1](https://github.com/waku-org/nwaku/commit/761ce7b1))
- handle errors w.r.t. configured cluster-id and pubsub topics ([#2368](https://github.com/waku-org/nwaku/issues/2368)) ([e04e35e2](https://github.com/waku-org/nwaku/commit/e04e35e2))
- add coverage target to Makefile ([#2382](https://github.com/waku-org/nwaku/issues/2382)) ([57378873](https://github.com/waku-org/nwaku/commit/57378873))
- Add check spell allowed words ([#2383](https://github.com/waku-org/nwaku/issues/2383)) ([c1121dd1](https://github.com/waku-org/nwaku/commit/c1121dd1))
- adding nwaku compose image update to release process ([#2370](https://github.com/waku-org/nwaku/issues/2370)) ([4f06dcff](https://github.com/waku-org/nwaku/commit/4f06dcff))
- changing digest and hash log format from bytes to hex ([#2363](https://github.com/waku-org/nwaku/issues/2363)) ([025c6ec9](https://github.com/waku-org/nwaku/commit/025c6ec9))
- log messageHash for lightpush request that helps in debugging ([#2366](https://github.com/waku-org/nwaku/issues/2366)) ([42204115](https://github.com/waku-org/nwaku/commit/42204115))
- **rln-relay:** enabled http based polling in OnchainGroupManager  ([#2364](https://github.com/waku-org/nwaku/issues/2364)) ([efdc5244](https://github.com/waku-org/nwaku/commit/efdc5244))
- improve POST /relay/v1/auto/messages/{topic} error handling ([#2339](https://github.com/waku-org/nwaku/issues/2339)) ([f841454e](https://github.com/waku-org/nwaku/commit/f841454e))
- Refactor of FilterV2 subscription management with Time-to-live maintenance ([#2341](https://github.com/waku-org/nwaku/issues/2341)) ([c3358409](https://github.com/waku-org/nwaku/commit/c3358409))
- Bump `nim-dnsdisc` ([#2354](https://github.com/waku-org/nwaku/issues/2354)) ([3d816c08](https://github.com/waku-org/nwaku/commit/3d816c08))
- postgres-adoption.md add metadata title, description, and better first-readable-title ([#2346](https://github.com/waku-org/nwaku/issues/2346)) ([2f8e8bcb](https://github.com/waku-org/nwaku/commit/2f8e8bcb))
- fix typo ([#2348](https://github.com/waku-org/nwaku/issues/2348)) ([a4a8dee3](https://github.com/waku-org/nwaku/commit/a4a8dee3))

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |
| [`66/WAKU2-METADATA`](https://github.com/waku-org/specs/blob/master/standards/core/metadata.md) | `raw` | `/vac/waku/metadata/1.0.0` |

The Waku v1 implementation has been removed from this repository and can be found in a separate [Waku Legacy](https://github.com/waku-org/waku-legacy) repository.

## v0.24.0 (2024-01-10)

> Note: The Waku message size limit (150 KiB) is now enforced according to the specifications. To change this limit please use `--max-msg-size="1MiB"`

> Note: `--ip-colocation-limit=2` is the new parameter for limiting connections from the same IP

## What's Changed

Release highlights:
* IP colocation filter can now be changed via a configuration parameter.
* New filter admin endpoint can now be used to access subscription data.
* Waku message size limit can now be changed via a configuration parameter.

### Features

- feat: adding filter data admin endpoint (REST) [#2314](https://github.com/waku-org/nwaku/pull/2314)
- ip colocation is parameterizable. if set to 0, it is disabled [#2323](https://github.com/waku-org/nwaku/pull/2323)

### Bug Fixes
- fix: revert "feat: shard aware peer management [#2151](https://github.com/waku-org/nwaku/pull/2151)" [#2312](https://github.com/waku-org/nwaku/pull/2312)
- fix: setting connectivity loop interval to 15 seconds [#2307](https://github.com/waku-org/nwaku/pull/2307)
- fix: set record to the Waku node builder in the examples as it is required [#2328](https://github.com/waku-org/nwaku/pull/2328)
- fix(discv5): add bootnode filter exception [#2267](https://github.com/waku-org/nwaku/pull/2267)


### Changes
- update CHANGELOG.md for 0.23.0 [#2309](https://github.com/waku-org/nwaku/pull/2309)
- test(store): Implement store tests [#2235](https://github.com/waku-org/nwaku/pull/2235), [#2240](https://github.com/waku-org/nwaku/commit/86353e22a871820c132deee077f65e7af4356671)
- refactor(store): HistoryQuery.direction [#2263](https://github.com/waku-org/nwaku/pull/2263)
- test_driver_postgres: enhance test coverage, multiple and single topic [#2301](https://github.com/waku-org/nwaku/pull/2301)
- chore: examples/nodejs - adapt code to latest callback and ctx/userData definitions [#2281](https://github.com/waku-org/nwaku/pull/2281)
- chore: update `CHANGELOG.md` to reflect bug fix for issue [#2317](https://github.com/waku-org/nwaku/issues/2317) [#2340](https://github.com/waku-org/nwaku/pull/2340) in v0.23.1
- test(peer-connection-managenent): functional tests [#2321](https://github.com/waku-org/nwaku/pull/2321)
- docs: update post-release steps [#2336](https://github.com/waku-org/nwaku/pull/2336)
- docs: fix typos across various documentation files [#2310](https://github.com/waku-org/nwaku/pull/2310)
- test(peer-connection-managenent): functional tests [#2321](https://github.com/waku-org/nwaku/pull/2321)
- bump vendors for 0.24.0 [#2333](https://github.com/waku-org/nwaku/pull/2333)
- test(autosharding): functional tests [#2318](https://github.com/waku-org/nwaku/pull/2318)
- docs: add benchmark around postgres adoption [#2316](https://github.com/waku-org/nwaku/pull/2316)
- chore: set max Waku message size to 150KiB according to spec [#2298](https://github.com/waku-org/nwaku/pull/2298)

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |
| [`66/WAKU2-METADATA`](https://github.com/waku-org/specs/blob/master/standards/core/metadata.md) | `raw` | `/vac/waku/metadata/1.0.0` |

The Waku v1 implementation has been removed from this repository and can be found in a separate [Waku Legacy](https://github.com/waku-org/waku-legacy) repository.

## v0.23.1 (2023-01-09)

This patch release fixes the following bug:
- Sort order ignored in store nodes.

### Bug Fix

- Bug definition: [#2317](https://github.com/waku-org/nwaku/issues/2317)
- Commit that fixes the bug [fae20bff](https://github.com/waku-org/nwaku/commit/fae20bff)

This is a patch release that is fully backwards-compatible with release `v0.23.0`.

It supports the same [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |
| [`66/WAKU2-METADATA`](https://github.com/waku-org/specs/blob/master/standards/core/metadata.md) | `raw` | `/vac/waku/metadata/1.0.0` |

The Waku v1 implementation has been removed from this repository and can be found in a separate [Waku Legacy](https://github.com/waku-org/waku-legacy) repository.


## v0.23.0 (2023-12-18)

## What's Changed

Release highlights:
* Bug fix in Postgres when querying more than one content topic.
* :warning: Add new DB column `messageHash`. This requires a manual database update in _Postgres_.
* Updated deterministic message hash algorithm.
* REST admin can inform whether a node supports lightpush and/or filter protocols.
* Improvements to cluster id and shards setup.
* Properly apply RLN when publishing from REST or jsonrpc API.
* Remove trailing commas from the RLN keystore json generated during credentials registration.
* General test cleanup, better relay tests and new filter unsubscribe tests.
* Rewrite docs for clarity and update screenshots.

### Features

- setting image deployment to harbor registry ([93dd5ae5](https://github.com/waku-org/nwaku/commit/93dd5ae5))
- Add new DB column `messageHash` ([#2202](https://github.com/waku-org/nwaku/issues/2202)) ([aeb77a3e](https://github.com/waku-org/nwaku/commit/aeb77a3e))

### Bug Fixes

- make rln rate limit spec compliant ([#2294](https://github.com/waku-org/nwaku/issues/2294)) ([5847f49d](https://github.com/waku-org/nwaku/commit/5847f49d))
- update num-msgs archive metrics every minute and not only at the beginning ([#2287](https://github.com/waku-org/nwaku/issues/2287)) ([0fc617ff](https://github.com/waku-org/nwaku/commit/0fc617ff))
- **rln-relay:** graceful retries on rpc calls ([#2250](https://github.com/waku-org/nwaku/issues/2250)) ([15c1f974](https://github.com/waku-org/nwaku/commit/15c1f974))
- add protection in rest service to always publish with timestamp if user doesn't provide it ([#2261](https://github.com/waku-org/nwaku/issues/2261)) ([42f19579](https://github.com/waku-org/nwaku/commit/42f19579))
- remove trailing commas from keystore json ([#2200](https://github.com/waku-org/nwaku/issues/2200)) ([103d3981](https://github.com/waku-org/nwaku/commit/103d3981))
- **dockerfile:** update dockerignore and base image ([#2262](https://github.com/waku-org/nwaku/issues/2262)) ([c86dc442](https://github.com/waku-org/nwaku/commit/c86dc442))
- waku_filter_v2/common: PEER_DIAL_FAILURE ret code change: 200 -> 504 ([#2236](https://github.com/waku-org/nwaku/issues/2236)) ([6301bec0](https://github.com/waku-org/nwaku/commit/6301bec0))
- extended Postgres code to support retention policy + refactoring ([#2244](https://github.com/waku-org/nwaku/issues/2244)) ([a1ed517f](https://github.com/waku-org/nwaku/commit/a1ed517f))
- admin REST API to be enabled only if config is set ([#2218](https://github.com/waku-org/nwaku/issues/2218)) ([110de90f](https://github.com/waku-org/nwaku/commit/110de90f))
- **rln:** error in api when rate limit ([#2212](https://github.com/waku-org/nwaku/issues/2212)) ([51f36099](https://github.com/waku-org/nwaku/commit/51f36099))
- **relay:** Failing protocol tests ([#2224](https://github.com/waku-org/nwaku/issues/2224)) ([c9e869fb](https://github.com/waku-org/nwaku/commit/c9e869fb))
- **tests:** Compilation failure fix ([#2222](https://github.com/waku-org/nwaku/issues/2222)) ([a5da1fc4](https://github.com/waku-org/nwaku/commit/a5da1fc4))
- **rest:** properly check if rln is used ([#2205](https://github.com/waku-org/nwaku/issues/2205)) ([2cb0989a](https://github.com/waku-org/nwaku/commit/2cb0989a))

### Changes

- archive - move error to trace level when insert row fails ([#2283](https://github.com/waku-org/nwaku/issues/2283)) ([574cdf55](https://github.com/waku-org/nwaku/commit/574cdf55))
- including content topics on FilterSubscribeRequest logs ([#2295](https://github.com/waku-org/nwaku/issues/2295)) ([306c8a62](https://github.com/waku-org/nwaku/commit/306c8a62))
- vendor bump for 0.23.0 ([#2274](https://github.com/waku-org/nwaku/issues/2274)) ([385daf16](https://github.com/waku-org/nwaku/commit/385daf16))
- peer_manager.nim - reduce logs from debug to trace ([#2279](https://github.com/waku-org/nwaku/issues/2279)) ([0cc0c805](https://github.com/waku-org/nwaku/commit/0cc0c805))
- Cbindings allow mounting the Store protocol from libwaku ([#2276](https://github.com/waku-org/nwaku/issues/2276)) ([28142f40](https://github.com/waku-org/nwaku/commit/28142f40))
- Better feedback invalid content topic ([#2254](https://github.com/waku-org/nwaku/issues/2254)) ([72a1f8c7](https://github.com/waku-org/nwaku/commit/72a1f8c7))
- fix typos ([#2239](https://github.com/waku-org/nwaku/issues/2239)) ([958b9bd7](https://github.com/waku-org/nwaku/commit/958b9bd7))
- creating prepare_release template ([#2225](https://github.com/waku-org/nwaku/issues/2225)) ([5883dbeb](https://github.com/waku-org/nwaku/commit/5883dbeb))
- **rest:** refactor message cache ([#2221](https://github.com/waku-org/nwaku/issues/2221)) ([bebaa59c](https://github.com/waku-org/nwaku/commit/bebaa59c))
- updating nim-json-serialization dependency ([#2248](https://github.com/waku-org/nwaku/issues/2248)) ([9f4e6f45](https://github.com/waku-org/nwaku/commit/9f4e6f45))
- **store-archive:** Remove duplicated code ([#2234](https://github.com/waku-org/nwaku/issues/2234)) ([38e100e9](https://github.com/waku-org/nwaku/commit/38e100e9))
- refactoring peer storage ([#2243](https://github.com/waku-org/nwaku/issues/2243)) ([c301e880](https://github.com/waku-org/nwaku/commit/c301e880))
- postres driver allow setting the max number of connection from a parameter ([#2246](https://github.com/waku-org/nwaku/issues/2246)) ([b31c1823](https://github.com/waku-org/nwaku/commit/b31c1823))
- deterministic message hash algorithm updated ([#2233](https://github.com/waku-org/nwaku/issues/2233)) ([a22ee604](https://github.com/waku-org/nwaku/commit/a22ee604))
- **REST:** returning lightpush support and updated filter protocol ([#2219](https://github.com/waku-org/nwaku/issues/2219)) ([59ee3c69](https://github.com/waku-org/nwaku/commit/59ee3c69))
- mics. improvements to cluster id and shards setup ([#2187](https://github.com/waku-org/nwaku/issues/2187)) ([897f4879](https://github.com/waku-org/nwaku/commit/897f4879))
- update docs for rln-keystore-generator ([#2210](https://github.com/waku-org/nwaku/issues/2210)) ([8c5666d2](https://github.com/waku-org/nwaku/commit/8c5666d2))
- removing automatic vacuuming from retention policy code ([#2228](https://github.com/waku-org/nwaku/issues/2228)) ([9ff441ab](https://github.com/waku-org/nwaku/commit/9ff441ab))
- decoupling announced and listen addresses ([#2203](https://github.com/waku-org/nwaku/issues/2203)) ([ef8ffbdb](https://github.com/waku-org/nwaku/commit/ef8ffbdb))
- **release:** update changelog for v0.22.0 release ([#2216](https://github.com/waku-org/nwaku/issues/2216)) ([9c4fdac6](https://github.com/waku-org/nwaku/commit/9c4fdac6))
- Allow text/plain content type descriptor for json formatted content body ([#2209](https://github.com/waku-org/nwaku/issues/2209)) ([6d81e384](https://github.com/waku-org/nwaku/commit/6d81e384))
- rewrite for clarity, update screenshots ([#2206](https://github.com/waku-org/nwaku/issues/2206)) ([a0ef3c2f](https://github.com/waku-org/nwaku/commit/a0ef3c2f))
- **release:** update changelog for v0.21.3 release ([#2208](https://github.com/waku-org/nwaku/issues/2208)) ([f74474b4](https://github.com/waku-org/nwaku/commit/f74474b4))

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |
| [`66/WAKU2-METADATA`](https://github.com/waku-org/specs/blob/master/standards/core/metadata.md) | `raw` | `/vac/waku/metadata/1.0.0` |

The Waku v1 implementation has been removed from this repository and can be found in a separate [Waku Legacy](https://github.com/waku-org/waku-legacy) repository.

## v0.22.0 (2023-11-15)

> Note: The `--topic` option is now deprecated in favor of a more specific options `--pubsub-topic` & `--content-topic`

> Note: The `--ext-multiaddr-only` CLI flag was introduced for cases in which the user wants to manually set their announced addresses

## What's Changed

Release highlights:
* simplified the process of generating RLN credentials through the new `generateRlnKeystore` subcommand
* added support for configuration of port 0 in order to bind to kernel selected ports
* shards are now automatically updated in metadata protocol when supported shards change on runtime
* introduced `messageHash` attribute to SQLite which will later replace the `id` attribute

### Features

- rln-keystore-generator is now a subcommand ([#2189](https://github.com/waku-org/nwaku/issues/2189)) ([3498a846](https://github.com/waku-org/nwaku/commit/3498a846))
- amending computeDigest func. + related test cases ([#2132](https://github.com/waku-org/nwaku/issues/2132))" ([#2180](https://github.com/waku-org/nwaku/issues/2180)) ([d7ef3ca1](https://github.com/waku-org/nwaku/commit/d7ef3ca1))
- **discv5:** filter out peers without any listed capability ([#2186](https://github.com/waku-org/nwaku/issues/2186)) ([200a11da](https://github.com/waku-org/nwaku/commit/200a11da))
- metadata protocol shard subscription ([#2149](https://github.com/waku-org/nwaku/issues/2149)) ([bcf8e963](https://github.com/waku-org/nwaku/commit/bcf8e963))
- REST APIs discovery handlers ([#2109](https://github.com/waku-org/nwaku/issues/2109)) ([7ca516a5](https://github.com/waku-org/nwaku/commit/7ca516a5))
- implementing port 0 support ([#2125](https://github.com/waku-org/nwaku/issues/2125)) ([f7b9afc2](https://github.com/waku-org/nwaku/commit/f7b9afc2))
- messageHash attribute added in SQLite + testcase ([#2142](https://github.com/waku-org/nwaku/issues/2142))" ([#2154](https://github.com/waku-org/nwaku/issues/2154)) ([13aeebe4](https://github.com/waku-org/nwaku/commit/13aeebe4))
- messageHash attribute added in SQLite + testcase ([#2142](https://github.com/waku-org/nwaku/issues/2142)) ([9cd8c73d](https://github.com/waku-org/nwaku/commit/9cd8c73d))
- amending computeDigest func. + related test cases ([#2132](https://github.com/waku-org/nwaku/issues/2132)) ([1669f710](https://github.com/waku-org/nwaku/commit/1669f710))

### Bug Fixes

- typo ([6dd28063](https://github.com/waku-org/nwaku/commit/6dd28063))
- lightpush rest  ([#2176](https://github.com/waku-org/nwaku/issues/2176)) ([fa467e24](https://github.com/waku-org/nwaku/commit/fa467e24))
- **ci:** fix Docker tag for latest and release jobs ([52759faa](https://github.com/waku-org/nwaku/commit/52759faa))
- **rest:** fix bug in rest api when sending rln message ([#2169](https://github.com/waku-org/nwaku/issues/2169)) ([250e8b98](https://github.com/waku-org/nwaku/commit/250e8b98))
- updating v0.21.1 release date in changelog ([#2160](https://github.com/waku-org/nwaku/issues/2160)) ([3be61636](https://github.com/waku-org/nwaku/commit/3be61636))

### Changes

- Optimize postgres - prepared statements in select ([#2182](https://github.com/waku-org/nwaku/issues/2182)) ([6da1aeec](https://github.com/waku-org/nwaku/commit/6da1aeec))
- **release:** update changelog for v0.21.2 release ([#2188](https://github.com/waku-org/nwaku/issues/2188)) ([d0a93e7c](https://github.com/waku-org/nwaku/commit/d0a93e7c))
- upgrade dependencies v0.22 ([#2185](https://github.com/waku-org/nwaku/issues/2185)) ([b9563ae0](https://github.com/waku-org/nwaku/commit/b9563ae0))
- Optimize postgres - use of rowCallback approach ([#2171](https://github.com/waku-org/nwaku/issues/2171)) ([2b4ca4d0](https://github.com/waku-org/nwaku/commit/2b4ca4d0))
- **networking:** lower dhigh to limit amplification factor ([#2168](https://github.com/waku-org/nwaku/issues/2168)) ([f0f69b32](https://github.com/waku-org/nwaku/commit/f0f69b32))
- Minor Postgres optimizations ([#2166](https://github.com/waku-org/nwaku/issues/2166)) ([282c2e81](https://github.com/waku-org/nwaku/commit/282c2e81))
- adding patch release instructions to release doc ([#2157](https://github.com/waku-org/nwaku/issues/2157)) ([cc01bb07](https://github.com/waku-org/nwaku/commit/cc01bb07))
- **release:** update changelog for v0.21.1 release ([#2155](https://github.com/waku-org/nwaku/issues/2155)) ([b109a583](https://github.com/waku-org/nwaku/commit/b109a583))
- adding ext-multiaddr-only CLI flag ([#2141](https://github.com/waku-org/nwaku/issues/2141)) ([944dfdaa](https://github.com/waku-org/nwaku/commit/944dfdaa))
- bumping nim-libp2p to include WSS fix ([#2150](https://github.com/waku-org/nwaku/issues/2150)) ([817a7b2e](https://github.com/waku-org/nwaku/commit/817a7b2e))
- **cbindings:** avoid using global var in libwaku.nim ([#2118](https://github.com/waku-org/nwaku/issues/2118)) ([1e8f5771](https://github.com/waku-org/nwaku/commit/1e8f5771))
- adding postgres flag to manual docker job instructions ([#2139](https://github.com/waku-org/nwaku/issues/2139)) ([459331e3](https://github.com/waku-org/nwaku/commit/459331e3))

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |
| [`66/WAKU2-METADATA`](https://github.com/waku-org/specs/blob/master/standards/core/metadata.md) | `raw` | `/vac/waku/metadata/1.0.0` |

The Waku v1 implementation has been removed from this repository and can be found in a separate [Waku Legacy](https://github.com/waku-org/waku-legacy) repository.

## Upgrade instructions

* Note that the `--topic` CLI option is now deprecated in favor of a more specific options `--pubsub-topic` & `--content-topic`.

## v0.21.3 (2023-11-09)

This patch release adds the following feature:
- Adding generateRlnKeystore subcommand for RLN membership generation

### Features

- rln-keystore-generator is now a subcommand ([#2189](https://github.com/waku-org/nwaku/issues/2189)) ([1e919177](https://github.com/waku-org/nwaku/commit/1e919177))

This is a patch release that is fully backwards-compatible with release `v0.21.0`, `v0.21.1` and `v0.21.2`.

It supports the same [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |

The Waku v1 implementation has been removed from this repository and can be found in a separate [Waku Legacy](https://github.com/waku-org/waku-legacy) repository.

## v0.21.2 (2023-11-07)

This patch release addresses the following issue:
- Inability to send RLN messages through the REST API

### Bug Fixes

- **rest:** fix bug in rest api when sending rln message ([#2169](https://github.com/waku-org/nwaku/issues/2169)) ([33decd7a](https://github.com/waku-org/nwaku/commit/33decd7a))

This is a patch release that is fully backwards-compatible with release `v0.21.0` and `v0.21.1`.

It supports the same [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |

The Waku v1 implementation has been removed from this repository and can be found in a separate [Waku Legacy](https://github.com/waku-org/waku-legacy) repository.

## v0.21.1 (2023-10-26)

This patch release addresses the following issues:
- WSS connections being suddenly terminated under rare conditions
- Ability for the user to control announced multiaddresses

### Changes

- adding ext-multiaddr-only CLI flag ([#2141](https://github.com/waku-org/nwaku/issues/2141)) ([e2dfc2ed](https://github.com/waku-org/nwaku/commit/e2dfc2ed))
- bumping nim-libp2p to include WSS fix ([#2150](https://github.com/waku-org/nwaku/issues/2150)) ([18b5149a](https://github.com/waku-org/nwaku/commit/18b5149a))

This is a patch release that is fully backwards-compatible with release `v0.21.0`.

It supports the same [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |

The Waku v1 implementation has been removed from this repository and can be found in a separate [Waku Legacy](https://github.com/waku-org/waku-legacy) repository.

## v0.21.0 (2023-10-18)

> Note: This is the last release supporting the `--topic` option. It is being deprecated in favor of a more specific options `--pubsub-topic` & `--content-topic`

## What's Changed

Release highlights:
* Implemented a req/resp [protocol](https://github.com/waku-org/specs/blob/master/standards/core/metadata.md) that provides information about the node's medatadata
* Added REST APIs for Filter v2 and Lightpush protocols' services
* Ported /admin endpoint to REST
* Added a size-based retention policy for the user to set a limit for SQLite storage used

### Features

- add new metadata protocol ([#2062](https://github.com/waku-org/nwaku/issues/2062)) ([d5c3ade5](https://github.com/waku-org/nwaku/commit/d5c3ade5))
- /admin rest api endpoint  ([#2094](https://github.com/waku-org/nwaku/issues/2094)) ([7b5c36b1](https://github.com/waku-org/nwaku/commit/7b5c36b1))
- **coverage:** Add simple coverage ([#2067](https://github.com/waku-org/nwaku/issues/2067)) ([d864db3f](https://github.com/waku-org/nwaku/commit/d864db3f))
- added RELAY openapi definitions ([#2081](https://github.com/waku-org/nwaku/issues/2081)) ([56dbe2a7](https://github.com/waku-org/nwaku/commit/56dbe2a7))
- **wakucanary:** add latency measurement using ping protocol ([#2074](https://github.com/waku-org/nwaku/issues/2074)) ([6cb9a8da](https://github.com/waku-org/nwaku/commit/6cb9a8da))
- Autosharding API for RELAY subscriptions ([#1983](https://github.com/waku-org/nwaku/issues/1983)) ([1763b1ef](https://github.com/waku-org/nwaku/commit/1763b1ef))
- **networkmonitor:** add ping latencies, optimize reconnections ([#2068](https://github.com/waku-org/nwaku/issues/2068)) ([ed473545](https://github.com/waku-org/nwaku/commit/ed473545))
- peer manager can filter peers by shard ([#2063](https://github.com/waku-org/nwaku/issues/2063)) ([0d9e9fbd](https://github.com/waku-org/nwaku/commit/0d9e9fbd))
- lightpush rest api ([#2052](https://github.com/waku-org/nwaku/issues/2052)) ([02a814bd](https://github.com/waku-org/nwaku/commit/02a814bd))
- HTTP REST API: Filter support v2 ([#1890](https://github.com/waku-org/nwaku/issues/1890)) ([dac072f8](https://github.com/waku-org/nwaku/commit/dac072f8))

### Bug Fixes

- fix wrong install of filter rest api ([#2133](https://github.com/waku-org/nwaku/issues/2133)) ([5277d122](https://github.com/waku-org/nwaku/commit/5277d122))
- consider WS extMultiAddrs before publishing host address ([#2122](https://github.com/waku-org/nwaku/issues/2122)) ([a5b1cfd0](https://github.com/waku-org/nwaku/commit/a5b1cfd0))
- return erring response if lightpush request is invalid ([#2083](https://github.com/waku-org/nwaku/issues/2083)) ([2c5eb427](https://github.com/waku-org/nwaku/commit/2c5eb427))
- sqlite limited delete query bug ([#2111](https://github.com/waku-org/nwaku/issues/2111)) ([06bc433a](https://github.com/waku-org/nwaku/commit/06bc433a))
- cluster id & sharding terminology ([#2104](https://github.com/waku-org/nwaku/issues/2104)) ([a47dc9e6](https://github.com/waku-org/nwaku/commit/a47dc9e6))
- **ci:** update the dependency list in pre-release WF ([#2088](https://github.com/waku-org/nwaku/issues/2088)) ([e85f05b0](https://github.com/waku-org/nwaku/commit/e85f05b0))
- **ci:** fix name of discord notify method ([aaf10e08](https://github.com/waku-org/nwaku/commit/aaf10e08))
- update wakuv2 fleet DNS discovery enrtree ([89854a96](https://github.com/waku-org/nwaku/commit/89854a96))
- libwaku.nim: unsubscribe -> unsubscribeAll to make it build properly ([#2082](https://github.com/waku-org/nwaku/issues/2082)) ([3264a4f5](https://github.com/waku-org/nwaku/commit/3264a4f5))
- **archive:** dburl check ([#2071](https://github.com/waku-org/nwaku/issues/2071)) ([a27d005f](https://github.com/waku-org/nwaku/commit/a27d005f))
- filter discv5 bootstrap nodes by shards ([#2073](https://github.com/waku-org/nwaku/issues/2073)) ([d178105d](https://github.com/waku-org/nwaku/commit/d178105d))
- **rln-relay:** segfault when no params except rln-relay are passed in ([#2047](https://github.com/waku-org/nwaku/issues/2047)) ([45fe2d3b](https://github.com/waku-org/nwaku/commit/45fe2d3b))
- **sqlite:** Properly set user_version to 7 so that the migration procedure is not started ([#2031](https://github.com/waku-org/nwaku/issues/2031)) ([aa3e1a66](https://github.com/waku-org/nwaku/commit/aa3e1a66))

### Changes

- remove js-node tests as release candidate dependencies ([#2123](https://github.com/waku-org/nwaku/issues/2123)) ([ce5fb340](https://github.com/waku-org/nwaku/commit/ce5fb340))
- added size based retention policy ([#2098](https://github.com/waku-org/nwaku/issues/2098)) ([25d6e52e](https://github.com/waku-org/nwaku/commit/25d6e52e))
- Clarify running instructions ([#2038](https://github.com/waku-org/nwaku/issues/2038)) ([12e8b122](https://github.com/waku-org/nwaku/commit/12e8b122))
- **rln:** add more hardcoded memberhips to static group ([#2108](https://github.com/waku-org/nwaku/issues/2108)) ([1042cacd](https://github.com/waku-org/nwaku/commit/1042cacd))
- Revert lightpush error handling to allow zero peer publish again succeed ([#2099](https://github.com/waku-org/nwaku/issues/2099)) ([f05528d4](https://github.com/waku-org/nwaku/commit/f05528d4))
- adding NetConfig test suite ([#2091](https://github.com/waku-org/nwaku/issues/2091)) ([23b49ca5](https://github.com/waku-org/nwaku/commit/23b49ca5))
- **cbindings:** Adding cpp example that integrates the 'libwaku' ([#2079](https://github.com/waku-org/nwaku/issues/2079)) ([8455b8dd](https://github.com/waku-org/nwaku/commit/8455b8dd))
- **networkmonitor:** refactor setConnectedPeersMetrics, make it partially concurrent, add version ([#2080](https://github.com/waku-org/nwaku/issues/2080)) ([c5aa9704](https://github.com/waku-org/nwaku/commit/c5aa9704))
- resolving DNS IP and publishing it when no extIp is provided ([#2030](https://github.com/waku-org/nwaku/issues/2030)) ([7797b2cd](https://github.com/waku-org/nwaku/commit/7797b2cd))
- Adding -d:postgres flag when creating a Docker image for release and PRs ([#2076](https://github.com/waku-org/nwaku/issues/2076)) ([7a376f59](https://github.com/waku-org/nwaku/commit/7a376f59))
- Moved external APIs out of node ([#2069](https://github.com/waku-org/nwaku/issues/2069)) ([3e72e830](https://github.com/waku-org/nwaku/commit/3e72e830))
- bump nim-libp2p, nim-toml-serialization, nim-unicodedb, nim-unittest2, nim-websock, nim-zlib, & nimbus-build-system ([#2065](https://github.com/waku-org/nwaku/issues/2065)) ([dc25057a](https://github.com/waku-org/nwaku/commit/dc25057a))
- **ci:** add js-waku as a dependency for pre-release createion ([#2022](https://github.com/waku-org/nwaku/issues/2022)) ([28b04000](https://github.com/waku-org/nwaku/commit/28b04000))
- Updating nim-chronicles, nim-chronos, nim-presto, nimcrypto, nim-libp2p, and nim-nat-transversal ([#2043](https://github.com/waku-org/nwaku/issues/2043)) ([f617cd97](https://github.com/waku-org/nwaku/commit/f617cd97))
- **cbindings:** Thread-safe communication between the main thread and the Waku Thread ([#1978](https://github.com/waku-org/nwaku/issues/1978)) ([72f90663](https://github.com/waku-org/nwaku/commit/72f90663))
- **rln-relay:** logs, updated submodule, leaves_set metric ([#2024](https://github.com/waku-org/nwaku/issues/2024)) ([2e515a06](https://github.com/waku-org/nwaku/commit/2e515a06))
- **release:** update changelog for v0.20.0 release ([#2026](https://github.com/waku-org/nwaku/issues/2026)) ([9085b1b3](https://github.com/waku-org/nwaku/commit/9085b1b3))
- **postgres:** not loading the libpq library by default & better user feedback ([#2028](https://github.com/waku-org/nwaku/issues/2028)) ([e8602021](https://github.com/waku-org/nwaku/commit/e8602021))
- move SubscriptionManager under waku_core  ([#2025](https://github.com/waku-org/nwaku/issues/2025)) ([563b2b20](https://github.com/waku-org/nwaku/commit/563b2b20))
- **README:** List possible WSL Issue ([#1995](https://github.com/waku-org/nwaku/issues/1995)) ([ebe715e9](https://github.com/waku-org/nwaku/commit/ebe715e9))
- **ci:** add js-waku test to pre-release workflow ([#2017](https://github.com/waku-org/nwaku/issues/2017)) ([e8776fd6](https://github.com/waku-org/nwaku/commit/e8776fd6))

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |

The Waku v1 implementation has been removed from this repository and can be found in a separate [Waku Legacy](https://github.com/waku-org/waku-legacy) repository.

## Upgrade instructions

* Note that the `--topic` CLI option is being deprecated in favor of a more specific options `--pubsub-topic` & `--content-topic`. This is the last release supporting the `--topic` option.
* The size-based retention policy has been tested with SQLite storage and is still on validation phases for Postgres

## 2023-09-14 v0.20.0

> Note: IP address 0.0.0.0 is no longer advertised by a node

> Note: Multiple CLI options have been removed in this release, please see _Upgrade instructions_ section for details.

## What's Changed

Release highlights:
* RLN is now part of standard release (is no longer EXPERIMENTAL feature)
* Interop tests between nwaku and js-waku are now gating PRs and releases
* Libwaku has been made more threadsafe (1 out of 3 improvements applied.)
* Added autosharding option on various protocol APIs



### Features

- **rln-relay:** removed rln from experimental 🚀 ([#2001](https://github.com/waku-org/nwaku/issues/2001)) ([645b0343](https://github.com/waku-org/nwaku/commit/645b0343))
- Rest endoint /health for rln ([#2011](https://github.com/waku-org/nwaku/issues/2011)) ([fc6194bb](https://github.com/waku-org/nwaku/commit/fc6194bb))
- **rln_db_inspector:** create rln_db_inspector tool ([#1999](https://github.com/waku-org/nwaku/issues/1999)) ([ec42e2c7](https://github.com/waku-org/nwaku/commit/ec42e2c7))
- **relay:** ordered validator execution ([#1966](https://github.com/waku-org/nwaku/issues/1966)) ([debc5f19](https://github.com/waku-org/nwaku/commit/debc5f19))
- **discv5:** topic subscriptions update discv5 filter predicate ([#1918](https://github.com/waku-org/nwaku/issues/1918)) ([4539dfc7](https://github.com/waku-org/nwaku/commit/4539dfc7))
- topic subscriptions updates discv5 ENR ([#1875](https://github.com/waku-org/nwaku/issues/1875)) ([c369b329](https://github.com/waku-org/nwaku/commit/c369b329))
- **rln_keystore_generator:** wired to onchain group manager ([#1931](https://github.com/waku-org/nwaku/issues/1931)) ([c9b48ea1](https://github.com/waku-org/nwaku/commit/c9b48ea1))
- **rln:** init rln_keystore_generator ([#1925](https://github.com/waku-org/nwaku/issues/1925)) ([3d849541](https://github.com/waku-org/nwaku/commit/3d849541))
- update various protocols to autoshard ([#1857](https://github.com/waku-org/nwaku/issues/1857)) ([cf301396](https://github.com/waku-org/nwaku/commit/cf301396))

### Bug Fixes

- **rln-relay:** waku_rln_number_registered_memberships metrics appropriately handled ([#2018](https://github.com/waku-org/nwaku/issues/2018)) ([a4e78330](https://github.com/waku-org/nwaku/commit/a4e78330))
- prevent IP 0.0.0.0 from being published and update peers with empty ENR data ([#1982](https://github.com/waku-org/nwaku/issues/1982)) ([47ae19c1](https://github.com/waku-org/nwaku/commit/47ae19c1))
- **rln-relay:** missed roots during sync ([#2015](https://github.com/waku-org/nwaku/issues/2015)) ([21604e6b](https://github.com/waku-org/nwaku/commit/21604e6b))
- **p2p:** fix possible connectivity issue ([#1996](https://github.com/waku-org/nwaku/issues/1996)) ([7d9d8a3f](https://github.com/waku-org/nwaku/commit/7d9d8a3f))
- **rln-db-inspector:** use valueOr pattern ([#2012](https://github.com/waku-org/nwaku/issues/2012)) ([a8095d87](https://github.com/waku-org/nwaku/commit/a8095d87))
- **tests:** relay tests use random port to avoid conflict ([#1998](https://github.com/waku-org/nwaku/issues/1998)) ([b991682b](https://github.com/waku-org/nwaku/commit/b991682b))
- **ci:** incorrect use of braces ([#1987](https://github.com/waku-org/nwaku/issues/1987)) ([4ed41457](https://github.com/waku-org/nwaku/commit/4ed41457))
- **Makefile:** invalid path to crate build ([#1981](https://github.com/waku-org/nwaku/issues/1981)) ([1a318c29](https://github.com/waku-org/nwaku/commit/1a318c29))
- --topic should be ignore when using --pubsub-topic or --content-topic ([#1977](https://github.com/waku-org/nwaku/issues/1977)) ([037b1662](https://github.com/waku-org/nwaku/commit/037b1662))
- **tests:** fix flaky test ([#1972](https://github.com/waku-org/nwaku/issues/1972)) ([f262397d](https://github.com/waku-org/nwaku/commit/f262397d))
- **rln-relay:** deserialization of valid merkle roots ([#1973](https://github.com/waku-org/nwaku/issues/1973)) ([d262837e](https://github.com/waku-org/nwaku/commit/d262837e))
- **ci:** rename tools artifact to prevent conflict ([#1971](https://github.com/waku-org/nwaku/issues/1971)) ([26c06b27](https://github.com/waku-org/nwaku/commit/26c06b27))
- **Makefile:** rln was enabled by default ([#1964](https://github.com/waku-org/nwaku/issues/1964)) ([9b1d2904](https://github.com/waku-org/nwaku/commit/9b1d2904))
- **rln-relay:** modify keystore credentials logic ([#1956](https://github.com/waku-org/nwaku/issues/1956)) ([e7b2b88f](https://github.com/waku-org/nwaku/commit/e7b2b88f))
- **Makefile:** error out if rln-keystore-generator not compiled with rln flag ([#1960](https://github.com/waku-org/nwaku/issues/1960)) ([ac258550](https://github.com/waku-org/nwaku/commit/ac258550))
- **rln-relay:** sync from deployed block number ([#1955](https://github.com/waku-org/nwaku/issues/1955)) ([bd3be219](https://github.com/waku-org/nwaku/commit/bd3be219))
- **rln-relay:** window of acceptable roots synced to rln metadata ([#1953](https://github.com/waku-org/nwaku/issues/1953)) ([01634f57](https://github.com/waku-org/nwaku/commit/01634f57))
- **rln-relay:** bump zerokit to v0.3.2 ([#1951](https://github.com/waku-org/nwaku/issues/1951)) ([32aa1c5b](https://github.com/waku-org/nwaku/commit/32aa1c5b))
- **rln-relay:** flush_interval incorrectly set ([#1933](https://github.com/waku-org/nwaku/issues/1933)) ([c07d63db](https://github.com/waku-org/nwaku/commit/c07d63db))
- **rln-relay:** RLN DB should be aware of chain and contract address ([#1932](https://github.com/waku-org/nwaku/issues/1932)) ([1ae5b5a9](https://github.com/waku-org/nwaku/commit/1ae5b5a9))
- **rln-relay:** waitFor startup, otherwise valid proofs will be marked invalid ([#1920](https://github.com/waku-org/nwaku/issues/1920)) ([6c6302f9](https://github.com/waku-org/nwaku/commit/6c6302f9))
- **test:** fix flaky rln test ([#1923](https://github.com/waku-org/nwaku/issues/1923)) ([0ac8a7f0](https://github.com/waku-org/nwaku/commit/0ac8a7f0))
- **rln-relay:** remove registration capability ([#1916](https://github.com/waku-org/nwaku/issues/1916)) ([f08315cd](https://github.com/waku-org/nwaku/commit/f08315cd))
- **rln-relay:** invalid start index being set results in invalid proofs  ([#1915](https://github.com/waku-org/nwaku/issues/1915)) ([b3bb7a11](https://github.com/waku-org/nwaku/commit/b3bb7a11))
- **rln-relay:** should error out on rln-relay mount failure ([#1904](https://github.com/waku-org/nwaku/issues/1904)) ([8c568cab](https://github.com/waku-org/nwaku/commit/8c568cab))
- **rln-relay:** timeout on macos runners, use fixed version of ganache ([#1913](https://github.com/waku-org/nwaku/issues/1913)) ([c9772af0](https://github.com/waku-org/nwaku/commit/c9772af0))
- no enr record in chat2 ([#1907](https://github.com/waku-org/nwaku/issues/1907)) ([fc604ca5](https://github.com/waku-org/nwaku/commit/fc604ca5))

### Changes

- **ci:** add js-waku test to pre-release workflow ([#2017](https://github.com/waku-org/nwaku/issues/2017)) ([e8776fd6](https://github.com/waku-org/nwaku/commit/e8776fd6))
- **rln-relay:** updated docs ([#1993](https://github.com/waku-org/nwaku/issues/1993)) ([76e34077](https://github.com/waku-org/nwaku/commit/76e34077))
- **ci:** execute js-waku integration tests on image build ([#2006](https://github.com/waku-org/nwaku/issues/2006)) ([5d976df9](https://github.com/waku-org/nwaku/commit/5d976df9))
- **rln-relay:** add isReady check ([#1989](https://github.com/waku-org/nwaku/issues/1989)) ([5638bd06](https://github.com/waku-org/nwaku/commit/5638bd06))
- **rln-relay:** clean up nullifier table every MaxEpochGap ([#1994](https://github.com/waku-org/nwaku/issues/1994)) ([483f40c8](https://github.com/waku-org/nwaku/commit/483f40c8))
- **ci:** use commit instead of master for docker image ([#1990](https://github.com/waku-org/nwaku/issues/1990)) ([98850192](https://github.com/waku-org/nwaku/commit/98850192))
- **rln-relay:** log levels for certain logs ([#1986](https://github.com/waku-org/nwaku/issues/1986)) ([97a7c9d0](https://github.com/waku-org/nwaku/commit/97a7c9d0))
- **rln-relay:** use the only key from keystore if only 1 exists ([#1984](https://github.com/waku-org/nwaku/issues/1984)) ([a14c3261](https://github.com/waku-org/nwaku/commit/a14c3261))
- **ci:** enable experimental for the PR image builds ([#1976](https://github.com/waku-org/nwaku/issues/1976)) ([1b835b4e](https://github.com/waku-org/nwaku/commit/1b835b4e))
- **rln-relay:** confirm that the provided credential is correct using onchain query ([#1980](https://github.com/waku-org/nwaku/issues/1980)) ([be48891f](https://github.com/waku-org/nwaku/commit/be48891f))
- **api:** validate rln message before sending (rest + rpc) ([#1968](https://github.com/waku-org/nwaku/issues/1968)) ([05c98864](https://github.com/waku-org/nwaku/commit/05c98864))
- **cbindings:** Thread-safe libwaku. WakuNode instance created directly from the Waku Thread ([#1957](https://github.com/waku-org/nwaku/issues/1957)) ([68e8d9a7](https://github.com/waku-org/nwaku/commit/68e8d9a7))
- add debug log indicating succesful message pushes and also log the message hash ([#1965](https://github.com/waku-org/nwaku/issues/1965)) ([e272bec9](https://github.com/waku-org/nwaku/commit/e272bec9))
- **rln-keystore-generator:** log out the membership index upon registration ([#1963](https://github.com/waku-org/nwaku/issues/1963)) ([7d53aec1](https://github.com/waku-org/nwaku/commit/7d53aec1))
- **rln-relay:** integrate waku rln registry ([#1943](https://github.com/waku-org/nwaku/issues/1943)) ([cc9f8d42](https://github.com/waku-org/nwaku/commit/cc9f8d42))
- **ci:** add a job checking config options and db schema ([#1927](https://github.com/waku-org/nwaku/issues/1927)) ([505d1967](https://github.com/waku-org/nwaku/commit/505d1967))
- **rln_keystore_generator:** generate and persist credentials ([#1928](https://github.com/waku-org/nwaku/issues/1928)) ([07945a37](https://github.com/waku-org/nwaku/commit/07945a37))
- **rln-relay:** rename keystore application to waku-rln-relay ([#1924](https://github.com/waku-org/nwaku/issues/1924)) ([8239b455](https://github.com/waku-org/nwaku/commit/8239b455))
- **rln:** remove old and add new rln metric ([#1926](https://github.com/waku-org/nwaku/issues/1926)) ([56c228f8](https://github.com/waku-org/nwaku/commit/56c228f8))
- **rln:** run rln in all relay pubsubtopics + remove cli flags ([#1917](https://github.com/waku-org/nwaku/issues/1917)) ([af95b571](https://github.com/waku-org/nwaku/commit/af95b571))
- **release:** update changelog for delayed v0.19.0 release ([#1911](https://github.com/waku-org/nwaku/issues/1911)) ([78690787](https://github.com/waku-org/nwaku/commit/78690787))

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |

The Waku v1 implementation has been removed from this repository and can be found in a separate [Waku Legacy](https://github.com/waku-org/waku-legacy) repository.

## Upgrade instructions

* Note that the `--topic` CLI option is being deprecated in favor of a more specific options `--pubsub-topic` & `--content-topic`. The `--topic` option will be available for next release with a deprecation note.
* CLI option `--store-resume-peer` has been removed.
* Following options related to RLN have been removed:
  * `--rln-relay-membership-group-index`
  * `--rln-relay-pubsub-topic`
  * `--rln-relay-content-topic`


## 2023-08-16 v0.19.0

> Note that the `--topic` CLI option is being deprecated in favor a more specific option `--pubsub-topic`.

> The Waku v1 implementation has been removed from this repository and can be found in a separate [Waku Legacy](https://github.com/waku-org/waku-legacy) repository.


## What's Changed

Release highlights:
* Improved connection management, including management for non-relay peers and limiting the number of connections from a single IP
* Postgres support has been added as a backend for archive module
* RLN initialization optimizations
* Update to the latest nim-libp2p
* Removed Waku v1 and also references to `v2` from the current version
* Basic implementation of Autosharding for the Waku Network
* REST API implementation for Filter protocol

### Features

- **ci:** add docker image builds per PR ([#1881](https://github.com/waku-org/nwaku/issues/1881)) ([84f94d5d](https://github.com/waku-org/nwaku/commit/84f94d5d))
- Rest API interface for legacy (v1) filter service.  ([#1851](https://github.com/waku-org/nwaku/issues/1851)) ([08ff6672](https://github.com/waku-org/nwaku/commit/08ff6672))
- autosharding content topics in config ([#1856](https://github.com/waku-org/nwaku/issues/1856)) ([afb93e29](https://github.com/waku-org/nwaku/commit/afb93e29))
- autosharding core algorithm ([#1854](https://github.com/waku-org/nwaku/issues/1854)) ([bbff1ac1](https://github.com/waku-org/nwaku/commit/bbff1ac1))
- **cbindings:** tiny waku relay example in Python ([#1793](https://github.com/waku-org/nwaku/issues/1793)) ([0b2cfae5](https://github.com/waku-org/nwaku/commit/0b2cfae5))
- **rln-relay:** close db connection appropriately ([#1858](https://github.com/waku-org/nwaku/issues/1858)) ([76c73b62](https://github.com/waku-org/nwaku/commit/76c73b62))
- enable TcpNoDelay ([#1470](https://github.com/waku-org/nwaku/issues/1470)) ([08f3bba3](https://github.com/waku-org/nwaku/commit/08f3bba3))
- limit relay connections below max conns ([#1813](https://github.com/waku-org/nwaku/issues/1813)) ([17b24cde](https://github.com/waku-org/nwaku/commit/17b24cde))
- **postgres:** integration of postgres in wakunode2 ([#1808](https://github.com/waku-org/nwaku/issues/1808)) ([88b7481f](https://github.com/waku-org/nwaku/commit/88b7481f))
- discovery peer filtering for relay shard ([#1804](https://github.com/waku-org/nwaku/issues/1804)) ([a4da87bb](https://github.com/waku-org/nwaku/commit/a4da87bb))
- **rln-relay:** resume onchain sync from persisted tree db ([#1805](https://github.com/waku-org/nwaku/issues/1805)) ([bbded9ee](https://github.com/waku-org/nwaku/commit/bbded9ee))
- **rln-relay:** metadata ffi api ([#1803](https://github.com/waku-org/nwaku/issues/1803)) ([045f07c6](https://github.com/waku-org/nwaku/commit/045f07c6))

### Bug Fixes

- bring back default topic in config ([#1902](https://github.com/waku-org/nwaku/issues/1902)) ([d5d2243c](https://github.com/waku-org/nwaku/commit/d5d2243c))
- **ci:** only add comment on PR and do not duplicate it ([#1908](https://github.com/waku-org/nwaku/issues/1908)) ([b785b6ba](https://github.com/waku-org/nwaku/commit/b785b6ba))
- **ci:** add mising OS arch option to image build ([#1905](https://github.com/waku-org/nwaku/issues/1905)) ([2575f3c4](https://github.com/waku-org/nwaku/commit/2575f3c4))
- **wakucanary:** add missing return on timeout ([#1901](https://github.com/waku-org/nwaku/issues/1901)) ([7dce0b9e](https://github.com/waku-org/nwaku/commit/7dce0b9e))
- fixes out of bounds crash when waku2 is not set ([#1895](https://github.com/waku-org/nwaku/issues/1895)) ([03363f1b](https://github.com/waku-org/nwaku/commit/03363f1b))
- **wakucanary:** add enr record to builder ([#1882](https://github.com/waku-org/nwaku/issues/1882)) ([831a093f](https://github.com/waku-org/nwaku/commit/831a093f))
- check nil before calling clearTimer ([#1869](https://github.com/waku-org/nwaku/issues/1869)) ([2fc48842](https://github.com/waku-org/nwaku/commit/2fc48842))
- **rln-relay:** mark duplicated messages as spam ([#1867](https://github.com/waku-org/nwaku/issues/1867)) ([4756ccc1](https://github.com/waku-org/nwaku/commit/4756ccc1))
- **ci:** do not depend on number of procesors with job name ([#1863](https://github.com/waku-org/nwaku/issues/1863)) ([c560af11](https://github.com/waku-org/nwaku/commit/c560af11))
- **libp2p:** Updating nim-libp2p to fix the `wss` connectivity issue ([#1848](https://github.com/waku-org/nwaku/issues/1848)) ([1d3410c7](https://github.com/waku-org/nwaku/commit/1d3410c7))
- **rln-relay:** chunk event fetching ([#1830](https://github.com/waku-org/nwaku/issues/1830)) ([e4d9ee1f](https://github.com/waku-org/nwaku/commit/e4d9ee1f))
- **discv5:** Fixing issue that prevented the wakunode2 from starting ([#1829](https://github.com/waku-org/nwaku/issues/1829)) ([3aefade6](https://github.com/waku-org/nwaku/commit/3aefade6))
- sanity-check the docker image start ([ae05f0a8](https://github.com/waku-org/nwaku/commit/ae05f0a8))
- **ci:** fix broken test with wrong import ([#1820](https://github.com/waku-org/nwaku/issues/1820)) ([4573e8c5](https://github.com/waku-org/nwaku/commit/4573e8c5))
- temporary fix to disable default experimental builds on fleets ([#1810](https://github.com/waku-org/nwaku/issues/1810)) ([e9028618](https://github.com/waku-org/nwaku/commit/e9028618))
- **rln-relay:** tree race condition upon initialization ([#1807](https://github.com/waku-org/nwaku/issues/1807)) ([f8e270fb](https://github.com/waku-org/nwaku/commit/f8e270fb))
- fix mac docker build alpine version ([#1801](https://github.com/waku-org/nwaku/issues/1801)) ([fce845bb](https://github.com/waku-org/nwaku/commit/fce845bb))
- **rln-relay:** flaky static group manager test ([#1798](https://github.com/waku-org/nwaku/issues/1798)) ([0e9ecbd6](https://github.com/waku-org/nwaku/commit/0e9ecbd6))

### Changes

- remove references to v2 ([#1898](https://github.com/waku-org/nwaku/issues/1898)) ([b9d5d28a](https://github.com/waku-org/nwaku/commit/b9d5d28a))
- **submodules:** use zerokit v0.3.1 only ([#1886](https://github.com/waku-org/nwaku/issues/1886)) ([311f5ea0](https://github.com/waku-org/nwaku/commit/311f5ea0))
- remove Waku v1 and wakubridge code ([#1874](https://github.com/waku-org/nwaku/issues/1874)) ([ab344a9d](https://github.com/waku-org/nwaku/commit/ab344a9d))
- **cbindings:** libwaku - run waku node in a secondary working thread  ([#1865](https://github.com/waku-org/nwaku/issues/1865)) ([069c1ad2](https://github.com/waku-org/nwaku/commit/069c1ad2))
- update docs link ([#1850](https://github.com/waku-org/nwaku/issues/1850)) ([d2b6075b](https://github.com/waku-org/nwaku/commit/d2b6075b))
- **changelog:** release notes for v0.19.0 ([#1861](https://github.com/waku-org/nwaku/issues/1861)) ([32c1276f](https://github.com/waku-org/nwaku/commit/32c1276f))
- **rln-relay:** verify proofs based on bandwidth usage ([#1844](https://github.com/waku-org/nwaku/issues/1844)) ([3fe4522a](https://github.com/waku-org/nwaku/commit/3fe4522a))
- **rln-relay:** bump zerokit ([#1838](https://github.com/waku-org/nwaku/issues/1838)) ([4f0bdf9a](https://github.com/waku-org/nwaku/commit/4f0bdf9a))
- bump nim-libp2p to 224f92e ([661638da](https://github.com/waku-org/nwaku/commit/661638da))
- **refactor:** Move record creation & fix libwaku compilation ([#1833](https://github.com/waku-org/nwaku/issues/1833)) ([97d3b9f7](https://github.com/waku-org/nwaku/commit/97d3b9f7))
- discv5 re-org clean-up ([#1823](https://github.com/waku-org/nwaku/issues/1823)) ([cf46fb7c](https://github.com/waku-org/nwaku/commit/cf46fb7c))
- **networking:** disconnect due to colocation ip in conn handler ([#1821](https://github.com/waku-org/nwaku/issues/1821)) ([e12c979c](https://github.com/waku-org/nwaku/commit/e12c979c))
- **rln-relay:** bump zerokit for version fix ([#1822](https://github.com/waku-org/nwaku/issues/1822)) ([add294a9](https://github.com/waku-org/nwaku/commit/add294a9))
- move discv5 out of node. ([#1818](https://github.com/waku-org/nwaku/issues/1818)) ([62d36530](https://github.com/waku-org/nwaku/commit/62d36530))
- **archive:** Moving waku archive logic from app.nim to the archive module ([#1817](https://github.com/waku-org/nwaku/issues/1817)) ([52894a82](https://github.com/waku-org/nwaku/commit/52894a82))
- add peer manager config to builder ([#1816](https://github.com/waku-org/nwaku/issues/1816)) ([71c4ac16](https://github.com/waku-org/nwaku/commit/71c4ac16))
- discv5 re-org setup ([#1815](https://github.com/waku-org/nwaku/issues/1815)) ([44f9d8dc](https://github.com/waku-org/nwaku/commit/44f9d8dc))
- **databases:** Creation of the databases folder to keep the logic for sqlite and postgres ([#1811](https://github.com/waku-org/nwaku/issues/1811)) ([a44d4bfb](https://github.com/waku-org/nwaku/commit/a44d4bfb))
- **deps:** bump libp2p & websock ([#1800](https://github.com/waku-org/nwaku/issues/1800)) ([f6e89c31](https://github.com/waku-org/nwaku/commit/f6e89c31))

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |

The Waku v1 implementation has been removed from this repository and can be found in a separate [Waku Legacy](https://github.com/waku-org/waku-legacy) repository.

## Upgrade instructions

* Note that the `--topic` CLI option is being deprecated in favor a more specific option `--pubsub-topic`. The `--topic` option will be available for next 2 releases with a deprecation note.

## 2023-06-14 v0.18.0

> Note that there is a new naming scheme for release artifacts.

## What's Changed

Release highlights:
* Support for Gossipsub scoring
* [Rendezvous discovery protocol](https://docs.libp2p.io/concepts/discovery-routing/rendezvous/) enabled by default with relay
* Initial support for postgresql as Store backend
* Atomic operations for insertions and deletions included in rln-relay

### Features

- **postgres:** complete implementation of driver and apply more tests ([#1785](https://github.com/waku-org/nwaku/issues/1785)) ([5fc5770d](https://github.com/waku-org/nwaku/commit/5fc5770d))
- **postgres:** adding a postgres async pool to make the db interactions asynchronous ([#1779](https://github.com/waku-org/nwaku/issues/1779)) ([cb2e3d86](https://github.com/waku-org/nwaku/commit/cb2e3d86))
- **rln-relay:** pass in index to keystore credentials ([#1777](https://github.com/waku-org/nwaku/issues/1777)) ([a00aa8cc](https://github.com/waku-org/nwaku/commit/a00aa8cc))
- **networking:** integrate gossipsub scoring ([#1769](https://github.com/waku-org/nwaku/issues/1769)) ([34a92631](https://github.com/waku-org/nwaku/commit/34a92631))
- **discv5:** added find random nodes with predicate ([#1762](https://github.com/waku-org/nwaku/issues/1762)) ([#1763](https://github.com/waku-org/nwaku/issues/1763)) ([21737c7c](https://github.com/waku-org/nwaku/commit/21737c7c))
- **wakunode2:** enable libp2p rendezvous protocol by default ([#1770](https://github.com/waku-org/nwaku/issues/1770)) ([835a409d](https://github.com/waku-org/nwaku/commit/835a409d))
- **postgresql:** align previous work's PR[#1590](https://github.com/waku-org/nwaku/issues/1590) changes into master ([#1764](https://github.com/waku-org/nwaku/issues/1764)) ([7df6f4c8](https://github.com/waku-org/nwaku/commit/7df6f4c8))
- **networking:** prune peers from same ip beyond collocation limit ([#1765](https://github.com/waku-org/nwaku/issues/1765)) ([047d1cf0](https://github.com/waku-org/nwaku/commit/047d1cf0))
- **ci:** add nightly builds ([#1758](https://github.com/waku-org/nwaku/issues/1758)) ([473af70a](https://github.com/waku-org/nwaku/commit/473af70a))
- **postgresql:** 1st commit to async sql (waku_archive/driver...) ([#1755](https://github.com/waku-org/nwaku/issues/1755)) ([59ca03a8](https://github.com/waku-org/nwaku/commit/59ca03a8))
- **ci:** add release-notes target ([#1734](https://github.com/waku-org/nwaku/issues/1734)) ([ceb54b18](https://github.com/waku-org/nwaku/commit/ceb54b18))
- **rln-relay:** use new atomic_operation ffi api ([#1733](https://github.com/waku-org/nwaku/issues/1733)) ([611e9539](https://github.com/waku-org/nwaku/commit/611e9539))

### Bug Fixes

- **ci:** enforce basic CPU instruction set to prevent CI issues ([#1759](https://github.com/waku-org/nwaku/issues/1759)) ([35520bd0](https://github.com/waku-org/nwaku/commit/35520bd0))
- **test:** wait more for gossip ([#1753](https://github.com/waku-org/nwaku/issues/1753)) ([0fce3d83](https://github.com/waku-org/nwaku/commit/0fce3d83))
- **rln-relay:** keystore usage ([#1750](https://github.com/waku-org/nwaku/issues/1750)) ([36266b43](https://github.com/waku-org/nwaku/commit/36266b43))
- **ci:** fix flaky test for dos topic ([#1747](https://github.com/waku-org/nwaku/issues/1747)) ([46e231d0](https://github.com/waku-org/nwaku/commit/46e231d0))
- **rln-relay:** trace log ([#1743](https://github.com/waku-org/nwaku/issues/1743)) ([5eae60e8](https://github.com/waku-org/nwaku/commit/5eae60e8))
- **ci:** make experimental default to true in fleet deployment ([#1742](https://github.com/waku-org/nwaku/issues/1742)) ([b148c305](https://github.com/waku-org/nwaku/commit/b148c305))

### Changes

- **rln:** bump zerokit ([#1787](https://github.com/waku-org/nwaku/issues/1787)) ([9c04b59b](https://github.com/waku-org/nwaku/commit/9c04b59b))
- **ci:** extend and rename nightly workflow to support RC builds ([#1784](https://github.com/waku-org/nwaku/issues/1784)) ([96074071](https://github.com/waku-org/nwaku/commit/96074071))
- **rln-relay:** pass in the path to the tree db ([#1782](https://github.com/waku-org/nwaku/issues/1782)) ([dba84248](https://github.com/waku-org/nwaku/commit/dba84248))
- **rln-relay:** update tree_config ([#1781](https://github.com/waku-org/nwaku/issues/1781)) ([ba8ec704](https://github.com/waku-org/nwaku/commit/ba8ec704))
- **ci:** properly set os and architecture for nightly and release ([#1780](https://github.com/waku-org/nwaku/issues/1780)) ([44bcf0f2](https://github.com/waku-org/nwaku/commit/44bcf0f2))
- **ci:** remove add-to-project workflow ([#1778](https://github.com/waku-org/nwaku/issues/1778)) ([a9505892](https://github.com/waku-org/nwaku/commit/a9505892))
- **ci:** add experimental builds to nightly ([#1761](https://github.com/waku-org/nwaku/issues/1761)) ([ffac7761](https://github.com/waku-org/nwaku/commit/ffac7761))
- **px:** close px streams after resp is sent ([#1746](https://github.com/waku-org/nwaku/issues/1746)) ([3c2d2891](https://github.com/waku-org/nwaku/commit/3c2d2891))
- **docs:** fix docs and mark some as deprecated ([#1754](https://github.com/waku-org/nwaku/issues/1754)) ([b51fb616](https://github.com/waku-org/nwaku/commit/b51fb616))
- **makefile:** unify where chronicles_log_level is set ([#1748](https://github.com/waku-org/nwaku/issues/1748)) ([39902dc2](https://github.com/waku-org/nwaku/commit/39902dc2))
- **rln-relay:** docs and config update for testnet 3 ([#1738](https://github.com/waku-org/nwaku/issues/1738)) ([bb9d231b](https://github.com/waku-org/nwaku/commit/bb9d231b))
- **rln-relay:** update metrics dashboard ([#1745](https://github.com/waku-org/nwaku/issues/1745)) ([0ced2195](https://github.com/waku-org/nwaku/commit/0ced2195))
- **rln-relay:** updated metrics for testnet 3 ([#1744](https://github.com/waku-org/nwaku/issues/1744)) ([62578746](https://github.com/waku-org/nwaku/commit/62578746))
- **networking:** set and use target outbound connections + prune ([#1739](https://github.com/waku-org/nwaku/issues/1739)) ([87f694a8](https://github.com/waku-org/nwaku/commit/87f694a8))
- proper use of setupNat ([#1740](https://github.com/waku-org/nwaku/issues/1740)) ([665484c1](https://github.com/waku-org/nwaku/commit/665484c1))

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |

The Waku v1 implementation is stable but not under active development.

## Upgrade instructions

There is a new naming scheme for release artifacts - `nwaku-${ARCHITECTURE}-${OS}-${VERSION}.tar.gz`. If you use any automation to download latest release, you may need to update it.

The `--topics` config option has been deprecated to unify the configuration style. It is still available in this release but will be removed in the next one. The new option `--topic` is introduced, which can be used repeatedly to achieve the same behavior.

## 2023-05-17 v0.17.0

> Note that the --topics config item has been deprecated and support will be dropped in future releases. To configure support for multiple pubsub topics, use the new --topic parameter repeatedly.

## What's Changed

Release highlights:
* New REST API for Waku Store protocol.
* New Filter protocol implentation. See [12/WAKU2-FILTER](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md).
* Initial C bindings support.
* Support for Heaptrack to investigate memory utilization ([tutorial](https://github.com/waku-org/nwaku/blob/master/docs/tutorial/heaptrack.md)).

### Features

- **cbindings:** first commit - waku relay ([#1632](https://github.com/waku-org/nwaku/issues/1632)) ([#1714](https://github.com/waku-org/nwaku/issues/1714)) ([2defbd23](https://github.com/waku-org/nwaku/commit/2defbd23))
- example using filter and lightpush ([#1720](https://github.com/waku-org/nwaku/issues/1720)) ([8987d4a3](https://github.com/waku-org/nwaku/commit/8987d4a3))
- configure protected topics via cli ([#1696](https://github.com/waku-org/nwaku/issues/1696)) ([16b44523](https://github.com/waku-org/nwaku/commit/16b44523))
- **mem-analysis:** Adding Dockerfile_with_heaptrack ([#1681](https://github.com/waku-org/nwaku/issues/1681)) ([9b9172ab](https://github.com/waku-org/nwaku/commit/9b9172ab))
- add metrics with msg size histogram ([#1697](https://github.com/waku-org/nwaku/issues/1697)) ([67e96ba8](https://github.com/waku-org/nwaku/commit/67e96ba8))
- curate peers shared over px protocol ([#1671](https://github.com/waku-org/nwaku/issues/1671)) ([14305c61](https://github.com/waku-org/nwaku/commit/14305c61))
- **enr:** added support for relay shards field ([96162536](https://github.com/waku-org/nwaku/commit/96162536))
- add tools maket target and build tools in CI ([#1668](https://github.com/waku-org/nwaku/issues/1668)) ([d5979e94](https://github.com/waku-org/nwaku/commit/d5979e94))
- integrate new filter protocol, other improvements ([#1637](https://github.com/waku-org/nwaku/issues/1637)) ([418efca2](https://github.com/waku-org/nwaku/commit/418efca2))
- **rest-api-store:** new rest api to retrieve store waku messages ([#1611](https://github.com/waku-org/nwaku/issues/1611)) ([#1630](https://github.com/waku-org/nwaku/issues/1630)) ([b2acb54d](https://github.com/waku-org/nwaku/commit/b2acb54d))
- **node:** added waku node builder type ([e931fa5d](https://github.com/waku-org/nwaku/commit/e931fa5d))
- dos protected topic relay msgs based on meta field ([#1614](https://github.com/waku-org/nwaku/issues/1614)) ([c26dcb2b](https://github.com/waku-org/nwaku/commit/c26dcb2b))
- further filter improvements ([#1617](https://github.com/waku-org/nwaku/issues/1617)) ([d920b973](https://github.com/waku-org/nwaku/commit/d920b973))
- **common:** added extensible implementation of the enr typed record ([ac56e1dc](https://github.com/waku-org/nwaku/commit/ac56e1dc))
- **rln-relay:** fetch release from zerokit ci, or build ([#1603](https://github.com/waku-org/nwaku/issues/1603)) ([179be681](https://github.com/waku-org/nwaku/commit/179be681))
- **filter-v2:** new filter protocol increment - message handling and clients ([#1600](https://github.com/waku-org/nwaku/issues/1600)) ([be446b98](https://github.com/waku-org/nwaku/commit/be446b98))

### Fixes

- **ci:** remove target flag from docker command ([#1725](https://github.com/waku-org/nwaku/issues/1725)) ([d822cdc5](https://github.com/waku-org/nwaku/commit/d822cdc5))
- wakunode2 config. adding new 'topic' config parameter. ([#1727](https://github.com/waku-org/nwaku/issues/1727)) ([2ec9809c](https://github.com/waku-org/nwaku/commit/2ec9809c))
- streams was used instead of connections ([#1722](https://github.com/waku-org/nwaku/issues/1722)) ([b9e0763e](https://github.com/waku-org/nwaku/commit/b9e0763e))
- change filter request default behaviour to ping ([#1721](https://github.com/waku-org/nwaku/issues/1721)) ([7c39be9a](https://github.com/waku-org/nwaku/commit/7c39be9a))
- **rln-relay:** handle invalid deletes ([#1717](https://github.com/waku-org/nwaku/issues/1717)) ([81dffee8](https://github.com/waku-org/nwaku/commit/81dffee8))
- fix filter v2 proto fields ([#1716](https://github.com/waku-org/nwaku/issues/1716)) ([68a39c65](https://github.com/waku-org/nwaku/commit/68a39c65))
- unstable peers in mesh ([#1710](https://github.com/waku-org/nwaku/issues/1710)) ([703c3ab5](https://github.com/waku-org/nwaku/commit/703c3ab5))
- **networkmonitor:** break import dependency with wakunode2 app ([043feacd](https://github.com/waku-org/nwaku/commit/043feacd))
- import nimchronos instead heartbeat ([#1695](https://github.com/waku-org/nwaku/issues/1695)) ([7d12adf6](https://github.com/waku-org/nwaku/commit/7d12adf6))
- **rest:** change rest server result error type to string ([d5ef9331](https://github.com/waku-org/nwaku/commit/d5ef9331))
- **rln-relay:** scope of getEvents ([#1672](https://github.com/waku-org/nwaku/issues/1672)) ([b62193e5](https://github.com/waku-org/nwaku/commit/b62193e5))
- **logs:** fix log reporting wrong ok connected peers ([#1675](https://github.com/waku-org/nwaku/issues/1675)) ([1a885b96](https://github.com/waku-org/nwaku/commit/1a885b96))
- move canBeConnected to PeerManager and check for potential overflow ([#1670](https://github.com/waku-org/nwaku/issues/1670)) ([d5c2770c](https://github.com/waku-org/nwaku/commit/d5c2770c))
- wrap untracked protocol handler exceptions ([9e1432c9](https://github.com/waku-org/nwaku/commit/9e1432c9))
- **wakunode2:** made setup nat return errors ([1cfb251b](https://github.com/waku-org/nwaku/commit/1cfb251b))
- fixed multiple bare except warnings ([caf78249](https://github.com/waku-org/nwaku/commit/caf78249))
- bump libp2p with traffic metrics fix ([#1642](https://github.com/waku-org/nwaku/issues/1642)) ([0ef46673](https://github.com/waku-org/nwaku/commit/0ef46673))
- **rln-relay:** buildscript bad cp ([#1636](https://github.com/waku-org/nwaku/issues/1636)) ([bd9857c1](https://github.com/waku-org/nwaku/commit/bd9857c1))
- **wakunode2:** fix main warnings and drop swap support ([f95147f5](https://github.com/waku-org/nwaku/commit/f95147f5))
- **rln-relay:** on chain registration ([#1627](https://github.com/waku-org/nwaku/issues/1627)) ([b1bafda2](https://github.com/waku-org/nwaku/commit/b1bafda2))
- connect instead of dialing relay peers ([#1622](https://github.com/waku-org/nwaku/issues/1622)) ([85f33a8e](https://github.com/waku-org/nwaku/commit/85f33a8e))
- fix hash size greater than 32 ([#1621](https://github.com/waku-org/nwaku/issues/1621)) ([c42ac16f](https://github.com/waku-org/nwaku/commit/c42ac16f))

### Changes

- **ci:** cache all of submodules/deps to speed up build time ([#1731](https://github.com/waku-org/nwaku/issues/1731)) ([4394c69d](https://github.com/waku-org/nwaku/commit/4394c69d))
- **rln-relay:** update args to contract ([#1724](https://github.com/waku-org/nwaku/issues/1724)) ([b277ce10](https://github.com/waku-org/nwaku/commit/b277ce10))
- **rln-relay:** use new config for ffi ([#1718](https://github.com/waku-org/nwaku/issues/1718)) ([44c54312](https://github.com/waku-org/nwaku/commit/44c54312))
- adding new tutorial on how to handle heaptrack with nim waku ([#1719](https://github.com/waku-org/nwaku/issues/1719)) ([4b59e472](https://github.com/waku-org/nwaku/commit/4b59e472))
- add timestamp and ephemeral for opt-in dos validator ([#1713](https://github.com/waku-org/nwaku/issues/1713)) ([3e0a693d](https://github.com/waku-org/nwaku/commit/3e0a693d))
- add test vectors dos protection validator ([#1711](https://github.com/waku-org/nwaku/issues/1711)) ([eaa162ee](https://github.com/waku-org/nwaku/commit/eaa162ee))
- add validator for dos protec metrics and move to app ([#1704](https://github.com/waku-org/nwaku/issues/1704)) ([3e146869](https://github.com/waku-org/nwaku/commit/3e146869))
- use QUICK_AND_DIRTY_COMPILER flag for CI ([#1708](https://github.com/waku-org/nwaku/issues/1708)) ([21510425](https://github.com/waku-org/nwaku/commit/21510425))
- move networkmonitor and wakucanary to apps directory ([209579b0](https://github.com/waku-org/nwaku/commit/209579b0))
- **wakunode2:** flatten and simplify app setup ([#1705](https://github.com/waku-org/nwaku/issues/1705)) ([ce92fc1a](https://github.com/waku-org/nwaku/commit/ce92fc1a))
- **wakunode2:** split setup logic into app module ([c8081c88](https://github.com/waku-org/nwaku/commit/c8081c88))
- add payload bytes to trace log ([#1703](https://github.com/waku-org/nwaku/issues/1703)) ([c6d291d3](https://github.com/waku-org/nwaku/commit/c6d291d3))
- refactor flaky test with while ([#1698](https://github.com/waku-org/nwaku/issues/1698)) ([dca0e9b2](https://github.com/waku-org/nwaku/commit/dca0e9b2))
- **core:** move peers utils module to waku_core ([e041e043](https://github.com/waku-org/nwaku/commit/e041e043))
- decouple test2 target from testcommon ([91baa232](https://github.com/waku-org/nwaku/commit/91baa232))
- **core:** move utils time module to waku_core ([93b0c071](https://github.com/waku-org/nwaku/commit/93b0c071))
- add deprecation notice to utils module. move heartbeat to common ([e8dceb2a](https://github.com/waku-org/nwaku/commit/e8dceb2a))
- **core:** rename waku_message module to waku_core ([c9b6b230](https://github.com/waku-org/nwaku/commit/c9b6b230))
- flatten waku v2 protocols folder ([d7b72ac7](https://github.com/waku-org/nwaku/commit/d7b72ac7))
- fix test failing intermittently ([#1679](https://github.com/waku-org/nwaku/issues/1679)) ([8d213e85](https://github.com/waku-org/nwaku/commit/8d213e85))
- **networking:** get relay number of connections from protocol conns/streams ([#1609](https://github.com/waku-org/nwaku/issues/1609)) ([73cbafa6](https://github.com/waku-org/nwaku/commit/73cbafa6))
- allow to call store api endpoints without a storenode ([#1575](https://github.com/waku-org/nwaku/issues/1575)) ([#1647](https://github.com/waku-org/nwaku/issues/1647)) ([0b4a2e68](https://github.com/waku-org/nwaku/commit/0b4a2e68))
- bump container image versions to v0.16.0 in quickstart ([#1640](https://github.com/waku-org/nwaku/issues/1640)) ([5c33d9d1](https://github.com/waku-org/nwaku/commit/5c33d9d1))
- **node:** remove deprecated constructor and extend testlib with builder ([9dadc1f5](https://github.com/waku-org/nwaku/commit/9dadc1f5))
- do not mount relay more than once ([#1650](https://github.com/waku-org/nwaku/issues/1650)) ([5d853b86](https://github.com/waku-org/nwaku/commit/5d853b86))
- pointed all waku node imports to the barrel import ([e8448dfd](https://github.com/waku-org/nwaku/commit/e8448dfd))
- **node:** added waku_node barrel import and split config module ([13942888](https://github.com/waku-org/nwaku/commit/13942888))
- remove deprecated enr record init method ([0627b4f8](https://github.com/waku-org/nwaku/commit/0627b4f8))
- **deps:** upgrade nim-chronos and nim-presto to latest version ([7c229ece](https://github.com/waku-org/nwaku/commit/7c229ece))
- remove waku swap protocol ([2b5fd2a2](https://github.com/waku-org/nwaku/commit/2b5fd2a2))
- **deps:** upgrade nim-confutils to latest version ([67fa736d](https://github.com/waku-org/nwaku/commit/67fa736d))
- **rln-relay:** gracefully handle chain forks ([#1623](https://github.com/waku-org/nwaku/issues/1623)) ([00a3812b](https://github.com/waku-org/nwaku/commit/00a3812b))
- bump nim-libp2p 53b060f ([#1633](https://github.com/waku-org/nwaku/issues/1633)) ([11ff93c2](https://github.com/waku-org/nwaku/commit/11ff93c2))
- added testcommon target to makefile ([048ca45d](https://github.com/waku-org/nwaku/commit/048ca45d))
- increase meta size to 64 bytes + tests ([#1629](https://github.com/waku-org/nwaku/issues/1629)) ([1f793756](https://github.com/waku-org/nwaku/commit/1f793756))
- **enr:** move waku enr multiaddr to typedrecord and builder extensions ([2ffd2f80](https://github.com/waku-org/nwaku/commit/2ffd2f80))
- **enr:** added waku2 capabilities accessor ([157724d9](https://github.com/waku-org/nwaku/commit/157724d9))
- **rln-relay:** reduce exports ([#1615](https://github.com/waku-org/nwaku/issues/1615)) ([2f3ba3d6](https://github.com/waku-org/nwaku/commit/2f3ba3d6))
- add dash between target and version ([#1613](https://github.com/waku-org/nwaku/issues/1613)) ([24d62791](https://github.com/waku-org/nwaku/commit/24d62791))
- **release:** added regression checking and clarifications ([#1610](https://github.com/waku-org/nwaku/issues/1610)) ([b495dd7b](https://github.com/waku-org/nwaku/commit/b495dd7b))


This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` <br />`/vac/waku/filter-subscribe/2.0.0-beta1` <br />`/vac/waku/filter-push/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |

The Waku v1 implementation is stable but not under active development.

## Upgrade instructions

* The `--topics` config option has been deprecated to unify the configuration style. It still available in this and will be in next release, but will be removed after that. The new option `--topic` is introduced, which can be use repeatedly to achieve the same behaviour.

## 2023-03-15 v0.16.0

## What's Changed

Release highlights:
- a fix for an issue that prevented the node from generating high-resolution (up to nanosecond) timestamps
- introduction of an application-defined `meta` attribute to the Waku Message. This can be quite valuable for network-wide deduplication, deterministic hashing, validity checking and other planned improvements to the protocol
- many optimizations in RLN implementation and its underlying dependencies

### Features

- Integrated a new group manager for RLN-protected relay [1496](https://github.com/waku-org/nwaku/pull/1496)
- Added application-defined meta attribute to Waku Message according to RFC [14/WAKU2-MESSAGE](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/14/message.md#message-attributes) [1581](https://github.com/waku-org/nwaku/pull/1581)
- Implemented deterministic hashing scheme for Waku Messages according to RFC [14/WAKU2-MESSAGE](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/14/message.md#deterministic-message-hashing) [1586](https://github.com/waku-org/nwaku/pull/1586)

### Changes

- Upgraded nim-sqlite3-abi to the latest version [1565](https://github.com/waku-org/nwaku/pull/1565)
- Better validation of protocol buffers [1563](https://github.com/waku-org/nwaku/pull/1563)
- Improved underlying Zerokit performance and FFI [1571](https://github.com/waku-org/nwaku/pull/1571)
- Node peer ID now logged with relay trace logging [1574](https://github.com/waku-org/nwaku/pull/1574)
- Continued refactoring of several protocol implementations to improve maintainability and readability
- Refactored and cleaned up peer manager [1539](https://github.com/waku-org/nwaku/pull/1539)
- Removed unused and legacy websocket submodule [1580](https://github.com/waku-org/nwaku/pull/1580) [1582](https://github.com/waku-org/nwaku/pull/1582)
- Use base64 URL-safe encoding for noise [1569](https://github.com/waku-org/nwaku/pull/1569)
- Various general improvements to RLN implementation [1585](https://github.com/waku-org/nwaku/pull/1585) [1587](https://github.com/waku-org/nwaku/pull/1587)
- Started on implementation for new and improved filter protocol [1584](https://github.com/waku-org/nwaku/pull/1584)
- Updated pubsub and content topic namespacing to reflect latest changes in RFC [23/WAKU2-TOPICS](https://github.com/vacp2p/rfc-index/blob/main/waku/informational/23/topics.md) [1589](https://github.com/waku-org/nwaku/pull/1589)
- Unified internal peer data models [1597](https://github.com/waku-org/nwaku/pull/1597)
- Improved internal implementation of Waku ENR encoding and decoding [1598](https://github.com/waku-org/nwaku/pull/1598) [1599](https://github.com/waku-org/nwaku/pull/1599)
- Underlying dependency for RLN implementation now loaded as a static library [1578](https://github.com/waku-org/nwaku/pull/1578)

### Fixes

- Fixed internally generated timestamps to allow higher resolution than seconds [1570](https://github.com/waku-org/nwaku/pull/1570)
- Fixed padded base64 usage for encoding and decoding payloads on the JSON RPC API [1572](https://github.com/waku-org/nwaku/pull/1572)
- Fixed incorrect relative module imports [1591](https://github.com/waku-org/nwaku/pull/1591)
- Fixed RLN relay erroneously storing messages from multiple apps [1594](https://github.com/waku-org/nwaku/pull/1594)

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`18/WAKU2-SWAP`](https://github.com/vacp2p/rfc-index/blob/main/waku/deprecated/18/swap.md) | `draft` | `/vac/waku/swap/2.0.0-beta1` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |

The Waku v1 implementation is stable but not under active development.

## 2023-02-15 v0.15.0

Release highlights:
- Relay connectivity is now maintained by a management loop that selects from the peerstore
- Ability to manually specify `multiaddrs` for the nwaku node to advertise
- Two important fixes related to historical message queries:
  - fixed archive bug that resulted in duplicate messages in store query response
  - fixed query page size limit not being respected

### Features

- New connectivity loop to maintain relay connectivity from peerstore [1482](https://github.com/waku-org/nwaku/pull/1482) [1462](https://github.com/waku-org/nwaku/pull/1462)
- Support for manually specifying `multiaddrs` to advertise [1509](https://github.com/waku-org/nwaku/pull/1509) [1512](https://github.com/waku-org/nwaku/pull/1512)
- Added dynamic keystore for membership credential storage and management [1466](https://github.com/waku-org/nwaku/pull/1466)

### Changes

- Abstracted RLN relay group management into its own API [1465](https://github.com/waku-org/nwaku/pull/1465)
- Prune peers from peerstore when exceeding capacity [1513](https://github.com/waku-org/nwaku/pull/1513)
- Removed Kilic submodule [1517](https://github.com/waku-org/nwaku/pull/1517)
- Continued refactoring of several protocol implementations to improve maintainability and readability
- Refactored and improved JSON RPC API
- Added safe default values for peer-store-capacity [1525](https://github.com/waku-org/nwaku/pull/1525)
- Improvements in regular CI test reliability and repeatability
- Improved archive query performance [1510](https://github.com/waku-org/nwaku/pull/1510)
- Added better e2e trace logging for relay messages [1526](https://github.com/waku-org/nwaku/pull/1526)
- Relay RPC API now encodes message payloads in base64 [572](https://github.com/vacp2p/rfc/pull/572) [1555](https://github.com/waku-org/nwaku/pull/1555)

### Fixes

- Fixed Waku archive queries returning duplicate messages due to incorrect reordering [1511](https://github.com/waku-org/nwaku/pull/1511)
- Fixed Admin RPC API crashing on returning peer with no multiaddresses [1507](https://github.com/waku-org/nwaku/pull/1507)
- Fixed page size limit not being respected in store query responses [1520](https://github.com/waku-org/nwaku/pull/1520)
- Fixed nwaku subscribing to default pubsub topic even if not configured [1548](https://github.com/waku-org/nwaku/pull/1548)
- Fixed underlying issue causing node to incorrectly report it's unreachable [1518](https://github.com/waku-org/nwaku/pull/1518) [1546](https://github.com/waku-org/nwaku/pull/1546)
- Fixed Relay RPC API not adhering to RFC [1139](https://github.com/waku-org/nwaku/issues/1139)
- Fixed message IDs in nwaku diverging from those in go-waku [1556](https://github.com/waku-org/nwaku/pull/1556)

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`18/WAKU2-SWAP`](https://github.com/vacp2p/rfc-index/blob/main/waku/deprecated/18/swap.md) | `draft` | `/vac/waku/swap/2.0.0-beta1` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |

The Waku v1 implementation is stable but not under active development.

## 2023-01-16 v0.14.0

Release highlights:
- An important fix for the Waku message archive returning inconsistent responses to history queries.
- Support for [AutoNAT](https://docs.libp2p.io/concepts/nat/autonat/) and [libp2p Circuit Relay](https://docs.libp2p.io/concepts/nat/circuit-relay/) that allows, among other things, for [NAT hole punching](https://docs.libp2p.io/concepts/nat/hole-punching/).
- Support for structured logging in JSON format.
- A fix for an underlying file descriptor leak that affected websocket connections.

### Features

- Support for [AutoNAT](https://docs.libp2p.io/concepts/nat/autonat/)
- Support for [libp2p Circuit Relay](https://docs.libp2p.io/concepts/nat/circuit-relay/) (server only)
- New Waku Archive implementation. This allows easy addition of drivers for different technologies to store historical messages.
- Support for structured logging and specifying log format.
- Node now keeps track of its external reachability.

### Changes

- Zerokit RLN library now statically linked.
- Use extended key generation in Zerokit API to comply with [32/RLN](https://github.com/vacp2p/rfc-index/blob/main/vac/32/rln-v1.md).
- Re-enable root validation in [`17/WAKU-RLN-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/17/rln-relay.md) implementation.
- [Network monitoring tool](https://github.com/status-im/nwaku/tree/2336522d7f478337237a5a4ec8c5702fb4babc7d/tools#networkmonitor) now supports DNS discovery.
- Added [dashboard](https://github.com/waku-org/nwaku/blob/3e0e1cb2398297fca761aa74f52d32fa837d556c/metrics/waku-network-monitor-dashboard.json) for network monitoring.
- Continued refactoring of several protocol implementations to improve maintainability and readability.
- Removed swap integration from store protocol.
- Peerstore now consolidated with libp2p peerstore.
- Peerstore now also tracks peer direction.
- SIGSEGV signals are now handled and logged properly.
- Waku v2 no longer imports libraries from Waku v1.
- Improved build and CI processes:
  - Added support for an `EXPERIMENTAL` compiler flag.
  - Simplified project Makefile.
  - Split Dockerfile into production and experimental stages.
  - Removed obsolete simulation libraries from build.
- Improved parallellisation (and therefore processing time) when dialing several peers simultaneously.
- Waku Archive now responds with error to historical queries containing more than 10 content topics.

### Fixes

- Fixed support for optional fields in several protocol rpc codecs. [#1393](https://github.com/waku-org/nwaku/pull/1393) [#1395](https://github.com/waku-org/nwaku/pull/1395) [#1396](https://github.com/waku-org/nwaku/pull/1396)
- Fixed clients with `--store=false` not installing Store Client JSON-RPC API handlers. [#1382](https://github.com/waku-org/nwaku/pull/1382)
- Fixed SQLite driver returning inconsistent responses to store queries. [#1415](https://github.com/waku-org/nwaku/pull/1415)
- Fixed peer exchange discv5 loop starting before discv5 has started. [#1407](https://github.com/waku-org/nwaku/pull/1407)
- Fixed wakubridge test timing. [#1429](https://github.com/waku-org/nwaku/pull/1429)
- Fixed bug in Noise module types equating `T_ss` incorrectly to `"se"` and not `"ss"`. [#1432](https://github.com/waku-org/nwaku/pull/1432)
- Fixed Ctrl-C quitting resulting in unreleased resources and exit failures. [#1416](https://github.com/waku-org/nwaku/pull/1416)
- Fixed CI workflows not cloning repo on startup. [#1454](https://github.com/waku-org/nwaku/pull/1454) [#1455](https://github.com/waku-org/nwaku/pull/1455)
- Fixed Admin API peer connection not returning error response if peer can't be connected. [#1476](https://github.com/waku-org/nwaku/pull/1476)
- Fixed underlying file descriptor leak. [#1483](https://github.com/waku-org/nwaku/pull/1483)

### Docs

- Added [instructions](https://github.com/waku-org/nwaku/blob/3e0e1cb2398297fca761aa74f52d32fa837d556c/docs/operators/quickstart.md) for running nwaku with docker compose.

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`18/WAKU2-SWAP`](https://github.com/vacp2p/rfc-index/blob/main/waku/deprecated/18/swap.md) | `draft` | `/vac/waku/swap/2.0.0-beta1` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |

The Waku v1 implementation is stable but not under active development.

## 2022-11-15 v0.13.0

Release highlights:
- A [Waku canary tool](https://github.com/status-im/nwaku/tree/2336522d7f478337237a5a4ec8c5702fb4babc7d/tools#waku-canary-tool) to check if nodes are reachable and what protocols they support.
- Simplified configuration for store protocol. This [new guide](https://github.com/status-im/nwaku/blob/4e5318bfbb204bd1239c95472d7b84b6a326dd9d/docs/operators/how-to/configure-store.md) explains how to configure store from this release forward.
- Support for environment variables to configure a nwaku node. See our [configuration guide](https://github.com/status-im/nwaku/blob/384abed614050bf3aa90c901d7f5e8bc383e8b22/docs/operators/how-to/configure.md) for more.
- A Waku [network monitoring tool](https://github.com/status-im/nwaku/tree/2336522d7f478337237a5a4ec8c5702fb4babc7d/tools#networkmonitor) to report network metrics, including network size, discoverable peer capabilities and more.

### Features

- Added Waku canary tool to check if i) a given node is reachable and ii) it supports a set of protocols.
- Simplified [Waku store configuration](https://github.com/status-im/nwaku/blob/4e5318bfbb204bd1239c95472d7b84b6a326dd9d/docs/operators/how-to/configure-store.md).
- Decoupled Waku peer persistence configuration from message store configuration.
- Added keyfile support for secure storage of RLN credentials.
- Added configurable libp2p agent string to nwaku switch.
- Support for [configuration with environment variables](https://github.com/status-im/nwaku/blob/384abed614050bf3aa90c901d7f5e8bc383e8b22/docs/operators/how-to/configure.md).
- Added [example module](https://github.com/status-im/nwaku/tree/2336522d7f478337237a5a4ec8c5702fb4babc7d/examples/v2) to showcase basic nwaku relay usage.
- Added a nwaku [network monitoring tool](https://github.com/status-im/nwaku/tree/2336522d7f478337237a5a4ec8c5702fb4babc7d/tools#networkmonitor) to provide metrics on peers, network size and more.

### Changes

- Removed support for Kilic's RLN library (obsolete).
- Improved logging for [`17/WAKU-RLN-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/17/rln-relay.md) implementation.
- Connection to eth node for RLN now more stable, maintains state and logs failures.
- Waku apps and tools now moved to their own subdirectory.
- Continued refactoring of several protocol implementations to improve maintainability and readability.
- Periodically log metrics when running RLN spam protection.
- Added metrics dashboard for RLN spam protection.
- Github CI test workflows are now run selectively, based on the content of a PR.
- Improved reliability of CI runs and added email notifications.
- Discv5 discovery loop now triggered to fill a [34/WAKU2-PEER-EXCHANGE](https://github.com/waku-org/specs/blob/master/standards/core/peer-exchange.md) peer list cache asynchronously.
- Upgraded to Nim v1.6.6.
- Cleaned up compiler warnings on unused imports.
- Improved exception handling and annotation.
- [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) no longer enabled by default on nwaku nodes.
- Merkle tree roots for RLN membership changes now on a per-block basis to allow poorly connected peers to operate within a window of acceptable roots.

### Fixes

- Fixed encoding of ID commitments for RLN from Big-Endian to Little-Endian. [#1256](https://github.com/status-im/nwaku/pull/1256)
- Fixed maxEpochGap to be the maximum allowed epoch gap (RLN). [#1257](https://github.com/status-im/nwaku/pull/1257)
- Fixed store cursors being retrieved incorrectly (truncated) from DB. [#1263](https://github.com/status-im/nwaku/pull/1263)
- Fixed message indexed by store cursor being excluded from history query results. [#1263](https://github.com/status-im/nwaku/pull/1263)
- Fixed log-level configuration being ignored by the nwaku node. [#1272](https://github.com/status-im/nwaku/pull/1272)
- Fixed incorrect error message when failing to set [34/WAKU2-PEER-EXCHANGE](https://github.com/waku-org/specs/blob/master/standards/core/peer-exchange.md) peer. [#1298](https://github.com/status-im/nwaku/pull/1298)
- Fixed and replaced deprecated `TaintedString` type. [#1326](https://github.com/status-im/nwaku/pull/1326)
- Fixed and replaced unreliable regex library and usage. [#1327](https://github.com/status-im/nwaku/pull/1327) [#1328](https://github.com/status-im/nwaku/pull/1328)
- Fixed and replaced deprecated `ganache-cli` node package with `ganache` for RLN onchain tests. Added graceful daemon termination. [#1347](https://github.com/status-im/nwaku/pull/1347)

### Docs

- Added cross client RLN testnet [tutorial](https://github.com/status-im/nwaku/blob/44d8a2026dc31a37e181043ceb67e2822376dc03/docs/tutorial/rln-chat-cross-client.md).
- Fixed broken link to Kibana in [cluster documentation](https://github.com/status-im/nwaku/blob/5e90085242e9e4d6f3cf307e189efbf7e59da9f9/docs/contributors/cluster-logs.md).
- Added an improved [quickstart guide](https://github.com/status-im/nwaku/blob/8f5363ea8f5e95fc1104307aa0d2fc59fda13698/docs/operators/quickstart.md) for operators.
- Added a [Docker usage guide](https://github.com/status-im/nwaku/blob/8f5363ea8f5e95fc1104307aa0d2fc59fda13698/docs/operators/docker-quickstart.md#prerequisites) for operators.
- Added operator [guide on running RLN spam prevention](https://github.com/status-im/nwaku/blob/bd516788cb39132ccbf0a4dcf0880e9694beb233/docs/operators/how-to/run-with-rln.md) on nwaku nodes.
- Extended guidelines on nwaku [configuration methods](https://github.com/status-im/nwaku/blob/384abed614050bf3aa90c901d7f5e8bc383e8b22/docs/operators/how-to/configure.md) for operators.
- Added new [store configuration guide](https://github.com/status-im/nwaku/blob/4e5318bfbb204bd1239c95472d7b84b6a326dd9d/docs/operators/how-to/configure-store.md) to reflect simplified options.

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`18/WAKU2-SWAP`](https://github.com/vacp2p/rfc-index/blob/main/waku/deprecated/18/swap.md) | `draft` | `/vac/waku/swap/2.0.0-beta1` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |

The Waku v1 implementation is stable but not under active development.

## 2022-10-06 v0.12.0

Release highlights:
- The performance and stability of the message `store` has improved dramatically. Query durations, even for long-term stores, have improved by more than a factor of 10.
- Support for Waku Peer Exchange - a discovery method for resource-restricted nodes.
- Messages can now be marked as "ephemeral" to prevent them from being stored.
- [Zerokit](https://github.com/vacp2p/zerokit) is now the default implementation for spam-protected `relay` with RLN.

The full list of changes is below.

### Features

- Default support for [Zerokit](https://github.com/vacp2p/zerokit) version of [`17/WAKU-RLN-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/17/rln-relay.md) implementation.
- Added Filter REST API OpenAPI specification.
- Added POC implementation for [43/WAKU2-DEVICE-PAIRING](https://github.com/waku-org/specs/blob/master/standards/application/device-pairing.md).
- [14/WAKU2-MESSAGE](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/14/message.md) can now be marked as `ephemeral` to prevent them from being stored.
- Support for [34/WAKU2-PEER-EXCHANGE](https://github.com/waku-org/specs/blob/master/standards/core/peer-exchange.md).

### Changes

- [`17/WAKU-RLN-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/17/rln-relay.md) implementation now handles on-chain transaction errors.
- [`17/WAKU-RLN-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/17/rln-relay.md) implementation now validates the Merkle tree root against a window of acceptable roots.
- Added metrics for [`17/WAKU-RLN-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/17/rln-relay.md) implementation.
- Continued refactoring of several protocol implementations to improve maintainability and readability.
- Cleaned up nwaku imports and dependencies.
- Refactored and organised nwaku unit tests.
- Nwaku now periodically logs node metrics by default.
- Further improvements to the `store` implementation:
  - Better logging and query traceability.
  - More useful metrics to measure query and insertion time.
  - Reworked indexing for faster inserts and queries.
  - Reworked data model to use a simple, single timestamp for indexing, ordering and querying.
  - Improved retention policy management with periodic execution.
  - Run sqlite database vacuum at node start.
  - Improved logging when migrating the database to a newer version.
- `relay` no longer auto-mounted on all nwaku nodes.
- The most complete node ENR now included in response to API requests for node `info()`.
- Updated Grafana dashboards included with nwaku.
- Github CI test execution now skipped for doc-only changes.

### Fixes

- Fixed nwaku unnecessary sleep when no dynamic bootstrap nodes retrieved.
- Fixed [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) not working from browser-based clients due to nwaku peer manager failing to reuse existing connection.
- Waku Message payload now correctly encoded as base64 in the Relay REST API.
- Fixed handling of bindParam(uint32) in sqlite.
- `chat2` application now correctly selects a random store node on startup.
- Fixed macos builds failing due to an unsupported dependency.
- Fixed nwaku not reconnecting to previously discovered nodes after losing connection.
- Fixed nwaku failing to start switch transports with external IP configuration.
- Fixed SIGSEGV crash when attempting to start nwaku store without `db-path` configuration.

### Docs

- Improved [RLN testnet tutorial](https://github.com/status-im/nwaku/blob/14abdef79677ddc828ff396ece321e05cedfca17/docs/tutorial/onchain-rln-relay-chat2.md)
- Added [tutorial](https://github.com/status-im/nwaku/blob/14abdef79677ddc828ff396ece321e05cedfca17/docs/operators/droplet-quickstart.md) on running nwaku from a DigitalOcean droplet.
- Added [guide](https://github.com/status-im/nwaku/blob/14abdef79677ddc828ff396ece321e05cedfca17/docs/operators/how-to/monitor.md) on how to monitor nwaku using Prometheus and Grafana.

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`18/WAKU2-SWAP`](https://github.com/vacp2p/rfc-index/blob/main/waku/deprecated/18/swap.md) | `draft` | `/vac/waku/swap/2.0.0-beta1` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |

The Waku v1 implementation is stable but not under active development.

## 2022-08-15 v0.11

Release highlights:
- Major improvements in the performance of historical message queries to longer-term, sqlite-only message stores.
- Introduction of an HTTP REST API with basic functionality
- On-chain RLN group management. This was also integrated into an [example spam-protected chat application](https://github.com/status-im/nwaku/blob/4f93510fc9a938954dd85593f8dc4135a1c367de/docs/tutorial/onchain-rln-relay-chat2.md).

The full list of changes is below.

### Features

- Support for on-chain group membership management in the [`17/WAKU-RLN-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/17/rln-relay.md) implementation.
- Integrated HTTP REST API for external access to some `wakunode2` functionality:
  - Debug REST API exposes debug information about a `wakunode2`.
  - Relay REST API allows basic pub/sub functionality according to [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md).
- [`35/WAKU2-NOISE`](https://github.com/waku-org/specs/blob/master/standards/application/noise.md) implementation now adds padding to ChaChaPoly encryptions to increase security and reduce metadata leakage.

### Changes

- Significantly improved the SQLite-only historical message `store` query performance.
- Refactored several protocol implementations to improve maintainability and readability.
- Major code reorganization for the [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) implementation to improve maintainability. This will also make the `store` extensible to support multiple implementations.
- Disabled compiler log colors when running in a CI environment.
- Refactored [`35/WAKU2-NOISE`](https://github.com/waku-org/specs/blob/master/standards/application/noise.md) implementation into smaller submodules.
- [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) implementation can now optionally be compiled with [Zerokit RLN](https://github.com/vacp2p/zerokit/tree/64f508363946b15ac6c52f8b59d8a739a33313ec/rln). Previously only [Kilic's RLN](https://github.com/kilic/rln/tree/7ac74183f8b69b399e3bc96c1ae8ab61c026dc43) was supported.

### Fixes

- Fixed wire encoding of protocol buffers to use proto3.
- Fixed Waku v1 <> Waku v2 bridge losing connection to statically configured v1 nodes.
- Fixed underlying issue causing DNS discovery to fail for records containing multiple strings.

### Docs

- Updated [release process](https://github.com/status-im/nwaku/blob/4f93510fc9a938954dd85593f8dc4135a1c367de/docs/contributors/release-process.md) documentation.
- Added [tutorial](https://github.com/status-im/nwaku/blob/4f93510fc9a938954dd85593f8dc4135a1c367de/docs/tutorial/onchain-rln-relay-chat2.md) on how to run a spam-protected chat2 application with on-chain group management.


This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`18/WAKU2-SWAP`](https://github.com/vacp2p/rfc-index/blob/main/waku/deprecated/18/swap.md) | `draft` | `/vac/waku/swap/2.0.0-beta1` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |

The Waku v1 implementation is stable but not under active development.

## 2022-06-15 v0.10

Release highlights:
- Support for key exchange using Noise handshakes.
- Support for a SQLite-only historical message `store`. This allows for cheaper, longer-term historical message storage on disk rather than in memory.
- Several fixes for native WebSockets, including slow or hanging connections and connections dropping unexpectedly due to timeouts.
- A fix for a memory leak in nodes running a local SQLite database.

### Features

- Support for [`35/WAKU2-NOISE`](https://github.com/waku-org/specs/blob/master/standards/application/noise.md) handshakes as key exchange protocols.
- Support for TOML config files via `--config-file=<path/to/config.toml>`.
- Support for `--version` command. This prints the current tagged version (or compiled commit hash, if not on a version).
- Support for running `store` protocol from a `filter` client, storing only the filtered messages.
- Start of an HTTP REST API implementation.
- Support for a memory-efficient SQLite-only `store` configuration.

### Changes

- Added index on `receiverTimestamp` in the SQLite `store` to improve query performance.
- GossipSub [Peer Exchange](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md#prune-backoff-and-peer-exchange) is now disabled by default. This is a more secure option.
- Progress towards dynamic group management for the [`17/WAKU-RLN-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/17/rln-relay.md) implementation.
- Nodes with `--keep-alive` enabled now sends more regular pings to keep connections more reliably alive.
- Disabled `swap` protocol by default.
- Reduced unnecessary and confusing logging, especially during startup.
- Added discv5 UDP port to the node's main discoverable ENR.

### Fixes

- The in-memory `store` now checks the validity of message timestamps before storing.
- Fixed underlying bug that caused connection leaks in the HTTP client.
- Fixed Docker image compilation to use the correct external variable for compile-time flags (`NIMFLAGS` instead of `NIM_PARAMS`).
- Fixed issue where `--dns4-domain-name` caused an unhandled exception if no external port was available.
- Avoids unnecessarily calling DB migration if a `--db-path` is set but nothing is persisted in the DB. This led to a misleading warning log.
- Fixed underlying issues that caused WebSocket connections to hang.
- Fixed underlying issue that caused WebSocket connections to time out after 10 mins.
- Fixed memory leak in nodes that implements a SQLite database.

### Docs

- Added [tutorial](https://github.com/status-im/nwaku/blob/16dd267bd9d25ff24c64fc5c92a20eb0d322217c/docs/operators/how-to/configure-key.md) on how to generate and configure a node key.
- Added first [guide](https://github.com/status-im/nwaku/tree/16dd267bd9d25ff24c64fc5c92a20eb0d322217c/docs/operators) for nwaku operators.

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`18/WAKU2-SWAP`](https://github.com/vacp2p/rfc-index/blob/main/waku/deprecated/18/swap.md) | `draft` | `/vac/waku/swap/2.0.0-beta1` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |

The Waku v1 implementation is stable but not under active development.

## 2022-03-31 v0.9

Release highlights:

- Support for Peer Exchange (PX) when a peer prunes a [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) mesh due to oversubscription. This can significantly increase mesh stability.
- Improved start-up times through managing the size of the underlying persistent message storage.
- New websocket connections are no longer blocked due to parsing failures in other connections.

The full list of changes is below.

### Features

- Support for bootstrapping [`33/WAKU-DISCV5`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/33/discv5.md) via [DNS discovery](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/10/waku2.md#discovery-methods)
- Support for GossipSub [Peer Exchange](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md#prune-backoff-and-peer-exchange)

### Changes

- Waku v1 <> v2 bridge now supports DNS `multiaddrs`
- Waku v1 <> v2 bridge now validates content topics before attempting to bridge a message from Waku v2 to Waku v1
- Persistent message storage now auto deletes messages once over specified `--store-capacity`. This can significantly improve node start-up times.
- Renamed Waku v1 <> v2 bridge `make` target and binary to `wakubridge`
- Increased `store` logging to assist with debugging
- Increased `rln-relay` logging to assist with debugging
- Message metrics no longer include the content topic as a dimension to keep Prometheus metric cardinality under control
- Waku v2 `toy-chat` application now sets the sender timestamp when creating messages
- The type of the `proof` field of the `WakuMessage` is changed to `RateLimitProof`
- Added method to the JSON-RPC API that returns the git tag and commit hash of the binary
- The node's ENR is now included in the JSON-RPC API response when requesting node info

### Fixes

- Fixed incorrect conversion of seconds to nanosecond timestamps
- Fixed store queries blocking due to failure in resource clean up
- Fixed underlying issue where new websocket connections are blocked due to parsing failures in other connections
- Fixed failure to log the ENR necessary for a discv5 connection to the node

### Docs

- Added [RAM requirements](https://github.com/status-im/nim-waku/tree/ee96705c7fbe4063b780ac43b7edee2f6c4e351b/waku/v2#wakunode) to `wakunode2` build instructions
- Added [tutorial](https://github.com/status-im/nim-waku/blob/ee96705c7fbe4063b780ac43b7edee2f6c4e351b/docs/tutorial/rln-chat2-live-testnet.md) on communicating with waku2 test fleets via the chat2 `toy-chat` application in spam-protected mode using [`17/WAKU-RLN-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/17/rln-relay.md).
- Added a [section on bug reporting](https://github.com/status-im/nim-waku/blob/ee96705c7fbe4063b780ac43b7edee2f6c4e351b/README.md#bugs-questions--features) to `wakunode2` README
- Fixed broken links in the [JSON-RPC API Tutorial](https://github.com/status-im/nim-waku/blob/5ceef37e15a15c52cbc589f0b366018e81a958ef/docs/tutorial/jsonrpc-api.md)

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`18/WAKU2-SWAP`](https://github.com/vacp2p/rfc-index/blob/main/waku/deprecated/18/swap.md) | `draft` | `/vac/waku/swap/2.0.0-beta1` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |

The Waku v1 implementation is stable but not under active development.

##  2022-03-03 v0.8

Release highlights:

- Working demonstration and integration of [`17/WAKU-RLN-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/17/rln-relay.md) in the Waku v2 `toy-chat` application
- Beta support for ambient peer discovery using [a version of Discovery v5](https://github.com/vacp2p/rfc/pull/487)
- A fix for the issue that caused a `store` node to run out of memory after serving a number of historical queries
- Ability to configure a `dns4` domain name for a node and resolve other dns-based `multiaddrs`

The full list of changes is below.

### Features

- [`17/WAKU-RLN-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/17/rln-relay.md) implementation now supports spam-protection for a specific combination of `pubsubTopic` and `contentTopic` (available under the `rln` compiler flag).
- [`17/WAKU-RLN-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/17/rln-relay.md) integrated into chat2 `toy-chat` (available under the `rln` compiler flag)
- Added support for resolving dns-based `multiaddrs`
- A Waku v2 node can now be configured with a domain name and `dns4` `multiaddr`
- Support for ambient peer discovery using [`33/WAKU-DISCV5`](https://github.com/vacp2p/rfc/pull/487)

### Changes

- Metrics: now monitoring content topics and the sources of new connections
- Metrics: improved default fleet monitoring dashboard
- Introduced a `Timestamp` type (currently an alias for int64).
- All timestamps changed to nanosecond resolution.
- `timestamp` field number in WakuMessage object changed from `4` to `10`
- [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) identifier updated to `/vac/waku/store/2.0.0-beta4`
- `toy-chat` application now uses DNS discovery to connect to existing fleets

### Fixes

- Fixed underlying bug that caused occasional failures when reading the certificate for secure websockets
- Fixed `store` memory usage issues when responding to history queries

### Docs

- Documented [use of domain certificates](https://github.com/status-im/nim-waku/tree/2972a5003568848164033da3fe0d7f52a3d54824/waku/v2#enabling-websocket) for secure websockets
- Documented [how to configure a `dns4` domain name](https://github.com/status-im/nim-waku/tree/2972a5003568848164033da3fe0d7f52a3d54824/waku/v2#using-dns-discovery-to-connect-to-existing-nodes) for a node
- Clarified [use of DNS discovery](https://github.com/status-im/nim-waku/tree/2972a5003568848164033da3fe0d7f52a3d54824/waku/v2#using-dns-discovery-to-connect-to-existing-nodes) and provided current URLs for discoverable fleet nodes
- Added [tutorial](https://github.com/status-im/nim-waku/blob/2972a5003568848164033da3fe0d7f52a3d54824/docs/tutorial/rln-chat2-local-test.md) on using [`17/WAKU-RLN-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/17/rln-relay.md) with the chat2 `toy-chat` application
- Added [tutorial](https://github.com/status-im/nim-waku/blob/2972a5003568848164033da3fe0d7f52a3d54824/docs/tutorial/bridge.md) on how to configure and a use a [`15/WAKU-BRIDGE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/15/bridge.md)

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta4` |
| [`18/WAKU2-SWAP`](https://github.com/vacp2p/rfc-index/blob/main/waku/deprecated/18/swap.md) | `draft` | `/vac/waku/swap/2.0.0-beta1` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |

The Waku v1 implementation is stable but not under active development.

##  2022-01-19 v0.7

Release highlights:

- Support for secure websockets.
- Ability to remove unreachable clients in a `filter` node.
- Several fixes to improve `store` performance and decrease query times. Query time for large stores decreased from longer than 8 min to under 100 ms.
- Fix for a long-standing bug that prevented proper database migration in some deployed Docker containers.

The full list of changes is below.

### Features

- Support for secure websocket transport

### Changes

- Filter nodes can now remove unreachable clients
- The WakuInfo `listenStr` is deprecated and replaced with a sequence of `listenAddresses` to accommodate multiple transports
- Removed cached `peerInfo` on local node. Rely on underlying libp2p switch instead
- Metrics: added counters for protocol messages
- Waku v2 node discovery now supports [`31/WAKU2-ENR`](https://github.com/waku-org/specs/blob/master/standards/core/enr.md)
- resuming the history via `resume` now takes the answers of all peers in `peerList` into consideration and consolidates them into one deduplicated list

### Fixes

- Fixed database migration failure in the Docker image
- All `HistoryResponse` messages are now auto-paginated to a maximum of 100 messages per response
- Increased maximum length for reading from a libp2p input stream to allow largest possible protocol messages, including `HistoryResponse` messages at max size
- Significantly improved `store` node query performance
- Implemented a GossipSub `MessageIdProvider` for `11/WAKU2-RELAY` messages instead of relying on the unstable default
- Receiver timestamps for message indexing in the `store` now have consistent millisecond resolution

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`17/WAKU-RLN-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/17/rln-relay.md) | `raw` | `/vac/waku/waku-rln-relay/2.0.0-alpha1` |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta3` |
| [`18/WAKU2-SWAP`](https://github.com/vacp2p/rfc-index/blob/main/waku/deprecated/18/swap.md) | `draft` | `/vac/waku/swap/2.0.0-beta1` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |

The Waku v1 implementation is stable but not under active development.

## 2021-11-05 v0.6

Some useful features and fixes in this release, include:
- two methods for Waku v2 node discovery
- support for unsecure websockets, which paves the way for native browser usage
- a fix for `nim-waku` store nodes running out of memory due to store size: the number of stored messages can now easily be configured
- a fix for densely connected nodes refusing new connections: the maximum number of allowed connections can now easily be configured
- support for larger message sizes (up from 64kb to 1Mb per message)

The full list of changes is below.

### Features

- Waku v2 node discovery via DNS following [EIP-1459](https://eips.ethereum.org/EIPS/eip-1459)
- Waku v2 node discovery via [Node Discovery v5](https://github.com/ethereum/devp2p/blob/master/discv5/discv5-theory.md)

### Changes

- Pagination of historical queries are now simplified
- GossipSub [prune backoff period](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md#prune-backoff-and-peer-exchange) is now the recommended 1 minute
- Bridge now uses content topic format according to [23/WAKU2-TOPICS](https://github.com/vacp2p/rfc-index/blob/main/waku/informational/23/topics.md)
- Better internal differentiation between local and remote peer info
- Maximum number of libp2p connections is now configurable
- `udp-port` CLI option has been removed for binaries where it's not used
- Waku v2 now supports unsecure WebSockets
- Waku v2 now supports larger message sizes of up to 1 Mb by default
- Further experimental development of [RLN for spam protection](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/17/rln-relay.md).
These changes are disabled by default under a compiler flag. Changes include:
  - Per-message rate limit proof defined
  - RLN proof generation and verification integrated into Waku v2
  - RLN tree depth changed from 32 to 20
  - Support added for static membership group formation

#### Docs

- Added [contributor guidelines](https://github.com/status-im/nim-waku/blob/master/docs/contributors/waku-fleets.md) on Waku v2 fleet monitoring and management
- Added [basic tutorial](https://github.com/status-im/nim-waku/blob/master/docs/tutorial/dns-disc.md) on using Waku v2 DNS-based discovery

### Fixes

- Bridge between `toy-chat` and matterbridge now shows correct announced addresses
- Bridge no longer re-encodes already encoded payloads when publishing to V1
- Bridge now populates WakuMessage timestamps when publishing to V2
- Store now has a configurable maximum number of stored messages
- Network simulations for Waku v1 and Waku v2 are runnable again

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`17/WAKU-RLN`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/17/rln-relay.md) | `raw` | `/vac/waku/waku-rln-relay/2.0.0-alpha1` |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta3` |
| [`18/WAKU2-SWAP`](https://github.com/vacp2p/rfc-index/blob/main/waku/deprecated/18/swap.md) | `draft` | `/vac/waku/swap/2.0.0-beta1` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |

The Waku v1 implementation is stable but not under active development.

## 2021-07-26 v0.5.1

This patch release contains the following fix:
- Support for multiple protocol IDs when reconnecting to previously connected peers:
A bug in `v0.5` caused clients using persistent peer storage to only support the mounted protocol ID.

This is a patch release that is fully backwards-compatible with release `v0.5`.
It supports the same [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`17/WAKU-RLN`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/17/rln-relay.md) | `raw` | `/vac/waku/waku-rln-relay/2.0.0-alpha1` |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta3` |
| [`18/WAKU2-SWAP`](https://github.com/vacp2p/rfc-index/blob/main/waku/deprecated/18/swap.md) | `draft` | `/vac/waku/swap/2.0.0-beta1` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |

The Waku v1 implementation is stable but not under active development.

## 2021-07-23 v0.5

This release contains the following:

### Features
- Support for keep-alives using [libp2p ping protocol](https://docs.libp2p.io/concepts/protocols/#ping).
- DB migration for the message and peer stores.
- Support for multiple protocol IDs. Mounted protocols now match versions of the same protocol that adds a postfix to the stable protocol ID.

### Changes
- Bridge topics are now configurable.
- The `resume` Nim API now eliminates duplicates messages before storing them.
- The `resume` Nim API now fetches historical messages in page sequence.
- Added support for stable version of `relay` protocol, with protocol ID `/vac/waku/relay/2.0.0`.
- Added optional `timestamp` to `WakuRelayMessage`.
- Removed `PCRE` as a prerequisite for building Waku v1 and Waku v2.
- Improved [`swap`](https://github.com/vacp2p/rfc-index/blob/main/waku/deprecated/18/swap.md) metrics.

#### General refactoring
- Refactored modules according to [Nim best practices](https://hackmd.io/1imOGULZRsed2HpgmzGleA).
- Simplified the [way protocols get notified](https://github.com/status-im/nim-waku/issues/574) of new messages.
- Refactored `wakunode2` setup into 6 distinct phases with improved logging and error handling.
- Moved `Whisper` types and protocol from the `nim-eth` module to `nim-waku`.

#### Docs
- Added [database migration tutorial](https://github.com/status-im/nim-waku/blob/master/docs/tutorial/db-migration.md).
- Added [tutorial to setup `websockify`](https://github.com/status-im/nim-waku/blob/master/docs/tutorial/websocket.md).

#### Schema
- Updated the `Message` table of the persistent message store:
  - Added `senderTimestamp` column.
  - Renamed the `timestamp` column to `receiverTimestamp` and changes its type to `REAL`.

#### API
- Added optional `timestamp` to [`WakuRelayMessage`](https://github.com/vacp2p/rfc-index/blob/main/waku/deprecated/16/rpc.md) on JSON-RPC API.

### Fixes
- Conversion between topics for the Waku v1 <-> v2 bridge now follows the [RFC recommendation](https://github.com/vacp2p/rfc-index/blob/main/waku/informational/23/topics.md).
- Fixed field order of `HistoryResponse` protobuf message: the field numbers of the `HistoryResponse` are shifted up by one to match up the [13/WAKU2-STORE](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) specs.

This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`17/WAKU-RLN`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/17/rln-relay.md) | `raw` | `/vac/waku/waku-rln-relay/2.0.0-alpha1` |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `stable` | `/vac/waku/relay/2.0.0` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta3` |
| [`18/WAKU2-SWAP`](https://github.com/vacp2p/rfc-index/blob/main/waku/deprecated/18/swap.md) | `draft` | `/vac/waku/swap/2.0.0-beta1` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |

The Waku v1 implementation is stable but not under active development.

## 2021-06-03 v0.4

This release contains the following:

### Features

- Initial [`toy-chat` implementation](https://github.com/vacp2p/rfc-index/blob/main/waku/informational/22/toy-chat.md)

### Changes

- The [toy-chat application](https://github.com/status-im/nim-waku/blob/master/docs/tutorial/chat2.md) can now perform `lightpush` and request content-filtered messages from remote peers.
- The [toy-chat application](https://github.com/status-im/nim-waku/blob/master/docs/tutorial/chat2.md) now uses default content topic `/toy-chat/2/huilong/proto`
- Improve `toy-chat` [briding to matterbridge]((https://github.com/status-im/nim-waku/blob/master/docs/tutorial/chat2.md#bridge-messages-between-chat2-and-matterbridge))
- Improve [`swap`](https://github.com/vacp2p/rfc-index/blob/main/waku/deprecated/18/swap.md) logging and enable soft mode by default
- Content topics are no longer in a redundant nested structure
- Improve error handling

#### API

- [JSON-RPC Store API](https://github.com/vacp2p/rfc-index/blob/main/waku/deprecated/16/rpc.md): Added an optional time-based query to filter historical messages.
- [Nim API](https://github.com/status-im/nim-waku/blob/master/docs/api/v2/node.md): Added `resume` method.

### Fixes

- Connections between nodes no longer become unstable due to keep-alive errors if mesh grows large
- Re-enable `lightpush` tests and fix Windows CI failure

The [Waku v2 suite of protocols](https://github.com/waku-org/specs) are still in a raw/draft state.
This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`17/WAKU-RLN`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/17/rln-relay.md) | `raw` | `/vac/waku/waku-rln-relay/2.0.0-alpha1` |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `draft` | `/vac/waku/relay/2.0.0-beta2` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta3` |
| [`18/WAKU2-SWAP`](https://github.com/vacp2p/rfc-index/blob/main/waku/deprecated/18/swap.md) | `draft` | `/vac/waku/swap/2.0.0-beta1` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `draft` | `/vac/waku/lightpush/2.0.0-beta1` |

The Waku v1 implementation is stable but not under active development.

## 2021-05-11 v0.3

This release contains the following:

### Features

- Start of [`RLN relay` implementation](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/17/rln-relay.md)
- Start of [`swap` implementation](https://github.com/vacp2p/rfc-index/blob/main/waku/deprecated/18/swap.md)
- Start of [fault-tolerant `store` implementation](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/application/21/fault-tolerant-store.md)
- Initial [`bridge` implementation](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/15/bridge.md) between Waku v1 and v2 protocols
- Initial [`lightpush` implementation](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md)
- A peer manager for `relay`, `filter`, `store` and `swap` peers
- Persistent storage for peers: A node with this feature enabled will now attempt to reconnect to `relay` peers after a restart. It will respect the gossipsub [PRUNE backoff](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md#prune-backoff-and-peer-exchange) period before attempting to do so.
- `--persist-peers` CLI option to persist peers in local storage
- `--persist-messages` CLI option to store historical messages locally
- `--keep-alive` CLI option to maintain a stable connection to `relay` peers on idle topics
- A CLI chat application ([`chat2`](https://github.com/status-im/nim-waku/blob/master/docs/tutorial/chat2.md)) over Waku v2 with [bridging to matterbridge](https://github.com/status-im/nim-waku/blob/master/docs/tutorial/chat2.md#bridge-messages-between-chat2-and-matterbridge)

### Changes
- Enable `swap` protocol by default and improve logging
#### General refactoring

- Split out `waku_types` types into the right place; create `utils` folder.
- Change type of `contentTopic` in [`ContentFilter`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md#protobuf) to `string`.
- Replace sequence of `contentTopics` in [`ContentFilter`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md#protobuf) with a single `contentTopic`.
- Add `timestamp` field to [`WakuMessage`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/14/message.md#payloads).
- Ensure CLI config parameters use a consistent naming scheme. Summary of changes [here](https://github.com/status-im/nim-waku/pull/543).

#### Docs

Several clarifications and additions aimed at contributors, including
  - information on [how to query Status test fleet](https://github.com/status-im/nim-waku/blob/master/docs/faq.md) for node addresses,
  - [how to view logs](https://github.com/status-im/nim-waku/blob/master/docs/contributors/cluster-logs.md), and
  - [how to update submodules](https://github.com/status-im/nim-waku/blob/master/docs/contributors/git-submodules.md).

#### Schema

- Add `Message` table to the persistent message store. This table replaces the old `messages` table. It has two additional columns, namely
  - `pubsubTopic`, and
  - `version`.
- Add `Peer` table for persistent peer storage.

#### API

- [JSON-RPC Admin API](https://github.com/vacp2p/rfc-index/blob/main/waku/deprecated/16/rpc.md): Added a [`post` method](https://github.com/vacp2p/rfc-index/blob/main/waku/deprecated/16/rpc.md#post_waku_v2_admin_v1_peers) to connect to peers on an ad-hoc basis.
- [Nim API](https://github.com/status-im/nim-waku/blob/master/docs/api/v2/node.md): PubSub topic `subscribe` and `unsubscribe` no longer returns a future (removed `async` designation).
- [`HistoryQuery`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md#historyquery): Added  `pubsubTopic` field. Message history can now be filtered and queried based on the `pubsubTopic`.
- [`HistoryQuery`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md#historyquery): Added support for querying a time window by specifying start and end times.

### Fixes

- Running nodes can now be shut down gracefully
- Content filtering now works on any PubSub topic and not just the `waku` default.
- Nodes can now mount protocols without supporting `relay` as a capability

The [Waku v2 suite of protocols](https://github.com/waku-org/specs) are still in a raw/draft state.
This release supports the following [libp2p protocols](https://docs.libp2p.io/concepts/protocols/):
| Protocol | Spec status | Protocol id |
| ---: | :---: | :--- |
| [`17/WAKU-RLN`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/17/rln-relay.md) | `raw` | `/vac/waku/waku-rln-relay/2.0.0-alpha1` |
| [`18/WAKU2-SWAP`](https://github.com/vacp2p/rfc-index/blob/main/waku/deprecated/18/swap.md) | `raw` | `/vac/waku/swap/2.0.0-alpha1` |
| [`19/WAKU2-LIGHTPUSH`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/19/lightpush.md) | `raw` | `/vac/waku/lightpush/2.0.0-alpha1` |
| [`11/WAKU2-RELAY`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/11/relay.md) | `draft` | `/vac/waku/relay/2.0.0-beta2` |
| [`12/WAKU2-FILTER`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/12/filter.md) | `draft` | `/vac/waku/filter/2.0.0-beta1` |
| [`13/WAKU2-STORE`](https://github.com/vacp2p/rfc-index/blob/main/waku/standards/core/13/store.md) | `draft` | `/vac/waku/store/2.0.0-beta3` |

The Waku v1 implementation is stable but not under active development.

## 2021-01-05 v0.2

This release contains the following changes:

- Calls to `publish` a message on `wakunode2` now `await` instead of `discard` dispatched [`WakuRelay`](https://github.com/vacp2p/specs/blob/master/specs/waku/v2/waku-relay.md) procedures.
- [`StrictNoSign`](https://github.com/libp2p/specs/tree/master/pubsub#message-signing) enabled.
- Add JSON-RPC API for external access to `wakunode2` functionality:
  - Admin API retrieves information about peers registered on the `wakunode2`.
  - Debug API exposes debug information about a `wakunode2`.
  - Filter API saves bandwidth by allowing light nodes to filter for specific content.
  - Private API enables symmetric or asymmetric cryptography to encrypt/decrypt message payloads.
  - Relay API allows basic pub/sub functionality.
  - Store API retrieves historical messages.
- Add tutorial on how to use JSON-RPC API.
- Refactor: Move `waku_filter` protocol into its own module.

The Waku v2 implementation, and [most protocols it consist of](https://specs.vac.dev/specs/waku/),
are still in a draft/beta state. The Waku v1 implementation is stable but not under active development.

## 2020-11-30 v0.1

Initial beta release.

This release contains:

- A Nim implementation of the [Waku v1 protocol](https://specs.vac.dev/waku/waku.html).
- A Nim implementation of the [Waku v2 protocol](https://specs.vac.dev/specs/waku/v2/waku-v2.html).
- CLI applications `wakunode` and `wakunode2` that allows you to run a Waku v1 or v2 node.
- Examples of Waku v1 and v2 usage.
- Various tests of above.

Currenty the Waku v2 implementation, and [most protocols it consist of](https://specs.vac.dev/specs/waku/),
are in a draft/beta state. The Waku v1 implementation is stable but not under active development.

Feedback welcome!
