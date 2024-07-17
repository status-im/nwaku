## Waku v2

# Waku core test suite
import
  ./waku_core/test_namespaced_topics,
  ./waku_core/test_time,
  ./waku_core/test_message_digest,
  ./waku_core/test_peers,
  ./waku_core/test_published_address

# Waku archive test suite
import
  ./waku_archive/test_driver_queue_index,
  ./waku_archive/test_driver_queue_pagination,
  ./waku_archive/test_driver_queue_query,
  ./waku_archive/test_driver_queue,
  ./waku_archive/test_driver_sqlite_query,
  ./waku_archive/test_driver_sqlite,
  ./waku_archive/test_retention_policy,
  ./waku_archive/test_waku_archive,
  ./waku_archive/test_partition_manager,
  ./waku_archive_legacy/test_driver_queue_index,
  ./waku_archive_legacy/test_driver_queue_pagination,
  ./waku_archive_legacy/test_driver_queue_query,
  ./waku_archive_legacy/test_driver_queue,
  ./waku_archive_legacy/test_driver_sqlite_query,
  ./waku_archive_legacy/test_driver_sqlite,
  ./waku_archive_legacy/test_waku_archive

const os* {.strdefine.} = ""
when os == "Linux" and
    # GitHub only supports container actions on Linux
    # and we need to start a postgress database in a docker container
    defined(postgres):
  import
    ./waku_archive/test_driver_postgres_query,
    ./waku_archive/test_driver_postgres,
    #./waku_archive_legacy/test_driver_postgres_query,
    #./waku_archive_legacy/test_driver_postgres,
    ./factory/test_node_factory,
    ./wakunode_rest/test_rest_store

# Waku store test suite
import
  ./waku_store/test_client,
  ./waku_store/test_rpc_codec,
  ./waku_store/test_waku_store,
  ./waku_store/test_wakunode_store

# Waku legacy store test suite
import
  ./waku_store_legacy/test_client,
  ./waku_store_legacy/test_rpc_codec,
  ./waku_store_legacy/test_waku_store,
  ./waku_store_legacy/test_wakunode_store

# Waku store sync suite
import ./waku_store_sync/test_all

when defined(waku_exp_store_resume):
  # TODO: Review store resume test cases (#1282)
  import ./waku_store_legacy/test_resume

import
  ./node/test_all,
  ./waku_filter_v2/test_all,
  ./waku_peer_exchange/test_all,
  ./waku_lightpush_legacy/test_all,
  ./waku_relay/test_all,
  ./incentivization/test_all

import
  # Waku v2 tests
  ./test_wakunode,
  # ./test_wakunode_lightpush,
  ./test_wakunode_legacy_lightpush,
  ./test_peer_store_extended,
  ./test_message_cache,
  ./test_peer_manager,
  ./test_peer_storage,
  ./test_waku_keepalive,
  ./test_waku_enr,
  ./test_waku_dnsdisc,
  ./test_relay_peer_exchange,
  ./test_waku_noise,
  ./test_waku_noise_sessions,
  ./test_waku_netconfig,
  ./test_waku_switch,
  ./test_waku_rendezvous

# Waku Keystore test suite
import ./test_waku_keystore_keyfile, ./test_waku_keystore

## Wakunode Rest API test suite
import
  ./wakunode_rest/test_rest_debug,
  ./wakunode_rest/test_rest_debug_serdes,
  ./wakunode_rest/test_rest_relay,
  ./wakunode_rest/test_rest_relay_serdes,
  ./wakunode_rest/test_rest_serdes,
  ./wakunode_rest/test_rest_filter,
  ./wakunode_rest/test_rest_lightpush_legacy,
  ./wakunode_rest/test_rest_admin,
  ./wakunode_rest/test_rest_cors,
  ./wakunode_rest/test_rest_health

import ./waku_rln_relay/test_all

# Node Factory
