find_package(GTest CONFIG REQUIRED)

find_package(nlohmann_json CONFIG REQUIRED)

add_executable(lucent_common_tests
  brute_force_test.cc
  config_test.cc
  coordinator_test.cc
  event_emitter_test.cc
  hnsw_io_test.cc
  hnsw_test.cc
  member_test.cc
  node_id_test.cc
  raft_test.cc
  raft_storage_test.cc
  replication_test.cc
  shard_server_test.cc
  shard_storage_test.cc
  spsc_ring_test.cc)

target_link_libraries(lucent_common_tests
  PRIVATE lucent_common lucent_index lucent_shard_lib lucent_coordinator_lib
          lucent_raft
          nlohmann_json::nlohmann_json
          lucent_warnings GTest::gtest GTest::gtest_main)
target_compile_definitions(lucent_common_tests
  PRIVATE LUCENT_REPO_ROOT="${CMAKE_SOURCE_DIR}")

include(GoogleTest)
# PRE_TEST discovery + generous timeout: sanitizer builds start slowly and the
# default 5s build-time discovery times out under ASan.
gtest_discover_tests(lucent_common_tests
  DISCOVERY_MODE PRE_TEST
  DISCOVERY_TIMEOUT 60)
