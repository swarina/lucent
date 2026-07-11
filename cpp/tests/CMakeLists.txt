find_package(GTest CONFIG REQUIRED)

add_executable(lucent_common_tests
  config_test.cc
  event_emitter_test.cc
  node_id_test.cc
  spsc_ring_test.cc)

target_link_libraries(lucent_common_tests
  PRIVATE lucent_common lucent_warnings GTest::gtest GTest::gtest_main)
target_compile_definitions(lucent_common_tests
  PRIVATE LUCENT_REPO_ROOT="${CMAKE_SOURCE_DIR}")

include(GoogleTest)
# PRE_TEST discovery + generous timeout: sanitizer builds start slowly and the
# default 5s build-time discovery times out under ASan.
gtest_discover_tests(lucent_common_tests
  DISCOVERY_MODE PRE_TEST
  DISCOVERY_TIMEOUT 60)
