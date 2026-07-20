# Mini-Raft core (M5-T1): a pure, I/O-free state machine + (in tests) a seeded
# simulator with invariant checkers. No networking here — that's M5-T2.
find_package(xxHash CONFIG REQUIRED)

add_library(lucent_raft STATIC raft_node.cc raft_storage.cc)
target_include_directories(lucent_raft PUBLIC ${CMAKE_SOURCE_DIR}/cpp)
target_link_libraries(lucent_raft
  PUBLIC lucent_proto
  PRIVATE xxHash::xxhash lucent_warnings)
