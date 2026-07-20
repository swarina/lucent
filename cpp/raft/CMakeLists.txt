# Mini-Raft core (M5-T1): a pure, I/O-free state machine + (in tests) a seeded
# simulator with invariant checkers. No networking here — that's M5-T2.
add_library(lucent_raft STATIC raft_node.cc)
target_include_directories(lucent_raft PUBLIC ${CMAKE_SOURCE_DIR}/cpp)
target_link_libraries(lucent_raft PUBLIC lucent_proto PRIVATE lucent_warnings)
