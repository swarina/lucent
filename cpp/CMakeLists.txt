# C++ core. Subdirectories are wired in as their roadmap tasks land:
#   common/      M0-T3    index/   M0-T4/M1-T1    shard/   M0-T5
#   coordinator/ M0-T8    raft/    M5-T1          tests/   per task
#
# For now the scaffold builds a single smoke binary that proves the toolchain
# end to end (compiler, C++20, presets, vcpkg toolchain load).

add_executable(lucent-hello hello.cc)
target_link_libraries(lucent-hello PRIVATE lucent_warnings)
target_compile_definitions(lucent-hello PRIVATE LUCENT_VERSION="${PROJECT_VERSION}")

# Generated protobuf/gRPC stubs (M0-T2) + a compile/link smoke.
add_subdirectory(proto)
add_executable(lucent-proto-smoke proto/proto_smoke.cc)
# Generated code is not warning-clean under our strict flags; keep warnings off it.
target_link_libraries(lucent-proto-smoke PRIVATE lucent_proto)

add_subdirectory(common)
add_subdirectory(tests)
# add_subdirectory(index)        M0-T4
# add_subdirectory(shard)        M0-T5
# add_subdirectory(coordinator)  M0-T8
