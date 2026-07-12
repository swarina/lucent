find_package(nlohmann_json CONFIG REQUIRED)

# Service implementation as a library so tests can run it in-process.
add_library(lucent_shard_lib STATIC shard_server.cc)
target_include_directories(lucent_shard_lib PUBLIC ${CMAKE_SOURCE_DIR}/cpp)
target_link_libraries(lucent_shard_lib
  PUBLIC lucent_common lucent_index
  PRIVATE nlohmann_json::nlohmann_json lucent_warnings)

add_executable(lucent-shard main.cc)
target_link_libraries(lucent-shard PRIVATE lucent_shard_lib lucent_warnings)
