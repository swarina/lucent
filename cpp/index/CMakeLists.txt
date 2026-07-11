find_package(Eigen3 CONFIG REQUIRED)
find_package(xxHash CONFIG REQUIRED)
find_package(zstd CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)

add_library(lucent_index STATIC
  brute_force.cc
  shard_storage.cc)

target_include_directories(lucent_index PUBLIC ${CMAKE_SOURCE_DIR}/cpp)
target_link_libraries(lucent_index
  PUBLIC lucent_common
  PRIVATE Eigen3::Eigen xxHash::xxhash zstd::libzstd nlohmann_json::nlohmann_json
          lucent_warnings)
