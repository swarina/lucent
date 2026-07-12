find_package(yaml-cpp CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)

add_library(lucent_common STATIC
  collector_sink.cc
  config.cc
  event_emitter.cc
  node_id.cc
  proc_stats.cc)

target_include_directories(lucent_common PUBLIC ${CMAKE_SOURCE_DIR}/cpp)
target_link_libraries(lucent_common
  PUBLIC lucent_proto spdlog::spdlog
  PRIVATE yaml-cpp::yaml-cpp lucent_warnings)
