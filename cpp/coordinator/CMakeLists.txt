find_package(nlohmann_json CONFIG REQUIRED)

find_package(spdlog CONFIG REQUIRED)

add_library(lucent_coordinator_lib STATIC coordinator_server.cc raft_store.cc)
target_include_directories(lucent_coordinator_lib PUBLIC ${CMAKE_SOURCE_DIR}/cpp)
target_link_libraries(lucent_coordinator_lib
  PUBLIC lucent_common
  PRIVATE nlohmann_json::nlohmann_json spdlog::spdlog lucent_warnings)

add_executable(lucent-coord main.cc)
target_link_libraries(lucent-coord PRIVATE lucent_coordinator_lib lucent_warnings)
