# Generates C++ protobuf + gRPC stubs for all lucent protos into the build tree
# (not checked in). Other C++ targets link `lucent_proto`.

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

set(PROTO_ROOT ${CMAKE_SOURCE_DIR}/proto)
file(GLOB PROTO_FILES CONFIGURE_DEPENDS ${PROTO_ROOT}/lucent/v1/*.proto)

add_library(lucent_proto STATIC ${PROTO_FILES})
target_link_libraries(lucent_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)
target_include_directories(lucent_proto PUBLIC "${CMAKE_CURRENT_BINARY_DIR}")

# protobuf messages (.pb.h / .pb.cc)
protobuf_generate(
  TARGET lucent_proto
  IMPORT_DIRS ${PROTO_ROOT}
  PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR})

# gRPC services (.grpc.pb.h / .grpc.pb.cc)
protobuf_generate(
  TARGET lucent_proto
  LANGUAGE grpc
  GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
  PLUGIN "protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
  IMPORT_DIRS ${PROTO_ROOT}
  PROTOC_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR})
