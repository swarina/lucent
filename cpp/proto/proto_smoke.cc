// Compile+link smoke for generated protobuf messages and gRPC service stubs.
// Proves the C++ codegen produced usable headers for both message and service
// types. Not a running server — instantiation only.

#include <iostream>

#include "lucent/v1/common.pb.h"
#include "lucent/v1/coordinator.grpc.pb.h"
#include "lucent/v1/events.pb.h"
#include "lucent/v1/shard.grpc.pb.h"

int main() {
  lucent::v1::SearchRequest req;
  req.set_k(10);
  req.set_ef_search(100);
  req.set_trace_level(lucent::v1::TRACE_LEVEL_FULL);

  lucent::v1::Hit hit;
  hit.set_doc_id(42);
  hit.set_score(0.87F);

  lucent::v1::Event ev;
  ev.set_node_id("shard-0a");
  ev.mutable_span()->set_kind(lucent::v1::SPAN_SHARD_SEARCH);

  // Reference the generated service stub types (no channel is dialed).
  using ShardStub = lucent::v1::ShardService::Stub;
  using CoordStub = lucent::v1::CoordinatorService::Stub;
  static_assert(sizeof(ShardStub*) > 0);
  static_assert(sizeof(CoordStub*) > 0);

  std::cout << "proto smoke OK: SearchRequest.k=" << req.k()
            << " hit.doc_id=" << hit.doc_id()
            << " span.kind=" << ev.span().kind() << "\n";
  return 0;
}
