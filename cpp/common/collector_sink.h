#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "common/event_emitter.h"
#include "lucent/v1/events.grpc.pb.h"

namespace lucent {

// EventEmitter sink that streams batches to CollectorService.PublishEvents.
// Failure policy (protocol.md §6, "Collector down"): never blocks the caller
// beyond the gRPC write itself, drops batches while the collector is
// unreachable (counted), and retries the connection with a fixed backoff.
// Called only from the emitter's publisher thread — no internal locking.
class CollectorSink {
 public:
  CollectorSink(std::string collector_addr, std::string node_id);
  ~CollectorSink();

  // Bind as the EventEmitter's publish function.
  EventEmitter::PublishFn Fn();

  uint64_t dropped_batches() const { return dropped_batches_; }

  // Closes the stream cleanly (called by dtor).
  void Shutdown();

 private:
  void Publish(std::vector<lucent::v1::Event>&& events);
  bool EnsureStream();
  void TearDownStream();

  const std::string addr_;
  const std::string node_id_;
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<lucent::v1::CollectorService::Stub> stub_;

  std::unique_ptr<grpc::ClientContext> ctx_;
  std::unique_ptr<grpc::ClientWriter<lucent::v1::EventBatch>> writer_;
  lucent::v1::Ack ack_;

  uint64_t dropped_batches_ = 0;
  uint64_t last_attempt_ns_ = 0;  // reconnect backoff (1s)
};

}  // namespace lucent
