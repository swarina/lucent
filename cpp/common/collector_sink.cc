#include "common/collector_sink.h"

#include <utility>

#include "common/monotime.h"

namespace lucent {

namespace {
constexpr uint64_t kReconnectBackoffNs = 1'000'000'000;  // 1s
}

CollectorSink::CollectorSink(std::string collector_addr, std::string node_id)
    : addr_(std::move(collector_addr)), node_id_(std::move(node_id)) {
  channel_ = grpc::CreateChannel(addr_, grpc::InsecureChannelCredentials());
  stub_ = lucent::v1::CollectorService::NewStub(channel_);
}

CollectorSink::~CollectorSink() { Shutdown(); }

EventEmitter::PublishFn CollectorSink::Fn() {
  return [this](std::vector<lucent::v1::Event>&& events) {
    Publish(std::move(events));
  };
}

void CollectorSink::Shutdown() {
  if (writer_) {
    writer_->WritesDone();
    (void)writer_->Finish();
    writer_.reset();
    ctx_.reset();
  }
}

bool CollectorSink::EnsureStream() {
  if (writer_) return true;
  const uint64_t now = MonoNanos();
  if (now - last_attempt_ns_ < kReconnectBackoffNs && last_attempt_ns_ != 0) {
    return false;
  }
  last_attempt_ns_ = now;
  ctx_ = std::make_unique<grpc::ClientContext>();
  writer_ = stub_->PublishEvents(ctx_.get(), &ack_);
  return writer_ != nullptr;
}

void CollectorSink::TearDownStream() {
  if (writer_) {
    (void)writer_->Finish();
    writer_.reset();
  }
  ctx_.reset();
}

void CollectorSink::Publish(std::vector<lucent::v1::Event>&& events) {
  if (!EnsureStream()) {
    ++dropped_batches_;
    return;
  }
  lucent::v1::EventBatch batch;
  batch.set_node_id(node_id_);
  for (auto& e : events) *batch.add_events() = std::move(e);
  if (!writer_->Write(batch)) {
    // Collector gone mid-stream: drop this batch, reset, retry after backoff.
    ++dropped_batches_;
    TearDownStream();
  }
}

}  // namespace lucent
