#include "common/event_emitter.h"

#include <utility>

#include "common/monotime.h"

namespace lucent {

EventEmitter::EventEmitter(std::string node_id, PublishFn publish)
    : EventEmitter(std::move(node_id), std::move(publish), Options{}) {}

EventEmitter::EventEmitter(std::string node_id, PublishFn publish, Options opts)
    : node_id_(std::move(node_id)),
      publish_(std::move(publish)),
      opts_(opts),
      ring_(opts.ring_capacity),
      publisher_([this] { PublisherLoop(); }) {}

EventEmitter::~EventEmitter() { Stop(); }

bool EventEmitter::Emit(lucent::v1::Event event) {
  event.set_node_id(node_id_);
  event.set_t_mono_ns(MonoNanos());
  bool pushed;
  {
    std::lock_guard<std::mutex> lock(producer_mu_);
    // Seq is assigned under the same lock as the push so ring order == seq
    // order; gaps in seq at the collector therefore mean exactly "drops".
    event.set_seq(seq_.fetch_add(1, std::memory_order_relaxed) + 1);
    pushed = ring_.TryPush(std::move(event));
  }
  if (pushed) wake_cv_.notify_one();
  return pushed;
}

void EventEmitter::Flush() {
  const uint64_t target = seq_.load(std::memory_order_relaxed);
  std::unique_lock<std::mutex> lock(wake_mu_);
  wake_cv_.notify_one();
  drained_cv_.wait(lock, [&] {
    // Every emitted event ends up either handed to the sink or dropped, so
    // published + dropped >= emitted-at-call-time is exact completion.
    return published_count_ + ring_.Dropped() >= target || stop_;
  });
}

void EventEmitter::Stop() {
  {
    std::lock_guard<std::mutex> lock(wake_mu_);
    if (stop_) return;
    stop_ = true;
  }
  wake_cv_.notify_all();
  if (publisher_.joinable()) publisher_.join();
}

void EventEmitter::PublisherLoop() {
  std::vector<lucent::v1::Event> batch;
  batch.reserve(opts_.batch_max);

  for (;;) {
    bool stopping;
    {
      std::unique_lock<std::mutex> lock(wake_mu_);
      wake_cv_.wait_for(lock, opts_.flush_interval,
                        [&] { return stop_ || !ring_.Empty(); });
      stopping = stop_;
    }

    // Drain up to batch_max per publish call; loop again immediately while
    // there is backlog so a burst never waits on the flush interval.
    do {
      batch.clear();
      lucent::v1::Event ev;
      while (batch.size() < opts_.batch_max && ring_.TryPop(ev)) {
        batch.push_back(std::move(ev));
      }
      if (batch.empty()) break;
      const size_t batch_size = batch.size();
      publish_(std::move(batch));
      batch = {};
      batch.reserve(opts_.batch_max);
      {
        std::lock_guard<std::mutex> lock(wake_mu_);
        published_count_ += batch_size;
      }
      drained_cv_.notify_all();
    } while (!ring_.Empty());

    if (stopping && ring_.Empty()) {
      drained_cv_.notify_all();
      return;
    }
  }
}

}  // namespace lucent
