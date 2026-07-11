#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/spsc_ring.h"
#include "lucent/v1/events.pb.h"

namespace lucent {

// Per-node event pipeline for the SPANS/METRICS tiers (internals.md §1.4):
// producers emit protobuf Events into a bounded ring; a background publisher
// thread drains them in batches to a sink. Invariants:
//  - Emit() never blocks on I/O and never allocates unboundedly: ring full ->
//    the event is dropped and counted (surfaced via dropped()).
//  - Batches are <= batch_max events, flushed at least every flush_interval.
//  - node_id / seq / t_mono_ns are stamped here; callers fill only the body.
//
// Producers may be multiple threads (gRPC handlers); pushes are serialized by
// an uncontended mutex in front of the SPSC ring — the lock-free hot path for
// FULL traces is the separate per-query buffer (M1-T3), not this.
//
// The sink is a callback so unit tests and non-gRPC binaries can capture
// events; the gRPC CollectorService client sink arrives with the shard (M0-T5).
class EventEmitter {
 public:
  using PublishFn = std::function<void(std::vector<lucent::v1::Event>&&)>;

  struct Options {
    size_t ring_capacity = 65536;
    size_t batch_max = 512;
    std::chrono::milliseconds flush_interval{50};
  };

  EventEmitter(std::string node_id, PublishFn publish);  // default Options
  EventEmitter(std::string node_id, PublishFn publish, Options opts);
  ~EventEmitter();

  EventEmitter(const EventEmitter&) = delete;
  EventEmitter& operator=(const EventEmitter&) = delete;

  // Stamps identity/seq/time and enqueues. Returns false iff dropped (full).
  bool Emit(lucent::v1::Event event);

  // Blocks until everything emitted before the call has been handed to the
  // sink. Test/shutdown aid — not for hot paths.
  void Flush();

  // Idempotent; drains remaining events, then joins the publisher thread.
  void Stop();

  uint64_t dropped() const { return ring_.Dropped(); }
  uint64_t emitted() const { return seq_.load(std::memory_order_relaxed); }
  const std::string& node_id() const { return node_id_; }

 private:
  void PublisherLoop();

  const std::string node_id_;
  const PublishFn publish_;
  const Options opts_;

  SpscRing<lucent::v1::Event> ring_;
  std::mutex producer_mu_;             // serializes multi-threaded Emit()
  std::atomic<uint64_t> seq_{0};

  std::mutex wake_mu_;
  std::condition_variable wake_cv_;    // producer -> publisher
  std::condition_variable drained_cv_; // publisher -> Flush()
  uint64_t published_count_ = 0;       // events handed to sink; guarded by wake_mu_
  bool stop_ = false;                  // guarded by wake_mu_
  std::thread publisher_;
};

}  // namespace lucent
