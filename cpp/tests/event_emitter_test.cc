#include "common/event_emitter.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>

#include "lucent/v1/events.pb.h"

namespace lucent {
namespace {

using lucent::v1::Event;

// Thread-safe capture sink.
class CaptureSink {
 public:
  EventEmitter::PublishFn Fn() {
    return [this](std::vector<Event>&& batch) {
      std::lock_guard<std::mutex> lock(mu_);
      batch_sizes_.push_back(batch.size());
      for (auto& e : batch) events_.push_back(std::move(e));
    };
  }
  std::vector<Event> events() {
    std::lock_guard<std::mutex> lock(mu_);
    return events_;
  }
  std::vector<size_t> batch_sizes() {
    std::lock_guard<std::mutex> lock(mu_);
    return batch_sizes_;
  }

 private:
  std::mutex mu_;
  std::vector<Event> events_;
  std::vector<size_t> batch_sizes_;
};

Event SpanEvent() {
  Event e;
  e.mutable_span()->set_kind(lucent::v1::SPAN_QUERY_RECEIVED);
  return e;
}

TEST(EventEmitter, StampsIdentitySeqAndTime) {
  CaptureSink sink;
  {
    EventEmitter emitter("shard-0a", sink.Fn());
    for (int i = 0; i < 10; ++i) EXPECT_TRUE(emitter.Emit(SpanEvent()));
    emitter.Flush();
  }
  auto events = sink.events();
  ASSERT_EQ(events.size(), 10u);
  for (size_t i = 0; i < events.size(); ++i) {
    EXPECT_EQ(events[i].node_id(), "shard-0a");
    EXPECT_EQ(events[i].seq(), i + 1);  // 1-based, gapless when nothing drops
    EXPECT_GT(events[i].t_mono_ns(), 0u);
  }
}

TEST(EventEmitter, BatchesRespectMax) {
  CaptureSink sink;
  EventEmitter::Options opts;
  opts.batch_max = 16;
  {
    EventEmitter emitter("coord-0", sink.Fn(), opts);
    for (int i = 0; i < 100; ++i) emitter.Emit(SpanEvent());
    emitter.Flush();
  }
  ASSERT_EQ(sink.events().size(), 100u);
  for (size_t s : sink.batch_sizes()) EXPECT_LE(s, 16u);
}

TEST(EventEmitter, DropsWhenFullAndCounts) {
  // Tiny ring + a sink that blocks until released: guarantees overflow.
  std::atomic<bool> release{false};
  std::atomic<int> delivered{0};
  EventEmitter::Options opts;
  opts.ring_capacity = 8;
  opts.batch_max = 4;
  EventEmitter emitter("embed-0", [&](std::vector<Event>&& batch) {
    while (!release.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    delivered += static_cast<int>(batch.size());
  }, opts);

  int accepted = 0;
  for (int i = 0; i < 100; ++i) {
    if (emitter.Emit(SpanEvent())) ++accepted;
  }
  EXPECT_LT(accepted, 100);  // ring (8) + one in-flight batch can't hold 100
  EXPECT_EQ(emitter.dropped(), static_cast<uint64_t>(100 - accepted));

  release = true;
  emitter.Flush();
  emitter.Stop();
  EXPECT_EQ(delivered.load(), accepted);
}

TEST(EventEmitter, MultipleProducerThreads) {
  CaptureSink sink;
  {
    EventEmitter emitter("collector-0", sink.Fn());
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
      threads.emplace_back([&] {
        for (int i = 0; i < 250; ++i) emitter.Emit(SpanEvent());
      });
    }
    for (auto& th : threads) th.join();
    emitter.Flush();
  }
  auto events = sink.events();
  ASSERT_EQ(events.size(), 1000u);
  // Seq order must match delivery order (assigned under the producer lock).
  for (size_t i = 0; i < events.size(); ++i) EXPECT_EQ(events[i].seq(), i + 1);
}

TEST(EventEmitter, StopDrainsRemaining) {
  CaptureSink sink;
  EventEmitter emitter("shard-1b", sink.Fn());
  for (int i = 0; i < 50; ++i) emitter.Emit(SpanEvent());
  emitter.Stop();
  EXPECT_EQ(sink.events().size(), 50u);
}

}  // namespace
}  // namespace lucent
