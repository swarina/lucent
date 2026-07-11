#include "common/spsc_ring.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <thread>
#include <vector>

namespace lucent {
namespace {

TEST(SpscRing, FifoOrder) {
  SpscRing<int> ring(8);
  for (int i = 0; i < 5; ++i) EXPECT_TRUE(ring.TryPush(int{i}));
  int out = -1;
  for (int i = 0; i < 5; ++i) {
    ASSERT_TRUE(ring.TryPop(out));
    EXPECT_EQ(out, i);
  }
  EXPECT_FALSE(ring.TryPop(out));
}

TEST(SpscRing, CapacityRoundsUpToPowerOfTwo) {
  SpscRing<int> ring(5);
  EXPECT_EQ(ring.Capacity(), 8u);
}

TEST(SpscRing, DropsNewestWhenFullAndCounts) {
  SpscRing<int> ring(4);
  for (int i = 0; i < 4; ++i) EXPECT_TRUE(ring.TryPush(int{i}));
  EXPECT_FALSE(ring.TryPush(99));
  EXPECT_FALSE(ring.TryPush(100));
  EXPECT_EQ(ring.Dropped(), 2u);
  // Contents are the OLD items, untouched by the failed pushes.
  int out = -1;
  for (int i = 0; i < 4; ++i) {
    ASSERT_TRUE(ring.TryPop(out));
    EXPECT_EQ(out, i);
  }
  // Space reclaimed after pops.
  EXPECT_TRUE(ring.TryPush(7));
}

TEST(SpscRing, ThreadedProducerConsumerDeliversAllInOrder) {
  constexpr uint64_t kCount = 200000;
  SpscRing<uint64_t> ring(1024);
  std::vector<uint64_t> received;
  received.reserve(kCount);

  std::thread consumer([&] {
    uint64_t v = 0;
    while (received.size() < kCount) {
      if (ring.TryPop(v)) received.push_back(v);
    }
  });
  // Producer retries on full (no drops in this test) so order+completeness
  // are exactly checkable.
  for (uint64_t i = 0; i < kCount; ++i) {
    while (!ring.TryPush(uint64_t{i})) {}
  }
  consumer.join();

  ASSERT_EQ(received.size(), kCount);
  for (uint64_t i = 0; i < kCount; ++i) ASSERT_EQ(received[i], i);
  // Every failed push counted; exact count is timing-dependent, but the
  // delivered stream above proves none of the drops were silent losses.
  SUCCEED();
}

}  // namespace
}  // namespace lucent
