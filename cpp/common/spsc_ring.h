#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace lucent {

// Bounded lock-free single-producer / single-consumer ring (internals.md §1.4).
//
// Semantics:
//  - TryPush never blocks. When full it drops the INCOMING item and counts it
//    (drop-newest): the producer cannot reclaim unconsumed slots without
//    coordinating with the consumer, which would forfeit lock-freedom. Older
//    events are the ones already being drained, so this is also the sensible
//    policy. The drop counter is monotone and readable from any thread.
//  - Capacity is rounded up to a power of two (index masking).
//  - Exactly one producer thread and one consumer thread; callers needing
//    multiple producers must serialize externally (see EventEmitter).
template <typename T>
class SpscRing {
 public:
  explicit SpscRing(size_t min_capacity)
      : capacity_(RoundUpPow2(min_capacity)),
        mask_(capacity_ - 1),
        slots_(capacity_) {}

  SpscRing(const SpscRing&) = delete;
  SpscRing& operator=(const SpscRing&) = delete;

  // Producer side.
  bool TryPush(T&& item) {
    const size_t head = head_.load(std::memory_order_relaxed);
    const size_t tail = tail_.load(std::memory_order_acquire);
    if (head - tail >= capacity_) {
      dropped_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
    slots_[head & mask_] = std::move(item);
    head_.store(head + 1, std::memory_order_release);
    return true;
  }

  // Consumer side.
  bool TryPop(T& out) {
    const size_t tail = tail_.load(std::memory_order_relaxed);
    const size_t head = head_.load(std::memory_order_acquire);
    if (tail == head) return false;
    out = std::move(slots_[tail & mask_]);
    tail_.store(tail + 1, std::memory_order_release);
    return true;
  }

  // Approximate; exact only from the owning side.
  size_t Size() const {
    const size_t head = head_.load(std::memory_order_acquire);
    const size_t tail = tail_.load(std::memory_order_acquire);
    return head - tail;
  }
  bool Empty() const { return Size() == 0; }
  size_t Capacity() const { return capacity_; }
  uint64_t Dropped() const { return dropped_.load(std::memory_order_relaxed); }

 private:
  static size_t RoundUpPow2(size_t v) {
    size_t p = 1;
    while (p < v) p <<= 1;
    return p;
  }

  const size_t capacity_;
  const size_t mask_;
  std::vector<T> slots_;
  // Producer and consumer cursors on separate cache lines to avoid false
  // sharing between the two threads.
  alignas(64) std::atomic<size_t> head_{0};
  alignas(64) std::atomic<size_t> tail_{0};
  alignas(64) std::atomic<uint64_t> dropped_{0};
};

}  // namespace lucent
