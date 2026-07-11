#pragma once

#include <chrono>
#include <cstdint>

namespace lucent {

// Per-node monotonic clock, nanoseconds. All event timestamps use this; wall
// clock never appears on a decision path (determinism rule, internals.md §6).
inline uint64_t MonoNanos() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

}  // namespace lucent
