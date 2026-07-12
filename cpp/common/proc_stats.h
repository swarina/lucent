#pragma once

#include <cstdint>

namespace lucent {

// Current resident set size in bytes (0 if unavailable). macOS: mach task
// info; Linux: /proc/self/statm. Feeds NodeStats.rss_bytes — the "watch
// memory grow as the index builds" number, so it must be current RSS, not
// getrusage()'s high-water mark.
uint64_t CurrentRssBytes();

}  // namespace lucent
