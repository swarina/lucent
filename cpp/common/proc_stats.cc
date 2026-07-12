#include "common/proc_stats.h"

#if defined(__APPLE__)
#include <mach/mach.h>
#elif defined(__linux__)
#include <unistd.h>

#include <cstdio>
#endif

namespace lucent {

uint64_t CurrentRssBytes() {
#if defined(__APPLE__)
  mach_task_basic_info info;
  mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
  if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO,
                reinterpret_cast<task_info_t>(&info), &count) != KERN_SUCCESS) {
    return 0;
  }
  return info.resident_size;
#elif defined(__linux__)
  FILE* f = std::fopen("/proc/self/statm", "r");
  if (f == nullptr) return 0;
  unsigned long size = 0;
  unsigned long resident = 0;
  const int matched = std::fscanf(f, "%lu %lu", &size, &resident);
  std::fclose(f);
  if (matched != 2) return 0;
  return static_cast<uint64_t>(resident) *
         static_cast<uint64_t>(sysconf(_SC_PAGESIZE));
#else
  return 0;
#endif
}

}  // namespace lucent
