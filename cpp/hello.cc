// Toolchain smoke test for the Lucent C++ core.
// Real binaries (lucent-shard, lucent-coord) replace this as M0 progresses.

#include <iostream>
#include <string_view>

#ifndef LUCENT_VERSION
#define LUCENT_VERSION "0.0.0"
#endif

int main() {
  constexpr std::string_view kVersion = LUCENT_VERSION;
  std::cout << "lucent " << kVersion
            << " — a distributed vector search engine you can watch think\n"
            << "C++ toolchain OK (C++" << __cplusplus / 100 % 100 << ")\n";
  return 0;
}
