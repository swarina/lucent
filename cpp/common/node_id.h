#pragma once

#include <optional>
#include <string>
#include <string_view>

namespace lucent {

// Node identity scheme (PLAN.md §4):
//   shard-{i}{a|b}   e.g. "shard-2a"  (a = primary slot, b = backup slot)
//   coord-{n}        e.g. "coord-0"
//   embed-{n}
//   collector-{n}
//   member-{n}       (M5 raft voters)
enum class Role { kShard, kCoordinator, kEmbed, kCollector, kMember };

struct NodeIdentity {
  Role role;
  std::string id;       // original string, e.g. "shard-2a"
  int shard_id = -1;    // valid for kShard
  char replica = '\0';  // 'a' | 'b', valid for kShard
  int ordinal = -1;     // valid for non-shard roles

  // Returns nullopt on malformed input. Strict: no extra characters allowed.
  static std::optional<NodeIdentity> Parse(std::string_view s);
};

}  // namespace lucent
