#include "common/node_id.h"

#include <charconv>

namespace lucent {
namespace {

// Parses a non-negative integer occupying exactly [begin, end).
std::optional<int> ParseExactInt(std::string_view s) {
  if (s.empty()) return std::nullopt;
  int value = 0;
  const char* begin = s.data();
  const char* end = s.data() + s.size();
  auto [ptr, ec] = std::from_chars(begin, end, value);
  if (ec != std::errc{} || ptr != end || value < 0) return std::nullopt;
  return value;
}

std::optional<NodeIdentity> ParseOrdinalRole(Role role, std::string_view rest,
                                             std::string_view full) {
  auto n = ParseExactInt(rest);
  if (!n) return std::nullopt;
  NodeIdentity out;
  out.role = role;
  out.id = std::string(full);
  out.ordinal = *n;
  return out;
}

}  // namespace

std::optional<NodeIdentity> NodeIdentity::Parse(std::string_view s) {
  constexpr std::string_view kShardPrefix = "shard-";
  constexpr std::string_view kCoordPrefix = "coord-";
  constexpr std::string_view kEmbedPrefix = "embed-";
  constexpr std::string_view kCollectorPrefix = "collector-";
  constexpr std::string_view kMemberPrefix = "member-";

  if (s.starts_with(kShardPrefix)) {
    std::string_view rest = s.substr(kShardPrefix.size());
    if (rest.size() < 2) return std::nullopt;
    char replica = rest.back();
    if (replica != 'a' && replica != 'b') return std::nullopt;
    auto shard = ParseExactInt(rest.substr(0, rest.size() - 1));
    if (!shard) return std::nullopt;
    NodeIdentity out;
    out.role = Role::kShard;
    out.id = std::string(s);
    out.shard_id = *shard;
    out.replica = replica;
    return out;
  }
  if (s.starts_with(kCoordPrefix))
    return ParseOrdinalRole(Role::kCoordinator, s.substr(kCoordPrefix.size()), s);
  if (s.starts_with(kEmbedPrefix))
    return ParseOrdinalRole(Role::kEmbed, s.substr(kEmbedPrefix.size()), s);
  if (s.starts_with(kCollectorPrefix))
    return ParseOrdinalRole(Role::kCollector, s.substr(kCollectorPrefix.size()), s);
  if (s.starts_with(kMemberPrefix))
    return ParseOrdinalRole(Role::kMember, s.substr(kMemberPrefix.size()), s);
  return std::nullopt;
}

}  // namespace lucent
