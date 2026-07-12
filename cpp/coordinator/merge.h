#pragma once

#include <algorithm>
#include <cstdint>
#include <vector>

#include "lucent/v1/common.pb.h"
#include "lucent/v1/shard.pb.h"

namespace lucent {

// Pure merge logic (internals.md §2.2): no I/O in these signatures — they are
// property-tested directly and the RPC layer just calls them.

struct ShardResult {
  uint32_t shard_id = 0;
  lucent::v1::SearchResponse resp;
};

// K-way merge of per-shard top-k lists into the global top-k, ordered by
// (score desc, doc_id asc) — the doc_id tiebreak is a determinism rule.
// Partitions are disjoint, so no dedupe. Hits are annotated with the source
// shard_id and serving node_id (provenance for the UI). Sizes are tiny
// (k × shards ≤ ~100), so a full sort beats heap bookkeeping.
inline std::vector<lucent::v1::Hit> MergeTopK(
    const std::vector<ShardResult>& results, uint32_t k) {
  std::vector<lucent::v1::Hit> all;
  for (const ShardResult& r : results) {
    for (const auto& h : r.resp.hits()) {
      lucent::v1::Hit annotated = h;
      annotated.set_shard_id(r.shard_id);
      annotated.set_node_id(r.resp.node_id());
      all.push_back(std::move(annotated));
    }
  }
  std::sort(all.begin(), all.end(),
            [](const lucent::v1::Hit& a, const lucent::v1::Hit& b) {
              if (a.score() != b.score()) return a.score() > b.score();
              return a.doc_id() < b.doc_id();
            });
  if (all.size() > k) all.resize(k);
  return all;
}

}  // namespace lucent
