#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "lucent/v1/common.pb.h"

namespace lucent {

// Pure planning logic (internals.md §2.1). M0 scope: hash partitioning, all
// shards or a `probe` prefix; replica selection = primary (round-robin over
// healthy replicas arrives with M3's HealthWatcher). Semantic centroid
// routing arrives at M4.

struct PlannedShard {
  uint32_t shard_id = 0;
  std::string node_id;  // replica chosen to serve
  std::string addr;
};

struct QueryPlan {
  std::vector<PlannedShard> probe;      // shards to query (with chosen replica)
  std::vector<uint32_t> unprobed;       // skipped by the probe knob
  std::vector<uint32_t> uncovered;      // probed but no healthy replica (M3)
  uint64_t epoch = 0;
};

// Under hash partitioning a probe subset is an arbitrary sample — we take the
// lowest shard_ids for determinism and say so in the UI (recall degrades;
// that contrast with semantic routing IS the M4 demo).
inline QueryPlan PlanQuery(const lucent::v1::ShardMap& map, uint32_t probe) {
  QueryPlan plan;
  plan.epoch = map.epoch();

  std::vector<const lucent::v1::ShardMapEntry*> entries;
  entries.reserve(static_cast<size_t>(map.shards_size()));
  for (const auto& e : map.shards()) entries.push_back(&e);
  std::sort(entries.begin(), entries.end(),
            [](const auto* a, const auto* b) { return a->shard_id() < b->shard_id(); });

  const size_t want = (probe == 0 || probe >= entries.size())
                          ? entries.size()
                          : static_cast<size_t>(probe);
  for (size_t i = 0; i < entries.size(); ++i) {
    if (i < want) {
      plan.probe.push_back(PlannedShard{entries[i]->shard_id(),
                                        entries[i]->primary_node(),
                                        entries[i]->primary_addr()});
    } else {
      plan.unprobed.push_back(entries[i]->shard_id());
    }
  }
  return plan;
}

}  // namespace lucent
