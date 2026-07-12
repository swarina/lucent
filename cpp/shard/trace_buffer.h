#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/monotime.h"
#include "index/vector_index.h"
#include "lucent/v1/events.pb.h"

namespace lucent {

// FULL-tier trace capture (internals.md §1.4): preallocated per-query buffer;
// Record() appends a fixed-size struct — no allocation past the reserve, no
// I/O, drop-with-counter on overflow (never blocks the search). Seal()
// transposes to the columnar TraceBlob wire form afterwards, off the timed
// search path.
class TraceBuffer final : public TraceSink {
 public:
  explicit TraceBuffer(size_t max_records) : cap_(max_records) {
    records_.reserve(cap_);
    t_start_mono_ns_ = MonoNanos();
  }

  void Record(uint8_t layer, TraceKind kind, uint32_t node_row,
              uint32_t parent_row, float dist) override {
    if (records_.size() >= cap_) {
      ++dropped_;
      return;
    }
    records_.push_back(Rec{
        node_row, parent_row, dist,
        static_cast<uint32_t>((MonoNanos() - t_start_mono_ns_) / 1000),
        // meta = (layer & 0xF) << 3 | kind   (protocol.md TraceBlob)
        static_cast<uint32_t>((static_cast<uint32_t>(layer) & 0xFU) << 3 |
                              static_cast<uint32_t>(kind))});
  }

  size_t size() const { return records_.size(); }
  uint32_t dropped() const { return dropped_; }

  lucent::v1::TraceBlob Seal(const std::string& trace_id,
                             const std::string& node_id,
                             uint32_t shard_id) const {
    lucent::v1::TraceBlob blob;
    blob.set_trace_id(trace_id);
    blob.set_node_id(node_id);
    blob.set_shard_id(shard_id);
    blob.set_t_start_mono_ns(t_start_mono_ns_);
    blob.mutable_node()->Reserve(static_cast<int>(records_.size()));
    blob.mutable_parent()->Reserve(static_cast<int>(records_.size()));
    blob.mutable_dist()->Reserve(static_cast<int>(records_.size()));
    blob.mutable_t_off_us()->Reserve(static_cast<int>(records_.size()));
    blob.mutable_meta()->Reserve(static_cast<int>(records_.size()));
    for (const Rec& r : records_) {
      blob.add_node(r.node);
      blob.add_parent(r.parent);
      blob.add_dist(r.dist);
      blob.add_t_off_us(r.t_off_us);
      blob.add_meta(r.meta);
    }
    blob.set_dropped(dropped_);
    return blob;
  }

 private:
  struct Rec {
    uint32_t node;
    uint32_t parent;
    float dist;
    uint32_t t_off_us;
    uint32_t meta;
  };

  const size_t cap_;
  std::vector<Rec> records_;
  uint32_t dropped_ = 0;
  uint64_t t_start_mono_ns_ = 0;
};

}  // namespace lucent
