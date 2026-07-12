#pragma once

#include <cstdint>
#include <random>
#include <vector>

#include "index/vector_index.h"

namespace lucent {

// Hand-rolled HNSW (internals.md §1) — the reason this project owns its
// search loop: TraceSink fires at every hop, which no library exposes.
//
// Determinism contract (internals.md §6): one RNG seeded from the manifest,
// consumed only by level assignment in insertion order; adjacency lists
// sorted by (distance, row) at seal; single-threaded build. Given the same
// seed and insertion order, two builds are byte-identical.
//
// Distance: d = 1 - <q, v> on L2-normalized vectors (smaller = closer),
// matching the TraceBlob wire encoding directly.
class HnswIndex : public VectorIndex {
 public:
  struct Params {
    int m = 16;                // max neighbors, layers >= 1
    int m0 = 32;               // max neighbors, layer 0
    int ef_construction = 200;
    uint64_t seed = 42;
  };

  HnswIndex(int dim, Params params);

  // Deserialization constructor (hnsw_io): adopts a fully built graph and
  // seals immediately. `adjacency` is indexed [layer][row], sized to
  // max_level+1 layers; missing upper layers are padded internally.
  HnswIndex(int dim, Params params, std::vector<uint64_t> ids,
            std::vector<float> vectors, std::vector<uint8_t> levels,
            std::vector<std::vector<std::vector<uint32_t>>> adjacency,
            uint32_t entry_row, int max_level);

  void Add(uint64_t doc_id, const float* vec) override;
  void Seal() override;
  IndexSearchResult Search(const float* query, uint32_t k, uint32_t ef,
                           TraceSink* sink) const override;

  size_t Size() const override { return ids_.size(); }
  int Dim() const override { return dim_; }
  bool Sealed() const override { return sealed_; }

  // Introspection (tests, serialization at M1-T2, build events).
  int max_level() const { return max_level_; }
  uint32_t entry_row() const { return entry_row_; }
  const Params& params() const { return params_; }
  const std::vector<uint64_t>& ids() const { return ids_; }
  const std::vector<float>& vectors() const { return vectors_; }
  const std::vector<uint8_t>& levels() const { return levels_; }
  const std::vector<uint32_t>& Neighbors(int layer, uint32_t row) const;
  uint64_t EdgeCount() const;

  static constexpr int kMaxLevels = 16;  // P(level >= 16) ~ 1e-13 at M=16

 private:
  struct Cand {  // (distance, row), ordered asc by (dist, row)
    float dist;
    uint32_t row;
    bool operator<(const Cand& o) const {
      if (dist != o.dist) return dist < o.dist;
      return row < o.row;
    }
  };

  float Dist(const float* q, uint32_t row) const;
  int DrawLevel();
  uint32_t GreedyDescend(const float* q, uint32_t from, int layer,
                         TraceSink* sink, uint32_t* visited) const;
  // ef-bounded best-first search within one layer; returns candidates sorted
  // asc. `visited_out` counts VISIT records (nullptr ok).
  std::vector<Cand> SearchLayer(const float* q, const std::vector<Cand>& entry,
                                uint32_t ef, int layer, TraceSink* sink,
                                uint32_t* visited_out) const;
  // Paper Alg. 4: extendCandidates=false, keepPrunedConnections=true.
  // Candidate dists are to the base point; the diversity test recomputes
  // candidate->selected dists, so no base pointer is needed.
  std::vector<uint32_t> SelectNeighbors(std::vector<Cand> candidates,
                                        size_t cap) const;
  void PruneNode(uint32_t row, int layer);
  size_t CapFor(int layer) const {
    return layer == 0 ? static_cast<size_t>(params_.m0)
                      : static_cast<size_t>(params_.m);
  }

  const int dim_;
  const Params params_;
  const double inv_log_m_;  // mL = 1 / ln(M)
  bool sealed_ = false;

  std::vector<uint64_t> ids_;    // row -> doc_id
  std::vector<float> vectors_;   // row-major n x dim
  std::vector<uint8_t> levels_;  // row -> top layer
  // adjacency_[layer][row] = neighbor rows; rows above their level have
  // empty vectors at that layer (wastes pointers, trivial at our scale).
  std::vector<std::vector<std::vector<uint32_t>>> adjacency_;

  uint32_t entry_row_ = 0;
  int max_level_ = -1;  // -1 == empty
  std::mt19937_64 rng_;
};

}  // namespace lucent
