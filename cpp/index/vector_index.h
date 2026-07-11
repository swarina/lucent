#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace lucent {

// Trace hook points inside an index search (docs/protocol.md TraceBlob
// meta.kind encoding; internals.md §1.2 marks where each fires).
enum class TraceKind : uint8_t {
  kEntry = 0,
  kVisit = 1,
  kAccept = 2,
  kPrune = 3,
  kResult = 4,
};

// Receives per-hop traversal records during a FULL-trace search. Implementors
// must be allocation-free and non-blocking in Record(): it runs inside the
// search loop. `sink == nullptr` disables tracing entirely (NONE/SPANS tiers).
class TraceSink {
 public:
  virtual ~TraceSink() = default;
  virtual void Record(uint8_t layer, TraceKind kind, uint32_t node_row,
                      uint32_t parent_row, float dist) = 0;
};

struct IndexHit {
  uint64_t doc_id = 0;
  float score = 0.0F;  // inner product on normalized vectors (higher = closer)
  uint32_t row = 0;    // shard-local row; keys projections and trace blobs
};

struct IndexSearchResult {
  std::vector<IndexHit> hits;  // <= k, sorted by (score desc, doc_id asc)
  uint32_t visited = 0;        // candidates evaluated
};

// Contract (PLAN §6.1): Add() only before Seal(); Search() only after. Sealed
// indexes are immutable — post-seal reads take no locks. Both implementations
// (BruteForceIndex now, HnswIndex at M1) live behind this interface so the
// shard swaps them without touching the serving path.
class VectorIndex {
 public:
  virtual ~VectorIndex() = default;

  virtual void Add(uint64_t doc_id, const float* vec) = 0;
  virtual void Seal() = 0;
  virtual IndexSearchResult Search(const float* query, uint32_t k, uint32_t ef,
                                   TraceSink* sink) const = 0;

  virtual size_t Size() const = 0;
  virtual int Dim() const = 0;
  virtual bool Sealed() const = 0;
};

}  // namespace lucent
