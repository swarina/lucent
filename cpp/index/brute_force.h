#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "index/vector_index.h"

namespace lucent {

// Exact KNN by dense inner product (Eigen matvec). Permanently serves two
// roles (PLAN §2.2): the M0 steel-thread index, and the recall oracle that
// referees HNSW forever. `ef` is accepted and ignored; `visited` == n.
class BruteForceIndex : public VectorIndex {
 public:
  explicit BruteForceIndex(int dim);

  // Takes ownership of preloaded arrays (row i of `vectors` = ids[i]).
  BruteForceIndex(int dim, std::vector<uint64_t> ids, std::vector<float> vectors,
                  bool sealed);

  void Add(uint64_t doc_id, const float* vec) override;
  void Seal() override;
  IndexSearchResult Search(const float* query, uint32_t k, uint32_t ef,
                           TraceSink* sink) const override;

  size_t Size() const override { return ids_.size(); }
  int Dim() const override { return dim_; }
  bool Sealed() const override { return sealed_; }

  const std::vector<uint64_t>& ids() const { return ids_; }
  const std::vector<float>& vectors() const { return vectors_; }

 private:
  const int dim_;
  bool sealed_ = false;
  std::vector<uint64_t> ids_;   // row -> doc_id
  std::vector<float> vectors_;  // row-major n x dim, L2-normalized
};

}  // namespace lucent
