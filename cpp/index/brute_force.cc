#include "index/brute_force.h"

#include <Eigen/Core>

#include <algorithm>
#include <cstring>
#include <numeric>
#include <stdexcept>
#include <utility>

namespace lucent {

BruteForceIndex::BruteForceIndex(int dim) : dim_(dim) {
  if (dim <= 0) throw std::invalid_argument("BruteForceIndex: dim must be > 0");
}

BruteForceIndex::BruteForceIndex(int dim, std::vector<uint64_t> ids,
                                 std::vector<float> vectors, bool sealed)
    : dim_(dim), sealed_(sealed), ids_(std::move(ids)), vectors_(std::move(vectors)) {
  if (dim <= 0) throw std::invalid_argument("BruteForceIndex: dim must be > 0");
  if (vectors_.size() != ids_.size() * static_cast<size_t>(dim_)) {
    throw std::invalid_argument("BruteForceIndex: ids/vectors size mismatch");
  }
}

void BruteForceIndex::Add(uint64_t doc_id, const float* vec) {
  if (sealed_) throw std::logic_error("BruteForceIndex::Add after Seal");
  ids_.push_back(doc_id);
  const size_t old = vectors_.size();
  vectors_.resize(old + static_cast<size_t>(dim_));
  std::memcpy(vectors_.data() + old, vec, sizeof(float) * static_cast<size_t>(dim_));
}

void BruteForceIndex::Seal() { sealed_ = true; }

IndexSearchResult BruteForceIndex::Search(const float* query, uint32_t k,
                                          uint32_t /*ef*/,
                                          TraceSink* /*sink*/) const {
  if (!sealed_) throw std::logic_error("BruteForceIndex::Search before Seal");
  IndexSearchResult result;
  const size_t n = ids_.size();
  result.visited = static_cast<uint32_t>(n);
  if (n == 0 || k == 0) return result;

  using RowMajor =
      Eigen::Matrix<float, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>;
  Eigen::Map<const RowMajor> mat(vectors_.data(),
                                 static_cast<Eigen::Index>(n),
                                 static_cast<Eigen::Index>(dim_));
  Eigen::Map<const Eigen::VectorXf> q(query, static_cast<Eigen::Index>(dim_));
  Eigen::VectorXf scores = mat * q;

  // Top-k rows by (score desc, doc_id asc) — the doc_id tiebreak is a
  // determinism rule (internals.md §6), not cosmetics.
  const size_t kk = std::min<size_t>(k, n);
  std::vector<uint32_t> rows(n);
  std::iota(rows.begin(), rows.end(), 0U);
  auto better = [&](uint32_t a, uint32_t b) {
    const float sa = scores[static_cast<Eigen::Index>(a)];
    const float sb = scores[static_cast<Eigen::Index>(b)];
    if (sa != sb) return sa > sb;
    return ids_[a] < ids_[b];
  };
  std::nth_element(rows.begin(), rows.begin() + static_cast<std::ptrdiff_t>(kk),
                   rows.end(), better);
  rows.resize(kk);
  std::sort(rows.begin(), rows.end(), better);

  result.hits.reserve(kk);
  for (uint32_t row : rows) {
    result.hits.push_back(IndexHit{ids_[row],
                                   scores[static_cast<Eigen::Index>(row)], row});
  }
  return result;
}

}  // namespace lucent
