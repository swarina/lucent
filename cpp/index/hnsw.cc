#include "index/hnsw.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <stdexcept>

namespace lucent {

namespace {

// Per-thread scratch: the epoch-stamped visited set (internals.md §1.2).
// thread_local so concurrent post-seal searches never race and steady-state
// searches never allocate (the vector grows once per thread per index size).
struct VisitScratch {
  std::vector<uint32_t> stamp;
  uint32_t epoch = 0;

  void Begin(size_t n) {
    if (stamp.size() < n) stamp.resize(n, 0);
    ++epoch;
    if (epoch == 0) {  // u32 wrap: reset stamps once every ~4B searches
      std::fill(stamp.begin(), stamp.end(), 0);
      epoch = 1;
    }
  }
  bool Visited(uint32_t row) const { return stamp[row] == epoch; }
  void Mark(uint32_t row) { stamp[row] = epoch; }
};

thread_local VisitScratch t_scratch;

}  // namespace

HnswIndex::HnswIndex(int dim, Params params)
    : dim_(dim),
      params_(params),
      inv_log_m_(1.0 / std::log(static_cast<double>(params.m))),
      adjacency_(kMaxLevels),
      rng_(params.seed) {
  if (dim <= 0) throw std::invalid_argument("HnswIndex: dim must be > 0");
  if (params.m < 2 || params.m0 < params.m || params.ef_construction < params.m) {
    throw std::invalid_argument("HnswIndex: bad params (want m>=2, m0>=m, efc>=m)");
  }
}

HnswIndex::HnswIndex(int dim, Params params, std::vector<uint64_t> ids,
                     std::vector<float> vectors, std::vector<uint8_t> levels,
                     std::vector<std::vector<std::vector<uint32_t>>> adjacency,
                     uint32_t entry_row, int max_level)
    : HnswIndex(dim, params) {
  const size_t n = ids.size();
  if (vectors.size() != n * static_cast<size_t>(dim) || levels.size() != n ||
      max_level < 0 || max_level >= kMaxLevels ||
      adjacency.size() != static_cast<size_t>(max_level) + 1 ||
      (n > 0 && entry_row >= n)) {
    throw std::invalid_argument("HnswIndex: inconsistent deserialized state");
  }
  ids_ = std::move(ids);
  vectors_ = std::move(vectors);
  levels_ = std::move(levels);
  for (size_t lc = 0; lc < adjacency.size(); ++lc) {
    if (adjacency[lc].size() != n) {
      throw std::invalid_argument("HnswIndex: adjacency layer size mismatch");
    }
    adjacency_[lc] = std::move(adjacency[lc]);
  }
  for (size_t lc = adjacency_.size(); lc-- > static_cast<size_t>(max_level) + 1;) {
    adjacency_[lc].assign(n, {});
  }
  entry_row_ = entry_row;
  max_level_ = max_level;
  sealed_ = true;  // sealed graphs are the only thing ever serialized
}

float HnswIndex::Dist(const float* q, uint32_t row) const {
  const float* v = vectors_.data() + static_cast<size_t>(row) * static_cast<size_t>(dim_);
  float dot = 0.0F;
  for (int i = 0; i < dim_; ++i) dot += q[i] * v[i];
  return 1.0F - dot;
}

int HnswIndex::DrawLevel() {
  // floor(-ln(U) * mL); U in (0,1]. The ONLY consumer of rng_ (determinism).
  std::uniform_real_distribution<double> uni(0.0, 1.0);
  double u = uni(rng_);
  if (u <= 0.0) u = 1e-18;
  const int level = static_cast<int>(-std::log(u) * inv_log_m_);
  return std::min(level, kMaxLevels - 1);
}

const std::vector<uint32_t>& HnswIndex::Neighbors(int layer, uint32_t row) const {
  return adjacency_[static_cast<size_t>(layer)][row];
}

uint64_t HnswIndex::EdgeCount() const {
  uint64_t edges = 0;
  for (const auto& layer : adjacency_) {
    for (const auto& nbrs : layer) edges += nbrs.size();
  }
  return edges;
}

uint32_t HnswIndex::GreedyDescend(const float* q, uint32_t from, int layer,
                                  TraceSink* sink, uint32_t* visited) const {
  uint32_t cur = from;
  float cur_dist = Dist(q, cur);
  bool improved = true;
  while (improved) {
    improved = false;
    for (uint32_t nbr : adjacency_[static_cast<size_t>(layer)][cur]) {
      const float d = Dist(q, nbr);
      if (visited != nullptr) ++*visited;
      if (sink != nullptr) {
        sink->Record(static_cast<uint8_t>(layer), TraceKind::kVisit, nbr, cur, d);
      }
      if (d < cur_dist || (d == cur_dist && nbr < cur)) {
        cur_dist = d;
        cur = nbr;
        improved = true;
      }
    }
  }
  return cur;
}

std::vector<HnswIndex::Cand> HnswIndex::SearchLayer(
    const float* q, const std::vector<Cand>& entry, uint32_t ef, int layer,
    TraceSink* sink, uint32_t* visited_out) const {
  const auto& adj = adjacency_[static_cast<size_t>(layer)];
  t_scratch.Begin(ids_.size());
  uint32_t visited = 0;

  // candidates: min-heap by dist (best first); result: max-heap (worst first).
  auto worse = [](const Cand& a, const Cand& b) { return a < b; };   // max-heap
  auto better = [](const Cand& a, const Cand& b) { return b < a; };  // min-heap
  std::vector<Cand> candidates;  // heap w/ `better`
  std::vector<Cand> result;      // heap w/ `worse`

  for (const Cand& e : entry) {
    if (t_scratch.Visited(e.row)) continue;
    t_scratch.Mark(e.row);
    candidates.push_back(e);
    result.push_back(e);
  }
  std::make_heap(candidates.begin(), candidates.end(), better);
  std::make_heap(result.begin(), result.end(), worse);

  while (!candidates.empty()) {
    std::pop_heap(candidates.begin(), candidates.end(), better);
    const Cand c = candidates.back();
    candidates.pop_back();
    if (!result.empty() && result.front().dist < c.dist &&
        result.size() >= ef) {
      break;  // best remaining candidate can't improve the frontier
    }
    for (uint32_t nbr : adj[c.row]) {
      if (t_scratch.Visited(nbr)) continue;
      t_scratch.Mark(nbr);
      const float d = Dist(q, nbr);
      ++visited;
      if (sink != nullptr) {
        sink->Record(static_cast<uint8_t>(layer), TraceKind::kVisit, nbr,
                     c.row, d);
      }
      if (result.size() < ef || d < result.front().dist) {
        candidates.push_back(Cand{d, nbr});
        std::push_heap(candidates.begin(), candidates.end(), better);
        result.push_back(Cand{d, nbr});
        std::push_heap(result.begin(), result.end(), worse);
        if (sink != nullptr) {
          sink->Record(static_cast<uint8_t>(layer), TraceKind::kAccept, nbr,
                       c.row, d);
        }
        if (result.size() > ef) {
          std::pop_heap(result.begin(), result.end(), worse);
          result.pop_back();
        }
      }
    }
  }
  if (visited_out != nullptr) *visited_out += visited;
  std::sort(result.begin(), result.end());  // asc (dist, row)
  return result;
}

std::vector<uint32_t> HnswIndex::SelectNeighbors(std::vector<Cand> candidates,
                                                 size_t cap) const {
  std::sort(candidates.begin(), candidates.end());
  std::vector<uint32_t> selected;
  std::vector<Cand> pruned;
  for (const Cand& c : candidates) {
    if (selected.size() >= cap) break;
    // Accept iff closer to base than to every already-selected neighbor —
    // spreads edges across directions instead of clustering (paper Alg. 4).
    bool ok = true;
    const float* cv =
        vectors_.data() + static_cast<size_t>(c.row) * static_cast<size_t>(dim_);
    for (uint32_t s : selected) {
      if (Dist(cv, s) < c.dist) {
        ok = false;
        break;
      }
    }
    if (ok) {
      selected.push_back(c.row);
    } else {
      pruned.push_back(c);
    }
  }
  // keepPrunedConnections: fill remaining slots from pruned, nearest first.
  for (const Cand& p : pruned) {
    if (selected.size() >= cap) break;
    selected.push_back(p.row);
  }
  return selected;
}

void HnswIndex::Add(uint64_t doc_id, const float* vec) {
  if (sealed_) throw std::logic_error("HnswIndex::Add after Seal");
  const auto row = static_cast<uint32_t>(ids_.size());
  ids_.push_back(doc_id);
  const size_t old = vectors_.size();
  vectors_.resize(old + static_cast<size_t>(dim_));
  std::memcpy(vectors_.data() + old, vec, sizeof(float) * static_cast<size_t>(dim_));

  const int level = DrawLevel();
  levels_.push_back(static_cast<uint8_t>(level));
  for (int lc = 0; lc < kMaxLevels; ++lc) {
    adjacency_[static_cast<size_t>(lc)].emplace_back();
  }

  if (max_level_ < 0) {  // first element
    entry_row_ = row;
    max_level_ = level;
    return;
  }

  const float* q = vectors_.data() + old;
  uint32_t ep = entry_row_;
  for (int lc = max_level_; lc > level; --lc) {
    ep = GreedyDescend(q, ep, lc, nullptr, nullptr);
  }

  std::vector<Cand> entry{Cand{Dist(q, ep), ep}};
  for (int lc = std::min(level, max_level_); lc >= 0; --lc) {
    std::vector<Cand> found = SearchLayer(
        q, entry, static_cast<uint32_t>(params_.ef_construction), lc, nullptr,
        nullptr);
    const std::vector<uint32_t> neighbors = SelectNeighbors(found, CapFor(lc));
    auto& adj = adjacency_[static_cast<size_t>(lc)];
    for (uint32_t nbr : neighbors) {
      adj[row].push_back(nbr);
      adj[nbr].push_back(row);
      if (adj[nbr].size() > CapFor(lc)) PruneNode(nbr, lc);
    }
    entry = std::move(found);
  }

  if (level > max_level_) {
    max_level_ = level;
    entry_row_ = row;
  }
}

void HnswIndex::PruneNode(uint32_t row, int layer) {
  auto& nbrs = adjacency_[static_cast<size_t>(layer)][row];
  const float* base =
      vectors_.data() + static_cast<size_t>(row) * static_cast<size_t>(dim_);
  std::vector<Cand> cands;
  cands.reserve(nbrs.size());
  for (uint32_t n : nbrs) cands.push_back(Cand{Dist(base, n), n});
  nbrs = SelectNeighbors(std::move(cands), CapFor(layer));
}

void HnswIndex::Seal() {
  if (sealed_) return;
  // Deterministic, cache-friendly iteration order for serving and traces:
  // neighbor lists sorted by (distance, row) — internals.md §1 + §6.
  for (int lc = 0; lc < kMaxLevels; ++lc) {
    auto& layer = adjacency_[static_cast<size_t>(lc)];
    for (uint32_t row = 0; row < layer.size(); ++row) {
      const float* base = vectors_.data() +
                          static_cast<size_t>(row) * static_cast<size_t>(dim_);
      std::vector<Cand> cands;
      cands.reserve(layer[row].size());
      for (uint32_t n : layer[row]) cands.push_back(Cand{Dist(base, n), n});
      std::sort(cands.begin(), cands.end());
      layer[row].clear();
      for (const Cand& c : cands) layer[row].push_back(c.row);
    }
  }
  sealed_ = true;
}

IndexSearchResult HnswIndex::Search(const float* query, uint32_t k,
                                    uint32_t ef, TraceSink* sink) const {
  if (!sealed_) throw std::logic_error("HnswIndex::Search before Seal");
  IndexSearchResult out;
  if (ids_.empty() || k == 0) return out;

  const uint32_t ef_eff = std::max(ef == 0 ? 10U : ef, k);
  uint32_t visited = 0;

  uint32_t ep = entry_row_;
  if (sink != nullptr) {
    sink->Record(static_cast<uint8_t>(max_level_), TraceKind::kEntry, ep, ep,
                 Dist(query, ep));
  }
  ++visited;  // the entry point is evaluated
  for (int lc = max_level_; lc >= 1; --lc) {
    ep = GreedyDescend(query, ep, lc, sink, &visited);
  }

  std::vector<Cand> entry{Cand{Dist(query, ep), ep}};
  if (sink != nullptr && ep != entry_row_) {
    sink->Record(0, TraceKind::kEntry, ep, ep, entry[0].dist);
  }
  std::vector<Cand> frontier =
      SearchLayer(query, entry, ef_eff, 0, sink, &visited);

  const size_t kk = std::min<size_t>(k, frontier.size());
  out.hits.reserve(kk);
  for (size_t i = 0; i < kk; ++i) {
    const Cand& c = frontier[i];
    const float score = 1.0F - c.dist;
    out.hits.push_back(IndexHit{ids_[c.row], score, c.row});
    if (sink != nullptr) {
      sink->Record(0, TraceKind::kResult, c.row, c.row, c.dist);
    }
  }
  // (score desc, doc_id asc): frontier is (dist asc, row asc) — rows with
  // equal dist may not be in doc_id order, so fix up explicitly.
  std::sort(out.hits.begin(), out.hits.end(),
            [](const IndexHit& a, const IndexHit& b) {
              if (a.score != b.score) return a.score > b.score;
              return a.doc_id < b.doc_id;
            });
  out.visited = visited;
  return out;
}

}  // namespace lucent
