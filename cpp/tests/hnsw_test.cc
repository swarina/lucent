#include "index/hnsw.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <random>
#include <set>
#include <thread>
#include <vector>

#include "index/brute_force.h"

namespace lucent {
namespace {

// Seeded synthetic corpus of unit vectors (recall is geometry, not semantics).
struct Corpus {
  int dim;
  std::vector<uint64_t> ids;
  std::vector<float> vecs;  // row-major

  const float* row(size_t i) const { return vecs.data() + i * static_cast<size_t>(dim); }
};

// clusters == 0 -> uniform random on the sphere (adversarial for graph ANN:
// distances concentrate, so the ef frontier churns and visited counts blow
// up — measured ~75% of n at d=64). clusters > 0 -> mixture of Gaussians,
// which is what real embedding corpora look like (MiniLM vectors live on a
// clustered manifold). We test recall on the hard case and sublinearity on
// the realistic one.
Corpus MakeCorpus(size_t n, int dim, uint64_t seed, int clusters = 0) {
  Corpus c;
  c.dim = dim;
  std::mt19937_64 rng(seed);
  std::normal_distribution<float> normal(0.0F, 1.0F);
  std::vector<float> centers(static_cast<size_t>(std::max(clusters, 1)) *
                             static_cast<size_t>(dim));
  for (auto& x : centers) x = normal(rng);
  c.vecs.resize(n * static_cast<size_t>(dim));
  for (size_t i = 0; i < n; ++i) {
    c.ids.push_back(1000 + 3 * i);  // non-contiguous: row/id confusion guard
    float* v = c.vecs.data() + i * static_cast<size_t>(dim);
    if (clusters > 0) {
      const float* ctr = centers.data() +
                         (i % static_cast<size_t>(clusters)) *
                             static_cast<size_t>(dim);
      for (int d = 0; d < dim; ++d) v[d] = ctr[d] + 0.3F * normal(rng);
    } else {
      for (int d = 0; d < dim; ++d) v[d] = normal(rng);
    }
    float norm = 0.0F;
    for (int d = 0; d < dim; ++d) norm += v[d] * v[d];
    norm = std::sqrt(norm);
    for (int d = 0; d < dim; ++d) v[d] /= norm;
  }
  return c;
}

HnswIndex BuildHnsw(const Corpus& c, uint64_t seed = 42) {
  HnswIndex::Params p;
  p.seed = seed;
  HnswIndex index(c.dim, p);
  for (size_t i = 0; i < c.ids.size(); ++i) index.Add(c.ids[i], c.row(i));
  index.Seal();
  return index;
}

BruteForceIndex BuildOracle(const Corpus& c) {
  BruteForceIndex oracle(c.dim);
  for (size_t i = 0; i < c.ids.size(); ++i) oracle.Add(c.ids[i], c.row(i));
  oracle.Seal();
  return oracle;
}

double RecallAt10(const HnswIndex& hnsw, const BruteForceIndex& oracle,
                  const Corpus& queries, uint32_t ef) {
  size_t hit = 0;
  size_t total = 0;
  for (size_t qi = 0; qi < queries.ids.size(); ++qi) {
    const auto truth = oracle.Search(queries.row(qi), 10, 0, nullptr);
    const auto got = hnsw.Search(queries.row(qi), 10, ef, nullptr);
    std::set<uint64_t> truth_ids;
    for (const auto& h : truth.hits) truth_ids.insert(h.doc_id);
    for (const auto& h : got.hits) hit += truth_ids.count(h.doc_id);
    total += truth.hits.size();
  }
  return static_cast<double>(hit) / static_cast<double>(total);
}

// Recording sink for structural trace checks.
class RecordingSink : public TraceSink {
 public:
  struct Rec {
    uint8_t layer;
    TraceKind kind;
    uint32_t node, parent;
    float dist;
  };
  std::vector<Rec> recs;
  void Record(uint8_t layer, TraceKind kind, uint32_t node, uint32_t parent,
              float dist) override {
    recs.push_back({layer, kind, node, parent, dist});
  }
};

// ---- recall: the M1 gate (PLAN §7: >= 0.95 @ ef=100 vs oracle) ----

TEST(Hnsw, RecallGateAt2k) {
  const Corpus corpus = MakeCorpus(2000, 64, 7);
  const Corpus queries = MakeCorpus(200, 64, 8);
  const HnswIndex hnsw = BuildHnsw(corpus);
  const BruteForceIndex oracle = BuildOracle(corpus);

  const double recall = RecallAt10(hnsw, oracle, queries, 100);
  EXPECT_GE(recall, 0.95) << "recall@10 ef=100 = " << recall;

  // ef sweep: more effort must not hurt recall (allow tiny noise).
  const double lo = RecallAt10(hnsw, oracle, queries, 16);
  const double hi = RecallAt10(hnsw, oracle, queries, 200);
  EXPECT_GE(hi, lo - 0.01) << "ef=200 " << hi << " vs ef=16 " << lo;
  EXPECT_GE(hi, 0.98);
}

TEST(Hnsw, VisitedIsSublinearOnClusteredData) {
  // Clustered like a real embedding corpus (measured mean ~443/2000 here;
  // uniform random d=64 measures ~1493/2000 — see MakeCorpus comment).
  const Corpus corpus = MakeCorpus(2000, 64, 7, /*clusters=*/20);
  const Corpus queries = MakeCorpus(20, 64, 9, /*clusters=*/20);
  const HnswIndex hnsw = BuildHnsw(corpus);
  uint64_t total = 0;
  for (size_t qi = 0; qi < queries.ids.size(); ++qi) {
    const auto r = hnsw.Search(queries.row(qi), 10, 100, nullptr);
    EXPECT_GT(r.visited, 0u);
    EXPECT_LT(r.visited, 1000u);
    total += r.visited;
  }
  EXPECT_LT(total / queries.ids.size(), 650u) << "mean visited too high";
}

// ---- determinism: the golden-trace foundation (internals.md §6) ----

TEST(Hnsw, BuildIsDeterministic) {
  const Corpus corpus = MakeCorpus(500, 32, 3);
  const HnswIndex a = BuildHnsw(corpus, 42);
  const HnswIndex b = BuildHnsw(corpus, 42);

  ASSERT_EQ(a.max_level(), b.max_level());
  ASSERT_EQ(a.entry_row(), b.entry_row());
  ASSERT_EQ(a.levels(), b.levels());
  for (int lc = 0; lc <= a.max_level(); ++lc) {
    for (uint32_t row = 0; row < corpus.ids.size(); ++row) {
      ASSERT_EQ(a.Neighbors(lc, row), b.Neighbors(lc, row))
          << "layer " << lc << " row " << row;
    }
  }

  // Different seed -> different structure (levels differ somewhere).
  const HnswIndex c = BuildHnsw(corpus, 43);
  EXPECT_NE(a.levels(), c.levels());
}

TEST(Hnsw, SearchIsDeterministicAcrossThreads) {
  const Corpus corpus = MakeCorpus(1000, 32, 5);
  const Corpus queries = MakeCorpus(20, 32, 6);
  const HnswIndex hnsw = BuildHnsw(corpus);

  std::vector<std::vector<uint64_t>> expected;
  for (size_t qi = 0; qi < queries.ids.size(); ++qi) {
    std::vector<uint64_t> ids;
    for (const auto& h : hnsw.Search(queries.row(qi), 10, 64, nullptr).hits) {
      ids.push_back(h.doc_id);
    }
    expected.push_back(std::move(ids));
  }

  std::atomic<bool> mismatch{false};
  std::vector<std::thread> threads;
  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&] {
      for (int rep = 0; rep < 50; ++rep) {
        for (size_t qi = 0; qi < queries.ids.size(); ++qi) {
          std::vector<uint64_t> ids;
          for (const auto& h :
               hnsw.Search(queries.row(qi), 10, 64, nullptr).hits) {
            ids.push_back(h.doc_id);
          }
          if (ids != expected[qi]) mismatch = true;
        }
      }
    });
  }
  for (auto& th : threads) th.join();
  EXPECT_FALSE(mismatch.load());
}

// ---- trace structure: what the inspector will replay (M1-T3/T8) ----

TEST(Hnsw, TraceIsWellFormed) {
  const Corpus corpus = MakeCorpus(1000, 32, 5);
  const Corpus queries = MakeCorpus(1, 32, 11);
  const HnswIndex hnsw = BuildHnsw(corpus);

  RecordingSink sink;
  const auto result = hnsw.Search(queries.row(0), 10, 64, &sink);
  ASSERT_FALSE(sink.recs.empty());

  // Starts with ENTRY at the top layer.
  EXPECT_EQ(sink.recs[0].kind, TraceKind::kEntry);
  EXPECT_EQ(static_cast<int>(sink.recs[0].layer), hnsw.max_level());
  EXPECT_EQ(sink.recs[0].node, hnsw.entry_row());

  // Layers never increase along the record stream (descent).
  int prev_layer = hnsw.max_level();
  for (const auto& r : sink.recs) {
    EXPECT_LE(static_cast<int>(r.layer), prev_layer);
    prev_layer = static_cast<int>(r.layer);
  }

  // RESULT records exactly match returned rows.
  std::set<uint32_t> result_rows;
  std::set<uint32_t> trace_rows;
  for (const auto& h : result.hits) result_rows.insert(h.row);
  size_t visits = 0;
  for (const auto& r : sink.recs) {
    if (r.kind == TraceKind::kResult) trace_rows.insert(r.node);
    if (r.kind == TraceKind::kVisit) ++visits;
    if (r.kind == TraceKind::kAccept || r.kind == TraceKind::kVisit) {
      EXPECT_NE(r.node, r.parent) << "visit must come from a different node";
    }
  }
  EXPECT_EQ(trace_rows, result_rows);
  // visited counter == entry(1) + VISIT records.
  EXPECT_EQ(result.visited, visits + 1);

  // No-sink search returns identical hits (tracing must not perturb).
  const auto untraced = hnsw.Search(queries.row(0), 10, 64, nullptr);
  ASSERT_EQ(untraced.hits.size(), result.hits.size());
  for (size_t i = 0; i < untraced.hits.size(); ++i) {
    EXPECT_EQ(untraced.hits[i].doc_id, result.hits[i].doc_id);
  }
}

// ---- lifecycle & edges ----

TEST(Hnsw, LifecycleAndEdgeCases) {
  HnswIndex::Params p;
  HnswIndex index(4, p);
  const float v[4] = {1, 0, 0, 0};
  EXPECT_THROW(index.Search(v, 1, 10, nullptr), std::logic_error);
  index.Add(7, v);
  index.Seal();
  EXPECT_THROW(index.Add(8, v), std::logic_error);

  const auto r = index.Search(v, 5, 10, nullptr);  // k > n
  ASSERT_EQ(r.hits.size(), 1u);
  EXPECT_EQ(r.hits[0].doc_id, 7u);
  EXPECT_NEAR(r.hits[0].score, 1.0F, 1e-6F);

  HnswIndex empty(4, p);
  empty.Seal();
  EXPECT_TRUE(empty.Search(v, 3, 10, nullptr).hits.empty());

  EXPECT_THROW(HnswIndex(4, HnswIndex::Params{1, 32, 200, 1}),
               std::invalid_argument);
}

TEST(Hnsw, DegreesRespectCaps) {
  const Corpus corpus = MakeCorpus(800, 16, 13);
  const HnswIndex hnsw = BuildHnsw(corpus);
  for (uint32_t row = 0; row < corpus.ids.size(); ++row) {
    EXPECT_LE(hnsw.Neighbors(0, row).size(), 32u);
    for (int lc = 1; lc <= hnsw.max_level(); ++lc) {
      EXPECT_LE(hnsw.Neighbors(lc, row).size(), 16u);
    }
  }
  EXPECT_GT(hnsw.EdgeCount(), corpus.ids.size());  // connected, not a forest
}

}  // namespace
}  // namespace lucent
