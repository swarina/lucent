#include "index/brute_force.h"

#include <gtest/gtest.h>

#include <nlohmann/json.hpp>

#include <cmath>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace lucent {
namespace {

using nlohmann::json;

std::string FixtureDir() {
  return std::string(LUCENT_REPO_ROOT) + "/testdata/bf_fixture";
}

template <typename T>
std::vector<T> ReadRaw(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) throw std::runtime_error("missing fixture file: " + path);
  std::ostringstream ss;
  ss << in.rdbuf();
  const std::string bytes = std::move(ss).str();
  std::vector<T> out(bytes.size() / sizeof(T));
  std::memcpy(out.data(), bytes.data(), out.size() * sizeof(T));
  return out;
}

// Cross-validation against an independent implementation: the fixture's
// expected top-k was computed by NumPy (py/tools/make_bf_fixture.py).
TEST(BruteForceIndex, MatchesNumpyFixtureExactly) {
  const json fx = json::parse(std::ifstream(FixtureDir() + "/expected.json"));
  const int dim = fx.at("dim").get<int>();
  const size_t n = fx.at("n").get<size_t>();
  const uint32_t k = fx.at("k").get<uint32_t>();

  const auto ids = ReadRaw<uint64_t>(FixtureDir() + "/ids.u64");
  const auto vecs = ReadRaw<float>(FixtureDir() + "/vectors.f32");
  const auto queries = ReadRaw<float>(FixtureDir() + "/queries.f32");
  ASSERT_EQ(ids.size(), n);
  ASSERT_EQ(vecs.size(), n * static_cast<size_t>(dim));

  BruteForceIndex index(dim);
  for (size_t i = 0; i < n; ++i) {
    index.Add(ids[i], vecs.data() + i * static_cast<size_t>(dim));
  }
  index.Seal();

  const auto& expected = fx.at("expected");
  const size_t nq = queries.size() / static_cast<size_t>(dim);
  ASSERT_EQ(nq, expected.size());

  for (size_t qi = 0; qi < nq; ++qi) {
    const auto result =
        index.Search(queries.data() + qi * static_cast<size_t>(dim), k,
                     /*ef=*/0, /*sink=*/nullptr);
    ASSERT_EQ(result.hits.size(), k) << "query " << qi;
    EXPECT_EQ(result.visited, n);
    for (uint32_t i = 0; i < k; ++i) {
      const uint64_t want_id = expected[qi][i][0].get<uint64_t>();
      const float want_score = expected[qi][i][1].get<float>();
      EXPECT_EQ(result.hits[i].doc_id, want_id)
          << "query " << qi << " rank " << i;
      EXPECT_NEAR(result.hits[i].score, want_score, 1e-5F);
      // row must map back to the same doc_id (row/id confusion guard).
      EXPECT_EQ(ids[result.hits[i].row], result.hits[i].doc_id);
    }
  }
}

TEST(BruteForceIndex, LifecycleContracts) {
  BruteForceIndex index(4);
  const float v[4] = {1, 0, 0, 0};
  index.Add(7, v);
  EXPECT_THROW(index.Search(v, 1, 0, nullptr), std::logic_error);  // pre-seal
  index.Seal();
  EXPECT_THROW(index.Add(8, v), std::logic_error);  // post-seal
  const auto r = index.Search(v, 5, 0, nullptr);    // k > n is fine
  ASSERT_EQ(r.hits.size(), 1u);
  EXPECT_EQ(r.hits[0].doc_id, 7u);
  EXPECT_NEAR(r.hits[0].score, 1.0F, 1e-6F);
}

TEST(BruteForceIndex, EmptyAndZeroK) {
  BruteForceIndex index(4);
  index.Seal();
  const float v[4] = {1, 0, 0, 0};
  EXPECT_TRUE(index.Search(v, 3, 0, nullptr).hits.empty());
  BruteForceIndex index2(4);
  index2.Add(1, v);
  index2.Seal();
  EXPECT_TRUE(index2.Search(v, 0, 0, nullptr).hits.empty());
}

}  // namespace
}  // namespace lucent
