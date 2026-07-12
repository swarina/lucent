#include "index/hnsw_io.h"

#include <gtest/gtest.h>

#include <cmath>
#include <filesystem>
#include <fstream>
#include <random>
#include <sstream>
#include <vector>

namespace lucent {
namespace {

namespace fs = std::filesystem;

struct TestData {
  int dim = 32;
  std::vector<uint64_t> ids;
  std::vector<float> vecs;
};

TestData MakeData(size_t n, uint64_t seed) {
  TestData d;
  std::mt19937_64 rng(seed);
  std::normal_distribution<float> normal(0.0F, 1.0F);
  d.vecs.resize(n * static_cast<size_t>(d.dim));
  for (size_t i = 0; i < n; ++i) {
    d.ids.push_back(5000 + 7 * i);
    float* v = d.vecs.data() + i * static_cast<size_t>(d.dim);
    float norm = 0.0F;
    for (int k = 0; k < d.dim; ++k) {
      v[k] = normal(rng);
      norm += v[k] * v[k];
    }
    norm = std::sqrt(norm);
    for (int k = 0; k < d.dim; ++k) v[k] /= norm;
  }
  return d;
}

HnswIndex Build(const TestData& d) {
  HnswIndex index(d.dim, HnswIndex::Params{});
  for (size_t i = 0; i < d.ids.size(); ++i) {
    index.Add(d.ids[i], d.vecs.data() + i * static_cast<size_t>(d.dim));
  }
  index.Seal();
  return index;
}

std::string FileBytes(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  std::ostringstream ss;
  ss << in.rdbuf();
  return std::move(ss).str();
}

class HnswIoTest : public testing::Test {
 protected:
  void SetUp() override {
    path_ = testing::TempDir() + "lucent_graph_" +
            testing::UnitTest::GetInstance()->current_test_info()->name() +
            ".bin";
    fs::remove(path_);
  }
  void TearDown() override { fs::remove(path_); }
  std::string path_;
};

TEST_F(HnswIoTest, RoundTripPreservesStructureAndResults) {
  const TestData data = MakeData(600, 21);
  const HnswIndex original = Build(data);
  SaveHnswGraph(original, path_);

  auto loaded = LoadHnswGraph(path_, data.dim, data.ids, data.vecs);
  ASSERT_TRUE(loaded->Sealed());
  EXPECT_EQ(loaded->Size(), original.Size());
  EXPECT_EQ(loaded->max_level(), original.max_level());
  EXPECT_EQ(loaded->entry_row(), original.entry_row());
  EXPECT_EQ(loaded->levels(), original.levels());
  EXPECT_EQ(loaded->EdgeCount(), original.EdgeCount());
  for (int lc = 0; lc <= original.max_level(); ++lc) {
    for (uint32_t row = 0; row < original.Size(); ++row) {
      ASSERT_EQ(loaded->Neighbors(lc, row), original.Neighbors(lc, row))
          << "layer " << lc << " row " << row;
    }
  }

  // Same searches, same answers.
  const TestData queries = MakeData(20, 22);
  for (size_t qi = 0; qi < queries.ids.size(); ++qi) {
    const float* q = queries.vecs.data() + qi * static_cast<size_t>(data.dim);
    const auto a = original.Search(q, 10, 64, nullptr);
    const auto b = loaded->Search(q, 10, 64, nullptr);
    ASSERT_EQ(a.hits.size(), b.hits.size());
    for (size_t i = 0; i < a.hits.size(); ++i) {
      EXPECT_EQ(a.hits[i].doc_id, b.hits[i].doc_id);
      EXPECT_EQ(a.hits[i].score, b.hits[i].score);
    }
    EXPECT_EQ(a.visited, b.visited);
  }
}

TEST_F(HnswIoTest, SaveIsByteDeterministic) {
  const TestData data = MakeData(400, 33);
  const HnswIndex a = Build(data);
  const HnswIndex b = Build(data);  // same seed + order -> identical graph
  SaveHnswGraph(a, path_);
  const std::string bytes_a = FileBytes(path_);
  SaveHnswGraph(b, path_);
  EXPECT_EQ(bytes_a, FileBytes(path_));  // the golden-trace foundation

  // Save -> load -> save is also identical.
  auto loaded = LoadHnswGraph(path_, data.dim, data.ids, data.vecs);
  SaveHnswGraph(*loaded, path_);
  EXPECT_EQ(bytes_a, FileBytes(path_));
}

TEST_F(HnswIoTest, DetectsCorruption) {
  const TestData data = MakeData(200, 44);
  SaveHnswGraph(Build(data), path_);

  std::string bytes = FileBytes(path_);
  bytes[bytes.size() / 2] = static_cast<char>(bytes[bytes.size() / 2] ^ 0x1);
  std::ofstream(path_, std::ios::binary | std::ios::trunc)
      .write(bytes.data(), static_cast<std::streamsize>(bytes.size()));

  EXPECT_THROW(
      { LoadHnswGraph(path_, data.dim, data.ids, data.vecs); },
      std::runtime_error);
}

TEST_F(HnswIoTest, RejectsMismatchedSidecars) {
  const TestData data = MakeData(200, 44);
  SaveHnswGraph(Build(data), path_);

  auto short_ids = data.ids;
  short_ids.pop_back();
  auto short_vecs = data.vecs;
  short_vecs.resize(short_vecs.size() - static_cast<size_t>(data.dim));
  EXPECT_THROW({ LoadHnswGraph(path_, data.dim, short_ids, short_vecs); },
               std::runtime_error);
  EXPECT_THROW({ LoadHnswGraph(path_, 64, data.ids, data.vecs); },
               std::runtime_error);
}

TEST_F(HnswIoTest, RefusesUnsealedAndMissingFile) {
  HnswIndex unsealed(4, HnswIndex::Params{});
  const float v[4] = {1, 0, 0, 0};
  unsealed.Add(1, v);
  EXPECT_THROW(SaveHnswGraph(unsealed, path_), std::logic_error);
  EXPECT_THROW(
      { LoadHnswGraph("/nonexistent/graph.bin", 4, {}, {}); },
      std::runtime_error);
}

}  // namespace
}  // namespace lucent
