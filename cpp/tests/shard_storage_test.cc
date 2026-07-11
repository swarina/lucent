#include "index/shard_storage.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace lucent {
namespace {

namespace fs = std::filesystem;

class ShardStorageTest : public testing::Test {
 protected:
  void SetUp() override {
    dir_ = testing::TempDir() + "lucent_shard_storage_" +
           testing::UnitTest::GetInstance()->current_test_info()->name();
    fs::remove_all(dir_);
  }
  void TearDown() override { fs::remove_all(dir_); }

  static LoadedShard MakeSample() {
    LoadedShard s;
    s.manifest.shard_id = 2;
    s.manifest.replica = "a";
    s.manifest.node_id = "shard-2a";
    s.manifest.n = 3;
    s.manifest.dim = 4;
    s.manifest.index_type = "bruteforce";
    s.manifest.seed = 42;
    s.manifest.corpus_hash = "deadbeefdeadbeef";
    s.ids = {10, 20, 30};
    s.vectors = {1, 0, 0, 0,  0, 1, 0, 0,  0, 0, 1, 0};
    s.docs = {{10, "Alpha", "first snippet", "cs.LG"},
              {20, "Beta", "second — with unicode ✓", "math.CO"},
              {30, "Gamma", "third", "q-bio"}};
    return s;
  }

  std::string dir_;
};

TEST_F(ShardStorageTest, RoundTrips) {
  const LoadedShard in = MakeSample();
  SaveShardDir(dir_, in.manifest, in.ids, in.vectors, in.docs);

  const LoadedShard out = LoadShardDir(dir_);
  EXPECT_EQ(out.manifest.shard_id, 2);
  EXPECT_EQ(out.manifest.node_id, "shard-2a");
  EXPECT_EQ(out.manifest.n, 3u);
  EXPECT_EQ(out.manifest.dim, 4);
  EXPECT_EQ(out.manifest.index_type, "bruteforce");
  EXPECT_EQ(out.manifest.seed, 42u);
  EXPECT_EQ(out.ids, in.ids);
  EXPECT_EQ(out.vectors, in.vectors);
  ASSERT_EQ(out.docs.size(), 3u);
  EXPECT_EQ(out.docs[1].title, "Beta");
  EXPECT_EQ(out.docs[1].snippet, "second — with unicode ✓");
  EXPECT_EQ(out.docs[2].category, "q-bio");
}

TEST_F(ShardStorageTest, DetectsCorruptedVectors) {
  const LoadedShard in = MakeSample();
  SaveShardDir(dir_, in.manifest, in.ids, in.vectors, in.docs);

  // Flip one byte in vectors.f32 (same length -> only the checksum catches it).
  const std::string path = dir_ + "/vectors.f32";
  std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
  f.seekp(5);
  char b;
  f.seekg(5);
  f.get(b);
  f.seekp(5);
  f.put(static_cast<char>(b ^ 0x1));
  f.close();

  try {
    LoadShardDir(dir_);
    FAIL() << "expected checksum mismatch";
  } catch (const std::runtime_error& e) {
    EXPECT_NE(std::string(e.what()).find("vectors.f32"), std::string::npos)
        << e.what();
  }
}

TEST_F(ShardStorageTest, DetectsMissingFile) {
  const LoadedShard in = MakeSample();
  SaveShardDir(dir_, in.manifest, in.ids, in.vectors, in.docs);
  fs::remove(dir_ + "/docs.jsonl.zst");
  EXPECT_THROW(LoadShardDir(dir_), std::runtime_error);
}

TEST_F(ShardStorageTest, RejectsInconsistentInputs) {
  LoadedShard in = MakeSample();
  in.ids.pop_back();  // n says 3, ids has 2
  EXPECT_THROW(SaveShardDir(dir_, in.manifest, in.ids, in.vectors, in.docs),
               std::invalid_argument);
}

}  // namespace
}  // namespace lucent
