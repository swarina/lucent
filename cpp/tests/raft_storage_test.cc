#include "raft/raft_storage.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>

#include "raft/raft_node.h"

namespace lucent::raft {
namespace {

namespace fs = std::filesystem;

LogEntry Entry(uint64_t index, uint64_t term, uint64_t epoch) {
  LogEntry e;
  e.index = index;
  e.term = term;
  e.mutation.set_expected_epoch(epoch - 1);
  e.mutation.mutable_proposed()->set_epoch(epoch);
  return e;
}

class RaftStorageTest : public testing::Test {
 protected:
  void SetUp() override {
    dir_ = testing::TempDir() + "lucent_raft_store";
    fs::remove_all(dir_);
  }
  void TearDown() override { fs::remove_all(dir_); }
  std::string dir_;
};

TEST_F(RaftStorageTest, FreshDirLoadsEmpty) {
  RaftStorage s(dir_);
  Persistent p = s.Load();
  EXPECT_EQ(p.current_term, 0u);
  EXPECT_TRUE(p.voted_for.empty());
  EXPECT_TRUE(p.log.empty());
}

TEST_F(RaftStorageTest, RoundTripsTermVoteAndLog) {
  Persistent p;
  p.current_term = 7;
  p.voted_for = "member-2";
  p.log = {Entry(1, 3, 1), Entry(2, 3, 2), Entry(3, 7, 3)};
  RaftStorage(dir_).Save(p);

  Persistent got = RaftStorage(dir_).Load();  // fresh store → reads from disk
  EXPECT_EQ(got.current_term, 7u);
  EXPECT_EQ(got.voted_for, "member-2");
  ASSERT_EQ(got.log.size(), 3u);
  EXPECT_EQ(got.log[2].index, 3u);
  EXPECT_EQ(got.log[2].term, 7u);
  EXPECT_EQ(got.log[2].mutation.proposed().epoch(), 3u);
  EXPECT_EQ(got.log[2].mutation.expected_epoch(), 2u);
}

TEST_F(RaftStorageTest, AppendsIncrementallyAcrossSaves) {
  RaftStorage s(dir_);
  Persistent p;
  p.current_term = 1;
  p.log = {Entry(1, 1, 1)};
  s.Save(p);
  p.log.push_back(Entry(2, 1, 2));  // grow by one
  s.Save(p);
  p.log.push_back(Entry(3, 1, 3));
  s.Save(p);

  Persistent got = RaftStorage(dir_).Load();
  ASSERT_EQ(got.log.size(), 3u);
  for (uint64_t i = 0; i < 3; ++i) EXPECT_EQ(got.log[i].index, i + 1);
}

TEST_F(RaftStorageTest, ConflictTruncationRewritesTail) {
  RaftStorage s(dir_);
  Persistent p;
  p.current_term = 1;
  p.log = {Entry(1, 1, 1), Entry(2, 1, 2), Entry(3, 1, 3)};
  s.Save(p);
  // A new leader overwrites index 2..3 with higher-term entries (Raft §5.3).
  p.current_term = 5;
  p.log = {Entry(1, 1, 1), Entry(2, 5, 9)};
  s.Save(p);

  Persistent got = RaftStorage(dir_).Load();
  ASSERT_EQ(got.log.size(), 2u);
  EXPECT_EQ(got.log[1].term, 5u);
  EXPECT_EQ(got.log[1].mutation.proposed().epoch(), 9u);
}

TEST_F(RaftStorageTest, TornLogTailTruncatesOnLoad) {
  RaftStorage s(dir_);
  Persistent p;
  p.current_term = 2;
  p.log = {Entry(1, 1, 1), Entry(2, 2, 2)};
  s.Save(p);

  // Simulate a crash mid-append: chop the last few bytes off log.bin.
  const std::string path = dir_ + "/log.bin";
  const auto sz = fs::file_size(path);
  fs::resize_file(path, sz - 5);

  Persistent got = RaftStorage(dir_).Load();
  EXPECT_EQ(got.current_term, 2u);        // state.bin intact (atomic)
  ASSERT_EQ(got.log.size(), 1u);          // torn 2nd entry dropped
  EXPECT_EQ(got.log[0].index, 1u);
}

TEST_F(RaftStorageTest, CorruptStateFallsBackToFresh) {
  RaftStorage s(dir_);
  Persistent p;
  p.current_term = 4;
  p.voted_for = "member-1";
  s.Save(p);

  // Flip a byte in the persisted term → checksum fails → treated as no state.
  const std::string path = dir_ + "/state.bin";
  std::fstream f(path, std::ios::binary | std::ios::in | std::ios::out);
  f.seekp(7);
  f.put('\xFF');
  f.close();

  Persistent got = RaftStorage(dir_).Load();
  EXPECT_EQ(got.current_term, 0u);
  EXPECT_TRUE(got.voted_for.empty());
}

}  // namespace
}  // namespace lucent::raft
