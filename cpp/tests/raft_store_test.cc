#include "coordinator/raft_store.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "lucent/v1/common.pb.h"
#include "raft/member_server.h"

namespace lucent {
namespace {

namespace fs = std::filesystem;
using namespace std::chrono_literals;

lucent::v1::ShardMap MakeMap(uint64_t epoch, const std::string& primary) {
  lucent::v1::ShardMap m;
  m.set_epoch(epoch);
  auto* s = m.add_shards();
  s->set_shard_id(0);
  s->set_primary_node(primary);
  return m;
}

// The coordinator's RaftStore against a real 3-member quorum: Propose commits a
// new shard map via the leader (CAS on epoch), and Watch streams every
// committed map — the M5-T3 client contract.
class RaftStoreTest : public testing::Test {
 protected:
  static constexpr int kBase = 7310;

  void SetUp() override {
    for (int j = 0; j < 3; ++j)
      members_["member-" + std::to_string(j)] =
          "127.0.0.1:" + std::to_string(kBase + j);
    for (int j = 0; j < 3; ++j) {
      const std::string id = "member-" + std::to_string(j);
      const std::string dir = testing::TempDir() + "lucent_rs_" + std::to_string(j);
      fs::remove_all(dir);
      dirs_.push_back(dir);
      auto m = std::make_unique<raft::MemberServer>(id, members_, dir,
                                                    static_cast<uint64_t>(j) + 1);
      ASSERT_TRUE(m->Start(members_[id]));
      servers_.push_back(std::move(m));
    }
  }

  void TearDown() override {
    for (auto& s : servers_)
      if (s) s->Shutdown();
    for (const std::string& d : dirs_) fs::remove_all(d);
  }

  std::map<std::string, std::string> members_;
  std::vector<std::unique_ptr<raft::MemberServer>> servers_;
  std::vector<std::string> dirs_;
};

TEST_F(RaftStoreTest, ProposeCommitsAndWatchStreams) {
  RaftStore store(members_);
  std::mutex mu;
  std::vector<uint64_t> watched;
  store.StartWatch(1, [&](const lucent::v1::ShardMap& m) {
    std::lock_guard<std::mutex> lk(mu);
    watched.push_back(m.epoch());
  });

  // Propose the boot-seed map (epoch 1, CAS from the empty epoch 0), retrying
  // until a leader exists.
  uint64_t committed = 0;
  for (int i = 0; i < 120 && committed == 0; ++i) {
    committed = store.Propose(MakeMap(1, "shard-0a"), 0);
    if (committed == 0) std::this_thread::sleep_for(50ms);
  }
  ASSERT_EQ(committed, 1u) << "no leader / commit within budget";

  // A second CAS chains off the committed epoch.
  EXPECT_EQ(store.Propose(MakeMap(2, "shard-0b"), 1u), 2u);
  // A stale expected_epoch loses the CAS → committed as a no-op; the state
  // machine stays at the current epoch (2), so that's what comes back.
  EXPECT_EQ(store.Propose(MakeMap(9, "shard-0a"), 0u), 2u);

  // Watch delivered both real commits (the no-op CAS produced no new epoch).
  bool got = false;
  for (int i = 0; i < 60 && !got; ++i) {
    {
      std::lock_guard<std::mutex> lk(mu);
      got = watched.size() >= 2 && watched[0] == 1u && watched[1] == 2u;
    }
    if (!got) std::this_thread::sleep_for(50ms);
  }
  EXPECT_TRUE(got) << "watch did not stream committed maps 1,2";

  store.Stop();
}

}  // namespace
}  // namespace lucent
