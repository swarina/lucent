#include "coordinator/coordinator_server.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "lucent/v1/coordinator.grpc.pb.h"
#include "raft/member_server.h"

namespace lucent {
namespace {

namespace fs = std::filesystem;
namespace pb = lucent::v1;
using namespace std::chrono_literals;

// One shard with a primary and a backup — enough to fail over. Addresses are
// never dialed (replicas=1 → no HealthWatcher; the test drives health itself).
pb::ShardMap BootMap() {
  pb::ShardMap m;
  m.set_epoch(1);
  auto* e = m.add_shards();
  e->set_shard_id(0);
  e->set_primary_node("shard-0a");
  e->set_primary_addr("127.0.0.1:65001");
  e->set_backup_node("shard-0b");
  e->set_backup_addr("127.0.0.1:65002");
  e->set_primary_state(pb::NODE_STATE_SERVING);
  e->set_backup_state(pb::NODE_STATE_SERVING);
  return m;
}

// A coordinator in Raft mode (M5-T3): the shard map is the quorum's state
// machine — boot-seeded via consensus, and every failover proposed through the
// leader. Three real lucent-member servers back it.
class CoordinatorRaftTest : public testing::Test {
 protected:
  static constexpr int kBase = 7320;

  void SetUp() override {
    for (int j = 0; j < 3; ++j)
      members_["member-" + std::to_string(j)] =
          "127.0.0.1:" + std::to_string(kBase + j);
    for (int j = 0; j < 3; ++j) {
      const std::string id = "member-" + std::to_string(j);
      const std::string dir = testing::TempDir() + "lucent_craft_" + std::to_string(j);
      fs::remove_all(dir);
      dirs_.push_back(dir);
      auto m = std::make_unique<raft::MemberServer>(id, members_, dir,
                                                    static_cast<uint64_t>(j) + 1);
      ASSERT_TRUE(m->Start(members_[id]));
      quorum_.push_back(std::move(m));
    }
    config_ = Config::Load(std::string(LUCENT_REPO_ROOT) + "/cluster.yaml");
    config_.cluster.replicas = 1;  // suppress the HealthWatcher; drive via the seam
  }

  void TearDown() override {
    coord_.reset();  // stops Watch + seed threads before the members go
    for (auto& s : quorum_)
      if (s) s->Shutdown();
    for (const std::string& d : dirs_) fs::remove_all(d);
  }

  // True once ANY member has committed+applied at least `want` (the quorum's
  // state machine reached that epoch).
  bool WaitQuorumEpoch(uint64_t want, uint64_t budget_ms = 8000) {
    for (uint64_t t = 0; t < budget_ms; t += 50) {
      for (auto& s : quorum_)
        if (s && s->applied_epoch() >= want) return true;
      std::this_thread::sleep_for(50ms);
    }
    return false;
  }

  bool WaitCoordEpoch(uint64_t want, uint64_t budget_ms = 5000) {
    for (uint64_t t = 0; t < budget_ms; t += 50) {
      if (State().shard_map().epoch() >= want) return true;
      std::this_thread::sleep_for(50ms);
    }
    return false;
  }

  pb::ClusterState State() {
    pb::ClusterStateRequest req;
    pb::ClusterState cs;
    coord_->GetClusterState(nullptr, &req, &cs);
    return cs;
  }

  raft::MemberServer* Leader() {
    for (auto& s : quorum_)
      if (s && s->role() == raft::Role::kLeader) return s.get();
    return nullptr;
  }

  Config config_{};
  std::map<std::string, std::string> members_;
  std::vector<std::unique_ptr<raft::MemberServer>> quorum_;
  std::vector<std::string> dirs_;
  std::unique_ptr<CoordinatorServer> coord_;
};

TEST_F(CoordinatorRaftTest, BootSeedsAndFailsOverThroughRaft) {
  coord_ = std::make_unique<CoordinatorServer>(
      config_, BootMap(), "127.0.0.1:65000" /*unused embed*/, nullptr, members_);

  // The config map is committed to the quorum as epoch 1 (boot-seed).
  ASSERT_TRUE(WaitQuorumEpoch(1)) << "boot-seed never committed";
  ASSERT_TRUE(WaitCoordEpoch(1)) << "coordinator never applied the committed map";

  // A failover is proposed THROUGH the quorum (not mutated locally): the
  // committed map bumps to epoch 2 and swaps in the backup.
  coord_->SetHealthForTest("shard-0a", pb::HEALTH_DOWN);
  ASSERT_TRUE(WaitQuorumEpoch(2)) << "failover not committed by the quorum";
  ASSERT_TRUE(WaitCoordEpoch(2));

  const pb::ClusterState cs = State();
  EXPECT_EQ(cs.shard_map().epoch(), 2u);
  ASSERT_EQ(cs.shard_map().shards_size(), 1);
  EXPECT_EQ(cs.shard_map().shards(0).primary_node(), "shard-0b");
  EXPECT_EQ(cs.shard_map().shards(0).backup_node(), "shard-0a");
}

TEST_F(CoordinatorRaftTest, ShardMapWritesContinueAcrossLeaderKill) {
  // The M5 gate: after a Raft leader dies, shard-map writes still commit.
  coord_ = std::make_unique<CoordinatorServer>(
      config_, BootMap(), "127.0.0.1:65000", nullptr, members_);
  ASSERT_TRUE(WaitQuorumEpoch(1));
  ASSERT_TRUE(WaitCoordEpoch(1));

  // Kill the current Raft leader; the other two re-elect.
  raft::MemberServer* ldr = Leader();
  ASSERT_NE(ldr, nullptr);
  for (auto& s : quorum_)
    if (s.get() == ldr) s.reset();  // stop + free the port; drop from the quorum

  // Wait for a new leader, then propose a failover — it must still commit.
  for (int i = 0; i < 100 && Leader() == nullptr; ++i) std::this_thread::sleep_for(50ms);
  ASSERT_NE(Leader(), nullptr) << "no re-election after leader kill";

  coord_->SetHealthForTest("shard-0a", pb::HEALTH_DOWN);
  ASSERT_TRUE(WaitCoordEpoch(2, 8000)) << "write did not commit after leader change";
  EXPECT_EQ(State().shard_map().shards(0).primary_node(), "shard-0b");
}

}  // namespace
}  // namespace lucent
