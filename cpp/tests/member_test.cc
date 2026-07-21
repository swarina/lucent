#include "raft/member_server.h"

#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "lucent/v1/raft.grpc.pb.h"

namespace lucent::raft {
namespace {

namespace fs = std::filesystem;
namespace pb = lucent::v1;
using namespace std::chrono_literals;

// Three real MemberServers over gRPC on fixed localhost ports — the M5-T2
// integration: a leader elects itself, MembershipService.Propose commits
// through it, and killing the leader triggers a re-election that keeps serving.
class MemberClusterTest : public testing::Test {
 protected:
  static constexpr int kBase = 7300;

  void SetUp() override {
    for (int j = 0; j < 3; ++j)
      members_["member-" + std::to_string(j)] =
          "127.0.0.1:" + std::to_string(kBase + j);
    for (int j = 0; j < 3; ++j) Boot(j);
  }

  void TearDown() override {
    for (auto& s : servers_)
      if (s) s->Shutdown();
    for (const std::string& d : dirs_) fs::remove_all(d);
  }

  void Boot(int j) {
    const std::string id = "member-" + std::to_string(j);
    const std::string dir = testing::TempDir() + "lucent_member_" + std::to_string(j);
    fs::remove_all(dir);
    dirs_.push_back(dir);
    auto m = std::make_unique<MemberServer>(id, members_, dir,
                                            static_cast<uint64_t>(j) + 1);
    ASSERT_TRUE(m->Start(members_[id]));
    if (static_cast<int>(servers_.size()) > j) servers_[static_cast<size_t>(j)] = std::move(m);
    else servers_.push_back(std::move(m));
  }

  MemberServer* WaitLeader(uint64_t budget_ms = 8000) {
    for (uint64_t t = 0; t < budget_ms; t += 50) {
      for (auto& s : servers_)
        if (s && s->role() == Role::kLeader) return s.get();
      std::this_thread::sleep_for(50ms);
    }
    return nullptr;
  }

  std::unique_ptr<pb::MembershipService::Stub> Stub(const std::string& id) {
    return pb::MembershipService::NewStub(grpc::CreateChannel(
        members_[id], grpc::InsecureChannelCredentials()));
  }

  // Propose one epoch bump, following a NOT_LEADER redirect once. Returns the
  // committed epoch, or 0 on failure.
  uint64_t ProposeBump(MemberServer* leader) {
    const uint64_t base = leader->log_head_epoch();  // chain off the log, not applied
    pb::ShardMapMutation mut;
    mut.set_expected_epoch(base);
    mut.mutable_proposed()->set_epoch(base + 1);
    std::string target = leader->id();
    for (int attempt = 0; attempt < 3; ++attempt) {
      grpc::ClientContext ctx;
      ctx.set_deadline(std::chrono::system_clock::now() + 5s);
      pb::ProposeResponse resp;
      if (!Stub(target)->Propose(&ctx, mut, &resp).ok()) return 0;
      if (resp.committed()) return resp.epoch();
      if (resp.leader_hint().empty()) return 0;
      target = resp.leader_hint();  // redirect to the current leader
    }
    return 0;
  }

  std::map<std::string, std::string> members_;
  std::vector<std::unique_ptr<MemberServer>> servers_;
  std::vector<std::string> dirs_;
};

TEST_F(MemberClusterTest, ElectsLeaderAndCommitsOverGrpc) {
  MemberServer* ldr = WaitLeader();
  ASSERT_NE(ldr, nullptr) << "no leader elected over gRPC";

  EXPECT_EQ(ProposeBump(ldr), 1u);
  EXPECT_EQ(ProposeBump(ldr), 2u);

  // Every member's replicated state machine converges to the committed epoch.
  bool converged = false;
  for (int i = 0; i < 40 && !converged; ++i) {
    converged = true;
    for (auto& s : servers_)
      if (s->applied_epoch() != 2u) converged = false;
    if (!converged) std::this_thread::sleep_for(50ms);
  }
  EXPECT_TRUE(converged) << "followers did not converge to epoch 2";
}

TEST_F(MemberClusterTest, LeaderFailoverKeepsCommitting) {
  MemberServer* ldr = WaitLeader();
  ASSERT_NE(ldr, nullptr);
  ASSERT_EQ(ProposeBump(ldr), 1u);

  // Kill the leader; the remaining two must elect a new one and keep serving.
  size_t dead = servers_.size();
  for (size_t j = 0; j < servers_.size(); ++j)
    if (servers_[j].get() == ldr) dead = j;
  ASSERT_LT(dead, servers_.size());
  servers_[dead]->Shutdown();
  servers_[dead].reset();

  MemberServer* next = WaitLeader();
  ASSERT_NE(next, nullptr) << "no re-election after the leader died";
  EXPECT_NE(next, ldr);
  // Writes continue on the new leader (epoch picks up where it left off).
  EXPECT_EQ(ProposeBump(next), 2u);
}

TEST_F(MemberClusterTest, WatchStreamsCommittedMaps) {
  MemberServer* ldr = WaitLeader();
  ASSERT_NE(ldr, nullptr);

  grpc::ClientContext ctx;
  pb::WatchRequest req;
  req.set_from_epoch(1);
  auto reader = Stub(ldr->id())->Watch(&ctx, req);

  std::vector<uint64_t> seen;
  std::thread consumer([&] {
    pb::ShardMap map;
    while (reader->Read(&map)) {
      seen.push_back(map.epoch());
      if (seen.size() >= 2) break;
    }
  });

  EXPECT_EQ(ProposeBump(ldr), 1u);
  std::this_thread::sleep_for(200ms);
  EXPECT_EQ(ProposeBump(ldr), 2u);

  for (int i = 0; i < 40 && seen.size() < 2; ++i) std::this_thread::sleep_for(50ms);
  ctx.TryCancel();
  consumer.join();

  ASSERT_GE(seen.size(), 2u) << "watch did not stream committed maps";
  EXPECT_EQ(seen[0], 1u);
  EXPECT_EQ(seen[1], 2u);
}

}  // namespace
}  // namespace lucent::raft
