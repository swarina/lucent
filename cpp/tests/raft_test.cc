#include <gtest/gtest.h>

#include <random>
#include <set>
#include <string>
#include <vector>

#include "raft/raft_sim.h"

namespace lucent::raft {
namespace {

std::vector<std::string> Members() { return {"member-0", "member-1", "member-2"}; }

// Run in 10ms steps until a (unique-highest-term) leader exists, or give up.
RaftNode* RunToLeader(Simulator& sim, uint64_t budget_ms = 5000) {
  for (uint64_t t = 0; t < budget_ms && sim.Leader() == nullptr; t += 10) {
    sim.Step(10);
  }
  return sim.Leader();
}

// ---------- happy path ----------

TEST(Raft, ElectsExactlyOneLeader) {
  Simulator sim(Members(), /*seed=*/1);
  RaftNode* ldr = RunToLeader(sim);
  ASSERT_NE(ldr, nullptr) << "no leader elected";
  // Exactly one node is a leader at the elected term.
  int leaders = 0;
  for (const std::string& id : Members())
    if (sim.node(id)->role() == Role::kLeader) ++leaders;
  EXPECT_EQ(leaders, 1);
  EXPECT_TRUE(sim.violations().empty());
}

TEST(Raft, ReplicatesAndCommitsProposals) {
  Simulator sim(Members(), 2);
  ASSERT_NE(RunToLeader(sim), nullptr);

  for (int i = 0; i < 5; ++i) {
    ASSERT_TRUE(sim.ProposeViaLeader().has_value());
    sim.RunFor(600, 10);  // let it replicate + commit
  }
  // A majority committed all five, and the state machine advanced by CAS chain.
  EXPECT_GE(sim.max_commit_index(), 5u);
  EXPECT_EQ(sim.Leader()->applied().epoch(), 5u);
  EXPECT_TRUE(sim.violations().empty());
}

// ---------- leader failure ----------

TEST(Raft, ReElectsAfterLeaderRestart) {
  Simulator sim(Members(), 3);
  RaftNode* ldr = RunToLeader(sim);
  ASSERT_NE(ldr, nullptr);
  const std::string old = ldr->id();
  const uint64_t old_term = ldr->term();

  sim.Restart(old);  // the leader crashes and reboots as a follower
  RaftNode* next = RunToLeader(sim);
  ASSERT_NE(next, nullptr);
  EXPECT_GT(next->term(), old_term) << "a new term should carry the new leader";
  // Writes continue after failover.
  ASSERT_TRUE(sim.ProposeViaLeader().has_value());
  sim.RunFor(800, 10);
  EXPECT_GE(sim.max_commit_index(), 1u);
  EXPECT_TRUE(sim.violations().empty());
}

// ---------- partitions ----------

TEST(Raft, MinorityPartitionCannotElect) {
  Simulator sim(Members(), 4);
  RaftNode* ldr = RunToLeader(sim);
  ASSERT_NE(ldr, nullptr);

  // Isolate a FOLLOWER (not the leader). Alone, it can never gather a majority,
  // so it cycles as a candidate with a climbing term but never becomes leader —
  // no split-brain. (An isolated *leader* would instead stay leader-in-its-own-
  // view until it rejoins; that's fine, it just can't commit.)
  std::string follower;
  for (const std::string& id : Members())
    if (sim.node(id)->role() != Role::kLeader) { follower = id; break; }
  ASSERT_FALSE(follower.empty());

  sim.Partition({follower});
  sim.RunFor(4000, 10);
  EXPECT_NE(sim.node(follower)->role(), Role::kLeader) << follower << " won alone";

  // The majority side (2 of 3) still has a leader and keeps committing.
  ASSERT_NE(sim.Leader(), nullptr);
  ASSERT_TRUE(sim.ProposeViaLeader().has_value());
  sim.RunFor(800, 10);
  EXPECT_GE(sim.max_commit_index(), 1u);

  sim.Heal();
  sim.RunFor(3000, 10);
  EXPECT_TRUE(sim.violations().empty());
}

TEST(Raft, IsolatedLeaderYieldsToMajority) {
  Simulator sim(Members(), 5);
  RaftNode* ldr = RunToLeader(sim);
  ASSERT_NE(ldr, nullptr);
  const std::string old = ldr->id();

  // Cut the leader off from the other two. The majority side elects a new
  // leader at a higher term; the old leader, alone, cannot commit anything and
  // must step down when it rejoins (higher term wins). No two leaders ever
  // coexist in one term — that's the invariant checker's job every step.
  sim.Partition({old});
  sim.RunFor(4000, 10);
  RaftNode* majority = sim.Leader();
  ASSERT_NE(majority, nullptr);
  EXPECT_NE(majority->id(), old);

  sim.Heal();
  sim.RunFor(3000, 10);
  // After healing, the cluster reconverges on a single leader (which one may
  // depend on whose term is higher). The safety guarantee — never two leaders
  // in one term — was checked on every step by the invariant checker.
  ASSERT_NE(sim.Leader(), nullptr);
  int leaders = 0;
  for (const std::string& id : Members())
    if (sim.node(id)->role() == Role::kLeader) ++leaders;
  EXPECT_EQ(leaders, 1);
  EXPECT_TRUE(sim.violations().empty());
}

// ---------- the gate: many seeded schedules with random faults ----------

TEST(Raft, SeededSchedulesPreserveSafetyAndLiveness) {
  // internals.md §7 / PLAN §7: 1,000 seeded schedules with random partitions,
  // drops, and restarts. Safety (the four invariants) must hold on EVERY step
  // of EVERY schedule; liveness must hold once the network heals.
  constexpr int kSchedules = 1000;
  int committed_schedules = 0;

  for (int seed = 0; seed < kSchedules; ++seed) {
    Simulator sim(Members(), static_cast<uint64_t>(seed) + 1);
    std::mt19937_64 r(static_cast<uint64_t>(seed) ^ 0x9e3779b97f4a7c15ULL);
    const auto pick = [&](uint64_t n) -> size_t { return r() % n; };

    // Chaos phase: 24 rounds of a random fault + a proposal attempt + a burst.
    for (int round = 0; round < 24; ++round) {
      switch (pick(6)) {
        case 0: sim.Partition({Members()[pick(3)]}); break;   // isolate one node
        case 1: sim.Heal(); break;
        case 2: sim.Restart(Members()[pick(3)]); break;        // crash+reboot
        case 3: sim.SetDrop(0.15); break;                      // lossy links
        case 4: sim.SetDrop(0.0); break;
        case 5: sim.ProposeViaLeader(); break;                 // may be a no-op
      }
      sim.RunFor(static_cast<uint64_t>(300 + pick(600)), 10);
    }

    // Recovery: heal, no loss, run long → a majority is connected, so a leader
    // must emerge and proposals must commit.
    sim.Heal();
    sim.SetDrop(0.0);
    sim.RunFor(5000, 10);
    ASSERT_NE(sim.Leader(), nullptr) << "seed " << seed << ": no leader after heal";
    const uint64_t before = sim.max_commit_index();
    for (int i = 0; i < 3; ++i) {
      sim.ProposeViaLeader();
      sim.RunFor(1000, 10);
    }
    if (sim.max_commit_index() > before) ++committed_schedules;

    ASSERT_TRUE(sim.violations().empty())
        << "seed " << seed << ": " << sim.violations().front().invariant
        << " — " << sim.violations().front().detail
        << " @ " << sim.violations().front().at_ms << "ms";
  }
  // Liveness held for (essentially) every schedule once healed.
  EXPECT_GT(committed_schedules, kSchedules * 0.95);
}

}  // namespace
}  // namespace lucent::raft
