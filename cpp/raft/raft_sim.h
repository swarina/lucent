#pragma once

// Seeded, deterministic network simulator for the mini-Raft core (M5-T1),
// internals.md §7. Drives logical time in fixed steps, ships messages with
// per-link delay, and can partition / heal / drop / restart nodes — then runs
// the four Raft safety invariants after every step. No real time, no threads,
// no sockets: same seed ⇒ same schedule ⇒ reproducible pass/fail, which is what
// makes the 1,000-schedule CI gate meaningful.

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "raft/raft_node.h"

namespace lucent::raft {

struct Violation {
  std::string invariant;
  std::string detail;
  uint64_t at_ms = 0;
};

class Simulator {
 public:
  Simulator(std::vector<std::string> members, uint64_t seed)
      : members_(members), rng_(seed) {
    uint64_t s = seed;
    for (const std::string& id : members_) {
      // Distinct per-node seed so election timeouts don't all fire together.
      nodes_[id] = std::make_unique<RaftNode>(id, members_, s * 2654435761u + 1);
      group_[id] = 0;
      ++s;
    }
  }

  // Advance one step of logical time: tick every node, deliver due messages,
  // ship what they produce, then check invariants.
  void Step(uint64_t step_ms) {
    now_ms_ += step_ms;
    for (const std::string& id : members_) {
      nodes_[id]->Tick(now_ms_);
      Ship(nodes_[id]->TakeOutbox());
    }
    std::vector<Envelope> due;
    std::vector<Envelope> keep;
    for (auto& e : inflight_) (e.deliver_at <= now_ms_ ? due : keep).push_back(e);
    inflight_ = std::move(keep);
    for (const Envelope& e : due) {
      if (Connected(e.msg.from, e.msg.to)) {  // partition can drop in flight
        nodes_[e.msg.to]->Handle(e.msg, now_ms_);
        Ship(nodes_[e.msg.to]->TakeOutbox());
      }
    }
    CheckInvariants();
  }

  void RunFor(uint64_t total_ms, uint64_t step_ms) {
    for (uint64_t t = 0; t < total_ms; t += step_ms) Step(step_ms);
  }

  // --- fault injection (internals.md §7) ---
  void Partition(const std::set<std::string>& side) {
    for (const std::string& id : members_) group_[id] = side.count(id) ? 1 : 0;
  }
  void Heal() {
    for (const std::string& id : members_) group_[id] = 0;
  }
  void SetDrop(double p) { drop_p_ = p; }
  void Restart(const std::string& id) {
    // Crash: keep the persisted snapshot (term, vote, log), lose volatile state.
    nodes_[id]->Restore(nodes_[id]->persistent(), now_ms_);
  }

  // Propose a shard-map bump through the current leader (CAS chain on epoch).
  // Returns the leader id if accepted, else nullopt.
  std::optional<std::string> ProposeViaLeader() {
    RaftNode* ldr = Leader();
    if (ldr == nullptr) return std::nullopt;
    lucent::v1::ShardMapMutation m;
    const uint64_t base = ldr->applied().epoch();
    m.set_expected_epoch(base);
    m.mutable_proposed()->set_epoch(base + 1);
    if (!ldr->Propose(m, now_ms_)) return std::nullopt;
    Ship(ldr->TakeOutbox());
    return ldr->id();
  }

  // The leader of the highest term, if exactly one node claims it there.
  RaftNode* Leader() {
    RaftNode* best = nullptr;
    for (const std::string& id : members_) {
      RaftNode* n = nodes_[id].get();
      if (n->role() == Role::kLeader &&
          (best == nullptr || n->term() > best->term())) {
        best = n;
      }
    }
    return best;
  }

  RaftNode* node(const std::string& id) { return nodes_[id].get(); }
  const std::vector<Violation>& violations() const { return violations_; }
  uint64_t now_ms() const { return now_ms_; }
  // Highest index that any node has committed (for liveness assertions).
  uint64_t max_commit_index() const {
    uint64_t m = 0;
    for (const auto& [id, n] : nodes_) m = std::max(m, n->commit_index());
    return m;
  }

 private:
  struct Envelope {
    uint64_t deliver_at = 0;
    Message msg;
  };

  bool Connected(const std::string& a, const std::string& b) const {
    return group_.at(a) == group_.at(b);
  }

  void Ship(std::vector<Message>&& msgs) {
    std::uniform_real_distribution<double> unit(0.0, 1.0);
    std::uniform_int_distribution<uint64_t> delay(1, 40);  // per-link latency
    for (Message& m : msgs) {
      if (!Connected(m.from, m.to)) continue;      // partitioned → dropped
      if (drop_p_ > 0 && unit(rng_) < drop_p_) continue;  // lossy link
      inflight_.push_back({now_ms_ + delay(rng_), std::move(m)});
    }
  }

  void Record(const std::string& inv, const std::string& detail) {
    violations_.push_back({inv, detail, now_ms_});
  }

  void CheckInvariants() {
    CheckElectionSafety();
    CheckLogMatching();
    CheckCommittedConsistency();  // leader-completeness + state-machine safety
  }

  // ≤ 1 leader per term, ever (§5.2).
  void CheckElectionSafety() {
    for (const std::string& id : members_) {
      RaftNode* n = nodes_[id].get();
      if (n->role() != Role::kLeader) continue;
      auto [it, inserted] = leader_of_term_.try_emplace(n->term(), id);
      if (!inserted && it->second != id) {
        Record("election-safety", "term " + std::to_string(n->term()) +
                                      " had leaders " + it->second + " and " + id);
      }
    }
  }

  // If two logs share (index, term), their prefixes up to that index are
  // identical (§5.3). Checked pairwise on current logs.
  void CheckLogMatching() {
    for (size_t i = 0; i < members_.size(); ++i) {
      for (size_t j = i + 1; j < members_.size(); ++j) {
        const auto& a = nodes_[members_[i]]->log();
        const auto& b = nodes_[members_[j]]->log();
        const size_t n = std::min(a.size(), b.size());
        bool diverged = false;
        for (size_t k = 0; k < n; ++k) {
          if (a[k].term == b[k].term) {
            if (diverged) {
              Record("log-matching", "terms re-converge after divergence at index " +
                                         std::to_string(k + 1));
              break;
            }
          } else {
            diverged = true;
          }
        }
      }
    }
  }

  // Every committed entry is permanent and identical across nodes — this is
  // leader completeness (a committed entry survives into every future leader)
  // and state-machine safety (no two nodes commit different entries at an
  // index) in one check: record each index's committed (term, epoch) the first
  // time any node commits it, and flag any later disagreement.
  void CheckCommittedConsistency() {
    for (const std::string& id : members_) {
      RaftNode* n = nodes_[id].get();
      const auto& log = n->log();
      for (uint64_t i = 1; i <= n->commit_index() && i <= log.size(); ++i) {
        const std::pair<uint64_t, uint64_t> sig{
            log[i - 1].term, log[i - 1].mutation.proposed().epoch()};
        auto [it, inserted] = committed_.try_emplace(i, sig);
        if (!inserted && it->second != sig) {
          Record("committed-consistency",
                 "index " + std::to_string(i) + " committed as (term " +
                     std::to_string(it->second.first) + ") then (term " +
                     std::to_string(sig.first) + ") by " + id);
        }
      }
    }
  }

  std::vector<std::string> members_;
  std::unordered_map<std::string, std::unique_ptr<RaftNode>> nodes_;
  std::unordered_map<std::string, int> group_;  // partition group
  std::vector<Envelope> inflight_;
  uint64_t now_ms_ = 0;
  double drop_p_ = 0.0;
  std::mt19937_64 rng_;

  // invariant-checker history
  std::map<uint64_t, std::string> leader_of_term_;
  std::map<uint64_t, std::pair<uint64_t, uint64_t>> committed_;  // index→(term,epoch)
  std::vector<Violation> violations_;
};

}  // namespace lucent::raft
