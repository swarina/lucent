#pragma once

// Mini-Raft core (M5-T1), internals.md §7. A PURE, single-threaded, I/O-free
// state machine: no threads, no clocks, no sockets. Time is injected via
// Tick(now_ms); messages arrive via Handle() and leave via TakeOutbox(). This
// is what lets the whole thing run under a seeded deterministic simulator with
// invariant checkers (raft_sim.h) — and later behind a real gRPC transport
// (M5-T2) that just marshals these structs. Scope-fenced per the spec: leader
// election + log replication + persistence only; no snapshots, compaction,
// membership change, or read leases. 3 fixed voters; state machine = ShardMap
// with CAS-on-epoch mutations.

#include <cstdint>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "lucent/v1/common.pb.h"
#include "lucent/v1/raft.pb.h"

namespace lucent::raft {

enum class Role { kFollower, kCandidate, kLeader };
const char* RoleName(Role r);

// Timing (internals.md §7): tuned for human-visible elections on localhost.
constexpr uint64_t kHeartbeatMs = 150;
constexpr uint64_t kElectionMinMs = 500;
constexpr uint64_t kElectionMaxMs = 1000;

struct LogEntry {
  uint64_t index = 0;
  uint64_t term = 0;
  lucent::v1::ShardMapMutation mutation;
};

// RPC envelopes — plain structs; the gRPC transport (M5-T2) marshals these to
// the raft.proto messages. Keeping the core free of wire types keeps it pure.
struct VoteReq {
  uint64_t term = 0;
  std::string candidate;
  uint64_t last_log_index = 0;
  uint64_t last_log_term = 0;
};
struct VoteResp {
  uint64_t term = 0;
  bool granted = false;
};
struct AppendReq {
  uint64_t term = 0;
  std::string leader;
  uint64_t prev_index = 0;
  uint64_t prev_term = 0;
  std::vector<LogEntry> entries;
  uint64_t commit_index = 0;
};
struct AppendResp {
  uint64_t term = 0;
  bool success = false;
  uint64_t match_index = 0;
};

struct Message {
  enum Kind { kVoteReq, kVoteResp, kAppendReq, kAppendResp } kind;
  std::string from;
  std::string to;
  VoteReq vote_req;
  VoteResp vote_resp;
  AppendReq append_req;
  AppendResp append_resp;
};

// The only state that must survive a crash (data-formats.md §7): current term,
// vote, and the log. Everything else is volatile and rebuilt after restart.
struct Persistent {
  uint64_t current_term = 0;
  std::string voted_for;  // "" = none
  std::vector<LogEntry> log;
};

class RaftNode {
 public:
  // `members` includes this node's own id. `seed` seeds the randomized election
  // timeout so schedules are reproducible.
  RaftNode(std::string id, std::vector<std::string> members, uint64_t seed);

  // Advance logical time: may start an election (follower/candidate) or emit
  // heartbeats (leader).
  void Tick(uint64_t now_ms);
  // Process one incoming message; may emit replies/heartbeats into the outbox.
  void Handle(const Message& m, uint64_t now_ms);
  // Leader-only: append a shard-map mutation to the log. Returns false (no-op)
  // if not the leader. The mutation's expected_epoch/proposed are set by caller.
  bool Propose(const lucent::v1::ShardMapMutation& mutation, uint64_t now_ms);

  // Drain messages produced since the last drain (the simulator/transport ships
  // them with delay/drop/partition applied).
  std::vector<Message> TakeOutbox();

  // Crash/restart (internals.md §7): reload the persisted snapshot (term, vote,
  // log), reset all volatile state (role→follower, commit/apply→0; the state
  // machine is rebuilt as the committed log re-applies). Re-arms the election
  // timer relative to now.
  void Restore(Persistent p, uint64_t now_ms);

  // --- observation (for invariant checkers, viz, and the RaftStore) ---
  Role role() const { return role_; }
  uint64_t term() const { return p_.current_term; }
  const std::string& id() const { return id_; }
  const std::string& leader() const { return leader_; }
  uint64_t commit_index() const { return commit_index_; }
  const std::vector<LogEntry>& log() const { return p_.log; }
  const lucent::v1::ShardMap& applied() const { return applied_; }
  const Persistent& persistent() const { return p_; }

 private:
  uint64_t LastLogIndex() const { return p_.log.empty() ? 0 : p_.log.back().index; }
  uint64_t LastLogTerm() const { return p_.log.empty() ? 0 : p_.log.back().term; }
  uint64_t TermAt(uint64_t index) const;  // 0 for index 0 or out of range
  bool CandidateUpToDate(uint64_t cand_last_index, uint64_t cand_last_term) const;

  void BecomeFollower(uint64_t term);
  void BecomeCandidate(uint64_t now_ms);
  void BecomeLeader(uint64_t now_ms);
  void ResetElectionDeadline(uint64_t now_ms);
  void ReplicateTo(const std::string& peer, uint64_t now_ms);  // leader → one peer
  void BroadcastAppends(uint64_t now_ms);                      // leader → all peers
  void AdvanceCommit();                                        // leader commit rule
  void ApplyCommitted();                                       // apply up to commit_index
  void Send(Message&& m);

  const std::string id_;
  const std::vector<std::string> members_;  // includes self
  const size_t majority_;

  Persistent p_;  // persistent state

  // volatile
  Role role_ = Role::kFollower;
  std::string leader_;
  uint64_t commit_index_ = 0;
  uint64_t last_applied_ = 0;
  lucent::v1::ShardMap applied_;

  // candidate
  std::vector<std::string> votes_;

  // leader (indexed by peer id)
  std::unordered_map<std::string, uint64_t> next_index_;
  std::unordered_map<std::string, uint64_t> match_index_;

  // timers
  uint64_t election_deadline_ms_ = 0;
  uint64_t last_heartbeat_ms_ = 0;

  std::mt19937_64 rng_;
  std::vector<Message> outbox_;
};

}  // namespace lucent::raft
