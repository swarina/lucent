#include "raft/raft_node.h"

#include <algorithm>
#include <utility>

namespace lucent::raft {

const char* RoleName(Role r) {
  switch (r) {
    case Role::kFollower: return "follower";
    case Role::kCandidate: return "candidate";
    case Role::kLeader: return "leader";
  }
  return "?";
}

RaftNode::RaftNode(std::string id, std::vector<std::string> members, uint64_t seed)
    : id_(std::move(id)),
      members_(std::move(members)),
      majority_(members_.size() / 2 + 1),
      rng_(seed) {
  // Election timers arm relative to the first Tick; ResetElectionDeadline runs
  // there. Start as a follower at term 0 with an empty log (Raft §5.2).
}

uint64_t RaftNode::TermAt(uint64_t index) const {
  if (index == 0 || index > p_.log.size()) return 0;
  return p_.log[index - 1].term;  // log is 1-indexed by `index`, dense from 1
}

bool RaftNode::CandidateUpToDate(uint64_t cand_last_index,
                                 uint64_t cand_last_term) const {
  // §5.4.1: candidate's log is at least as up-to-date as ours.
  const uint64_t my_term = LastLogTerm();
  if (cand_last_term != my_term) return cand_last_term > my_term;
  return cand_last_index >= LastLogIndex();
}

void RaftNode::ResetElectionDeadline(uint64_t now_ms) {
  std::uniform_int_distribution<uint64_t> d(kElectionMinMs, kElectionMaxMs);
  election_deadline_ms_ = now_ms + d(rng_);
}

void RaftNode::BecomeFollower(uint64_t term) {
  if (term > p_.current_term) {
    p_.current_term = term;
    p_.voted_for.clear();  // new term → vote is fresh (must persist before ack)
  }
  role_ = Role::kFollower;
  votes_.clear();
}

void RaftNode::BecomeCandidate(uint64_t now_ms) {
  ++p_.current_term;               // §5.2
  p_.voted_for = id_;              // vote for self
  role_ = Role::kCandidate;
  leader_.clear();
  votes_ = {id_};
  ResetElectionDeadline(now_ms);
  for (const std::string& peer : members_) {
    if (peer == id_) continue;
    Message m{Message::kVoteReq, id_, peer, {}, {}, {}, {}};
    m.vote_req = {p_.current_term, id_, LastLogIndex(), LastLogTerm()};
    Send(std::move(m));
  }
}

void RaftNode::BecomeLeader(uint64_t now_ms) {
  role_ = Role::kLeader;
  leader_ = id_;
  const uint64_t next = LastLogIndex() + 1;
  for (const std::string& peer : members_) {
    if (peer == id_) continue;
    next_index_[peer] = next;
    match_index_[peer] = 0;
  }
  last_heartbeat_ms_ = now_ms;
  BroadcastAppends(now_ms);  // assert leadership immediately (§5.2)
}

void RaftNode::Tick(uint64_t now_ms) {
  if (election_deadline_ms_ == 0) ResetElectionDeadline(now_ms);  // first tick
  if (role_ == Role::kLeader) {
    if (now_ms - last_heartbeat_ms_ >= kHeartbeatMs) {
      last_heartbeat_ms_ = now_ms;
      BroadcastAppends(now_ms);
    }
    return;
  }
  // follower or candidate: election timeout → (re)start an election (§5.2)
  if (now_ms >= election_deadline_ms_) BecomeCandidate(now_ms);
}

bool RaftNode::Propose(const lucent::v1::ShardMapMutation& mutation,
                       uint64_t now_ms) {
  if (role_ != Role::kLeader) return false;
  LogEntry e;
  e.index = LastLogIndex() + 1;
  e.term = p_.current_term;
  e.mutation = mutation;
  p_.log.push_back(std::move(e));
  BroadcastAppends(now_ms);  // push it out now; heartbeats also retry
  return true;
}

void RaftNode::ReplicateTo(const std::string& peer, uint64_t now_ms) {
  (void)now_ms;
  const uint64_t next = next_index_[peer];
  const uint64_t prev_index = next - 1;
  Message m{Message::kAppendReq, id_, peer, {}, {}, {}, {}};
  AppendReq& r = m.append_req;
  r.term = p_.current_term;
  r.leader = id_;
  r.prev_index = prev_index;
  r.prev_term = TermAt(prev_index);
  r.commit_index = commit_index_;
  for (uint64_t i = next; i <= LastLogIndex(); ++i) r.entries.push_back(p_.log[i - 1]);
  Send(std::move(m));
}

void RaftNode::BroadcastAppends(uint64_t now_ms) {
  for (const std::string& peer : members_) {
    if (peer != id_) ReplicateTo(peer, now_ms);
  }
}

void RaftNode::AdvanceCommit() {
  // §5.3/§5.4.2: commit the highest N replicated on a majority whose entry is
  // from the CURRENT term (a leader never commits prior-term entries by count).
  for (uint64_t n = LastLogIndex(); n > commit_index_; --n) {
    if (TermAt(n) != p_.current_term) continue;
    size_t count = 1;  // self
    for (const std::string& peer : members_) {
      if (peer != id_ && match_index_[peer] >= n) ++count;
    }
    if (count >= majority_) {
      commit_index_ = n;
      ApplyCommitted();
      return;
    }
  }
}

void RaftNode::ApplyCommitted() {
  while (last_applied_ < commit_index_) {
    ++last_applied_;
    const lucent::v1::ShardMapMutation& mut = p_.log[last_applied_ - 1].mutation;
    // CAS-on-epoch: apply only if the map is at the expected epoch (internals
    // §7). A stale mutation is a committed no-op — every node skips it alike,
    // so the state machine stays identical across the cluster.
    if (applied_.epoch() == mut.expected_epoch()) applied_ = mut.proposed();
  }
}

void RaftNode::Handle(const Message& m, uint64_t now_ms) {
  switch (m.kind) {
    case Message::kVoteReq: {
      const VoteReq& req = m.vote_req;
      if (req.term > p_.current_term) BecomeFollower(req.term);
      bool granted = false;
      if (req.term == p_.current_term &&
          (p_.voted_for.empty() || p_.voted_for == req.candidate) &&
          CandidateUpToDate(req.last_log_index, req.last_log_term)) {
        granted = true;
        p_.voted_for = req.candidate;
        ResetElectionDeadline(now_ms);  // granting a vote defers our own election
      }
      Message r{Message::kVoteResp, id_, m.from, {}, {}, {}, {}};
      r.vote_resp = {p_.current_term, granted};
      Send(std::move(r));
      break;
    }
    case Message::kVoteResp: {
      const VoteResp& resp = m.vote_resp;
      if (resp.term > p_.current_term) {
        BecomeFollower(resp.term);
        break;
      }
      if (role_ != Role::kCandidate || resp.term != p_.current_term) break;
      if (resp.granted &&
          std::find(votes_.begin(), votes_.end(), m.from) == votes_.end()) {
        votes_.push_back(m.from);
        if (votes_.size() >= majority_) BecomeLeader(now_ms);
      }
      break;
    }
    case Message::kAppendReq: {
      const AppendReq& req = m.append_req;
      Message r{Message::kAppendResp, id_, m.from, {}, {}, {}, {}};
      if (req.term < p_.current_term) {  // stale leader (§5.1)
        r.append_resp = {p_.current_term, false, 0};
        Send(std::move(r));
        break;
      }
      // Valid leader for this (≥) term: (re)become follower, defer election.
      if (req.term > p_.current_term || role_ != Role::kFollower) {
        BecomeFollower(req.term);
      }
      leader_ = req.leader;
      ResetElectionDeadline(now_ms);

      // Log consistency check (§5.3): our log must contain prev_index@prev_term.
      if (req.prev_index > LastLogIndex() || TermAt(req.prev_index) != req.prev_term) {
        r.append_resp = {p_.current_term, false, 0};
        Send(std::move(r));
        break;
      }
      // Append, truncating on the first conflicting term (§5.3).
      for (const LogEntry& e : req.entries) {
        if (e.index <= LastLogIndex()) {
          if (TermAt(e.index) == e.term) continue;   // already have it
          p_.log.resize(e.index - 1);                // conflict → drop suffix
        }
        p_.log.push_back(e);
      }
      const uint64_t last_new = req.prev_index + req.entries.size();
      if (req.commit_index > commit_index_) {
        commit_index_ = std::min(req.commit_index, last_new);
        ApplyCommitted();
      }
      r.append_resp = {p_.current_term, true, last_new};
      Send(std::move(r));
      break;
    }
    case Message::kAppendResp: {
      const AppendResp& resp = m.append_resp;
      if (resp.term > p_.current_term) {
        BecomeFollower(resp.term);
        break;
      }
      if (role_ != Role::kLeader || resp.term != p_.current_term) break;
      if (resp.success) {
        match_index_[m.from] = std::max(match_index_[m.from], resp.match_index);
        next_index_[m.from] = match_index_[m.from] + 1;
        AdvanceCommit();
      } else if (next_index_[m.from] > 1) {
        --next_index_[m.from];  // back off; the next heartbeat retries (§5.3)
      }
      break;
    }
  }
}

void RaftNode::Restore(Persistent p, uint64_t now_ms) {
  p_ = std::move(p);  // reload the on-disk snapshot (term, vote, log)
  // Reset all volatile state; the committed log re-applies from index 1 as the
  // leader re-advances commit_index (no snapshots — scope fence).
  role_ = Role::kFollower;
  leader_.clear();
  votes_.clear();
  next_index_.clear();
  match_index_.clear();
  commit_index_ = 0;
  last_applied_ = 0;
  applied_.Clear();
  election_deadline_ms_ = 0;
  last_heartbeat_ms_ = 0;
  ResetElectionDeadline(now_ms);
}

void RaftNode::Send(Message&& m) { outbox_.push_back(std::move(m)); }

std::vector<Message> RaftNode::TakeOutbox() {
  return std::exchange(outbox_, {});
}

}  // namespace lucent::raft
