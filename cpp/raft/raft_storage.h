#pragma once

// Durable Raft state (M5-T2), data-formats.md §7 — `data/member-{j}/`. Only
// {current_term, voted_for, log} must survive a crash (internals.md §7):
//   state.bin — {magic "LCRT", format u16, term u64, vote_len u8 + bytes,
//               xxh3 u64}, rewritten atomically (temp + rename) on term/vote
//               change so a torn write never yields a half-updated vote.
//   log.bin   — append-only entries {index u64, term u64, len u32,
//               ShardMapMutation bytes, xxh32 u32}. On load, a torn tail entry
//               (bad checksum / short read) truncates the log there.
//
// The driver calls Save() after every RaftNode step; the store reconciles the
// on-disk log to the in-memory one (append the new tail, or rewrite when a
// conflict truncation shortened it) and rewrites state.bin only when it changed.

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "raft/raft_node.h"

namespace lucent::raft {

class RaftStorage {
 public:
  explicit RaftStorage(std::string dir);

  // Recover {term, vote, log} from disk. Missing files → a fresh node (term 0,
  // no vote, empty log). A corrupt state.bin → fresh; a torn log tail → the
  // longest valid prefix.
  Persistent Load();

  // Reconcile disk to `p`: atomically rewrite state.bin iff term/vote changed;
  // append new log entries, or rewrite log.bin when the log diverged (a Raft
  // conflict truncation). Persist before the caller acks any RPC (§7).
  void Save(const Persistent& p);

 private:
  void WriteStateAtomic(uint64_t term, const std::string& vote);
  void RewriteLog(const std::vector<LogEntry>& log);
  void AppendLog(const std::vector<LogEntry>& log, size_t from);

  std::string dir_;
  std::string state_path_;
  std::string log_path_;

  // Mirror of what's on disk, to compute the minimal write on Save().
  uint64_t disk_term_ = 0;
  std::string disk_vote_;
  std::vector<std::pair<uint64_t, uint64_t>> disk_log_keys_;  // (index, term) per entry
};

}  // namespace lucent::raft
