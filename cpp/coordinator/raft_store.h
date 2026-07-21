#pragma once

// RaftStore (M5-T3): the coordinator's client to the mini-Raft membership
// quorum. The shard map is no longer owned by the coordinator's memory — it is
// the Raft state machine. The coordinator PROPOSES map changes (failover,
// add/remove replica) through the leader and WATCHES the committed stream to
// learn the authoritative map, so a coordinator restart just re-watches instead
// of rebuilding from config (retiring the M3 epoch-reconcile footgun).
//
// Transport only: Propose follows a NOT_LEADER redirect via leader_hint; Watch
// runs a background thread that reconnects across leader changes. Both talk
// MembershipService (raft.proto).

#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "lucent/v1/common.pb.h"
#include "lucent/v1/raft.grpc.pb.h"

namespace lucent {

class RaftStore {
 public:
  // `members` maps member id ("member-0") → "host:port". A leader_hint redirect
  // names an id, which we resolve here.
  explicit RaftStore(std::map<std::string, std::string> members);
  ~RaftStore();

  // Propose a new committed shard map via CAS on `expected_epoch` (the map's
  // current epoch). Follows leader_hint. Returns the committed epoch on success,
  // 0 if it could not be committed (no leader reachable, CAS lost, timeout).
  uint64_t Propose(const lucent::v1::ShardMap& proposed, uint64_t expected_epoch);

  // Start watching committed maps from `from_epoch`; `on_map` fires for each new
  // committed ShardMap (monotonic epoch). Idempotent-ish: reconnects to another
  // member on stream end. Call once.
  void StartWatch(uint64_t from_epoch,
                  std::function<void(const lucent::v1::ShardMap&)> on_map);
  void Stop();

 private:
  lucent::v1::MembershipService::Stub* StubFor(const std::string& id);
  void WatchLoop(uint64_t from_epoch,
                 std::function<void(const lucent::v1::ShardMap&)> on_map);

  const std::map<std::string, std::string> members_;
  std::mutex stubs_mu_;
  std::map<std::string, std::unique_ptr<lucent::v1::MembershipService::Stub>> stubs_;
  std::string last_leader_;  // sticky hint (guarded by stubs_mu_)

  std::thread watch_thread_;
  std::atomic<bool> stop_{false};
  grpc::ClientContext* watch_ctx_ = nullptr;  // for TryCancel on Stop
  std::mutex watch_ctx_mu_;
};

}  // namespace lucent
