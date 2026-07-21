#pragma once

// lucent-member: the process that drives a mini-Raft RaftNode over real gRPC
// (M5-T2). The pure core (raft_node.h) is single-threaded; here a single mutex
// serializes all access to it, a driver thread injects wall-clock time via
// Tick, and outbound request messages are shipped with the async client API
// (the coordinator's proven fan-out pattern) so no thread ever blocks the core
// while waiting on the network. Persistence (raft_storage.h) is written before
// any RPC is acked (internals.md §7).
//
//   RaftService      (peer↔peer)     RequestVote, AppendEntries
//   MembershipService (client-facing) Propose (leader; commit-wait), Watch
//
// One class hosts both services via two thin adapters (the generated service
// bases each derive from grpc::Service, so a single object can't be both).

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "common/collector_sink.h"
#include "common/event_emitter.h"
#include "lucent/v1/raft.grpc.pb.h"
#include "raft/raft_node.h"
#include "raft/raft_storage.h"

namespace lucent::raft {

class MemberServer {
 public:
  // `members` maps every member id (incl. self) to its "host:port". `dir` is
  // the persistence directory (data/member-j/). `seed` seeds the election timer.
  // `collector_addr` (optional): emit RaftEvents (role/term/leader) here for the
  // election viz; empty ⇒ no eventing (tests).
  MemberServer(std::string id, std::map<std::string, std::string> members,
               std::string dir, uint64_t seed, std::string collector_addr = "");
  ~MemberServer();

  // Build + start the gRPC server (both services) on `bind_addr`, load persisted
  // state, and start the driver thread. Returns false if the port can't bind.
  bool Start(const std::string& bind_addr);
  void Shutdown();

  const std::string& id() const { return id_; }
  // Observation (for tests/viz). Locks internally.
  Role role();
  uint64_t term();
  std::string leader();
  uint64_t applied_epoch();
  // Proposed epoch of the last log entry (0 if empty) — the base a client
  // chains the next CAS off, since a just-elected leader may not have *applied*
  // a committed prior-term entry yet (it commits it only with a new-term entry).
  uint64_t log_head_epoch();

  // --- RPC cores (called by the service adapters) ---
  void OnRequestVote(const lucent::v1::VoteRequest& req, lucent::v1::VoteResponse* resp);
  void OnAppendEntries(const lucent::v1::AppendRequest& req, lucent::v1::AppendResponse* resp);
  void OnPropose(const lucent::v1::ShardMapMutation& req, lucent::v1::ProposeResponse* resp);
  void OnWatch(const lucent::v1::WatchRequest& req,
               grpc::ServerContext* ctx,
               grpc::ServerWriter<lucent::v1::ShardMap>* writer);

 private:
  uint64_t NowMs() const;
  void DriverLoop();
  void Persist();                         // caller holds mu_
  // Emit a RaftEvent (role/term/leader) on any change, and at ≥1 Hz otherwise —
  // the heartbeat lets a UI that connects after the election still see current
  // state (there is no snapshot for member roles). Caller holds mu_.
  void EmitState();
  void Dispatch(std::vector<Message> out);  // caller must NOT hold mu_
  void Deliver(Message m);                // feed a peer's reply back into the core

  const std::string id_;
  const std::map<std::string, std::string> members_;

  std::mutex mu_;
  std::condition_variable cv_;  // commit advanced / applied changed / shutdown
  RaftNode node_;
  RaftStorage storage_;

  // Optional RaftEvent eventing for the viz (null in tests).
  std::unique_ptr<CollectorSink> sink_;
  std::unique_ptr<EventEmitter> emitter_;
  Role last_role_ = Role::kFollower;
  uint64_t last_term_ = 0;
  std::string last_leader_;
  uint64_t last_emit_ms_ = 0;

  std::unordered_map<std::string, std::unique_ptr<lucent::v1::RaftService::Stub>> peers_;
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<lucent::v1::RaftService::Service> raft_svc_;
  std::unique_ptr<lucent::v1::MembershipService::Service> member_svc_;

  std::thread driver_;
  std::atomic<bool> stop_{false};
  const std::chrono::steady_clock::time_point epoch_;
};

}  // namespace lucent::raft
