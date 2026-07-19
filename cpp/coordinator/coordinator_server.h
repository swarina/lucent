#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "common/config.h"
#include "common/event_emitter.h"
#include "coordinator/planner.h"
#include "lucent/v1/coordinator.grpc.pb.h"
#include "lucent/v1/embed.grpc.pb.h"
#include "lucent/v1/shard.grpc.pb.h"

namespace lucent {

// CoordinatorService (protocol.md §1.3): embed -> plan -> fan-out -> merge,
// with coverage as the first-class partial-results contract. M0 scope:
// static shard map, primary replicas, parallel fan-out via std::async
// (callback-API async fan-out lands with M2 where it matters).
class CoordinatorServer final : public lucent::v1::CoordinatorService::Service {
 public:
  // `shard_map` is the static M0 membership (synthesized from config or a
  // shardmap.json); `embed_addr` the embed service target.
  CoordinatorServer(Config config, lucent::v1::ShardMap shard_map,
                    std::string embed_addr, EventEmitter* emitter);
  ~CoordinatorServer();

  grpc::Status Query(grpc::ServerContext* ctx,
                     const lucent::v1::QueryRequest* req,
                     lucent::v1::QueryResponse* resp) override;
  grpc::Status GetClusterState(grpc::ServerContext* ctx,
                               const lucent::v1::ClusterStateRequest* req,
                               lucent::v1::ClusterState* resp) override;

  // Membership admin (M3-T5). Mutate the live shard map under map_mu_, bump the
  // epoch, and (Add) seed the new node's health as HEALTHY pending confirmation.
  grpc::Status AddReplica(grpc::ServerContext* ctx,
                          const lucent::v1::AddReplicaRequest* req,
                          lucent::v1::AddReplicaResponse* resp) override;
  grpc::Status RemoveReplica(grpc::ServerContext* ctx,
                             const lucent::v1::RemoveReplicaRequest* req,
                             lucent::v1::RemoveReplicaResponse* resp) override;

  // HealthWatcher: one pass over all nodes (own thread in prod; callable
  // directly from tests for determinism). Returns after updating health +
  // performing any failover.
  void HealthTick();
  // Test seam: force a health verdict for a node without pinging.
  void SetHealthForTest(const std::string& node_id, lucent::v1::HealthState h);

 private:
  struct NodeHealth {
    lucent::v1::HealthState state = lucent::v1::HEALTH_HEALTHY;
    int misses = 0;
    // Set on the first successful ping. Failover only promotes away from a node
    // that was *observed healthy* and then died — a node that has never come up
    // (still loading its sealed index at boot) must not trigger a spurious
    // promotion just because the coordinator can't reach it yet.
    bool ever_healthy = false;
  };

  lucent::v1::ShardService::Stub* ShardStub(const std::string& addr);
  std::string MintTraceId();
  void EmitSpan(const std::string& trace_id, lucent::v1::SpanKind kind,
                uint64_t t_start_ns, uint64_t t_end_ns, uint32_t shard_id,
                std::string detail_json);
  void HealthLoop();
  // On boot, adopt the highest shard-map epoch any reachable shard has already
  // accepted, so a restarted coordinator doesn't issue queries the shards
  // reject as stale. Best-effort; runs before the server serves.
  void ReconcileEpochFromShards();
  // Applies a ping result to a node's state machine (healthy→suspect→down),
  // emits NodeStateChange on transition, and triggers failover on primary DOWN.
  // Caller holds map_mu_.
  void RecordHealth(const std::string& node_id, bool alive);
  void MaybeFailover(const std::string& down_node);  // caller holds map_mu_
  bool IsHealthy(const std::string& node_id) const;  // caller holds map_mu_
  // Build the query plan with health-aware replica selection (caller must NOT
  // hold map_mu_ — this takes it). `qvec` is the embedded query; used only for
  // semantic (centroid) routing, ignored under hash partitioning.
  QueryPlan PlanWithHealth(uint32_t probe, const std::vector<float>& qvec);
  // dot(query, centroid[shard]); 0 if no centroid. Caller holds map_mu_.
  float CentroidScore(uint32_t shard_id, const std::vector<float>& qvec) const;
  // Load data/centroids.f32 for semantic routing (M4). No-op under hash or if
  // the file is absent/malformed → falls back to all-shard routing.
  void LoadCentroids();

  const Config config_;
  EventEmitter* const emitter_;  // not owned; may be null in tests

  // Dynamic membership + health (M3). Guarded together: routing reads them,
  // the HealthWatcher mutates them.
  mutable std::mutex map_mu_;
  lucent::v1::ShardMap shard_map_;
  std::unordered_map<std::string, NodeHealth> health_;
  uint32_t rr_ = 0;  // replica round-robin cursor (guarded by map_mu_)

  // Semantic routing table (M4): centroids_[shard_id] = normalized centroid.
  // Immutable after LoadCentroids(); empty ⇒ route all shards (hash behaviour).
  bool semantic_routing_ = false;
  std::vector<std::vector<float>> centroids_;

  std::unique_ptr<lucent::v1::EmbedService::Stub> embed_stub_;

  std::mutex stubs_mu_;
  std::unordered_map<std::string,
                     std::unique_ptr<lucent::v1::ShardService::Stub>>
      shard_stubs_;

  std::mutex rng_mu_;
  std::mt19937_64 rng_;

  std::thread health_thread_;
  std::atomic<bool> health_stop_{false};
};

// Builds the M0 static shard map from config: `shards` primaries, replica
// 'a', epoch 1, all SERVING.
lucent::v1::ShardMap StaticShardMapFromConfig(const Config& config);

}  // namespace lucent
