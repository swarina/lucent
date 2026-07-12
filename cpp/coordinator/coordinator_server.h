#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "common/config.h"
#include "common/event_emitter.h"
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

  grpc::Status Query(grpc::ServerContext* ctx,
                     const lucent::v1::QueryRequest* req,
                     lucent::v1::QueryResponse* resp) override;
  grpc::Status GetClusterState(grpc::ServerContext* ctx,
                               const lucent::v1::ClusterStateRequest* req,
                               lucent::v1::ClusterState* resp) override;

 private:
  lucent::v1::ShardService::Stub* ShardStub(const std::string& addr);
  std::string MintTraceId();
  void EmitSpan(const std::string& trace_id, lucent::v1::SpanKind kind,
                uint64_t t_start_ns, uint64_t t_end_ns, uint32_t shard_id,
                std::string detail_json);

  const Config config_;
  const lucent::v1::ShardMap shard_map_;
  EventEmitter* const emitter_;  // not owned; may be null in tests

  std::unique_ptr<lucent::v1::EmbedService::Stub> embed_stub_;

  // Long-lived channel per shard address (protocol.md §2 keepalive).
  std::mutex stubs_mu_;
  std::unordered_map<std::string,
                     std::unique_ptr<lucent::v1::ShardService::Stub>>
      shard_stubs_;

  std::mutex rng_mu_;
  std::mt19937_64 rng_;  // trace-id minting (uniqueness, not determinism)
};

// Builds the M0 static shard map from config: `shards` primaries, replica
// 'a', epoch 1, all SERVING.
lucent::v1::ShardMap StaticShardMapFromConfig(const Config& config);

}  // namespace lucent
