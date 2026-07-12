#include "coordinator/coordinator_server.h"

#include <spdlog/spdlog.h>

#include <chrono>
#include <future>
#include <random>
#include <utility>

#include <nlohmann/json.hpp>

#include "common/monotime.h"
#include "common/node_id.h"
#include "coordinator/merge.h"
#include "coordinator/planner.h"

namespace lucent {

namespace {

std::chrono::system_clock::time_point DeadlineIn(int ms) {
  return std::chrono::system_clock::now() + std::chrono::milliseconds(ms);
}

const char* StatusLabel(grpc::StatusCode code) {
  switch (code) {
    case grpc::StatusCode::OK: return "OK";
    case grpc::StatusCode::DEADLINE_EXCEEDED: return "DEADLINE";
    case grpc::StatusCode::UNAVAILABLE: return "UNAVAILABLE";
    case grpc::StatusCode::FAILED_PRECONDITION: return "STALE_EPOCH";
    default: return "ERROR";
  }
}

}  // namespace

lucent::v1::ShardMap StaticShardMapFromConfig(const Config& config) {
  lucent::v1::ShardMap map;
  map.set_epoch(1);
  map.set_partitioning(config.cluster.partitioning);
  for (int s = 0; s < config.cluster.shards; ++s) {
    auto* e = map.add_shards();
    e->set_shard_id(static_cast<uint32_t>(s));
    e->set_primary_node("shard-" + std::to_string(s) + "a");
    e->set_primary_addr(config.ShardAddr(s, 'a'));
    e->set_primary_state(lucent::v1::NODE_STATE_SERVING);
    if (config.cluster.replicas > 1) {
      e->set_backup_node("shard-" + std::to_string(s) + "b");
      e->set_backup_addr(config.ShardAddr(s, 'b'));
      e->set_backup_state(lucent::v1::NODE_STATE_SERVING);
    }
  }
  return map;
}

CoordinatorServer::CoordinatorServer(Config config,
                                     lucent::v1::ShardMap shard_map,
                                     std::string embed_addr,
                                     EventEmitter* emitter)
    : config_(std::move(config)),
      shard_map_(std::move(shard_map)),
      emitter_(emitter),
      rng_(std::random_device{}()) {
  embed_stub_ = lucent::v1::EmbedService::NewStub(
      grpc::CreateChannel(embed_addr, grpc::InsecureChannelCredentials()));
}

lucent::v1::ShardService::Stub* CoordinatorServer::ShardStub(
    const std::string& addr) {
  std::lock_guard<std::mutex> lock(stubs_mu_);
  auto it = shard_stubs_.find(addr);
  if (it == shard_stubs_.end()) {
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);  // protocol.md §2
    auto stub = lucent::v1::ShardService::NewStub(grpc::CreateCustomChannel(
        addr, grpc::InsecureChannelCredentials(), args));
    it = shard_stubs_.emplace(addr, std::move(stub)).first;
  }
  return it->second.get();
}

std::string CoordinatorServer::MintTraceId() {
  std::lock_guard<std::mutex> lock(rng_mu_);
  std::string id(16, '\0');
  for (int i = 0; i < 2; ++i) {
    const uint64_t r = rng_();
    for (int b = 0; b < 8; ++b) {
      id[static_cast<size_t>(i * 8 + b)] =
          static_cast<char>((r >> (8 * b)) & 0xFF);
    }
  }
  return id;
}

void CoordinatorServer::EmitSpan(const std::string& trace_id,
                                 lucent::v1::SpanKind kind, uint64_t t_start_ns,
                                 uint64_t t_end_ns, uint32_t shard_id,
                                 std::string detail_json) {
  if (emitter_ == nullptr) return;
  lucent::v1::Event e;
  auto* span = e.mutable_span();
  span->set_trace_id(trace_id);
  span->set_kind(kind);
  span->set_t_start_ns(t_start_ns);
  span->set_t_end_ns(t_end_ns);
  span->set_shard_id(shard_id);
  span->set_detail_json(std::move(detail_json));
  emitter_->Emit(std::move(e));
}

grpc::Status CoordinatorServer::Query(grpc::ServerContext* /*ctx*/,
                                      const lucent::v1::QueryRequest* req,
                                      lucent::v1::QueryResponse* resp) {
  const uint64_t t_query0 = MonoNanos();
  const std::string trace_id = MintTraceId();
  resp->set_trace_id(trace_id);

  const uint32_t k = req->k() == 0 ? 10 : req->k();
  const uint32_t ef = req->ef_search() == 0
                          ? static_cast<uint32_t>(config_.index.ef_search_default)
                          : req->ef_search();
  if (req->text().empty()) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "empty query text");
  }
  EmitSpan(trace_id, lucent::v1::SPAN_QUERY_RECEIVED, t_query0, t_query0, 0,
           nlohmann::json{{"k", k}, {"ef", ef}}.dump());

  // --- Embed (fail-fast: no vector, no results — protocol.md §6). ---
  const uint64_t t_embed0 = MonoNanos();
  lucent::v1::EmbedRequest ereq;
  ereq.add_texts(req->text());
  lucent::v1::EmbedResponse eresp;
  grpc::ClientContext ectx;
  ectx.set_deadline(DeadlineIn(config_.timeouts_ms.embed));
  const grpc::Status estatus = embed_stub_->Embed(&ectx, ereq, &eresp);
  const uint64_t t_embed1 = MonoNanos();
  EmitSpan(trace_id, lucent::v1::SPAN_EMBED, t_embed0, t_embed1, 0,
           nlohmann::json{{"status", StatusLabel(estatus.error_code())}}.dump());
  if (!estatus.ok()) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                        "embed failed: " + estatus.error_message());
  }
  if (eresp.dim() != static_cast<uint32_t>(config_.model.dim)) {
    return grpc::Status(grpc::StatusCode::INTERNAL, "embed dim mismatch");
  }

  // --- Plan. ---
  const uint64_t t_plan0 = MonoNanos();
  const QueryPlan plan = PlanQuery(shard_map_, req->probe());
  const uint64_t t_plan1 = MonoNanos();
  {
    nlohmann::json shards = nlohmann::json::array();
    for (const auto& p : plan.probe) shards.push_back(p.shard_id);
    EmitSpan(trace_id, lucent::v1::SPAN_PLAN, t_plan0, t_plan1, 0,
             nlohmann::json{{"probe", plan.probe.size()},
                            {"shards", shards},
                            {"epoch", plan.epoch}}.dump());
  }

  // --- Fan-out (parallel; per-shard deadline; failures become coverage). ---
  const uint64_t t_fan0 = MonoNanos();
  struct FanoutOutcome {
    grpc::Status status = grpc::Status::OK;
    lucent::v1::SearchResponse resp;
    uint64_t t0 = 0, t1 = 0;
  };
  std::vector<std::future<FanoutOutcome>> futures;
  futures.reserve(plan.probe.size());
  for (const PlannedShard& target : plan.probe) {
    futures.push_back(std::async(std::launch::async, [&, target] {
      FanoutOutcome out;
      out.t0 = MonoNanos();
      lucent::v1::SearchRequest sreq;
      sreq.set_trace_id(trace_id);
      *sreq.mutable_vector() = eresp.vectors();
      sreq.set_k(k);
      sreq.set_ef_search(ef);
      sreq.set_trace_level(req->trace_level());
      sreq.set_shard_map_epoch(plan.epoch);
      grpc::ClientContext sctx;
      sctx.set_deadline(DeadlineIn(config_.timeouts_ms.shard_search));
      out.status = ShardStub(target.addr)->Search(&sctx, sreq, &out.resp);
      out.t1 = MonoNanos();
      return out;
    }));
  }

  std::vector<ShardResult> answered;
  auto* coverage = resp->mutable_coverage();
  coverage->set_probed(static_cast<uint32_t>(plan.probe.size()));
  for (uint32_t s : plan.unprobed) coverage->add_unprobed_shards(s);
  for (size_t i = 0; i < futures.size(); ++i) {
    FanoutOutcome out = futures[i].get();
    const PlannedShard& target = plan.probe[i];
    EmitSpan(trace_id, lucent::v1::SPAN_SHARD_RPC, out.t0, out.t1,
             target.shard_id,
             nlohmann::json{{"status", StatusLabel(out.status.error_code())},
                            {"node", target.node_id}}.dump());
    if (out.status.ok()) {
      resp->set_visited_total(resp->visited_total() + out.resp.visited());
      answered.push_back(ShardResult{target.shard_id, std::move(out.resp)});
    } else {
      coverage->add_missing_shards(target.shard_id);
      spdlog::warn("shard {} ({}): {}", target.shard_id, target.node_id,
                   out.status.error_message());
    }
  }
  coverage->set_answered(static_cast<uint32_t>(answered.size()));
  const uint64_t t_fan1 = MonoNanos();

  // --- Merge. ---
  const uint64_t t_merge0 = MonoNanos();
  for (auto& hit : MergeTopK(answered, k)) *resp->add_hits() = std::move(hit);
  const uint64_t t_merge1 = MonoNanos();
  EmitSpan(trace_id, lucent::v1::SPAN_MERGE, t_merge0, t_merge1, 0, "{}");

  auto* timings = resp->mutable_timings();
  timings->set_embed_us((t_embed1 - t_embed0) / 1000);
  timings->set_plan_us((t_plan1 - t_plan0) / 1000);
  timings->set_fanout_us((t_fan1 - t_fan0) / 1000);
  timings->set_merge_us((t_merge1 - t_merge0) / 1000);
  timings->set_total_us((MonoNanos() - t_query0) / 1000);

  EmitSpan(trace_id, lucent::v1::SPAN_QUERY_DONE, t_query0, MonoNanos(), 0,
           nlohmann::json{
               {"coverage", std::to_string(coverage->answered()) + "/" +
                                std::to_string(coverage->probed())},
               {"k", k}}.dump());
  return grpc::Status::OK;
}

grpc::Status CoordinatorServer::GetClusterState(
    grpc::ServerContext* /*ctx*/, const lucent::v1::ClusterStateRequest* /*req*/,
    lucent::v1::ClusterState* resp) {
  *resp->mutable_shard_map() = shard_map_;
  // M3's HealthWatcher will report real health; static map == all healthy.
  for (const auto& e : shard_map_.shards()) {
    auto* nh = resp->add_nodes();
    nh->set_node_id(e.primary_node());
    nh->set_health(lucent::v1::HEALTH_HEALTHY);
  }
  return grpc::Status::OK;
}

}  // namespace lucent
