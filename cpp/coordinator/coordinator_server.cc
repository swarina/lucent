#include "coordinator/coordinator_server.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <latch>
#include <memory>
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
      emitter_(emitter),
      shard_map_(std::move(shard_map)),
      rng_(std::random_device{}()) {
  embed_stub_ = lucent::v1::EmbedService::NewStub(
      grpc::CreateChannel(embed_addr, grpc::InsecureChannelCredentials()));
  // Semantic routing table (M4): load the k-means centroids so a probe subset
  // lands on the shards nearest the query, not an arbitrary prefix.
  LoadCentroids();
  // A restarted coordinator resets its epoch to the config seed; shards that
  // already advanced past it would reject every query as stale. Adopt their
  // floor before serving. (Pre-M5; when Raft owns the map this goes away.)
  ReconcileEpochFromShards();
  // Start the HealthWatcher only when there is something to fail over to
  // (replicas == 2); with a single replica per shard it would just add pings.
  if (config_.cluster.replicas >= 2) {
    health_thread_ = std::thread([this] { HealthLoop(); });
  }
}

void CoordinatorServer::ReconcileEpochFromShards() {
  std::vector<std::string> addrs;
  {
    std::lock_guard<std::mutex> lock(map_mu_);
    for (const auto& e : shard_map_.shards()) {
      if (!e.primary_addr().empty()) addrs.push_back(e.primary_addr());
      if (!e.backup_addr().empty()) addrs.push_back(e.backup_addr());
    }
  }
  uint64_t max_seen = 0;
  for (const auto& addr : addrs) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::milliseconds(300));
    lucent::v1::StatusResponse resp;
    // Unreachable shards (a fresh cluster boot) are simply skipped — they've
    // seen no epoch, so there is nothing to adopt.
    if (ShardStub(addr)->Status(&ctx, lucent::v1::StatusRequest{}, &resp).ok()) {
      max_seen = std::max(max_seen, resp.max_epoch_seen());
    }
  }
  std::lock_guard<std::mutex> lock(map_mu_);
  if (max_seen > shard_map_.epoch()) {
    spdlog::warn("epoch reconcile: adopting {} from live shards (config seed {})",
                 max_seen, shard_map_.epoch());
    shard_map_.set_epoch(max_seen);
  }
}

CoordinatorServer::~CoordinatorServer() {
  health_stop_.store(true);
  if (health_thread_.joinable()) health_thread_.join();
}

void CoordinatorServer::HealthLoop() {
  while (!health_stop_.load()) {
    HealthTick();
    // Sleep in small slices so shutdown is prompt.
    for (int i = 0; i < config_.health.heartbeat_ms / 20 &&
                    !health_stop_.load(); ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }
}

void CoordinatorServer::HealthTick() {
  // Snapshot the node list under the lock, ping outside it, then apply.
  std::vector<std::pair<std::string, std::string>> nodes;  // (node_id, addr)
  {
    std::lock_guard<std::mutex> lock(map_mu_);
    for (const auto& e : shard_map_.shards()) {
      nodes.emplace_back(e.primary_node(), e.primary_addr());
      if (!e.backup_node().empty()) nodes.emplace_back(e.backup_node(), e.backup_addr());
    }
  }
  for (const auto& [node_id, addr] : nodes) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() +
                     std::chrono::milliseconds(300));
    lucent::v1::StatusResponse resp;
    const bool alive =
        ShardStub(addr)->Status(&ctx, lucent::v1::StatusRequest{}, &resp).ok();
    std::lock_guard<std::mutex> lock(map_mu_);
    RecordHealth(node_id, alive);
  }
}

bool CoordinatorServer::IsHealthy(const std::string& node_id) const {
  auto it = health_.find(node_id);
  return it == health_.end() || it->second.state == lucent::v1::HEALTH_HEALTHY;
}

void CoordinatorServer::RecordHealth(const std::string& node_id, bool alive) {
  NodeHealth& h = health_[node_id];
  const auto prev = h.state;
  if (alive) {
    h.misses = 0;
    h.state = lucent::v1::HEALTH_HEALTHY;
    h.ever_healthy = true;
  } else {
    ++h.misses;
    if (h.misses >= config_.health.down_after_misses) {
      h.state = lucent::v1::HEALTH_DOWN;
    } else if (h.misses >= config_.health.suspect_after_misses) {
      h.state = lucent::v1::HEALTH_SUSPECT;
    }
  }
  if (h.state == prev) return;
  spdlog::info("health: {} {} (misses {})", node_id,
               lucent::v1::HealthState_Name(h.state), h.misses);
  // Only a node we've actually seen serving can "fail" over; one that never
  // came up (boot/loading) just stays DOWN in the map until it answers.
  if (h.state == lucent::v1::HEALTH_DOWN && h.ever_healthy) MaybeFailover(node_id);
}

// A primary going DOWN with a healthy backup hands the shard over: swap roles,
// bump the map epoch, emit FailoverExecuted. Caller holds map_mu_.
void CoordinatorServer::MaybeFailover(const std::string& down_node) {
  for (auto& e : *shard_map_.mutable_shards()) {
    if (e.primary_node() != down_node) continue;
    if (e.backup_node().empty() || !IsHealthy(e.backup_node())) return;
    const std::string old_primary = e.primary_node();
    const std::string old_paddr = e.primary_addr();
    e.set_primary_node(e.backup_node());
    e.set_primary_addr(e.backup_addr());
    e.set_backup_node(old_primary);
    e.set_backup_addr(old_paddr);
    shard_map_.set_epoch(shard_map_.epoch() + 1);
    spdlog::warn("FAILOVER shard {}: {} -> {} (epoch {})", e.shard_id(),
                 old_primary, e.primary_node(), shard_map_.epoch());
    if (emitter_ != nullptr) {
      lucent::v1::Event ev;
      auto* f = ev.mutable_failover();
      f->set_shard_id(e.shard_id());
      f->set_old_primary(old_primary);
      f->set_new_primary(e.primary_node());
      f->set_new_epoch(shard_map_.epoch());
      emitter_->Emit(std::move(ev));
    }
    return;
  }
}

void CoordinatorServer::SetHealthForTest(const std::string& node_id,
                                         lucent::v1::HealthState state) {
  std::lock_guard<std::mutex> lock(map_mu_);
  NodeHealth& h = health_[node_id];
  h.state = state;
  h.misses = state == lucent::v1::HEALTH_DOWN      ? config_.health.down_after_misses
             : state == lucent::v1::HEALTH_SUSPECT ? config_.health.suspect_after_misses
                                                   : 0;
  // The seam models a node that has been in service; forcing it DOWN therefore
  // represents a real failure (which may promote a backup), not a boot no-show.
  h.ever_healthy = true;
  if (state == lucent::v1::HEALTH_DOWN) MaybeFailover(node_id);
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

float CoordinatorServer::CentroidScore(uint32_t shard_id,
                                       const std::vector<float>& qvec) const {
  if (shard_id >= centroids_.size()) return 0.0F;
  const std::vector<float>& c = centroids_[shard_id];
  const size_t d = std::min(c.size(), qvec.size());
  float s = 0.0F;
  for (size_t i = 0; i < d; ++i) s += c[i] * qvec[i];
  return s;
}

// Health-aware planning: pick the probe subset, then a HEALTHY, SERVING replica
// per probed shard (round-robin across {primary, backup} for read balancing).
// The subset order is the whole M4 point: hash → lowest shard_ids (arbitrary);
// semantic → shards whose centroid is nearest the query (top-P by dot product),
// so a partial probe lands on the *right* shards. No-healthy-replica → uncovered.
QueryPlan CoordinatorServer::PlanWithHealth(uint32_t probe,
                                            const std::vector<float>& qvec) {
  std::lock_guard<std::mutex> lock(map_mu_);
  QueryPlan plan;
  plan.epoch = shard_map_.epoch();

  std::vector<const lucent::v1::ShardMapEntry*> entries;
  for (const auto& e : shard_map_.shards()) entries.push_back(&e);
  std::sort(entries.begin(), entries.end(),
            [](const auto* a, const auto* b) { return a->shard_id() < b->shard_id(); });

  // Probe order over `entries`. Default is the shard_id order (hash); under
  // semantic routing, stable-sort by descending centroid similarity so the
  // shard_id order breaks ties (deterministic).
  std::vector<size_t> order(entries.size());
  for (size_t i = 0; i < entries.size(); ++i) order[i] = i;
  if (semantic_routing_ && !qvec.empty()) {
    std::vector<float> score(entries.size());
    for (size_t i = 0; i < entries.size(); ++i)
      score[i] = CentroidScore(entries[i]->shard_id(), qvec);
    std::stable_sort(order.begin(), order.end(),
                     [&](size_t a, size_t b) { return score[a] > score[b]; });
  }

  // Advance the round-robin cursor once PER QUERY (not per shard): a per-shard
  // increment would move by shard_count each query, preserving parity — so with
  // 2 replicas every shard would keep picking the *same* replica forever. One
  // per-query step + a per-shard offset makes each shard genuinely alternate.
  const uint32_t base = rr_++;
  const size_t want = (probe == 0 || probe >= entries.size()) ? entries.size()
                                                              : probe;
  for (size_t rank = 0; rank < order.size(); ++rank) {
    const auto* e = entries[order[rank]];
    if (rank >= want) {
      plan.unprobed.push_back(e->shard_id());
      continue;
    }
    // Candidate replicas that are healthy (backup may be absent/empty).
    std::vector<std::pair<std::string, std::string>> reps;  // (node, addr)
    if (IsHealthy(e->primary_node())) reps.emplace_back(e->primary_node(), e->primary_addr());
    if (!e->backup_node().empty() && IsHealthy(e->backup_node())) {
      reps.emplace_back(e->backup_node(), e->backup_addr());
    }
    if (reps.empty()) {
      plan.uncovered.push_back(e->shard_id());
      continue;
    }
    const auto& r = reps[(base + e->shard_id()) % reps.size()];
    plan.probe.push_back(PlannedShard{e->shard_id(), r.first, r.second});
  }
  return plan;
}

void CoordinatorServer::LoadCentroids() {
  if (config_.cluster.partitioning != "semantic") return;
  const std::string path = config_.paths.data + "/centroids.f32";
  std::ifstream f(path, std::ios::binary);
  if (!f) {
    spdlog::info("semantic routing: no centroids at {} — routing all shards", path);
    return;
  }
  const size_t k = static_cast<size_t>(config_.cluster.shards);
  const size_t dim = static_cast<size_t>(config_.model.dim);
  std::vector<float> buf(k * dim);
  f.read(reinterpret_cast<char*>(buf.data()),
         static_cast<std::streamsize>(buf.size() * sizeof(float)));
  if (static_cast<size_t>(f.gcount()) != buf.size() * sizeof(float)) {
    spdlog::warn("semantic routing: {} is {}B, expected {}x{} floats — routing "
                 "all shards", path, f.gcount(), k, dim);
    return;
  }
  centroids_.assign(k, {});
  for (size_t s = 0; s < k; ++s) {
    centroids_[s].assign(buf.begin() + static_cast<long>(s * dim),
                         buf.begin() + static_cast<long>((s + 1) * dim));
  }
  semantic_routing_ = true;
  spdlog::info("semantic routing: loaded {} centroids (dim {})", k, dim);
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

  // --- Plan (health-aware replica selection; centroid routing if semantic). ---
  const uint64_t t_plan0 = MonoNanos();
  const std::vector<float> qvec(eresp.vectors().begin(), eresp.vectors().end());
  const QueryPlan plan = PlanWithHealth(req->probe(), qvec);
  const uint64_t t_plan1 = MonoNanos();
  {
    nlohmann::json shards = nlohmann::json::array();
    for (const auto& p : plan.probe) shards.push_back(p.shard_id);
    EmitSpan(trace_id, lucent::v1::SPAN_PLAN, t_plan0, t_plan1, 0,
             nlohmann::json{{"probe", plan.probe.size()},
                            {"shards", shards},
                            {"epoch", plan.epoch}}.dump());
  }

  // --- Fan-out (gRPC async callback API; per-shard deadline; failures become
  // coverage). All N calls are issued without blocking a thread each; the
  // per-shard deadline guarantees every callback fires, so a latch of N is a
  // sufficient barrier (no separate overall timeout needed). ---
  const uint64_t t_fan0 = MonoNanos();
  struct Call {
    PlannedShard target;
    grpc::ClientContext ctx;
    lucent::v1::SearchRequest req;
    lucent::v1::SearchResponse resp;
    grpc::Status status;
    uint64_t t0 = 0, t1 = 0;
  };

  auto fill = [&](Call& c) {
    c.req.set_trace_id(trace_id);
    *c.req.mutable_vector() = eresp.vectors();
    c.req.set_k(k);
    c.req.set_ef_search(ef);
    c.req.set_trace_level(req->trace_level());
    c.req.set_shard_map_epoch(plan.epoch);
    c.ctx.set_deadline(DeadlineIn(config_.timeouts_ms.shard_search));
    c.t0 = MonoNanos();
  };

  std::vector<std::unique_ptr<Call>> calls;
  calls.reserve(plan.probe.size());
  std::latch done(static_cast<std::ptrdiff_t>(plan.probe.size()));
  for (const PlannedShard& target : plan.probe) {
    auto c = std::make_unique<Call>();
    c->target = target;
    fill(*c);
    Call* cp = c.get();
    ShardStub(target.addr)->async()->Search(
        &cp->ctx, &cp->req, &cp->resp, [cp, &done](grpc::Status s) {
          cp->status = std::move(s);
          cp->t1 = MonoNanos();
          done.count_down();
        });
    calls.push_back(std::move(c));
  }
  done.wait();

  std::vector<ShardResult> answered;
  auto* coverage = resp->mutable_coverage();
  // "probed" = shards we intended to serve this query: reachable replicas plus
  // shards whose every replica is down (those are counted below as missing).
  coverage->set_probed(
      static_cast<uint32_t>(plan.probe.size() + plan.uncovered.size()));
  for (uint32_t s : plan.unprobed) coverage->add_unprobed_shards(s);
  // Shards with no healthy replica never got an RPC — they're missing coverage.
  for (uint32_t s : plan.uncovered) {
    coverage->add_missing_shards(s);
    spdlog::warn("shard {}: no healthy replica (failover exhausted)", s);
  }
  for (auto& c : calls) {
    // Epoch-mismatch retry (protocol.md §2): a shard that knows a higher map
    // epoch rejects with FAILED_PRECONDITION; refresh our epoch and retry once
    // synchronously. Static map (M0–M2) → same epoch, so this is a no-op path
    // that M3's live shard map plugs into; correctness comes for free now.
    if (c->status.error_code() == grpc::StatusCode::FAILED_PRECONDITION) {
      lucent::v1::SearchRequest rreq = c->req;
      {
        std::lock_guard<std::mutex> lock(map_mu_);
        rreq.set_shard_map_epoch(shard_map_.epoch());
      }
      grpc::ClientContext rctx;
      rctx.set_deadline(DeadlineIn(config_.timeouts_ms.shard_search));
      lucent::v1::SearchResponse rresp;
      const grpc::Status rs =
          ShardStub(c->target.addr)->Search(&rctx, rreq, &rresp);
      c->t1 = MonoNanos();
      if (rs.ok()) {
        c->status = rs;
        c->resp = std::move(rresp);
      }
    }

    EmitSpan(trace_id, lucent::v1::SPAN_SHARD_RPC, c->t0, c->t1,
             c->target.shard_id,
             nlohmann::json{{"status", StatusLabel(c->status.error_code())},
                            {"node", c->target.node_id}}.dump());
    if (c->status.ok()) {
      resp->set_visited_total(resp->visited_total() + c->resp.visited());
      answered.push_back(ShardResult{c->target.shard_id, std::move(c->resp)});
    } else {
      coverage->add_missing_shards(c->target.shard_id);
      spdlog::warn("shard {} ({}): {}", c->target.shard_id, c->target.node_id,
                   c->status.error_message());
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
  std::lock_guard<std::mutex> lock(map_mu_);
  *resp->mutable_shard_map() = shard_map_;
  // Report every distinct node (primary + backup) with its live HealthWatcher
  // verdict. Unseen nodes default to HEALTHY (optimistic, matches IsHealthy).
  std::unordered_map<std::string, lucent::v1::HealthState> seen;
  auto add = [&](const std::string& node_id) {
    if (node_id.empty() || seen.count(node_id) != 0) return;
    auto it = health_.find(node_id);
    const lucent::v1::HealthState h =
        it == health_.end() ? lucent::v1::HEALTH_HEALTHY : it->second.state;
    seen.emplace(node_id, h);
    auto* nh = resp->add_nodes();
    nh->set_node_id(node_id);
    nh->set_health(h);
  };
  for (const auto& e : shard_map_.shards()) {
    add(e.primary_node());
    add(e.backup_node());
  }
  return grpc::Status::OK;
}

grpc::Status CoordinatorServer::AddReplica(
    grpc::ServerContext* /*ctx*/, const lucent::v1::AddReplicaRequest* req,
    lucent::v1::AddReplicaResponse* resp) {
  if (req->node_id().empty() || req->addr().empty()) {
    resp->set_error("node_id and addr are required");
    return grpc::Status::OK;
  }
  std::lock_guard<std::mutex> lock(map_mu_);
  lucent::v1::ShardMapEntry* target = nullptr;
  for (auto& e : *shard_map_.mutable_shards()) {
    if (e.shard_id() == req->shard_id()) target = &e;
    // Guard against a node_id already live anywhere in the map.
    if (e.primary_node() == req->node_id() || e.backup_node() == req->node_id()) {
      resp->set_error("node " + req->node_id() + " is already in the map");
      return grpc::Status::OK;
    }
  }
  if (target == nullptr) {
    resp->set_error("no such shard " + std::to_string(req->shard_id()));
    return grpc::Status::OK;
  }
  if (!target->backup_node().empty() && IsHealthy(target->backup_node())) {
    resp->set_error("shard " + std::to_string(req->shard_id()) +
                    " already has a healthy backup");
    return grpc::Status::OK;
  }
  target->set_backup_node(req->node_id());
  target->set_backup_addr(req->addr());
  target->set_backup_state(lucent::v1::NODE_STATE_SERVING);
  // Seed optimistic health; the HealthWatcher's next ping confirms/demotes.
  health_[req->node_id()] = NodeHealth{lucent::v1::HEALTH_HEALTHY, 0};
  shard_map_.set_epoch(shard_map_.epoch() + 1);
  resp->set_epoch(shard_map_.epoch());
  spdlog::info("ADD REPLICA shard {}: backup <- {} ({}) (epoch {})",
               req->shard_id(), req->node_id(), req->addr(), shard_map_.epoch());
  return grpc::Status::OK;
}

grpc::Status CoordinatorServer::RemoveReplica(
    grpc::ServerContext* /*ctx*/, const lucent::v1::RemoveReplicaRequest* req,
    lucent::v1::RemoveReplicaResponse* resp) {
  if (req->node_id().empty()) {
    resp->set_error("node_id is required");
    return grpc::Status::OK;
  }
  std::lock_guard<std::mutex> lock(map_mu_);
  for (auto& e : *shard_map_.mutable_shards()) {
    if (e.backup_node() == req->node_id()) {
      e.clear_backup_node();
      e.clear_backup_addr();
      e.set_backup_state(lucent::v1::NODE_STATE_UNSPECIFIED);
      health_.erase(req->node_id());
      shard_map_.set_epoch(shard_map_.epoch() + 1);
      resp->set_epoch(shard_map_.epoch());
      spdlog::info("REMOVE REPLICA shard {}: backup {} drained (epoch {})",
                   e.shard_id(), req->node_id(), shard_map_.epoch());
      return grpc::Status::OK;
    }
    if (e.primary_node() == req->node_id()) {
      // Removing a primary would strand the shard unless a healthy backup can
      // take over first — refuse and let the operator fail over.
      resp->set_error("node " + req->node_id() + " is the primary of shard " +
                      std::to_string(e.shard_id()) + "; fail over before removing");
      return grpc::Status::OK;
    }
  }
  resp->set_error("node " + req->node_id() + " is not in the map");
  return grpc::Status::OK;
}

}  // namespace lucent
