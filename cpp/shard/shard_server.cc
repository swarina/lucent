#include "shard/shard_server.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <thread>
#include <utility>

#include <nlohmann/json.hpp>

#include <xxhash.h>

#include "common/monotime.h"
#include "common/proc_stats.h"
#include "index/brute_force.h"
#include "index/hnsw.h"
#include "index/hnsw_io.h"
#include "shard/trace_buffer.h"

namespace lucent {

namespace {
constexpr size_t kLatencyWindow = 1024;

// Replica seed rule (data-formats.md §2.1): replicas intentionally build
// DIFFERENT graphs — same recall, slightly different result sets, surfaced
// in the UI rather than hidden.
uint64_t ReplicaSeed(uint64_t config_seed, const std::string& node_id) {
  return config_seed ^ XXH3_64bits(node_id.data(), node_id.size());
}

lucent::v1::Event StateChangeEvent(lucent::v1::NodeState from,
                                   lucent::v1::NodeState to,
                                   const std::string& reason) {
  lucent::v1::Event e;
  auto* sc = e.mutable_state();
  sc->set_from(from);
  sc->set_to(to);
  sc->set_health(lucent::v1::HEALTH_HEALTHY);
  sc->set_reason(reason);
  return e;
}
}  // namespace

ShardServer::ShardServer(Config config, NodeIdentity identity,
                         std::string data_dir, EventEmitter* emitter)
    : config_(std::move(config)),
      identity_(std::move(identity)),
      data_dir_(std::move(data_dir)),
      emitter_(emitter),
      is_primary_(identity_.replica == 'a'),
      has_backup_(is_primary_ && config_.cluster.replicas >= 2),
      latencies_us_(kLatencyWindow, 0) {
  if (has_backup_) StartReplicator();
}

ShardServer::~ShardServer() {
  replicator_stop_.store(true);
  oplog_cv_.notify_all();
  {
    std::lock_guard<std::mutex> lock(repl_ctx_mu_);
    if (repl_ctx_ != nullptr) repl_ctx_->TryCancel();  // unblock a dead-backup Read
  }
  if (replicator_.joinable()) replicator_.join();
}

void ShardServer::Init() {
  if (!std::filesystem::exists(data_dir_ + "/manifest.json")) {
    spdlog::info("{}: no sealed index at {}, starting EMPTY", identity_.id,
                 data_dir_);
    return;
  }
  SetState(lucent::v1::NODE_STATE_LOADING, "manifest found");
  LoadedShard loaded = LoadShardDir(data_dir_);  // throws on checksum mismatch
  const uint64_t n = loaded.manifest.n;
  docs_ = std::move(loaded.docs);
  if (loaded.manifest.index_type == "hnsw") {
    index_ = LoadHnswGraph(data_dir_ + "/graph.bin", loaded.manifest.dim,
                           std::move(loaded.ids), std::move(loaded.vectors));
  } else if (loaded.manifest.index_type == "bruteforce") {
    index_ = std::make_unique<BruteForceIndex>(loaded.manifest.dim,
                                               std::move(loaded.ids),
                                               std::move(loaded.vectors),
                                               /*sealed=*/true);
  } else {
    throw std::runtime_error("unknown index type '" +
                             loaded.manifest.index_type + "'");
  }
  applied_seq_.store(n);  // sealed dir == fully applied
  SetState(lucent::v1::NODE_STATE_SERVING, "loaded sealed index");
  spdlog::info("{}: serving {} docs from {}", identity_.id, n, data_dir_);
}

uint64_t ShardServer::doc_count() const {
  return index_ ? index_->Size() : 0;
}

void ShardServer::SetState(lucent::v1::NodeState next,
                           const std::string& reason) {
  const auto prev = state_.exchange(next);
  if (emitter_ != nullptr) emitter_->Emit(StateChangeEvent(prev, next, reason));
  spdlog::info("{}: {} -> {} ({})", identity_.id,
               lucent::v1::NodeState_Name(prev),
               lucent::v1::NodeState_Name(next), reason);
}

grpc::Status ShardServer::ApplyFault() {
  uint64_t pause_until = 0;
  uint32_t slow_ms = 0;
  bool drop = false;
  {
    std::lock_guard<std::mutex> lock(fault_mu_);
    pause_until = pause_until_ns_;
    slow_ms = slow_ms_;
    if (drop_p_ > 0.0F) {
      std::uniform_real_distribution<float> dist(0.0F, 1.0F);
      drop = dist(fault_rng_) < drop_p_;
    }
  }
  const uint64_t now = MonoNanos();
  if (pause_until > now) {
    std::this_thread::sleep_for(std::chrono::nanoseconds(pause_until - now));
  }
  if (slow_ms > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(slow_ms));
  }
  if (drop) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE,
                        "injected fault: drop");
  }
  return grpc::Status::OK;
}

grpc::Status ShardServer::Search(grpc::ServerContext* /*ctx*/,
                                 const lucent::v1::SearchRequest* req,
                                 lucent::v1::SearchResponse* resp) {
  if (grpc::Status f = ApplyFault(); !f.ok()) return f;

  if (state_.load() != lucent::v1::NODE_STATE_SERVING) {
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                        "shard not SERVING");
  }
  if (req->k() == 0) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "k must be > 0");
  }
  if (req->vector_size() != config_.model.dim) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "vector dim mismatch: got " +
                            std::to_string(req->vector_size()) + ", want " +
                            std::to_string(config_.model.dim));
  }
  // Epoch staleness: highest epoch seen wins (protocol.md §2).
  uint64_t seen = max_epoch_seen_.load();
  while (req->shard_map_epoch() > seen &&
         !max_epoch_seen_.compare_exchange_weak(seen, req->shard_map_epoch())) {
  }
  if (req->shard_map_epoch() < max_epoch_seen_.load()) {
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                        "stale shard map epoch " +
                            std::to_string(req->shard_map_epoch()) +
                            " < " + std::to_string(max_epoch_seen_.load()));
  }

  // FULL tier: capture per-hop records unless the rate cap says otherwise.
  // Over-cap queries silently serve at SPANS (the cap degrades the trace,
  // never the search — protocol.md §2).
  std::unique_ptr<TraceBuffer> trace;
  if (req->trace_level() == lucent::v1::TRACE_LEVEL_FULL && GrantFullTrace()) {
    trace = std::make_unique<TraceBuffer>(
        static_cast<size_t>(config_.trace.max_visits_per_query));
  }

  const uint64_t t0 = MonoNanos();
  const IndexSearchResult result =
      index_->Search(req->vector().data(), req->k(), req->ef_search(),
                     trace.get());
  const uint64_t t1 = MonoNanos();

  if (trace != nullptr) {
    // Blob assembly happens after the timed search; GetTraceBlob pulls it
    // out-of-band right after the response returns.
    StoreBlob(trace->Seal(req->trace_id(), identity_.id,
                          static_cast<uint32_t>(identity_.shard_id)));
  }

  for (const IndexHit& h : result.hits) {
    auto* hit = resp->add_hits();
    hit->set_doc_id(h.doc_id);
    hit->set_score(h.score);
    if (h.row < docs_.size()) {
      hit->set_title(docs_[h.row].title);
      hit->set_snippet(docs_[h.row].snippet);
    }
  }
  resp->set_visited(result.visited);
  resp->set_t_search_ns(t1 - t0);
  resp->set_trace_available(trace != nullptr);
  resp->set_node_id(identity_.id);

  searches_total_.fetch_add(1, std::memory_order_relaxed);
  RecordLatency((t1 - t0) / 1000);

  if (emitter_ != nullptr &&
      req->trace_level() >= lucent::v1::TRACE_LEVEL_SPANS) {
    lucent::v1::Event e;
    auto* span = e.mutable_span();
    span->set_trace_id(req->trace_id());
    span->set_kind(lucent::v1::SPAN_SHARD_SEARCH);
    span->set_t_start_ns(t0);
    span->set_t_end_ns(t1);
    span->set_shard_id(static_cast<uint32_t>(identity_.shard_id));
    span->set_detail_json(nlohmann::json{{"visited", result.visited},
                                         {"ef", req->ef_search()},
                                         {"k", req->k()}}
                              .dump());
    emitter_->Emit(std::move(e));
  }
  return grpc::Status::OK;
}

grpc::Status ShardServer::InsertBatch(
    grpc::ServerContext* /*ctx*/,
    grpc::ServerReader<lucent::v1::InsertBatchRequest>* reader,
    lucent::v1::InsertBatchResponse* resp) {
  if (grpc::Status f = ApplyFault(); !f.ok()) return f;

  const auto st = state_.load();
  if (st != lucent::v1::NODE_STATE_EMPTY &&
      st != lucent::v1::NODE_STATE_BUILDING) {
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                        "InsertBatch only in EMPTY/BUILDING (index is "
                        "immutable after seal)");
  }

  std::lock_guard<std::mutex> lock(build_mu_);
  if (state_.load() == lucent::v1::NODE_STATE_EMPTY) {
    SetState(lucent::v1::NODE_STATE_BUILDING, "first InsertBatch");
  }

  lucent::v1::InsertBatchRequest batch;
  const size_t dim = static_cast<size_t>(config_.model.dim);
  while (reader->Read(&batch)) {
    for (const auto& doc : batch.docs()) {
      if (static_cast<size_t>(doc.vector_size()) != dim) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                            "doc " + std::to_string(doc.doc_id()) +
                                " has dim " +
                                std::to_string(doc.vector_size()));
      }
    }
    StageDocs(batch);
    primary_seq_.store(batch.seq());

    // Hand the batch to the replicator (async; ingest never blocks on it).
    if (has_backup_) {
      {
        std::lock_guard<std::mutex> rl(oplog_mu_);
        oplog_.push_back(batch);
      }
      oplog_cv_.notify_one();
    }

    if (emitter_ != nullptr) {
      lucent::v1::Event e;
      auto* b = e.mutable_build();
      b->set_inserted(staged_ids_.size());
      b->set_total(0);  // total unknown during streaming ingest
      b->set_rss_bytes(CurrentRssBytes());
      emitter_->Emit(std::move(e));
    }
  }
  resp->set_applied_seq(applied_seq_.load());
  resp->set_doc_count(staged_ids_.size());
  return grpc::Status::OK;
}

void ShardServer::StageDocs(const lucent::v1::InsertBatchRequest& batch) {
  const size_t dim = static_cast<size_t>(config_.model.dim);
  for (const auto& doc : batch.docs()) {
    staged_ids_.push_back(doc.doc_id());
    const size_t old = staged_vectors_.size();
    staged_vectors_.resize(old + dim);
    std::memcpy(staged_vectors_.data() + old, doc.vector().data(),
                dim * sizeof(float));
    staged_docs_.push_back(
        DocMeta{doc.doc_id(), doc.title(), doc.snippet(), doc.category()});
  }
  applied_seq_.store(batch.seq());
}

// Backup side: apply the primary's replicated batches, ack each. Bidi stream —
// first message out is our current position so the primary can resume.
grpc::Status ShardServer::Replicate(
    grpc::ServerContext* /*ctx*/,
    grpc::ServerReaderWriter<lucent::v1::ReplicationAck,
                             lucent::v1::ReplicationBatch>* stream) {
  {
    lucent::v1::ReplicationAck ack;
    ack.set_applied_seq(applied_seq_.load());
    stream->Write(ack);
  }
  lucent::v1::ReplicationBatch rbatch;
  while (stream->Read(&rbatch)) {
    {
      std::lock_guard<std::mutex> lock(build_mu_);
      if (state_.load() == lucent::v1::NODE_STATE_EMPTY) {
        SetState(lucent::v1::NODE_STATE_BUILDING, "first replication batch");
      }
      lucent::v1::InsertBatchRequest as_insert;
      as_insert.set_seq(rbatch.seq());
      *as_insert.mutable_docs() = rbatch.docs();
      StageDocs(as_insert);
    }
    lucent::v1::ReplicationAck ack;
    ack.set_applied_seq(rbatch.seq());
    stream->Write(ack);

    if (emitter_ != nullptr) {
      lucent::v1::Event e;
      auto* b = e.mutable_build();
      b->set_inserted(applied_seq_.load());
      b->set_rss_bytes(CurrentRssBytes());
      emitter_->Emit(std::move(e));
    }
  }
  return grpc::Status::OK;
}

// Primary side: maintain a Replicate stream to the backup, resuming from its
// acked position, streaming oplog batches as ingest produces them. Reconnects
// with backoff; lag is visible as backup_applied_seq_ trails primary_seq_.
void ShardServer::ReplicatorLoop() {
  const std::string backup_addr = config_.ShardAddr(identity_.shard_id, 'b');
  while (!replicator_stop_.load()) {
    auto channel =
        grpc::CreateChannel(backup_addr, grpc::InsecureChannelCredentials());
    auto stub = lucent::v1::ShardService::NewStub(channel);
    grpc::ClientContext ctx;
    {
      std::lock_guard<std::mutex> lock(repl_ctx_mu_);
      repl_ctx_ = &ctx;
    }
    auto stream = stub->Replicate(&ctx);

    lucent::v1::ReplicationAck ack;
    if (!stream->Read(&ack)) {  // initial position
      {
        std::lock_guard<std::mutex> lock(repl_ctx_mu_);
        repl_ctx_ = nullptr;
      }
      if (!replicator_stop_.load()) std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }
    uint64_t sent_seq = ack.applied_seq();
    backup_applied_seq_.store(ack.applied_seq());

    bool alive = true;
    while (alive && !replicator_stop_.load()) {
      std::vector<lucent::v1::InsertBatchRequest> to_send;
      {
        std::unique_lock<std::mutex> lock(oplog_mu_);
        oplog_cv_.wait_for(lock, std::chrono::milliseconds(200), [&] {
          return replicator_stop_.load() ||
                 (!oplog_.empty() && oplog_.back().seq() > sent_seq);
        });
        for (const auto& b : oplog_) {
          if (b.seq() > sent_seq) to_send.push_back(b);
        }
      }
      for (const auto& b : to_send) {
        lucent::v1::ReplicationBatch rb;
        rb.set_seq(b.seq());
        *rb.mutable_docs() = b.docs();
        if (!stream->Write(rb) || !stream->Read(&ack)) {
          alive = false;
          break;
        }
        sent_seq = b.seq();
        backup_applied_seq_.store(ack.applied_seq());
      }
    }
    stream->WritesDone();
    ctx.TryCancel();
    (void)stream->Finish();
    {
      std::lock_guard<std::mutex> lock(repl_ctx_mu_);
      repl_ctx_ = nullptr;
    }
    if (!replicator_stop_.load()) {
      std::this_thread::sleep_for(std::chrono::seconds(1));  // reconnect backoff
    }
  }
}

void ShardServer::StartReplicator() {
  replicator_ = std::thread([this] { ReplicatorLoop(); });
}

grpc::Status ShardServer::SealIndex(grpc::ServerContext* /*ctx*/,
                                    const lucent::v1::SealRequest* /*req*/,
                                    lucent::v1::SealResponse* resp) {
  if (grpc::Status f = ApplyFault(); !f.ok()) return f;

  std::unique_lock<std::mutex> lock(build_mu_);
  if (state_.load() != lucent::v1::NODE_STATE_BUILDING) {
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                        "SealIndex only in BUILDING");
  }
  if (staged_ids_.empty()) {
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION,
                        "nothing staged — insert before sealing");
  }
  // Primary: drain replication so the backup holds every doc before we seal.
  if (has_backup_) {
    const uint64_t target = primary_seq_.load();
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (backup_applied_seq_.load() < target &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
  }
  const uint64_t t0 = MonoNanos();
  const uint64_t seed = ReplicaSeed(config_.index.seed, identity_.id);

  std::unique_ptr<VectorIndex> index;
  if (config_.index.type == "hnsw") {
    HnswIndex::Params params;
    params.m = config_.index.m;
    params.m0 = config_.index.m0;
    params.ef_construction = config_.index.ef_construction;
    params.seed = seed;
    auto hnsw = std::make_unique<HnswIndex>(config_.model.dim, params);
    const size_t dim = static_cast<size_t>(config_.model.dim);
    for (size_t i = 0; i < staged_ids_.size(); ++i) {
      hnsw->Add(staged_ids_[i], staged_vectors_.data() + i * dim);
      if (emitter_ != nullptr && (i + 1) % 512 == 0) {
        lucent::v1::Event e;
        auto* b = e.mutable_build();
        b->set_inserted(i + 1);
        b->set_total(staged_ids_.size());
        b->set_edge_count(hnsw->EdgeCount());
        b->set_rss_bytes(CurrentRssBytes());
        emitter_->Emit(std::move(e));
      }
    }
    hnsw->Seal();
    std::filesystem::create_directories(data_dir_);
    SaveHnswGraph(*hnsw, data_dir_ + "/graph.bin");  // before SaveShardDir:
    index = std::move(hnsw);                         // manifest checksums it
  } else {
    auto bf = std::make_unique<BruteForceIndex>(
        config_.model.dim, staged_ids_, staged_vectors_, /*sealed=*/false);
    bf->Seal();
    index = std::move(bf);
  }

  ShardManifest manifest;
  manifest.shard_id = identity_.shard_id;
  manifest.replica = std::string(1, identity_.replica);
  manifest.node_id = identity_.id;
  manifest.n = staged_ids_.size();
  manifest.dim = config_.model.dim;
  manifest.index_type = config_.index.type;
  manifest.seed = seed;
  if (config_.index.type == "hnsw") {
    manifest.m = config_.index.m;
    manifest.m0 = config_.index.m0;
    manifest.ef_construction = config_.index.ef_construction;
  }
  manifest.corpus_hash = "";  // populated once ingest carries it (M0-T7 note)
  SaveShardDir(data_dir_, manifest, staged_ids_, staged_vectors_, staged_docs_);

  docs_ = std::move(staged_docs_);
  index_ = std::move(index);
  staged_ids_ = {};
  staged_vectors_ = {};
  staged_docs_ = {};

  resp->set_doc_count(index_->Size());
  const auto* hnsw = dynamic_cast<const HnswIndex*>(index_.get());
  resp->set_edge_count(hnsw != nullptr ? hnsw->EdgeCount() : 0);
  resp->set_build_ms((MonoNanos() - t0) / 1'000'000);
  SetState(lucent::v1::NODE_STATE_SERVING, "sealed");
  lock.unlock();

  // Propagate the seal to the backup (best-effort; it built the same staged
  // docs with its own replica seed). If the backup is down it seals on its
  // own recovery path — not blocking here.
  if (has_backup_) {
    auto ch = grpc::CreateChannel(config_.ShardAddr(identity_.shard_id, 'b'),
                                  grpc::InsecureChannelCredentials());
    auto stub = lucent::v1::ShardService::NewStub(ch);
    grpc::ClientContext bctx;
    bctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
    lucent::v1::SealResponse bresp;
    const grpc::Status bs = stub->SealIndex(&bctx, lucent::v1::SealRequest{}, &bresp);
    if (!bs.ok()) {
      spdlog::warn("{}: backup seal failed: {}", identity_.id, bs.error_message());
    }
  }
  return grpc::Status::OK;
}

grpc::Status ShardServer::Status(grpc::ServerContext* /*ctx*/,
                                 const lucent::v1::StatusRequest* /*req*/,
                                 lucent::v1::StatusResponse* resp) {
  if (grpc::Status f = ApplyFault(); !f.ok()) return f;

  resp->set_node_id(identity_.id);
  resp->set_shard_id(static_cast<uint32_t>(identity_.shard_id));
  resp->set_replica(std::string(1, identity_.replica));
  resp->set_state(state_.load());
  resp->set_doc_count(doc_count());
  resp->set_rss_bytes(CurrentRssBytes());
  if (has_backup_) {
    resp->set_primary_seq(primary_seq_.load());
    resp->set_applied_seq(backup_applied_seq_.load());
  } else {
    resp->set_primary_seq(applied_seq_.load());
    resp->set_applied_seq(applied_seq_.load());
  }
  resp->set_m(static_cast<uint32_t>(config_.index.m));
  resp->set_m0(static_cast<uint32_t>(config_.index.m0));
  resp->set_ef_construction(static_cast<uint32_t>(config_.index.ef_construction));
  resp->set_seed(config_.index.seed);
  {
    std::lock_guard<std::mutex> lock(build_mu_);
    resp->set_build_progress(staged_ids_.size());
  }
  return grpc::Status::OK;
}

grpc::Status ShardServer::GetTraceBlob(grpc::ServerContext* /*ctx*/,
                                       const lucent::v1::TraceBlobRequest* req,
                                       lucent::v1::TraceBlob* resp) {
  std::lock_guard<std::mutex> lock(trace_mu_);
  auto it = blobs_.find(req->trace_id());
  if (it == blobs_.end()) {
    return grpc::Status(grpc::StatusCode::NOT_FOUND,
                        "no trace blob for this trace_id (not FULL, "
                        "rate-capped, or evicted)");
  }
  *resp = it->second;
  return grpc::Status::OK;
}

bool ShardServer::GrantFullTrace() {
  const uint64_t now = MonoNanos();
  std::lock_guard<std::mutex> lock(trace_mu_);
  while (!full_grants_ns_.empty() &&
         now - full_grants_ns_.front() > 1'000'000'000ULL) {
    full_grants_ns_.pop_front();
  }
  if (full_grants_ns_.size() >=
      static_cast<size_t>(config_.trace.full_trace_max_qps)) {
    return false;
  }
  full_grants_ns_.push_back(now);
  return true;
}

void ShardServer::StoreBlob(lucent::v1::TraceBlob blob) {
  std::lock_guard<std::mutex> lock(trace_mu_);
  const std::string key = blob.trace_id();
  if (blobs_.emplace(key, std::move(blob)).second) {
    blob_order_.push_back(key);
    while (blob_order_.size() > kMaxStoredBlobs) {
      blobs_.erase(blob_order_.front());
      blob_order_.pop_front();
    }
  }
}

grpc::Status ShardServer::InjectFault(grpc::ServerContext* /*ctx*/,
                                      const lucent::v1::FaultRequest* req,
                                      lucent::v1::FaultResponse* resp) {
  std::lock_guard<std::mutex> lock(fault_mu_);
  switch (req->fault_case()) {
    case lucent::v1::FaultRequest::kPauseMs:
      pause_until_ns_ =
          MonoNanos() + static_cast<uint64_t>(req->pause_ms()) * 1'000'000;
      resp->set_active("pause " + std::to_string(req->pause_ms()) + "ms");
      break;
    case lucent::v1::FaultRequest::kSlowMs:
      slow_ms_ = req->slow_ms();
      resp->set_active("slow +" + std::to_string(slow_ms_) + "ms");
      break;
    case lucent::v1::FaultRequest::kDropP:
      drop_p_ = req->drop_p();
      resp->set_active("drop p=" + std::to_string(drop_p_));
      break;
    case lucent::v1::FaultRequest::kClear:
      pause_until_ns_ = 0;
      slow_ms_ = 0;
      drop_p_ = 0.0F;
      resp->set_active("");
      break;
    default:
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "empty fault");
  }
  spdlog::warn("{}: fault -> {}", identity_.id,
               resp->active().empty() ? "clear" : resp->active());
  return grpc::Status::OK;
}

void ShardServer::RecordLatency(uint64_t us) {
  std::lock_guard<std::mutex> lock(lat_mu_);
  latencies_us_[lat_next_ % kLatencyWindow] = us;
  ++lat_next_;
}

void ShardServer::EmitStats() {
  if (emitter_ == nullptr) return;

  const uint64_t now = MonoNanos();
  const uint64_t total = searches_total_.load(std::memory_order_relaxed);
  float qps = 0.0F;
  if (last_stats_ns_ != 0 && now > last_stats_ns_) {
    const double dt_s = static_cast<double>(now - last_stats_ns_) / 1e9;
    qps = static_cast<float>(
        static_cast<double>(total - searches_at_last_stats_) / dt_s);
  }
  last_stats_ns_ = now;
  searches_at_last_stats_ = total;

  uint64_t p50 = 0;
  uint64_t p99 = 0;
  {
    std::lock_guard<std::mutex> lock(lat_mu_);
    const size_t count = std::min(lat_next_, kLatencyWindow);
    if (count > 0) {
      std::vector<uint64_t> window(latencies_us_.begin(),
                                   latencies_us_.begin() +
                                       static_cast<std::ptrdiff_t>(count));
      auto nth = [&](double q) {
        const size_t idx = static_cast<size_t>(q * static_cast<double>(count - 1));
        std::nth_element(window.begin(),
                         window.begin() + static_cast<std::ptrdiff_t>(idx),
                         window.end());
        return window[idx];
      };
      p50 = nth(0.50);
      p99 = nth(0.99);
    }
  }

  lucent::v1::Event e;
  auto* stats = e.mutable_stats();
  stats->set_rss_bytes(CurrentRssBytes());
  stats->set_doc_count(doc_count());
  stats->set_qps_1s(qps);
  stats->set_p50_us(p50);
  stats->set_p99_us(p99);
  // Lag = primary_seq - applied_seq. The primary reports its own seq vs the
  // backup's acked position (so its card shows replication lag); other nodes
  // report their own applied position (lag 0).
  if (has_backup_) {
    stats->set_primary_seq(primary_seq_.load());
    stats->set_applied_seq(backup_applied_seq_.load());
  } else {
    stats->set_primary_seq(applied_seq_.load());
    stats->set_applied_seq(applied_seq_.load());
  }
  stats->set_state(state_.load());
  if (const auto* hnsw = dynamic_cast<const HnswIndex*>(index_.get())) {
    stats->set_edge_count(hnsw->EdgeCount());  // "watch edges" in steady state
  }
  emitter_->Emit(std::move(e));
}

}  // namespace lucent
