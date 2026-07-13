#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
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
#include "common/node_id.h"
#include "index/shard_storage.h"
#include "index/vector_index.h"
#include "lucent/v1/shard.grpc.pb.h"

namespace lucent {

// ShardService implementation (protocol.md §1.2, internals.md §4 fault
// semantics). Lifecycle: EMPTY -> BUILDING (first InsertBatch) -> SERVING
// (SealIndex), or LOADING -> SERVING when a sealed directory exists at
// startup. Post-seal the index is immutable; Search takes no locks on it.
class ShardServer final : public lucent::v1::ShardService::Service {
 public:
  ShardServer(Config config, NodeIdentity identity, std::string data_dir,
              EventEmitter* emitter);
  ~ShardServer() override;

  // Attempts to load a sealed directory; EMPTY if none. Call before Serve.
  void Init();

  lucent::v1::NodeState state() const { return state_.load(); }
  uint64_t doc_count() const;

  // gRPC handlers.
  grpc::Status Search(grpc::ServerContext* ctx,
                      const lucent::v1::SearchRequest* req,
                      lucent::v1::SearchResponse* resp) override;
  grpc::Status InsertBatch(
      grpc::ServerContext* ctx,
      grpc::ServerReader<lucent::v1::InsertBatchRequest>* reader,
      lucent::v1::InsertBatchResponse* resp) override;
  grpc::Status Replicate(
      grpc::ServerContext* ctx,
      grpc::ServerReaderWriter<lucent::v1::ReplicationAck,
                               lucent::v1::ReplicationBatch>* stream) override;
  grpc::Status SealIndex(grpc::ServerContext* ctx,
                         const lucent::v1::SealRequest* req,
                         lucent::v1::SealResponse* resp) override;
  grpc::Status Status(grpc::ServerContext* ctx,
                      const lucent::v1::StatusRequest* req,
                      lucent::v1::StatusResponse* resp) override;
  grpc::Status GetTraceBlob(grpc::ServerContext* ctx,
                            const lucent::v1::TraceBlobRequest* req,
                            lucent::v1::TraceBlob* resp) override;
  grpc::Status InjectFault(grpc::ServerContext* ctx,
                           const lucent::v1::FaultRequest* req,
                           lucent::v1::FaultResponse* resp) override;

  // Stats loop body, public for the driver: emits one NodeStats now.
  void EmitStats();

 private:
  // Applies any active fault at handler entry. Returns non-OK to fail the RPC
  // (DROP), or sleeps (PAUSE window / SLOW) then returns OK.
  grpc::Status ApplyFault();

  void SetState(lucent::v1::NodeState next, const std::string& reason);
  void RecordLatency(uint64_t us);

  const Config config_;
  const NodeIdentity identity_;
  const std::string data_dir_;
  EventEmitter* const emitter_;  // not owned

  std::atomic<lucent::v1::NodeState> state_{lucent::v1::NODE_STATE_EMPTY};

  // Build-phase staging (guarded by build_mu_), swapped into the immutable
  // serving set at seal/load.
  std::mutex build_mu_;
  std::vector<uint64_t> staged_ids_;
  std::vector<float> staged_vectors_;
  std::vector<DocMeta> staged_docs_;
  std::atomic<uint64_t> applied_seq_{0};  // last batch seq this node applied

  // Replication (M3-T1). When replicas==2, the primary (shard-N a) streams its
  // insert stream to the backup (shard-N b) via Replicate; the backup applies
  // and acks. Lag = primary_seq_ - backup_applied_seq_ (reported by the
  // primary). Docs staged identically to ingest — StageDocs is shared.
  void StageDocs(const lucent::v1::InsertBatchRequest& batch);  // caller holds build_mu_
  void StartReplicator();
  void ReplicatorLoop();
  const bool is_primary_;
  const bool has_backup_;
  std::atomic<uint64_t> primary_seq_{0};         // last batch accepted from ingest
  std::atomic<uint64_t> backup_applied_seq_{0};  // primary's view of backup progress
  std::mutex oplog_mu_;
  std::condition_variable oplog_cv_;
  std::vector<lucent::v1::InsertBatchRequest> oplog_;  // batches awaiting replication
  std::thread replicator_;
  std::atomic<bool> replicator_stop_{false};
  // Active replication client context, so Stop() can TryCancel a blocking
  // Read/Write on a dead backup (no deadline is set on the long-lived stream).
  std::mutex repl_ctx_mu_;
  grpc::ClientContext* repl_ctx_ = nullptr;

  // Serving set: written once (Init load or SealIndex), then read-only.
  std::unique_ptr<VectorIndex> index_;
  std::vector<DocMeta> docs_;  // row-aligned

  // Epoch staleness check (protocol.md §2): highest epoch seen wins.
  std::atomic<uint64_t> max_epoch_seen_{0};

  // FULL-trace machinery (M1-T3). Blob store: small FIFO keyed by trace_id,
  // pulled out-of-band via GetTraceBlob. Rate cap: sliding 1s window of FULL
  // grants; over the cap a query silently serves at SPANS (protocol.md §2 —
  // the cap degrades the trace, never the search).
  bool GrantFullTrace();
  void StoreBlob(lucent::v1::TraceBlob blob);
  static constexpr size_t kMaxStoredBlobs = 16;
  std::mutex trace_mu_;
  std::unordered_map<std::string, lucent::v1::TraceBlob> blobs_;
  std::deque<std::string> blob_order_;
  std::deque<uint64_t> full_grants_ns_;

  // Fault injection (internals.md §4).
  std::mutex fault_mu_;
  uint64_t pause_until_ns_ = 0;   // PAUSE: auto-clears when passed
  uint32_t slow_ms_ = 0;          // SLOW: until CLEAR
  float drop_p_ = 0.0F;           // DROP: until CLEAR
  std::mt19937 fault_rng_{12345}; // seeded (determinism rules)

  // Latency window for p50/p99 (last 1024 searches) + qps counter.
  std::mutex lat_mu_;
  std::vector<uint64_t> latencies_us_;
  size_t lat_next_ = 0;
  std::atomic<uint64_t> searches_total_{0};
  uint64_t searches_at_last_stats_ = 0;
  uint64_t last_stats_ns_ = 0;
};

}  // namespace lucent
