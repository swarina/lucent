#include "shard/shard_server.h"

#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include <cmath>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/event_emitter.h"
#include "common/node_id.h"
#include "lucent/v1/shard.grpc.pb.h"

namespace lucent {
namespace {

namespace fs = std::filesystem;
namespace pb = lucent::v1;

Config TestConfig() {
  Config c = Config::Load(std::string(LUCENT_REPO_ROOT) + "/cluster.yaml");
  c.model.dim = 4;          // tiny vectors for the test
  c.cluster.replicas = 1;   // single-shard tests: no backup replication
  return c;
}

// Real gRPC server on a kernel-assigned port + real channel — exercises the
// actual wire path, not just the handler methods.
class ShardServerTest : public testing::Test {
 protected:
  void SetUp() override {
    data_dir_ = testing::TempDir() + "lucent_shard_t5_" +
                testing::UnitTest::GetInstance()->current_test_info()->name();
    fs::remove_all(data_dir_);
    StartServer();
  }

  void TearDown() override {
    StopServer();
    fs::remove_all(data_dir_);
  }

  void StartServer() {
    emitter_ = std::make_unique<EventEmitter>(
        "shard-0a", [this](std::vector<pb::Event>&& batch) {
          std::lock_guard<std::mutex> lock(events_mu_);
          for (auto& e : batch) events_.push_back(std::move(e));
        });
    auto identity = NodeIdentity::Parse("shard-0a");
    service_ = std::make_unique<ShardServer>(TestConfig(), *identity, data_dir_,
                                             emitter_.get());
    service_->Init();

    int port = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                             &port);
    builder.RegisterService(service_.get());
    server_ = builder.BuildAndStart();
    ASSERT_TRUE(server_);
    stub_ = pb::ShardService::NewStub(grpc::CreateChannel(
        "127.0.0.1:" + std::to_string(port),
        grpc::InsecureChannelCredentials()));
  }

  void StopServer() {
    if (server_) server_->Shutdown();
    server_.reset();
    stub_.reset();
    service_.reset();
    if (emitter_) emitter_->Stop();
  }

  // Streams `docs` as one InsertBatch RPC (single batch, seq=1), then seals.
  void InsertAndSeal(const std::vector<std::pair<uint64_t, std::vector<float>>>& docs) {
    {
      grpc::ClientContext ctx;
      pb::InsertBatchResponse resp;
      auto writer = stub_->InsertBatch(&ctx, &resp);
      pb::InsertBatchRequest batch;
      batch.set_seq(1);
      for (const auto& [id, vec] : docs) {
        auto* d = batch.add_docs();
        d->set_doc_id(id);
        for (float v : vec) d->add_vector(v);
        d->set_title("title-" + std::to_string(id));
        d->set_snippet("snippet-" + std::to_string(id));
        d->set_category("cs.LG");
      }
      ASSERT_TRUE(writer->Write(batch));
      writer->WritesDone();
      ASSERT_TRUE(writer->Finish().ok());
      EXPECT_EQ(resp.doc_count(), docs.size());
      EXPECT_EQ(resp.applied_seq(), 1u);
    }
    grpc::ClientContext ctx;
    pb::SealResponse resp;
    ASSERT_TRUE(stub_->SealIndex(&ctx, pb::SealRequest{}, &resp).ok());
    EXPECT_EQ(resp.doc_count(), docs.size());
  }

  pb::StatusResponse GetStatus() {
    grpc::ClientContext ctx;
    pb::StatusResponse resp;
    EXPECT_TRUE(stub_->Status(&ctx, pb::StatusRequest{}, &resp).ok());
    return resp;
  }

  grpc::Status DoSearch(const std::vector<float>& query, uint32_t k,
                        uint64_t epoch, pb::SearchResponse* resp) {
    grpc::ClientContext ctx;
    pb::SearchRequest req;
    req.set_trace_id("0123456789abcdef");
    for (float v : query) req.add_vector(v);
    req.set_k(k);
    req.set_ef_search(10);
    req.set_trace_level(pb::TRACE_LEVEL_SPANS);
    req.set_shard_map_epoch(epoch);
    return stub_->Search(&ctx, req, resp);
  }

  static std::vector<std::pair<uint64_t, std::vector<float>>> UnitDocs() {
    return {{10, {1, 0, 0, 0}}, {20, {0, 1, 0, 0}}, {30, {0, 0, 1, 0}},
            {40, {0, 0, 0, 1}},
            {50, {0.7071F, 0.7071F, 0, 0}}};
  }

  std::string data_dir_;
  std::mutex events_mu_;
  std::vector<pb::Event> events_;
  std::unique_ptr<EventEmitter> emitter_;
  std::unique_ptr<ShardServer> service_;
  std::unique_ptr<grpc::Server> server_;
  std::unique_ptr<pb::ShardService::Stub> stub_;
};

TEST_F(ShardServerTest, LifecycleInsertSealSearch) {
  EXPECT_EQ(GetStatus().state(), pb::NODE_STATE_EMPTY);
  InsertAndSeal(UnitDocs());
  EXPECT_EQ(GetStatus().state(), pb::NODE_STATE_SERVING);
  EXPECT_EQ(GetStatus().doc_count(), 5u);

  pb::SearchResponse resp;
  ASSERT_TRUE(DoSearch({1, 0, 0, 0}, 3, 1, &resp).ok());
  ASSERT_EQ(resp.hits_size(), 3);
  EXPECT_EQ(resp.hits(0).doc_id(), 10u);            // exact match first
  EXPECT_NEAR(resp.hits(0).score(), 1.0F, 1e-5F);
  EXPECT_EQ(resp.hits(1).doc_id(), 50u);            // 45° neighbor second
  EXPECT_EQ(resp.hits(0).title(), "title-10");      // snippet hydration
  EXPECT_EQ(resp.hits(0).snippet(), "snippet-10");
  EXPECT_GE(resp.visited(), 1u);  // hnsw: entry + beam, <= n
  EXPECT_LE(resp.visited(), 5u);
  EXPECT_EQ(resp.node_id(), "shard-0a");
  EXPECT_FALSE(resp.trace_available());

  // SPANS event emitted with the trace id.
  emitter_->Flush();
  std::lock_guard<std::mutex> lock(events_mu_);
  bool saw_span = false;
  for (const auto& e : events_) {
    if (e.has_span() && e.span().kind() == pb::SPAN_SHARD_SEARCH) {
      saw_span = true;
      EXPECT_EQ(e.span().trace_id(), "0123456789abcdef");
      EXPECT_GT(e.span().t_end_ns(), e.span().t_start_ns());
    }
  }
  EXPECT_TRUE(saw_span);
}

TEST_F(ShardServerTest, SearchGuards) {
  pb::SearchResponse resp;
  // Not serving yet.
  EXPECT_EQ(DoSearch({1, 0, 0, 0}, 3, 1, &resp).error_code(),
            grpc::StatusCode::FAILED_PRECONDITION);
  InsertAndSeal(UnitDocs());
  // Bad dim.
  EXPECT_EQ(DoSearch({1, 0}, 3, 1, &resp).error_code(),
            grpc::StatusCode::INVALID_ARGUMENT);
  // k = 0.
  EXPECT_EQ(DoSearch({1, 0, 0, 0}, 0, 1, &resp).error_code(),
            grpc::StatusCode::INVALID_ARGUMENT);
  // Insert after seal is rejected (immutable index).
  grpc::ClientContext ctx;
  pb::InsertBatchResponse ir;
  auto writer = stub_->InsertBatch(&ctx, &ir);
  writer->WritesDone();
  EXPECT_EQ(writer->Finish().error_code(),
            grpc::StatusCode::FAILED_PRECONDITION);
}

TEST_F(ShardServerTest, StaleEpochRejected) {
  InsertAndSeal(UnitDocs());
  pb::SearchResponse resp;
  ASSERT_TRUE(DoSearch({1, 0, 0, 0}, 1, /*epoch=*/5, &resp).ok());
  // Older epoch now rejected, message carries the known epoch.
  grpc::Status st = DoSearch({1, 0, 0, 0}, 1, /*epoch=*/3, &resp);
  EXPECT_EQ(st.error_code(), grpc::StatusCode::FAILED_PRECONDITION);
  EXPECT_NE(st.error_message().find("5"), std::string::npos);
  // Same epoch fine.
  EXPECT_TRUE(DoSearch({1, 0, 0, 0}, 1, 5, &resp).ok());
}

TEST_F(ShardServerTest, FaultInjection) {
  InsertAndSeal(UnitDocs());
  pb::SearchResponse resp;

  auto inject = [&](pb::FaultRequest req) {
    grpc::ClientContext ctx;
    pb::FaultResponse fr;
    ASSERT_TRUE(stub_->InjectFault(&ctx, req, &fr).ok());
  };

  // DROP p=1.0: every search fails UNAVAILABLE.
  pb::FaultRequest drop;
  drop.set_drop_p(1.0F);
  inject(drop);
  EXPECT_EQ(DoSearch({1, 0, 0, 0}, 1, 1, &resp).error_code(),
            grpc::StatusCode::UNAVAILABLE);

  // CLEAR restores service.
  pb::FaultRequest clear;
  clear.set_clear(true);
  inject(clear);
  EXPECT_TRUE(DoSearch({1, 0, 0, 0}, 1, 1, &resp).ok());

  // SLOW adds measurable latency.
  pb::FaultRequest slow;
  slow.set_slow_ms(120);
  inject(slow);
  const auto t0 = std::chrono::steady_clock::now();
  EXPECT_TRUE(DoSearch({1, 0, 0, 0}, 1, 1, &resp).ok());
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - t0)
                      .count();
  EXPECT_GE(ms, 100);
  inject(clear);
}

TEST_F(ShardServerTest, FullTraceCapturedAndFetchable) {
  InsertAndSeal(UnitDocs());
  grpc::ClientContext ctx;
  pb::SearchRequest req;
  req.set_trace_id("full-trace-000001");
  for (float v : {1.0F, 0.0F, 0.0F, 0.0F}) req.add_vector(v);
  req.set_k(3);
  req.set_ef_search(10);
  req.set_trace_level(pb::TRACE_LEVEL_FULL);
  req.set_shard_map_epoch(1);
  pb::SearchResponse resp;
  ASSERT_TRUE(stub_->Search(&ctx, req, &resp).ok());
  EXPECT_TRUE(resp.trace_available());

  grpc::ClientContext bctx;
  pb::TraceBlobRequest breq;
  breq.set_trace_id("full-trace-000001");
  pb::TraceBlob blob;
  ASSERT_TRUE(stub_->GetTraceBlob(&bctx, breq, &blob).ok());
  EXPECT_EQ(blob.node_id(), "shard-0a");
  EXPECT_GT(blob.t_start_mono_ns(), 0u);
  const int v = blob.node_size();
  ASSERT_GT(v, 0);
  ASSERT_EQ(blob.parent_size(), v);      // parallel arrays, equal length
  ASSERT_EQ(blob.dist_size(), v);
  ASSERT_EQ(blob.t_off_us_size(), v);
  ASSERT_EQ(blob.meta_size(), v);
  EXPECT_EQ(blob.dropped(), 0u);
  int results = 0;
  for (int i = 0; i < v; ++i) {
    const uint32_t kind = blob.meta(i) & 0x7U;
    EXPECT_LE(kind, 4u);                 // ENTRY..RESULT only
    if (kind == 4) ++results;
    EXPECT_LT(blob.node(i), 5u);         // rows in range
  }
  EXPECT_EQ(results, resp.hits_size());  // RESULT marks == returned hits

  // SPANS-level search leaves no blob.
  grpc::ClientContext ctx2;
  req.set_trace_id("spans-only-000002");
  req.set_trace_level(pb::TRACE_LEVEL_SPANS);
  pb::SearchResponse resp2;
  ASSERT_TRUE(stub_->Search(&ctx2, req, &resp2).ok());
  EXPECT_FALSE(resp2.trace_available());
  grpc::ClientContext bctx2;
  breq.set_trace_id("spans-only-000002");
  pb::TraceBlob blob2;
  EXPECT_EQ(stub_->GetTraceBlob(&bctx2, breq, &blob2).error_code(),
            grpc::StatusCode::NOT_FOUND);
}

TEST_F(ShardServerTest, FullTraceRateCapDegradesNotFails) {
  InsertAndSeal(UnitDocs());
  // Repo config caps FULL at 5/s; fire 9 quickly: all succeed, only 5 traced.
  int available = 0;
  for (int i = 0; i < 9; ++i) {
    grpc::ClientContext ctx;
    pb::SearchRequest req;
    req.set_trace_id("cap-test-" + std::to_string(i));
    for (float v : {1.0F, 0.0F, 0.0F, 0.0F}) req.add_vector(v);
    req.set_k(1);
    req.set_ef_search(10);
    req.set_trace_level(pb::TRACE_LEVEL_FULL);
    req.set_shard_map_epoch(1);
    pb::SearchResponse resp;
    ASSERT_TRUE(stub_->Search(&ctx, req, &resp).ok()) << i;
    if (resp.trace_available()) ++available;
  }
  EXPECT_EQ(available, 5);  // trace.full_trace_max_qps in cluster.yaml
}

TEST_F(ShardServerTest, RestartReloadsSealedIndex) {
  InsertAndSeal(UnitDocs());
  StopServer();

  // Fresh process-equivalent: new service over the same data dir.
  StartServer();
  EXPECT_EQ(GetStatus().state(), pb::NODE_STATE_SERVING);
  EXPECT_EQ(GetStatus().doc_count(), 5u);
  pb::SearchResponse resp;
  ASSERT_TRUE(DoSearch({0, 1, 0, 0}, 1, 1, &resp).ok());
  EXPECT_EQ(resp.hits(0).doc_id(), 20u);
  EXPECT_EQ(resp.hits(0).title(), "title-20");  // docs reloaded too
}

}  // namespace
}  // namespace lucent
