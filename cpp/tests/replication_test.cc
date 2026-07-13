#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>
#include <vector>

#include "common/config.h"
#include "common/node_id.h"
#include "lucent/v1/shard.grpc.pb.h"
#include "shard/shard_server.h"

namespace lucent {
namespace {

namespace fs = std::filesystem;
namespace pb = lucent::v1;

// Two real ShardServers — a primary (shard-0a) that replicates to a backup
// (shard-0b) on fixed ports so the primary's replicator can dial it.
class ReplicationTest : public testing::Test {
 protected:
  Config MakeConfig() {
    Config c = Config::Load(std::string(LUCENT_REPO_ROOT) + "/cluster.yaml");
    c.model.dim = 4;
    c.cluster.replicas = 2;
    c.cluster.shards = 1;
    c.ports.shard_base = kBase;  // primary 0a -> kBase, backup 0b -> kBase+1
    return c;
  }

  void SetUp() override {
    dirA_ = testing::TempDir() + "lucent_repl_a";
    dirB_ = testing::TempDir() + "lucent_repl_b";
    fs::remove_all(dirA_);
    fs::remove_all(dirB_);

    const Config cfg = MakeConfig();
    backup_ = std::make_unique<ShardServer>(cfg, *NodeIdentity::Parse("shard-0b"),
                                            dirB_, nullptr);
    backup_srv_ = Serve(backup_.get(), cfg.ShardPort(0, 'b'));
    // Primary starts its replicator in the ctor → dials the backup above.
    primary_ = std::make_unique<ShardServer>(cfg, *NodeIdentity::Parse("shard-0a"),
                                             dirA_, nullptr);
    primary_srv_ = Serve(primary_.get(), cfg.ShardPort(0, 'a'));

    stubA_ = pb::ShardService::NewStub(grpc::CreateChannel(
        "127.0.0.1:" + std::to_string(cfg.ShardPort(0, 'a')),
        grpc::InsecureChannelCredentials()));
    stubB_ = pb::ShardService::NewStub(grpc::CreateChannel(
        "127.0.0.1:" + std::to_string(cfg.ShardPort(0, 'b')),
        grpc::InsecureChannelCredentials()));
  }

  void TearDown() override {
    // Order matters: stop the primary (its dtor stops the replicator, closing
    // the Replicate stream) BEFORE shutting the backup server — otherwise the
    // backup's Shutdown blocks on the still-open Replicate handler.
    if (primary_srv_) primary_srv_->Shutdown();
    primary_.reset();
    if (backup_srv_) backup_srv_->Shutdown();
    backup_.reset();
    fs::remove_all(dirA_);
    fs::remove_all(dirB_);
  }

  static std::unique_ptr<grpc::Server> Serve(grpc::Service* svc, int port) {
    grpc::ServerBuilder b;
    b.AddListeningPort("127.0.0.1:" + std::to_string(port),
                       grpc::InsecureServerCredentials());
    b.RegisterService(svc);
    return b.BuildAndStart();
  }

  void InsertToPrimary(int n) {
    grpc::ClientContext ctx;
    pb::InsertBatchResponse resp;
    auto w = stubA_->InsertBatch(&ctx, &resp);
    pb::InsertBatchRequest batch;
    batch.set_seq(1);
    for (int i = 0; i < n; ++i) {
      auto* d = batch.add_docs();
      d->set_doc_id(100 + static_cast<uint64_t>(i));
      for (int k = 0; k < 4; ++k) d->add_vector(k == (i % 4) ? 1.0F : 0.0F);
      d->set_title("t" + std::to_string(i));
      d->set_snippet("s" + std::to_string(i));
      d->set_category("cs.LG");
    }
    ASSERT_TRUE(w->Write(batch));
    w->WritesDone();
    ASSERT_TRUE(w->Finish().ok());
  }

  pb::StatusResponse StatusOf(pb::ShardService::Stub* stub) {
    grpc::ClientContext ctx;
    pb::StatusResponse r;
    EXPECT_TRUE(stub->Status(&ctx, pb::StatusRequest{}, &r).ok());
    return r;
  }

  static constexpr int kBase = 7700;
  std::string dirA_, dirB_;
  std::unique_ptr<ShardServer> primary_, backup_;
  std::unique_ptr<grpc::Server> primary_srv_, backup_srv_;
  std::unique_ptr<pb::ShardService::Stub> stubA_, stubB_;
};

TEST_F(ReplicationTest, BackupReceivesAndServesReplicatedDocs) {
  InsertToPrimary(20);

  // Replication is async — wait for the backup to catch up (lag → 0).
  bool caught_up = false;
  for (int i = 0; i < 100 && !caught_up; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    const auto ps = StatusOf(stubA_.get());
    // primary reports primary_seq vs backup applied_seq
    if (ps.primary_seq() > 0 && ps.applied_seq() >= ps.primary_seq()) caught_up = true;
  }
  ASSERT_TRUE(caught_up) << "backup never caught up to the primary";

  // Backup has staged the 20 docs (BUILDING) before any seal.
  const auto bstatus = StatusOf(stubB_.get());
  EXPECT_EQ(bstatus.build_progress(), 20u);

  // Seal the primary → it drains + seals the backup too.
  {
    grpc::ClientContext ctx;
    pb::SealResponse resp;
    ASSERT_TRUE(stubA_->SealIndex(&ctx, pb::SealRequest{}, &resp).ok());
    EXPECT_EQ(resp.doc_count(), 20u);
  }

  // Both replicas now SERVING with all 20 docs; both answer the same query
  // (possibly slightly different order — different replica seeds — but same
  // doc set at this scale/ef).
  EXPECT_EQ(StatusOf(stubA_.get()).state(), pb::NODE_STATE_SERVING);
  // backup seal is best-effort/async from the primary's call; allow a beat.
  bool backup_serving = false;
  for (int i = 0; i < 60 && !backup_serving; ++i) {
    if (StatusOf(stubB_.get()).state() == pb::NODE_STATE_SERVING) backup_serving = true;
    else std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  ASSERT_TRUE(backup_serving);
  EXPECT_EQ(StatusOf(stubB_.get()).doc_count(), 20u);

  auto search = [&](pb::ShardService::Stub* stub) {
    grpc::ClientContext ctx;
    pb::SearchRequest req;
    req.set_trace_id("repl-query-00000");
    for (float v : {1.0F, 0.0F, 0.0F, 0.0F}) req.add_vector(v);
    req.set_k(5);
    req.set_ef_search(50);
    req.set_shard_map_epoch(1);
    pb::SearchResponse r;
    EXPECT_TRUE(stub->Search(&ctx, req, &r).ok());
    return r;
  };
  EXPECT_EQ(search(stubA_.get()).hits_size(), 5);
  EXPECT_EQ(search(stubB_.get()).hits_size(), 5);  // backup serves reads too
}

}  // namespace
}  // namespace lucent
