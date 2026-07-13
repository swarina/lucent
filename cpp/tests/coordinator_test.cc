#include "coordinator/coordinator_server.h"

#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include <memory>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "common/config.h"
#include "coordinator/merge.h"
#include "coordinator/planner.h"
#include "lucent/v1/coordinator.grpc.pb.h"
#include "lucent/v1/embed.grpc.pb.h"
#include "lucent/v1/shard.grpc.pb.h"

namespace lucent {
namespace {

namespace pb = lucent::v1;

// ---------- MergeTopK (pure) ----------

pb::SearchResponse Resp(const std::string& node,
                        std::vector<std::pair<uint64_t, float>> hits) {
  pb::SearchResponse r;
  r.set_node_id(node);
  for (auto& [id, score] : hits) {
    auto* h = r.add_hits();
    h->set_doc_id(id);
    h->set_score(score);
  }
  return r;
}

TEST(MergeTopK, OrdersAcrossShardsWithTiebreak) {
  std::vector<ShardResult> results;
  results.push_back({0, Resp("shard-0a", {{1, 0.9F}, {2, 0.5F}})});
  results.push_back({1, Resp("shard-1a", {{3, 0.7F}, {4, 0.5F}, {5, 0.3F}})});
  auto merged = MergeTopK(results, 4);
  ASSERT_EQ(merged.size(), 4u);
  EXPECT_EQ(merged[0].doc_id(), 1u);
  EXPECT_EQ(merged[1].doc_id(), 3u);
  EXPECT_EQ(merged[2].doc_id(), 2u);  // 0.5 tie -> doc_id 2 before 4
  EXPECT_EQ(merged[3].doc_id(), 4u);
  EXPECT_EQ(merged[2].shard_id(), 0u);  // provenance annotated
  EXPECT_EQ(merged[3].node_id(), "shard-1a");
}

TEST(MergeTopK, CapsAtKAndHandlesEmpty) {
  std::vector<ShardResult> results;
  results.push_back({0, Resp("a", {{1, 0.9F}, {2, 0.8F}, {3, 0.7F}})});
  EXPECT_EQ(MergeTopK(results, 2).size(), 2u);
  EXPECT_TRUE(MergeTopK({}, 5).empty());
}

// Property: merging per-shard top-k lists == the exact top-k of their union
// (partitions are disjoint, ordered by (score desc, doc_id asc)). This is the
// scatter-gather correctness contract — internals.md §2.2.
TEST(MergeTopK, EqualsExactTopKOfUnion) {
  std::mt19937_64 rng(12345);
  std::uniform_int_distribution<int> shards_dist(1, 6);
  std::uniform_int_distribution<int> per_shard(0, 20);
  std::uniform_int_distribution<uint64_t> id_dist(1, 100000);
  std::uniform_real_distribution<float> score_dist(0.0F, 1.0F);

  for (int trial = 0; trial < 300; ++trial) {
    const int nshards = shards_dist(rng);
    std::vector<ShardResult> results;
    std::vector<std::pair<float, uint64_t>> all;  // (score, doc_id)
    std::set<uint64_t> used;                        // keep partitions disjoint

    for (int s = 0; s < nshards; ++s) {
      std::vector<std::pair<uint64_t, float>> hits;
      const int m = per_shard(rng);
      for (int i = 0; i < m; ++i) {
        uint64_t id = id_dist(rng);
        while (used.count(id)) id = id_dist(rng);
        used.insert(id);
        const float sc = score_dist(rng);
        hits.emplace_back(id, sc);
        all.emplace_back(sc, id);
      }
      // Each shard returns its own hits sorted desc (as a real shard would).
      std::sort(hits.begin(), hits.end(),
                [](auto& a, auto& b) { return a.second > b.second; });
      results.push_back({static_cast<uint32_t>(s),
                         Resp("shard-" + std::to_string(s) + "a", hits)});
    }

    const uint32_t k = 1 + (rng() % 15);

    // Reference: exact top-k of the union by (score desc, doc_id asc).
    std::sort(all.begin(), all.end(), [](const auto& a, const auto& b) {
      if (a.first != b.first) return a.first > b.first;
      return a.second < b.second;
    });
    if (all.size() > k) all.resize(k);

    const auto merged = MergeTopK(results, k);
    ASSERT_EQ(merged.size(), all.size()) << "trial " << trial;
    for (size_t i = 0; i < merged.size(); ++i) {
      EXPECT_EQ(merged[i].doc_id(), all[i].second) << "trial " << trial << " rank " << i;
      EXPECT_FLOAT_EQ(merged[i].score(), all[i].first);
    }
  }
}

// ---------- PlanQuery (pure) ----------

TEST(PlanQuery, ProbeKnob) {
  Config cfg = Config::Load(std::string(LUCENT_REPO_ROOT) + "/cluster.yaml");
  cfg.cluster.shards = 4;
  const pb::ShardMap map = StaticShardMapFromConfig(cfg);

  auto all = PlanQuery(map, 0);
  EXPECT_EQ(all.probe.size(), 4u);
  EXPECT_TRUE(all.unprobed.empty());
  EXPECT_EQ(all.epoch, 1u);

  auto two = PlanQuery(map, 2);
  ASSERT_EQ(two.probe.size(), 2u);
  EXPECT_EQ(two.probe[0].shard_id, 0u);  // lowest ids, deterministic
  EXPECT_EQ(two.probe[1].shard_id, 1u);
  EXPECT_EQ(two.unprobed, (std::vector<uint32_t>{2, 3}));

  EXPECT_EQ(PlanQuery(map, 99).probe.size(), 4u);  // probe > shards == all
}

// ---------- Query integration with fake embed + shards ----------

class FakeEmbed final : public pb::EmbedService::Service {
 public:
  explicit FakeEmbed(int dim) : dim_(dim) {}
  grpc::Status Embed(grpc::ServerContext*, const pb::EmbedRequest* req,
                     pb::EmbedResponse* resp) override {
    resp->set_dim(static_cast<uint32_t>(dim_));
    resp->set_count(static_cast<uint32_t>(req->texts_size()));
    for (int i = 0; i < req->texts_size() * dim_; ++i) {
      resp->add_vectors(i == 0 ? 1.0F : 0.0F);  // unit vector
    }
    return grpc::Status::OK;
  }
  grpc::Status Info(grpc::ServerContext*, const pb::InfoRequest*,
                    pb::InfoResponse* resp) override {
    resp->set_ready(true);
    return grpc::Status::OK;
  }

 private:
  int dim_;
};

// Fake shard returning canned hits; can be told to fail.
class FakeShard final : public pb::ShardService::Service {
 public:
  FakeShard(std::string node, std::vector<std::pair<uint64_t, float>> hits)
      : node_(std::move(node)), hits_(std::move(hits)) {}

  void set_fail(bool fail) { fail_ = fail; }
  uint64_t last_epoch() const { return last_epoch_; }
  uint32_t last_k() const { return last_k_; }

  grpc::Status Search(grpc::ServerContext*, const pb::SearchRequest* req,
                      pb::SearchResponse* resp) override {
    last_epoch_ = req->shard_map_epoch();
    last_k_ = req->k();
    if (fail_) {
      return grpc::Status(grpc::StatusCode::UNAVAILABLE, "fake down");
    }
    *resp = Resp(node_, hits_);
    resp->set_visited(42);
    return grpc::Status::OK;
  }

 private:
  std::string node_;
  std::vector<std::pair<uint64_t, float>> hits_;
  std::atomic<bool> fail_{false};
  std::atomic<uint64_t> last_epoch_{0};
  std::atomic<uint32_t> last_k_{0};
};

struct BoundService {
  std::unique_ptr<grpc::Server> server;
  int port = 0;
};

BoundService Bind(grpc::Service* service) {
  BoundService out;
  grpc::ServerBuilder builder;
  builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                           &out.port);
  builder.RegisterService(service);
  out.server = builder.BuildAndStart();
  return out;
}

class CoordinatorQueryTest : public testing::Test {
 protected:
  void SetUp() override {
    config_ = Config::Load(std::string(LUCENT_REPO_ROOT) + "/cluster.yaml");
    config_.cluster.shards = 2;
    config_.model.dim = 4;

    embed_ = std::make_unique<FakeEmbed>(4);
    shard0_ = std::make_unique<FakeShard>(
        "shard-0a", std::vector<std::pair<uint64_t, float>>{{1, 0.9F}, {2, 0.4F}});
    shard1_ = std::make_unique<FakeShard>(
        "shard-1a", std::vector<std::pair<uint64_t, float>>{{3, 0.7F}, {4, 0.6F}});
    embed_srv_ = Bind(embed_.get());
    shard0_srv_ = Bind(shard0_.get());
    shard1_srv_ = Bind(shard1_.get());

    pb::ShardMap map;
    map.set_epoch(7);
    map.set_partitioning("hash");
    auto add = [&](uint32_t id, int port, const std::string& node) {
      auto* e = map.add_shards();
      e->set_shard_id(id);
      e->set_primary_node(node);
      e->set_primary_addr("127.0.0.1:" + std::to_string(port));
      e->set_primary_state(pb::NODE_STATE_SERVING);
    };
    add(0, shard0_srv_.port, "shard-0a");
    add(1, shard1_srv_.port, "shard-1a");

    emitter_ = std::make_unique<EventEmitter>(
        "coord-0", [this](std::vector<pb::Event>&& batch) {
          std::lock_guard<std::mutex> lock(events_mu_);
          for (auto& e : batch) events_.push_back(std::move(e));
        });
    coord_ = std::make_unique<CoordinatorServer>(
        config_, map, "127.0.0.1:" + std::to_string(embed_srv_.port),
        emitter_.get());
    coord_srv_ = Bind(coord_.get());
    stub_ = pb::CoordinatorService::NewStub(
        grpc::CreateChannel("127.0.0.1:" + std::to_string(coord_srv_.port),
                            grpc::InsecureChannelCredentials()));
  }

  void TearDown() override {
    for (auto* s : {&coord_srv_, &embed_srv_, &shard0_srv_, &shard1_srv_}) {
      if (s->server) s->server->Shutdown();
    }
    emitter_->Stop();
  }

  pb::QueryResponse Query(uint32_t probe = 0) {
    grpc::ClientContext ctx;
    pb::QueryRequest req;
    req.set_text("test query");
    req.set_k(3);
    req.set_probe(probe);
    pb::QueryResponse resp;
    const grpc::Status st = stub_->Query(&ctx, req, &resp);
    EXPECT_TRUE(st.ok()) << st.error_message();
    return resp;
  }

  std::vector<pb::SpanKind> SpanKinds() {
    emitter_->Flush();
    std::lock_guard<std::mutex> lock(events_mu_);
    std::vector<pb::SpanKind> kinds;
    for (const auto& e : events_) {
      if (e.has_span()) kinds.push_back(e.span().kind());
    }
    return kinds;
  }

  Config config_{};
  std::unique_ptr<FakeEmbed> embed_;
  std::unique_ptr<FakeShard> shard0_, shard1_;
  BoundService embed_srv_, shard0_srv_, shard1_srv_, coord_srv_;
  std::mutex events_mu_;
  std::vector<pb::Event> events_;
  std::unique_ptr<EventEmitter> emitter_;
  std::unique_ptr<CoordinatorServer> coord_;
  std::unique_ptr<pb::CoordinatorService::Stub> stub_;
};

TEST_F(CoordinatorQueryTest, MergesAcrossShardsWithProvenance) {
  const pb::QueryResponse resp = Query();
  ASSERT_EQ(resp.hits_size(), 3);
  EXPECT_EQ(resp.hits(0).doc_id(), 1u);
  EXPECT_EQ(resp.hits(1).doc_id(), 3u);
  EXPECT_EQ(resp.hits(2).doc_id(), 4u);
  EXPECT_EQ(resp.hits(0).shard_id(), 0u);
  EXPECT_EQ(resp.hits(1).node_id(), "shard-1a");
  EXPECT_EQ(resp.coverage().probed(), 2u);
  EXPECT_EQ(resp.coverage().answered(), 2u);
  EXPECT_EQ(resp.trace_id().size(), 16u);
  EXPECT_GT(resp.timings().total_us(), 0u);
  // Shards saw the map epoch and full k (no per-shard k reduction).
  EXPECT_EQ(shard0_->last_epoch(), 7u);
  EXPECT_EQ(shard0_->last_k(), 3u);

  // Full span ladder emitted.
  const auto kinds = SpanKinds();
  for (pb::SpanKind want :
       {pb::SPAN_QUERY_RECEIVED, pb::SPAN_EMBED, pb::SPAN_PLAN,
        pb::SPAN_SHARD_RPC, pb::SPAN_MERGE, pb::SPAN_QUERY_DONE}) {
    EXPECT_NE(std::find(kinds.begin(), kinds.end(), want), kinds.end())
        << pb::SpanKind_Name(want);
  }
}

TEST_F(CoordinatorQueryTest, DeadShardBecomesCoverageNotFailure) {
  shard1_->set_fail(true);
  const pb::QueryResponse resp = Query();
  ASSERT_EQ(resp.hits_size(), 2);  // only shard-0's hits
  EXPECT_EQ(resp.coverage().answered(), 1u);
  ASSERT_EQ(resp.coverage().missing_shards_size(), 1);
  EXPECT_EQ(resp.coverage().missing_shards(0), 1u);
}

TEST_F(CoordinatorQueryTest, ProbeKnobReportsUnprobed) {
  const pb::QueryResponse resp = Query(/*probe=*/1);
  EXPECT_EQ(resp.coverage().probed(), 1u);
  ASSERT_EQ(resp.coverage().unprobed_shards_size(), 1);
  EXPECT_EQ(resp.coverage().unprobed_shards(0), 1u);
  for (const auto& h : resp.hits()) EXPECT_EQ(h.shard_id(), 0u);
}

TEST_F(CoordinatorQueryTest, EmbedDownFailsFast) {
  embed_srv_.server->Shutdown();
  grpc::ClientContext ctx;
  pb::QueryRequest req;
  req.set_text("q");
  pb::QueryResponse resp;
  const grpc::Status st = stub_->Query(&ctx, req, &resp);
  EXPECT_EQ(st.error_code(), grpc::StatusCode::UNAVAILABLE);
}

TEST_F(CoordinatorQueryTest, EmptyTextRejected) {
  grpc::ClientContext ctx;
  pb::QueryRequest req;
  pb::QueryResponse resp;
  EXPECT_EQ(stub_->Query(&ctx, req, &resp).error_code(),
            grpc::StatusCode::INVALID_ARGUMENT);
}

}  // namespace
}  // namespace lucent
