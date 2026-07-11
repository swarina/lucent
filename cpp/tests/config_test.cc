#include "common/config.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <stdexcept>
#include <string>

namespace lucent {
namespace {

// Writes content to a unique temp file; removed on destruction.
class TempYaml {
 public:
  explicit TempYaml(const std::string& content) {
    path_ = testing::TempDir() + "lucent_config_test_" +
            std::to_string(counter_++) + ".yaml";
    std::ofstream out(path_);
    out << content;
  }
  ~TempYaml() { std::remove(path_.c_str()); }
  const std::string& path() const { return path_; }

 private:
  static inline int counter_ = 0;
  std::string path_;
};

constexpr char kValidYaml[] = R"(
cluster: { shards: 4, replicas: 2, partitioning: hash }
model: { name: all-MiniLM-L6-v2, dim: 384, metric: ip_normalized }
ports:
  coordinator: 7000
  embed: 7001
  collector: 7010
  gateway_http: 8080
  supervisor_ctl: 7999
  shard_base: 7100
timeouts_ms: { query_total: 250, embed: 80, shard_search: 150 }
health: { heartbeat_ms: 500, suspect_after_misses: 2, down_after_misses: 5 }
index: { M: 16, M0: 32, ef_construction: 200, ef_search_default: 100, seed: 42 }
trace: { max_visits_per_query: 65536, full_trace_max_qps: 5 }
paths: { data: ./data, cache: ~/.cache/lucent }
)";

TEST(Config, LoadsAllFields) {
  TempYaml f(kValidYaml);
  Config c = Config::Load(f.path());
  EXPECT_EQ(c.cluster.shards, 4);
  EXPECT_EQ(c.cluster.replicas, 2);
  EXPECT_EQ(c.cluster.partitioning, "hash");
  EXPECT_EQ(c.model.dim, 384);
  EXPECT_EQ(c.ports.coordinator, 7000);
  EXPECT_EQ(c.timeouts_ms.embed, 80);
  EXPECT_EQ(c.health.down_after_misses, 5);
  EXPECT_EQ(c.index.m, 16);
  EXPECT_EQ(c.index.m0, 32);
  EXPECT_EQ(c.index.seed, 42u);
  EXPECT_EQ(c.trace.max_visits_per_query, 65536);
  EXPECT_EQ(c.paths.data, "./data");
}

TEST(Config, PortScheme) {
  TempYaml f(kValidYaml);
  Config c = Config::Load(f.path());
  EXPECT_EQ(c.ShardPort(0, 'a'), 7100);
  EXPECT_EQ(c.ShardPort(0, 'b'), 7101);
  EXPECT_EQ(c.ShardPort(2, 'b'), 7121);
  EXPECT_EQ(c.ShardAddr(3, 'a'), "127.0.0.1:7130");
  EXPECT_EQ(c.CoordinatorAddr(), "127.0.0.1:7000");
  EXPECT_THROW(c.ShardPort(0, 'c'), std::runtime_error);
  EXPECT_THROW(c.ShardPort(-1, 'a'), std::runtime_error);
}

TEST(Config, MissingFieldNamesThePath) {
  TempYaml f("cluster: { shards: 4, replicas: 2 }\n");  // partitioning absent
  try {
    Config::Load(f.path());
    FAIL() << "expected throw";
  } catch (const std::runtime_error& e) {
    EXPECT_NE(std::string(e.what()).find("cluster.partitioning"),
              std::string::npos)
        << e.what();
  }
}

TEST(Config, RejectsBadPartitioning) {
  std::string bad = kValidYaml;
  bad.replace(bad.find("partitioning: hash"), 18, "partitioning: rand");
  TempYaml f(bad);
  EXPECT_THROW(Config::Load(f.path()), std::runtime_error);
}

TEST(Config, MissingFileThrows) {
  EXPECT_THROW(Config::Load("/nonexistent/cluster.yaml"), std::runtime_error);
}

TEST(Config, RepoDefaultClusterYamlLoads) {
  // The checked-in cluster.yaml must always parse. Test binary runs from
  // build/dev/cpp/tests; walk up to the repo root.
  Config c = Config::Load(std::string(LUCENT_REPO_ROOT) + "/cluster.yaml");
  EXPECT_EQ(c.model.dim, 384);
  EXPECT_EQ(c.ports.shard_base, 7100);
}

}  // namespace
}  // namespace lucent
