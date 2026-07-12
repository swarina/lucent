#pragma once

#include <cstdint>
#include <string>

namespace lucent {

// Typed view of cluster.yaml (PLAN.md §4). Load() throws std::runtime_error
// with a field path on missing/malformed entries — a misconfigured node must
// die loudly at startup, never run on defaults silently.
struct Config {
  struct Cluster {
    int shards = 4;
    int replicas = 2;
    std::string partitioning = "hash";  // "hash" | "semantic"
  } cluster;

  struct Model {
    std::string name = "all-MiniLM-L6-v2";
    int dim = 384;
    std::string metric = "ip_normalized";
  } model;

  struct Ports {
    int coordinator = 7000;
    int embed = 7001;
    int collector = 7010;
    int gateway_http = 8080;
    int supervisor_ctl = 7999;
    int shard_base = 7100;  // shard i, replica r -> shard_base + i*10 + (r=='a'?0:1)
  } ports;

  struct TimeoutsMs {
    int query_total = 250;
    int embed = 80;
    int shard_search = 150;
  } timeouts_ms;

  struct Health {
    int heartbeat_ms = 500;
    int suspect_after_misses = 2;
    int down_after_misses = 5;
  } health;

  struct Index {
    std::string type = "hnsw";  // "hnsw" | "bruteforce"
    int m = 16;
    int m0 = 32;
    int ef_construction = 200;
    int ef_search_default = 100;
    uint64_t seed = 42;
  } index;

  struct Trace {
    int max_visits_per_query = 65536;
    int full_trace_max_qps = 5;
  } trace;

  struct Paths {
    std::string data = "./data";
    std::string cache = "~/.cache/lucent";
  } paths;

  static Config Load(const std::string& path);

  // Port scheme helpers (PLAN.md §4).
  int ShardPort(int shard_id, char replica) const;
  std::string ShardAddr(int shard_id, char replica) const;  // "127.0.0.1:port"
  std::string CoordinatorAddr() const;
  std::string EmbedAddr() const;
  std::string CollectorAddr() const;
};

}  // namespace lucent
