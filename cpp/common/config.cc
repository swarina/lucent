#include "common/config.h"

#include <yaml-cpp/yaml.h>

#include <stdexcept>
#include <string>

namespace lucent {
namespace {

// Fetches map[key], throwing with a dotted field path when absent.
YAML::Node Require(const YAML::Node& map, const std::string& key,
                   const std::string& path) {
  YAML::Node node = map[key];
  if (!node.IsDefined() || node.IsNull()) {
    throw std::runtime_error("cluster.yaml: missing required field '" + path +
                             "." + key + "'");
  }
  return node;
}

template <typename T>
T Get(const YAML::Node& map, const std::string& key, const std::string& path) {
  try {
    return Require(map, key, path).as<T>();
  } catch (const YAML::Exception& e) {
    throw std::runtime_error("cluster.yaml: bad value for '" + path + "." +
                             key + "': " + e.what());
  }
}

}  // namespace

Config Config::Load(const std::string& path) {
  YAML::Node root;
  try {
    root = YAML::LoadFile(path);
  } catch (const YAML::Exception& e) {
    throw std::runtime_error("cluster.yaml: cannot load '" + path +
                             "': " + e.what());
  }

  Config c;

  YAML::Node cluster = Require(root, "cluster", "");
  c.cluster.shards = Get<int>(cluster, "shards", "cluster");
  c.cluster.replicas = Get<int>(cluster, "replicas", "cluster");
  c.cluster.partitioning = Get<std::string>(cluster, "partitioning", "cluster");
  if (c.cluster.partitioning != "hash" && c.cluster.partitioning != "semantic") {
    throw std::runtime_error(
        "cluster.yaml: cluster.partitioning must be 'hash' or 'semantic', got '" +
        c.cluster.partitioning + "'");
  }
  if (c.cluster.shards < 1 || c.cluster.replicas < 1 || c.cluster.replicas > 2) {
    throw std::runtime_error(
        "cluster.yaml: cluster.shards must be >=1 and replicas in {1,2}");
  }

  YAML::Node model = Require(root, "model", "");
  c.model.name = Get<std::string>(model, "name", "model");
  c.model.dim = Get<int>(model, "dim", "model");
  c.model.metric = Get<std::string>(model, "metric", "model");

  YAML::Node ports = Require(root, "ports", "");
  c.ports.coordinator = Get<int>(ports, "coordinator", "ports");
  c.ports.embed = Get<int>(ports, "embed", "ports");
  c.ports.collector = Get<int>(ports, "collector", "ports");
  c.ports.gateway_http = Get<int>(ports, "gateway_http", "ports");
  c.ports.supervisor_ctl = Get<int>(ports, "supervisor_ctl", "ports");
  c.ports.shard_base = Get<int>(ports, "shard_base", "ports");

  YAML::Node timeouts = Require(root, "timeouts_ms", "");
  c.timeouts_ms.query_total = Get<int>(timeouts, "query_total", "timeouts_ms");
  c.timeouts_ms.embed = Get<int>(timeouts, "embed", "timeouts_ms");
  c.timeouts_ms.shard_search = Get<int>(timeouts, "shard_search", "timeouts_ms");

  YAML::Node health = Require(root, "health", "");
  c.health.heartbeat_ms = Get<int>(health, "heartbeat_ms", "health");
  c.health.suspect_after_misses =
      Get<int>(health, "suspect_after_misses", "health");
  c.health.down_after_misses = Get<int>(health, "down_after_misses", "health");

  YAML::Node index = Require(root, "index", "");
  c.index.type = Get<std::string>(index, "type", "index");
  if (c.index.type != "hnsw" && c.index.type != "bruteforce") {
    throw std::runtime_error(
        "cluster.yaml: index.type must be 'hnsw' or 'bruteforce', got '" +
        c.index.type + "'");
  }
  c.index.m = Get<int>(index, "M", "index");
  c.index.m0 = Get<int>(index, "M0", "index");
  c.index.ef_construction = Get<int>(index, "ef_construction", "index");
  c.index.ef_search_default = Get<int>(index, "ef_search_default", "index");
  c.index.seed = Get<uint64_t>(index, "seed", "index");

  YAML::Node trace = Require(root, "trace", "");
  c.trace.max_visits_per_query =
      Get<int>(trace, "max_visits_per_query", "trace");
  c.trace.full_trace_max_qps = Get<int>(trace, "full_trace_max_qps", "trace");

  YAML::Node paths = Require(root, "paths", "");
  c.paths.data = Get<std::string>(paths, "data", "paths");
  c.paths.cache = Get<std::string>(paths, "cache", "paths");

  return c;
}

int Config::ShardPort(int shard_id, char replica) const {
  if (shard_id < 0 || (replica != 'a' && replica != 'b')) {
    throw std::runtime_error("ShardPort: invalid shard/replica");
  }
  return ports.shard_base + shard_id * 10 + (replica == 'a' ? 0 : 1);
}

std::string Config::ShardAddr(int shard_id, char replica) const {
  return "127.0.0.1:" + std::to_string(ShardPort(shard_id, replica));
}
std::string Config::CoordinatorAddr() const {
  return "127.0.0.1:" + std::to_string(ports.coordinator);
}
std::string Config::EmbedAddr() const {
  return "127.0.0.1:" + std::to_string(ports.embed);
}
std::string Config::CollectorAddr() const {
  return "127.0.0.1:" + std::to_string(ports.collector);
}

}  // namespace lucent
