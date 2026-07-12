// lucent-coord: query planning, scatter-gather, merge, cluster state.
//   lucent-coord --config cluster.yaml [--node-id coord-0] [--port N]

#include <grpcpp/grpcpp.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <csignal>
#include <memory>
#include <string>
#include <thread>

#include "common/collector_sink.h"
#include "common/config.h"
#include "common/event_emitter.h"
#include "common/node_id.h"
#include "common/proc_stats.h"
#include "coordinator/coordinator_server.h"

namespace {

std::atomic<bool> g_shutdown{false};
void HandleSignal(int /*sig*/) { g_shutdown.store(true); }

std::string ArgValue(int argc, char** argv, const std::string& flag,
                     const std::string& fallback) {
  for (int i = 1; i + 1 < argc; ++i) {
    if (argv[i] == flag) return argv[i + 1];
  }
  return fallback;
}

}  // namespace

int main(int argc, char** argv) {
  const std::string node_id = ArgValue(argc, argv, "--node-id", "coord-0");
  const std::string config_path = ArgValue(argc, argv, "--config", "cluster.yaml");

  auto identity = lucent::NodeIdentity::Parse(node_id);
  if (!identity || identity->role != lucent::Role::kCoordinator) {
    spdlog::error("--node-id must be coord-N, got '{}'", node_id);
    return 2;
  }

  lucent::Config config = lucent::Config::Load(config_path);
  const int port = std::stoi(
      ArgValue(argc, argv, "--port", std::to_string(config.ports.coordinator)));

  lucent::CollectorSink sink(config.CollectorAddr(), identity->id);
  lucent::EventEmitter emitter(identity->id, sink.Fn());

  lucent::CoordinatorServer service(config,
                                    lucent::StaticShardMapFromConfig(config),
                                    config.EmbedAddr(), &emitter);

  const std::string addr = "127.0.0.1:" + std::to_string(port);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
  if (!server) {
    spdlog::error("{}: failed to bind {}", identity->id, addr);
    return 1;
  }
  spdlog::info("{}: listening on {} ({} shards, {})", identity->id, addr,
               config.cluster.shards, config.cluster.partitioning);

  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  std::thread watcher([&] {
    while (!g_shutdown.load()) {
      lucent::v1::Event e;
      auto* stats = e.mutable_stats();
      stats->set_rss_bytes(lucent::CurrentRssBytes());
      stats->set_state(lucent::v1::NODE_STATE_SERVING);
      emitter.Emit(std::move(e));
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    server->Shutdown();
  });

  server->Wait();
  g_shutdown.store(true);
  watcher.join();
  emitter.Stop();
  spdlog::info("{}: shut down cleanly", identity->id);
  return 0;
}
