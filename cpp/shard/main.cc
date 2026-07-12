// lucent-shard: one shard replica process.
//   lucent-shard --node-id shard-0a --config cluster.yaml [--port N] [--data-dir D]
// Port and data dir default from the config + identity (PLAN §4):
//   port = shard_base + shard_id*10 + (replica=='a'?0:1); data = paths.data/node_id

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
#include "shard/shard_server.h"

namespace {

std::atomic<bool> g_shutdown{false};

// Async-signal-safe: only sets a flag; a watcher thread does the real work.
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
  const std::string node_id = ArgValue(argc, argv, "--node-id", "");
  const std::string config_path = ArgValue(argc, argv, "--config", "cluster.yaml");
  if (node_id.empty()) {
    spdlog::error("usage: lucent-shard --node-id shard-0a --config cluster.yaml");
    return 2;
  }

  auto identity = lucent::NodeIdentity::Parse(node_id);
  if (!identity || identity->role != lucent::Role::kShard) {
    spdlog::error("--node-id must be shard-{{i}}{{a|b}}, got '{}'", node_id);
    return 2;
  }

  lucent::Config config = lucent::Config::Load(config_path);
  const int default_port = config.ShardPort(identity->shard_id, identity->replica);
  const int port =
      std::stoi(ArgValue(argc, argv, "--port", std::to_string(default_port)));
  const std::string data_dir = ArgValue(
      argc, argv, "--data-dir", config.paths.data + "/" + identity->id);

  lucent::CollectorSink sink(config.CollectorAddr(), identity->id);
  lucent::EventEmitter emitter(identity->id, sink.Fn());

  lucent::ShardServer service(config, *identity, data_dir, &emitter);
  try {
    service.Init();
  } catch (const std::exception& e) {
    spdlog::error("{}: refusing to serve: {}", identity->id, e.what());
    return 1;
  }

  const std::string addr = "127.0.0.1:" + std::to_string(port);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
  if (!server) {
    spdlog::error("{}: failed to bind {}", identity->id, addr);
    return 1;
  }
  spdlog::info("{}: listening on {} (data: {})", identity->id, addr, data_dir);

  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  // Watcher: turns the signal flag into a clean server shutdown. Also the
  // 2 Hz NodeStats heartbeat (METRICS tier) lives here.
  std::thread watcher([&] {
    while (!g_shutdown.load()) {
      service.EmitStats();
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    server->Shutdown();
  });

  server->Wait();
  g_shutdown.store(true);  // covers Shutdown from elsewhere (e.g. tests)
  watcher.join();
  emitter.Stop();
  spdlog::info("{}: shut down cleanly", identity->id);
  return 0;
}
