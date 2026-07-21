// lucent-member: one mini-Raft voter (M5-T2).
//   lucent-member --node-id member-0 --config cluster.yaml [--port N]
// Three fixed voters (member-0..2) form the membership quorum that owns the
// shard map; the coordinator proposes/watches via MembershipService (M5-T3).

#include <spdlog/spdlog.h>

#include <atomic>
#include <csignal>
#include <chrono>
#include <map>
#include <string>
#include <thread>

#include "common/config.h"
#include "raft/member_server.h"

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

constexpr int kVoters = 3;  // scope-fenced: fixed 3-member quorum (internals §7)

}  // namespace

int main(int argc, char** argv) {
  const std::string node_id = ArgValue(argc, argv, "--node-id", "");
  const std::string config_path = ArgValue(argc, argv, "--config", "cluster.yaml");
  if (node_id.rfind("member-", 0) != 0) {
    spdlog::error("usage: lucent-member --node-id member-N --config cluster.yaml");
    return 2;
  }
  const int j = std::stoi(node_id.substr(std::string("member-").size()));

  const lucent::Config config = lucent::Config::Load(config_path);
  const int base = config.ports.member_base;
  std::map<std::string, std::string> members;
  for (int k = 0; k < kVoters; ++k) {
    members["member-" + std::to_string(k)] =
        "127.0.0.1:" + std::to_string(base + k);
  }
  const int port = std::stoi(ArgValue(argc, argv, "--port", std::to_string(base + j)));
  const std::string dir = config.paths.data + "/" + node_id;

  // Seed the election timer off the node index so the three don't all fire at
  // once — deterministic per node, like the sim.
  lucent::raft::MemberServer member(node_id, members, dir,
                                    static_cast<uint64_t>(j) + 1);
  if (!member.Start("127.0.0.1:" + std::to_string(port))) {
    spdlog::error("{}: failed to bind port {}", node_id, port);
    return 1;
  }

  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);
  while (!g_shutdown.load()) std::this_thread::sleep_for(std::chrono::milliseconds(50));

  member.Shutdown();
  spdlog::info("{}: shut down cleanly", node_id);
  return 0;
}
