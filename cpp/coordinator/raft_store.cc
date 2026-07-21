#include "coordinator/raft_store.h"

#include <spdlog/spdlog.h>

#include <chrono>
#include <utility>

namespace lucent {
namespace pb = lucent::v1;

RaftStore::RaftStore(std::map<std::string, std::string> members)
    : members_(std::move(members)) {
  if (!members_.empty()) last_leader_ = members_.begin()->first;
}

RaftStore::~RaftStore() { Stop(); }

pb::MembershipService::Stub* RaftStore::StubFor(const std::string& id) {
  std::lock_guard<std::mutex> lk(stubs_mu_);
  auto it = stubs_.find(id);
  if (it == stubs_.end()) {
    const auto addr = members_.find(id);
    if (addr == members_.end()) return nullptr;
    it = stubs_.emplace(id, pb::MembershipService::NewStub(grpc::CreateChannel(
                                addr->second, grpc::InsecureChannelCredentials())))
             .first;
  }
  return it->second.get();
}

uint64_t RaftStore::Propose(const pb::ShardMap& proposed, uint64_t expected_epoch) {
  pb::ShardMapMutation mut;
  *mut.mutable_proposed() = proposed;
  mut.set_expected_epoch(expected_epoch);

  std::string target;
  {
    std::lock_guard<std::mutex> lk(stubs_mu_);
    target = last_leader_;
  }
  // Follow a NOT_LEADER redirect a few hops (leader may be mid-election).
  for (int hop = 0; hop < 5; ++hop) {
    pb::MembershipService::Stub* stub = StubFor(target);
    if (stub == nullptr) return 0;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    pb::ProposeResponse resp;
    if (!stub->Propose(&ctx, mut, &resp).ok()) return 0;
    if (resp.committed()) {
      std::lock_guard<std::mutex> lk(stubs_mu_);
      last_leader_ = target;  // remember who committed it
      return resp.epoch();
    }
    if (resp.leader_hint().empty()) return 0;  // no leader known yet
    target = resp.leader_hint();
  }
  return 0;
}

void RaftStore::StartWatch(uint64_t from_epoch,
                           std::function<void(const pb::ShardMap&)> on_map) {
  watch_thread_ =
      std::thread([this, from_epoch, on_map = std::move(on_map)]() mutable {
        WatchLoop(from_epoch, std::move(on_map));
      });
}

void RaftStore::WatchLoop(uint64_t from_epoch,
                          std::function<void(const pb::ShardMap&)> on_map) {
  uint64_t next = from_epoch;
  while (!stop_.load()) {
    // Any member serves the committed stream (followers included, per §7).
    for (const auto& [id, addr] : members_) {
      if (stop_.load()) return;
      pb::MembershipService::Stub* stub = StubFor(id);
      if (stub == nullptr) continue;
      grpc::ClientContext ctx;
      {
        std::lock_guard<std::mutex> lk(watch_ctx_mu_);
        watch_ctx_ = &ctx;
      }
      pb::WatchRequest req;
      req.set_from_epoch(next);
      auto reader = stub->Watch(&ctx, req);
      pb::ShardMap map;
      while (!stop_.load() && reader->Read(&map)) {
        if (map.epoch() >= next) {
          next = map.epoch() + 1;
          on_map(map);
        }
      }
      reader->Finish().IgnoreError();
      {
        std::lock_guard<std::mutex> lk(watch_ctx_mu_);
        watch_ctx_ = nullptr;
      }
      if (stop_.load()) return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));  // brief backoff
  }
}

void RaftStore::Stop() {
  if (stop_.exchange(true)) return;
  {
    std::lock_guard<std::mutex> lk(watch_ctx_mu_);
    if (watch_ctx_ != nullptr) watch_ctx_->TryCancel();  // unblock a live Read
  }
  if (watch_thread_.joinable()) watch_thread_.join();
}

}  // namespace lucent
