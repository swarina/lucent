#include "coordinator/raft_store.h"

#include <spdlog/spdlog.h>

#include <chrono>
#include <iterator>
#include <thread>
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
  // Try a bounded number of hops: follow a NOT_LEADER redirect, and — crucially
  // — step to a different member if the target is unreachable (the sticky
  // last-leader may be the one that just died). Enough hops to survive an
  // election in flight.
  for (int hop = 0; hop < 12; ++hop) {
    pb::MembershipService::Stub* stub = StubFor(target);
    if (stub == nullptr) return 0;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
    pb::ProposeResponse resp;
    const grpc::Status st = stub->Propose(&ctx, mut, &resp);
    if (!st.ok()) {  // dead / unreachable → try the next member
      target = NextMember(target);
      continue;
    }
    if (resp.committed()) {
      std::lock_guard<std::mutex> lk(stubs_mu_);
      last_leader_ = target;  // remember who committed it
      return resp.epoch();
    }
    if (!resp.leader_hint().empty()) {
      target = resp.leader_hint();  // redirect to the known leader
    } else {
      target = NextMember(target);  // no leader yet → poll another member
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
  return 0;
}

std::string RaftStore::NextMember(const std::string& cur) const {
  auto it = members_.find(cur);
  if (it == members_.end() || std::next(it) == members_.end()) {
    return members_.begin()->first;  // wrap around
  }
  return std::next(it)->first;
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
