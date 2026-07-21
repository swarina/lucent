#include "raft/member_server.h"

#include <spdlog/spdlog.h>

#include <utility>

namespace lucent::raft {
namespace pb = lucent::v1;
namespace {

// --- struct <-> proto translation (the transport's only job) ---
VoteReq FromProto(const pb::VoteRequest& r) {
  return {r.term(), r.candidate(), r.last_log_index(), r.last_log_term()};
}
pb::VoteRequest ToProto(const VoteReq& r) {
  pb::VoteRequest p;
  p.set_term(r.term);
  p.set_candidate(r.candidate);
  p.set_last_log_index(r.last_log_index);
  p.set_last_log_term(r.last_log_term);
  return p;
}
AppendReq FromProto(const pb::AppendRequest& r) {
  AppendReq a{r.term(), r.leader(), r.prev_index(), r.prev_term(), {}, r.commit_index()};
  for (const pb::LogEntry& e : r.entries())
    a.entries.push_back({e.index(), e.term(), e.mutation()});
  return a;
}
pb::AppendRequest ToProto(const AppendReq& r) {
  pb::AppendRequest p;
  p.set_term(r.term);
  p.set_leader(r.leader);
  p.set_prev_index(r.prev_index);
  p.set_prev_term(r.prev_term);
  p.set_commit_index(r.commit_index);
  for (const LogEntry& e : r.entries) {
    pb::LogEntry* pe = p.add_entries();
    pe->set_index(e.index);
    pe->set_term(e.term);
    *pe->mutable_mutation() = e.mutation;
  }
  return p;
}

// Service adapters: the generated service bases both derive from grpc::Service,
// so one object can't be both — forward to the MemberServer core instead.
class RaftSvc final : public pb::RaftService::Service {
 public:
  explicit RaftSvc(MemberServer* m) : m_(m) {}
  grpc::Status RequestVote(grpc::ServerContext*, const pb::VoteRequest* req,
                           pb::VoteResponse* resp) override {
    m_->OnRequestVote(*req, resp);
    return grpc::Status::OK;
  }
  grpc::Status AppendEntries(grpc::ServerContext*, const pb::AppendRequest* req,
                             pb::AppendResponse* resp) override {
    m_->OnAppendEntries(*req, resp);
    return grpc::Status::OK;
  }

 private:
  MemberServer* m_;
};

class MemberSvc final : public pb::MembershipService::Service {
 public:
  explicit MemberSvc(MemberServer* m) : m_(m) {}
  grpc::Status Propose(grpc::ServerContext*, const pb::ShardMapMutation* req,
                       pb::ProposeResponse* resp) override {
    m_->OnPropose(*req, resp);
    return grpc::Status::OK;
  }
  grpc::Status Watch(grpc::ServerContext* ctx, const pb::WatchRequest* req,
                     grpc::ServerWriter<pb::ShardMap>* writer) override {
    m_->OnWatch(*req, ctx, writer);
    return grpc::Status::OK;
  }

 private:
  MemberServer* m_;
};

std::vector<std::string> Ids(const std::map<std::string, std::string>& m) {
  std::vector<std::string> ids;
  for (const auto& [id, addr] : m) ids.push_back(id);
  return ids;
}

}  // namespace

MemberServer::MemberServer(std::string id,
                           std::map<std::string, std::string> members,
                           std::string dir, uint64_t seed)
    : id_(std::move(id)),
      members_(std::move(members)),
      node_(id_, Ids(members_), seed),
      storage_(std::move(dir)),
      epoch_(std::chrono::steady_clock::now()) {}

MemberServer::~MemberServer() { Shutdown(); }

uint64_t MemberServer::NowMs() const {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - epoch_)
          .count());
}

void MemberServer::Persist() { storage_.Save(node_.persistent()); }

bool MemberServer::Start(const std::string& bind_addr) {
  // Recover persisted {term, vote, log} before serving (internals.md §7).
  {
    std::lock_guard<std::mutex> lk(mu_);
    node_.Restore(storage_.Load(), NowMs());
  }
  for (const auto& [pid, addr] : members_) {
    if (pid != id_) {
      peers_[pid] = pb::RaftService::NewStub(
          grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
    }
  }
  raft_svc_ = std::make_unique<RaftSvc>(this);
  member_svc_ = std::make_unique<MemberSvc>(this);
  grpc::ServerBuilder b;
  b.AddListeningPort(bind_addr, grpc::InsecureServerCredentials());
  b.RegisterService(raft_svc_.get());
  b.RegisterService(member_svc_.get());
  server_ = b.BuildAndStart();
  if (!server_) return false;
  driver_ = std::thread([this] { DriverLoop(); });
  spdlog::info("{}: raft member on {}", id_, bind_addr);
  return true;
}

void MemberServer::Shutdown() {
  if (stop_.exchange(true)) return;
  cv_.notify_all();  // wake Propose/Watch waiters
  if (driver_.joinable()) driver_.join();
  if (server_) server_->Shutdown();
}

void MemberServer::DriverLoop() {
  while (!stop_.load()) {
    std::vector<Message> out;
    {
      std::lock_guard<std::mutex> lk(mu_);
      node_.Tick(NowMs());
      Persist();
      out = node_.TakeOutbox();
    }
    cv_.notify_all();
    Dispatch(std::move(out));
    std::this_thread::sleep_for(std::chrono::milliseconds(15));
  }
}

// Ship outbound REQUEST messages to peers with the async client API; the reply
// (VoteResponse / AppendResponse) is fed back into the core via Deliver on a
// gRPC thread. Never holds mu_ across the network call.
void MemberServer::Dispatch(std::vector<Message> out) {
  for (Message& m : out) {
    auto it = peers_.find(m.to);
    if (it == peers_.end()) continue;
    pb::RaftService::Stub* stub = it->second.get();
    if (m.kind == Message::kVoteReq) {
      struct Call {
        grpc::ClientContext ctx;
        pb::VoteRequest req;
        pb::VoteResponse resp;
      };
      auto* c = new Call;
      c->req = ToProto(m.vote_req);
      c->ctx.set_deadline(std::chrono::system_clock::now() +
                          std::chrono::milliseconds(300));
      const std::string from = m.to;
      stub->async()->RequestVote(&c->ctx, &c->req, &c->resp,
                                 [this, c, from](grpc::Status s) {
        if (s.ok()) {
          Message r{Message::kVoteResp, from, id_, {}, {}, {}, {}};
          r.vote_resp = {c->resp.term(), c->resp.granted()};
          Deliver(std::move(r));
        }
        delete c;
      });
    } else if (m.kind == Message::kAppendReq) {
      struct Call {
        grpc::ClientContext ctx;
        pb::AppendRequest req;
        pb::AppendResponse resp;
      };
      auto* c = new Call;
      c->req = ToProto(m.append_req);
      c->ctx.set_deadline(std::chrono::system_clock::now() +
                          std::chrono::milliseconds(300));
      const std::string from = m.to;
      stub->async()->AppendEntries(&c->ctx, &c->req, &c->resp,
                                   [this, c, from](grpc::Status s) {
        if (s.ok()) {
          Message r{Message::kAppendResp, from, id_, {}, {}, {}, {}};
          r.append_resp = {c->resp.term(), c->resp.success(), c->resp.match_index()};
          Deliver(std::move(r));
        }
        delete c;
      });
    }
  }
}

void MemberServer::Deliver(Message m) {
  std::vector<Message> out;
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (stop_.load()) return;
    node_.Handle(m, NowMs());
    Persist();
    out = node_.TakeOutbox();
  }
  cv_.notify_all();
  Dispatch(std::move(out));
}

void MemberServer::OnRequestVote(const pb::VoteRequest& req, pb::VoteResponse* resp) {
  std::vector<Message> out;
  {
    std::lock_guard<std::mutex> lk(mu_);
    Message m{Message::kVoteReq, req.candidate(), id_, {}, {}, {}, {}};
    m.vote_req = FromProto(req);
    node_.Handle(m, NowMs());
    Persist();
    out = node_.TakeOutbox();
  }
  cv_.notify_all();
  for (Message& o : out) {
    if (o.kind == Message::kVoteResp) {  // the reply to the candidate
      resp->set_term(o.vote_resp.term);
      resp->set_granted(o.vote_resp.granted);
    }
  }
}

void MemberServer::OnAppendEntries(const pb::AppendRequest& req, pb::AppendResponse* resp) {
  std::vector<Message> out;
  {
    std::lock_guard<std::mutex> lk(mu_);
    Message m{Message::kAppendReq, req.leader(), id_, {}, {}, {}, {}};
    m.append_req = FromProto(req);
    node_.Handle(m, NowMs());
    Persist();
    out = node_.TakeOutbox();
  }
  cv_.notify_all();
  for (Message& o : out) {
    if (o.kind == Message::kAppendResp) {  // the reply to the leader
      resp->set_term(o.append_resp.term);
      resp->set_success(o.append_resp.success);
      resp->set_match_index(o.append_resp.match_index);
    }
  }
}

void MemberServer::OnPropose(const pb::ShardMapMutation& req, pb::ProposeResponse* resp) {
  uint64_t target = 0;
  std::vector<Message> out;
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (node_.role() != Role::kLeader) {
      resp->set_committed(false);
      resp->set_leader_hint(node_.leader());  // redirect (NOT_LEADER)
      return;
    }
    node_.Propose(req, NowMs());
    Persist();
    target = node_.log().back().index;
    out = node_.TakeOutbox();
  }
  Dispatch(std::move(out));

  std::unique_lock<std::mutex> lk(mu_);
  cv_.wait_for(lk, std::chrono::seconds(3), [&] {
    return stop_.load() || node_.commit_index() >= target ||
           node_.role() != Role::kLeader;
  });
  if (node_.commit_index() >= target) {
    resp->set_committed(true);
    resp->set_epoch(node_.applied().epoch());
  } else {  // lost leadership or timed out before this entry committed
    resp->set_committed(false);
    resp->set_leader_hint(node_.leader());
  }
}

void MemberServer::OnWatch(const pb::WatchRequest& req, grpc::ServerContext* ctx,
                           grpc::ServerWriter<pb::ShardMap>* writer) {
  uint64_t sent = 0;
  while (!ctx->IsCancelled() && !stop_.load()) {
    pb::ShardMap snapshot;
    {
      std::unique_lock<std::mutex> lk(mu_);
      // Wait (with a wakeup cap so cancellation is noticed) for a newer map.
      cv_.wait_for(lk, std::chrono::milliseconds(200), [&] {
        return stop_.load() || node_.applied().epoch() > sent;
      });
      if (stop_.load()) break;
      if (node_.applied().epoch() <= sent) continue;  // timeout → re-check cancel
      snapshot = node_.applied();
    }
    if (snapshot.epoch() < req.from_epoch()) continue;
    sent = snapshot.epoch();
    if (!writer->Write(snapshot)) break;  // client disconnected
  }
}

Role MemberServer::role() {
  std::lock_guard<std::mutex> lk(mu_);
  return node_.role();
}
uint64_t MemberServer::term() {
  std::lock_guard<std::mutex> lk(mu_);
  return node_.term();
}
std::string MemberServer::leader() {
  std::lock_guard<std::mutex> lk(mu_);
  return node_.leader();
}
uint64_t MemberServer::applied_epoch() {
  std::lock_guard<std::mutex> lk(mu_);
  return node_.applied().epoch();
}
uint64_t MemberServer::log_head_epoch() {
  std::lock_guard<std::mutex> lk(mu_);
  const std::vector<LogEntry>& log = node_.log();
  return log.empty() ? 0 : log.back().mutation.proposed().epoch();
}

}  // namespace lucent::raft
