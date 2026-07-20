#include "raft/raft_storage.h"

#include <xxhash.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>

namespace lucent::raft {
namespace {

namespace fs = std::filesystem;

constexpr char kMagic[4] = {'L', 'C', 'R', 'T'};
constexpr uint16_t kFormat = 1;

void PutU16(std::string& b, uint16_t v) {
  for (int i = 0; i < 2; ++i) b.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}
void PutU32(std::string& b, uint32_t v) {
  for (int i = 0; i < 4; ++i) b.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}
void PutU64(std::string& b, uint64_t v) {
  for (int i = 0; i < 8; ++i) b.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}
uint32_t GetU32(const char* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(static_cast<uint8_t>(p[i])) << (8 * i);
  return v;
}
uint64_t GetU64(const char* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(static_cast<uint8_t>(p[i])) << (8 * i);
  return v;
}

std::string ReadFile(const std::string& path) {
  std::ifstream f(path, std::ios::binary);
  if (!f) return {};
  return std::string((std::istreambuf_iterator<char>(f)),
                     std::istreambuf_iterator<char>());
}

void WriteFileAtomic(const std::string& path, const std::string& bytes) {
  const std::string tmp = path + ".tmp";
  {
    std::ofstream f(tmp, std::ios::binary | std::ios::trunc);
    f.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    f.flush();
  }
  fs::rename(tmp, path);  // atomic replace
}

// One log record: index u64, term u64, len u32, mutation bytes, xxh32 u32 over
// everything before the checksum.
std::string EncodeEntry(const LogEntry& e) {
  std::string body;
  PutU64(body, e.index);
  PutU64(body, e.term);
  std::string mut;
  e.mutation.SerializeToString(&mut);
  PutU32(body, static_cast<uint32_t>(mut.size()));
  body += mut;
  const uint32_t sum = XXH32(body.data(), body.size(), 0);
  PutU32(body, sum);
  return body;
}

}  // namespace

RaftStorage::RaftStorage(std::string dir)
    : dir_(std::move(dir)),
      state_path_(dir_ + "/state.bin"),
      log_path_(dir_ + "/log.bin") {
  fs::create_directories(dir_);
}

Persistent RaftStorage::Load() {
  Persistent p;

  // --- state.bin ---
  const std::string s = ReadFile(state_path_);
  // magic(4) + format(2) + term(8) + vote_len(1) + [vote] + xxh3(8)
  if (s.size() >= 4 + 2 + 8 + 1 + 8 && std::memcmp(s.data(), kMagic, 4) == 0) {
    const size_t body = s.size() - 8;
    const uint64_t want = GetU64(s.data() + body);
    if (XXH3_64bits(s.data(), body) == want) {
      size_t off = 4 + 2;  // skip magic + format
      p.current_term = GetU64(s.data() + off);
      off += 8;
      const uint8_t vlen = static_cast<uint8_t>(s[off]);
      off += 1;
      if (off + vlen <= body) p.voted_for.assign(s.data() + off, vlen);
    }
  }
  disk_term_ = p.current_term;
  disk_vote_ = p.voted_for;

  // --- log.bin (stop at the first torn/short/bad-checksum record) ---
  const std::string lg = ReadFile(log_path_);
  size_t off = 0;
  while (off + 20 <= lg.size()) {  // index+term+len headers = 20 bytes min
    const uint64_t index = GetU64(lg.data() + off);
    const uint64_t term = GetU64(lg.data() + off + 8);
    const uint32_t len = GetU32(lg.data() + off + 16);
    const size_t rec = 20 + len + 4;  // + checksum
    if (off + rec > lg.size()) break;  // torn tail
    const uint32_t want = GetU32(lg.data() + off + 20 + len);
    if (XXH32(lg.data() + off, 20 + len, 0) != want) break;  // corrupt → truncate here
    LogEntry e;
    e.index = index;
    e.term = term;
    if (!e.mutation.ParseFromArray(lg.data() + off + 20, static_cast<int>(len))) break;
    p.log.push_back(std::move(e));
    disk_log_keys_.push_back({index, term});
    off += rec;
  }
  return p;
}

void RaftStorage::WriteStateAtomic(uint64_t term, const std::string& vote) {
  std::string b;
  b.append(kMagic, 4);
  PutU16(b, kFormat);
  PutU64(b, term);
  b.push_back(static_cast<char>(vote.size() & 0xFF));
  b += vote;
  PutU64(b, XXH3_64bits(b.data(), b.size()));
  WriteFileAtomic(state_path_, b);
}

void RaftStorage::RewriteLog(const std::vector<LogEntry>& log) {
  std::string b;
  for (const LogEntry& e : log) b += EncodeEntry(e);
  WriteFileAtomic(log_path_, b);
}

void RaftStorage::AppendLog(const std::vector<LogEntry>& log, size_t from) {
  std::ofstream f(log_path_, std::ios::binary | std::ios::app);
  for (size_t i = from; i < log.size(); ++i) {
    const std::string rec = EncodeEntry(log[i]);
    f.write(rec.data(), static_cast<std::streamsize>(rec.size()));
  }
  f.flush();
}

void RaftStorage::Save(const Persistent& p) {
  if (p.current_term != disk_term_ || p.voted_for != disk_vote_) {
    WriteStateAtomic(p.current_term, p.voted_for);
    disk_term_ = p.current_term;
    disk_vote_ = p.voted_for;
  }

  // Longest common (index, term) prefix of the on-disk log and the new one.
  size_t common = 0;
  while (common < disk_log_keys_.size() && common < p.log.size() &&
         disk_log_keys_[common].first == p.log[common].index &&
         disk_log_keys_[common].second == p.log[common].term) {
    ++common;
  }
  if (common == disk_log_keys_.size()) {
    if (common < p.log.size()) AppendLog(p.log, common);  // pure extension
  } else {
    RewriteLog(p.log);  // a conflict truncation diverged the tail → rewrite
  }
  disk_log_keys_.clear();
  disk_log_keys_.reserve(p.log.size());
  for (const LogEntry& e : p.log) disk_log_keys_.push_back({e.index, e.term});
}

}  // namespace lucent::raft
