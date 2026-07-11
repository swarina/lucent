#include "index/shard_storage.h"

#include <xxhash.h>
#include <zstd.h>

#include <nlohmann/json.hpp>

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace lucent {
namespace {

namespace fs = std::filesystem;
using nlohmann::json;

std::string ReadFileBytes(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) throw std::runtime_error("shard storage: cannot open " + path);
  std::ostringstream ss;
  ss << in.rdbuf();
  return std::move(ss).str();
}

void WriteFileBytes(const std::string& path, const void* data, size_t size) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out) throw std::runtime_error("shard storage: cannot write " + path);
  out.write(static_cast<const char*>(data), static_cast<std::streamsize>(size));
  if (!out) throw std::runtime_error("shard storage: short write to " + path);
}

std::string HashHex(const void* data, size_t size) {
  const XXH64_hash_t h = XXH3_64bits(data, size);
  char buf[17];
  std::snprintf(buf, sizeof(buf), "%016llx",
                static_cast<unsigned long long>(h));
  return std::string(buf);
}

std::string CompressZstd(const std::string& raw) {
  const size_t bound = ZSTD_compressBound(raw.size());
  std::string out(bound, '\0');
  const size_t written =
      ZSTD_compress(out.data(), bound, raw.data(), raw.size(), /*level=*/3);
  if (ZSTD_isError(written)) {
    throw std::runtime_error(std::string("zstd compress: ") +
                             ZSTD_getErrorName(written));
  }
  out.resize(written);
  return out;
}

std::string DecompressZstd(const std::string& compressed,
                           const std::string& what) {
  const unsigned long long raw_size =
      ZSTD_getFrameContentSize(compressed.data(), compressed.size());
  if (raw_size == ZSTD_CONTENTSIZE_ERROR || raw_size == ZSTD_CONTENTSIZE_UNKNOWN) {
    throw std::runtime_error("zstd: bad frame in " + what);
  }
  std::string out(raw_size, '\0');
  const size_t got = ZSTD_decompress(out.data(), out.size(), compressed.data(),
                                     compressed.size());
  if (ZSTD_isError(got) || got != raw_size) {
    throw std::runtime_error("zstd: decompress failed for " + what);
  }
  return out;
}

}  // namespace

std::string XxhFileHex(const std::string& path) {
  const std::string bytes = ReadFileBytes(path);
  return HashHex(bytes.data(), bytes.size());
}

void SaveShardDir(const std::string& dir, const ShardManifest& manifest,
                  const std::vector<uint64_t>& ids,
                  const std::vector<float>& vectors,
                  const std::vector<DocMeta>& docs) {
  if (manifest.n != ids.size() ||
      vectors.size() != ids.size() * static_cast<size_t>(manifest.dim) ||
      docs.size() != ids.size()) {
    throw std::invalid_argument("SaveShardDir: manifest/ids/vectors/docs disagree");
  }
  fs::create_directories(dir);

  WriteFileBytes(dir + "/ids.u64", ids.data(), ids.size() * sizeof(uint64_t));
  WriteFileBytes(dir + "/vectors.f32", vectors.data(),
                 vectors.size() * sizeof(float));

  std::ostringstream jsonl;
  for (const DocMeta& d : docs) {
    jsonl << json{{"doc_id", d.doc_id},
                  {"title", d.title},
                  {"snippet", d.snippet},
                  {"category", d.category}}
                 .dump()
          << '\n';
  }
  const std::string compressed = CompressZstd(std::move(jsonl).str());
  WriteFileBytes(dir + "/docs.jsonl.zst", compressed.data(), compressed.size());

  json files;
  for (const char* name : {"ids.u64", "vectors.f32", "docs.jsonl.zst"}) {
    const std::string path = dir + "/" + name;
    files[name] = {{"bytes", fs::file_size(path)}, {"xxh3", XxhFileHex(path)}};
  }
  const json m = {
      {"schema", manifest.schema},   {"shard_id", manifest.shard_id},
      {"replica", manifest.replica}, {"node_id", manifest.node_id},
      {"n", manifest.n},             {"dim", manifest.dim},
      {"metric", manifest.metric},   {"index", {{"type", manifest.index_type},
                                                {"seed", manifest.seed}}},
      {"corpus_hash", manifest.corpus_hash},
      {"files", files},
  };
  const std::string pretty = m.dump(2) + "\n";
  WriteFileBytes(dir + "/manifest.json", pretty.data(), pretty.size());
}

LoadedShard LoadShardDir(const std::string& dir) {
  const json m = json::parse(ReadFileBytes(dir + "/manifest.json"));

  LoadedShard out;
  out.manifest.schema = m.at("schema").get<int>();
  out.manifest.shard_id = m.at("shard_id").get<int>();
  out.manifest.replica = m.at("replica").get<std::string>();
  out.manifest.node_id = m.at("node_id").get<std::string>();
  out.manifest.n = m.at("n").get<uint64_t>();
  out.manifest.dim = m.at("dim").get<int>();
  out.manifest.metric = m.at("metric").get<std::string>();
  out.manifest.index_type = m.at("index").at("type").get<std::string>();
  out.manifest.seed = m.at("index").at("seed").get<uint64_t>();
  out.manifest.corpus_hash = m.at("corpus_hash").get<std::string>();

  // Verify every listed file before trusting any of it.
  for (const auto& [name, meta] : m.at("files").items()) {
    const std::string path = dir + "/" + name;
    const std::string bytes = ReadFileBytes(path);
    if (bytes.size() != meta.at("bytes").get<size_t>()) {
      throw std::runtime_error("shard storage: size mismatch for " + path);
    }
    if (HashHex(bytes.data(), bytes.size()) != meta.at("xxh3").get<std::string>()) {
      throw std::runtime_error("shard storage: checksum mismatch for " + path);
    }
  }

  const std::string ids_bytes = ReadFileBytes(dir + "/ids.u64");
  if (ids_bytes.size() != out.manifest.n * sizeof(uint64_t)) {
    throw std::runtime_error("shard storage: ids.u64 wrong length");
  }
  out.ids.resize(out.manifest.n);
  std::memcpy(out.ids.data(), ids_bytes.data(), ids_bytes.size());

  const std::string vec_bytes = ReadFileBytes(dir + "/vectors.f32");
  const size_t want =
      out.manifest.n * static_cast<size_t>(out.manifest.dim) * sizeof(float);
  if (vec_bytes.size() != want) {
    throw std::runtime_error("shard storage: vectors.f32 wrong length");
  }
  out.vectors.resize(want / sizeof(float));
  std::memcpy(out.vectors.data(), vec_bytes.data(), vec_bytes.size());

  const std::string jsonl =
      DecompressZstd(ReadFileBytes(dir + "/docs.jsonl.zst"), "docs.jsonl.zst");
  out.docs.reserve(out.manifest.n);
  std::istringstream lines(jsonl);
  std::string line;
  while (std::getline(lines, line)) {
    if (line.empty()) continue;
    const json d = json::parse(line);
    DocMeta doc;
    doc.doc_id = d.at("doc_id").get<uint64_t>();
    doc.title = d.at("title").get<std::string>();
    doc.snippet = d.at("snippet").get<std::string>();
    doc.category = d.at("category").get<std::string>();
    out.docs.push_back(std::move(doc));
  }
  if (out.docs.size() != out.manifest.n) {
    throw std::runtime_error("shard storage: docs.jsonl.zst row count mismatch");
  }
  return out;
}

}  // namespace lucent
