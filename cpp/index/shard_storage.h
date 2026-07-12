#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace lucent {

// Shard data directory (docs/data-formats.md §2):
//   manifest.json   ids.u64   vectors.f32   docs.jsonl.zst   [graph.bin @ M1]
// Load verifies the xxh3-64 of every listed file before the shard may serve;
// a mismatch throws — refuse-to-serve beats silently serving corrupt data.

struct DocMeta {
  uint64_t doc_id = 0;
  std::string title;
  std::string snippet;   // <= 400 chars
  std::string category;
};

struct ShardManifest {
  int schema = 1;
  int shard_id = 0;
  std::string replica;      // "a" | "b"
  std::string node_id;      // e.g. "shard-2a"
  uint64_t n = 0;
  int dim = 0;
  std::string metric = "ip_normalized";
  std::string index_type;   // "bruteforce" | "hnsw"
  uint64_t seed = 0;
  int m = 0;                 // hnsw params (0 when bruteforce)
  int m0 = 0;
  int ef_construction = 0;
  std::string corpus_hash;  // hex; ties the dir to its source corpus
};

struct LoadedShard {
  ShardManifest manifest;
  std::vector<uint64_t> ids;     // row -> doc_id
  std::vector<float> vectors;    // row-major n x dim
  std::vector<DocMeta> docs;     // row-aligned with ids
};

// Writes the full directory (creates it if needed), computing checksums into
// manifest.json. `vectors` must be n*dim floats; `docs` row-aligned with `ids`.
void SaveShardDir(const std::string& dir, const ShardManifest& manifest,
                  const std::vector<uint64_t>& ids,
                  const std::vector<float>& vectors,
                  const std::vector<DocMeta>& docs);

// Reads + verifies everything. Throws std::runtime_error naming the offending
// file on checksum mismatch, size mismatch, or malformed content.
LoadedShard LoadShardDir(const std::string& dir);

// xxh3-64 of a whole file, as a 16-char lowercase hex string.
std::string XxhFileHex(const std::string& path);

}  // namespace lucent
