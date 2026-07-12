#pragma once

#include <memory>
#include <string>
#include <vector>

#include "index/hnsw.h"

namespace lucent {

// graph.bin serialization — exact byte layout per docs/data-formats.md §2.3:
// header (magic/format/n/dim/params/max_level/entry/seed), levels u8[n],
// per-layer sections (count, rows for L>=1, CSR offsets, neighbors), xxh3-64
// trailer over all preceding bytes. Only sealed graphs are saved; a sealed
// deterministic build therefore serializes byte-identically (golden gate).

void SaveHnswGraph(const HnswIndex& index, const std::string& path);

// Loads and verifies graph.bin (magic, format, trailer checksum, structural
// consistency) and adopts the caller's ids/vectors (from ids.u64 /
// vectors.f32, which the shard storage layer already checksums). Throws
// std::runtime_error naming the problem on any mismatch.
std::unique_ptr<HnswIndex> LoadHnswGraph(const std::string& path, int dim,
                                         std::vector<uint64_t> ids,
                                         std::vector<float> vectors);

}  // namespace lucent
