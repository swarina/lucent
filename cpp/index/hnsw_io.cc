#include "index/hnsw_io.h"

#include <xxhash.h>

#include <cstring>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace lucent {
namespace {

constexpr char kMagic[4] = {'L', 'C', 'N', 'T'};
constexpr uint16_t kFormat = 1;

// Little-endian append helpers (we only target LE hosts; static_assert-ish
// guard below keeps the assumption visible).
template <typename T>
void Put(std::string& out, T value) {
  static_assert(std::is_trivially_copyable_v<T>);
  out.append(reinterpret_cast<const char*>(&value), sizeof(T));
}

template <typename T>
void PutVec(std::string& out, const std::vector<T>& v) {
  out.append(reinterpret_cast<const char*>(v.data()), v.size() * sizeof(T));
}

class Reader {
 public:
  Reader(const std::string& buf, const std::string& path)
      : buf_(buf), path_(path) {}

  template <typename T>
  T Get() {
    T value;
    Need(sizeof(T));
    std::memcpy(&value, buf_.data() + pos_, sizeof(T));
    pos_ += sizeof(T);
    return value;
  }

  template <typename T>
  std::vector<T> GetVec(size_t count) {
    std::vector<T> v(count);
    Need(count * sizeof(T));
    std::memcpy(v.data(), buf_.data() + pos_, count * sizeof(T));
    pos_ += count * sizeof(T);
    return v;
  }

  size_t pos() const { return pos_; }

 private:
  void Need(size_t bytes) {
    if (pos_ + bytes > buf_.size()) {
      throw std::runtime_error("graph.bin: truncated file " + path_);
    }
  }
  const std::string& buf_;
  const std::string path_;
  size_t pos_ = 0;
};

}  // namespace

void SaveHnswGraph(const HnswIndex& index, const std::string& path) {
  if (!index.Sealed()) {
    throw std::logic_error("SaveHnswGraph: only sealed graphs are serialized");
  }
  const size_t n = index.Size();
  const auto& params = index.params();

  std::string out;
  out.reserve(64 + n + index.EdgeCount() * sizeof(uint32_t));

  out.append(kMagic, 4);
  Put<uint16_t>(out, kFormat);
  Put<uint16_t>(out, 0);  // flags
  Put<uint64_t>(out, n);
  Put<uint32_t>(out, static_cast<uint32_t>(index.Dim()));
  Put<uint32_t>(out, static_cast<uint32_t>(params.m));
  Put<uint32_t>(out, static_cast<uint32_t>(params.m0));
  Put<uint32_t>(out, static_cast<uint32_t>(params.ef_construction));
  Put<uint32_t>(out, static_cast<uint32_t>(index.max_level()));
  Put<uint32_t>(out, 0);  // pad
  Put<uint64_t>(out, index.entry_row());
  Put<uint64_t>(out, params.seed);
  PutVec(out, index.levels());

  for (int lc = 0; lc <= index.max_level(); ++lc) {
    // Rows present at this layer (all rows at layer 0; list omitted there).
    std::vector<uint32_t> rows;
    if (lc == 0) {
      Put<uint64_t>(out, n);
      rows.resize(n);
      for (uint32_t r = 0; r < n; ++r) rows[r] = r;
    } else {
      for (uint32_t r = 0; r < n; ++r) {
        if (index.levels()[r] >= lc) rows.push_back(r);
      }
      Put<uint64_t>(out, rows.size());
      PutVec(out, rows);
    }
    std::vector<uint64_t> offsets;
    offsets.reserve(rows.size() + 1);
    offsets.push_back(0);
    std::vector<uint32_t> neighbors;
    for (uint32_t r : rows) {
      const auto& nbrs = index.Neighbors(lc, r);
      neighbors.insert(neighbors.end(), nbrs.begin(), nbrs.end());
      offsets.push_back(neighbors.size());
    }
    PutVec(out, offsets);
    PutVec(out, neighbors);
  }

  const uint64_t checksum = XXH3_64bits(out.data(), out.size());
  Put<uint64_t>(out, checksum);

  std::ofstream f(path, std::ios::binary | std::ios::trunc);
  if (!f) throw std::runtime_error("graph.bin: cannot write " + path);
  f.write(out.data(), static_cast<std::streamsize>(out.size()));
  if (!f) throw std::runtime_error("graph.bin: short write to " + path);
}

std::unique_ptr<HnswIndex> LoadHnswGraph(const std::string& path, int dim,
                                         std::vector<uint64_t> ids,
                                         std::vector<float> vectors) {
  std::ifstream f(path, std::ios::binary);
  if (!f) throw std::runtime_error("graph.bin: cannot open " + path);
  std::ostringstream ss;
  ss << f.rdbuf();
  const std::string buf = std::move(ss).str();
  if (buf.size() < 0x38 + sizeof(uint64_t)) {
    throw std::runtime_error("graph.bin: too small: " + path);
  }

  // Trailer first: nothing inside is trusted until the checksum passes.
  uint64_t stored = 0;
  std::memcpy(&stored, buf.data() + buf.size() - sizeof(uint64_t), sizeof(uint64_t));
  const uint64_t computed = XXH3_64bits(buf.data(), buf.size() - sizeof(uint64_t));
  if (stored != computed) {
    throw std::runtime_error("graph.bin: checksum mismatch for " + path);
  }

  Reader r(buf, path);
  char magic[4];
  std::memcpy(magic, r.GetVec<char>(4).data(), 4);
  if (std::memcmp(magic, kMagic, 4) != 0) {
    throw std::runtime_error("graph.bin: bad magic in " + path);
  }
  if (r.Get<uint16_t>() != kFormat) {
    throw std::runtime_error("graph.bin: unsupported format in " + path);
  }
  r.Get<uint16_t>();  // flags
  const auto n = r.Get<uint64_t>();
  const auto file_dim = r.Get<uint32_t>();
  HnswIndex::Params params;
  params.m = static_cast<int>(r.Get<uint32_t>());
  params.m0 = static_cast<int>(r.Get<uint32_t>());
  params.ef_construction = static_cast<int>(r.Get<uint32_t>());
  const auto max_level = static_cast<int>(r.Get<uint32_t>());
  r.Get<uint32_t>();  // pad
  const auto entry_row = static_cast<uint32_t>(r.Get<uint64_t>());
  params.seed = r.Get<uint64_t>();

  if (file_dim != static_cast<uint32_t>(dim) || n != ids.size() ||
      vectors.size() != n * static_cast<size_t>(dim)) {
    throw std::runtime_error("graph.bin: header disagrees with ids/vectors for " + path);
  }

  std::vector<uint8_t> levels = r.GetVec<uint8_t>(n);

  std::vector<std::vector<std::vector<uint32_t>>> adjacency(
      static_cast<size_t>(max_level) + 1);
  for (int lc = 0; lc <= max_level; ++lc) {
    const auto count = r.Get<uint64_t>();
    std::vector<uint32_t> rows;
    if (lc == 0) {
      if (count != n) throw std::runtime_error("graph.bin: layer0 count != n");
      rows.resize(n);
      for (uint32_t i = 0; i < n; ++i) rows[i] = i;
    } else {
      rows = r.GetVec<uint32_t>(count);
    }
    const std::vector<uint64_t> offsets = r.GetVec<uint64_t>(count + 1);
    const std::vector<uint32_t> neighbors =
        r.GetVec<uint32_t>(offsets.empty() ? 0 : offsets.back());

    auto& layer = adjacency[static_cast<size_t>(lc)];
    layer.assign(n, {});
    for (size_t i = 0; i < rows.size(); ++i) {
      const uint32_t row = rows[i];
      if (row >= n) throw std::runtime_error("graph.bin: row out of range");
      for (uint64_t j = offsets[i]; j < offsets[i + 1]; ++j) {
        if (neighbors[j] >= n) {
          throw std::runtime_error("graph.bin: neighbor out of range");
        }
        layer[row].push_back(neighbors[j]);
      }
    }
  }

  return std::make_unique<HnswIndex>(dim, params, std::move(ids),
                                     std::move(vectors), std::move(levels),
                                     std::move(adjacency), entry_row, max_level);
}

}  // namespace lucent
