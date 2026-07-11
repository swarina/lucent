# Lucent

**A distributed vector search engine you can watch think.**

Lucent is a self-hostable distributed semantic search engine whose defining feature is that its own internals are the interface. Documents are embedded locally (CPU-only sentence-transformers), sharded across processes with a hand-rolled HNSW index per shard, replicated with failover, and **every query's full journey — embedding → shard routing → per-shard ANN traversal → merge → failover events — renders live** in the frontend as the primary product surface.

> Status: **early implementation.** Design and full specs are complete; the build is underway, starting at milestone M0 (single-node steel thread).

## Layers

1. **AI/ML** — local embeddings (`all-MiniLM-L6-v2`, 384-dim, CPU), hand-rolled HNSW approximate nearest-neighbor search per shard.
2. **Distributed systems** — hash + semantic sharding, primary-backup replication, a coordinator doing scatter-gather with coverage-based partial results, failure detection and failover, and (stretch) mini-Raft membership.
3. **Visualization** — a live "watch it think" view: query fan-out, per-shard HNSW traversal, merge, replication lag, and injected failure/recovery, all driven by real trace events.

## Building

Requires a local toolchain (macOS shown; see [PLAN.md §9](PLAN.md)):

```sh
brew install cmake ninja node uv
git submodule update --init            # vcpkg
cmake --preset dev && cmake --build --preset dev
uv --project py run lucent --help
```

Once past milestone M0, `uv --project py run lucent dev` will spin up a local multi-process cluster.

## Constraints

Zero recurring cost — no paid APIs, ever. Embeddings run locally on CPU; the "cluster" is N processes on one machine; the hosted demo is a static replay of recorded traces. The vector index is immutable after build (rebuild to change the corpus), and instrumentation never touches the search path (bounded buffers, drop-on-overflow, full traces only for UI-initiated queries).

## License

[MIT](LICENSE) © 2026 Swarina Jaiswal
