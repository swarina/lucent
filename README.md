# Lucent

**A distributed vector search engine you can watch think.**

Lucent is a self-hostable distributed semantic search engine whose defining feature is that its own internals are the interface. Documents are embedded locally (CPU-only sentence-transformers), sharded across processes with a hand-rolled HNSW index per shard, replicated with failover, and **every query's full journey — embedding → shard routing → per-shard ANN traversal → merge → failover events — renders live** in the frontend as the primary product surface.

> **Status:** milestones **M0–M3 complete** — the full single-machine cluster runs end to end: local embeddings, hand-rolled HNSW per shard, scatter-gather with coverage-based partial results, primary-backup replication, health detection, automatic failover, a live 3D traversal inspector, and a chaos surface (kill / restart / fault / node add-remove). Next up: **M4** semantic partitioning + the recall/latency chart, **M5** mini-Raft membership, **M6** recorded static demo.

## Layers

1. **AI/ML** — local embeddings (`all-MiniLM-L6-v2`, 384-dim, CPU), hand-rolled HNSW approximate nearest-neighbor search per shard.
2. **Distributed systems** — hash sharding, primary-backup replication, a coordinator doing scatter-gather with coverage-based partial results, failure detection and failover, node add/remove, and (upcoming) mini-Raft membership.
3. **Visualization** — a live "watch it think" view: query fan-out, per-shard HNSW traversal in 3D, merge, replication lag, and injected failure/recovery, all driven by real trace events.

## Quickstart

**Prerequisites** (macOS shown; Linux equivalents work):

```sh
brew install cmake ninja node uv
git submodule update --init            # vcpkg (C++ deps)
```

**1. Build all three runtimes** — C++ engine, Node gateway, web UI:

```sh
cmake --preset dev && cmake --build --preset dev     # embed/coordinator/shard binaries
(cd gateway && npm install && npm run build)         # collector + REST/WS gateway
(cd web && npm install && npm run build)             # the UI the gateway serves
```

**2. Start a 2-shard cluster.** `--fake-embed` uses a deterministic hash encoder (instant, no model download) so you can see the machinery immediately — the whole pipeline is real, but result *ranking* isn't semantically meaningful (the UI says so):

```sh
uv --project py run lucent dev --shards 2 --fake-embed
```

**3. Ingest the bundled 2k-doc test corpus** (in a second terminal). The supervisor wrote the effective config to `.lucent/cluster-dev.yaml`:

```sh
uv --project py run lucent ingest --config .lucent/cluster-dev.yaml \
  --corpus testdata/corpus-2k.jsonl --n 2000
```

**4. Open the UI** at **http://localhost:8080** — type a query, watch the fan-out and span waterfall, click *"watch it think"* for the 3D HNSW traversal, and open the **cluster** panel to kill a shard and watch coverage degrade (or, with `--replicas 2`, watch a backup get promoted).

### Real semantic search

The fake encoder is for fast iteration and CI. For meaningful results, install the model and drop `--fake-embed` (first run downloads ~90 MB of `all-MiniLM-L6-v2`):

```sh
(cd py && uv sync --extra embed)
uv --project py run lucent dev --shards 2            # real MiniLM
```

To search a larger real corpus, `uv --project py run lucent corpus fetch` pulls the arXiv abstracts dump, then `lucent ingest` without `--corpus` samples from it.

## Constraints

Zero recurring cost — no paid APIs, ever. Embeddings run locally on CPU; the "cluster" is N processes on one machine; the hosted demo (M6) is a static replay of recorded traces. The vector index is immutable after build (rebuild to change the corpus), and instrumentation never touches the search path (bounded buffers, drop-on-overflow, full traces only for UI-initiated queries).

## License

[MIT](LICENSE) © 2026 Swarina Jaiswal
