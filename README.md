# Lucent

**A distributed vector search engine you can watch think.**

Lucent is a self-hostable distributed semantic search engine whose defining feature is that its own internals are the interface. Documents are embedded locally (CPU-only sentence-transformers), sharded across processes with a hand-rolled HNSW index per shard, replicated with failover, and **every query's full journey — embedding → shard routing → per-shard ANN traversal → merge → failover events — renders live** in the frontend as the primary product surface.

> **Status:** milestones **M0–M5 complete**, **M6 in progress**. The full single-machine cluster runs end to end — local embeddings, hand-rolled HNSW per shard, scatter-gather with coverage-based partial results, primary-backup replication with automatic failover, a live 3D traversal inspector, a chaos surface (kill / restart / fault / node add-remove), semantic (k-means) partitioning with a recall/latency chart, and a from-scratch **mini-Raft** that owns the shard map (leader election, log replication, crash-safe persistence, a seeded 1,000-schedule invariant simulator). M6 has shipped the recorded **static demo**: the entire "watch it think" experience — fan-out, 3D HNSW traversal, and failover — replays from a captured bundle with **no backend**.

## Live demo

**▶ [swarina.github.io/lucent](https://swarina.github.io/lucent/)** — a recorded session replayed entirely in the browser (no server, no cluster). Watch queries fan out across shards, open *"watch it think"* for the 3D HNSW traversal, and see a shard get killed and its backup take over.

Every inspector view is a **shareable permalink**: the URL carries the trace as `#/trace/{id}`, so you can link someone straight to a specific query's traversal. (For live, interactive search — real ranking, your own queries, the chaos controls — run it locally, below.)

## Layers

1. **AI/ML** — local embeddings (`all-MiniLM-L6-v2`, 384-dim, CPU), hand-rolled HNSW approximate nearest-neighbor search per shard, seeded k-means semantic partitioning.
2. **Distributed systems** — sharding, primary-backup replication, a coordinator doing scatter-gather with coverage-based partial results, failure detection and failover, node add/remove, and a mini-Raft that owns the shard map through consensus.
3. **Visualization** — a live "watch it think" view: query fan-out, per-shard HNSW traversal in 3D, merge, replication lag, injected failure/recovery, and live Raft elections, all driven by real trace events.

## Architecture

```mermaid
flowchart LR
  user(["browser UI<br/>watch it think"])

  subgraph gw["Node gateway :8080"]
    rest["REST + WebSocket"]
    collector["trace collector<br/>(bounded, drop-oldest)"]
  end

  embed["embed service<br/>MiniLM · CPU"]
  coord["coordinator<br/>scatter-gather + merge"]

  subgraph shards["shards (C++ · HNSW)"]
    s0["shard-0<br/>primary → backup"]
    s1["shard-1<br/>primary → backup"]
  end

  members[("mini-Raft voters<br/>own the shard map")]

  user -->|query| rest
  rest -->|embed text| embed
  rest -->|route| coord
  coord -->|fan-out gRPC| s0 & s1
  coord -->|merged top-k| rest
  rest -->|results| user

  s0 & s1 & coord & embed -.->|trace spans| collector
  collector -.->|WS event stream| user
  members -.->|committed map| coord
```

**A query's path:** the gateway embeds the text (embed service), the coordinator routes it to the relevant shards and fans out over gRPC, each shard walks its own HNSW graph and returns its local top-k, the coordinator merges them (labeling coverage if a shard is down), and the gateway streams the result plus **every span of that journey** to the UI over a WebSocket. Failover, replication, and Raft elections emit their own events on the same stream — the visualization is a faithful read of what actually happened, never a scripted animation.

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
uv --project py run lucent ingest --config .lucent/cluster-dev.yaml --corpus testdata/corpus-2k.jsonl --n 2000
```

**4. Open the UI** at **http://localhost:8080** — type a query, watch the fan-out and span waterfall, click *"watch it think"* for the 3D HNSW traversal, and open the **cluster** panel to kill a shard and watch coverage degrade (or, with `--replicas 2`, watch a backup get promoted).

### Real semantic search

The fake encoder is for fast iteration and CI. For meaningful results, install the model and drop `--fake-embed` (first run downloads ~90 MB of `all-MiniLM-L6-v2`):

```sh
(cd py && uv sync --extra embed)
uv --project py run lucent dev --shards 2            # real MiniLM
```

To search a larger real corpus, `uv --project py run lucent corpus fetch` pulls the arXiv abstracts dump, then `lucent ingest` without `--corpus` samples from it.

### Record your own demo

`lucent record` tees a live cluster into a replay bundle (events, results, decoded traces, projections) that the frontend can play back with no backend — this is exactly what the hosted demo runs on:

```sh
uv --project py run lucent record --out web/public/bundle
```

## Limitations (by design, and honestly)

This is a learning-first portfolio project under a hard zero-cost constraint, not a production datastore. The interesting engineering is real; the scope is deliberately bounded:

- **Single-host "cluster."** The cluster is N processes on one machine. Real network partitions, clock skew across hosts, and NIC failures aren't modeled; inter-node latency is loopback. Chaos is injected in-process or via the supervisor.
- **The coordinator is a single query router.** With `--raft`, three voters own the *shard map* — so membership changes and failover commit through consensus and survive a coordinator restart — but the coordinator process that routes each individual query is still a single point on the query path.
- **The index is immutable after build.** An HNSW shard is sealed once built: no online inserts, updates, or deletes. Changing the corpus means a rebuild. This buys determinism (no concurrent insert-during-search) and is a conscious trade.
- **Single-host clocks.** Span timings come from one machine's monotonic clock; cross-node ordering is meaningful only because it *is* one clock. This is not distributed clock synchronization.
- **Traces are tiered, not total.** Only UI-initiated FULL queries capture per-hop HNSW blobs; background and rate-capped queries degrade their *trace*, never their *search*. Observability is bounded (drop-oldest on overflow, counted) and never sits on the data path.
- **`--fake-embed` ranks by a hash, not meaning.** It exercises the entire pipeline (routing, HNSW, merge, failover) deterministically for CI and instant demos, but for semantically ranked results you need the real MiniLM model.

## Constraints

Zero recurring cost — no paid APIs, ever. Embeddings run locally on CPU; the "cluster" is N processes on one machine; the hosted demo is a static replay of recorded traces. The vector index is immutable after build (rebuild to change the corpus), and instrumentation never touches the search path (bounded buffers, drop-on-overflow, full traces only for UI-initiated queries).

## Data

The bundled `testdata/corpus-2k.jsonl` and the optional larger corpus are [arXiv](https://arxiv.org/) paper metadata (titles + abstracts), used here purely as a search corpus. Thanks to arXiv for use of its open metadata; Lucent is not affiliated with or endorsed by arXiv.

## License

[MIT](LICENSE) © 2026 Swarina Jaiswal
