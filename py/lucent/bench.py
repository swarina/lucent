"""Recall/latency bench harness (M1-T4, internals.md §8).

The brute-force oracle referees the live cluster: query vectors come from the
running embed service (exactly what the coordinator uses), ground truth from
NumPy over the ingest's embedding cache, and each (ef, probe) config is swept
against the real coordinator at TRACE_LEVEL_NONE (the bench tier).

Outputs bench/results/bench.json (data-formats.md §5) — consumed by CI gates
(--gate) and the ops-panel recall/latency scatter (same artifact, two
consumers, by design).
"""

from __future__ import annotations

import json
import logging
import pathlib
import time
from dataclasses import dataclass

log = logging.getLogger("lucent.bench")

K = 10


@dataclass
class ConfigResult:
    partitioning: str
    shards: int
    probe: int
    ef: int
    recall_at_10: float
    p50_ms: float
    p99_ms: float
    mean_visited: float


def load_oracle(cfg, data_dir: pathlib.Path):
    """(doc_ids u64[n], vectors f32[n, dim]) from the ingest sidecars."""
    import numpy as np

    manifest = json.loads((data_dir / "ingest-manifest.json").read_text())
    chash = manifest["corpus_hash"]
    cache_dir = cfg.paths.cache / "embeddings" / manifest["model"] / chash
    cache_manifest = json.loads((cache_dir / "manifest.json").read_text())
    vecs = np.concatenate(
        [np.load(cache_dir / blk["file"]) for blk in cache_manifest["blocks"]],
        axis=0,
    )
    ids = np.fromfile(data_dir / "doc_ids.u64", dtype="<u8")
    if vecs.shape != (manifest["n"], manifest["dim"]) or len(ids) != manifest["n"]:
        raise RuntimeError("oracle sidecars inconsistent with ingest manifest")
    return ids, vecs, manifest


def oracle_topk(ids, vecs, qvecs, k: int = K):
    """Exact top-k doc_id sets per query: (-score, doc_id) order."""
    import numpy as np

    truth = []
    scores = qvecs @ vecs.T  # (nq, n)
    for row in scores:
        order = np.lexsort((ids, -row))[:k]
        truth.append({int(ids[i]) for i in order})
    return truth


def run_sweep(config_path: str, ef_list: list[int], probe_list: list[int],
              max_queries: int, out_path: pathlib.Path) -> list[ConfigResult]:
    import grpc
    import numpy as np

    from lucent import config as config_mod
    from lucent.v1 import (common_pb2, coordinator_pb2, coordinator_pb2_grpc,
                           embed_pb2, embed_pb2_grpc)

    cfg = config_mod.load(config_path)
    data_dir = cfg.paths.data
    queries = json.loads((data_dir / "queries.json").read_text())["queries"]
    queries = queries[:max_queries]
    ids, vecs, manifest = load_oracle(cfg, data_dir)
    log.info("oracle: %d docs, %d queries", len(ids), len(queries))

    # Query vectors via the embed service — the same embedding the coordinator
    # computes, so oracle and live search see identical geometry.
    embed_channel = grpc.insecure_channel(cfg.embed_addr)
    embed = embed_pb2_grpc.EmbedServiceStub(embed_channel)
    qvecs_parts = []
    texts = [q["text"] for q in queries]
    for i in range(0, len(texts), 256):
        resp = embed.Embed(embed_pb2.EmbedRequest(texts=texts[i:i + 256]), timeout=120)
        qvecs_parts.append(
            np.asarray(resp.vectors, dtype=np.float32).reshape(-1, resp.dim))
    qvecs = np.concatenate(qvecs_parts, axis=0)
    truth = oracle_topk(ids, vecs, qvecs)

    coord = coordinator_pb2_grpc.CoordinatorServiceStub(
        grpc.insecure_channel(cfg.coordinator_addr))

    results: list[ConfigResult] = []
    for probe in probe_list:
        for ef in ef_list:
            lat_ms: list[float] = []
            visited: list[int] = []
            hit = 0
            total = 0
            for qi, q in enumerate(queries):
                t0 = time.perf_counter()
                resp = coord.Query(coordinator_pb2.QueryRequest(
                    text=q["text"], k=K, ef_search=ef, probe=probe,
                    trace_level=common_pb2.TRACE_LEVEL_NONE), timeout=10)
                lat_ms.append((time.perf_counter() - t0) * 1000)
                visited.append(resp.visited_total)
                got = {h.doc_id for h in resp.hits}
                hit += len(got & truth[qi])
                total += K
            lat = np.asarray(lat_ms)
            r = ConfigResult(
                partitioning=manifest["partitioning"],
                shards=manifest["shards"],
                probe=probe, ef=ef,
                recall_at_10=hit / total,
                p50_ms=float(np.percentile(lat, 50)),
                p99_ms=float(np.percentile(lat, 99)),
                mean_visited=float(np.mean(visited)),
            )
            results.append(r)
            log.info("probe=%-3s ef=%-4d recall@10=%.3f p50=%.1fms p99=%.1fms visited=%.0f",
                     probe or "all", ef, r.recall_at_10, r.p50_ms, r.p99_ms,
                     r.mean_visited)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({
        "schema": 1,
        "recorded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "corpus_hash": manifest["corpus_hash"],
        "model": manifest["model"],
        "oracle": "bruteforce-f32",
        "query_set": f"queries.json[:{len(queries)}]",
        "configs": [vars(r) for r in results],
    }, indent=1) + "\n")
    log.info("wrote %s", out_path)
    return results


def check_gates(results: list[ConfigResult]) -> list[str]:
    """PLAN §7 gates that apply to a full-probe sweep. Returns failures."""
    failures = []
    full = {r.ef: r for r in results if r.probe == 0}
    if 100 in full and full[100].recall_at_10 < 0.95:
        failures.append(
            f"recall@10 ef=100 full-probe = {full[100].recall_at_10:.3f} < 0.95")
    efs = sorted(full)
    for lo, hi in zip(efs, efs[1:]):
        if full[hi].recall_at_10 < full[lo].recall_at_10 - 0.02:
            failures.append(
                f"recall not ~monotonic in ef: ef={hi} ({full[hi].recall_at_10:.3f}) "
                f"< ef={lo} ({full[lo].recall_at_10:.3f}) - 0.02")
    return failures


def run_overhead(config_path: str, n_queries: int = 300) -> dict:
    """SPANS/FULL latency overhead vs NONE, measured at the SHARD (the query
    path is embed-dominated, which would hide instrumentation cost)."""
    import grpc
    import numpy as np

    from lucent import config as config_mod
    from lucent.v1 import common_pb2, embed_pb2, embed_pb2_grpc, shard_pb2, shard_pb2_grpc

    cfg = config_mod.load(config_path)
    embed = embed_pb2_grpc.EmbedServiceStub(grpc.insecure_channel(cfg.embed_addr))
    qvec = list(embed.Embed(
        embed_pb2.EmbedRequest(texts=["instrumentation overhead probe"]),
        timeout=60).vectors)
    shard = shard_pb2_grpc.ShardServiceStub(
        grpc.insecure_channel(cfg.shard_addr(0, "a")))

    def measure(level, label: str) -> float:
        lats = []
        for i in range(n_queries):
            req = shard_pb2.SearchRequest(
                trace_id=f"bench-{label}-{i:06d}".encode()[:16],
                vector=qvec, k=K, ef_search=100, trace_level=level,
                shard_map_epoch=1)
            t0 = time.perf_counter()
            shard.Search(req, timeout=5)
            lats.append((time.perf_counter() - t0) * 1e6)
        return float(np.percentile(np.asarray(lats), 50))

    measure(common_pb2.TRACE_LEVEL_NONE, "warm")  # warmup
    none_us = measure(common_pb2.TRACE_LEVEL_NONE, "none")
    spans_us = measure(common_pb2.TRACE_LEVEL_SPANS, "spans")
    full_us = measure(common_pb2.TRACE_LEVEL_FULL, "full")
    out = {
        "none_p50_us": none_us,
        "spans_p50_us": spans_us,
        "full_p50_us": full_us,
        "spans_overhead_pct": (spans_us - none_us) / none_us * 100,
        "full_overhead_pct": (full_us - none_us) / none_us * 100,
    }
    log.info("overhead p50: NONE=%.0fus SPANS=%.0fus (%+.1f%%) FULL=%.0fus (%+.1f%%)",
             none_us, spans_us, out["spans_overhead_pct"], full_us,
             out["full_overhead_pct"])
    return out
