"""Lucent ingest pipeline (PLAN §6.4, data-formats.md §4).

fetch -> normalize -> (stratified) sample -> embed (disk-cached) -> partition
-> load shards (InsertBatch stream) -> seal -> queries.json

Everything seeded and deterministic. Embedding goes through the running embed
service (a real, visible hop) — `lucent ingest` therefore needs `lucent dev`
or at least `lucent embedsvc` up.
"""

from __future__ import annotations

import gzip
import json
import logging
import pathlib
import random
import urllib.request
from dataclasses import dataclass
from typing import Callable, Iterable, Iterator, Sequence

import xxhash

log = logging.getLogger("lucent.ingest")

CORPUS_URL = (
    "https://huggingface.co/datasets/gfissore/arxiv-abstracts-2021"
    "/resolve/main/arxiv-abstracts.jsonl.gz"
)
SNIPPET_CHARS = 400
EMBED_RPC_BATCH = 256
CACHE_BLOCK_ROWS = 8192

# Stratification buckets -> share of the sample. Physics subfields collapse
# into one bucket so shard-category labels stay legible under semantic
# partitioning (M4).
BUCKET_WEIGHTS: dict[str, float] = {
    "cs": 0.35,
    "math": 0.20,
    "physics": 0.30,
    "q-bio": 0.05,
    "other": 0.10,
}
_PHYSICS_PREFIXES = (
    "hep-", "astro-ph", "cond-mat", "gr-qc", "nucl-", "quant-ph", "physics",
    "math-ph", "nlin",
)


def doc_id_of(arxiv_id: str) -> int:
    return xxhash.xxh3_64_intdigest(arxiv_id.encode("utf-8"))


def bucket_of(category: str) -> str:
    if category.startswith("cs."):
        return "cs"
    if category.startswith("math."):
        return "math"
    if category.startswith(_PHYSICS_PREFIXES):
        return "physics"
    if category.startswith("q-bio"):
        return "q-bio"
    return "other"


def normalize_record(raw: dict) -> dict | None:
    """Raw arxiv-abstracts record -> normalized doc, or None to skip."""
    arxiv_id = raw.get("id")
    title = " ".join((raw.get("title") or "").split())
    abstract = " ".join((raw.get("abstract") or "").split())
    categories = raw.get("categories") or []
    if isinstance(categories, str):  # some dumps ship space-separated strings
        categories = categories.split()
    if not arxiv_id or not title or len(abstract) < 100 or not categories:
        return None
    return {
        "doc_id": doc_id_of(arxiv_id),
        "arxiv_id": arxiv_id,
        "title": title,
        "abstract": abstract,
        "category": categories[0],
    }


def stratified_reservoir(
    records: Iterable[dict], n: int, seed: int
) -> list[dict]:
    """Single-pass per-bucket reservoir sample with quota weights."""
    rng = random.Random(seed)
    quotas = {b: max(1, int(n * w)) for b, w in BUCKET_WEIGHTS.items()}
    reservoirs: dict[str, list[dict]] = {b: [] for b in quotas}
    seen: dict[str, int] = {b: 0 for b in quotas}

    for rec in records:
        b = bucket_of(rec["category"])
        seen[b] += 1
        res, quota = reservoirs[b], quotas[b]
        if len(res) < quota:
            res.append(rec)
        else:
            j = rng.randrange(seen[b])
            if j < quota:
                res[j] = rec

    out = [r for res in reservoirs.values() for r in res]
    # Deterministic global order; also where doc_id collisions would surface.
    out.sort(key=lambda r: r["doc_id"])
    ids = {r["doc_id"] for r in out}
    if len(ids) != len(out):
        raise RuntimeError("doc_id collision in sample — refusing to continue")
    return out


def iter_corpus_gz(path: pathlib.Path, limit_bytes: int | None = None) -> Iterator[dict]:
    """Yields normalized docs from a (possibly truncated) .jsonl.gz stream."""
    with open(path, "rb") as f:
        raw_stream: Iterable[bytes]
        gz = gzip.GzipFile(fileobj=f)
        try:
            for line in gz:
                rec = normalize_record(json.loads(line))
                if rec is not None:
                    yield rec
                if limit_bytes is not None and f.tell() > limit_bytes:
                    break
        except (EOFError, gzip.BadGzipFile):
            pass  # truncated tail of a partial download — expected


def fetch_corpus_file(cache_dir: pathlib.Path, max_bytes: int | None = None) -> pathlib.Path:
    """Downloads (once) the corpus dump into the cache; supports partial fetch."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    suffix = f".head{max_bytes}" if max_bytes else ""
    out = cache_dir / f"arxiv-abstracts.jsonl.gz{suffix}"
    if out.exists() and out.stat().st_size > 0:
        return out
    req = urllib.request.Request(CORPUS_URL)
    if max_bytes:
        req.add_header("Range", f"bytes=0-{max_bytes - 1}")
    log.info("downloading %s%s ...", CORPUS_URL, f" (first {max_bytes}B)" if max_bytes else "")
    tmp = out.with_suffix(".part")
    with urllib.request.urlopen(req) as resp, open(tmp, "wb") as w:
        while chunk := resp.read(1 << 20):
            w.write(chunk)
    tmp.rename(out)
    return out


def corpus_hash(docs: Sequence[dict]) -> str:
    """xxh3 over (doc_id, abstract) in doc_id order (data-formats.md §4)."""
    h = xxhash.xxh3_64()
    for d in sorted(docs, key=lambda r: r["doc_id"]):
        h.update(d["doc_id"].to_bytes(8, "little"))
        h.update(d["abstract"].encode("utf-8"))
    return h.hexdigest()


def embed_with_cache(
    docs: Sequence[dict],
    embed_batch: Callable[[list[str]], "object"],
    cache_root: pathlib.Path,
    model_name: str,
    dim: int,
):
    """Returns an (n, dim) float32 array; disk-cached by corpus hash.

    `embed_batch(texts) -> array-like (len(texts), dim)` — normally the embed
    service; injectable for tests.
    """
    import numpy as np

    chash = corpus_hash(docs)
    cache_dir = cache_root / "embeddings" / model_name / chash
    manifest_path = cache_dir / "manifest.json"

    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        blocks = []
        for blk in manifest["blocks"]:
            data = (cache_dir / blk["file"]).read_bytes()
            if xxhash.xxh3_64_hexdigest(data) != blk["xxh3"]:
                raise RuntimeError(f"embedding cache corrupt: {blk['file']}")
            blocks.append(np.load(cache_dir / blk["file"]))
        vecs = np.concatenate(blocks, axis=0)
        if vecs.shape != (len(docs), dim):
            raise RuntimeError("embedding cache shape mismatch")
        log.info("embedding cache hit (%s)", chash)
        return vecs, chash

    texts = [d["abstract"] for d in docs]
    parts = []
    for i in range(0, len(texts), EMBED_RPC_BATCH):
        part = np.asarray(embed_batch(texts[i : i + EMBED_RPC_BATCH]), dtype=np.float32)
        parts.append(part.reshape(-1, dim))
    vecs = np.concatenate(parts, axis=0) if parts else np.zeros((0, dim), np.float32)
    if vecs.shape != (len(docs), dim):
        raise RuntimeError(f"embed returned {vecs.shape}, want {(len(docs), dim)}")

    cache_dir.mkdir(parents=True, exist_ok=True)
    blocks_meta = []
    for bi, start in enumerate(range(0, len(docs), CACHE_BLOCK_ROWS)):
        name = f"block-{bi:04d}.npy"
        np.save(cache_dir / name, vecs[start : start + CACHE_BLOCK_ROWS])
        data = (cache_dir / name).read_bytes()
        blocks_meta.append(
            {"file": name, "rows": int(min(CACHE_BLOCK_ROWS, len(docs) - start)),
             "xxh3": xxhash.xxh3_64_hexdigest(data)}
        )
    manifest_path.write_text(json.dumps(
        {"schema": 1, "model": model_name, "dim": dim, "n": len(docs),
         "block_rows": CACHE_BLOCK_ROWS, "blocks": blocks_meta}, indent=1))
    return vecs, chash


def hash_partition(doc_ids: Sequence[int], shards: int) -> list[int]:
    """shard = xxh3_64(u64-LE doc_id) % shards (data-formats.md §1)."""
    return [
        xxhash.xxh3_64_intdigest(did.to_bytes(8, "little")) % shards
        for did in doc_ids
    ]


def write_queries(docs: Sequence[dict], out_path: pathlib.Path, n: int, seed: int) -> None:
    rng = random.Random(seed)
    picks = rng.sample(range(len(docs)), min(n, len(docs)))
    queries = [
        {"qid": qi, "text": docs[i]["title"], "source_doc_id": docs[i]["doc_id"]}
        for qi, i in enumerate(sorted(picks))
    ]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({"schema": 1, "seed": seed, "queries": queries}, indent=1))


@dataclass
class LoadResult:
    shard_doc_counts: list[int]
    sealed: bool


def load_shards(cfg, docs, vectors, assignments, *, seal: bool = True) -> LoadResult:
    """Streams docs to each shard primary via InsertBatch, then seals."""
    import grpc

    from lucent.v1 import shard_pb2, shard_pb2_grpc

    shards = cfg.cluster.shards
    per_shard: list[list[int]] = [[] for _ in range(shards)]
    for row, s in enumerate(assignments):
        per_shard[s].append(row)

    counts = []
    for s in range(shards):
        addr = cfg.shard_addr(s, "a")
        channel = grpc.insecure_channel(addr)
        stub = shard_pb2_grpc.ShardServiceStub(channel)

        def batches(rows: list[int]) -> Iterator[shard_pb2.InsertBatchRequest]:
            seq = 0
            for i in range(0, len(rows), EMBED_RPC_BATCH):
                seq += 1
                req = shard_pb2.InsertBatchRequest(seq=seq)
                for row in rows[i : i + EMBED_RPC_BATCH]:
                    d = docs[row]
                    req.docs.add(
                        doc_id=d["doc_id"],
                        vector=vectors[row].tolist(),
                        title=d["title"],
                        snippet=d["abstract"][:SNIPPET_CHARS],
                        category=d["category"],
                    )
                yield req

        resp = stub.InsertBatch(batches(per_shard[s]))
        if resp.doc_count != len(per_shard[s]):
            raise RuntimeError(
                f"shard {s}: loaded {resp.doc_count}, want {len(per_shard[s])}"
            )
        if seal:
            stub.SealIndex(shard_pb2.SealRequest())
        counts.append(len(per_shard[s]))
        channel.close()
        log.info("shard %d: %d docs%s", s, counts[-1], " (sealed)" if seal else "")
    return LoadResult(shard_doc_counts=counts, sealed=seal)


def make_embed_batch_fn(embed_addr: str, dim: int) -> Callable[[list[str]], "object"]:
    import grpc
    import numpy as np

    from lucent.v1 import embed_pb2, embed_pb2_grpc

    channel = grpc.insecure_channel(embed_addr)
    stub = embed_pb2_grpc.EmbedServiceStub(channel)

    def fn(texts: list[str]):
        resp = stub.Embed(embed_pb2.EmbedRequest(texts=texts), timeout=120)
        return np.asarray(resp.vectors, dtype=np.float32).reshape(-1, dim)

    return fn


def run(config_path: str, corpus_path: str, n: int, seed: int | None = None) -> None:
    """Full pipeline against a running cluster (embedsvc + shard primaries)."""
    from lucent import config as config_mod

    cfg = config_mod.load(config_path)
    seed = cfg.index_seed if seed is None else seed

    src = pathlib.Path(corpus_path)
    log.info("sampling %d docs from %s ...", n, src)
    docs = stratified_reservoir(iter_corpus_gz(src) if src.suffix == ".gz"
                                else iter_jsonl(src), n, seed)
    log.info("sampled %d docs; embedding ...", len(docs))

    embed_fn = make_embed_batch_fn(cfg.embed_addr, cfg.model.dim)
    vectors, chash = embed_with_cache(
        docs, embed_fn, cfg.paths.cache, cfg.model.name, cfg.model.dim)
    log.info("embedded (corpus_hash=%s); partitioning + loading ...", chash)

    assignments = hash_partition([d["doc_id"] for d in docs], cfg.cluster.shards)
    load_shards(cfg, docs, vectors, assignments)
    write_queries(docs, cfg.paths.data / "queries.json", n=1000, seed=seed)
    log.info("ingest complete: %d docs across %d shards", len(docs), cfg.cluster.shards)


def iter_jsonl(path: pathlib.Path) -> Iterator[dict]:
    """Pre-normalized corpus files (e.g. testdata/corpus-2k.jsonl)."""
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)
