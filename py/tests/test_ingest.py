"""Ingest pipeline unit tests — no network, no model, no cluster."""

import collections
import json
import pathlib

import numpy as np
import pytest

from lucent import ingest


def make_raw(i: int, category: str = "cs.LG") -> dict:
    return {
        "id": f"2101.{i:05d}",
        "title": f"Paper {i}\n  with folded   whitespace",
        "abstract": f"Abstract for paper {i}. " + "Lorem ipsum dolor sit amet. " * 10,
        "categories": [category, "cs.AI"],
    }


def test_normalize_record() -> None:
    rec = ingest.normalize_record(make_raw(7))
    assert rec is not None
    assert rec["arxiv_id"] == "2101.00007"
    assert rec["doc_id"] == ingest.doc_id_of("2101.00007")
    assert "\n" not in rec["title"] and "  " not in rec["title"]
    assert rec["category"] == "cs.LG"


def test_normalize_drops_bad_records() -> None:
    assert ingest.normalize_record({"id": "x", "title": "t", "abstract": "short",
                                    "categories": ["cs.LG"]}) is None
    raw = make_raw(1)
    raw["categories"] = []
    assert ingest.normalize_record(raw) is None
    raw = make_raw(2)
    del raw["id"]
    assert ingest.normalize_record(raw) is None


def test_normalize_handles_string_categories() -> None:
    raw = make_raw(3)
    raw["categories"] = "hep-ph hep-th"
    rec = ingest.normalize_record(raw)
    assert rec is not None
    assert rec["category"] == "hep-ph"


def test_bucket_mapping() -> None:
    assert ingest.bucket_of("cs.LG") == "cs"
    assert ingest.bucket_of("math.CO") == "math"
    for c in ("hep-ph", "astro-ph.GA", "cond-mat.str-el", "quant-ph", "math-ph"):
        assert ingest.bucket_of(c) == "physics", c
    assert ingest.bucket_of("q-bio.NC") == "q-bio"
    assert ingest.bucket_of("econ.EM") == "other"


def test_stratified_reservoir_deterministic_and_quotaed() -> None:
    cats = ["cs.LG", "math.CO", "hep-ph", "q-bio.NC", "econ.EM"]
    records = [
        ingest.normalize_record(make_raw(i, cats[i % len(cats)]))
        for i in range(2000)
    ]
    a = ingest.stratified_reservoir(records, 100, seed=42)
    b = ingest.stratified_reservoir(records, 100, seed=42)
    assert [r["doc_id"] for r in a] == [r["doc_id"] for r in b]  # deterministic
    c = ingest.stratified_reservoir(records, 100, seed=43)
    assert [r["doc_id"] for r in a] != [r["doc_id"] for r in c]  # seed matters

    buckets = collections.Counter(ingest.bucket_of(r["category"]) for r in a)
    assert buckets["cs"] == 35
    assert buckets["math"] == 20
    assert buckets["physics"] == 30
    assert buckets["q-bio"] == 5
    assert buckets["other"] == 10
    # Sorted by doc_id (deterministic global order).
    ids = [r["doc_id"] for r in a]
    assert ids == sorted(ids)


def test_corpus_hash_order_independent() -> None:
    docs = [ingest.normalize_record(make_raw(i)) for i in range(10)]
    h1 = ingest.corpus_hash(docs)
    h2 = ingest.corpus_hash(list(reversed(docs)))
    assert h1 == h2
    docs[0]["abstract"] += "!"
    assert ingest.corpus_hash(docs) != h1


def test_hash_partition_deterministic_and_balanced() -> None:
    ids = [ingest.doc_id_of(f"2101.{i:05d}") for i in range(10000)]
    a = ingest.hash_partition(ids, 4)
    assert a == ingest.hash_partition(ids, 4)
    counts = collections.Counter(a)
    for s in range(4):
        assert abs(counts[s] - 2500) < 250, counts  # within 10%


def _clustered_vectors(rng, k=3, per=60, dim=8, spread=0.05):
    """k well-separated blobs on the unit sphere; returns (vectors, true_label)."""
    centers = rng.standard_normal((k, dim)).astype("float32")
    centers /= np.linalg.norm(centers, axis=1, keepdims=True)
    vecs, truth = [], []
    for c in range(k):
        pts = centers[c] + spread * rng.standard_normal((per, dim)).astype("float32")
        pts /= np.linalg.norm(pts, axis=1, keepdims=True)
        vecs.append(pts)
        truth += [c] * per
    return np.concatenate(vecs).astype("float32"), truth


def test_semantic_partition_deterministic_and_balanced() -> None:
    rng = np.random.default_rng(0)
    vecs, truth = _clustered_vectors(rng)
    n = len(vecs)
    cats = ["cs.LG" if t == 0 else "math.CO" if t == 1 else "hep-th" for t in truth]

    a1, c1, m1 = ingest.semantic_partition(vecs, list(range(n)), cats, 3, seed=42)
    a2, c2, m2 = ingest.semantic_partition(vecs, list(range(n)), cats, 3, seed=42)
    assert a1 == a2 and np.array_equal(c1, c2)  # seeded → reproducible

    assert len(a1) == n
    counts = collections.Counter(a1)
    for s in range(3):
        assert counts[s] <= m1["cap"]  # capacity-balanced, no shard overflows
    assert c1.shape == (3, vecs.shape[1])
    assert np.allclose(np.linalg.norm(c1, axis=1), 1.0, atol=1e-4)  # normalized
    assert m1["scheme"] == "semantic" and m1["k"] == 3
    assert sum(m1["sizes"]) == n
    assert set(m1["labels"]) == {"0", "1", "2"}


def test_semantic_partition_recovers_separated_clusters() -> None:
    # Tight, well-separated clusters (all fit under cap → no spill) should map
    # almost perfectly onto shards: the whole point of semantic partitioning.
    rng = np.random.default_rng(1)
    vecs, truth = _clustered_vectors(rng, spread=0.02)
    n = len(vecs)
    a, _c, m = ingest.semantic_partition(vecs, list(range(n)), ["cs.LG"] * n, 3, seed=7)
    assert m["spilled"] == 0
    purity = sum(collections.Counter(truth[i] for i in range(n) if a[i] == s)
                 .most_common(1)[0][1]
                 for s in range(3) if s in set(a))
    assert purity / n > 0.95, f"clustering purity {purity / n:.2f}"


def test_embed_with_cache_roundtrip(tmp_path: pathlib.Path) -> None:
    dim = 8
    docs = [ingest.normalize_record(make_raw(i)) for i in range(20)]
    calls = {"n": 0}

    def fake_embed(texts):
        calls["n"] += 1
        rng = np.random.default_rng(0)
        v = rng.standard_normal((len(texts), dim)).astype(np.float32)
        return v / np.linalg.norm(v, axis=1, keepdims=True)

    v1, h1 = ingest.embed_with_cache(docs, fake_embed, tmp_path, "test-model", dim)
    assert v1.shape == (20, dim)
    assert calls["n"] == 1

    def exploding_embed(texts):
        raise AssertionError("cache miss — should not embed again")

    v2, h2 = ingest.embed_with_cache(docs, exploding_embed, tmp_path, "test-model", dim)
    assert h1 == h2
    np.testing.assert_array_equal(v1, v2)

    # Corrupt a cache block -> loud failure, not silent bad vectors.
    block = next((tmp_path / "embeddings" / "test-model" / h1).glob("block-*.npy"))
    data = bytearray(block.read_bytes())
    data[100] ^= 1
    block.write_bytes(bytes(data))
    with pytest.raises(RuntimeError, match="corrupt"):
        ingest.embed_with_cache(docs, exploding_embed, tmp_path, "test-model", dim)


def test_write_queries(tmp_path: pathlib.Path) -> None:
    docs = [ingest.normalize_record(make_raw(i)) for i in range(50)]
    out = tmp_path / "queries.json"
    ingest.write_queries(docs, out, n=10, seed=42)
    data = json.loads(out.read_text())
    assert len(data["queries"]) == 10
    ids = {d["doc_id"] for d in docs}
    for q in data["queries"]:
        assert q["source_doc_id"] in ids
        assert q["text"].startswith("Paper ")
    ingest.write_queries(docs, tmp_path / "q2.json", n=10, seed=42)
    assert json.loads((tmp_path / "q2.json").read_text()) == data  # deterministic


def test_pca_project_shape_and_determinism() -> None:
    rng = np.random.default_rng(7)
    vecs = rng.standard_normal((300, 32)).astype(np.float32)
    a = ingest.pca_project(vecs)
    b = ingest.pca_project(vecs)
    assert a.shape == (300, 2)
    assert a.dtype == np.float32
    np.testing.assert_array_equal(a, b)          # deterministic
    assert np.abs(a).max() <= 1.0 + 1e-6         # normalized to [-1,1]^2
    assert np.abs(a).max() > 0.5                 # actually spans the box


def test_write_projections(tmp_path, monkeypatch) -> None:
    class Cfg:
        class paths:
            data = tmp_path
    rng = np.random.default_rng(1)
    vecs = rng.standard_normal((50, 8)).astype(np.float32)
    rows = [[i for i in range(50) if i % 2 == 0], [i for i in range(50) if i % 2 == 1]]
    ingest.write_projections(Cfg, vecs, rows, method="pca", seed=42)
    for s, r in enumerate(rows):
        data = np.fromfile(tmp_path / "projections" / f"shard-{s}.f32", dtype="<f4")
        assert data.shape == (len(r) * 2,)
