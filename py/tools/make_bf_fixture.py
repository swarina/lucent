"""Generates the brute-force cross-validation fixture (testdata/bf_fixture/).

The C++ BruteForceIndex test replays these queries and must reproduce NumPy's
exact top-k (score desc, doc_id asc tiebreak) — an independent implementation
checking ours. Deterministic: fixed seed, little-endian raw arrays.

Run from repo root:  uv --project py run python py/tools/make_bf_fixture.py
"""

from __future__ import annotations

import json
import pathlib

import numpy as np

SEED = 42
N, DIM, NQ, K = 500, 32, 8, 10

def main() -> None:
    root = pathlib.Path(__file__).resolve().parents[2]
    out = root / "testdata" / "bf_fixture"
    out.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(SEED)

    vecs = rng.standard_normal((N, DIM), dtype=np.float32)
    vecs /= np.linalg.norm(vecs, axis=1, keepdims=True)
    # Non-contiguous ids so row != doc_id bugs can't hide.
    ids = (1000 + 3 * np.arange(N, dtype=np.uint64)).astype("<u8")

    queries = rng.standard_normal((NQ, DIM), dtype=np.float32)
    queries /= np.linalg.norm(queries, axis=1, keepdims=True)

    expected = []
    for q in queries:
        scores = vecs @ q
        # Sort by (-score, doc_id): NumPy lexsort keys are last-key-primary.
        order = np.lexsort((ids, -scores))[:K]
        expected.append(
            [[int(ids[i]), float(scores[i])] for i in order]
        )

    vecs.astype("<f4").tofile(out / "vectors.f32")
    ids.tofile(out / "ids.u64")
    queries.astype("<f4").tofile(out / "queries.f32")
    (out / "expected.json").write_text(
        json.dumps({"seed": SEED, "n": N, "dim": DIM, "k": K, "queries": NQ,
                    "expected": expected}, indent=1) + "\n"
    )
    print(f"wrote {out} (n={N} dim={DIM} nq={NQ} k={K})")

if __name__ == "__main__":
    main()
