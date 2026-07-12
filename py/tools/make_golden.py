"""Golden traces: capture + determinism digest (M1-T5).

Runs fixed queries at FULL against a running (fake-encoder) cluster and
produces:
  --out DIR      a golden bundle for frontend fixtures / ReplaySource dev:
                 responses.json, trace-<i>.spans.json, trace-<i>.<node>.pb
  --digest FILE  a structural digest: per query, per shard blob, the xxh3 of
                 the node/parent/meta/dist arrays + the merged result doc_ids.

The determinism gate diffs digests from two INDEPENDENT ingest+query rounds
on the same machine. Deliberately excluded from the digest: all timestamps
(observational, never deterministic) and trace_ids (minted randomly). Digests
are architecture-specific — float math may differ ulp-wise across ISAs — so
the gate compares run-vs-run, never a checked-in file (recorded in
internals.md §6).
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
import time
import urllib.request

import xxhash

GW = "http://127.0.0.1:8080"

GOLDEN_QUERIES = [
    "quantum entanglement between photons",
    "neural networks for image recognition",
    "prime number distribution",
]


def call(method: str, url: str, body: dict | None = None) -> bytes:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"content-type": "application/json"} if data else {})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()


def capture() -> list[dict]:
    """Runs the golden queries; returns per-query {response, spans, blobs}."""
    out = []
    for text in GOLDEN_QUERIES:
        resp = json.loads(call("POST", f"{GW}/api/query",
                               {"text": text, "k": 5, "trace": "full"}))
        tid = resp["traceId"]
        time.sleep(0.5)  # spans flush (50ms) + margin
        tr = json.loads(call("GET", f"{GW}/api/trace/{tid}"))
        blobs = {}
        for ref in sorted(tr["blobs"], key=lambda r: r["nodeId"]):
            blobs[ref["nodeId"]] = call(
                "GET", f"{GW}/api/trace/{tid}/blob/{ref['nodeId']}")
        out.append({"text": text, "response": resp, "spans": tr["spans"],
                    "blobs": blobs})
    return out


def structural_digest(captures: list[dict]) -> dict:
    from lucent.v1 import events_pb2

    digest: dict = {"schema": 1, "queries": []}
    for cap in captures:
        entry = {
            "text": cap["text"],
            # Merged results: the cross-shard contract.
            "result_doc_ids": [h["docId"] for h in cap["response"].get("hits", [])],
            "coverage": cap["response"].get("coverage", {}),
            "blobs": {},
        }
        import numpy as np

        for node_id, raw in cap["blobs"].items():
            blob = events_pb2.TraceBlob.FromString(raw)
            h = xxhash.xxh3_64()
            for arr, fmt in ((blob.node, "<u4"), (blob.parent, "<u4"),
                             (blob.meta, "<u4"), (blob.dist, "<f4")):
                h.update(np.asarray(arr, dtype=fmt).tobytes())
            entry["blobs"][node_id] = {
                "records": len(blob.node),
                "dropped": blob.dropped,
                "xxh3": h.hexdigest(),
            }
        digest["queries"].append(entry)
    return digest


def write_bundle(captures: list[dict], out_dir: pathlib.Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    responses = []
    for i, cap in enumerate(captures):
        responses.append({"text": cap["text"], "response": cap["response"]})
        (out_dir / f"trace-{i}.spans.json").write_text(
            json.dumps(cap["spans"], indent=1) + "\n")
        for node_id, raw in cap["blobs"].items():
            (out_dir / f"trace-{i}.{node_id}.pb").write_bytes(raw)
    (out_dir / "responses.json").write_text(json.dumps(responses, indent=1) + "\n")
    # Manifest for ReplaySource: which files belong to which query.
    (out_dir / "manifest.json").write_text(json.dumps({
        "schema": 1,
        "queries": [
            {"i": i, "text": c["text"],
             "blobs": sorted(c["blobs"].keys()),
             "spans": f"trace-{i}.spans.json"}
            for i, c in enumerate(captures)
        ],
    }, indent=1) + "\n")
    print(f"golden bundle -> {out_dir}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=pathlib.Path, help="write golden bundle dir")
    ap.add_argument("--digest", type=pathlib.Path, help="write structural digest")
    ap.add_argument("--compare", type=pathlib.Path, nargs=2, metavar=("A", "B"),
                    help="diff two digests; exit 1 on mismatch")
    args = ap.parse_args()

    if args.compare:
        a = json.loads(args.compare[0].read_text())
        b = json.loads(args.compare[1].read_text())
        if a == b:
            print("DETERMINISM OK: structural digests identical")
            return
        for qa, qb in zip(a["queries"], b["queries"]):
            if qa != qb:
                print(f"MISMATCH on {qa['text']!r}:", file=sys.stderr)
                print(f"  A: {json.dumps(qa, sort_keys=True)[:300]}", file=sys.stderr)
                print(f"  B: {json.dumps(qb, sort_keys=True)[:300]}", file=sys.stderr)
        sys.exit(1)

    captures = capture()
    if args.out:
        write_bundle(captures, args.out)
    if args.digest:
        args.digest.parent.mkdir(parents=True, exist_ok=True)
        args.digest.write_text(json.dumps(structural_digest(captures),
                                          indent=1, sort_keys=True) + "\n")
        print(f"digest -> {args.digest}")


if __name__ == "__main__":
    main()
