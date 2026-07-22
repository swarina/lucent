"""`lucent record` — tee a live cluster into a replay bundle (M6-T2).

Subscribes to the gateway's WebSocket for the full event stream, drives a short
scripted scenario (a few FULL-trace queries, then a kill/recover), and captures
everything a static demo needs to replay it with no backend (data-formats.md §6):

    bundle/
      manifest.json                       schema, duration, ClusterState@start, chapters
      events.ndjson                       every Event (proto3-JSON), one per line
      results/{trace_hex}.json            QueryResponse per query (results panel)
      traces/{trace_hex}.{node}.pb        FULL TraceBlobs, raw proto (archival)
      traces_json/{trace_hex}.json        decoded /api/trace blobs (static 3D inspector)
      projections/shard-{id}.f32          per-shard 2D point cloud (static 3D inspector)
      bench.json                          latest recall/latency sweep

REST calls run in a thread so the WS collector keeps draining while they block.
"""

from __future__ import annotations

import asyncio
import json
import pathlib
import time
import urllib.error
import urllib.request

QUERIES = [
    "quantum entanglement between photons",
    "neural networks for image recognition",
    "convex optimization duality",
    "black hole thermodynamics",
]


def _get(url: str, timeout: float = 10.0) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read())


def _get_bytes(url: str, timeout: float = 10.0) -> bytes | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.read()
    except urllib.error.URLError:
        return None


def _post(url: str, body: dict, timeout: float = 15.0) -> dict:
    req = urllib.request.Request(
        url, data=json.dumps(body).encode(), method="POST",
        headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read() or b"{}")
        except json.JSONDecodeError:
            return {"error": str(e)}


def _save_result(out: pathlib.Path, trace_hex: str, resp: dict) -> None:
    (out / "results").mkdir(parents=True, exist_ok=True)
    (out / "results" / f"{trace_hex}.json").write_text(json.dumps(resp))


def _save_traces(gateway: str, out: pathlib.Path, trace_hex: str) -> int:
    """Fetch each FULL blob holder's TraceBlob (.pb) named by node id."""
    meta = _get(f"{gateway}/api/trace/{trace_hex}")
    holders = {b.get("nodeId") for b in meta.get("blobs", []) if b.get("nodeId")}
    (out / "traces").mkdir(parents=True, exist_ok=True)
    n = 0
    for node in holders:
        blob = _get_bytes(f"{gateway}/api/trace/{trace_hex}/blob/{node}")
        if blob:
            (out / "traces" / f"{trace_hex}.{node}.pb").write_bytes(blob)
            n += 1
    return n


def _save_trace_json(gateway: str, out: pathlib.Path, trace_hex: str) -> set[int]:
    """Save the gateway-decoded blobs JSON the 3D inspector consumes directly,
    so the static demo needs no server-side proto decode. Returns the shard ids
    that hold blobs for this trace (so their projections get fetched)."""
    try:
        body = _get(f"{gateway}/api/trace/{trace_hex}/blobs")
    except (urllib.error.URLError, json.JSONDecodeError):
        return set()
    blobs = body.get("blobs") or {}
    if not blobs:
        return set()
    (out / "traces_json").mkdir(parents=True, exist_ok=True)
    (out / "traces_json" / f"{trace_hex}.json").write_text(json.dumps(body))
    return {int(b.get("shardId", 0)) for b in blobs.values()}


def _save_projections(gateway: str, out: pathlib.Path, shard_ids: set[int]) -> int:
    """Per-shard 2D projection point clouds (raw f32), same bytes the gateway
    serves at /api/projection/{id} — the inspector's static point cloud."""
    (out / "projections").mkdir(parents=True, exist_ok=True)
    n = 0
    for sid in sorted(shard_ids):
        buf = _get_bytes(f"{gateway}/api/projection/{sid}")
        if buf:
            (out / "projections" / f"shard-{sid}.f32").write_bytes(buf)
            n += 1
    return n


async def _record(gateway: str, ws_url: str, out: pathlib.Path,
                  corpus_hash: str) -> dict:
    import websockets

    events: list[dict] = []
    cluster0 = _get(f"{gateway}/api/cluster")
    chapters: list[dict] = []
    t0 = time.monotonic()

    def ms() -> int:
        return int((time.monotonic() - t0) * 1000)

    async with websockets.connect(ws_url, max_size=None) as ws:
        await ws.send(json.dumps({"subscribe": ["spans", "metrics", "cluster"]}))
        stop = asyncio.Event()

        async def collect() -> None:
            while not stop.is_set():
                try:
                    frame = await asyncio.wait_for(ws.recv(), timeout=0.4)
                except (asyncio.TimeoutError, Exception):  # noqa: BLE001
                    continue
                try:
                    f = json.loads(frame)
                except json.JSONDecodeError:
                    continue
                events.extend(f.get("events", []))

        task = asyncio.create_task(collect())

        # --- Chapter 1: queries (FULL traces → 3D inspector replay) ---
        chapters.append({"t_ms": ms(), "label": "queries"})
        shard_ids: set[int] = set()
        for q in QUERIES:
            resp = await asyncio.to_thread(
                _post, f"{gateway}/api/query", {"text": q, "k": 10, "trace": "full"})
            tid = resp.get("traceId")
            if tid:
                _save_result(out, tid, resp)
                await asyncio.sleep(0.35)  # let spans + blobs settle
                await asyncio.to_thread(_save_traces, gateway, out, tid)
                shard_ids |= await asyncio.to_thread(_save_trace_json, gateway, out, tid)
            await asyncio.sleep(0.4)

        # Per-shard projections are static; fetch each once for the shards that
        # actually held FULL blobs above.
        await asyncio.to_thread(_save_projections, gateway, out, shard_ids)

        # --- Chapter 2: failover (kill a shard → degraded coverage → recover) ---
        shards = cluster0.get("shardMap", {}).get("shards", [])
        victim = shards[-1].get("primaryNode") if shards else None
        if victim:
            chapters.append({"t_ms": ms(), "label": "failover"})
            await asyncio.to_thread(_post, f"{gateway}/api/chaos/kill", {"nodeId": victim})
            await asyncio.sleep(1.0)
            for q in QUERIES[:2]:
                resp = await asyncio.to_thread(
                    _post, f"{gateway}/api/query", {"text": q, "k": 10, "trace": "spans"})
                if resp.get("traceId"):
                    _save_result(out, resp["traceId"], resp)
                await asyncio.sleep(0.4)
            await asyncio.to_thread(_post, f"{gateway}/api/chaos/restart", {"nodeId": victim})
            await asyncio.sleep(2.0)

        stop.set()
        await task

    return {
        "schema": 1,
        "recorded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "duration_ms": ms(),
        "cluster": cluster0,
        "corpus_hash": corpus_hash,
        "chapters": chapters,
        "_events": events,
    }


def run(config_path: str, out: str) -> None:
    """Blocking `lucent record` entrypoint."""
    import logging

    from lucent import config as config_mod

    logging.basicConfig(level=logging.INFO, format="[lucent.record] %(message)s")
    log = logging.getLogger("lucent.record")
    cfg = config_mod.load(config_path)
    gateway = f"http://127.0.0.1:{cfg.ports.gateway_http}"
    ws_url = f"ws://127.0.0.1:{cfg.ports.gateway_http}/ws/live"

    # readiness check
    try:
        if not _get(f"{gateway}/api/ready").get("ready"):
            raise SystemExit("gateway not ready — is the cluster up? (lucent dev)")
    except urllib.error.URLError as e:
        raise SystemExit(f"cannot reach gateway at {gateway}: {e}") from e

    corpus_hash = ""
    manifest_path = cfg.paths.data / "ingest-manifest.json"
    if manifest_path.exists():
        corpus_hash = json.loads(manifest_path.read_text()).get("corpus_hash", "")

    out_dir = pathlib.Path(out)
    out_dir.mkdir(parents=True, exist_ok=True)
    log.info("recording a scripted session into %s ...", out_dir)
    manifest = asyncio.run(_record(gateway, ws_url, out_dir, corpus_hash))

    events = manifest.pop("_events")
    with (out_dir / "events.ndjson").open("w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=1))

    bench = pathlib.Path("bench/results/bench.json")
    if bench.exists():
        (out_dir / "bench.json").write_text(bench.read_text())

    def _count(sub: str, pat: str) -> int:
        d = out_dir / sub
        return len(list(d.glob(pat))) if d.exists() else 0

    log.info(
        "bundle done: %d events, %d results, %d trace blobs, %d decoded traces, "
        "%d projections, chapters=%s",
        len(events), _count("results", "*.json"), _count("traces", "*.pb"),
        _count("traces_json", "*.json"), _count("projections", "*.f32"),
        [c["label"] for c in manifest["chapters"]])
