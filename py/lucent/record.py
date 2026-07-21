"""`lucent record` — tee a live cluster into a replay bundle (M6-T2).

Subscribes to the gateway's WebSocket for the full event stream, drives a short
scripted scenario (a few FULL-trace queries, then a kill/recover), and captures
everything a static demo needs to replay it with no backend (data-formats.md §6):

    bundle/
      manifest.json                       schema, duration, ClusterState@start, chapters
      events.ndjson                       every Event (proto3-JSON), one per line
      results/{trace_hex}.json            QueryResponse per query (results panel)
      traces/{trace_hex}.{node}.pb        FULL TraceBlobs (3D inspector)
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
        for q in QUERIES:
            resp = await asyncio.to_thread(
                _post, f"{gateway}/api/query", {"text": q, "k": 10, "trace": "full"})
            tid = resp.get("traceId")
            if tid:
                _save_result(out, tid, resp)
                await asyncio.sleep(0.35)  # let spans + blobs settle
                await asyncio.to_thread(_save_traces, gateway, out, tid)
            await asyncio.sleep(0.4)

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

    traces = len(list((out_dir / "traces").glob("*.pb"))) if (out_dir / "traces").exists() else 0
    results = len(list((out_dir / "results").glob("*.json"))) if (out_dir / "results").exists() else 0
    log.info("bundle done: %d events, %d results, %d trace blobs, chapters=%s",
             len(events), results, traces, [c["label"] for c in manifest["chapters"]])
