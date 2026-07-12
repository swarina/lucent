"""End-to-end integration smoke against a running Lucent cluster (M0-T12).

Assumes: `lucent dev --shards 2 --fake-embed` is up and the 2k test corpus is
ingested. Verifies the M0 steel thread: query -> merged hits + coverage,
spans across processes, chaos kill -> honest degradation, restart -> recovery.
stdlib only; exits non-zero with a reason on any failure.
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request

GW = "http://127.0.0.1:8080"
CTL = "http://127.0.0.1:7999"


def call(method: str, url: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"content-type": "application/json"} if data else {},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def fail(msg: str) -> None:
    print(f"SMOKE FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def query(text: str) -> dict:
    return call("POST", f"{GW}/api/query", {"text": text, "k": 5})


def expect_coverage(resp: dict, answered: int, probed: int, ctx: str) -> None:
    cov = resp.get("coverage", {})
    got = (cov.get("answered", 0), cov.get("probed", 0))
    if got != (answered, probed):
        fail(f"{ctx}: coverage {got}, want ({answered}, {probed})")


def main() -> None:
    # 1. Ready gate.
    for _ in range(60):
        try:
            if call("GET", f"{GW}/api/ready").get("ready"):
                break
        except (urllib.error.URLError, ConnectionError):
            pass
        time.sleep(2)
    else:
        fail("cluster never became ready")
    print("1. ready OK")

    # 2. Healthy query: merged hits, full coverage, trace id.
    resp = query("quantum entanglement between photons")
    if len(resp.get("hits", [])) != 5:
        fail(f"expected 5 hits, got {len(resp.get('hits', []))}")
    expect_coverage(resp, 2, 2, "healthy query")
    trace_id = resp.get("traceId", "")
    if len(trace_id) != 32:
        fail(f"bad traceId: {trace_id!r}")
    shards_seen = {h.get("shardId", 0) for h in resp["hits"]}
    print(f"2. query OK: 5 hits from shards {sorted(shards_seen)}, "
          f"{int(resp['timings']['totalUs'])/1000:.1f}ms")

    # 3. Spans arrive at the collector, across processes.
    time.sleep(1.0)  # publisher flush interval + WS not needed, ring is enough
    spans = call("GET", f"{GW}/api/trace/{trace_id}").get("spans", [])
    kinds = {s["span"]["kind"] for s in spans}
    need = {"SPAN_QUERY_RECEIVED", "SPAN_EMBED", "SPAN_PLAN",
            "SPAN_SHARD_RPC", "SPAN_SHARD_SEARCH", "SPAN_MERGE", "SPAN_QUERY_DONE"}
    if not need.issubset(kinds):
        fail(f"span kinds missing: {sorted(need - kinds)} (got {sorted(kinds)})")
    print(f"3. trace OK: {len(spans)} spans, all 7 kinds present")

    # 4. Chaos: real SIGKILL -> partial results, honest coverage.
    kill = call("POST", f"{CTL}/kill", {"nodeId": "shard-1a"})
    if "error" in kill:
        fail(f"kill failed: {kill}")
    time.sleep(0.5)
    resp = query("dark matter halos")
    expect_coverage(resp, 1, 2, "post-kill query")
    missing = resp.get("coverage", {}).get("missingShards", [])
    if missing != [1]:
        fail(f"missingShards {missing}, want [1]")
    if any(h.get("shardId", 0) == 1 for h in resp.get("hits", [])):
        fail("hits from a dead shard")
    print("4. chaos OK: coverage 1/2, missing=[1], results served from s0")

    # 5. Restart -> reload sealed index -> full coverage restored.
    call("POST", f"{CTL}/restart", {"nodeId": "shard-1a"})
    deadline = time.time() + 30
    while time.time() < deadline:
        time.sleep(1.0)
        resp = query("dark matter halos")
        cov = resp.get("coverage", {})
        if cov.get("answered") == 2:
            break
    else:
        fail("shard never recovered after restart")
    print("5. recovery OK: coverage 2/2 restored after restart")

    print("SMOKE PASS: steel thread verified end to end")


if __name__ == "__main__":
    main()
