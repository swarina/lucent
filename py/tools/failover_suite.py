"""Deterministic failover-under-load suite (M3-T6, PLAN §7).

Assumes a REPLICAS=2 cluster is up and the 2k corpus is ingested:
    lucent dev --shards 2 --replicas 2 --fake-embed

Drives a steady query storm at a fixed offered rate and injects a fault at a
fixed offset, then asserts the failover contract:

  * zero *failed* queries — every request returns 200 with a coverage object;
    a shard going away becomes partial coverage, never an error (the core
    "failures become coverage" property, now under load);
  * coverage flags are exact throughout — answered <= probed == shard count,
    and a shortfall always names the missing shard;
  * recovery < 5 s — the cluster returns to full coverage (via backup
    promotion, no restart) within the window, and p99 latency comes back down.

Scenarios: `kill` (real SIGKILL of a primary → HealthWatcher → promotion) and
`slow` (InjectFault slow on a primary → elevated latency, no promotion, clear
restores). stdlib only; exits non-zero with a reason on any breach.

Determinism note (internals.md §6): wall-clock timing can't be bit-identical,
so the *assertions* are the invariant — pass/fail is reproducible even though
individual latencies aren't. Query texts rotate over a fixed list; the offered
schedule is fixed; the fault fires at a fixed offset.
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

GW = "http://127.0.0.1:8080"
CTL = "http://127.0.0.1:7999"

# Fixed query set so the offered load is reproducible run to run.
QUERIES = [
    "quantum entanglement between photons",
    "dark matter halos in galaxy clusters",
    "transformer attention mechanisms",
    "protein folding energy landscapes",
    "convex optimization duality",
    "black hole thermodynamics",
    "graph neural network message passing",
    "topological insulators band structure",
]


def _call(method: str, url: str, body: dict | None = None, timeout: float = 10.0) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={"content-type": "application/json"} if data else {})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def fail(msg: str) -> None:
    print(f"FAILOVER-SUITE FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


@dataclass
class Sample:
    t: float             # seconds since storm start
    ok: bool             # 200 + well-formed coverage
    answered: int
    probed: int
    missing: list[int]
    latency_ms: float
    error: str = ""


class Storm:
    """Fires queries at a fixed offered rate, each on its own thread so a slow
    or timing-out request never throttles the schedule."""

    def __init__(self, qps: int, shards: int) -> None:
        self.interval = 1.0 / qps
        self.shards = shards
        self.samples: list[Sample] = []
        self._lock = threading.Lock()
        self._threads: list[threading.Thread] = []
        self.t0 = 0.0

    def _one(self, i: int) -> None:
        text = QUERIES[i % len(QUERIES)]
        sent = time.monotonic()
        s = Sample(t=sent - self.t0, ok=False, answered=0, probed=0, missing=[],
                   latency_ms=0.0)
        try:
            r = _call("POST", f"{GW}/api/query", {"text": text, "k": 5}, timeout=10.0)
            s.latency_ms = (time.monotonic() - sent) * 1000
            cov = r.get("coverage")
            if cov is None or "error" in r:
                s.error = f"no coverage / error: {r.get('error')}"
            else:
                s.ok = True
                s.answered = cov.get("answered", 0)
                s.probed = cov.get("probed", 0)
                s.missing = list(cov.get("missingShards", []))
        except (urllib.error.URLError, ConnectionError, TimeoutError, OSError) as e:
            s.latency_ms = (time.monotonic() - sent) * 1000
            s.error = f"{type(e).__name__}: {e}"
        with self._lock:
            self.samples.append(s)

    def run(self, duration: float, at: float, action) -> None:
        """Fire for `duration`s; call `action()` once at offset `at`s."""
        self.t0 = time.monotonic()
        fired_action = False
        i = 0
        while True:
            now = time.monotonic() - self.t0
            if now >= duration:
                break
            if not fired_action and now >= at:
                action()
                fired_action = True
                print(f"  [t={now:4.1f}s] fault injected")
            th = threading.Thread(target=self._one, args=(i,), daemon=True)
            th.start()
            self._threads.append(th)
            i += 1
            # keep the schedule steady regardless of per-request latency
            target = self.t0 + i * self.interval
            time.sleep(max(0, target - time.monotonic()))
        for th in self._threads:
            th.join(timeout=12)


def p99(values: list[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    return s[min(len(s) - 1, int(len(s) * 0.99))]


def analyze(storm: Storm, kill_at: float, shards: int, *,
            expect_recovery: bool) -> None:
    samples = sorted(storm.samples, key=lambda s: s.t)
    if not samples:
        fail("no samples collected")

    # 1. Zero failed queries — the whole point. Partial coverage is fine.
    failures = [s for s in samples if not s.ok]
    if failures:
        for s in failures[:5]:
            print(f"    FAIL @t={s.t:.2f}s: {s.error}", file=sys.stderr)
        fail(f"{len(failures)}/{len(samples)} queries FAILED (must be 0)")

    # 2. Coverage flags exact: answered<=probed==shards; shortfall names shards.
    for s in samples:
        if s.probed != shards:
            fail(f"@t={s.t:.2f}s probed={s.probed}, want {shards}")
        if s.answered > s.probed:
            fail(f"@t={s.t:.2f}s answered {s.answered} > probed {s.probed}")
        if s.answered < s.probed and not s.missing:
            fail(f"@t={s.t:.2f}s partial ({s.answered}/{s.probed}) but no missingShards")
        if s.answered == s.probed and s.missing:
            fail(f"@t={s.t:.2f}s full coverage but missingShards={s.missing}")

    pre = [s for s in samples if s.t < kill_at]
    post = [s for s in samples if s.t >= kill_at]
    base_p99 = p99([s.latency_ms for s in pre])
    degraded = [s for s in post if s.answered < s.probed]

    # 3. Recovery: last moment of partial coverage after the fault, back to full.
    if expect_recovery:
        last_partial = max((s.t for s in degraded), default=kill_at)
        recovery = last_partial - kill_at
        if recovery >= 5.0:
            fail(f"recovery took {recovery:.1f}s (>= 5s): still partial at t={last_partial:.1f}s")
        # p99 must also come back: measure the tail 3s of the run.
        tail = [s.latency_ms for s in samples if s.t >= samples[-1].t - 3.0]
        tail_p99 = p99(tail)
        print(f"  pre-fault p99={base_p99:.0f}ms · degraded queries={len(degraded)} · "
              f"recovery={recovery:.1f}s · tail p99={tail_p99:.0f}ms")
        if tail_p99 > max(50.0, base_p99 * 4):
            fail(f"p99 did not recover: tail {tail_p99:.0f}ms vs baseline {base_p99:.0f}ms")
    else:
        # slow variant: coverage stays full, but we should see elevated latency
        # while the fault is active, then a return to baseline after clear.
        print(f"  pre-fault p99={base_p99:.0f}ms · partial queries={len(degraded)} "
              f"(want 0 for slow) · post samples={len(post)}")
        if degraded:
            fail(f"slow fault caused {len(degraded)} partial-coverage queries (want 0)")

    print(f"  {len(samples)} queries, 0 failed, coverage exact throughout")


def scenario_kill(qps: int, duration: float, at: float, shards: int, victim: str) -> None:
    print(f"=== KILL scenario: {qps} qps, SIGKILL {victim} at t={at}s ===")
    storm = Storm(qps, shards)

    def action() -> None:
        r = _call("POST", f"{CTL}/kill", {"nodeId": victim})
        if "error" in r:
            fail(f"kill failed: {r}")

    storm.run(duration, at, action)
    analyze(storm, at, shards, expect_recovery=True)
    # Restore the victim so a following scenario / the cluster is whole again.
    _call("POST", f"{CTL}/restart", {"nodeId": victim})
    print(f"  restarted {victim}")


def scenario_slow(qps: int, duration: float, at: float, shards: int, victim: str) -> None:
    print(f"=== SLOW scenario: {qps} qps, slow+120ms on {victim} at t={at}s ===")
    storm = Storm(qps, shards)

    def action() -> None:
        r = _call("POST", f"{GW}/api/fault", {"nodeId": victim, "kind": "slow", "ms": 120})
        if r.get("error"):
            fail(f"slow fault failed: {r}")

    storm.run(duration, at, action)
    analyze(storm, at, shards, expect_recovery=False)
    _call("POST", f"{GW}/api/fault", {"nodeId": victim, "kind": "clear"})
    print(f"  cleared fault on {victim}")


def wait_ready(shards: int) -> None:
    for _ in range(60):
        try:
            if _call("GET", f"{GW}/api/ready").get("ready"):
                # also require full coverage before we start (all replicas up)
                r = _call("POST", f"{GW}/api/query", {"text": QUERIES[0], "k": 5})
                if r.get("coverage", {}).get("answered") == shards:
                    return
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
        time.sleep(2)
    fail("cluster never reached full coverage")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--qps", type=int, default=20)
    ap.add_argument("--duration", type=float, default=12.0)
    ap.add_argument("--at", type=float, default=5.0, help="fault offset (s)")
    ap.add_argument("--shards", type=int, default=2)
    ap.add_argument("--victim", default="shard-1a")
    ap.add_argument("--scenario", choices=["kill", "slow", "all"], default="all")
    args = ap.parse_args()

    wait_ready(args.shards)
    print(f"cluster ready ({args.shards} shards, full coverage)\n")

    if args.scenario in ("kill", "all"):
        scenario_kill(args.qps, args.duration, args.at, args.shards, args.victim)
        if args.scenario == "all":
            wait_ready(args.shards)  # let the restarted victim rejoin
            print()
    if args.scenario in ("slow", "all"):
        scenario_slow(args.qps, args.duration, args.at, args.shards, args.victim)

    print("\nFAILOVER-SUITE PASS: zero failed queries, exact coverage, recovery < 5s")


if __name__ == "__main__":
    main()
