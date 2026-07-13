// Background load generator (internals.md §5): Poisson arrivals at a target
// qps against the coordinator, SPANS tier only (never FULL — the rate-capped
// full-trace tier is for UI-initiated queries). Query pool = data/queries.json
// (falls back to a built-in list). `skew: "zipf"` weights the pool so a few
// queries dominate → hot shards under semantic partitioning; under hash it's
// uniform regardless, which is itself a thing to observe.

import { existsSync, readFileSync } from "node:fs";
import path from "node:path";

import * as grpc from "@grpc/grpc-js";

import { CoordinatorServiceClient, QueryRequest } from "./gen/lucent/v1/coordinator.js";
import { TraceLevel } from "./gen/lucent/v1/common.js";

const FALLBACK_QUERIES = [
  "quantum entanglement between photons",
  "neural networks for image recognition",
  "black hole thermodynamics",
  "prime number distribution",
  "protein folding dynamics",
  "dark matter halos",
  "graph algorithms and complexity",
  "topological insulators",
];

function loadQueryPool(dataDir: string): string[] {
  const p = path.join(dataDir, "queries.json");
  if (existsSync(p)) {
    try {
      const j = JSON.parse(readFileSync(p, "utf-8")) as { queries?: { text: string }[] };
      const texts = (j.queries ?? []).map((q) => q.text).filter(Boolean);
      if (texts.length > 0) return texts;
    } catch {
      /* fall through to fallback */
    }
  }
  return FALLBACK_QUERIES;
}

export interface LoadGenState {
  enabled: boolean;
  qps: number;
  skew: "uniform" | "zipf";
  sent: number;
  errors: number;
}

export class LoadGen {
  private coord: CoordinatorServiceClient;
  private pool: string[];
  private timer: NodeJS.Timeout | null = null;
  private state: LoadGenState = { enabled: false, qps: 10, skew: "uniform", sent: 0, errors: 0 };
  /** zipf(1.1) cumulative weights over the pool, popular-first */
  private zipfCdf: number[];

  constructor(coordAddr: string, dataDir: string) {
    this.coord = new CoordinatorServiceClient(coordAddr, grpc.credentials.createInsecure());
    this.pool = loadQueryPool(dataDir);
    const w = this.pool.map((_, i) => 1 / Math.pow(i + 1, 1.1));
    const total = w.reduce((a, b) => a + b, 0);
    let acc = 0;
    this.zipfCdf = w.map((x) => (acc += x / total));
  }

  set(enabled: boolean, qps?: number, skew?: "uniform" | "zipf"): LoadGenState {
    if (qps !== undefined) this.state.qps = Math.max(1, Math.min(50, qps));
    if (skew !== undefined) this.state.skew = skew;
    if (enabled && !this.state.enabled) {
      this.state.enabled = true;
      this.scheduleNext();
    } else if (!enabled && this.state.enabled) {
      this.state.enabled = false;
      if (this.timer) clearTimeout(this.timer);
      this.timer = null;
    }
    return { ...this.state };
  }

  status(): LoadGenState {
    return { ...this.state };
  }

  private pick(): string {
    if (this.state.skew === "zipf") {
      const r = Math.random();
      const idx = this.zipfCdf.findIndex((c) => r <= c);
      return this.pool[idx < 0 ? this.pool.length - 1 : idx]!;
    }
    return this.pool[Math.floor(Math.random() * this.pool.length)]!;
  }

  private scheduleNext(): void {
    if (!this.state.enabled) return;
    // Exponential inter-arrival for a Poisson process at `qps`.
    const meanMs = 1000 / this.state.qps;
    const delay = -Math.log(1 - Math.random()) * meanMs;
    this.timer = setTimeout(() => {
      this.fire();
      this.scheduleNext();
    }, delay);
  }

  private fire(): void {
    const req = QueryRequest.fromPartial({
      text: this.pick(),
      k: 10,
      traceLevel: TraceLevel.TRACE_LEVEL_SPANS, // never FULL from load-gen
    });
    this.coord.query(req, (err) => {
      if (err) this.state.errors += 1;
      else this.state.sent += 1;
    });
  }

  shutdown(): void {
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
  }
}
