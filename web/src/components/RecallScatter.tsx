// Recall/latency "money chart" (frontend.md §5.3). Reads the bench sweep and
// plots recall@10 against p99 latency, hash vs semantic as two series — the
// point of semantic partitioning is that its curve stays high-recall at low
// latency (small probe) where hash craters. Overlaps under --fake-embed (fake
// vectors carry no topic); the banner up top already explains why.

import { useEffect, useState } from "react";

interface BenchConfig {
  partitioning: string;
  probe: number;
  ef: number;
  recall_at_10: number;
  p50_ms: number;
  p99_ms: number;
}
interface Bench {
  configs?: BenchConfig[];
  model?: string;
}

const SERIES: Record<string, { color: string; label: string }> = {
  semantic: { color: "var(--ok)", label: "semantic" },
  hash: { color: "var(--text-dim)", label: "hash" },
};

const W = 320;
const H = 188;
const M = { l: 34, r: 10, t: 10, b: 26 };

export function RecallScatter() {
  const [bench, setBench] = useState<Bench | null>(null);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    void fetch("/api/bench")
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error("no bench"))))
      .then(setBench)
      .catch(() => setFailed(true));
  }, []);

  if (failed || !bench?.configs?.length) {
    return (
      <div className="scatter-empty">
        <span className="mono">
          run <code>lucent bench --probe 0,1 --merge</code> to chart the
          recall / latency tradeoff
        </span>
      </div>
    );
  }

  const cfgs = bench.configs;
  const maxP99 = Math.max(...cfgs.map((c) => c.p99_ms), 1);
  const minRecall = Math.min(0.4, ...cfgs.map((c) => c.recall_at_10));
  const px = M.l;
  const pw = W - M.l - M.r;
  const py = M.t;
  const ph = H - M.t - M.b;
  const xOf = (v: number) => px + (v / maxP99) * pw;
  const yOf = (v: number) => py + ph - ((v - minRecall) / (1 - minRecall)) * ph;

  const bySeries = new Map<string, BenchConfig[]>();
  for (const c of cfgs) {
    (bySeries.get(c.partitioning) ?? bySeries.set(c.partitioning, []).get(c.partitioning)!).push(c);
  }
  const yTicks = [minRecall, (minRecall + 1) / 2, 1];

  return (
    <div className="scatter">
      <div className="scatter-legend mono">
        <span className="scatter-title">recall @10 vs p99</span>
        <span className="spacer" />
        {[...bySeries.keys()].map((k) => (
          <span key={k} className="leg">
            <span className="swatch" style={{ background: SERIES[k]?.color ?? "var(--text)" }} />
            {SERIES[k]?.label ?? k}
          </span>
        ))}
      </div>
      <svg className="scatter-svg" viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="xMidYMid meet">
        {/* y grid + labels */}
        {yTicks.map((t) => (
          <g key={t}>
            <line className="grid" x1={px} y1={yOf(t)} x2={px + pw} y2={yOf(t)} />
            <text className="axlabel" x={px - 5} y={yOf(t) + 3} textAnchor="end">
              {t.toFixed(2)}
            </text>
          </g>
        ))}
        {/* x axis label */}
        <text className="axlabel" x={px + pw} y={H - 6} textAnchor="end">
          {maxP99.toFixed(1)}ms
        </text>
        <text className="axlabel" x={px} y={H - 6} textAnchor="start">0ms</text>

        {/* one polyline + points per series, sorted by latency */}
        {[...bySeries.entries()].map(([part, pts]) => {
          const color = SERIES[part]?.color ?? "var(--text)";
          const sorted = [...pts].sort((a, b) => a.p99_ms - b.p99_ms);
          const path = sorted.map((c) => `${xOf(c.p99_ms)},${yOf(c.recall_at_10)}`).join(" ");
          return (
            <g key={part}>
              <polyline className="scatter-line" points={path} stroke={color} />
              {sorted.map((c, i) => (
                <g key={i}>
                  <circle cx={xOf(c.p99_ms)} cy={yOf(c.recall_at_10)} r={3.5} fill={color}>
                    <title>
                      {part} · probe {c.probe === 0 ? "all" : c.probe} · ef {c.ef} ·
                      recall {c.recall_at_10.toFixed(3)} · p99 {c.p99_ms.toFixed(1)}ms
                    </title>
                  </circle>
                  <text className="ptlabel" x={xOf(c.p99_ms) + 5} y={yOf(c.recall_at_10) - 4}>
                    P{c.probe === 0 ? "∙" : c.probe}
                  </text>
                </g>
              ))}
            </g>
          );
        })}
      </svg>
    </div>
  );
}
