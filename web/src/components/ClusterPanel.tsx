// Live cluster / ingest view (frontend.md §5.4). Per-node cards driven by the
// always-on NodeStats (rss/docs/edges/qps at 2 Hz) and the IndexBuildProgress
// events emitted while a shard builds its HNSW — so you literally watch memory
// and edge counts climb as the index builds.

import { useEffect, useMemo, useState } from "react";

import { useLucent, NodeLive } from "../state/store";
import { presetsFor, runChaos } from "./chaos";

const SHARD_COLORS = [
  "var(--s0)", "var(--s1)", "var(--s2)", "var(--s3)",
  "var(--s4)", "var(--s5)", "var(--s6)", "var(--s7)",
];

function fmtBytes(b: number): string {
  if (b <= 0) return "—";
  const mb = b / (1024 * 1024);
  return mb >= 1024 ? `${(mb / 1024).toFixed(2)} GB` : `${mb.toFixed(0)} MB`;
}
function fmtInt(n: number): string {
  return n > 0 ? n.toLocaleString() : "—";
}
function stateLabel(s: string | null): string {
  return (s ?? "").replace("NODE_STATE_", "").toLowerCase() || "—";
}

function Sparkline({ values, color }: { values: number[]; color: string }) {
  if (values.length < 2) return <svg className="spark" viewBox="0 0 100 24" />;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = Math.max(1, max - min);
  const pts = values
    .map((v, i) => {
      const x = (i / (values.length - 1)) * 100;
      const y = 24 - ((v - min) / range) * 22 - 1;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  return (
    <svg className="spark" viewBox="0 0 100 24" preserveAspectRatio="none">
      <polyline points={pts} fill="none" stroke={color} strokeWidth="1.5" />
    </svg>
  );
}

function NodeCard({ nodeId, live, color }: { nodeId: string; live: NodeLive; color: string }) {
  const s = live.stats;
  const building = live.state === "NODE_STATE_BUILDING" || live.build !== null;
  const docs = Number(s?.docCount ?? 0);
  const edges = Number(s?.edgeCount ?? 0);
  const rss = Number(s?.rssBytes ?? 0);
  const p99 = Number(s?.p99Us ?? 0) / 1000;

  return (
    <div className={`nodecard ${building ? "building" : ""}`}>
      <div className="nodecard-head">
        <span className="dot" style={{ background: color }} />
        <span className="mono nodeid">{nodeId}</span>
        <span className={`state ${live.state === "NODE_STATE_SERVING" ? "ok" : ""}`}>
          {stateLabel(live.state)}
        </span>
      </div>

      {building && live.build && (
        <div className="buildbar-wrap">
          <div className="buildbar" style={{
            width: `${live.build.total ? (live.build.inserted / live.build.total) * 100 : 30}%`,
            background: color,
          }} />
          <span className="mono buildlabel">
            building · {fmtInt(live.build.inserted)}
            {live.build.total ? ` / ${fmtInt(live.build.total)}` : ""} ·{" "}
            {fmtInt(live.build.edgeCount)} edges
          </span>
        </div>
      )}

      <div className="nodecard-stats">
        <Stat label="docs" value={fmtInt(docs)} />
        <Stat label="edges" value={fmtInt(edges)} />
        <Stat label="p99" value={p99 > 0 ? `${p99.toFixed(1)}ms` : "—"} />
        <Stat label="qps" value={s?.qps1s ? s.qps1s.toFixed(1) : "—"} />
      </div>

      <div className="nodecard-mem">
        <span className="mono memval">{fmtBytes(rss)}</span>
        <Sparkline values={live.rssHistory} color={color} />
      </div>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="stat">
      <span className="mono statval">{value}</span>
      <span className="statlabel">{label}</span>
    </div>
  );
}

// Background traffic control (M2-T5). Poisson arrivals at the coordinator so
// the stage is alive and shards run hot; SPANS tier, never FULL.
function LoadGenControl() {
  const [on, setOn] = useState(false);
  const [qps, setQps] = useState(10);
  const [skew, setSkew] = useState<"uniform" | "zipf">("uniform");
  const [sent, setSent] = useState(0);

  // Reflect the actual backend state on mount (load-gen may already be running).
  useEffect(() => {
    void fetch("/api/loadgen")
      .then((r) => r.json())
      .then((st) => {
        setOn(!!st.enabled);
        if (st.qps) setQps(st.qps);
        if (st.skew) setSkew(st.skew);
        setSent(st.sent ?? 0);
      })
      .catch(() => {});
  }, []);

  const push = (enabled: boolean, q = qps, s = skew) =>
    fetch("/api/loadgen", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ enabled, qps: q, skew: s }),
    }).then((r) => r.json()).then((st) => setOn(st.enabled));

  // Poll sent count while running (visible proof traffic is flowing).
  useEffect(() => {
    if (!on) return;
    const id = setInterval(() => {
      void fetch("/api/loadgen").then((r) => r.json()).then((st) => setSent(st.sent));
    }, 800);
    return () => clearInterval(id);
  }, [on]);

  return (
    <div className="loadgen">
      <button className={`lg-toggle ${on ? "on" : ""}`} onClick={() => void push(!on)}>
        load {on ? "on" : "off"}
      </button>
      <label className="mono">
        <input
          type="range" min={1} max={50} value={qps}
          onChange={(e) => {
            const q = Number(e.target.value);
            setQps(q);
            if (on) void push(true, q, skew);
          }}
        />
        {qps} qps
      </label>
      <select
        value={skew}
        onChange={(e) => {
          const s = e.target.value as "uniform" | "zipf";
          setSkew(s);
          if (on) void push(true, qps, s);
        }}
      >
        <option value="uniform">uniform</option>
        <option value="zipf">zipf (hot)</option>
      </select>
      {on && <span className="mono dim">{sent.toLocaleString()} sent</span>}
    </div>
  );
}

// Chaos block (frontend.md §5.3): node picker + the six commands. Same actions
// as right-clicking a stage node — both build from CHAOS_PRESETS. Faults only
// show for shard nodes; kill/restart apply to any supervised process.
function ChaosControl({ nodeIds }: { nodeIds: string[] }) {
  const source = useLucent((s) => s.source);
  const setToast = useLucent((s) => s.setChaosToast);
  const [node, setNode] = useState<string>("");

  // Default the picker to the first shard once the node list is known.
  const options = useMemo(() => nodeIds, [nodeIds]);
  useEffect(() => {
    if (!node && options.length) setNode(options.find((n) => /^shard-/.test(n)) ?? options[0]!);
  }, [options, node]);

  const fire = async (label: string, cmd: ReturnType<typeof presetsFor>[number]["cmd"]) => {
    if (!source || !node) return;
    try {
      setToast({ text: await runChaos(source, cmd(node)), error: false });
    } catch (err) {
      setToast({ text: `${node}: ${label} failed — ${(err as Error).message}`, error: true });
    }
  };

  return (
    <div className="chaos-block">
      <span className="chaos-label mono">chaos</span>
      <select className="mono" value={node} onChange={(e) => setNode(e.target.value)}>
        {options.length === 0 && <option value="">no nodes</option>}
        {options.map((id) => <option key={id} value={id}>{id}</option>)}
      </select>
      <div className="chaos-buttons">
        {presetsFor(node || "shard-0a").map((p) => (
          <button
            key={p.label}
            className={`chaos-btn ${p.danger ? "danger" : ""}`}
            disabled={!node}
            onClick={() => void fire(p.label, p.cmd)}
          >
            {p.label}
          </button>
        ))}
      </div>
    </div>
  );
}

export function ClusterPanel() {
  const open = useLucent((s) => s.clusterOpen);
  const close = useLucent((s) => s.setClusterOpen);
  const nodes = useLucent((s) => s.nodes);
  const cluster = useLucent((s) => s.cluster);

  if (!open) return null;

  const entries = [...nodes.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  const shardOf = (id: string) => {
    const m = /^shard-(\d+)/.exec(id);
    return m ? Number(m[1]) : -1;
  };

  // Chaos picker options: every process we can address — the coordinator and
  // embed plus each shard's primary AND backup from the live map (backups may
  // not emit stats yet, so union the map in, not just `nodes`).
  const chaosNodes = new Set<string>(["coord-0", "embed-0"]);
  for (const id of nodes.keys()) chaosNodes.add(id);
  for (const s of cluster?.shardMap?.shards ?? []) {
    if (s.primaryNode) chaosNodes.add(s.primaryNode);
    if (s.backupNode) chaosNodes.add(s.backupNode);
  }
  const chaosNodeIds = [...chaosNodes].sort((a, b) => a.localeCompare(b));
  const totalRss = entries.reduce((t, [, l]) => t + Number(l.stats?.rssBytes ?? 0), 0);
  const totalEdges = entries.reduce((t, [, l]) => t + Number(l.stats?.edgeCount ?? 0), 0);
  const totalDocs = entries.reduce((t, [, l]) => t + Number(l.stats?.docCount ?? 0), 0);

  return (
    <div className="cluster-panel">
      <div className="cluster-head">
        <span className="brand">cluster</span>
        <span className="mono dim">
          {entries.length} nodes · {totalDocs.toLocaleString()} docs ·{" "}
          {totalEdges.toLocaleString()} edges · {fmtBytes(totalRss)} resident
        </span>
        <div className="spacer" />
        <button className="close" onClick={() => close(false)}>✕</button>
      </div>
      <LoadGenControl />
      <ChaosControl nodeIds={chaosNodeIds} />
      <div className="cluster-grid">
        {entries.length === 0 && (
          <div className="cluster-empty">waiting for node stats…</div>
        )}
        {entries.map(([id, live]) => {
          const sid = shardOf(id);
          const color = sid >= 0 ? SHARD_COLORS[sid % SHARD_COLORS.length]! : "var(--text-dim)";
          return <NodeCard key={id} nodeId={id} live={live} color={color} />;
        })}
      </div>
    </div>
  );
}
