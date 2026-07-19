// The cluster stage (2D SVG, frontend.md §5.1): embed — coord — shards
// radially. Nodes glow on recent event activity; on each query a fan-out beam
// sweeps coord→shard and the answer returns coord←shard, so you watch
// scatter-gather happen. Dead shards get a red outbound pulse that fizzles;
// unprobed shards stay dim. Linked highlighting via the shared hover state.

import { useEffect, useReducer, useState } from "react";

import { useLucent } from "../state/store";

const SHARD_COLORS = [
  "var(--s0)", "var(--s1)", "var(--s2)", "var(--s3)",
  "var(--s4)", "var(--s5)", "var(--s6)", "var(--s7)",
];

const GLOW_HALF_LIFE_MS = 1500;

function glowFor(last: number | undefined, now: number): number {
  if (last === undefined) return 0;
  return Math.pow(0.5, (now - last) / GLOW_HALF_LIFE_MS);
}

// One fan-out beam: coord→shard (query) then shard→coord (result), animated by
// a CSS keyframe (mount-relative, so remounting via the parent <g> key replays
// it each query — no React re-renders, unlike rAF; no document-timeline gotcha,
// unlike SMIL). --dx/--dy carry the coord→shard delta into the keyframe.
// Missing shards: an outbound-only red pulse that fades (request left, nothing
// returned).
function Beam({ sx, sy, ex, ey, color, missing }: {
  sx: number; sy: number; ex: number; ey: number; color: string; missing: boolean;
}) {
  const style = {
    "--dx": `${ex - sx}`,
    "--dy": `${ey - sy}`,
  } as React.CSSProperties;
  return (
    <circle
      className={missing ? "beam beam-miss" : "beam beam-fanout"}
      r={4}
      cx={sx}
      cy={sy}
      fill={missing ? "var(--down)" : color}
      style={style}
    />
  );
}

export function Stage() {
  const cluster = useLucent((s) => s.cluster);
  const lastActivity = useLucent((s) => s.lastActivity);
  const hover = useLucent((s) => s.hover);
  const setHover = useLucent((s) => s.setHover);
  const lastResponse = useLucent((s) => s.lastResponse);
  const activeTrace = useLucent((s) => s.activeTrace);
  const setChaosMenu = useLucent((s) => s.setChaosMenu);

  // Right-click any node → chaos menu at the cursor (frontend.md §5.3).
  const onNodeContext = (nodeId: string, e: React.MouseEvent) => {
    e.preventDefault();
    setChaosMenu({ nodeId, x: e.clientX, y: e.clientY });
  };

  const [, tick] = useReducer((n: number) => n + 1, 0);
  useEffect(() => {
    const id = setInterval(tick, 120); // glow decay
    return () => clearInterval(id);
  }, []);

  // Semantic partition legend: dominant category per shard, e.g. "cs.*(72%)".
  // Absent under hash partitioning (endpoint 404s) → no labels shown.
  const [labels, setLabels] = useState<Record<string, string>>({});
  useEffect(() => {
    void fetch("/api/partition")
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error("hash"))))
      .then((p: { labels?: Record<string, string> }) => setLabels(p.labels ?? {}))
      .catch(() => setLabels({}));
  }, []);

  const shards = cluster?.shardMap?.shards ?? [];
  const now = performance.now();
  const missing = new Set(lastResponse?.coverage?.missingShards ?? []);
  const unprobed = new Set(lastResponse?.coverage?.unprobedShards ?? []);

  const W = 640;
  const H = 420;
  const cx = W / 2;
  const cy = H / 2;
  const R = 150;

  return (
    <svg className="stage" viewBox={`0 0 ${W} ${H}`}>
      <StageNode
        id="embed-0" x={cx - 210} y={cy} r={16} color="var(--text-dim)"
        glow={glowFor(lastActivity.get("embed-0"), now)} label="embed"
        hovered={hover?.ref === "embed-0"} onHover={setHover} onContext={onNodeContext}
      />
      <line className="wire" x1={cx - 194} y1={cy} x2={cx - 26} y2={cy} />
      {/* embed pulse: fires just before the fan-out (query -> embed) */}
      {activeTrace && (
        <circle
          key={`embed-${activeTrace}`}
          className="beam beam-embed"
          r={4}
          cx={cx - 26}
          cy={cy}
          fill="var(--trace-a)"
          style={{ "--dx": `${-168}`, "--dy": "0" } as React.CSSProperties}
        />
      )}

      {/* fan-out beams, keyed by trace so each query replays them */}
      <g key={activeTrace ?? "none"}>
        {activeTrace && shards.map((s, i) => {
          const angle = -Math.PI / 2 + (i * 2 * Math.PI) / Math.max(1, shards.length);
          const shardId = s.shardId ?? i;
          if (unprobed.has(shardId)) return null;
          const sx = cx + 24 * Math.cos(angle);
          const sy = cy + 24 * Math.sin(angle);
          const ex = cx + (R - 22) * Math.cos(angle);
          const ey = cy + (R - 22) * Math.sin(angle);
          return (
            <Beam key={shardId} sx={sx} sy={sy} ex={ex} ey={ey}
              color={SHARD_COLORS[shardId % SHARD_COLORS.length]!}
              missing={missing.has(shardId)} />
          );
        })}
      </g>

      {shards.map((s, i) => {
        const angle = -Math.PI / 2 + (i * 2 * Math.PI) / Math.max(1, shards.length);
        const x = cx + R * Math.cos(angle);
        const y = cy + R * Math.sin(angle);
        const shardId = s.shardId ?? i;
        const nodeId = s.primaryNode ?? `shard-${shardId}a`;
        const isHover = hover?.shardId === shardId || hover?.ref === nodeId;
        const color = SHARD_COLORS[shardId % SHARD_COLORS.length]!;
        const isMissing = missing.has(shardId);
        const isUnprobed = unprobed.has(shardId);
        const ex = cx + (R - 22) * Math.cos(angle);
        const ey = cy + (R - 22) * Math.sin(angle);

        return (
          <g key={nodeId}>
            <line
              className={`wire ${isMissing ? "wire-dead" : ""} ${isUnprobed ? "wire-dim" : ""}`}
              x1={cx} y1={cy} x2={ex} y2={ey}
            />
            <StageNode
              id={nodeId} x={x} y={y} r={18} color={color}
              glow={glowFor(lastActivity.get(nodeId), now)} label={`s${shardId}`}
              dead={isMissing} dim={isUnprobed} hovered={isHover} shardId={shardId}
              onHover={setHover} onContext={onNodeContext}
            />
            {labels[String(shardId)] && (
              <text
                className="shard-label"
                x={x}
                y={y + (Math.sin(angle) >= 0 ? 36 : -28)}
                textAnchor="middle"
                fill={color}
              >
                {labels[String(shardId)]}
              </text>
            )}
          </g>
        );
      })}

      <StageNode
        id="coord-0" x={cx} y={cy} r={22} color="var(--text)"
        glow={glowFor(lastActivity.get("coord-0"), now)} label="coord"
        hovered={hover?.ref === "coord-0"} onHover={setHover} onContext={onNodeContext}
      />

      {/* Discoverability: the chaos menu is a right-click that's otherwise
          invisible. Only shown once there are shards to act on. */}
      {shards.length > 0 && (
        <text className="stage-hint" x={cx} y={H - 10} textAnchor="middle">
          right-click any node to inject a fault
        </text>
      )}
    </svg>
  );
}

function StageNode(props: {
  id: string;
  x: number;
  y: number;
  r: number;
  color: string;
  glow: number;
  label: string;
  dead?: boolean;
  dim?: boolean;
  hovered?: boolean;
  shardId?: number;
  onHover: (h: { kind: "stageNode"; ref: string; shardId?: number } | null) => void;
  onContext?: (id: string, e: React.MouseEvent) => void;
}) {
  const { id, x, y, r, color, glow, label, dead, dim, hovered, shardId, onHover, onContext } = props;
  return (
    <g
      className={`stagenode ${dead ? "dead" : ""} ${dim ? "dim" : ""}`}
      onMouseEnter={() => onHover({ kind: "stageNode", ref: id, shardId })}
      onMouseLeave={() => onHover(null)}
      onContextMenu={onContext ? (e) => onContext(id, e) : undefined}
    >
      {glow > 0.02 && (
        <circle cx={x} cy={y} r={r + 6 + glow * 8} fill={color} opacity={glow * 0.25} />
      )}
      <circle
        cx={x} cy={y} r={r}
        fill="var(--panel-2)"
        stroke={dead ? "var(--down)" : color}
        strokeWidth={hovered ? 3 : 1.5}
        opacity={dim ? 0.35 : 1}
      />
      <text className="nodelabel" x={x} y={y + 4} textAnchor="middle">
        {label}
      </text>
      <title>{id} — right-click for chaos (kill / restart / fault)</title>
    </g>
  );
}
