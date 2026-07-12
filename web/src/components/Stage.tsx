// The cluster stage (2D SVG, frontend.md §5.1 minimal slice): embed — coord —
// shards radially, nodes glow on recent event activity, linked highlighting
// with the waterfall via the shared hover state.

import { useEffect, useReducer } from "react";

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

export function Stage() {
  const cluster = useLucent((s) => s.cluster);
  const lastActivity = useLucent((s) => s.lastActivity);
  const hover = useLucent((s) => s.hover);
  const setHover = useLucent((s) => s.setHover);
  const lastResponse = useLucent((s) => s.lastResponse);

  // Re-render for glow decay while anything is lit.
  const [, tick] = useReducer((n: number) => n + 1, 0);
  useEffect(() => {
    const id = setInterval(tick, 120);
    return () => clearInterval(id);
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
      {/* embed node, left of coordinator */}
      <StageNode
        id="embed-0"
        x={cx - 210}
        y={cy}
        r={16}
        color="var(--text-dim)"
        glow={glowFor(lastActivity.get("embed-0"), now)}
        label="embed"
        hovered={hover?.ref === "embed-0"}
        onHover={setHover}
      />
      <line className="wire" x1={cx - 194} y1={cy} x2={cx - 26} y2={cy} />

      {/* shards, radial */}
      {shards.map((s, i) => {
        const angle = -Math.PI / 2 + (i * 2 * Math.PI) / Math.max(1, shards.length);
        const x = cx + R * Math.cos(angle);
        const y = cy + R * Math.sin(angle);
        const shardId = s.shardId ?? i;
        const nodeId = s.primaryNode ?? `shard-${shardId}a`;
        const isHover = hover?.shardId === shardId || hover?.ref === nodeId;
        return (
          <g key={nodeId}>
            <line
              className={`wire ${missing.has(shardId) ? "wire-dead" : ""} ${unprobed.has(shardId) ? "wire-dim" : ""}`}
              x1={cx} y1={cy}
              x2={cx + (R - 22) * Math.cos(angle)}
              y2={cy + (R - 22) * Math.sin(angle)}
            />
            <StageNode
              id={nodeId}
              x={x}
              y={y}
              r={18}
              color={SHARD_COLORS[shardId % SHARD_COLORS.length]!}
              glow={glowFor(lastActivity.get(nodeId), now)}
              label={`s${shardId}`}
              dead={missing.has(shardId)}
              dim={unprobed.has(shardId)}
              hovered={isHover}
              shardId={shardId}
              onHover={setHover}
            />
          </g>
        );
      })}

      {/* coordinator, center — drawn last so it sits on top of wires */}
      <StageNode
        id="coord-0"
        x={cx}
        y={cy}
        r={22}
        color="var(--text)"
        glow={glowFor(lastActivity.get("coord-0"), now)}
        label="coord"
        hovered={hover?.ref === "coord-0"}
        onHover={setHover}
      />
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
}) {
  const { id, x, y, r, color, glow, label, dead, dim, hovered, shardId, onHover } = props;
  return (
    <g
      className={`stagenode ${dead ? "dead" : ""} ${dim ? "dim" : ""}`}
      onMouseEnter={() => onHover({ kind: "stageNode", ref: id, shardId })}
      onMouseLeave={() => onHover(null)}
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
      <title>{id}</title>
    </g>
  );
}
