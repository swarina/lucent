// Jaeger-style span waterfall for the active trace (frontend.md §5.1). The
// credibility view: real timestamps, one row per span, linked highlighting
// with the stage through the shared hover state.

import { useLucent, waterfallLayout } from "../state/store";
import { SpanJson } from "../types";

const KIND_LABEL: Record<string, string> = {
  SPAN_QUERY_RECEIVED: "received",
  SPAN_EMBED: "embed",
  SPAN_PLAN: "plan",
  SPAN_SHARD_RPC: "shard rpc",
  SPAN_SHARD_SEARCH: "shard search",
  SPAN_MERGE: "merge",
  SPAN_QUERY_DONE: "done",
};

const SHARD_COLORS = [
  "var(--s0)", "var(--s1)", "var(--s2)", "var(--s3)",
  "var(--s4)", "var(--s5)", "var(--s6)", "var(--s7)",
];

function colorFor(span: SpanJson): string {
  if (span.kind === "SPAN_SHARD_RPC" || span.kind === "SPAN_SHARD_SEARCH") {
    return SHARD_COLORS[(span.shardId ?? 0) % SHARD_COLORS.length]!;
  }
  return "var(--trace-a)";
}

function label(span: SpanJson): string {
  const base = KIND_LABEL[span.kind] ?? span.kind;
  if (span.kind === "SPAN_SHARD_RPC" || span.kind === "SPAN_SHARD_SEARCH") {
    return `${base} s${span.shardId ?? "?"}`;
  }
  return base;
}

function durationUs(span: SpanJson): number {
  return (Number(span.tEndNs ?? 0) - Number(span.tStartNs ?? 0)) / 1000;
}

export function Waterfall() {
  const activeTrace = useLucent((s) => s.activeTrace);
  const spansByTrace = useLucent((s) => s.spansByTrace);
  const hover = useLucent((s) => s.hover);
  const setHover = useLucent((s) => s.setHover);

  const spans = activeTrace ? (spansByTrace.get(activeTrace) ?? []) : [];
  if (spans.length === 0) {
    return (
      <div className="waterfall empty">
        {activeTrace ? "waiting for spans…" : "run a query to see its trace"}
      </div>
    );
  }

  // Stable narrative order: coordinator ladder first, shard work by shard id.
  const ORDER: Record<string, number> = {
    SPAN_QUERY_RECEIVED: 0, SPAN_EMBED: 1, SPAN_PLAN: 2,
    SPAN_SHARD_RPC: 3, SPAN_SHARD_SEARCH: 4, SPAN_MERGE: 5, SPAN_QUERY_DONE: 6,
  };
  const rows = waterfallLayout(spans).sort((a, b) => {
    const ka = ORDER[a.span.kind] ?? 9;
    const kb = ORDER[b.span.kind] ?? 9;
    if (ka !== kb) return ka - kb;
    return (a.span.shardId ?? 0) - (b.span.shardId ?? 0);
  });

  return (
    <div className="waterfall">
      <div className="waterfall-head">
        trace <span className="mono">{activeTrace?.slice(0, 16)}…</span>
      </div>
      {rows.map(({ span, start, width }, i) => {
        const isShardSpan =
          span.kind === "SPAN_SHARD_RPC" || span.kind === "SPAN_SHARD_SEARCH";
        const highlighted =
          hover !== null &&
          ((isShardSpan && hover.shardId === span.shardId) ||
            (!isShardSpan && hover.kind === "span" && hover.ref === `${span.kind}-${i}`));
        return (
          <div
            key={`${span.kind}-${span.shardId}-${i}`}
            className={`spanrow ${highlighted ? "hl" : ""}`}
            onMouseEnter={() =>
              setHover({
                kind: "span",
                ref: `${span.kind}-${i}`,
                shardId: isShardSpan ? span.shardId : undefined,
              })
            }
            onMouseLeave={() => setHover(null)}
          >
            <span className="spanlabel">{label(span)}</span>
            <div className="spantrack">
              <div
                className="spanbar"
                style={{
                  left: `${start * 100}%`,
                  width: `${Math.max(0.4, width * 100)}%`,
                  background: colorFor(span),
                }}
              />
            </div>
            <span className="spandur mono">{durationUs(span).toFixed(0)}µs</span>
          </div>
        );
      })}
    </div>
  );
}
