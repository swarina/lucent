// Zustand stores (frontend.md §3, M0 slice). `hover` is the single source of
// linked highlighting: waterfall spans and stage elements read/write the same
// selection.

import { create } from "zustand";

import {
  ClusterStateJson,
  EventJson,
  HitJson,
  QueryResponseJson,
  SpanJson,
} from "../types";

export interface HoverRef {
  kind: "span" | "stageNode";
  /** stage node id ("coord-0", "shard-1a", "embed-0") or span key */
  ref: string;
  /** shard id the hover maps to, if any — the linkage key */
  shardId?: number;
}

interface LucentState {
  connected: boolean;
  cluster: ClusterStateJson | null;

  activeTrace: string | null;
  spansByTrace: Map<string, SpanJson[]>;
  /** nodeId -> last activity mono-ns (drives stage glow decay) */
  lastActivity: Map<string, number>;

  results: HitJson[];
  lastResponse: QueryResponseJson | null;
  queryState: "idle" | "inflight" | "error";
  queryError: string | null;

  hover: HoverRef | null;

  /** trace id the 3D inspector is open on, or null (frontend.md §5.2) */
  inspectorTrace: string | null;

  setConnected(c: boolean): void;
  openInspector(traceId: string): void;
  closeInspector(): void;
  setCluster(c: ClusterStateJson): void;
  ingestEvents(events: EventJson[]): void;
  queryStarted(): void;
  queryFinished(resp: QueryResponseJson): void;
  queryFailed(message: string): void;
  setHover(h: HoverRef | null): void;
}

const MAX_TRACES = 50;

export const useLucent = create<LucentState>((set) => ({
  connected: false,
  cluster: null,
  activeTrace: null,
  spansByTrace: new Map(),
  lastActivity: new Map(),
  results: [],
  lastResponse: null,
  queryState: "idle",
  queryError: null,
  hover: null,
  inspectorTrace: null,

  setConnected: (connected) => set({ connected }),
  openInspector: (inspectorTrace) => set({ inspectorTrace }),
  closeInspector: () => set({ inspectorTrace: null }),
  setCluster: (cluster) => set({ cluster }),

  ingestEvents: (events) =>
    set((state) => {
      const spansByTrace = new Map(state.spansByTrace);
      const lastActivity = new Map(state.lastActivity);
      const now = performance.now();
      for (const e of events) {
        lastActivity.set(e.nodeId, now);
        if (e.span) {
          const spans = spansByTrace.get(e.span.traceId) ?? [];
          spansByTrace.set(e.span.traceId, [...spans, e.span]);
        }
      }
      // Bound retained traces (oldest-first eviction by insertion order).
      while (spansByTrace.size > MAX_TRACES) {
        const oldest = spansByTrace.keys().next().value as string;
        spansByTrace.delete(oldest);
      }
      return { spansByTrace, lastActivity };
    }),

  queryStarted: () => set({ queryState: "inflight", queryError: null }),
  queryFinished: (resp) =>
    set({
      queryState: "idle",
      results: resp.hits ?? [],
      lastResponse: resp,
      activeTrace: resp.traceId,
    }),
  queryFailed: (message) =>
    set({ queryState: "error", queryError: message, results: [] }),

  setHover: (hover) => set({ hover }),
}));

/** Layout helper: normalize a trace's spans to [0,1] against its time span. */
export function waterfallLayout(
  spans: SpanJson[],
): { span: SpanJson; start: number; width: number }[] {
  if (spans.length === 0) return [];
  // Numbers are safe here: mono-ns values fit double precision for years of
  // uptime, and only differences matter for layout.
  const ts = spans.map((s) => [Number(s.tStartNs ?? 0), Number(s.tEndNs ?? 0)]);
  const t0 = Math.min(...ts.map(([a]) => a));
  const t1 = Math.max(...ts.map(([, b]) => b));
  const range = Math.max(1, t1 - t0);
  return spans.map((span, i) => {
    const [a, b] = ts[i]!;
    return {
      span,
      start: (a - t0) / range,
      width: Math.max(0.002, (b - a) / range),
    };
  });
}
