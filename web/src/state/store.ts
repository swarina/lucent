// Zustand stores (frontend.md §3, M0 slice). `hover` is the single source of
// linked highlighting: waterfall spans and stage elements read/write the same
// selection.

import { create } from "zustand";

import type { LiveSource } from "../sources/live";
import {
  ClusterStateJson,
  EventJson,
  HitJson,
  NodeStatsJson,
  QueryResponseJson,
  SpanJson,
} from "../types";

/** Right-click chaos menu anchored at a viewport point over a stage node. */
export interface ChaosMenuState {
  nodeId: string;
  x: number;
  y: number;
}
/** Transient feedback line after a chaos action fires. */
export interface ChaosToast {
  text: string;
  error: boolean;
}

export interface NodeLive {
  stats: NodeStatsJson | null;
  /** rolling RSS samples (bytes) for a sparkline, newest last */
  rssHistory: number[];
  build: { inserted: number; total: number; edgeCount: number; rssBytes: number } | null;
  state: string | null; // last NodeStateChange 'to'
}

const RSS_HISTORY = 60;

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

  /** live per-node stats/build (metrics + cluster topics) */
  nodes: Map<string, NodeLive>;
  clusterOpen: boolean;

  /** trace id the 3D inspector is open on, or null (frontend.md §5.2) */
  inspectorTrace: string | null;

  /** the live source (held here so any component can drive chaos/queries) */
  source: LiveSource | null;
  /** open right-click chaos menu, or null */
  chaosMenu: ChaosMenuState | null;
  /** last chaos action result, shown briefly then cleared */
  chaosToast: ChaosToast | null;

  setSource(s: LiveSource): void;
  setChaosMenu(m: ChaosMenuState | null): void;
  setChaosToast(t: ChaosToast | null): void;
  setConnected(c: boolean): void;
  openInspector(traceId: string): void;
  closeInspector(): void;
  setClusterOpen(open: boolean): void;
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
  nodes: new Map(),
  clusterOpen: false,
  inspectorTrace: null,
  source: null,
  chaosMenu: null,
  chaosToast: null,

  setSource: (source) => set({ source }),
  setChaosMenu: (chaosMenu) => set({ chaosMenu }),
  setChaosToast: (chaosToast) => set({ chaosToast }),
  setConnected: (connected) => set({ connected }),
  openInspector: (inspectorTrace) => set({ inspectorTrace }),
  closeInspector: () => set({ inspectorTrace: null }),
  setClusterOpen: (clusterOpen) => set({ clusterOpen }),
  setCluster: (cluster) => set({ cluster }),

  ingestEvents: (events) =>
    set((state) => {
      const spansByTrace = new Map(state.spansByTrace);
      const lastActivity = new Map(state.lastActivity);
      const nodes = new Map(state.nodes);
      const now = performance.now();

      const live = (id: string): NodeLive =>
        nodes.get(id) ?? { stats: null, rssHistory: [], build: null, state: null };

      for (const e of events) {
        lastActivity.set(e.nodeId, now);
        if (e.span) {
          const spans = spansByTrace.get(e.span.traceId) ?? [];
          spansByTrace.set(e.span.traceId, [...spans, e.span]);
        }
        if (e.stats) {
          const n = { ...live(e.nodeId) };
          n.stats = e.stats;
          const rss = Number(e.stats.rssBytes ?? 0);
          n.rssHistory = [...n.rssHistory, rss].slice(-RSS_HISTORY);
          if (e.stats.state) n.state = e.stats.state;
          nodes.set(e.nodeId, n);
        }
        if (e.build) {
          const n = { ...live(e.nodeId) };
          n.build = {
            inserted: Number(e.build.inserted ?? 0),
            total: Number(e.build.total ?? 0),
            edgeCount: Number(e.build.edgeCount ?? 0),
            rssBytes: Number(e.build.rssBytes ?? 0),
          };
          nodes.set(e.nodeId, n);
        }
        if (e.state?.to) {
          const n = { ...live(e.nodeId) };
          n.state = e.state.to;
          // build finished — clear the transient progress once serving
          if (e.state.to === "NODE_STATE_SERVING") n.build = null;
          nodes.set(e.nodeId, n);
        }
      }
      while (spansByTrace.size > MAX_TRACES) {
        const oldest = spansByTrace.keys().next().value as string;
        spansByTrace.delete(oldest);
      }
      return { spansByTrace, lastActivity, nodes };
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
