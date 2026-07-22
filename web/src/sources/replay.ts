// ReplaySource: plays a recorded bundle back with no backend (M6-T2). Same
// Source surface as LiveSource, so the whole UI — stage, waterfall, cluster,
// raft — animates identically off `bundle/events.ndjson` driven by a virtual
// clock. This is what the static GitHub Pages demo (M6-T3) runs on.

import { base64ToHex, ClusterStateJson, EventJson, QueryResponseJson } from "../types";
import type {
  ChaosCmd,
  EventHandler,
  QueryParams,
  ReadyInfo,
  ReplayInfo,
  Source,
} from "./live";

interface BundleManifest {
  duration_ms?: number;
  cluster?: ClusterStateJson;
  chapters?: { t_ms: number; label: string }[];
}

// A recorded query completion: fire `resp` into the store when the replay clock
// crosses `idx` (the index of that trace's last event in the stream).
interface QueryFire {
  idx: number;
  resp: QueryResponseJson;
}

export class ReplaySource implements Source {
  onStatusChange: ((connected: boolean) => void) | null = null;
  onQueryReplay: ((resp: QueryResponseJson) => void) | null = null;
  onReplayLoop: (() => void) | null = null;
  replay: ReplayInfo;

  private handlers = new Set<EventHandler>();
  private events: EventJson[] = [];
  private fires: QueryFire[] = [];
  private manifest: BundleManifest = {};
  private raf = 0;
  private wall0 = 0;
  private idx = 0;

  private constructor(manifest: BundleManifest, events: EventJson[], fires: QueryFire[]) {
    this.manifest = manifest;
    this.events = events;
    this.fires = fires;
    this.replay = {
      chapters: (manifest.chapters ?? []).map((c) => ({ tMs: c.t_ms, label: c.label })),
      durationMs: manifest.duration_ms ?? 0,
      progress: 0,
    };
  }

  // Try to load a bundle from the static `bundle/` path. Returns null if none.
  static async load(base = "bundle"): Promise<ReplaySource | null> {
    try {
      const m = await fetch(`${base}/manifest.json`);
      if (!m.ok) return null;
      const manifest = (await m.json()) as BundleManifest;
      const e = await fetch(`${base}/events.ndjson`);
      const text = e.ok ? await e.text() : "";
      const events: EventJson[] = [];
      for (const line of text.split("\n")) {
        if (!line.trim()) continue;
        try {
          const ev = JSON.parse(line) as EventJson;
          // Normalize proto3-JSON once, at load (same as LiveSource does live).
          if (ev.span) {
            if (ev.span.traceId) ev.span.traceId = base64ToHex(ev.span.traceId);
            ev.span.shardId = ev.span.shardId ?? 0;
          }
          events.push(ev);
        } catch {
          /* skip a malformed line */
        }
      }
      const fires = await ReplaySource.loadFires(base, events);
      return new ReplaySource(manifest, events, fires);
    } catch {
      return null;
    }
  }

  // Pair each recorded query trace with its saved result JSON, anchored to the
  // index of that trace's last event — so the replay clock can fire it exactly
  // when the traversal finishes, lighting the results panel + waterfall.
  private static async loadFires(base: string, events: EventJson[]): Promise<QueryFire[]> {
    const lastIdx = new Map<string, number>();
    for (let i = 0; i < events.length; i++) {
      const t = events[i]?.span?.traceId;
      if (t) lastIdx.set(t, i);
    }
    const fetched = await Promise.all(
      [...lastIdx.keys()].map(async (hex) => {
        try {
          const r = await fetch(`${base}/results/${hex}.json`);
          if (!r.ok) return null;  // trace with no saved result (e.g. failover)
          const resp = (await r.json()) as QueryResponseJson;
          return { idx: lastIdx.get(hex) ?? 0, resp };
        } catch {
          return null;
        }
      }),
    );
    return fetched.filter((f): f is QueryFire => f !== null).sort((a, b) => a.idx - b.idx);
  }

  onEvents(handler: EventHandler): () => void {
    this.handlers.add(handler);
    return () => this.handlers.delete(handler);
  }

  start(): void {
    this.onStatusChange?.(true);  // "connected" to the recording
    this.wall0 = performance.now();
    this.idx = 0;
    const tick = () => {
      // Spread the recorded events over the recorded duration (min 3s so a short
      // capture is still watchable), then loop.
      const dur = Math.max(3000, this.replay.durationMs);
      const frac = (performance.now() - this.wall0) / dur;
      const target = Math.min(this.events.length, Math.floor(frac * this.events.length));
      if (target > this.idx) {
        const batch = this.events.slice(this.idx, target);
        const prev = this.idx;
        this.idx = target;
        for (const h of this.handlers) h("replay", batch);
        // Fire any recorded query completions whose last event just played, so
        // the results panel + waterfall resolve in step with the traversal.
        for (const f of this.fires) {
          if (f.idx >= prev && f.idx < target) this.onQueryReplay?.(f.resp);
        }
      }
      this.replay.progress = Math.min(1, Math.max(0, frac));
      if (frac >= 1) {
        this.wall0 = performance.now();  // loop
        this.idx = 0;
        // The same trace ids are about to replay — clear accumulated per-trace
        // state so spans don't stack up into a doubled waterfall each loop.
        this.onReplayLoop?.();
      }
      this.raf = requestAnimationFrame(tick);
    };
    this.raf = requestAnimationFrame(tick);
  }

  stop(): void {
    if (this.raf) cancelAnimationFrame(this.raf);
  }

  async cluster(): Promise<ClusterStateJson> {
    return this.manifest.cluster ?? ({} as ClusterStateJson);
  }

  async ready(): Promise<ReadyInfo> {
    // Replay is its own thing — not the fake encoder, so no fake-embed banner.
    return { ready: true, embedModel: null, fakeEmbed: false };
  }

  async query(_params: QueryParams): Promise<QueryResponseJson> {
    // The recorded queries replay themselves through the event stream; live
    // search has no backend to answer it.
    throw new Error("recorded session — search is replayed, not live");
  }

  async chaos(_cmd: ChaosCmd): Promise<{ active?: string }> {
    // No live cluster to poke — surface that as feedback (the caller toasts it)
    // rather than silently doing nothing under the "inject a fault" hint.
    throw new Error("recorded session — the cluster isn't live to inject faults");
  }
}
