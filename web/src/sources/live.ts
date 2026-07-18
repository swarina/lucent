// LiveSource: WS /ws/live + REST against the gateway (frontend.md §2). The
// same interface will be implemented by ReplaySource (M1-T5 fixtures, M6
// static demo) — keep this surface small and JSON-only.

import {
  base64ToHex,
  ClusterStateJson,
  EventJson,
  QueryResponseJson,
} from "../types";

export interface QueryParams {
  text: string;
  k?: number;
  ef?: number;
  probe?: number;
}

export type EventHandler = (topic: string, events: EventJson[]) => void;

// Chaos surface (frontend.md §5.3). Process-level chaos (kill/restart) hits the
// supervisor; in-process faults (pause/slow/drop/clear) hit the shard. `ms`/`p`
// are only meaningful for the fault kinds.
export type ChaosCmd =
  | { kind: "kill" | "restart"; nodeId: string }
  | { kind: "pause" | "slow"; nodeId: string; ms: number }
  | { kind: "drop"; nodeId: string; p: number }
  | { kind: "clear"; nodeId: string };

export class LiveSource {
  private ws: WebSocket | null = null;
  private handlers = new Set<EventHandler>();
  private reconnectMs = 500;
  connected = false;
  onStatusChange: ((connected: boolean) => void) | null = null;

  start(): void {
    const url = `ws://${window.location.host}/ws/live`;
    this.ws = new WebSocket(url);
    this.ws.onopen = () => {
      this.connected = true;
      this.reconnectMs = 500;
      this.onStatusChange?.(true);
      this.ws?.send(JSON.stringify({ subscribe: ["spans", "metrics", "cluster"] }));
    };
    this.ws.onmessage = (msg) => {
      const frame = JSON.parse(String(msg.data)) as {
        topic: string;
        events?: EventJson[];
      };
      if (!frame.events) return;
      // Normalize proto3-JSON quirks once, at the boundary: bytes arrive as
      // base64 (traceId -> hex), and zero-valued fields are omitted entirely
      // (shardId 0 arrives as undefined, which would break shard-0 linkage).
      for (const e of frame.events) {
        if (e.span) {
          if (e.span.traceId) e.span.traceId = base64ToHex(e.span.traceId);
          e.span.shardId = e.span.shardId ?? 0;
        }
      }
      for (const h of this.handlers) h(frame.topic, frame.events);
    };
    this.ws.onclose = () => {
      this.connected = false;
      this.onStatusChange?.(false);
      setTimeout(() => this.start(), this.reconnectMs);
      this.reconnectMs = Math.min(this.reconnectMs * 2, 10_000);
    };
    this.ws.onerror = () => this.ws?.close();
  }

  onEvents(handler: EventHandler): () => void {
    this.handlers.add(handler);
    return () => this.handlers.delete(handler);
  }

  async query(params: QueryParams): Promise<QueryResponseJson> {
    const resp = await fetch("/api/query", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(params),
    });
    const body = (await resp.json()) as QueryResponseJson & {
      error?: { code: string; message: string };
    };
    if (!resp.ok || body.error) {
      throw new Error(body.error?.message ?? `query failed (${resp.status})`);
    }
    return body;
  }

  async cluster(): Promise<ClusterStateJson> {
    const resp = await fetch("/api/cluster");
    if (!resp.ok) throw new Error(`cluster state failed (${resp.status})`);
    return (await resp.json()) as ClusterStateJson;
  }

  /** Readiness + which embed model is running (fake vs real) for the UI banner. */
  async ready(): Promise<{ ready: boolean; embedModel: string | null; fakeEmbed: boolean }> {
    const resp = await fetch("/api/ready");
    return (await resp.json()) as { ready: boolean; embedModel: string | null; fakeEmbed: boolean };
  }

  async chaos(cmd: ChaosCmd): Promise<{ active?: string }> {
    const [path, body] =
      cmd.kind === "kill" || cmd.kind === "restart"
        ? [`/api/chaos/${cmd.kind}`, { nodeId: cmd.nodeId }]
        : ["/api/fault", cmd];
    const resp = await fetch(path, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    const json = (await resp.json()) as {
      active?: string;
      error?: { code: string; message: string };
    };
    if (!resp.ok || json.error) {
      throw new Error(json.error?.message ?? `chaos ${cmd.kind} failed (${resp.status})`);
    }
    return json;
  }
}
