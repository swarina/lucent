// WebSocket /ws/live (protocol.md §4): topic subscriptions, frames flushed at
// <= 30 Hz, per-connection queue cap 1000 frames with drop-oldest (a slow
// browser must never back up the collector).

import type { Server as HttpServer } from "node:http";
import { WebSocket, WebSocketServer } from "ws";

import type { EventStore, Topic } from "./collector.js";
import { Event } from "./gen/lucent/v1/events.js";

const FLUSH_MS = 33; // ~30 Hz
const QUEUE_CAP = 1000;
const TOPICS: readonly Topic[] = ["spans", "metrics", "cluster"];

interface Conn {
  ws: WebSocket;
  topics: Set<Topic>;
  queue: { topic: Topic; events: Event[] }[];
  droppedFrames: number;
}

export function attachLiveWs(httpServer: HttpServer, store: EventStore): WebSocketServer {
  const wss = new WebSocketServer({ server: httpServer, path: "/ws/live" });
  const conns = new Set<Conn>();

  wss.on("connection", (ws) => {
    const conn: Conn = { ws, topics: new Set(), queue: [], droppedFrames: 0 };
    conns.add(conn);
    ws.on("message", (data) => {
      try {
        const msg = JSON.parse(String(data)) as { subscribe?: string[] };
        if (Array.isArray(msg.subscribe)) {
          // Resubscribe replaces the set (protocol.md §4).
          conn.topics = new Set(
            msg.subscribe.filter((t): t is Topic => (TOPICS as readonly string[]).includes(t)),
          );
        }
      } catch {
        ws.send(JSON.stringify({ error: { code: "BAD_MESSAGE", message: "expected JSON" } }));
      }
    });
    ws.on("close", () => conns.delete(conn));
  });

  const unsubscribe = store.subscribe((topic, events) => {
    for (const conn of conns) {
      if (!conn.topics.has(topic)) continue;
      conn.queue.push({ topic, events });
      if (conn.queue.length > QUEUE_CAP) {
        conn.queue.splice(0, conn.queue.length - QUEUE_CAP);
        conn.droppedFrames += 1;
      }
    }
  });

  const flusher = setInterval(() => {
    for (const conn of conns) {
      if (conn.queue.length === 0 || conn.ws.readyState !== WebSocket.OPEN) continue;
      // Coalesce queued batches per topic into one frame each.
      const byTopic = new Map<Topic, Event[]>();
      for (const { topic, events } of conn.queue) {
        let bucket = byTopic.get(topic);
        if (bucket === undefined) byTopic.set(topic, (bucket = []));
        bucket.push(...events);
      }
      conn.queue = [];
      for (const [topic, events] of byTopic) {
        conn.ws.send(
          JSON.stringify({ topic, events: events.map((e) => Event.toJSON(e)) }),
        );
      }
      if (conn.droppedFrames > 0) {
        conn.ws.send(JSON.stringify({ topic: "meta", lagging: true, droppedFrames: conn.droppedFrames }));
        conn.droppedFrames = 0;
      }
    }
  }, FLUSH_MS);

  wss.on("close", () => {
    clearInterval(flusher);
    unsubscribe();
  });
  return wss;
}
