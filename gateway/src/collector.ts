// Collector core: receives EventBatch streams from every node, retains a
// bounded ring, and fans events out to subscribers (the WS layer) by topic.
// Topic routing per protocol.md §4: spans | metrics | cluster.

import * as grpc from "@grpc/grpc-js";

import { Ack, CollectorServiceService, Event, EventBatch } from "./gen/lucent/v1/events.js";

export type Topic = "spans" | "metrics" | "cluster";
export type Subscriber = (topic: Topic, events: Event[]) => void;

export const EVENT_RING_CAPACITY = 5000;

export function topicOf(event: Event): Topic {
  if (event.span !== undefined) return "spans";
  if (event.stats !== undefined) return "metrics";
  return "cluster"; // state changes, failover, ingest/build progress, raft
}

export class EventStore {
  private ring: Event[] = [];
  private subscribers = new Set<Subscriber>();
  /** seq gaps per node mean drops (protocol.md events contract). */
  readonly lastSeqByNode = new Map<string, bigint>();
  droppedDetected = 0;

  ingest(events: Event[]): void {
    const byTopic = new Map<Topic, Event[]>();
    for (const e of events) {
      const last = this.lastSeqByNode.get(e.nodeId);
      if (last !== undefined && e.seq > last + 1n) {
        this.droppedDetected += Number(e.seq - last - 1n);
      }
      this.lastSeqByNode.set(e.nodeId, e.seq);

      this.ring.push(e);
      const t = topicOf(e);
      let bucket = byTopic.get(t);
      if (bucket === undefined) byTopic.set(t, (bucket = []));
      bucket.push(e);
    }
    if (this.ring.length > EVENT_RING_CAPACITY) {
      this.ring.splice(0, this.ring.length - EVENT_RING_CAPACITY);
    }
    for (const [topic, batch] of byTopic) {
      for (const sub of this.subscribers) sub(topic, batch);
    }
  }

  subscribe(sub: Subscriber): () => void {
    this.subscribers.add(sub);
    return () => this.subscribers.delete(sub);
  }

  /** All retained spans for a trace id (hex), oldest first. */
  spansForTrace(traceIdHex: string): Event[] {
    return this.ring.filter(
      (e) => e.span !== undefined && Buffer.from(e.span.traceId).toString("hex") === traceIdHex,
    );
  }

  recent(limit: number): Event[] {
    return this.ring.slice(-limit);
  }

  get size(): number {
    return this.ring.length;
  }
}

/** Starts the gRPC CollectorService; resolves with the bound server. */
export function startCollector(store: EventStore, bindAddr: string): Promise<grpc.Server> {
  const server = new grpc.Server();
  server.addService(CollectorServiceService, {
    publishEvents: (
      call: grpc.ServerReadableStream<EventBatch, Ack>,
      callback: grpc.sendUnaryData<Ack>,
    ) => {
      call.on("data", (batch: EventBatch) => store.ingest(batch.events));
      call.on("end", () => callback(null, Ack.create()));
      call.on("error", () => {
        /* node died mid-stream — data path unaffected, nothing to do */
      });
    },
  });
  return new Promise((resolve, reject) => {
    server.bindAsync(bindAddr, grpc.ServerCredentials.createInsecure(), (err) => {
      if (err) reject(err);
      else resolve(server);
    });
  });
}
