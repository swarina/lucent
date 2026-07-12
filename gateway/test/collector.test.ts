import { describe, expect, it } from "vitest";

import { EVENT_RING_CAPACITY, EventStore, topicOf } from "../src/collector.js";
import { Event, SpanKind } from "../src/gen/lucent/v1/events.js";
import { NodeState } from "../src/gen/lucent/v1/common.js";

function span(nodeId: string, seq: number, traceId = "aa"): Event {
  return Event.fromPartial({
    nodeId,
    seq: BigInt(seq),
    span: { kind: SpanKind.SPAN_SHARD_SEARCH, traceId: Buffer.from(traceId, "hex") },
  });
}
function stats(nodeId: string, seq: number): Event {
  return Event.fromPartial({ nodeId, seq: BigInt(seq), stats: { rssBytes: 1n } });
}
function stateChange(nodeId: string, seq: number): Event {
  return Event.fromPartial({ nodeId, seq: BigInt(seq), state: { to: NodeState.NODE_STATE_SERVING } });
}

describe("topicOf", () => {
  it("routes span/stats/other to spans/metrics/cluster", () => {
    expect(topicOf(span("n", 1))).toBe("spans");
    expect(topicOf(stats("n", 1))).toBe("metrics");
    expect(topicOf(stateChange("n", 1))).toBe("cluster");
  });
});

describe("EventStore", () => {
  it("fans out to subscribers by topic", () => {
    const store = new EventStore();
    const got: Record<string, number> = {};
    store.subscribe((topic, events) => {
      got[topic] = (got[topic] ?? 0) + events.length;
    });
    store.ingest([span("a", 1), stats("a", 2), stateChange("a", 3), span("a", 4)]);
    expect(got).toEqual({ spans: 2, metrics: 1, cluster: 1 });
  });

  it("caps the ring", () => {
    const store = new EventStore();
    for (let i = 0; i < EVENT_RING_CAPACITY + 500; i += 100) {
      store.ingest(Array.from({ length: 100 }, (_, j) => span("a", i + j + 1)));
    }
    expect(store.size).toBe(EVENT_RING_CAPACITY);
    // Oldest were evicted: the most recent event is retained.
    const recent = store.recent(1)[0]!;
    expect(recent.seq).toBe(BigInt(EVENT_RING_CAPACITY + 500));
  });

  it("detects drops via seq gaps per node", () => {
    const store = new EventStore();
    store.ingest([span("a", 1), span("a", 2)]);
    store.ingest([span("a", 5)]); // 3,4 lost
    store.ingest([span("b", 1)]); // other node unaffected
    expect(store.droppedDetected).toBe(2);
  });

  it("finds spans by trace id hex", () => {
    const store = new EventStore();
    store.ingest([span("a", 1, "deadbeef"), span("a", 2, "cafe"), stats("a", 3)]);
    expect(store.spansForTrace("deadbeef")).toHaveLength(1);
    expect(store.spansForTrace("cafe")).toHaveLength(1);
    expect(store.spansForTrace("0000")).toHaveLength(0);
  });

  it("unsubscribe stops delivery", () => {
    const store = new EventStore();
    let n = 0;
    const unsub = store.subscribe(() => { n += 1; });
    store.ingest([span("a", 1)]);
    unsub();
    store.ingest([span("a", 2)]);
    expect(n).toBe(1);
  });
});
