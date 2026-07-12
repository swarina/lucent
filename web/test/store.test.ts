import { beforeEach, describe, expect, it } from "vitest";

import { useLucent, waterfallLayout } from "../src/state/store";
import { EventJson, SpanJson } from "../src/types";

function spanEvent(nodeId: string, traceId: string, kind: SpanJson["kind"],
                   t0: number, t1: number, shardId?: number): EventJson {
  return {
    nodeId,
    span: { traceId, kind, tStartNs: String(t0), tEndNs: String(t1), shardId },
  };
}

beforeEach(() => {
  useLucent.setState({
    spansByTrace: new Map(),
    lastActivity: new Map(),
    results: [],
    lastResponse: null,
    activeTrace: null,
    queryState: "idle",
    hover: null,
  });
});

describe("store.ingestEvents", () => {
  it("groups spans by trace and tracks node activity", () => {
    useLucent.getState().ingestEvents([
      spanEvent("coord-0", "aaaa", "SPAN_EMBED", 100, 200),
      spanEvent("shard-0a", "aaaa", "SPAN_SHARD_SEARCH", 210, 300, 0),
      spanEvent("coord-0", "bbbb", "SPAN_QUERY_RECEIVED", 400, 400),
    ]);
    const s = useLucent.getState();
    expect(s.spansByTrace.get("aaaa")).toHaveLength(2);
    expect(s.spansByTrace.get("bbbb")).toHaveLength(1);
    expect(s.lastActivity.has("shard-0a")).toBe(true);
  });

  it("evicts oldest traces beyond the cap", () => {
    for (let i = 0; i < 55; i++) {
      useLucent.getState().ingestEvents([
        spanEvent("coord-0", `t${i}`, "SPAN_QUERY_DONE", i, i + 1),
      ]);
    }
    const s = useLucent.getState();
    expect(s.spansByTrace.size).toBe(50);
    expect(s.spansByTrace.has("t0")).toBe(false);
    expect(s.spansByTrace.has("t54")).toBe(true);
  });
});

describe("query lifecycle", () => {
  it("finished sets results and active trace", () => {
    useLucent.getState().queryStarted();
    expect(useLucent.getState().queryState).toBe("inflight");
    useLucent.getState().queryFinished({
      traceId: "cafe",
      hits: [{ docId: "1", score: 0.9 }],
      coverage: { probed: 2, answered: 2 },
    });
    const s = useLucent.getState();
    expect(s.activeTrace).toBe("cafe");
    expect(s.results).toHaveLength(1);
    expect(s.queryState).toBe("idle");
  });

  it("failed records the message", () => {
    useLucent.getState().queryFailed("embed down");
    expect(useLucent.getState().queryState).toBe("error");
    expect(useLucent.getState().queryError).toBe("embed down");
  });
});

describe("waterfallLayout", () => {
  it("normalizes to [0,1] across the trace window", () => {
    const spans: SpanJson[] = [
      { traceId: "x", kind: "SPAN_QUERY_RECEIVED", tStartNs: "1000", tEndNs: "1000" },
      { traceId: "x", kind: "SPAN_EMBED", tStartNs: "1000", tEndNs: "2000" },
      { traceId: "x", kind: "SPAN_MERGE", tStartNs: "2500", tEndNs: "3000" },
    ];
    const rows = waterfallLayout(spans);
    expect(rows[1]!.start).toBeCloseTo(0);
    expect(rows[1]!.width).toBeCloseTo(0.5);
    expect(rows[2]!.start).toBeCloseTo(0.75);
    expect(rows[0]!.width).toBeGreaterThan(0); // zero-duration span still visible
  });

  it("handles empty", () => {
    expect(waterfallLayout([])).toEqual([]);
  });
});
