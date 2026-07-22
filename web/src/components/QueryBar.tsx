import { FormEvent, useState } from "react";

import { Source } from "../sources/live";
import { useLucent } from "../state/store";

export function QueryBar({ source }: { source: Source | null }) {
  const [text, setText] = useState("");
  const [k, setK] = useState(10);
  const [probe, setProbe] = useState(0); // 0 = all shards
  const shardCount = useLucent((s) => {
    let n = 0;
    for (const id of s.nodes.keys()) if (id.startsWith("shard-") && id.endsWith("a")) n++;
    return n;
  });
  const queryState = useLucent((s) => s.queryState);
  const connected = useLucent((s) => s.connected);
  const setClusterOpen = useLucent((s) => s.setClusterOpen);
  const clusterOpen = useLucent((s) => s.clusterOpen);
  const nodeCount = useLucent((s) => s.nodes.size);

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    if (!source || !text.trim()) return;
    useLucent.getState().queryStarted();
    try {
      const resp = await source.query({ text, k, probe });
      useLucent.getState().queryFinished(resp);
    } catch (err) {
      useLucent.getState().queryFailed(err instanceof Error ? err.message : String(err));
    }
  };

  return (
    <form className="querybar" onSubmit={(e) => void submit(e)}>
      <span className="brand">lucent</span>
      <input
        className="queryinput"
        placeholder="search the corpus — e.g. quantum entanglement between photons"
        value={text}
        onChange={(e) => setText(e.target.value)}
        autoFocus
      />
      <label className="knob">
        k
        <input
          type="number"
          min={1}
          max={50}
          value={k}
          onChange={(e) => setK(Number(e.target.value))}
        />
      </label>
      <label className="knob" title="shards to probe — fewer = faster, lower recall (0 = all)">
        probe
        <select value={probe} onChange={(e) => setProbe(Number(e.target.value))}>
          <option value={0}>all</option>
          {Array.from({ length: Math.max(0, shardCount) }, (_, i) => i + 1).map((n) => (
            <option key={n} value={n}>{n}</option>
          ))}
        </select>
      </label>
      <button type="submit" disabled={queryState === "inflight"}>
        {queryState === "inflight" ? "…" : "search"}
      </button>
      <button
        type="button"
        className={`cluster-toggle ${clusterOpen ? "active" : ""}`}
        onClick={() => setClusterOpen(!clusterOpen)}
        title="live cluster: memory, edges, build progress"
      >
        cluster{nodeCount > 0 ? ` ${nodeCount}` : ""}
      </button>
      <span
        className={`conn ${connected ? "ok" : "down"}`}
        title={connected ? "live event stream connected" : "event stream disconnected"}
      />
    </form>
  );
}
