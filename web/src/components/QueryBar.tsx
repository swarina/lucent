import { FormEvent, useState } from "react";

import { LiveSource } from "../sources/live";
import { useLucent } from "../state/store";

export function QueryBar({ source }: { source: LiveSource }) {
  const [text, setText] = useState("");
  const [k, setK] = useState(10);
  const queryState = useLucent((s) => s.queryState);
  const connected = useLucent((s) => s.connected);

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    if (!text.trim()) return;
    useLucent.getState().queryStarted();
    try {
      const resp = await source.query({ text, k });
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
      <button type="submit" disabled={queryState === "inflight"}>
        {queryState === "inflight" ? "…" : "search"}
      </button>
      <span
        className={`conn ${connected ? "ok" : "down"}`}
        title={connected ? "live event stream connected" : "event stream disconnected"}
      />
    </form>
  );
}
