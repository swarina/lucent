// Ranked results with provenance — Lucent is a real search engine first
// (frontend.md §5.1); coverage is honest and always visible.

import { useLucent } from "../state/store";

export function ResultsPanel() {
  const results = useLucent((s) => s.results);
  const lastResponse = useLucent((s) => s.lastResponse);
  const queryState = useLucent((s) => s.queryState);
  const queryError = useLucent((s) => s.queryError);
  const setHover = useLucent((s) => s.setHover);
  const hover = useLucent((s) => s.hover);

  if (queryState === "error") {
    return (
      <aside className="results">
        <div className="coverage bad">query failed: {queryError}</div>
      </aside>
    );
  }
  if (lastResponse === null) {
    return (
      <aside className="results">
        <div className="placeholder">
          results appear here — with which shard and replica served each one
        </div>
      </aside>
    );
  }

  const cov = lastResponse.coverage ?? {};
  const probed = cov.probed ?? 0;
  const answered = cov.answered ?? 0;
  const degraded = answered < probed;
  const totalMs = Number(lastResponse.timings?.totalUs ?? 0) / 1000;

  return (
    <aside className="results">
      <div className={`coverage ${degraded ? "bad" : "ok"}`}>
        coverage {answered}/{probed}
        {degraded && ` — shard${(cov.missingShards ?? []).length > 1 ? "s" : ""} ${(cov.missingShards ?? []).join(", ")} did not respond; results may be incomplete`}
        <span className="mono"> · {totalMs.toFixed(1)}ms</span>
      </div>
      {results.length === 0 && <div className="placeholder">no results</div>}
      <ol>
        {results.map((h, i) => (
          <li
            key={`${h.docId}-${i}`}
            className={hover?.shardId === h.shardId ? "hl" : ""}
            onMouseEnter={() =>
              setHover({ kind: "stageNode", ref: h.nodeId ?? "", shardId: h.shardId })
            }
            onMouseLeave={() => setHover(null)}
          >
            <div className="rtitle">{h.title || `doc ${h.docId}`}</div>
            <div className="rsnippet">{h.snippet}</div>
            <div className="rmeta mono">
              s{h.shardId ?? 0} · {h.nodeId} · {h.score.toFixed(3)}
            </div>
          </li>
        ))}
      </ol>
    </aside>
  );
}
