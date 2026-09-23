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
  const openInspector = useLucent((s) => s.openInspector);

  if (queryState === "error") {
    return (
      <aside className="results">
        <div className="coverage bad">query failed: {queryError}</div>
      </aside>
    );
  }
  // In flight: no spinner on the stage (the animation is the loading state);
  // here we show skeleton rows so the panel doesn't flash stale results.
  if (queryState === "inflight") {
    return (
      <aside className="results">
        <div className="coverage skeleton" />
        <ol className="results-skeleton">
          {Array.from({ length: 6 }).map((_, i) => (
            <li key={i}>
              <div className="sk sk-title" />
              <div className="sk sk-snippet" />
              <div className="sk sk-meta" />
            </li>
          ))}
        </ol>
      </aside>
    );
  }
  if (lastResponse === null) {
    return (
      <aside className="results">
        <div className="placeholder">
          results appear here, with which shard and replica served each one
        </div>
      </aside>
    );
  }

  const cov = lastResponse.coverage ?? {};
  const probed = cov.probed ?? 0;
  const answered = cov.answered ?? 0;
  const degraded = answered < probed;

  // Coverage = 0: an explicit error card, so a total outage never reads as
  // "your query simply had no matches" (frontend.md §6).
  if (probed > 0 && answered === 0) {
    return (
      <aside className="results">
        <div className="coverage bad">
          no shards responded ({probed} probed). No results to show; the query
          did not reach the index.
        </div>
      </aside>
    );
  }
  const totalMs = Number(lastResponse.timings?.totalUs ?? 0) / 1000;
  const visited = Number(lastResponse.visitedTotal ?? 0);

  return (
    <aside className="results">
      <div className={`coverage ${degraded ? "bad" : "ok"}`}>
        coverage {answered}/{probed}
        {degraded && `: shard${(cov.missingShards ?? []).length > 1 ? "s" : ""} ${(cov.missingShards ?? []).join(", ")} did not respond, results may be incomplete`}
        <span className="mono"> · {totalMs.toFixed(1)}ms</span>
      </div>
      <button
        className="inspect-btn"
        onClick={() => openInspector(lastResponse.traceId)}
        title="watch the per-shard HNSW traversal in 3D"
      >
        ▸ watch it think{visited > 0 ? ` (${visited.toLocaleString()} nodes visited)` : ""}
      </button>
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
