// Trace permalinks (frontend.md §2, M6-T3). The inspector's open trace is
// mirrored into the URL hash (`#/trace/{hex}`) so a "watch it think" view is
// shareable — including on the static GitHub Pages demo, where hash routing
// needs no server rewrites. Kept as a hash (not a path) for exactly that reason.

const RE = /^#\/trace\/([0-9a-f]{6,64})$/i;

/** The trace id in the current URL hash, or null. */
export function traceFromHash(): string | null {
  const m = window.location.hash.match(RE);
  return m ? m[1]!.toLowerCase() : null;
}

/** Reflect the open trace into the hash without adding a history entry. */
export function writeTraceHash(traceId: string | null): void {
  const want = traceId ? `#/trace/${traceId}` : "";
  if (window.location.hash === want) return;
  // Clearing: drop the hash entirely rather than leave a bare "#".
  const url = want || window.location.pathname + window.location.search;
  window.history.replaceState(null, "", url);
}
