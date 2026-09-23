// First-run onboarding (frontend.md §6, "first run, no corpus"). When the
// cluster is up but no corpus has been ingested, the stage is an empty cluster
// with nothing to search. Instead of a blank stage, show one clear next step:
// the ingest command. Once any documents land the panel removes itself and the
// stage fills in. Ingest is CLI-driven (there is no in-browser trigger), so the
// CTA is a copyable command rather than a button.

import { useState } from "react";

import { useLucent } from "../state/store";

const INGEST_CMD =
  "uv --project py run lucent ingest --config .lucent/cluster-dev.yaml \\\n  --corpus testdata/corpus-2k.jsonl --n 2000";

export function FirstRunWizard() {
  const connected = useLucent((s) => s.connected);
  const replay = useLucent((s) => !!s.source?.replay);
  const nodes = useLucent((s) => s.nodes);
  const [copied, setCopied] = useState(false);

  // Replay bundles always carry data; only a live, connected cluster can be empty.
  if (replay || !connected) return null;

  let shardNodes = 0;
  let statsSeen = false;
  let totalDocs = 0;
  for (const [id, n] of nodes) {
    if (!id.startsWith("shard-")) continue;
    shardNodes++;
    if (n.stats) {
      statsSeen = true;
      totalDocs += Number(n.stats.docCount ?? 0);
    }
  }
  // Wait until the shards have reported stats, and only when every one is empty.
  if (shardNodes === 0 || !statsSeen || totalDocs > 0) return null;

  const copy = () => {
    const flat = INGEST_CMD.replace(/\\\n\s*/g, " ");
    void navigator.clipboard?.writeText(flat).then(
      () => {
        setCopied(true);
        setTimeout(() => setCopied(false), 1500);
      },
      () => {},
    );
  };

  return (
    <div className="firstrun">
      <div className="firstrun-card">
        <div className="firstrun-tag">no corpus loaded yet</div>
        <h2>The cluster is running. Now give it something to search.</h2>
        <p>
          The shards are up but empty. Ingest the bundled 2k-document arXiv sample
          (local, no download) to start searching. This panel disappears and the
          stage fills in as documents load.
        </p>
        <div className="firstrun-cmd">
          <pre>
            <code>{INGEST_CMD}</code>
          </pre>
          <button type="button" onClick={copy}>
            {copied ? "copied ✓" : "copy"}
          </button>
        </div>
        <p className="firstrun-hint">
          Run it in a second terminal, then try a query like{" "}
          <em>quantum entanglement between photons</em> or{" "}
          <em>neural networks for image recognition</em>.
        </p>
      </div>
    </div>
  );
}
