// Theater shell (M0-T10 slice of frontend.md §5.1): QueryBar on top, Stage
// center, Results right, Waterfall drawer below. The inspector (3D), ops and
// ingest views land with M1/M3.

import { useEffect, useState } from "react";

import { ChaosMenu } from "./components/ChaosMenu";
import { ClusterPanel } from "./components/ClusterPanel";
import { FirstRunWizard } from "./components/FirstRunWizard";
import { QueryBar } from "./components/QueryBar";
import { ReplayChip } from "./components/ReplayChip";
import { ResultsPanel } from "./components/ResultsPanel";
import { Stage } from "./components/Stage";
import { Waterfall } from "./components/Waterfall";
import { InspectorOverlay } from "./scenes/inspector/InspectorOverlay";
import { pickSource } from "./sources/pick";
import type { Source } from "./sources/live";
import { traceFromHash, writeTraceHash } from "./state/permalink";
import { useLucent } from "./state/store";

export function App() {
  // Source is chosen asynchronously at boot: live gateway if one answers,
  // else a recorded bundle (ReplaySource). Null until that resolves (a few
  // hundred ms) — the theater renders its "connecting" state meanwhile.
  const [source, setSource] = useState<Source | null>(null);

  useEffect(() => {
    let unsub = () => {};
    let cancelled = false;
    void pickSource().then((src) => {
      if (cancelled) return;
      setSource(src);
      useLucent.getState().setSource(src);
      src.onStatusChange = (c) => {
        useLucent.getState().setConnected(c);
        if (c) {
          void src.cluster().then((cl) => useLucent.getState().setCluster(cl));
          void src
            .ready()
            .then((r) => useLucent.getState().setEmbedInfo(r.embedModel, r.fakeEmbed))
            .catch(() => {});
        }
      };
      unsub = src.onEvents((_topic, events) =>
        useLucent.getState().ingestEvents(events),
      );
      // Replay mode has no live query() to resolve — recorded completions arrive
      // through this callback as the clock reaches each trace's end.
      if (src.replay) {
        src.onQueryReplay = (resp) => useLucent.getState().queryFinished(resp);
        src.onReplayLoop = () => useLucent.getState().resetReplay();
      }
      src.start();
      // A `#/trace/{hex}` permalink opens the inspector once the source (and
      // thus replay-vs-live) is known, so it loads from the right place.
      const t = traceFromHash();
      if (t) useLucent.getState().openInspector(t);
    });
    return () => {
      cancelled = true;
      unsub();
    };
  }, []);

  // Keep the URL hash and the open inspector trace in sync both ways.
  useEffect(() => {
    const unsub = useLucent.subscribe((s, p) => {
      if (s.inspectorTrace !== p.inspectorTrace) writeTraceHash(s.inspectorTrace);
    });
    const onHash = () => {
      const t = traceFromHash();
      const st = useLucent.getState();
      if (t && t !== st.inspectorTrace) st.openInspector(t);
      else if (!t && st.inspectorTrace) st.closeInspector();
    };
    window.addEventListener("hashchange", onHash);
    return () => {
      unsub();
      window.removeEventListener("hashchange", onHash);
    };
  }, []);

  return (
    <div className="theater">
      <FakeEmbedBanner />
      <ConnectionBanner />
      {source?.replay && <ReplayChip source={source} />}
      <QueryBar source={source} />
      <div className="mainrow">
        <Stage />
        <FirstRunWizard />
        <ResultsPanel />
      </div>
      <Waterfall />
      <ClusterPanel />
      <InspectorOverlay />
      <ChaosMenu />
    </div>
  );
}

// Backend gone (frontend.md §6): once a live cluster has been reachable, a
// dropped connection shows a reconnect banner. Gated behind a short delay so the
// normal sub-second boot handshake never flashes it.
function ConnectionBanner() {
  const connected = useLucent((s) => s.connected);
  const replay = useLucent((s) => !!s.source?.replay);
  const source = useLucent((s) => s.source);
  const [show, setShow] = useState(false);

  useEffect(() => {
    if (replay || !source || connected) {
      setShow(false);
      return;
    }
    const t = setTimeout(() => setShow(true), 1500);
    return () => clearTimeout(t);
  }, [connected, replay, source]);

  if (!show) return null;
  return (
    <div className="conn-banner">
      <span className="cb-dot" />
      Connection to the cluster lost. Reconnecting…
    </div>
  );
}

// Dev-mode warning: on the fake encoder (`--fake-embed`) the pipeline is fully
// exercised but result *ranking* carries no semantic meaning, and HNSW visits
// almost the whole shard (random vectors defeat pruning). Say so, so nobody
// reads the mechanics as broken. Hidden entirely on a real model.
function FakeEmbedBanner() {
  const fake = useLucent((s) => s.fakeEmbed);
  if (!fake) return null;
  return (
    <div className="fake-embed-banner">
      <span className="feb-tag">fake embeddings</span>
      <span>
        dev mode (<code>--fake-embed</code>): the full query path is real, but
        results aren’t semantically ranked and HNSW scans nearly the whole shard.
        Run with the real model (<code>uv sync --extra embed</code>, then{" "}
        <code>lucent dev</code>) for meaningful search.
      </span>
    </div>
  );
}
