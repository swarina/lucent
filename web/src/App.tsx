// Theater shell (M0-T10 slice of frontend.md §5.1): QueryBar on top, Stage
// center, Results right, Waterfall drawer below. The inspector (3D), ops and
// ingest views land with M1/M3.

import { useEffect, useMemo } from "react";

import { ChaosMenu } from "./components/ChaosMenu";
import { ClusterPanel } from "./components/ClusterPanel";
import { QueryBar } from "./components/QueryBar";
import { ResultsPanel } from "./components/ResultsPanel";
import { Stage } from "./components/Stage";
import { Waterfall } from "./components/Waterfall";
import { InspectorOverlay } from "./scenes/inspector/InspectorOverlay";
import { LiveSource } from "./sources/live";
import { useLucent } from "./state/store";

export function App() {
  const source = useMemo(() => new LiveSource(), []);

  useEffect(() => {
    useLucent.getState().setSource(source);
    source.onStatusChange = (c) => {
      useLucent.getState().setConnected(c);
      if (c) {
        void source.cluster().then((cl) => useLucent.getState().setCluster(cl));
        void source
          .ready()
          .then((r) => useLucent.getState().setEmbedInfo(r.embedModel, r.fakeEmbed))
          .catch(() => {});
      }
    };
    const unsub = source.onEvents((_topic, events) =>
      useLucent.getState().ingestEvents(events),
    );
    source.start();
    return unsub;
  }, [source]);

  return (
    <div className="theater">
      <FakeEmbedBanner />
      <QueryBar source={source} />
      <div className="mainrow">
        <Stage />
        <ResultsPanel />
      </div>
      <Waterfall />
      <ClusterPanel />
      <InspectorOverlay />
      <ChaosMenu />
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
