// Theater shell (M0-T10 slice of frontend.md §5.1): QueryBar on top, Stage
// center, Results right, Waterfall drawer below. The inspector (3D), ops and
// ingest views land with M1/M3.

import { useEffect, useMemo } from "react";

import { QueryBar } from "./components/QueryBar";
import { ResultsPanel } from "./components/ResultsPanel";
import { Stage } from "./components/Stage";
import { Waterfall } from "./components/Waterfall";
import { LiveSource } from "./sources/live";
import { useLucent } from "./state/store";

export function App() {
  const source = useMemo(() => new LiveSource(), []);

  useEffect(() => {
    source.onStatusChange = (c) => {
      useLucent.getState().setConnected(c);
      if (c) {
        void source.cluster().then((cl) => useLucent.getState().setCluster(cl));
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
      <QueryBar source={source} />
      <div className="mainrow">
        <Stage />
        <ResultsPanel />
      </div>
      <Waterfall />
    </div>
  );
}
