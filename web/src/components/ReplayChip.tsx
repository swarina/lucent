// Replay-mode indicator (M6-T2). Shown only when the app booted off a recorded
// bundle (ReplaySource). Reads `source.replay` — whose `progress` is mutated in
// place by the replay clock — via its own rAF tick, and surfaces the current
// chapter so a viewer knows which scene ("queries", "failover", …) is playing.

import { useEffect, useState } from "react";

import { Source } from "../sources/live";

export function ReplayChip({ source }: { source: Source }) {
  const info = source.replay;
  const [progress, setProgress] = useState(0);

  useEffect(() => {
    let raf = 0;
    const tick = () => {
      setProgress(source.replay?.progress ?? 0);
      raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [source]);

  if (!info) return null;

  // Latest chapter whose start-time we've passed (chapters are sorted by tMs).
  const tMs = progress * info.durationMs;
  let chapter = info.chapters[0]?.label ?? "replay";
  for (const c of info.chapters) if (tMs >= c.tMs) chapter = c.label;

  return (
    <div className="replay-chip" title="recorded session — playing back a captured run">
      <span className="rc-tag">▶ replay</span>
      <span className="rc-chapter">{chapter}</span>
      <span className="rc-bar">
        <span className="rc-fill" style={{ width: `${Math.round(progress * 100)}%` }} />
      </span>
    </div>
  );
}
