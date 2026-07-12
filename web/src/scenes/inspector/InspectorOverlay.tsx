// Full-screen inspector overlay: loads a trace's per-shard traversal data,
// hosts the 3D scene, and drives the dilated replay clock + HUD (shard tabs,
// counters, scrubber, speed). "Replay, ×N slower than life" is always shown —
// the animation is real data time-dilated, never live-speed theatre.

import { OrbitControls } from "@react-three/drei";
import { Canvas, useFrame } from "@react-three/fiber";
import { useEffect, useMemo, useRef, useState } from "react";

import { useLucent } from "../../state/store";
import { loadTraceInspector, ShardInspectorData } from "./data";
import { InspectorScene } from "./InspectorScene";

const PLAY_SECONDS = 5; // full traversal plays over ~5s at speed ×1

export function InspectorOverlay() {
  const traceId = useLucent((s) => s.inspectorTrace);
  const close = useLucent((s) => s.closeInspector);
  const [shards, setShards] = useState<ShardInspectorData[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [idx, setIdx] = useState(0);

  useEffect(() => {
    if (!traceId) return;
    let live = true;
    setShards(null);
    setError(null);
    setIdx(0);
    loadTraceInspector(traceId)
      .then((s) => live && (s.length ? setShards(s) : setError("no FULL trace blobs for this query")))
      .catch((e) => live && setError(String(e)));
    return () => {
      live = false;
    };
  }, [traceId]);

  if (!traceId) return null;
  const data = shards?.[idx] ?? null;

  return (
    <div className="inspector">
      <div className="inspector-bar">
        <span className="brand">inspector</span>
        {shards && shards.length > 1 && (
          <div className="shard-tabs">
            {shards.map((s, i) => (
              <button
                key={s.nodeId}
                className={i === idx ? "active" : ""}
                onClick={() => setIdx(i)}
              >
                shard {s.shardId}
              </button>
            ))}
          </div>
        )}
        <div className="spacer" />
        <button className="close" onClick={close}>
          ✕ close
        </button>
      </div>

      {error && <div className="inspector-msg">{error}</div>}
      {!shards && !error && <div className="inspector-msg">loading traversal…</div>}
      {data && <InspectorStage data={data} />}
    </div>
  );
}

function InspectorStage({ data }: { data: ShardInspectorData }) {
  const cursorUsRef = useRef(0);
  const [cursorUs, setCursorUs] = useState(0);
  const [playing, setPlaying] = useState(true);
  const [speed, setSpeed] = useState(1);

  // Reset the clock when the shard changes.
  useEffect(() => {
    cursorUsRef.current = 0;
    setCursorUs(0);
    setPlaying(true);
  }, [data]);

  // R3F v8's initial measure can latch onto 300×150 before layout settles
  // (StrictMode double-mount); nudge a re-measure so the renderer buffer
  // matches the container.
  useEffect(() => {
    const ids = [0, 60, 200].map((ms) =>
      window.setTimeout(() => window.dispatchEvent(new Event("resize")), ms),
    );
    return () => ids.forEach(clearTimeout);
  }, []);

  const dilation = useMemo(
    () => Math.max(1, (PLAY_SECONDS * 1e6) / Math.max(1, data.durationUs) / speed),
    [data.durationUs, speed],
  );
  const visitedShown = useMemo(() => {
    let v = 0;
    for (const h of data.hops) if (h.tOffUs <= cursorUs && h.kind === 1) v++;
    return v;
  }, [data.hops, cursorUs]);
  const pct = data.pointCount ? ((data.visited / data.pointCount) * 100).toFixed(1) : "0";

  return (
    <>
      <div className="inspector-canvas">
        <Canvas
          dpr={[1, 2]}
          style={{ width: "100%", height: "100%" }}
          camera={{ fov: 45, near: 0.1, far: 1000 }}
        >
          <InspectorScene data={data} cursorUsRef={cursorUsRef} />
          <OrbitControls
            enablePan={false}
            enableDamping
            dampingFactor={0.08}
            minDistance={6}
            maxDistance={40}
          />
          <ClockDriver
            durationUs={data.durationUs}
            playing={playing}
            speed={speed}
            playSeconds={PLAY_SECONDS}
            cursorUsRef={cursorUsRef}
            onTick={setCursorUs}
            onEnd={() => setPlaying(false)}
          />
        </Canvas>
      </div>

      <div className="inspector-hud">
        <div className="hud-counter">
          <span className="mono big">
            visited {data.visited} / {data.pointCount}
          </span>
          <span className="mono dim">({pct}% of the shard)</span>
          <span className="mono dim">
            layers {data.maxLayer + 1} · results {data.hops.filter((h) => h.kind === 4).length}
            {data.dropped > 0 && ` · dropped ${data.dropped}`}
          </span>
        </div>
        <div className="hud-controls">
          <button onClick={() => setPlaying((p) => !p)}>{playing ? "❚❚" : "▶"}</button>
          <input
            type="range"
            min={0}
            max={Math.max(1, data.durationUs)}
            value={cursorUs}
            onChange={(e) => {
              const v = Number(e.target.value);
              cursorUsRef.current = v;
              setCursorUs(v);
              setPlaying(false);
            }}
          />
          <span className="mono dim">
            {(cursorUs / 1000).toFixed(1)} / {(data.durationUs / 1000).toFixed(1)} ms
          </span>
          <label className="mono dim">
            speed
            <select value={speed} onChange={(e) => setSpeed(Number(e.target.value))}>
              <option value={0.25}>0.25×</option>
              <option value={0.5}>0.5×</option>
              <option value={1}>1×</option>
              <option value={2}>2×</option>
              <option value={4}>4×</option>
            </select>
          </label>
        </div>
        <div className="hud-dilation mono">
          replay · ~{Math.round(dilation)}× slower than life · visited so far {visitedShown}
        </div>
      </div>
    </>
  );
}

function ClockDriver({
  durationUs,
  playing,
  speed,
  playSeconds,
  cursorUsRef,
  onTick,
  onEnd,
}: {
  durationUs: number;
  playing: boolean;
  speed: number;
  playSeconds: number;
  cursorUsRef: React.MutableRefObject<number>;
  onTick: (us: number) => void;
  onEnd: () => void;
}) {
  const accum = useRef(0);
  useFrame((_, delta) => {
    if (!playing) return;
    const ratePerSec = (durationUs / playSeconds) * speed; // µs of trace per real s
    cursorUsRef.current += delta * ratePerSec;
    if (cursorUsRef.current >= durationUs) {
      cursorUsRef.current = durationUs;
      onTick(durationUs);
      onEnd();
      return;
    }
    // Throttle React updates to ~20 Hz; the scene reads the ref every frame.
    accum.current += delta;
    if (accum.current >= 0.05) {
      accum.current = 0;
      onTick(cursorUsRef.current);
    }
  });
  return null;
}
