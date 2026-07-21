// Mini-Raft election viz (M5-T4). Renders the three voters live from
// RaftEvents: the leader glows gold, followers sit quiet, a candidate flares
// amber mid-election, and a member that goes silent (killed) fades to "down".
// Kill the leader (chaos, below) and watch a new one take over in well under a
// second. Hidden entirely unless the cluster runs --raft.

import { useEffect, useReducer } from "react";

import { useLucent } from "../state/store";

// A member that hasn't emitted its ~1 Hz heartbeat in this long is "down".
const STALE_MS = 2500;

export function RaftPanel() {
  const raft = useLucent((s) => s.raft);
  // A silent member won't push an event, so re-evaluate staleness on a timer.
  const [, tick] = useReducer((n: number) => n + 1, 0);
  useEffect(() => {
    const id = setInterval(tick, 500);
    return () => clearInterval(id);
  }, []);

  if (raft.size === 0) return null;

  const now = performance.now();
  const members = [...raft.entries()]
    .map(([id, m]) => ({ id, ...m, down: now - m.seen > STALE_MS }))
    .sort((a, b) => a.id.localeCompare(b.id));

  // The authoritative leader is the live member claiming leader at the highest
  // term; a stale "leader" from a dead node (older term / silent) is demoted.
  const liveTerms = members.filter((m) => !m.down).map((m) => m.term);
  const maxTerm = liveTerms.length ? Math.max(...liveTerms) : 0;

  return (
    <div className="raft-panel">
      <div className="raft-head">
        <span className="raft-title mono">raft membership</span>
        <span className="mono dim">term {maxTerm}</span>
      </div>
      <div className="raft-members">
        {members.map((m) => {
          const isLeader = !m.down && m.role === "leader" && m.term === maxTerm;
          const cls = m.down
            ? "raft-down"
            : isLeader
              ? "raft-leader"
              : m.role === "candidate"
                ? "raft-candidate"
                : "raft-follower";
          const tag = m.down ? "down" : isLeader ? "leader" : m.role;
          return (
            <div key={m.id} className={`raft-member ${cls}`} title={`${m.id} · term ${m.term}`}>
              <span className="raft-dot" />
              <span className="raft-id mono">{m.id.replace("member-", "m")}</span>
              <span className="raft-role mono">{tag}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
