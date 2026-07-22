// Shared chaos command presets (frontend.md §5.3). The same six commands are
// reachable two ways — the ops/cluster panel's chaos block and a right-click on
// a stage node — so both surfaces build their buttons from this one list.

import type { ChaosCmd, Source } from "../sources/live";

export interface ChaosPreset {
  label: string;
  /** faults only make sense on a shard; kill/restart work on any supervised node */
  shardOnly: boolean;
  /** build the command for a given node */
  cmd(nodeId: string): ChaosCmd;
  /** destructive actions get a warmer button */
  danger?: boolean;
}

export const CHAOS_PRESETS: ChaosPreset[] = [
  { label: "kill", shardOnly: false, danger: true, cmd: (nodeId) => ({ kind: "kill", nodeId }) },
  { label: "restart", shardOnly: false, cmd: (nodeId) => ({ kind: "restart", nodeId }) },
  { label: "pause 3s", shardOnly: true, cmd: (nodeId) => ({ kind: "pause", nodeId, ms: 3000 }) },
  { label: "slow 200ms", shardOnly: true, cmd: (nodeId) => ({ kind: "slow", nodeId, ms: 200 }) },
  { label: "drop 20%", shardOnly: true, cmd: (nodeId) => ({ kind: "drop", nodeId, p: 0.2 }) },
  { label: "clear", shardOnly: true, cmd: (nodeId) => ({ kind: "clear", nodeId }) },
];

export function isShardNode(nodeId: string): boolean {
  return /^shard-\d+[ab]$/.test(nodeId);
}

export function presetsFor(nodeId: string): ChaosPreset[] {
  const shard = isShardNode(nodeId);
  return CHAOS_PRESETS.filter((p) => shard || !p.shardOnly);
}

// Run a preset and return a short human string for the toast (or throw).
export async function runChaos(source: Source, cmd: ChaosCmd): Promise<string> {
  const res = await source.chaos(cmd);
  const detail =
    cmd.kind === "pause" || cmd.kind === "slow"
      ? ` ${cmd.ms}ms`
      : cmd.kind === "drop"
        ? ` ${Math.round(cmd.p * 100)}%`
        : "";
  const active = res.active ? ` (${res.active})` : "";
  return `${cmd.nodeId}: ${cmd.kind}${detail}${active}`;
}
