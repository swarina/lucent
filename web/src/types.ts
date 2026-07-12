// JSON shapes as they cross the gateway boundary (proto3-JSON from ts-proto's
// toJSON: u64 fields arrive as strings, enums as names). The frontend speaks
// JSON, not protos — the gateway is the translation layer.

export interface HitJson {
  docId: string;
  score: number;
  title?: string;
  snippet?: string;
  shardId?: number;
  nodeId?: string;
}

export interface CoverageJson {
  probed?: number;
  answered?: number;
  missingShards?: number[];
  unprobedShards?: number[];
}

export interface TimingsJson {
  embedUs?: string;
  planUs?: string;
  fanoutUs?: string;
  mergeUs?: string;
  totalUs?: string;
}

export interface QueryResponseJson {
  hits?: HitJson[];
  traceId: string; // hex (gateway converts)
  coverage?: CoverageJson;
  timings?: TimingsJson;
}

export type SpanKindName =
  | "SPAN_QUERY_RECEIVED" | "SPAN_EMBED" | "SPAN_PLAN" | "SPAN_SHARD_RPC"
  | "SPAN_SHARD_SEARCH" | "SPAN_MERGE" | "SPAN_QUERY_DONE";

export interface SpanJson {
  traceId: string; // base64 in Event JSON (proto bytes) — normalized to hex by LiveSource
  kind: SpanKindName;
  tStartNs?: string;
  tEndNs?: string;
  shardId?: number;
  detailJson?: string;
}

export interface EventJson {
  nodeId: string;
  seq?: string;
  tMonoNs?: string;
  span?: SpanJson;
  stats?: { rssBytes?: string; docCount?: string; qps1s?: number; state?: string };
  state?: { from?: string; to?: string; reason?: string };
}

export interface ShardMapEntryJson {
  shardId?: number;
  primaryNode?: string;
  backupNode?: string;
  primaryState?: string;
  backupState?: string;
}

export interface ClusterStateJson {
  shardMap?: { epoch?: string; shards?: ShardMapEntryJson[]; partitioning?: string };
  nodes?: { nodeId?: string; health?: string; misses?: number }[];
}

/** proto bytes serialize to base64 in JSON; traces are hex everywhere else. */
export function base64ToHex(b64: string): string {
  const bin = atob(b64);
  let hex = "";
  for (let i = 0; i < bin.length; i++) {
    hex += bin.charCodeAt(i).toString(16).padStart(2, "0");
  }
  return hex;
}
