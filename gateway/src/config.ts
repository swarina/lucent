// Typed view of cluster.yaml — the fields the gateway needs (mirrors
// cpp/common/config and py lucent/config: die loudly, no silent defaults).

import { readFileSync } from "node:fs";
import path from "node:path";
import { parse } from "yaml";

export interface GatewayConfig {
  shards: number;
  replicas: number;
  partitioning: "hash" | "semantic";
  /** absolute path to the data dir (paths.data resolved vs process cwd —
   *  the convention all processes share; the supervisor spawns everything
   *  at the repo root) */
  dataDir: string;
  ports: {
    coordinator: number;
    embed: number;
    collector: number;
    gatewayHttp: number;
    supervisorCtl: number;
    shardBase: number;
  };
}

function require_(obj: Record<string, unknown>, key: string, path: string): unknown {
  const v = obj?.[key];
  if (v === undefined || v === null) {
    throw new Error(`cluster.yaml: missing required field '${path}.${key}'`);
  }
  return v;
}

export function loadConfig(configPath: string): GatewayConfig {
  const raw = parse(readFileSync(configPath, "utf-8")) as Record<string, unknown>;
  const cluster = require_(raw, "cluster", "") as Record<string, unknown>;
  const ports = require_(raw, "ports", "") as Record<string, unknown>;
  const paths = require_(raw, "paths", "") as Record<string, unknown>;
  const partitioning = require_(cluster, "partitioning", "cluster") as string;
  if (partitioning !== "hash" && partitioning !== "semantic") {
    throw new Error(`cluster.yaml: bad partitioning '${partitioning}'`);
  }
  return {
    shards: Number(require_(cluster, "shards", "cluster")),
    replicas: Number(require_(cluster, "replicas", "cluster")),
    partitioning,
    dataDir: path.resolve(String(require_(paths, "data", "paths"))),
    ports: {
      coordinator: Number(require_(ports, "coordinator", "ports")),
      embed: Number(require_(ports, "embed", "ports")),
      collector: Number(require_(ports, "collector", "ports")),
      gatewayHttp: Number(require_(ports, "gateway_http", "ports")),
      supervisorCtl: Number(require_(ports, "supervisor_ctl", "ports")),
      shardBase: Number(require_(ports, "shard_base", "ports")),
    },
  };
}
