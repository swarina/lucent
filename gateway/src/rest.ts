// REST API (protocol.md §3): browser-facing JSON over the coordinator's gRPC.
// Errors: HTTP status + {"error":{"code","message"}}.

import { readFile } from "node:fs/promises";
import path from "node:path";

import * as grpc from "@grpc/grpc-js";
import type { FastifyInstance } from "fastify";

import type { EventStore } from "./collector.js";
import type { GatewayConfig } from "./config.js";
import { LoadGen } from "./loadgen.js";
import {
  AddReplicaRequest,
  AddReplicaResponse,
  ClusterState,
  ClusterStateRequest,
  CoordinatorServiceClient,
  QueryRequest,
  QueryResponse,
  RemoveReplicaRequest,
  RemoveReplicaResponse,
} from "./gen/lucent/v1/coordinator.js";
import { TraceLevel } from "./gen/lucent/v1/common.js";
import {
  EmbedServiceClient,
  InfoRequest,
  InfoResponse,
} from "./gen/lucent/v1/embed.js";
import { Event, TraceBlob } from "./gen/lucent/v1/events.js";
import {
  FaultRequest,
  FaultResponse,
  ShardServiceClient,
  TraceBlobRequest,
} from "./gen/lucent/v1/shard.js";

interface QueryBody {
  text?: string;
  k?: number;
  ef?: number;
  probe?: number;
  trace?: "none" | "spans" | "full";
}

const TRACE_LEVELS = {
  none: TraceLevel.TRACE_LEVEL_NONE,
  spans: TraceLevel.TRACE_LEVEL_SPANS,
  full: TraceLevel.TRACE_LEVEL_FULL,
} as const;

function grpcToHttp(code: grpc.status): number {
  switch (code) {
    case grpc.status.INVALID_ARGUMENT: return 400;
    case grpc.status.NOT_FOUND: return 404;
    case grpc.status.FAILED_PRECONDITION: return 409;
    case grpc.status.DEADLINE_EXCEEDED: return 504;
    case grpc.status.UNAVAILABLE: return 503;
    default: return 500;
  }
}

export function registerRoutes(
  app: FastifyInstance,
  config: GatewayConfig,
  store: EventStore,
): void {
  const coord = new CoordinatorServiceClient(
    `127.0.0.1:${config.ports.coordinator}`,
    grpc.credentials.createInsecure(),
  );

  app.post<{ Body: QueryBody }>("/api/query", (req, reply) => {
    const body = req.body ?? {};
    const qreq = QueryRequest.fromPartial({
      text: body.text ?? "",
      k: body.k ?? 10,
      efSearch: body.ef ?? 0, // 0 = coordinator default
      probe: body.probe ?? 0,
      traceLevel: TRACE_LEVELS[body.trace ?? "full"],
    });
    coord.query(qreq, (err: grpc.ServiceError | null, resp?: QueryResponse) => {
      if (err || !resp) {
        const code = err?.code ?? grpc.status.UNKNOWN;
        void reply.code(grpcToHttp(code)).send({
          error: { code: grpc.status[code], message: err?.details ?? "unknown" },
        });
        return;
      }
      const json = QueryResponse.toJSON(resp) as Record<string, unknown>;
      json["traceId"] = Buffer.from(resp.traceId).toString("hex");
      void reply.send(json);
    });
  });

  app.get("/api/cluster", (_req, reply) => {
    coord.getClusterState(
      ClusterStateRequest.create(),
      (err: grpc.ServiceError | null, resp?: ClusterState) => {
        if (err || !resp) {
          void reply.code(503).send({
            error: { code: "COORDINATOR_DOWN", message: err?.details ?? "unreachable" },
          });
          return;
        }
        void reply.send(ClusterState.toJSON(resp));
      },
    );
  });

  // Shard clients for out-of-band TraceBlob pulls, keyed by node id.
  const shardClients = new Map<string, ShardServiceClient>();
  const shardClient = (nodeId: string): ShardServiceClient | null => {
    const m = /^shard-(\d+)([ab])$/.exec(nodeId);
    if (!m) return null;
    let client = shardClients.get(nodeId);
    if (!client) {
      const port =
        config.ports.shardBase + Number(m[1]) * 10 + (m[2] === "a" ? 0 : 1);
      client = new ShardServiceClient(
        `127.0.0.1:${port}`,
        grpc.credentials.createInsecure(),
      );
      shardClients.set(nodeId, client);
    }
    return client;
  };

  app.get<{ Params: { id: string } }>("/api/trace/:id", (req, reply) => {
    const events = store.spansForTrace(req.params.id);
    const spans = events.map((e) => Event.toJSON(e));
    // Shard-side SHARD_SEARCH spans identify which nodes hold FULL blobs.
    const blobs = events
      .filter((e) => e.span?.kind === 5 /* SPAN_SHARD_SEARCH */)
      .map((e) => ({ nodeId: e.nodeId, shardId: e.span?.shardId ?? 0 }));
    void reply.send({ spans, blobs });
  });

  // All of a trace's FULL blobs, decoded to JSON in one call — the inspector
  // wants every shard's traversal without N binary round-trips + a browser
  // protobuf decoder. Shard ids come from the SHARD_SEARCH spans.
  app.get<{ Params: { id: string } }>("/api/trace/:id/blobs", async (req, reply) => {
    const holders = store
      .spansForTrace(req.params.id)
      .filter((e) => e.span?.kind === 5)
      .map((e) => ({ nodeId: e.nodeId, shardId: e.span?.shardId ?? 0 }));

    const fetchBlob = (nodeId: string) =>
      new Promise<TraceBlob | null>((resolve) => {
        const client = shardClient(nodeId);
        if (!client) return resolve(null);
        const breq = TraceBlobRequest.fromPartial({
          traceId: Buffer.from(req.params.id, "hex"),
        });
        client.getTraceBlob(breq, (err, blob) => resolve(err ? null : (blob ?? null)));
      });

    const out: Record<string, unknown> = {};
    await Promise.all(
      holders.map(async ({ nodeId, shardId }) => {
        const blob = await fetchBlob(nodeId);
        if (!blob) return;
        out[nodeId] = {
          shardId,
          node: blob.node,
          parent: blob.parent,
          dist: blob.dist,
          tOffUs: blob.tOffUs,
          meta: blob.meta, // (layer & 0xF) << 3 | kind
          dropped: blob.dropped,
        };
      }),
    );
    return reply.send({ traceId: req.params.id, blobs: out });
  });

  app.get<{ Params: { id: string; nodeId: string } }>(
    "/api/trace/:id/blob/:nodeId",
    (req, reply) => {
      const client = shardClient(req.params.nodeId);
      if (!client) {
        void reply.code(400).send({
          error: { code: "BAD_NODE", message: `not a shard node: ${req.params.nodeId}` },
        });
        return;
      }
      const breq = TraceBlobRequest.fromPartial({
        traceId: Buffer.from(req.params.id, "hex"),
      });
      client.getTraceBlob(breq, (err: grpc.ServiceError | null, blob?: TraceBlob) => {
        if (err || !blob) {
          const code = err?.code ?? grpc.status.UNKNOWN;
          void reply.code(grpcToHttp(code)).send({
            error: { code: grpc.status[code], message: err?.details ?? "unknown" },
          });
          return;
        }
        void reply
          .header("content-type", "application/octet-stream")
          .send(Buffer.from(TraceBlob.encode(blob).finish()));
      });
    },
  );

  // Per-shard 2D layout for the inspector point cloud (f32 x,y pairs,
  // shard-local row order) — an ingest-owned UI artifact (data-formats.md §2).
  app.get<{ Params: { shardId: string } }>(
    "/api/projection/:shardId",
    async (req, reply) => {
      const id = Number(req.params.shardId);
      if (!Number.isInteger(id) || id < 0 || id >= config.shards) {
        return reply.code(400).send({
          error: { code: "BAD_SHARD", message: `shard id out of range: ${req.params.shardId}` },
        });
      }
      try {
        const buf = await readFile(
          path.join(config.dataDir, "projections", `shard-${id}.f32`));
        return reply.header("content-type", "application/octet-stream").send(buf);
      } catch {
        return reply.code(404).send({
          error: { code: "NO_PROJECTION", message: "ingest has not written projections" },
        });
      }
    },
  );

  // Serve a small JSON artifact from disk, 404 if absent (M4). One helper for
  // both the recall/latency chart data and the semantic partition legend.
  const serveJsonFile = async (
    file: string, code: string, msg: string,
    reply: import("fastify").FastifyReply,
  ) => {
    try {
      return reply.header("content-type", "application/json").send(await readFile(file));
    } catch {
      return reply.code(404).send({ error: { code, message: msg } });
    }
  };

  // Latest bench sweep (data-formats.md §5) — the ops-panel recall/latency
  // scatter. bench/results/ is a sibling of the data dir.
  const benchPath = path.resolve(config.dataDir, "..", "bench", "results", "bench.json");
  app.get("/api/bench", (_req, reply) =>
    serveJsonFile(benchPath, "NO_BENCH", "run `lucent bench` to produce bench.json", reply));

  // Semantic partition metadata (sizes, spilled, per-shard category labels).
  // Absent under hash partitioning.
  app.get("/api/partition", (_req, reply) =>
    serveJsonFile(path.join(config.dataDir, "partition.json"),
      "NO_PARTITION", "hash partitioning (no centroids/labels)", reply));

  // Embed model identity, so the UI can warn when running on the fake encoder
  // (`--fake-embed`): search still works, but results carry no semantic meaning.
  // The model name is stable for a process's lifetime, so cache it once learned.
  const embed = new EmbedServiceClient(
    `127.0.0.1:${config.ports.embed}`, grpc.credentials.createInsecure());
  let embedModel: string | null = null;
  const fetchEmbedModel = () =>
    new Promise<string | null>((resolve) => {
      if (embedModel) return resolve(embedModel);
      const deadline = new Date(Date.now() + 500);
      embed.info(InfoRequest.create(), new grpc.Metadata(), { deadline },
        (err: grpc.ServiceError | null, r?: InfoResponse) => {
          if (!err && r?.model) embedModel = r.model;
          resolve(embedModel);
        });
    });

  app.get("/api/ready", (_req, reply) => {
    coord.getClusterState(ClusterStateRequest.create(), (err) => {
      void fetchEmbedModel().then((model) => {
        void reply.send({
          ready: !err,
          embedModel: model,
          // "fake-hash" is the deterministic dev encoder (EmbedFake); anything
          // else is a real sentence-transformer.
          fakeEmbed: model === "fake-hash",
          collector: { events: store.size, dropsDetected: store.droppedDetected },
        });
      });
    });
  });

  // Load generator (M2-T5): background SPANS-tier traffic so the stage is alive.
  const loadgen = new LoadGen(`127.0.0.1:${config.ports.coordinator}`, config.dataDir);
  app.post<{ Body: { enabled?: boolean; qps?: number; skew?: "uniform" | "zipf" } }>(
    "/api/loadgen",
    (req, reply) => {
      const b = req.body ?? {};
      void reply.send(loadgen.set(b.enabled ?? false, b.qps, b.skew));
    },
  );
  app.get("/api/loadgen", (_req, reply) => void reply.send(loadgen.status()));

  // Chaos surface (M3-T4). Two backends: process-level chaos (kill/restart) is
  // the supervisor's control API; in-process faults (pause/slow/drop) are the
  // shard's InjectFault RPC. Both are honest — a "kill" is a real SIGKILL.
  const supervisor = `http://127.0.0.1:${config.ports.supervisorCtl}`;
  const proxySupervisor = async (path: string, nodeId: string, reply: import("fastify").FastifyReply) => {
    if (!nodeId) {
      return reply.code(400).send({
        error: { code: "BAD_NODE", message: "nodeId is required" },
      });
    }
    try {
      const r = await fetch(`${supervisor}${path}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ nodeId }),
      });
      const json = (await r.json()) as { error?: string };
      // The supervisor answers 200 with {error} for an unknown node; surface
      // that as a 404 rather than a misleading success.
      return reply.code(json.error ? 404 : 200).send(json);
    } catch {
      return reply.code(503).send({
        error: { code: "SUPERVISOR_DOWN", message: `no supervisor at ${supervisor}` },
      });
    }
  };

  app.post<{ Body: { nodeId?: string } }>("/api/chaos/kill", (req, reply) =>
    proxySupervisor("/kill", req.body?.nodeId ?? "", reply));
  app.post<{ Body: { nodeId?: string } }>("/api/chaos/restart", (req, reply) =>
    proxySupervisor("/restart", req.body?.nodeId ?? "", reply));
  // Node add (M3-T5): spawn a replacement backup, then register it with the
  // coordinator. Order matters — the process must be SERVING (it loads the
  // primary's sealed index) before the coordinator starts routing to it.
  const addReplica = (shardId: number, nodeId: string, addr: string) =>
    new Promise<AddReplicaResponse>((resolve, reject) => {
      coord.addReplica(
        AddReplicaRequest.fromPartial({ shardId, nodeId, addr }),
        (err: grpc.ServiceError | null, r?: AddReplicaResponse) =>
          err || !r ? reject(err ?? new Error("no response")) : resolve(r));
    });

  app.post<{ Body: { shardId?: number; replica?: string } }>(
    "/api/chaos/spawn",
    async (req, reply) => {
      const shardId = req.body?.shardId ?? -1;
      const replica = req.body?.replica ?? "b";
      let spawn: { nodeId?: string; addr?: string; error?: string };
      try {
        const r = await fetch(`${supervisor}/spawn`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ shardId, replica }),
        });
        spawn = (await r.json()) as typeof spawn;
      } catch {
        return reply.code(503).send({
          error: { code: "SUPERVISOR_DOWN", message: `no supervisor at ${supervisor}` },
        });
      }
      if (spawn.error || !spawn.nodeId || !spawn.addr) {
        return reply.code(400).send({
          error: { code: "SPAWN_FAILED", message: spawn.error ?? "supervisor returned no node" },
        });
      }
      // Process is up and serving from the sealed copy; register it.
      try {
        const reg = await addReplica(shardId, spawn.nodeId, spawn.addr);
        if (reg.error) {
          return reply.code(409).send({
            error: { code: "REGISTER_FAILED", message: reg.error },
            nodeId: spawn.nodeId,
          });
        }
        return reply.send({ nodeId: spawn.nodeId, addr: spawn.addr, epoch: Number(reg.epoch) });
      } catch (err) {
        return reply.code(503).send({
          error: { code: "COORD_UNAVAILABLE", message: (err as Error).message },
          nodeId: spawn.nodeId,
        });
      }
    });

  // Node remove (M3-T5): drain from the map first (coordinator stops routing),
  // then stop the process with a graceful SIGTERM.
  app.post<{ Body: { nodeId?: string } }>("/api/chaos/drain", async (req, reply) => {
    const nodeId = req.body?.nodeId ?? "";
    if (!nodeId) {
      return reply.code(400).send({ error: { code: "BAD_NODE", message: "nodeId is required" } });
    }
    let epoch: number;
    try {
      const rem = await new Promise<RemoveReplicaResponse>((resolve, reject) => {
        coord.removeReplica(
          RemoveReplicaRequest.fromPartial({ nodeId }),
          (err: grpc.ServiceError | null, r?: RemoveReplicaResponse) =>
            err || !r ? reject(err ?? new Error("no response")) : resolve(r));
      });
      if (rem.error) {
        return reply.code(400).send({ error: { code: "DRAIN_REFUSED", message: rem.error } });
      }
      epoch = Number(rem.epoch);
    } catch (err) {
      return reply.code(503).send({
        error: { code: "COORD_UNAVAILABLE", message: (err as Error).message },
      });
    }
    // Drained from routing; now a graceful stop (SIGTERM). A missing process is
    // fine — the point was to remove it from the cluster.
    try {
      await fetch(`${supervisor}/kill`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ nodeId, signal: "SIGTERM" }),
      });
    } catch {
      // report success of the drain even if the stop couldn't be delivered
      return reply.send({ nodeId, epoch, stopped: false });
    }
    return reply.send({ nodeId, epoch, stopped: true });
  });

  // In-process fault injection on a single shard (internals.md §4).
  interface FaultBody {
    nodeId?: string;
    kind?: "pause" | "slow" | "drop" | "clear";
    ms?: number;
    p?: number;
  }
  app.post<{ Body: FaultBody }>("/api/fault", (req, reply) => {
    const b = req.body ?? {};
    const client = shardClient(b.nodeId ?? "");
    if (!client) {
      void reply.code(400).send({
        error: { code: "BAD_NODE", message: `not a shard node: ${b.nodeId ?? ""}` },
      });
      return;
    }
    let freq: FaultRequest;
    switch (b.kind) {
      case "pause": freq = FaultRequest.fromPartial({ pauseMs: Math.max(0, b.ms ?? 0) }); break;
      case "slow": freq = FaultRequest.fromPartial({ slowMs: Math.max(0, b.ms ?? 0) }); break;
      case "drop": freq = FaultRequest.fromPartial({ dropP: Math.min(1, Math.max(0, b.p ?? 0)) }); break;
      case "clear": freq = FaultRequest.fromPartial({ clear: true }); break;
      default:
        void reply.code(400).send({
          error: { code: "BAD_FAULT", message: `kind must be pause|slow|drop|clear, got ${b.kind}` },
        });
        return;
    }
    client.injectFault(freq, (err: grpc.ServiceError | null, resp?: FaultResponse) => {
      if (err || !resp) {
        const code = err?.code ?? grpc.status.UNKNOWN;
        void reply.code(grpcToHttp(code)).send({
          error: { code: grpc.status[code], message: err?.details ?? "unknown" },
        });
        return;
      }
      void reply.send({ nodeId: b.nodeId, active: resp.active });
    });
  });
}
