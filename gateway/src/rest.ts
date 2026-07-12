// REST API (protocol.md §3): browser-facing JSON over the coordinator's gRPC.
// Errors: HTTP status + {"error":{"code","message"}}.

import * as grpc from "@grpc/grpc-js";
import type { FastifyInstance } from "fastify";

import type { EventStore } from "./collector.js";
import type { GatewayConfig } from "./config.js";
import {
  ClusterState,
  ClusterStateRequest,
  CoordinatorServiceClient,
  QueryRequest,
  QueryResponse,
} from "./gen/lucent/v1/coordinator.js";
import { TraceLevel } from "./gen/lucent/v1/common.js";
import { Event, TraceBlob } from "./gen/lucent/v1/events.js";
import { ShardServiceClient, TraceBlobRequest } from "./gen/lucent/v1/shard.js";

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

  app.get("/api/ready", (_req, reply) => {
    coord.getClusterState(ClusterStateRequest.create(), (err) => {
      void reply.send({
        ready: !err,
        collector: { events: store.size, dropsDetected: store.droppedDetected },
      });
    });
  });

  // Chaos/fault/loadgen/bench land with M0-T11 (supervisor), M2 and M3.
  for (const path of ["/api/chaos/kill", "/api/chaos/restart", "/api/chaos/spawn", "/api/fault", "/api/loadgen"]) {
    app.post(path, (_req, reply) => {
      void reply.code(501).send({
        error: { code: "NOT_IMPLEMENTED", message: `${path} lands with its roadmap task` },
      });
    });
  }
}
