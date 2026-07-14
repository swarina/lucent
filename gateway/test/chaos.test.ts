// Chaos surface routes (M3-T4). /api/chaos/{kill,restart} proxy the supervisor
// control API over HTTP; we stand up a fake supervisor and assert the gateway
// forwards nodeId, maps the supervisor's {error} into a 404, and reports 503
// when the supervisor is unreachable. The /api/fault gRPC path mirrors the
// existing trace-blob shard-client pattern and is exercised live.

import http from "node:http";
import type { AddressInfo } from "node:net";

import * as grpc from "@grpc/grpc-js";
import Fastify, { type FastifyInstance } from "fastify";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { EventStore } from "../src/collector.js";
import type { GatewayConfig } from "../src/config.js";
import {
  AddReplicaResponse,
  CoordinatorServiceService,
  RemoveReplicaResponse,
} from "../src/gen/lucent/v1/coordinator.js";
import { registerRoutes } from "../src/rest.js";

// A stand-in supervisor: records the last request and echoes a canned reply.
function fakeSupervisor(reply: (path: string, body: unknown) => { code: number; json: unknown }) {
  const seen: { path: string; body: unknown }[] = [];
  const server = http.createServer((req, res) => {
    let raw = "";
    req.on("data", (c) => (raw += c));
    req.on("end", () => {
      const body = raw ? JSON.parse(raw) : {};
      seen.push({ path: req.url ?? "", body });
      const { code, json } = reply(req.url ?? "", body);
      res.writeHead(code, { "content-type": "application/json" });
      res.end(JSON.stringify(json));
    });
  });
  return { server, seen };
}

function makeConfig(supervisorCtl: number): GatewayConfig {
  return {
    shards: 2,
    replicas: 2,
    partitioning: "hash",
    dataDir: "/tmp/lucent-test",
    ports: {
      coordinator: 1, embed: 2, collector: 3, gatewayHttp: 4,
      supervisorCtl, shardBase: 7100,
    },
  };
}

describe("chaos routes", () => {
  let app: FastifyInstance;
  let sup: ReturnType<typeof fakeSupervisor>;
  let port: number;

  const boot = async (reply: (p: string, b: unknown) => { code: number; json: unknown }) => {
    sup = fakeSupervisor(reply);
    await new Promise<void>((r) => sup.server.listen(0, "127.0.0.1", r));
    port = (sup.server.address() as AddressInfo).port;
    app = Fastify({ logger: false });
    registerRoutes(app, makeConfig(port), new EventStore());
    await app.ready();
  };

  afterEach(async () => {
    await app?.close();
    // Some tests close the fake supervisor themselves; closing again just
    // resolves (the callback fires with a harmless "not running" error).
    await new Promise<void>((r) => sup.server.close(() => r()));
  });

  it("forwards kill to the supervisor and returns its reply", async () => {
    await boot(() => ({ code: 200, json: { nodeId: "shard-1a", signal: "SIGKILL", pid: 42 } }));
    const res = await app.inject({
      method: "POST", url: "/api/chaos/kill", payload: { nodeId: "shard-1a" },
    });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toMatchObject({ nodeId: "shard-1a", pid: 42 });
    expect(sup.seen).toEqual([{ path: "/kill", body: { nodeId: "shard-1a" } }]);
  });

  it("forwards restart to the supervisor", async () => {
    await boot(() => ({ code: 200, json: { nodeId: "shard-0a", pid: 99 } }));
    const res = await app.inject({
      method: "POST", url: "/api/chaos/restart", payload: { nodeId: "shard-0a" },
    });
    expect(res.statusCode).toBe(200);
    expect(sup.seen[0]).toEqual({ path: "/restart", body: { nodeId: "shard-0a" } });
  });

  it("maps the supervisor's {error} for an unknown node to a 404", async () => {
    await boot(() => ({ code: 200, json: { error: "unknown node zzz" } }));
    const res = await app.inject({
      method: "POST", url: "/api/chaos/kill", payload: { nodeId: "zzz" },
    });
    expect(res.statusCode).toBe(404);
    expect(res.json()).toMatchObject({ error: "unknown node zzz" });
  });

  it("rejects a kill with no nodeId before hitting the supervisor", async () => {
    await boot(() => ({ code: 200, json: {} }));
    const res = await app.inject({ method: "POST", url: "/api/chaos/kill", payload: {} });
    expect(res.statusCode).toBe(400);
    expect(sup.seen).toHaveLength(0);
  });

  it("returns 503 when the supervisor is unreachable", async () => {
    await boot(() => ({ code: 200, json: {} }));
    // Close the supervisor so the proxy fetch fails at the socket.
    await new Promise<void>((r) => sup.server.close(() => r()));
    const res = await app.inject({
      method: "POST", url: "/api/chaos/kill", payload: { nodeId: "shard-1a" },
    });
    expect(res.statusCode).toBe(503);
    expect(res.json().error.code).toBe("SUPERVISOR_DOWN");
  });

  it("rejects an unknown fault kind with 400", async () => {
    await boot(() => ({ code: 200, json: {} }));
    const res = await app.inject({
      method: "POST", url: "/api/fault", payload: { nodeId: "shard-0a", kind: "explode" },
    });
    expect(res.statusCode).toBe(400);
    expect(res.json().error.code).toBe("BAD_FAULT");
  });

  it("rejects a fault on a non-shard node with 400", async () => {
    await boot(() => ({ code: 200, json: {} }));
    const res = await app.inject({
      method: "POST", url: "/api/fault", payload: { nodeId: "coord-0", kind: "pause", ms: 100 },
    });
    expect(res.statusCode).toBe(400);
    expect(res.json().error.code).toBe("BAD_NODE");
  });
});

// Node add/remove (M3-T5) orchestrate the supervisor AND the coordinator, so
// these tests stand up a fake coordinator gRPC server alongside the fake
// supervisor and assert the gateway sequences them correctly.
describe("node add/remove routes", () => {
  let app: FastifyInstance;
  let sup: ReturnType<typeof fakeSupervisor>;
  let coord: grpc.Server;
  let coordCalls: { method: string; req: unknown }[];

  const boot = async (opts: {
    supReply: (p: string, b: unknown) => { code: number; json: unknown };
    addReplica?: (req: unknown) => AddReplicaResponse;
    removeReplica?: (req: unknown) => RemoveReplicaResponse;
    coordUp?: boolean;
  }) => {
    coordCalls = [];
    sup = fakeSupervisor(opts.supReply);
    await new Promise<void>((r) => sup.server.listen(0, "127.0.0.1", r));
    const supPort = (sup.server.address() as AddressInfo).port;

    coord = new grpc.Server();
    coord.addService(CoordinatorServiceService, {
      query: (_c: unknown, cb: grpc.sendUnaryData<never>) => cb(null, {} as never),
      getClusterState: (_c: unknown, cb: grpc.sendUnaryData<never>) => cb(null, {} as never),
      addReplica: (c: { request: unknown }, cb: grpc.sendUnaryData<AddReplicaResponse>) => {
        coordCalls.push({ method: "addReplica", req: c.request });
        cb(null, opts.addReplica?.(c.request) ?? AddReplicaResponse.fromPartial({ epoch: 5n }));
      },
      removeReplica: (c: { request: unknown }, cb: grpc.sendUnaryData<RemoveReplicaResponse>) => {
        coordCalls.push({ method: "removeReplica", req: c.request });
        cb(null, opts.removeReplica?.(c.request) ?? RemoveReplicaResponse.fromPartial({ epoch: 7n }));
      },
    });
    const coordPort = await new Promise<number>((resolve, reject) =>
      coord.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(),
        (err, p) => (err ? reject(err) : resolve(p))));
    if (opts.coordUp === false) coord.forceShutdown(); // simulate a down coordinator

    const config: GatewayConfig = {
      shards: 2, replicas: 2, partitioning: "hash", dataDir: "/tmp/lucent-test",
      ports: { coordinator: coordPort, embed: 2, collector: 3, gatewayHttp: 4,
        supervisorCtl: supPort, shardBase: 7100 },
    };
    app = Fastify({ logger: false });
    registerRoutes(app, config, new EventStore());
    await app.ready();
  };

  afterEach(async () => {
    await app?.close();
    await new Promise<void>((r) => sup.server.close(() => r()));
    coord?.forceShutdown();
  });

  it("spawns then registers the backup (supervisor → coordinator)", async () => {
    await boot({
      supReply: () => ({ code: 200, json: { nodeId: "shard-1b", addr: "127.0.0.1:7111", port: 7111 } }),
      addReplica: () => AddReplicaResponse.fromPartial({ epoch: 9n }),
    });
    const res = await app.inject({
      method: "POST", url: "/api/chaos/spawn", payload: { shardId: 1 },
    });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toMatchObject({ nodeId: "shard-1b", addr: "127.0.0.1:7111", epoch: 9 });
    // The coordinator was told about the exact node the supervisor spawned.
    expect(coordCalls).toHaveLength(1);
    expect(coordCalls[0]).toMatchObject({
      method: "addReplica", req: { shardId: 1, nodeId: "shard-1b", addr: "127.0.0.1:7111" },
    });
  });

  it("returns 400 and never calls the coordinator if the spawn fails", async () => {
    await boot({ supReply: () => ({ code: 200, json: { error: "primary not sealed" } }) });
    const res = await app.inject({
      method: "POST", url: "/api/chaos/spawn", payload: { shardId: 1 },
    });
    expect(res.statusCode).toBe(400);
    expect(res.json().error.code).toBe("SPAWN_FAILED");
    expect(coordCalls).toHaveLength(0);
  });

  it("surfaces a coordinator AddReplica error as 409", async () => {
    await boot({
      supReply: () => ({ code: 200, json: { nodeId: "shard-1b", addr: "127.0.0.1:7111" } }),
      addReplica: () => AddReplicaResponse.fromPartial({ error: "shard 1 already has a healthy backup" }),
    });
    const res = await app.inject({
      method: "POST", url: "/api/chaos/spawn", payload: { shardId: 1 },
    });
    expect(res.statusCode).toBe(409);
    expect(res.json().error.code).toBe("REGISTER_FAILED");
  });

  it("drains via the coordinator then stops the process", async () => {
    await boot({
      supReply: () => ({ code: 200, json: { nodeId: "shard-1b", signal: "SIGTERM", pid: 5 } }),
      removeReplica: () => RemoveReplicaResponse.fromPartial({ epoch: 11n }),
    });
    const res = await app.inject({
      method: "POST", url: "/api/chaos/drain", payload: { nodeId: "shard-1b" },
    });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toMatchObject({ nodeId: "shard-1b", epoch: 11, stopped: true });
    expect(coordCalls[0]).toMatchObject({ method: "removeReplica", req: { nodeId: "shard-1b" } });
    // SIGTERM (graceful), not SIGKILL.
    expect(sup.seen.at(-1)).toEqual({ path: "/kill", body: { nodeId: "shard-1b", signal: "SIGTERM" } });
  });

  it("refuses to drain a primary (coordinator error → 400) and never stops it", async () => {
    await boot({
      supReply: () => ({ code: 200, json: {} }),
      removeReplica: () => RemoveReplicaResponse.fromPartial({ error: "node shard-1a is the primary of shard 1" }),
    });
    const res = await app.inject({
      method: "POST", url: "/api/chaos/drain", payload: { nodeId: "shard-1a" },
    });
    expect(res.statusCode).toBe(400);
    expect(res.json().error.code).toBe("DRAIN_REFUSED");
    expect(sup.seen).toHaveLength(0); // process was never signalled
  });
});
