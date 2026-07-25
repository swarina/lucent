// Lucent collector + gateway: gRPC event ingest from all nodes, REST + WS for
// the browser, static hosting of the web build.
//   node dist/index.js --config ../cluster.yaml [--web-dist ../web/dist]

import { existsSync } from "node:fs";
import path from "node:path";

import fastifyStatic from "@fastify/static";
import Fastify from "fastify";

import { EventStore, startCollector } from "./collector.js";
import { loadConfig } from "./config.js";
import { attachLiveWs } from "./livews.js";
import { registerRoutes } from "./rest.js";

function argValue(flag: string, fallback: string): string {
  const i = process.argv.indexOf(flag);
  return i >= 0 && i + 1 < process.argv.length ? process.argv[i + 1]! : fallback;
}

async function main(): Promise<void> {
  const configPath = argValue("--config", "cluster.yaml");
  const webDist = path.resolve(argValue("--web-dist", path.join(process.cwd(), "..", "web", "dist")));
  const config = loadConfig(configPath);

  const store = new EventStore();
  await startCollector(store, `127.0.0.1:${config.ports.collector}`);
  console.log(`collector-0: gRPC ingest on 127.0.0.1:${config.ports.collector}`);

  const app = Fastify({ logger: false });
  registerRoutes(app, config, store);
  if (existsSync(webDist)) {
    await app.register(fastifyStatic, { root: webDist });
  } else {
    console.warn(`web dist not found at ${webDist} — API only`);
  }

  // Bind localhost by default (safe for `lucent dev`); containers/hosts that need
  // the gateway reachable from outside set LUCENT_HTTP_HOST=0.0.0.0. Only the
  // public HTTP listener changes — inter-service gRPC stays on 127.0.0.1.
  const httpHost = process.env.LUCENT_HTTP_HOST ?? "127.0.0.1";
  await app.listen({ port: config.ports.gatewayHttp, host: httpHost });
  attachLiveWs(app.server, store);
  console.log(
    `collector-0: http://${httpHost}:${config.ports.gatewayHttp} (REST + /ws/live + static)`,
  );

  const shutdown = (): void => {
    void app.close().then(() => process.exit(0));
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}

main().catch((err) => {
  console.error("gateway failed:", err);
  process.exit(1);
});
