// Boot-time source selection (M6-T2). Live if a gateway answers /api/ready;
// otherwise the static replay bundle (GitHub Pages demo, M6-T3); otherwise a
// disconnected LiveSource so the UI still renders its connection-error state.

import { LiveSource, Source } from "./live";
import { ReplaySource } from "./replay";

export async function pickSource(): Promise<Source> {
  try {
    const r = await fetch("/api/ready", { signal: AbortSignal.timeout(1500) });
    // Must be the actual ready endpoint, not a static host's SPA/404 fallback
    // (which happily returns 200 + index.html for any path). Confirm the JSON
    // shape before trusting it as a live gateway.
    if (r.ok && r.headers.get("content-type")?.includes("json")) {
      const body = (await r.json()) as { ready?: unknown };
      if (typeof body.ready === "boolean") return new LiveSource();
    }
  } catch {
    /* no live gateway — fall through to replay */
  }
  return (await ReplaySource.load()) ?? new LiveSource();
}
