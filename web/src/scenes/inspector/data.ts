// Inspector data: fetch a shard's 2D projection (point-cloud positions) and a
// trace's decoded blobs (per-hop traversal), then bake typed-array render
// buffers. Kept out of React so the heavy arrays never become per-record JS
// objects (frontend.md §5.2 perf discipline).

export interface BlobJson {
  shardId: number;
  node: number[];
  parent: number[];
  dist: number[];
  tOffUs: number[];
  meta: number[]; // (layer & 0xF) << 3 | kind
  dropped: number;
}

export type TraceKind = 0 | 1 | 2 | 3 | 4; // ENTRY VISIT ACCEPT PRUNE RESULT

export interface Hop {
  node: number;
  parent: number;
  layer: number;
  kind: TraceKind;
  dist: number;
  tOffUs: number;
}

export interface ShardInspectorData {
  shardId: number;
  nodeId: string;
  /** projection xy for every row, [x0,y0,x1,y1,...] in [-1,1] */
  projection: Float32Array;
  pointCount: number;
  hops: Hop[];
  maxLayer: number;
  durationUs: number;
  visited: number;
  dropped: number;
}

export function kindOf(meta: number): TraceKind {
  return (meta & 0x7) as TraceKind;
}
export function layerOf(meta: number): number {
  return (meta >> 3) & 0xf;
}

// World layout: projection xy in [-1,1] → [-SCALE, SCALE] on the horizontal
// plane; each HNSW layer is stacked GAP units up the Y axis (layer 0 at the
// bottom, the full point cloud lives there).
export const SCALE = 6;
export const GAP = 2.6;

// Kind → RGB (dim structure, luminous traversal — frontend.md §5).
const KIND_COLOR: Record<number, [number, number, number]> = {
  0: [1.0, 0.82, 0.35], // ENTRY  — gold
  1: [0.35, 0.5, 0.7], //  VISIT  — dim steel
  2: [0.6, 0.91, 1.0], //  ACCEPT — cyan
  3: [0.4, 0.4, 0.45], //  PRUNE  — grey (rare)
  4: [1.0, 1.0, 1.0], //   RESULT — white
};

export interface RenderBuffers {
  cloud: Float32Array; //     pointCount × 3, layer-0 plane
  hopPos: Float32Array; //    hops × 3
  hopColor: Float32Array; //  hops × 3
  hopTimeUs: Float32Array; // hops (ascending)
  edgePos: Float32Array; //   hops × 2 × 3 (parent, node)
  hopCount: number;
}

export function buildBuffers(d: ShardInspectorData): RenderBuffers {
  const n = d.pointCount;
  const cloud = new Float32Array(n * 3);
  for (let r = 0; r < n; r++) {
    cloud[r * 3] = d.projection[r * 2]! * SCALE;
    cloud[r * 3 + 1] = 0;
    cloud[r * 3 + 2] = d.projection[r * 2 + 1]! * SCALE;
  }

  const h = d.hops.length;
  const hopPos = new Float32Array(h * 3);
  const hopColor = new Float32Array(h * 3);
  const hopTimeUs = new Float32Array(h);
  const edgePos = new Float32Array(h * 6);

  const px = (row: number) => (row < n ? d.projection[row * 2]! * SCALE : 0);
  const py = (row: number) => (row < n ? d.projection[row * 2 + 1]! * SCALE : 0);

  for (let i = 0; i < h; i++) {
    const hop = d.hops[i]!;
    const y = hop.layer * GAP;
    hopPos[i * 3] = px(hop.node);
    hopPos[i * 3 + 1] = y;
    hopPos[i * 3 + 2] = py(hop.node);
    const c = KIND_COLOR[hop.kind] ?? KIND_COLOR[1]!;
    hopColor[i * 3] = c[0];
    hopColor[i * 3 + 1] = c[1];
    hopColor[i * 3 + 2] = c[2];
    hopTimeUs[i] = hop.tOffUs;
    // edge parent → node, both at this hop's layer height (a within-layer hop).
    // ENTRY has parent == node → degenerate (invisible) segment.
    edgePos[i * 6] = px(hop.parent);
    edgePos[i * 6 + 1] = y;
    edgePos[i * 6 + 2] = py(hop.parent);
    edgePos[i * 6 + 3] = px(hop.node);
    edgePos[i * 6 + 4] = y;
    edgePos[i * 6 + 5] = py(hop.node);
  }
  return { cloud, hopPos, hopColor, hopTimeUs, edgePos, hopCount: h };
}

// Records are in emission (== time) order, so a linear cursor is a prefix.
export function visibleCount(hopTimeUs: Float32Array, cursorUs: number): number {
  // small arrays (<= 64k); linear-from-last would need state, binary search is fine
  let lo = 0;
  let hi = hopTimeUs.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (hopTimeUs[mid]! <= cursorUs) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

async function fetchProjection(url: string, shardId: number): Promise<Float32Array> {
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`projection ${shardId}: HTTP ${resp.status}`);
  return new Float32Array(await resp.arrayBuffer());
}

function bakeHops(blob: BlobJson): { hops: Hop[]; maxLayer: number; durationUs: number } {
  const hops: Hop[] = new Array(blob.node.length);
  let maxLayer = 0;
  let durationUs = 0;
  for (let i = 0; i < blob.node.length; i++) {
    const layer = layerOf(blob.meta[i]!);
    if (layer > maxLayer) maxLayer = layer;
    if (blob.tOffUs[i]! > durationUs) durationUs = blob.tOffUs[i]!;
    hops[i] = {
      node: blob.node[i]!,
      parent: blob.parent[i]!,
      layer,
      kind: kindOf(blob.meta[i]!),
      dist: blob.dist[i]!,
      tOffUs: blob.tOffUs[i]!,
    };
  }
  return { hops, maxLayer, durationUs };
}

/**
 * Loads every shard's inspector data for a trace. Live mode hits the gateway;
 * replay mode (`base` set, e.g. "bundle") reads the same shapes from the static
 * recorded bundle — `traces_json/{hex}.json` and `projections/shard-{id}.f32` —
 * so the 3D inspector works with no backend (M6-T3).
 */
export async function loadTraceInspector(
  traceIdHex: string,
  base?: string,
): Promise<ShardInspectorData[]> {
  const blobsUrl = base
    ? `${base}/traces_json/${traceIdHex}.json`
    : `/api/trace/${traceIdHex}/blobs`;
  const projUrl = (shardId: number) =>
    base ? `${base}/projections/shard-${shardId}.f32` : `/api/projection/${shardId}`;

  const resp = await fetch(blobsUrl);
  if (!resp.ok) throw new Error(`trace blobs: HTTP ${resp.status}`);
  const body = (await resp.json()) as { blobs: Record<string, BlobJson> };

  const entries = Object.entries(body.blobs);
  const out = await Promise.all(
    entries.map(async ([nodeId, blob]) => {
      const projection = await fetchProjection(projUrl(blob.shardId), blob.shardId).catch(
        () => new Float32Array(0),
      );
      const { hops, maxLayer, durationUs } = bakeHops(blob);
      return {
        shardId: blob.shardId,
        nodeId,
        projection,
        pointCount: projection.length / 2,
        hops,
        maxLayer,
        durationUs,
        visited: hops.filter((h) => h.kind === 1).length,
        dropped: blob.dropped,
      } satisfies ShardInspectorData;
    }),
  );
  out.sort((a, b) => a.shardId - b.shardId);
  return out;
}
