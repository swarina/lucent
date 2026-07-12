// The HNSW traversal scene (frontend.md §5.2): a horizontal point cloud per
// layer stacked up the Y axis, with the query's traversal replayed from real
// microsecond offsets. One THREE.Points draw call for the whole cloud (50k is
// trivial); traversal points + edges are revealed by a time cursor via
// setDrawRange, so animation costs no per-frame allocation.

import { useFrame, useThree } from "@react-three/fiber";
import { useEffect, useMemo } from "react";
import * as THREE from "three";

import { buildBuffers, GAP, RenderBuffers, ShardInspectorData, visibleCount } from "./data";

export function InspectorScene({
  data,
  cursorUsRef,
}: {
  data: ShardInspectorData;
  cursorUsRef: React.MutableRefObject<number>;
}) {
  const bufs = useMemo(() => buildBuffers(data), [data]);
  const { camera } = useThree();

  // Frame the stack once per shard.
  useEffect(() => {
    camera.position.set(9, 7.5, 12);
    camera.lookAt(0, (data.maxLayer * GAP) / 2, 0);
  }, [camera, data.maxLayer]);

  return (
    <>
      <color attach="background" args={["#0b0e14"]} />
      <ambientLight intensity={0.6} />
      <LayerPlanes count={data.maxLayer + 1} />
      <PointCloud positions={bufs.cloud} />
      <TraversalEdges bufs={bufs} cursorUsRef={cursorUsRef} />
      <TraversalPoints bufs={bufs} cursorUsRef={cursorUsRef} />
    </>
  );
}

function LayerPlanes({ count }: { count: number }) {
  const planes = [];
  for (let l = 0; l < count; l++) {
    planes.push(
      <mesh key={l} rotation={[-Math.PI / 2, 0, 0]} position={[0, l * GAP, 0]}>
        <planeGeometry args={[15, 15]} />
        <meshBasicMaterial
          color="#4a5c80"
          transparent
          opacity={l === 0 ? 0.05 : 0.035}
          side={THREE.DoubleSide}
          depthWrite={false}
        />
      </mesh>,
    );
  }
  return <>{planes}</>;
}

function PointCloud({ positions }: { positions: Float32Array }) {
  const geom = useMemo(() => {
    const g = new THREE.BufferGeometry();
    g.setAttribute("position", new THREE.BufferAttribute(positions, 3));
    return g;
  }, [positions]);
  return (
    <points geometry={geom}>
      <pointsMaterial
        size={0.045}
        color="#2b3648"
        sizeAttenuation
        transparent
        opacity={0.85}
        depthWrite={false}
      />
    </points>
  );
}

function TraversalPoints({
  bufs,
  cursorUsRef,
}: {
  bufs: RenderBuffers;
  cursorUsRef: React.MutableRefObject<number>;
}) {
  const geom = useMemo(() => {
    const g = new THREE.BufferGeometry();
    g.setAttribute("position", new THREE.BufferAttribute(bufs.hopPos, 3));
    g.setAttribute("color", new THREE.BufferAttribute(bufs.hopColor, 3));
    g.setDrawRange(0, 0);
    return g;
  }, [bufs]);

  useFrame(() => {
    const n = visibleCount(bufs.hopTimeUs, cursorUsRef.current);
    geom.setDrawRange(0, n);
  });

  return (
    <points geometry={geom}>
      <pointsMaterial
        size={0.16}
        vertexColors
        sizeAttenuation
        transparent
        opacity={0.95}
        depthWrite={false}
        blending={THREE.AdditiveBlending}
      />
    </points>
  );
}

function TraversalEdges({
  bufs,
  cursorUsRef,
}: {
  bufs: RenderBuffers;
  cursorUsRef: React.MutableRefObject<number>;
}) {
  const geom = useMemo(() => {
    const g = new THREE.BufferGeometry();
    g.setAttribute("position", new THREE.BufferAttribute(bufs.edgePos, 3));
    g.setDrawRange(0, 0);
    return g;
  }, [bufs]);

  useFrame(() => {
    const n = visibleCount(bufs.hopTimeUs, cursorUsRef.current);
    geom.setDrawRange(0, n * 2); // 2 vertices per hop edge
  });

  return (
    <lineSegments geometry={geom}>
      <lineBasicMaterial
        color="#3d5170"
        transparent
        opacity={0.35}
        depthWrite={false}
      />
    </lineSegments>
  );
}
