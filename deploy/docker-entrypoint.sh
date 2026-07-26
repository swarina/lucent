#!/usr/bin/env bash
# Boot the cluster inside the container (M6-T4). Mirrors the README quickstart:
# start the supervisor, wait for the gateway to report ready, ingest the corpus
# once (into the running cluster — ingest is online), then hand the foreground
# back to the supervisor so container lifecycle == cluster lifecycle.
set -euo pipefail
cd /app

SHARDS="${LUCENT_SHARDS:-2}"
REPLICAS="${LUCENT_REPLICAS:-1}"
N="${LUCENT_INGEST_N:-2000}"
CORPUS="${LUCENT_CORPUS:-testdata/corpus-2k.jsonl}"
FAKE="${LUCENT_FAKE_EMBED:-1}"
MARKER="/app/data/.docker-ingested"   # persists in the data volume

dev_args=(--shards "$SHARDS" --replicas "$REPLICAS" --config cluster.yaml)
[ "$FAKE" = "1" ] && dev_args+=(--fake-embed)

echo "[entrypoint] starting supervisor: lucent dev ${dev_args[*]}"
uv --project py run lucent dev "${dev_args[@]}" &
dev_pid=$!

# Stop the supervisor cleanly if the container is asked to stop.
trap 'echo "[entrypoint] stopping"; kill -TERM "$dev_pid" 2>/dev/null || true; wait "$dev_pid" 2>/dev/null || true' TERM INT

echo "[entrypoint] waiting for gateway :8080 to become ready ..."
ready=0
for _ in $(seq 1 180); do
  if curl -sf http://127.0.0.1:8080/api/ready >/dev/null 2>&1; then ready=1; break; fi
  if ! kill -0 "$dev_pid" 2>/dev/null; then
    echo "[entrypoint] supervisor exited before becoming ready" >&2
    wait "$dev_pid"; exit 1
  fi
  sleep 1
done
[ "$ready" = 1 ] || { echo "[entrypoint] gateway never became ready" >&2; kill "$dev_pid" 2>/dev/null || true; exit 1; }

# Ingest once. The supervisor wrote the effective config to .lucent/cluster-dev.yaml.
if [ -n "$CORPUS" ] && [ ! -f "$MARKER" ]; then
  echo "[entrypoint] ingesting $N docs from $CORPUS ..."
  uv --project py run lucent ingest \
    --config .lucent/cluster-dev.yaml --corpus "$CORPUS" --n "$N"
  mkdir -p /app/data && touch "$MARKER"
  echo "[entrypoint] ingest complete — open http://localhost:8080"
else
  echo "[entrypoint] skipping ingest (already done, or LUCENT_CORPUS empty)"
fi

wait "$dev_pid"
