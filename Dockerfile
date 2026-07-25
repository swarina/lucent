# syntax=docker/dockerfile:1
# Lucent — all-in-one image (M6-T4).
#
# Built + published by .github/workflows/docker.yml (GitHub Actions has a Docker
# daemon; the dev env doesn't). The C++ (vcpkg) stage uses a BuildKit cache mount
# so grpc/protobuf aren't rebuilt from scratch on every CI iteration.
#
# Why one image, not one-container-per-service: Lucent's whole model is a
# supervisor (`lucent dev`) that spawns N processes on one host and orchestrates
# failover / chaos / node add-remove *in-process*. Splitting shards into separate
# containers would bypass the supervisor and break those features. So the faithful
# packaging is a single image that runs the supervisor; Debian bookworm is used
# across build and runtime stages so the C++ binaries' glibc matches at runtime.

# ---------------------------------------------------------------------------
# Stage 1 — C++ engine (shard / coordinator / raft member) via vcpkg + CMake.
# This is the slow stage: vcpkg builds grpc/protobuf/abseil from source (tens of
# minutes). Kept first so its layer caches independently of the JS/Python stages.
# ---------------------------------------------------------------------------
FROM debian:bookworm AS cpp-build
ARG VCPKG_BASELINE=f87344cac03158cbf1467264565f1fd36b382a24
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential cmake ninja-build git curl zip unzip tar pkg-config \
      autoconf automake libtool ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Pin vcpkg to the manifest's builtin-baseline (must match vcpkg.json).
RUN git clone https://github.com/microsoft/vcpkg.git /opt/vcpkg \
 && git -C /opt/vcpkg checkout "${VCPKG_BASELINE}" \
 && /opt/vcpkg/bootstrap-vcpkg.sh -disableMetrics
ENV VCPKG_ROOT=/opt/vcpkg VCPKG_FORCE_SYSTEM_BINARIES=1

WORKDIR /src
COPY . /src
# The CMake preset references ${sourceDir}/vcpkg; point it at the cloned copy.
RUN ln -s /opt/vcpkg /src/vcpkg
# Release build → static-linked binaries on the default linux triplet. The
# BuildKit cache mount persists vcpkg's built binary packages across CI runs so
# grpc/protobuf/abseil aren't recompiled every iteration.
ENV VCPKG_DEFAULT_BINARY_CACHE=/vcpkg-cache
RUN --mount=type=cache,target=/vcpkg-cache \
    mkdir -p /vcpkg-cache && \
    cmake --preset release && cmake --build --preset release

# ---------------------------------------------------------------------------
# Stage 2 — Node gateway (REST/WS + collector). Needs generated TS proto stubs.
# ---------------------------------------------------------------------------
FROM node:20-bookworm AS gateway-build
WORKDIR /src
COPY gateway/package*.json gateway/
RUN cd gateway && npm install
COPY proto/ proto/
COPY tools/ tools/
COPY gateway/ gateway/
RUN bash tools/gen-proto.sh ts && cd gateway && npm run build

# ---------------------------------------------------------------------------
# Stage 3 — Web UI (Vite). Copies web/public/bundle → dist/bundle for the demo.
# ---------------------------------------------------------------------------
FROM node:20-bookworm AS web-build
WORKDIR /src/web
COPY web/package*.json ./
RUN npm install
COPY web/ ./
RUN npm run build

# ---------------------------------------------------------------------------
# Stage 4 — runtime. Node (for the gateway) + Python/uv (embed + supervisor +
# ingest) + the C++ binaries on PATH (dev.py's find_binary has a PATH fallback).
# ---------------------------------------------------------------------------
FROM node:20-bookworm-slim AS runtime
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 python3-venv ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Python project + locked deps. `ingest` (grpcio + numpy) is always required —
# the embed service and ingest can't run without it. EMBED_EXTRA="--extra embed"
# additionally pulls the real MiniLM stack (torch — large); empty keeps the image
# small and runs the deterministic --fake-embed encoder (no model download).
COPY py/ py/
ARG EMBED_EXTRA=""
RUN cd py && uv sync --frozen --extra ingest ${EMBED_EXTRA}

# C++ binaries on PATH (find_binary: build/dev → build/release → which()).
COPY --from=cpp-build /src/build/release/cpp/shard/lucent-shard        /usr/local/bin/
COPY --from=cpp-build /src/build/release/cpp/coordinator/lucent-coord  /usr/local/bin/
COPY --from=cpp-build /src/build/release/cpp/raft/lucent-member        /usr/local/bin/

# Gateway (dist + its runtime node_modules) and web assets at the repo-relative
# paths the supervisor expects (repo_root = the config file's parent = /app).
COPY --from=gateway-build /src/gateway/dist/         gateway/dist/
COPY --from=gateway-build /src/gateway/node_modules/ gateway/node_modules/
COPY --from=web-build     /src/web/dist/             web/dist/

# Config, corpus, and the boot script.
COPY cluster.yaml ./
COPY testdata/ testdata/
COPY deploy/docker-entrypoint.sh /usr/local/bin/lucent-entrypoint
RUN chmod +x /usr/local/bin/lucent-entrypoint

# Tunables (overridable in docker-compose or `docker run -e`). The fake/real
# default tracks the build: a real-model image (EMBED_EXTRA set) defaults to the
# real encoder, a slim image to --fake-embed. DEFAULT_FAKE_EMBED must be passed
# accordingly by whoever builds (docker.yml sets it to 0 for the published image).
ARG DEFAULT_FAKE_EMBED=1
ENV LUCENT_SHARDS=2 \
    LUCENT_REPLICAS=1 \
    LUCENT_FAKE_EMBED=${DEFAULT_FAKE_EMBED} \
    LUCENT_INGEST_N=2000 \
    LUCENT_CORPUS=testdata/corpus-2k.jsonl
EXPOSE 8080
ENTRYPOINT ["lucent-entrypoint"]
