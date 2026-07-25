# Docker packaging

> **⚠️ Status: authored, not yet built.** These files were written without a
> Docker daemon available, so they have **not** been built or run. The C++
> (vcpkg) stage especially is likely to need a build-iterate pass. Treat this as
> a reviewed first draft — run `docker compose build` and fix from the errors.

## What it is

A single all-in-one image that runs `lucent dev` (the supervisor) inside the
container. Lucent's failover / chaos / node add-remove are orchestrated by the
supervisor **in-process**, so splitting shards into separate containers would
break them — the faithful packaging is one image running the supervisor, which
spawns the embed service, gateway, coordinator, and shard processes internally.

Build and runtime stages both use Debian **bookworm** so the C++ binaries' glibc
matches at runtime.

## Usage

One-command demo (builds a 2k-doc index on first boot, then serves):

```sh
docker compose --profile demo up --build
```

Then open **http://localhost:8080**. The index is cached in a named volume, so
later `up`s skip ingest.

Base cluster (no auto-ingest — bring your own corpus):

```sh
docker compose up --build
# then, in another terminal:
docker compose exec lucent uv --project py run lucent ingest \
  --config .lucent/cluster-dev.yaml --corpus testdata/corpus-2k.jsonl --n 2000
```

### Real semantic search (instead of the fake encoder)

The default image uses the deterministic `--fake-embed` encoder (no model
download, small image, instant — but ranking isn't semantic; the UI says so).
For real MiniLM: uncomment `EMBED_EXTRA: "--extra embed"` under the demo
service's `build.args` in `docker-compose.yml`, set `LUCENT_FAKE_EMBED: "0"`,
and rebuild. Note this pulls the torch stack (large image, ~model download).

## Tunables (env)

| Var | Default | Meaning |
|-----|---------|---------|
| `LUCENT_SHARDS` | `2` | shard count |
| `LUCENT_REPLICAS` | `1` | replicas per shard (`2` → backups + failover) |
| `LUCENT_FAKE_EMBED` | `1` | `1` = fake encoder, `0` = real MiniLM (needs `EMBED_EXTRA`) |
| `LUCENT_INGEST_N` | `2000` | docs to ingest on first boot |
| `LUCENT_CORPUS` | corpus path | empty string disables auto-ingest |

## Known risks to check on the first real build

1. **vcpkg C++ stage is long** (grpc/protobuf/abseil from source, tens of
   minutes) and may need extra apt packages for a given vcpkg baseline.
2. **Static vs dynamic linking.** The runtime image copies only the binaries,
   assuming the default linux triplet links statically. If a binary fails at
   runtime with a missing `.so`, either force a static triplet in the CMake
   configure or also `COPY` the needed libs from the `cpp-build` stage.
3. **`VCPKG_BASELINE`** (Dockerfile ARG) must equal `builtin-baseline` in
   `vcpkg.json` (`f87344c…`). Keep them in sync.
4. **Gateway proto-gen** runs `tools/gen-proto.sh ts` using the gateway's
   `node_modules` binaries — the copy order in stage 2 matters.
5. **Architecture.** Build on the arch you'll run (or use `docker buildx` with
   `--platform`); the C++ binaries are native.
6. **First-boot ingest timing.** The entrypoint waits up to 180 s for the
   gateway; a very large `LUCENT_INGEST_N` may need a longer window.
