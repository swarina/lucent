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

## Deploying a live public instance (Level C)

Goal: a 24/7 public URL where anyone can type a query and watch it think, on the
real MiniLM model. The image is **built + published by CI** (`.github/workflows/docker.yml`,
GitHub's arm64 runner → `ghcr.io/swarina/lucent:latest`), so the host just pulls
and runs it — no build on the box.

**Why Oracle Cloud Always-Free ARM:** the real model pulls in PyTorch (~2 GB
RAM), which rules out the tiny free tiers (Render/Fly free = 256 MB–1 GB). Oracle's
Always-Free **Ampere A1** gives up to 4 OCPU / 24 GB RAM at no cost, and it's
arm64 — matching the published image. (A home machine + Cloudflare Tunnel is a
fine free alternative if you'd rather not use a cloud VM.)

### One-time setup

1. **Create the VM.** Oracle Cloud → Compute → Instances → Create. Shape
   **VM.Standard.A1.Flex**, e.g. 2 OCPU / 12 GB, image **Ubuntu 24.04 (aarch64)**.
   Save the SSH key. (Signup needs a card for identity check; Always-Free isn't billed.)
2. **Open the port.** VCN → the instance's subnet → Security List → add an
   ingress rule: source `0.0.0.0/0`, TCP, dest port **80**. Then on the box:
   `sudo iptables -I INPUT 6 -p tcp --dport 80 -j ACCEPT` (Oracle images ship a
   restrictive iptables) and persist with `sudo netfilter-persistent save`.
3. **Install Docker:** `curl -fsSL https://get.docker.com | sudo sh && sudo usermod -aG docker $USER` (re-login).
4. **Make the image pullable.** Either make the GHCR package public
   (github.com → your profile → Packages → `lucent` → Package settings → Change
   visibility → Public), or `docker login ghcr.io -u <you>` with a PAT that has
   `read:packages`.

### Run it

```sh
docker run -d --name lucent --restart unless-stopped \
  -p 80:8080 \
  -e LUCENT_FAKE_EMBED=0 \
  -e LUCENT_SHARDS=2 \
  -v lucent-data:/app/data \
  ghcr.io/swarina/lucent:latest
```

First boot downloads MiniLM (~90 MB) and ingests the bundled corpus (a couple of
minutes on CPU); the index is cached in the `lucent-data` volume for restarts.
Watch it come up with `docker logs -f lucent`, then open `http://<vm-public-ip>/`.

### HTTPS + a real hostname (optional)

Put **Caddy** in front for automatic TLS (needs a domain pointed at the VM):

```sh
# /etc/caddy/Caddyfile
lucent.example.com {
    reverse_proxy localhost:8080
}
```

Or expose it without a domain via a free **Cloudflare Tunnel**
(`cloudflared tunnel --url http://localhost:8080`).

### Guardrails (public exposure)

The cluster exposes chaos endpoints (`/api/chaos/kill`, `/api/fault`, `/api/loadgen`)
that anyone hitting the URL could trigger — fine for a demo, but if you want to
lock them down, front the app with Caddy and allow only `GET /`, `/assets/*`,
`/api/query`, `/api/cluster`, `/api/ready`, `/ws/live`, and the `/api/trace|projection`
reads; block the mutating `POST`s. Consider a basic rate limit too.

