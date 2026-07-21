"""Lucent command-line entrypoint.

The command surface is stubbed here so the tree and `--help` are stable from
day one; each command's real implementation lands with its roadmap task. Stubs
exit non-zero so nothing mistakes an unimplemented command for success.
"""

from __future__ import annotations

import sys

import click

from . import __version__


def _todo(task: str, note: str = "") -> None:
    msg = f"'{click.get_current_context().command_path}' is not implemented yet ({task})."
    if note:
        msg += f" {note}"
    click.secho(msg, fg="yellow", err=True)
    click.echo("See docs/roadmap.md for status.", err=True)
    sys.exit(2)


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(__version__, prog_name="lucent")
def main() -> None:
    """Lucent — a distributed vector search engine you can watch think."""


@main.command()
@click.option("--shards", default=4, show_default=True, help="Number of shards.")
@click.option("--replicas", default=1, show_default=True,
              help="Replicas per shard (replication lands at M3; 'b' nodes idle EMPTY).")
@click.option(
    "--partitioning",
    type=click.Choice(["hash", "semantic"]),
    default="hash",
    show_default=True,
)
@click.option("--config", default="cluster.yaml", show_default=True)
@click.option("--fake-embed", is_flag=True,
              help="Use the deterministic no-model encoder (CI / fast dev).")
@click.option("--raft", is_flag=True,
              help="Run 3 mini-Raft voters that own the shard map (M5).")
def dev(shards: int, replicas: int, partitioning: str, config: str,
        fake_embed: bool, raft: bool) -> None:
    """Spin up a local multi-process cluster (supervisor, blocking)."""
    from lucent import dev as dev_mod

    sys.exit(dev_mod.run_dev(config, shards, replicas, partitioning,
                             fake_embed=fake_embed, raft=raft))


@main.command()
@click.option("--config", default="cluster.yaml", show_default=True)
@click.option("--corpus", default="arxiv", show_default=True,
              help="'arxiv' (fetch/cached dump) or a path to a .jsonl/.jsonl.gz")
@click.option("--n", default=50000, show_default=True, help="Documents to ingest.")
@click.option("--seed", default=None, type=int, help="Sampling seed (default: index.seed).")
@click.option("--projection", type=click.Choice(["pca", "umap", "none"]),
              default="pca", show_default=True,
              help="2D layout for the inspector point cloud (umap = nicer, slower).")
def ingest(config: str, corpus: str, n: int, seed: int | None, projection: str) -> None:
    """Ingest a corpus into a RUNNING cluster: sample -> embed -> load -> seal."""
    import logging

    logging.basicConfig(level=logging.INFO, format="[%(name)s] %(message)s")
    from lucent import config as config_mod
    from lucent import ingest as ingest_mod

    if corpus == "arxiv":
        cfg = config_mod.load(config)
        corpus = str(ingest_mod.fetch_corpus_file(cfg.paths.cache / "corpus"))
    ingest_mod.run(config, corpus, n=n, seed=seed, projection=projection)


@main.command()
@click.option("--config", default="cluster.yaml", show_default=True)
@click.option("--ef", default="16,32,64,100,200", show_default=True,
              help="Comma-separated efSearch sweep.")
@click.option("--probe", default="0", show_default=True,
              help="Comma-separated probe sweep (0 = all shards).")
@click.option("--queries", default=200, show_default=True,
              help="Held-out queries to run per config.")
@click.option("--out", default="bench/results/bench.json", show_default=True)
@click.option("--gate", is_flag=True, help="Exit non-zero if recall gates fail.")
@click.option("--merge", is_flag=True,
              help="Keep other partitionings' configs in the output (accumulate "
                   "hash + semantic series across runs for the money chart).")
@click.option("--overhead", is_flag=True,
              help="Also measure SPANS/FULL instrumentation overhead vs NONE.")
def bench(config: str, ef: str, probe: str, queries: int, out: str,
          gate: bool, merge: bool, overhead: bool) -> None:
    """Measure recall/latency vs the brute-force oracle (cluster must be up)."""
    import logging
    import pathlib

    logging.basicConfig(level=logging.INFO, format="[%(name)s] %(message)s")
    from lucent import bench as bench_mod
    from lucent import config as config_mod

    results = bench_mod.run_sweep(
        config,
        ef_list=[int(x) for x in ef.split(",")],
        probe_list=[int(x) for x in probe.split(",")],
        max_queries=queries,
        out_path=pathlib.Path(out),
        merge=merge,
    )
    if overhead:
        bench_mod.run_overhead(config)
    if gate:
        import json as _json

        cfg = config_mod.load(config)
        mf = cfg.paths.data / "ingest-manifest.json"
        model = _json.loads(mf.read_text()).get("model", "") if mf.exists() else ""
        failures = bench_mod.check_gates(results, cfg.cluster.shards, model)
        for f in failures:
            click.secho(f"GATE FAIL: {f}", fg="red", err=True)
        if failures:
            sys.exit(1)
        click.secho("all bench gates green", fg="green")


@main.command()
@click.option("--config", default="cluster.yaml", show_default=True)
@click.option("--out", default="bundle", show_default=True)
def record(config: str, out: str) -> None:
    """Record a live cluster session into a replay bundle (M6)."""
    try:
        from lucent import record as record_mod
    except ImportError as e:
        click.secho(f"missing dependency: {e}", fg="red", err=True)
        click.echo("install with: uv sync --extra dev", err=True)
        sys.exit(1)
    record_mod.run(config, out)


@main.command()
@click.option("--config", default="cluster.yaml", show_default=True)
@click.option("--fake", is_flag=True,
              help="Deterministic hash-vector encoder (no model, no torch) — CI/dev.")
def embedsvc(config: str, fake: bool) -> None:
    """Run the embedding service (gRPC, blocking). Needs the 'embed' extra."""
    try:
        from lucent import embedsvc as svc
    except ImportError as e:  # sentence-transformers / grpcio absent
        click.secho(f"missing dependency: {e}", fg="red", err=True)
        click.echo("install with: uv sync --extra embed", err=True)
        sys.exit(1)
    svc.serve(config, fake=fake)


@main.group()
def corpus() -> None:
    """Corpus utilities."""


@corpus.command("fetch")
@click.option("--config", default="cluster.yaml", show_default=True)
@click.option("--max-mb", default=None, type=int,
              help="Fetch only the first N MB (partial stream; for samples/CI).")
def corpus_fetch(config: str, max_mb: int | None) -> None:
    """Download the arXiv abstracts dump into the corpus cache (one-time)."""
    import logging

    logging.basicConfig(level=logging.INFO, format="[%(name)s] %(message)s")
    from lucent import config as config_mod
    from lucent import ingest as ingest_mod

    cfg = config_mod.load(config)
    path = ingest_mod.fetch_corpus_file(
        cfg.paths.cache / "corpus",
        max_bytes=max_mb * (1 << 20) if max_mb else None,
    )
    click.echo(f"corpus at {path}")


@main.group()
def node() -> None:
    """Cluster node operations."""


def _gateway_post(config: str, path: str, payload: dict) -> dict:
    """POST JSON to the running gateway (which orchestrates supervisor +
    coordinator). Returns the parsed reply; exits non-zero on transport error."""
    import json
    import urllib.error
    import urllib.request

    from lucent import config as config_mod

    cfg = config_mod.load(config)
    url = f"http://127.0.0.1:{cfg.ports.gateway_http}{path}"
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read() or b"{}")
        except json.JSONDecodeError:
            return {"error": {"code": str(e.code), "message": e.reason}}
    except OSError as e:
        click.secho(f"cannot reach gateway at {url}: {e}", fg="red", err=True)
        click.echo("is the cluster running? (lucent dev …)", err=True)
        sys.exit(1)


@node.command("add")
@click.option("--shard", required=True, type=int)
@click.option("--replica", type=click.Choice(["a", "b"]), default="b")
@click.option("--config", default="cluster.yaml", show_default=True)
def node_add(shard: int, replica: str, config: str) -> None:
    """Add a replacement backup to a shard: spawn the process (it loads the
    primary's sealed index) and register it with the coordinator."""
    res = _gateway_post(config, "/api/chaos/spawn",
                        {"shardId": shard, "replica": replica})
    if res.get("error"):
        err = res["error"]
        click.secho(f"add failed: {err.get('message', err) if isinstance(err, dict) else err}",
                    fg="red", err=True)
        sys.exit(1)
    click.secho(f"added {res.get('nodeId')} as backup of shard {shard} "
                f"(epoch {res.get('epoch')})", fg="green")


@node.command("remove")
@click.option("--node", "node_id", required=True, help="e.g. shard-1b (a backup).")
@click.option("--config", default="cluster.yaml", show_default=True)
def node_remove(node_id: str, config: str) -> None:
    """Drain a backup from the map, then stop its process (graceful SIGTERM)."""
    res = _gateway_post(config, "/api/chaos/drain", {"nodeId": node_id})
    if res.get("error"):
        err = res["error"]
        click.secho(f"remove failed: {err.get('message', err) if isinstance(err, dict) else err}",
                    fg="red", err=True)
        sys.exit(1)
    click.secho(f"drained {node_id} (epoch {res.get('epoch')}) and stopped it", fg="green")


if __name__ == "__main__":
    main()
