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
def dev(shards: int, replicas: int, partitioning: str, config: str,
        fake_embed: bool) -> None:
    """Spin up a local multi-process cluster (supervisor, blocking)."""
    from lucent import dev as dev_mod

    sys.exit(dev_mod.run_dev(config, shards, replicas, partitioning,
                             fake_embed=fake_embed))


@main.command()
@click.option("--config", default="cluster.yaml", show_default=True)
@click.option("--corpus", default="arxiv", show_default=True,
              help="'arxiv' (fetch/cached dump) or a path to a .jsonl/.jsonl.gz")
@click.option("--n", default=50000, show_default=True, help="Documents to ingest.")
@click.option("--seed", default=None, type=int, help="Sampling seed (default: index.seed).")
def ingest(config: str, corpus: str, n: int, seed: int | None) -> None:
    """Ingest a corpus into a RUNNING cluster: sample -> embed -> load -> seal."""
    import logging

    logging.basicConfig(level=logging.INFO, format="[%(name)s] %(message)s")
    from lucent import config as config_mod
    from lucent import ingest as ingest_mod

    if corpus == "arxiv":
        cfg = config_mod.load(config)
        corpus = str(ingest_mod.fetch_corpus_file(cfg.paths.cache / "corpus"))
    ingest_mod.run(config, corpus, n=n, seed=seed)


@main.command()
@click.option("--gate", is_flag=True, help="Exit non-zero if recall/latency gates fail.")
def bench(gate: bool) -> None:
    """Measure recall/latency vs the brute-force oracle."""
    _todo("M1-T4")


@main.command()
@click.option("--out", default="bundle", show_default=True)
def record(out: str) -> None:
    """Record a live cluster session into a replay bundle."""
    _todo("M6-T2")


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


@node.command("add")
@click.option("--shard", required=True, type=int)
@click.option("--replica", type=click.Choice(["a", "b"]), default="b")
def node_add(shard: int, replica: str) -> None:
    """Add a shard node to a running cluster."""
    _todo("M3-T5")


if __name__ == "__main__":
    main()
