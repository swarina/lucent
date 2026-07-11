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
@click.option("--replicas", default=2, show_default=True, help="Replicas per shard.")
@click.option(
    "--partitioning",
    type=click.Choice(["hash", "semantic"]),
    default="hash",
    show_default=True,
)
@click.option("--config", default="cluster.yaml", show_default=True)
def dev(shards: int, replicas: int, partitioning: str, config: str) -> None:
    """Spin up a local multi-process cluster (supervisor)."""
    _todo("M0-T11")


@main.command()
@click.option("--corpus", default="arxiv", show_default=True)
@click.option("--n", default=50000, show_default=True, help="Documents to ingest.")
@click.option("--shards", default=4, show_default=True)
@click.option(
    "--partitioning", type=click.Choice(["hash", "semantic"]), default="hash"
)
def ingest(corpus: str, n: int, shards: int, partitioning: str) -> None:
    """Ingest a corpus: fetch -> embed -> partition -> project -> load -> seal."""
    _todo("M0-T7")


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


@main.group()
def corpus() -> None:
    """Corpus utilities."""


@corpus.command("fetch")
@click.option("--name", default="arxiv", show_default=True)
@click.option("--n", default=50000, show_default=True)
def corpus_fetch(name: str, n: int) -> None:
    """Fetch and normalize a public corpus."""
    _todo("M0-T7")


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
