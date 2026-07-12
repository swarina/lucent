"""Supervisor spec-building tests (no processes spawned)."""

import pathlib

import pytest

from lucent import config as config_mod
from lucent.dev import ProcSpec, Supervisor, build_specs, find_repo_root

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture()
def cfg():
    return config_mod.load(REPO_ROOT / "cluster.yaml")


def has_binaries() -> bool:
    return (REPO_ROOT / "build" / "dev" / "cpp" / "shard" / "lucent-shard").exists()


@pytest.mark.skipif(not has_binaries(), reason="C++ binaries not built")
def test_build_specs_shape(cfg) -> None:
    specs = build_specs(REPO_ROOT, REPO_ROOT / "cluster.yaml", cfg, replicas=1)
    by_id = {s.node_id: s for s in specs}
    # embed + gateway + coord + 4 shard primaries
    assert set(by_id) == {"embed-0", "collector-0", "coord-0",
                          "shard-0a", "shard-1a", "shard-2a", "shard-3a"}
    assert by_id["shard-2a"].port == 7120
    assert by_id["coord-0"].port == 7000
    assert by_id["collector-0"].port == 8080  # readiness probes the HTTP port
    assert "--node-id" in by_id["shard-0a"].argv


@pytest.mark.skipif(not has_binaries(), reason="C++ binaries not built")
def test_build_specs_replicas_two(cfg) -> None:
    specs = build_specs(REPO_ROOT, REPO_ROOT / "cluster.yaml", cfg, replicas=2)
    ids = {s.node_id for s in specs}
    assert "shard-0b" in ids and "shard-3b" in ids
    assert len([i for i in ids if i.startswith("shard-")]) == 8


def test_supervisor_snapshot_and_unknown_node() -> None:
    sup = Supervisor([ProcSpec("shard-0a", "shard", ["true"], 1)])
    snap = sup.snapshot()
    assert snap[0]["nodeId"] == "shard-0a"
    assert snap[0]["state"] == "spawning"
    assert "error" in sup.kill("nope")
    assert "error" in sup.restart("nope")


def test_find_repo_root() -> None:
    assert find_repo_root(REPO_ROOT / "cluster.yaml") == REPO_ROOT
