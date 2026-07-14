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
    assert by_id["coord-0"].port == 7002  # 7000 is macOS AirPlay
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


# --- node add (M3-T5): Supervisor.spawn ---------------------------------------

def _configured_sup(cfg, data_root: pathlib.Path, shard_bin: str = "/bin/echo") -> Supervisor:
    return Supervisor([], config_path=str(REPO_ROOT / "cluster.yaml"), cfg=cfg,
                      shard_bin=shard_bin, data_root=data_root)


def test_spawn_unconfigured_supervisor_errors() -> None:
    sup = Supervisor([ProcSpec("shard-0a", "shard", ["true"], 1)])
    assert "error" in sup.spawn(0, "b")  # no cfg/shard_bin/data_root


def test_spawn_rejects_bad_shard_and_replica(cfg, tmp_path) -> None:
    sup = _configured_sup(cfg, tmp_path / "data")
    assert "no such shard" in sup.spawn(999, "b")["error"]
    assert "replica" in sup.spawn(0, "z")["error"]


def test_spawn_requires_sealed_primary(cfg, tmp_path) -> None:
    # No data dir for shard-0a at all → cannot copy a sealed index.
    sup = _configured_sup(cfg, tmp_path / "data")
    assert "no sealed index" in sup.spawn(0, "b")["error"]


def test_spawn_copies_sealed_index_and_registers_proc(cfg, tmp_path) -> None:
    data_root = tmp_path / "data"
    primary = data_root / "shard-0a"
    primary.mkdir(parents=True)
    (primary / "manifest.json").write_text('{"sealed": true}')
    (primary / "graph.bin").write_bytes(b"\x00\x01\x02")

    sup = _configured_sup(cfg, data_root)
    try:
        res = sup.spawn(0, "b")
        assert res.get("error") is None, res
        assert res["nodeId"] == "shard-0b"
        assert res["port"] == cfg.shard_port(0, "b")
        assert res["addr"].endswith(str(cfg.shard_port(0, "b")))
        # The sealed index was copied so the new backup can load it.
        assert (data_root / "shard-0b" / "manifest.json").exists()
        assert (data_root / "shard-0b" / "graph.bin").read_bytes() == b"\x00\x01\x02"
        # It's tracked and appears in the snapshot.
        assert any(p["nodeId"] == "shard-0b" for p in sup.snapshot())
    finally:
        sup.kill("shard-0b")
