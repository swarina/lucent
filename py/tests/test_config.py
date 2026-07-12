import pathlib

import pytest

from lucent import config as config_mod

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]


def test_loads_repo_cluster_yaml() -> None:
    cfg = config_mod.load(REPO_ROOT / "cluster.yaml")
    assert cfg.model.dim == 384
    assert cfg.ports.shard_base == 7100
    assert cfg.shard_port(2, "b") == 7121
    assert cfg.shard_addr(0, "a") == "127.0.0.1:7100"
    assert cfg.embed_addr.endswith(":7001")
    assert "~" not in str(cfg.paths.cache)  # expanded


def test_missing_section_names_it(tmp_path: pathlib.Path) -> None:
    p = tmp_path / "c.yaml"
    p.write_text("cluster: { shards: 4, replicas: 2, partitioning: hash }\n")
    with pytest.raises(config_mod.ConfigError, match=r"\.model"):
        config_mod.load(p)


def test_missing_field_names_path(tmp_path: pathlib.Path) -> None:
    p = tmp_path / "c.yaml"
    p.write_text(
        (REPO_ROOT / "cluster.yaml").read_text().replace(
            "  partitioning: hash          # hash | semantic\n", ""
        )
    )
    with pytest.raises(config_mod.ConfigError, match="cluster.partitioning"):
        config_mod.load(p)


def test_bad_partitioning(tmp_path: pathlib.Path) -> None:
    p = tmp_path / "c.yaml"
    p.write_text(
        (REPO_ROOT / "cluster.yaml").read_text().replace(
            "partitioning: hash", "partitioning: rand"
        )
    )
    with pytest.raises(config_mod.ConfigError, match="hash|semantic"):
        config_mod.load(p)


def test_bad_replica_arg() -> None:
    cfg = config_mod.load(REPO_ROOT / "cluster.yaml")
    with pytest.raises(config_mod.ConfigError):
        cfg.shard_port(0, "c")
