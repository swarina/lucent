"""Typed view of cluster.yaml for Python components.

Mirrors the C++ loader (cpp/common/config.cc) for the fields Python needs.
Same philosophy: die loudly on missing fields, never run on silent defaults.
"""

from __future__ import annotations

import pathlib
from dataclasses import dataclass

import yaml


class ConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class Model:
    name: str
    dim: int
    metric: str


@dataclass(frozen=True)
class Ports:
    coordinator: int
    embed: int
    collector: int
    gateway_http: int
    supervisor_ctl: int
    shard_base: int
    member_base: int = 7200  # raft member j -> member_base + j (M5)


@dataclass(frozen=True)
class Cluster:
    shards: int
    replicas: int
    partitioning: str


@dataclass(frozen=True)
class Paths:
    data: pathlib.Path
    cache: pathlib.Path  # '~' expanded


@dataclass(frozen=True)
class Config:
    cluster: Cluster
    model: Model
    ports: Ports
    paths: Paths
    index_seed: int

    def shard_port(self, shard_id: int, replica: str) -> int:
        if replica not in ("a", "b") or shard_id < 0:
            raise ConfigError(f"bad shard/replica: {shard_id}{replica}")
        return self.ports.shard_base + shard_id * 10 + (0 if replica == "a" else 1)

    def shard_addr(self, shard_id: int, replica: str) -> str:
        return f"127.0.0.1:{self.shard_port(shard_id, replica)}"

    @property
    def embed_addr(self) -> str:
        return f"127.0.0.1:{self.ports.embed}"

    @property
    def coordinator_addr(self) -> str:
        return f"127.0.0.1:{self.ports.coordinator}"

    @property
    def collector_addr(self) -> str:
        return f"127.0.0.1:{self.ports.collector}"


def _require(mapping: dict, key: str, path: str):
    if key not in mapping or mapping[key] is None:
        raise ConfigError(f"cluster.yaml: missing required field '{path}.{key}'")
    return mapping[key]


def load(path: str | pathlib.Path) -> Config:
    try:
        raw = yaml.safe_load(pathlib.Path(path).read_text())
    except OSError as e:
        raise ConfigError(f"cluster.yaml: cannot load '{path}': {e}") from e

    cluster = _require(raw, "cluster", "")
    model = _require(raw, "model", "")
    ports = _require(raw, "ports", "")
    paths = _require(raw, "paths", "")
    index = _require(raw, "index", "")

    partitioning = _require(cluster, "partitioning", "cluster")
    if partitioning not in ("hash", "semantic"):
        raise ConfigError(
            f"cluster.yaml: cluster.partitioning must be hash|semantic, got {partitioning!r}"
        )

    return Config(
        cluster=Cluster(
            shards=int(_require(cluster, "shards", "cluster")),
            replicas=int(_require(cluster, "replicas", "cluster")),
            partitioning=partitioning,
        ),
        model=Model(
            name=str(_require(model, "name", "model")),
            dim=int(_require(model, "dim", "model")),
            metric=str(_require(model, "metric", "model")),
        ),
        ports=Ports(
            coordinator=int(_require(ports, "coordinator", "ports")),
            embed=int(_require(ports, "embed", "ports")),
            collector=int(_require(ports, "collector", "ports")),
            gateway_http=int(_require(ports, "gateway_http", "ports")),
            supervisor_ctl=int(_require(ports, "supervisor_ctl", "ports")),
            shard_base=int(_require(ports, "shard_base", "ports")),
            member_base=int(ports.get("member_base", 7200)),
        ),
        paths=Paths(
            data=pathlib.Path(str(_require(paths, "data", "paths"))),
            cache=pathlib.Path(str(_require(paths, "cache", "paths"))).expanduser(),
        ),
        index_seed=int(_require(index, "seed", "index")),
    )
