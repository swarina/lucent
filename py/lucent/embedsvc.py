"""Lucent embedding service (protocol.md §1.4).

gRPC EmbedService wrapping a CPU sentence-transformer. The model warms at
startup and Info.ready gates cluster readiness. Vectors are L2-normalized by
the encoder and re-asserted here — normalized vectors are what make inner
product == cosine across the whole system (data-formats.md).

The EMBED span is emitted by the coordinator (it owns trace_id + deadline);
this service is a pure encoder by design — EmbedRequest carries no trace_id.
"""

from __future__ import annotations

import concurrent.futures
import logging
import signal
from typing import Protocol, Sequence

import grpc
import numpy as np

from lucent.v1 import embed_pb2, embed_pb2_grpc

log = logging.getLogger("lucent.embedsvc")

MAX_BATCH = 256
NORM_TOLERANCE = 1e-3


class Encoder(Protocol):
    """Minimal encoder surface; tests inject fakes, production uses MiniLM."""

    name: str
    dim: int

    def encode(self, texts: Sequence[str]) -> np.ndarray:  # (n, dim) float32
        ...


class MiniLmEncoder:
    """sentence-transformers encoder, model cached under paths.cache/models."""

    def __init__(self, model_name: str, cache_dir: str) -> None:
        # Imported lazily: heavy (torch), and only this class needs it.
        from sentence_transformers import SentenceTransformer

        self._model = SentenceTransformer(
            model_name, cache_folder=cache_dir, device="cpu"
        )
        self.name = model_name
        # sentence-transformers >=5 renamed the accessor; support both.
        get_dim = getattr(self._model, "get_embedding_dimension", None) or (
            self._model.get_sentence_embedding_dimension
        )
        self.dim = int(get_dim())

    def encode(self, texts: Sequence[str]) -> np.ndarray:
        return self._model.encode(
            list(texts),
            batch_size=64,
            convert_to_numpy=True,
            normalize_embeddings=True,
        ).astype(np.float32)


class HashEncoder:
    """Deterministic torch-free encoder: vector = seeded-RNG(xxh3(text)).

    For CI and fast local dev (`--fake`): same text always maps to the same
    unit vector, so ingest->query plumbing works end to end; semantic quality
    is obviously absent and that's fine — this tests distribution, not ML.
    """

    def __init__(self, dim: int) -> None:
        self.name = "fake-hash"
        self.dim = dim

    def encode(self, texts: Sequence[str]) -> np.ndarray:
        import xxhash

        out = np.empty((len(texts), self.dim), dtype=np.float32)
        for i, t in enumerate(texts):
            rng = np.random.default_rng(xxhash.xxh3_64_intdigest(t.encode()))
            v = rng.standard_normal(self.dim).astype(np.float32)
            out[i] = v / np.linalg.norm(v)
        return out


class EmbedServicer(embed_pb2_grpc.EmbedServiceServicer):
    def __init__(self, encoder: Encoder) -> None:
        self._encoder = encoder

    def Embed(self, request, context):  # noqa: N802 (grpc naming)
        n = len(request.texts)
        if n == 0 or n > MAX_BATCH:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"texts must be 1..{MAX_BATCH}, got {n}",
            )
        vectors = self._encoder.encode(request.texts)
        if vectors.shape != (n, self._encoder.dim):
            context.abort(
                grpc.StatusCode.INTERNAL,
                f"encoder returned shape {vectors.shape}, want ({n}, {self._encoder.dim})",
            )
        norms = np.linalg.norm(vectors, axis=1)
        if not np.allclose(norms, 1.0, atol=NORM_TOLERANCE):
            context.abort(
                grpc.StatusCode.INTERNAL,
                f"encoder output not normalized (min={norms.min():.4f} max={norms.max():.4f})",
            )
        return embed_pb2.EmbedResponse(
            dim=self._encoder.dim,
            count=n,
            vectors=vectors.reshape(-1).tolist(),
        )

    def Info(self, request, context):  # noqa: N802
        return embed_pb2.InfoResponse(
            model=self._encoder.name, dim=self._encoder.dim, ready=True
        )


def make_server(encoder: Encoder, bind_addr: str) -> tuple[grpc.Server, int]:
    """Builds an insecure server; returns (server, bound_port)."""
    server = grpc.server(
        concurrent.futures.ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="embed"
        )
    )
    embed_pb2_grpc.add_EmbedServiceServicer_to_server(EmbedServicer(encoder), server)
    port = server.add_insecure_port(bind_addr)
    if port == 0:
        raise RuntimeError(f"embedsvc: cannot bind {bind_addr}")
    return server, port


def serve(config_path: str, fake: bool = False) -> None:
    """Blocking entrypoint used by `lucent embedsvc` and the supervisor."""
    from lucent import config as config_mod

    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(name)s] %(message)s")
    cfg = config_mod.load(config_path)

    encoder: Encoder
    if fake:
        log.info("FAKE encoder (deterministic hash vectors, dim=%d)", cfg.model.dim)
        encoder = HashEncoder(cfg.model.dim)
    else:
        cache_dir = str(cfg.paths.cache / "models")
        log.info("loading %s (cache: %s) ...", cfg.model.name, cache_dir)
        encoder = MiniLmEncoder(cfg.model.name, cache_dir)
        if encoder.dim != cfg.model.dim:
            raise RuntimeError(
                f"model dim {encoder.dim} != config model.dim {cfg.model.dim}"
            )
    # Warm the encoder before advertising readiness: torch's first encode in a
    # fresh process can take hundreds of ms, which would blow the
    # coordinator's embed deadline on the very first query.
    encoder.encode(["warmup"])

    server, port = make_server(encoder, f"127.0.0.1:{cfg.ports.embed}")
    server.start()
    log.info("embed-0: ready on 127.0.0.1:%d (dim=%d)", port, encoder.dim)

    stopped = server.wait_for_termination  # keep pyright quiet on closure
    signal.signal(signal.SIGTERM, lambda *_: server.stop(grace=2))
    signal.signal(signal.SIGINT, lambda *_: server.stop(grace=2))
    stopped()
    log.info("embed-0: shut down cleanly")
