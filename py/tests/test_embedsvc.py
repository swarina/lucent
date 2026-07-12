"""EmbedService tests with an injected fake encoder (no model download).

The real MiniLM path is exercised locally / at cluster boot; these tests pin
the service contract: batching limits, normalization enforcement, shapes.
"""

import numpy as np
import pytest

pytest.importorskip(
    "lucent.v1.embed_pb2",
    reason="proto bindings not generated — run tools/gen-proto.sh py",
)
grpc = pytest.importorskip("grpc")

from lucent.embedsvc import MAX_BATCH, make_server  # noqa: E402
from lucent.v1 import embed_pb2, embed_pb2_grpc  # noqa: E402

DIM = 8


class FakeEncoder:
    name = "fake-encoder"
    dim = DIM

    def __init__(self, normalize: bool = True) -> None:
        self._normalize = normalize

    def encode(self, texts):
        rng = np.random.default_rng(len(texts))  # deterministic per size
        vecs = rng.standard_normal((len(texts), DIM)).astype(np.float32)
        if self._normalize:
            vecs /= np.linalg.norm(vecs, axis=1, keepdims=True)
        return vecs


@pytest.fixture()
def stub():
    server, port = make_server(FakeEncoder(), "127.0.0.1:0")
    server.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    yield embed_pb2_grpc.EmbedServiceStub(channel)
    channel.close()
    server.stop(grace=None)


def test_embed_shapes_and_normalization(stub) -> None:
    resp = stub.Embed(embed_pb2.EmbedRequest(texts=["a", "b", "c"]))
    assert resp.dim == DIM
    assert resp.count == 3
    assert len(resp.vectors) == 3 * DIM
    vecs = np.array(resp.vectors, dtype=np.float32).reshape(3, DIM)
    assert np.allclose(np.linalg.norm(vecs, axis=1), 1.0, atol=1e-3)


def test_embed_rejects_empty_and_oversized(stub) -> None:
    with pytest.raises(grpc.RpcError) as e:
        stub.Embed(embed_pb2.EmbedRequest(texts=[]))
    assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT

    too_many = ["x"] * (MAX_BATCH + 1)
    with pytest.raises(grpc.RpcError) as e:
        stub.Embed(embed_pb2.EmbedRequest(texts=too_many))
    assert e.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_embed_max_batch_accepted(stub) -> None:
    resp = stub.Embed(embed_pb2.EmbedRequest(texts=["x"] * MAX_BATCH))
    assert resp.count == MAX_BATCH


def test_unnormalized_encoder_is_caught() -> None:
    server, port = make_server(FakeEncoder(normalize=False), "127.0.0.1:0")
    server.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            stub = embed_pb2_grpc.EmbedServiceStub(channel)
            with pytest.raises(grpc.RpcError) as e:
                stub.Embed(embed_pb2.EmbedRequest(texts=["a"]))
            assert e.value.code() == grpc.StatusCode.INTERNAL
    finally:
        server.stop(grace=None)


def test_info(stub) -> None:
    resp = stub.Info(embed_pb2.InfoRequest())
    assert resp.model == "fake-encoder"
    assert resp.dim == DIM
    assert resp.ready
