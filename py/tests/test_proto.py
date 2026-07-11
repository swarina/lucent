"""Verifies generated Python bindings import and round-trip.

Generated code is not checked in; run `tools/gen-proto.sh py` first (CI does).
Skips (rather than fails) when bindings are absent so the base suite stays green
without a codegen step.
"""

import importlib

import pytest

pytest.importorskip(
    "lucent.v1.shard_pb2",
    reason="proto bindings not generated — run tools/gen-proto.sh py",
)


def test_messages_roundtrip() -> None:
    common = importlib.import_module("lucent.v1.common_pb2")
    shard = importlib.import_module("lucent.v1.shard_pb2")

    req = shard.SearchRequest(k=10, ef_search=100, trace_level=common.TRACE_LEVEL_FULL)
    blob = req.SerializeToString()
    back = shard.SearchRequest.FromString(blob)
    assert back.k == 10
    assert back.trace_level == common.TRACE_LEVEL_FULL


def test_service_stub_present() -> None:
    grpc_mod = importlib.import_module("lucent.v1.shard_pb2_grpc")
    assert hasattr(grpc_mod, "ShardServiceStub")
    assert hasattr(grpc_mod, "add_ShardServiceServicer_to_server")
