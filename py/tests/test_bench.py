"""Bench pure-logic tests (oracle + gates); the live sweep is exercised
against the real cluster locally and in the CI integration job."""

import numpy as np

from lucent.bench import ConfigResult, check_gates, oracle_topk


def test_oracle_topk_order_and_tiebreak() -> None:
    # 4 docs on axes; query along x → doc on x wins, ties broken by doc_id.
    ids = np.asarray([40, 30, 20, 10], dtype="<u8")
    vecs = np.asarray([[1, 0], [0, 1], [1, 0], [0, -1]], dtype=np.float32)
    q = np.asarray([[1, 0]], dtype=np.float32)
    truth = oracle_topk(ids, vecs, q, k=2)
    # docs 40 and 20 both score 1.0; both make top-2.
    assert truth[0] == {40, 20}


def test_oracle_topk_k_larger_than_n() -> None:
    ids = np.asarray([1, 2], dtype="<u8")
    vecs = np.asarray([[1, 0], [0, 1]], dtype=np.float32)
    q = np.asarray([[0.6, 0.8]], dtype=np.float32)
    assert oracle_topk(ids, vecs, q, k=10)[0] == {1, 2}


def make(ef: int, recall: float, probe: int = 0) -> ConfigResult:
    return ConfigResult("hash", 2, probe, ef, recall, 1.0, 2.0, 100.0)


def test_gates_pass() -> None:
    assert check_gates([make(16, 0.90), make(100, 0.96), make(200, 0.99)]) == []


def test_gate_recall_floor() -> None:
    fails = check_gates([make(100, 0.90)])
    assert len(fails) == 1 and "0.900 < 0.95" in fails[0]


def test_gate_monotonicity() -> None:
    fails = check_gates([make(16, 0.97), make(100, 0.99), make(200, 0.90)])
    assert any("monotonic" in f for f in fails)


def test_gates_ignore_partial_probe() -> None:
    # probe sweeps legitimately trade recall; gates only judge full probe.
    assert check_gates([make(100, 0.5, probe=1)]) == []
