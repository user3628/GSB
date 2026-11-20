from __future__ import annotations

from multiprocessing import get_context
from pathlib import Path
import json
import time

import numpy as np
import pytest

from spmm_ops.engine import RunOptions, spmm
from spmm_ops.errors import SpMMError
from spmm_ops.version_store import GENESIS_DIGEST, VersionStore
from spmm_ops.util import array_digest


def _scheme_payload(tag: str) -> dict:
    return {
        "scheme": {
            "row_block": 8,
            "col_block": 8,
            "source": "user",
            "scheme_id": f"scheme-{tag}",
        },
        "cache_budget_bytes": 65536,
        "sum_strategy": "sequential",
        "tag": tag,
    }


def _concurrent_publish(root: str, tag: str, barrier, queue) -> None:
    try:
        store = VersionStore(root)
        barrier.wait(timeout=10)
        record = store.publish_scheme(_scheme_payload(tag), expected_sequence=0)
        queue.put(("ok", record["sequence"]))
    except SpMMError as exc:
        queue.put((exc.code, exc.details))
    except Exception as exc:  # pragma: no cover - diagnostic path
        queue.put(("EXCEPTION", repr(exc)))


def test_versions_advance_continuously_and_reject_stale_expected_version(tmp_path: Path) -> None:
    store = VersionStore(tmp_path / "store")
    initial = store.status(expected_sequence=0, expected_digest=GENESIS_DIGEST)
    assert initial["sequence"] == 0
    first = store.publish_scheme(_scheme_payload("a"), expected_sequence=0)
    second = store.publish_scheme(_scheme_payload("b"), expected_sequence=1)
    assert first["sequence"] == 1
    assert second["sequence"] == 2
    with pytest.raises(SpMMError) as caught:
        store.publish_scheme(_scheme_payload("c"), expected_sequence=1)
    assert caught.value.code == "E_STALE_PUBLISH"
    status = store.status(expected_sequence=2)
    assert status["records"][0]["prev_digest"] == GENESIS_DIGEST
    assert status["records"][1]["prev_digest"] == first["digest"]


def test_sequence_rollback_is_detected(tmp_path: Path) -> None:
    store = VersionStore(tmp_path / "store")
    store.publish_scheme(_scheme_payload("a"), expected_sequence=0)
    first_state = store.state_path.read_bytes()
    store.publish_scheme(_scheme_payload("b"), expected_sequence=1)
    store.state_path.write_bytes(first_state)
    with pytest.raises(SpMMError) as caught:
        store.status(expected_sequence=2)
    assert caught.value.code == "E_STALE_PUBLISH"
    assert caught.value.details["relation"] == "sequence_regression"


def test_same_sequence_wholesale_replacement_is_detected(tmp_path: Path) -> None:
    original = VersionStore(tmp_path / "original")
    first = original.publish_scheme(_scheme_payload("a"), expected_sequence=0)
    replacement = VersionStore(tmp_path / "replacement")
    replacement.publish_scheme(_scheme_payload("different"), expected_sequence=0)
    original.state_path.write_bytes(replacement.state_path.read_bytes())
    with pytest.raises(SpMMError) as caught:
        original.status(expected_sequence=1, expected_digest=first["digest"])
    assert caught.value.code == "E_STALE_PUBLISH"
    assert caught.value.details["relation"] == "history_replacement"


def test_anchor_detects_rollback_without_caller_expectation(tmp_path: Path) -> None:
    store = VersionStore(tmp_path / "store")
    store.publish_scheme(_scheme_payload("a"), expected_sequence=0)
    first_state = store.state_path.read_bytes()
    store.publish_scheme(_scheme_payload("b"), expected_sequence=1)
    store.state_path.write_bytes(first_state)
    with pytest.raises(SpMMError) as caught:
        store.status()
    assert caught.value.code == "E_STALE_PUBLISH"
    assert caught.value.details["relation"] == "sequence_regression"


def test_concurrent_publishers_only_one_wins(tmp_path: Path) -> None:
    root = str(tmp_path / "store")
    context = get_context("spawn")
    barrier = context.Barrier(2)
    queue = context.Queue()
    processes = [
        context.Process(target=_concurrent_publish, args=(root, tag, barrier, queue))
        for tag in ("a", "b")
    ]
    for process in processes:
        process.start()
    results = [queue.get(timeout=20) for _ in processes]
    for process in processes:
        process.join(timeout=20)
        assert process.exitcode == 0
    codes = sorted(result[0] for result in results)
    assert codes == ["E_STALE_PUBLISH", "ok"]
    assert VersionStore(root).status()["sequence"] == 1


def test_cross_scheme_cache_publish_is_rejected(tmp_path: Path) -> None:
    store = VersionStore(tmp_path / "store")
    scheme_a = store.publish_scheme(_scheme_payload("a"), expected_sequence=0)
    scheme_b = store.publish_scheme(_scheme_payload("b"), expected_sequence=1)
    output = np.arange(6, dtype=np.float64).reshape(2, 3)
    payload = {
        "scheme_sequence": scheme_a["sequence"],
        "scheme_digest": scheme_a["digest"],
        "sum_strategy": "sequential",
        "accumulation_order": "ascending",
    }
    cache_key = array_digest(output)
    with pytest.raises(SpMMError) as caught:
        store.publish_cache(cache_key, output, payload)
    assert caught.value.code == "E_STALE_PUBLISH"
    assert caught.value.details["active_scheme_sequence"] == scheme_b["sequence"]


def test_plan_change_invalidates_old_cache_even_when_key_returns(sample_dir: Path, tmp_path: Path) -> None:
    store_dir = tmp_path / "store"
    sparse = sample_dir / "degenerate_struct.npz"
    dense = sample_dir / "degenerate_struct.dense.npy"

    def run(plan: tuple[int, int]):
        return spmm(
            sparse,
            dense,
            options=RunOptions(
                block_scheme=plan,
                order="block",
                audit=False,
                version_store=store_dir,
                memory_limit_bytes=4 * 1024 * 1024,
            ),
        )

    first_a = run((16, 24))
    repeat_a = run((16, 24))
    plan_b = run((24, 40))
    return_a = run((16, 24))
    assert first_a.cache["hit"] is False
    assert repeat_a.cache["hit"] is True
    assert plan_b.cache["hit"] is False
    assert return_a.cache["hit"] is False
    assert first_a.output_digest == repeat_a.output_digest == return_a.output_digest
    assert plan_b.output_digest != first_a.output_digest
    status = VersionStore(store_dir).status()
    assert status["scheme_version_count"] == 3
    assert status["cache_version_count"] == 3
    assert status["sequence"] == 6

