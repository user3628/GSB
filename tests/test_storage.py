from __future__ import annotations

import json
import multiprocessing as mp
import shutil
from pathlib import Path

import numpy as np
import pytest

from chunkops.errors import ChunkOpsError, E_STALE_PUBLISH, E_STALE_VERSION
from chunkops.fingerprint import digest_object
from chunkops.storage import (
    CheckpointManager,
    ResultCache,
    VersionStore,
    atomic_write_json,
    read_json,
)


def _payload(shape: str = "shape-a", value: str = "value-a", marker: int = 1) -> dict:
    return {
        "shape_fingerprint": shape,
        "value_fingerprint": value,
        "marker": marker,
    }


def _concurrent_publisher(workspace: str, index: int, queue: "mp.Queue") -> None:
    try:
        store = VersionStore(workspace, "concurrent")
        store.publish(
            {"shape_fingerprint": "shape-a", "publisher_index": index},
            expected_seq=0,
            scope={"op": "softmax", "axis": 1},
            publisher=f"worker-{index}",
        )
        queue.put(("OK", index))
    except ChunkOpsError as exc:
        queue.put((exc.code, index))


def test_checkpoint_chain_and_tamper_detection(tmp_path: Path) -> None:
    manager = CheckpointManager(tmp_path)
    for index, stage in enumerate(("plan", "reduce", "validate", "summary"), start=1):
        manager.write(stage, {"value": index})
    discovered = manager.discover()
    assert discovered["latest_stage"] == "summary"
    assert discovered["latest_seq"] == 4
    assert len(discovered["valid_checkpoints"]) == 4
    path = tmp_path / "checkpoints" / "0003_validate.json"
    record = read_json(path)
    record["payload"]["value"] = 999
    atomic_write_json(path, record)
    damaged = manager.discover()
    assert damaged["latest_stage"] == "reduce"
    assert damaged["latest_seq"] == 2
    assert damaged["invalid"]["problems"]


def test_version_chain_advance_and_stale_publish(tmp_path: Path) -> None:
    store = VersionStore(tmp_path, "plans")
    first = store.publish(_payload(marker=1), expected_seq=0)
    second = store.publish(_payload(marker=2), expected_seq=1)
    assert first["seq"] == 1
    assert second["seq"] == 2
    assert store.verify()["status"] == "healthy"
    with pytest.raises(ChunkOpsError) as stale:
        store.publish(_payload(marker=3), expected_seq=1)
    assert stale.value.code == E_STALE_PUBLISH
    with pytest.raises(ChunkOpsError) as duplicate:
        store.publish(_payload(marker=2), expected_seq=1)
    assert duplicate.value.code == E_STALE_PUBLISH


def test_version_shape_change_and_scope_mismatch(tmp_path: Path) -> None:
    store = VersionStore(tmp_path, "plans")
    store.publish(_payload("shape-a"), expected_seq=0, scope={"op": "softmax", "axis": 1})
    with pytest.raises(ChunkOpsError) as shape_changed:
        store.publish(_payload("shape-b"), expected_seq=1, scope={"op": "softmax", "axis": 1})
    assert shape_changed.value.code == E_STALE_VERSION
    assert shape_changed.value.details["stale_reason"] == "shape_fingerprint_changed"
    with pytest.raises(ChunkOpsError) as scope_changed:
        store.publish(_payload("shape-a"), expected_seq=1, scope={"op": "layernorm", "axis": 0})
    assert scope_changed.value.code == E_STALE_PUBLISH


def test_version_rollback_and_tamper_are_identified(tmp_path: Path) -> None:
    workspace = tmp_path / "rollback"
    store = VersionStore(workspace, "plans")
    store.publish(_payload(marker=1), expected_seq=0)
    snapshot = tmp_path / "snapshot"
    shutil.copytree(store.root, snapshot)
    store.publish(_payload(marker=2), expected_seq=1)
    shutil.rmtree(store.root)
    shutil.copytree(snapshot, store.root)
    with pytest.raises(ChunkOpsError) as rollback:
        store.verify()
    assert rollback.value.code == E_STALE_VERSION
    assert rollback.value.details["stale_reason"] == "rollback"

    workspace2 = tmp_path / "tamper"
    other = VersionStore(workspace2, "plans")
    other.publish(_payload(marker=1), expected_seq=0)
    head = read_json(other.head_path)
    record_path = other.root / head["record"] if not Path(head["record"]).is_absolute() else Path(head["record"])
    record = read_json(record_path)
    record["payload"]["marker"] = 999
    atomic_write_json(record_path, record)
    with pytest.raises(ChunkOpsError) as tampered:
        other.verify()
    assert tampered.value.code == E_STALE_VERSION
    assert tampered.value.details["stale_reason"] == "tamper"


def test_concurrent_publishers_only_one_advances(tmp_path: Path) -> None:
    context = mp.get_context("fork")
    queue = context.Queue()
    processes = [
        context.Process(target=_concurrent_publisher, args=(str(tmp_path), index, queue))
        for index in range(6)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=20)
        assert process.exitcode == 0
    results = [queue.get(timeout=5) for _ in range(len(processes))]
    successes = [item for item in results if item[0] == "OK"]
    stale = [item for item in results if item[0] == E_STALE_PUBLISH]
    assert len(successes) == 1
    assert len(stale) == len(processes) - 1
    store = VersionStore(tmp_path, "concurrent")
    assert store.verify()["seq"] == 1


def test_result_cache_integrity_and_miss(tmp_path: Path) -> None:
    cache = ResultCache(tmp_path)
    key = "a" * 64
    array = np.arange(12, dtype=np.float32).reshape(3, 4)
    metadata = {"shape_fingerprint": "shape-a", "sum_strategy": "pairwise"}
    cache.put(key, array, metadata)
    loaded, meta = cache.get(key)
    np.testing.assert_array_equal(loaded, array)
    assert meta["array_digest"]
    assert cache.get("b" * 64) is None
    array_path = cache._array_path(key)
    array_path.write_bytes(b"corrupted")
    assert cache.get(key) is None


def test_cache_key_changes_on_every_required_component() -> None:
    from chunkops.strategies import build_auto_plan, build_requested_plan

    plan = build_auto_plan(op="softmax", axis=1, rows=128, cols=512)
    kwargs = dict(
        shape_fp="shape-a",
        value_fp="value-a",
        input_dtype="float32",
        output_dtype="float32",
        eps=1e-5,
        gamma_fp=None,
        beta_fp=None,
        memory_budget_bytes=1024,
        effective_row_block=plan.row_block,
        effective_col_block=plan.col_block,
    )
    base = plan.cache_key(**kwargs)
    strategy_plan = build_requested_plan(
        op="softmax",
        axis=1,
        rows=128,
        cols=512,
        row_block=plan.row_block,
        col_block=plan.col_block,
        sum_strategy="compensated",
    )
    chunk_plan = build_requested_plan(
        op="softmax",
        axis=1,
        rows=128,
        cols=512,
        row_block=plan.row_block,
        col_block=plan.col_block,
        sum_strategy=plan.sum_strategy,
    )
    chunk_plan.row_block = plan.row_block + 1
    changed = {
        plan.cache_key(**{**kwargs, "shape_fp": "shape-b"}),
        plan.cache_key(**{**kwargs, "value_fp": "value-b"}),
        plan.cache_key(**{**kwargs, "memory_budget_bytes": 2048}),
        plan.cache_key(**{**kwargs, "effective_row_block": 1}),
        plan.cache_key(**{**kwargs, "effective_col_block": 1}),
        strategy_plan.cache_key(**kwargs),
        chunk_plan.cache_key(**kwargs),
    }
    assert base not in changed
    assert len(changed) >= 6
