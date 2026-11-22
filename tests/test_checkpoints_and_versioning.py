from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from feature_workbench import Workbench
from feature_workbench.checkpoints import CheckpointStore, make_record
from feature_workbench.errors import WorkbenchError
from feature_workbench.util import FileLock, FileLockTimeout, atomic_write_bytes

from conftest import mutate_config


def _hold_file_lock(path: str, ready, release) -> None:
    with FileLock(Path(path)):
        ready.set()
        release.wait(5)


def _record(stage: str, scope: str, version: str, dependencies=None) -> dict:
    return make_record(
        stage=stage,
        scope=scope,
        pipeline_version="pipeline-a",
        stage_version=version,
        input_fingerprint="input",
        config_version="config",
        artifact_digest="artifact",
        dependencies=dependencies or [],
    )


def test_checkpoint_stale_duplicate_and_dependency_errors(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "state")
    type_record = store.publish(_record("type_inference", "ALL", "type-v1"), expected_sequence=1)
    assert type_record["sequence"] == 1
    with pytest.raises(WorkbenchError) as stale:
        store.publish(_record("encoding", "shard:0", "enc-v1"), expected_sequence=1)
    assert stale.value.code == "E_STALE_CHECKPOINT"
    with pytest.raises(WorkbenchError) as duplicate:
        store.publish(_record("type_inference", "ALL", "type-v1"), expected_sequence=2)
    assert duplicate.value.code == "E_STALE_CHECKPOINT"
    missing_dependency = [
        {"stage": "encoding", "stage_version": "enc-v1", "scope": "shard:0"}
    ]
    with pytest.raises(WorkbenchError) as dependency:
        store.publish(
            _record("selection", "tier:small", "sel-v1", missing_dependency),
            expected_sequence=2,
        )
    assert dependency.value.code == "E_STALE_CHECKPOINT"


def test_checkpoint_rejects_cross_stage_regression(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "state")
    store.publish(_record("type_inference", "ALL", "type-v1"), expected_sequence=1)
    store.publish(_record("encoding", "shard:0", "enc-v1"), expected_sequence=2)
    with pytest.raises(WorkbenchError) as raised:
        store.publish(
            _record("type_inference", "ALL", "type-v2"),
            expected_sequence=3,
        )
    assert raised.value.code == "E_STALE_CHECKPOINT"


def test_atomic_write_preserves_previous_file_when_replace_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "record.json"
    target.write_bytes(b"old")

    def fail_replace(source, destination):
        raise OSError("injected replace failure")

    monkeypatch.setattr(os, "replace", fail_replace)
    with pytest.raises(OSError):
        atomic_write_bytes(target, b"new")
    assert target.read_bytes() == b"old"


def test_real_file_lock_blocks_another_process(tmp_path: Path) -> None:
    import multiprocessing as mp

    path = tmp_path / "lockfile"
    context = mp.get_context("spawn")
    ready = context.Event()
    release = context.Event()
    holder = context.Process(target=_hold_file_lock, args=(str(path), ready, release))
    holder.start()
    try:
        assert ready.wait(5)
        with pytest.raises(FileLockTimeout):
            FileLock(path).acquire(timeout=0.1)
    finally:
        release.set()
        holder.join(5)


def test_selection_change_invalidates_only_selection_and_downstream(workspace: Path) -> None:
    workbench = Workbench(root=workspace, state_dir=workspace / "state", export_dir=workspace / "exports")
    first = workbench.run(worker_mode="sequential")
    mutate_config(
        workspace,
        {"selection.threshold": 0.02},
    )
    status = workbench.status()
    assert status["affected_earliest_stage"] == "selection"
    assert status["stages"]["type_inference"]["complete"] is True
    assert status["stages"]["encoding"]["complete"] is True
    assert status["stages"]["selection"]["complete"] is False
    second = workbench.run(worker_mode="sequential")
    assert second["result_digest"] != first["result_digest"]
    assert second["reused"]["type_inference"] is True
    assert second["reused"]["encoding_records"] == 5
    assert second["reused"]["selection_records"] == 0


def test_budget_runtime_change_invalidates_aggregate_only(workspace: Path) -> None:
    workbench = Workbench(root=workspace, state_dir=workspace / "state", export_dir=workspace / "exports")
    first = workbench.run(worker_mode="sequential")
    mutate_config(workspace, {"budget.query_latency_ms": 300.0})
    status = workbench.status()
    assert status["affected_earliest_stage"] == "aggregate"
    assert status["stages"]["type_inference"]["complete"] is True
    assert status["stages"]["encoding"]["complete"] is True
    assert status["stages"]["selection"]["complete"] is True
    assert status["stages"]["aggregate"]["complete"] is False
    second = workbench.run(worker_mode="sequential")
    assert second["result_digest"] != first["result_digest"]
    assert second["reused"]["encoding_records"] == 5
    assert second["reused"]["selection_records"] == 2


def test_encoding_change_invalidates_encoding_and_downstream(workspace: Path) -> None:
    workbench = Workbench(root=workspace, state_dir=workspace / "state", export_dir=workspace / "exports")
    workbench.run(worker_mode="sequential")
    mutate_config(
        workspace,
        {
            "encoding.strategy": "target",
            "encoding.target.smoothing": 4.5,
            "selection.method": "mutual_info",
            "selection.threshold": 0.0,
        },
    )
    status = workbench.status()
    assert status["affected_earliest_stage"] == "encoding"
    assert status["stages"]["type_inference"]["complete"] is True
    assert status["stages"]["encoding"]["complete"] is False
    result = workbench.run(worker_mode="sequential")
    assert result["reused"]["type_inference"] is True
    assert result["reused"]["encoding_records"] == 0
    assert result["status"] == "complete"


def test_sample_content_change_invalidates_from_type_inference(workspace: Path) -> None:
    workbench = Workbench(root=workspace, state_dir=workspace / "state", export_dir=workspace / "exports")
    workbench.run(worker_mode="sequential")
    path = workspace / "data" / "small_00.csv"
    lines = path.read_text(encoding="utf-8").splitlines()
    fields = lines[1].split(",")
    fields[1] = f"{float(fields[1]) + 1.0:.6f}"
    lines[1] = ",".join(fields)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    status = workbench.status()
    assert status["affected_earliest_stage"] == "type_inference"
    assert status["stages"]["type_inference"]["complete"] is False
    assert status["stages"]["encoding"]["complete"] is False

