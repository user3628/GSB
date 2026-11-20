from __future__ import annotations

import multiprocessing
from pathlib import Path

import pytest

from linreg_workbench.canonical import atomic_write_json
from linreg_workbench.checkpoints import CheckpointStore
from linreg_workbench.demo import _concurrent_publish_child
from linreg_workbench.errors import InvalidStage, StaleCheckpoint


def setup_store(tmp_path: Path) -> CheckpointStore:
    run_dir = tmp_path / "run"
    (run_dir / "artifacts").mkdir(parents=True)
    atomic_write_json(
        run_dir / "artifacts" / "preprocess.json", {"artifact_type": "preprocess"}
    )
    atomic_write_json(run_dir / "artifacts" / "folds.json", {"artifact_type": "folds"})
    atomic_write_json(
        run_dir / "artifacts" / "solve.json", {"artifact_type": "solve"}
    )
    return CheckpointStore(run_dir, "run-id")


def publish_pre(store: CheckpointStore) -> dict:
    return store.publish(
        stage="preprocess",
        fold=None,
        task_index=None,
        role="preprocess",
        input_fingerprint="pre",
        config_version="cfg",
        coefficient_summary=None,
        artifact_path="artifacts/preprocess.json",
    )


def test_stage_order_duplicates_and_stale_sequence(tmp_path: Path) -> None:
    store = setup_store(tmp_path)
    with pytest.raises(InvalidStage) as jump:
        store.publish(
            stage="folds",
            fold=None,
            task_index=None,
            role="folds",
            input_fingerprint="fold",
            config_version="cfg",
            coefficient_summary=None,
            artifact_path="artifacts/folds.json",
        )
    assert jump.value.code == "E_INVALID_STAGE"

    first = publish_pre(store)
    assert first["sequence"] == 1
    with pytest.raises(StaleCheckpoint):
        publish_pre(store)
    store.publish(
        stage="folds",
        fold=None,
        task_index=None,
        role="folds",
        input_fingerprint="fold",
        config_version="cfg",
        coefficient_summary=None,
        artifact_path="artifacts/folds.json",
    )
    with pytest.raises(StaleCheckpoint):
        store.publish(
            stage="folds",
            fold=None,
            task_index=None,
            role="folds",
            input_fingerprint="fold",
            config_version="cfg",
            coefficient_summary=None,
            artifact_path="artifacts/folds.json",
        )
    with pytest.raises(StaleCheckpoint):
        store.publish(
            stage="solve",
            fold=1,
            task_index=1,
            role="solve",
            input_fingerprint="solve",
            config_version="cfg",
            coefficient_summary=None,
            artifact_path="artifacts/solve.json",
            sequence=99,
        )


def test_corrupt_and_incomplete_records_are_ignored(tmp_path: Path) -> None:
    store = setup_store(tmp_path)
    publish_pre(store)
    store.directory.joinpath("00000002.json").write_bytes(b'{"bad":')
    store.directory.joinpath(".00000003.json.tmp").write_bytes(b"partial")
    state = store.inspect()
    assert [record["sequence"] for record in state["records"]] == [1]
    assert state["next_sequence"] == 2
    assert any(item["file"] == "00000002.json" for item in state["ignored"])
    store.repair_from_gap()
    assert not store.directory.joinpath("00000002.json").exists()


def test_concurrent_publishers_one_success_one_stale(tmp_path: Path) -> None:
    store = setup_store(tmp_path)
    publish_pre(store)
    context = multiprocessing.get_context("fork")
    barrier = context.Barrier(2)
    queue = context.Queue()
    processes = [
        context.Process(
            target=_concurrent_publish_child,
            args=(
                str(store.run_dir),
                store.run_id,
                "artifacts/folds.json",
                barrier,
                queue,
            ),
        )
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    outcomes = sorted(queue.get(timeout=15)["result"] for _ in processes)
    for process in processes:
        process.join(timeout=10)
    assert outcomes == ["E_STALE_CHECKPOINT", "OK"]
    assert [record["sequence"] for record in store.inspect()["records"]] == [1, 2]


def test_cross_fold_misplacement_is_stale(tmp_path: Path) -> None:
    store = setup_store(tmp_path)
    publish_pre(store)
    store.publish(
        stage="folds",
        fold=None,
        task_index=None,
        role="folds",
        input_fingerprint="fold",
        config_version="cfg",
        coefficient_summary=None,
        artifact_path="artifacts/folds.json",
    )
    with pytest.raises(StaleCheckpoint):
        store.publish(
            stage="solve",
            fold=2,
            task_index=1,
            role="solve",
            input_fingerprint="solve",
            config_version="cfg",
            coefficient_summary=None,
            artifact_path="artifacts/solve.json",
        )
    store.publish(
        stage="solve",
        fold=1,
        task_index=1,
        role="solve",
        input_fingerprint="solve",
        config_version="cfg",
        coefficient_summary=None,
        artifact_path="artifacts/solve.json",
    )
    with pytest.raises(StaleCheckpoint):
        store.publish(
            stage="solve",
            fold=3,
            task_index=2,
            role="solve",
            input_fingerprint="solve2",
            config_version="cfg",
            coefficient_summary=None,
            artifact_path="artifacts/solve.json",
        )
    store.publish(
        stage="solve",
        fold=2,
        task_index=2,
        role="solve",
        input_fingerprint="solve2",
        config_version="cfg",
        coefficient_summary=None,
        artifact_path="artifacts/solve.json",
    )
    with pytest.raises(StaleCheckpoint):
        store.publish(
            stage="solve",
            fold=1,
            task_index=3,
            role="solve",
            input_fingerprint="solve3",
            config_version="cfg",
            coefficient_summary=None,
            artifact_path="artifacts/solve.json",
        )
