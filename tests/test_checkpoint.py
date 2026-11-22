from __future__ import annotations

import json
import multiprocessing
from pathlib import Path

import numpy as np
import pytest

from blockgemm import BlockGemmError, E_INVALID_SHAPE, E_STALE_CHECKPOINT, run_gemm_job
from blockgemm.checkpoint import CheckpointStore


def _publish_race_worker(workspace: str, result_queue) -> None:
    import numpy as np

    from blockgemm import BlockGemmError
    from blockgemm.checkpoint import CheckpointStore

    store = CheckpointStore(workspace)
    record = json.loads((Path(workspace) / "checkpoints" / "0000000000.json").read_text())
    record["sequence"] = 1
    record["block_coord"] = [0, 1]
    record["col_range"] = [32, 64]
    record.pop("record_checksum", None)
    try:
        store.publish_block(record, np.zeros((32, 32), dtype=np.float64))
        result_queue.put(("ok", None))
    except BlockGemmError as error:
        result_queue.put((error.code, error.reason))


def _paths(tmp_path: Path, manifest_path: Path):
    loaded = load_case_for_test(manifest_path, "small_gemm")
    return loaded


def load_case_for_test(manifest_path: Path, case_id: str):
    from blockgemm.manifest import load_case

    return load_case(manifest_path, case_id)


def test_require_missing_prerequisite_returns_invalid_shape(manifest_path, tmp_path) -> None:
    loaded = load_case_for_test(manifest_path, "small_gemm")
    run_gemm_job(
        tmp_path / "ws",
        loaded.a,
        loaded.b,
        expected=loaded.expected,
        l2_cache_kib=32,
        stop_after_blocks=1,
    )
    with pytest.raises(BlockGemmError) as caught:
        CheckpointStore(tmp_path / "ws").require_block(1, expected_shard_id="gemm", expected_block_coord=[0, 1])
    assert caught.value.code == E_INVALID_SHAPE
    assert caught.value.reason == "missing_prerequisite_block"
    assert caught.value.shard_id == "gemm"
    assert caught.value.block_coord == [0, 1]


def test_cross_plan_publish_returns_stale(manifest_path, tmp_path) -> None:
    loaded = load_case_for_test(manifest_path, "small_gemm")
    workspace = tmp_path / "ws"
    run_gemm_job(
        workspace,
        loaded.a,
        loaded.b,
        expected=loaded.expected,
        l2_cache_kib=32,
        stop_after_blocks=1,
    )
    store = CheckpointStore(workspace)
    record = json.loads((workspace / "checkpoints" / "0000000000.json").read_text())
    record["sequence"] = 1
    record["block_coord"] = [0, 1]
    record["col_range"] = [32, 64]
    record["plan_fingerprint"] = "different-plan"
    record.pop("record_checksum", None)
    with pytest.raises(BlockGemmError) as caught:
        store.publish_block(record, np.zeros((32, 32), dtype=np.float64))
    assert caught.value.code == E_STALE_CHECKPOINT
    assert caught.value.reason == "cross_plan_publish"


def test_two_processes_only_one_can_publish_next_sequence(manifest_path, tmp_path) -> None:
    loaded = load_case_for_test(manifest_path, "small_gemm")
    workspace = tmp_path / "race"
    run_gemm_job(
        workspace,
        loaded.a,
        loaded.b,
        expected=loaded.expected,
        l2_cache_kib=32,
        stop_after_blocks=1,
    )
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    processes = [
        context.Process(target=_publish_race_worker, args=(str(workspace), queue))
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    results = [queue.get(timeout=30) for _ in processes]
    for process in processes:
        process.join(timeout=30)
        assert process.exitcode == 0
    assert sorted(result[0] for result in results) == sorted(["ok", E_STALE_CHECKPOINT])


def test_corrupt_checkpoint_is_ignored_and_rebuilt(manifest_path, tmp_path) -> None:
    loaded = load_case_for_test(manifest_path, "small_gemm")
    baseline_workspace = tmp_path / "baseline"
    corrupt_workspace = tmp_path / "corrupt"
    baseline = run_gemm_job(
        baseline_workspace,
        loaded.a,
        loaded.b,
        expected=loaded.expected,
        l2_cache_kib=32,
    )
    run_gemm_job(
        corrupt_workspace,
        loaded.a,
        loaded.b,
        expected=loaded.expected,
        l2_cache_kib=32,
    )
    (corrupt_workspace / "checkpoints" / "0000000001.json").write_text("{", encoding="utf-8")
    resumed = run_gemm_job(
        corrupt_workspace,
        loaded.a,
        loaded.b,
        expected=loaded.expected,
        l2_cache_kib=32,
    )
    assert resumed.resume_decision.start_sequence == 1
    assert resumed.resume_decision.reason == "checkpoint_corrupt_or_missing"
    assert baseline.output_path.read_bytes() == resumed.output_path.read_bytes()
    assert baseline.summary_path.read_bytes() == resumed.summary_path.read_bytes()


def test_artifact_corruption_is_detected(manifest_path, tmp_path) -> None:
    loaded = load_case_for_test(manifest_path, "small_gemm")
    workspace = tmp_path / "artifact"
    run_gemm_job(workspace, loaded.a, loaded.b, expected=loaded.expected, l2_cache_kib=32)
    artifact = workspace / "blocks" / "0000000002.npy"
    artifact.write_bytes(artifact.read_bytes()[:128])
    result = run_gemm_job(workspace, loaded.a, loaded.b, expected=loaded.expected, l2_cache_kib=32)
    assert result.resume_decision.start_sequence == 2
    assert result.resume_decision.reason == "checkpoint_corrupt_or_missing"


def test_duplicate_sequence_is_ignored_and_rebuilt(manifest_path, tmp_path) -> None:
    loaded = load_case_for_test(manifest_path, "small_gemm")
    baseline_workspace = tmp_path / "baseline"
    duplicate_workspace = tmp_path / "duplicate"
    baseline = run_gemm_job(
        baseline_workspace,
        loaded.a,
        loaded.b,
        expected=loaded.expected,
        l2_cache_kib=32,
    )
    run_gemm_job(
        duplicate_workspace,
        loaded.a,
        loaded.b,
        expected=loaded.expected,
        l2_cache_kib=32,
    )
    first_record = (duplicate_workspace / "checkpoints" / "0000000000.json").read_bytes()
    (duplicate_workspace / "checkpoints" / "0000000001.json").write_bytes(first_record)
    rebuilt = run_gemm_job(
        duplicate_workspace,
        loaded.a,
        loaded.b,
        expected=loaded.expected,
        l2_cache_kib=32,
    )
    assert rebuilt.resume_decision.start_sequence == 0
    assert rebuilt.resume_decision.reason == "checkpoint_corrupt_or_missing"
    assert baseline.output_path.read_bytes() == rebuilt.output_path.read_bytes()
    assert baseline.summary_path.read_bytes() == rebuilt.summary_path.read_bytes()
