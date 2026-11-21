from __future__ import annotations

import json
import shutil
from pathlib import Path

import numpy as np
import pytest

from chunkops.errors import (
    ChunkOpsError,
    E_INVALID_STATE,
    E_MEMORY_BUDGET,
    E_MISSING_FRAGMENT,
    E_STALE_VERSION,
    E_ZERO_ROW,
)
from chunkops.fingerprint import sha256_file
from chunkops.pipeline import Pipeline, import_fragment, load_output


def _import(
    workspace: Path,
    fragment: str,
    *,
    op: str = "softmax",
    axis: int = 1,
    memory_budget_bytes: int = 512 * 1024 * 1024,
    reject_zero_rows: bool = False,
    reject_nonfinite: bool = False,
) -> dict:
    return import_fragment(
        workspace,
        fragment,
        op=op,
        axis=axis,
        fragment_id=Path(fragment).stem,
        memory_budget_bytes=memory_budget_bytes,
        reject_zero_rows=reject_zero_rows,
        reject_nonfinite=reject_nonfinite,
    )


def test_pipeline_four_stages_and_summary(
    tmp_path: Path, fragment_paths: dict[str, str]
) -> None:
    workspace = tmp_path / "full"
    _import(workspace, fragment_paths["small_base"])
    pipeline = Pipeline(workspace)
    result = pipeline.run()
    assert result["state"] == "complete"
    assert result["latest_stage"] == "summary"
    assert all(stage["completed"] for stage in result["stages"].values())
    assert result["error_report_status"] == "ok"
    summary = pipeline.summary()
    assert [item["stage"] for item in summary["stage_chain"]] == ["plan", "reduce", "validate", "summary"]
    report = pipeline.error_report()
    assert report["status"] == "ok"
    assert report["error_estimates"]
    assert len(pipeline.checkpoints.discover()["valid_checkpoints"]) == 4
    assert pipeline.plan_versions.inspect()["latest_seq"] == 1
    assert pipeline.result_versions.inspect()["latest_seq"] == 1
    output = load_output(workspace)
    assert output.shape == (128, 512)
    assert np.isfinite(output).all()


def test_stop_after_stage_and_resume_is_byte_identical(
    tmp_path: Path, fragment_paths: dict[str, str]
) -> None:
    full_workspace = tmp_path / "full"
    recovered_workspace = tmp_path / "recovered"
    _import(full_workspace, fragment_paths["small_base"])
    Pipeline(full_workspace).run()
    _import(recovered_workspace, fragment_paths["small_base"])
    pipeline = Pipeline(recovered_workspace)
    stopped = pipeline.run(stop_after_stage="plan")
    assert stopped["state"] == "stopped"
    assert stopped["latest_stage"] == "plan"
    assert stopped["next_stage"] == "reduce"
    assert stopped["stop_requested"] is True
    resumed = pipeline.resume()
    assert resumed["state"] == "complete"
    assert resumed["stop_requested"] is False
    full_sha = sha256_file(full_workspace / "outputs" / "output.npy")
    resumed_sha = sha256_file(recovered_workspace / "outputs" / "output.npy")
    assert full_sha == resumed_sha
    assert Path(full_workspace / "outputs" / "output.npy").read_bytes() == Path(
        recovered_workspace / "outputs" / "output.npy"
    ).read_bytes()


def test_incremental_reduction_uses_cache_and_keeps_bytes(
    tmp_path: Path, fragment_paths: dict[str, str]
) -> None:
    workspace = tmp_path / "cache_replay"
    _import(workspace, fragment_paths["small_base"])
    pipeline = Pipeline(workspace)
    pipeline.run()
    original_sha = sha256_file(workspace / "outputs" / "output.npy")
    original_summary = pipeline.summary()
    for path in (workspace / "checkpoints").glob("000[234]_*.json"):
        path.unlink()
    replayed = pipeline.run()
    assert replayed["state"] == "complete"
    assert sha256_file(workspace / "outputs" / "output.npy") == original_sha
    replaced_summary = pipeline.summary()
    assert replaced_summary["cache"]["cache_hit"] is True
    assert replaced_summary["cache"]["cache_key"] == original_summary["cache"]["cache_key"]


def test_missing_fragment_after_import(
    tmp_path: Path, fragment_paths: dict[str, str]
) -> None:
    fragment = tmp_path / "temporary.npz"
    shutil.copyfile(fragment_paths["small_base"], fragment)
    workspace = tmp_path / "missing"
    _import(workspace, str(fragment))
    fragment.unlink()
    with pytest.raises(ChunkOpsError) as caught:
        Pipeline(workspace).run()
    assert caught.value.code == E_MISSING_FRAGMENT
    assert caught.value.details["fragment_id"] == "temporary"
    assert "path" in caught.value.details


def test_shape_change_after_import_is_stale_version(
    tmp_path: Path, fragment_paths: dict[str, str]
) -> None:
    fragment = tmp_path / "changing.npz"
    shutil.copyfile(fragment_paths["small_base"], fragment)
    workspace = tmp_path / "changing_ws"
    _import(workspace, str(fragment))
    np.savez(fragment, x=np.zeros((64, 512), dtype=np.float32))
    with pytest.raises(ChunkOpsError) as caught:
        Pipeline(workspace).run()
    assert caught.value.code == E_STALE_VERSION
    assert caught.value.details["stale_reason"] == "shape_fingerprint_changed"


def test_value_change_after_import_is_stale_version(
    tmp_path: Path, fragment_paths: dict[str, str]
) -> None:
    fragment = tmp_path / "changing_values.npz"
    shutil.copyfile(fragment_paths["small_base"], fragment)
    workspace = tmp_path / "changing_values_ws"
    _import(workspace, str(fragment))
    x = np.load(fragment)["x"]
    np.savez(fragment, x=(x + np.float32(1.0)).astype(np.float32))
    with pytest.raises(ChunkOpsError) as caught:
        Pipeline(workspace).run()
    assert caught.value.code == E_STALE_VERSION
    assert caught.value.details["stale_reason"] == "value_fingerprint_changed"


def test_memory_budget_business_error(
    tmp_path: Path, fragment_paths: dict[str, str]
) -> None:
    workspace = tmp_path / "tiny_budget"
    _import(workspace, fragment_paths["small_base"], memory_budget_bytes=256 * 1024)
    with pytest.raises(ChunkOpsError) as caught:
        Pipeline(workspace).run()
    assert caught.value.code == E_MEMORY_BUDGET
    details = caught.value.details
    assert details["budget_bytes"] == 256 * 1024
    assert details["input_bytes"] > 0
    assert details["output_bytes"] > 0
    assert details["required_bytes"] > details["budget_bytes"]


def test_strict_zero_row_and_nonfinite_codes(
    tmp_path: Path, fragment_paths: dict[str, str]
) -> None:
    with pytest.raises(ChunkOpsError) as zero:
        _import(
            tmp_path / "zero",
            fragment_paths["small_anomaly"],
            reject_zero_rows=True,
        )
    assert zero.value.code == E_ZERO_ROW
    assert zero.value.details["zero_rows"]
    with pytest.raises(ChunkOpsError) as nonfinite:
        _import(
            tmp_path / "nonfinite",
            fragment_paths["small_anomaly"],
            reject_nonfinite=True,
        )
    assert nonfinite.value.code == "E_NONFINITE"
    assert nonfinite.value.details["first_nan"]


def test_invalid_axis_and_shape_codes(tmp_path: Path) -> None:
    one_dimensional = tmp_path / "bad.npz"
    np.savez(one_dimensional, x=np.zeros(8, dtype=np.float32))
    with pytest.raises(ChunkOpsError) as shape:
        _import(tmp_path / "bad_shape", str(one_dimensional))
    assert shape.value.code == "E_INVALID_SHAPE"
    with pytest.raises(ChunkOpsError) as axis:
        import_fragment(
            tmp_path / "bad_axis",
            str(one_dimensional),
            op="softmax",
            axis=0,
        )
    assert axis.value.code in {"E_INVALID_AXIS", "E_INVALID_SHAPE"}


def test_anomaly_pipeline_propagates_nonfinite_and_validates(
    tmp_path: Path, fragment_paths: dict[str, str]
) -> None:
    workspace = tmp_path / "anomaly"
    _import(workspace, fragment_paths["small_anomaly"], op="layernorm", axis=0)
    pipeline = Pipeline(workspace)
    status = pipeline.run()
    assert status["state"] == "complete"
    report = pipeline.error_report()
    assert report["status"] == "ok"
    assert report["input_nonfinite"]["nan_count"] > 0
    assert report["input_nonfinite"]["pos_inf_count"] > 0
    assert report["input_nonfinite"]["neg_inf_count"] > 0
    assert report["all_zero_rows"]
    assert any(block["nan_match_count"] for block in report["sample_blocks"])
    output = load_output(workspace)
    assert np.isnan(output).any()
    assert not np.isinf(output).any()


def test_run_refuses_tampered_version_chain(
    tmp_path: Path, fragment_paths: dict[str, str]
) -> None:
    from chunkops.storage import atomic_write_json, read_json

    workspace = tmp_path / "tampered_versions"
    _import(workspace, fragment_paths["small_base"])
    pipeline = Pipeline(workspace)
    pipeline.run()
    head = read_json(pipeline.plan_versions.head_path)
    record_path = pipeline.plan_versions.root / head["record"]
    record = read_json(record_path)
    record["payload"]["plan"]["selection_reason"] = "tampered"
    atomic_write_json(record_path, record)
    with pytest.raises(ChunkOpsError) as caught:
        pipeline.run()
    assert caught.value.code == E_STALE_VERSION
    assert caught.value.details["stale_reason"] == "tamper"


def test_corrupt_latest_checkpoint_recovers_from_consistent_prefix(
    tmp_path: Path, fragment_paths: dict[str, str]
) -> None:
    from chunkops.storage import atomic_write_json, read_json

    workspace = tmp_path / "checkpoint_recovery"
    _import(workspace, fragment_paths["small_base"])
    pipeline = Pipeline(workspace)
    pipeline.run()
    original_sha = sha256_file(workspace / "outputs" / "output.npy")
    path = workspace / "checkpoints" / "0003_validate.json"
    record = read_json(path)
    record["payload"]["tampered"] = True
    atomic_write_json(path, record)
    assert pipeline.status()["state"] == "invalid_checkpoint"
    recovered = pipeline.run()
    assert recovered["state"] == "complete"
    assert (workspace / "reports" / "checkpoint_recovery.json").exists()
    assert sha256_file(workspace / "outputs" / "output.npy") == original_sha
    assert len(pipeline.checkpoints.discover()["valid_checkpoints"]) == 4


def test_pipeline_numeric_mismatch_code_and_report(
    tmp_path: Path, fragment_paths: dict[str, str]
) -> None:
    workspace = tmp_path / "numeric_mismatch"
    import_fragment(
        workspace,
        fragment_paths["small_base"],
        op="softmax",
        axis=1,
        fragment_id="small_base",
        tolerance={"atol": 0.0, "rtol": 0.0},
    )
    pipeline = Pipeline(workspace)
    with pytest.raises(ChunkOpsError) as caught:
        pipeline.run()
    assert caught.value.code == "E_NUMERIC_MISMATCH"
    assert caught.value.details["failed_blocks"]
    report = pipeline.error_report()
    assert report["status"] == "mismatch"
    assert pipeline.status()["latest_stage"] == "reduce"
