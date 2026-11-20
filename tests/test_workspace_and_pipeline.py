from __future__ import annotations

import pytest

from gsb.errors import WorkbenchError
from gsb.pipeline import Pipeline
from gsb.workspace import Workspace


def build_complete(workspace, dataset, config):
    workspace.import_dataset(dataset)
    workspace.set_config(config)
    pipeline = Pipeline(workspace, dataset)
    result = pipeline.run_all()
    assert result["status"]["overall"] == "complete"
    return pipeline, result


@pytest.mark.parametrize(
    ("field", "value", "earliest"),
    [
        ("bins", 7, "binning"),
        ("rounds", 5, "boosting"),
        ("learning_rate", 0.1, "boosting"),
        ("max_depth", 3, "boosting"),
        ("calibrator", "isotonic", "calibration"),
        ("calibration_folds", 3, "calibration"),
        ("calibration_seed", 8, "calibration"),
        ("diagnostic_bins", 6, "diagnostics"),
    ],
)
def test_invalidation_propagates_from_earliest_stage(
    make_workspace, small_dataset, fast_config, field, value, earliest
):
    workspace = make_workspace(f"invalidate_{field}")
    pipeline, _ = build_complete(workspace, small_dataset, fast_config)
    before = {
        stage: workspace.status()["stages"][stage]["version_fingerprint"]
        for stage in ("binning", "boosting", "calibration", "diagnostics")
    }
    changed = workspace.set_config({field: value})
    status = changed["status"]
    assert changed["earliest_invalid_stage"] == earliest
    stage_order = ["binning", "boosting", "calibration", "diagnostics"]
    for stage in stage_order[: stage_order.index(earliest)]:
        assert status["stages"][stage]["status"] == "valid"
        assert status["stages"][stage]["version_fingerprint"] == before[stage]
    for stage in stage_order[stage_order.index(earliest) :]:
        assert status["stages"][stage]["status"] == "invalid"
        assert "reasons" in status["stages"][stage]


def test_changed_calibrator_cannot_reuse_old_calibration(make_workspace, small_dataset, fast_config):
    workspace = make_workspace("stale_calibration")
    build_complete(workspace, small_dataset, fast_config)
    workspace.set_config({"calibrator": "isotonic"})
    with pytest.raises(WorkbenchError) as calibration_error:
        workspace.read_stage_payload("calibration")
    assert calibration_error.value.code == "E_INVALID_STAGE"
    with pytest.raises(WorkbenchError) as diagnostics_error:
        workspace.read_stage_payload("diagnostics")
    assert diagnostics_error.value.code == "E_INVALID_STAGE"


def test_incremental_calibrator_switch_matches_full_rebuild(make_workspace, small_dataset, fast_config):
    workspace = make_workspace("incremental_verify")
    pipeline, _ = build_complete(workspace, small_dataset, fast_config)
    upstream = {
        stage: workspace.status()["stages"][stage]["version_fingerprint"]
        for stage in ("binning", "boosting")
    }
    workspace.set_config({"calibrator": "isotonic"})
    incremental = Pipeline(workspace, small_dataset).run_all()
    assert incremental["status"]["overall"] == "complete"
    assert upstream == {
        stage: workspace.status()["stages"][stage]["version_fingerprint"]
        for stage in ("binning", "boosting")
    }
    verification = Pipeline(workspace, small_dataset).compare_with_full_rebuild()
    assert verification["differences"] == []
    assert verification["incremental_run_digest"] == verification["full_run_digest"]


def test_stop_after_round_then_resume_matches_from_scratch(make_workspace, small_dataset, fast_config):
    reference_workspace = make_workspace("resume_reference")
    _, reference = build_complete(reference_workspace, small_dataset, fast_config)

    workspace = make_workspace("resume_target")
    workspace.import_dataset(small_dataset)
    workspace.set_config(fast_config)
    stopped = Pipeline(workspace, small_dataset, stop_after_rounds=2).run_all()
    assert stopped["stopped"] is True
    assert stopped["status"]["overall"] == "stopped"
    progress = workspace.read_progress("boosting")
    assert progress["rounds_done"] == 2
    assert workspace.status()["stages"]["boosting"]["status"] == "invalid"
    resumed = Pipeline(workspace, small_dataset).run_all(resume=True)
    assert resumed["status"]["overall"] == "complete"
    assert resumed["run_digest"] == reference["run_digest"]


def test_pending_stop_before_train_returns_stopped(make_workspace, small_dataset, fast_config):
    workspace = make_workspace("pending_stop")
    workspace.import_dataset(small_dataset)
    workspace.set_config(fast_config)
    workspace.request_stop("test_pending")
    result = Pipeline(workspace, small_dataset).run_all()
    assert result["stopped"] is True
    assert result["status"]["overall"] == "stopped"


def test_stop_inside_calibration_fold_then_resume_matches(make_workspace, small_dataset, fast_config):
    reference_workspace = make_workspace("fold_resume_reference")
    _, reference = build_complete(reference_workspace, small_dataset, fast_config)

    workspace = make_workspace("fold_resume_target")
    workspace.import_dataset(small_dataset)
    workspace.set_config(fast_config)
    stopped = Pipeline(workspace, small_dataset, stop_after_rounds=6).run_all()
    assert stopped["stopped"] is True
    assert stopped["stopped_stage"] == "calibration"
    progress = workspace.read_progress("calibration")
    assert progress["fold_index"] == 0
    assert progress["model"]["rounds_done"] > 0
    resumed = Pipeline(workspace, small_dataset).run_all(resume=True)
    assert resumed["run_digest"] == reference["run_digest"]


def test_publish_sequence_and_prerequisite_errors(make_workspace, small_dataset, fast_config):
    workspace = make_workspace("publish_errors")
    workspace.import_dataset(small_dataset)
    workspace.set_config(fast_config)
    with pytest.raises(WorkbenchError) as missing:
        workspace.publish_checkpoint(
            stage="calibration",
            input_fingerprint="x",
            upstream_fingerprint="y",
            config_version="z",
            payload={"bad": True},
            accumulator={},
        )
    assert missing.value.code == "E_INVALID_STAGE"

    Pipeline(workspace, small_dataset).run_all(until_stage="binning")
    with pytest.raises(WorkbenchError) as stale:
        workspace.publish_checkpoint(
            stage="boosting",
            input_fingerprint="x",
            upstream_fingerprint="deliberately-wrong",
            config_version="z",
            payload={"bad": True},
            accumulator={},
        )
    assert stale.value.code == "E_STALE_CHECKPOINT"

    with pytest.raises(WorkbenchError) as old_sequence:
        workspace.publish_checkpoint(
            stage="boosting",
            input_fingerprint="x",
            upstream_fingerprint="y",
            config_version="z",
            payload={"another": True},
            accumulator={},
            expected_seq=1,
        )
    assert old_sequence.value.code == "E_STALE_CHECKPOINT"


def test_importing_new_sample_content_invalidates_all(make_workspace, small_dataset, medium_dataset, fast_config):
    workspace = make_workspace("data_change")
    build_complete(workspace, small_dataset, fast_config)
    workspace.import_dataset(medium_dataset)
    status = workspace.status()
    assert status["earliest_invalid_stage"] == "binning"
    assert all(status["stages"][stage]["status"] == "invalid" for stage in status["stages"])
    assert any(reason["code"] == "sample_content_changed" for reason in status["stages"]["binning"]["reasons"])


def test_export_and_diagnostics_are_byte_deterministic(make_workspace, small_dataset, fast_config, project_root):
    workspace = make_workspace("exports")
    pipeline, _ = build_complete(workspace, small_dataset, fast_config)
    first = pipeline.write_exports(project_root / "out" / "export_a")
    second = pipeline.write_exports(project_root / "out" / "export_b")
    assert first["probabilities_sha256"] == second["probabilities_sha256"]
    assert first["report_sha256"] == second["report_sha256"]
    assert first["run_digest"] == second["run_digest"]
    _, diagnostics = workspace.read_stage_payload("diagnostics")
    precision = diagnostics["precision"]
    assert precision["observed_log_loss_error"] <= precision["log_loss_error_upper_bound"] + 1e-18
    assert precision["observed_brier_error"] <= precision["brier_error_upper_bound"] + 1e-18
