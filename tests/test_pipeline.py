from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from audioq.canonical import atomic_write_json
from audioq.errors import AudioQError
from audioq.pipeline import EvaluationWorkbench


def _workbench(workspace, state: str, config: Path | None = None, manifest: Path | None = None):
    root = workspace["root"]
    return EvaluationWorkbench(
        root,
        manifest or workspace["manifest"],
        config or workspace["config"],
        state_dir=state,
    )


def _read_report(wb: EvaluationWorkbench, state: str) -> bytes:
    return (wb.root / state / "report.json").read_bytes()


def test_full_run_is_deterministic_and_reports_all_error_classes(workspace):
    first = _workbench(workspace, "state_det_a")
    second = _workbench(workspace, "state_det_b")
    first_result = first.run()
    second_result = second.run()
    assert first_result["status"] == second_result["status"] == "complete"
    assert _read_report(first, "state_det_a") == _read_report(second, "state_det_b")
    report = first_result["report"]
    assert report["summary"]["shard_count"] == 4
    assert report["summary"]["error_codes"] == ["E_CLIPPED_AUDIO", "E_EMPTY_AUDIO", "E_INVALID_WEIGHT"]
    by_id = {shard["shard_id"]: shard for shard in report["shards"]}
    assert by_id["clean"]["fusion"]["fused_score"] > 4.0
    assert by_id["low_snr"]["fusion"]["fused_score"] < 2.5
    assert by_id["clipped"]["errors"][0]["code"] == "E_CLIPPED_AUDIO"
    assert "E_EMPTY_AUDIO" in [item["code"] for item in by_id["silent"]["errors"]]
    assert by_id["silent"]["metrics"]["scores"]["snr"] is None


def test_stop_after_shards_then_resume_is_byte_identical(workspace):
    baseline = _workbench(workspace, "state_resume_baseline")
    baseline.run()
    interrupted = _workbench(workspace, "state_resume_test")
    stopped = interrupted.run(stop_after_shards=2, write_report=False)
    assert stopped["status"] == "stopped"
    assert stopped["completed_count"] == 2
    assert stopped["report"] is None
    assert not (interrupted.root / "state_resume_test/report.json").exists()
    interrupted.request_stop(reason="pytest-stop")
    resumed = interrupted.resume()
    assert resumed["status"] == "complete"
    assert _read_report(baseline, "state_resume_baseline") == _read_report(interrupted, "state_resume_test")


def test_corrupt_checkpoint_is_quarantined_and_resume_matches_full(workspace):
    baseline = _workbench(workspace, "state_corrupt_baseline")
    baseline.run()
    interrupted = _workbench(workspace, "state_corrupt")
    interrupted.run(stop_after_shards=1, write_report=False)
    corrupt = interrupted.root / "state_corrupt/checkpoints/clean/00000002-metrics.json"
    corrupt.write_text('{"record_version": 1, "half":', encoding="utf-8")
    resumed = interrupted.resume()
    assert resumed["status"] == "complete"
    assert _read_report(baseline, "state_corrupt_baseline") == _read_report(interrupted, "state_corrupt")
    scan = interrupted.checkpoints.scan_shard("clean", repair=False)
    assert scan.last_seq == 4
    assert list((interrupted.root / "state_corrupt/quarantine/clean").glob("*00000002-metrics.json*.bad"))


def test_length_and_sample_rate_mismatch_are_kept_as_diagnostics(workspace):
    root = workspace["root"]
    cases = root / "test_cases"
    cases.mkdir(exist_ok=True)
    raw = json.loads((root / workspace["manifest"]).read_text(encoding="utf-8"))

    length_manifest = dict(raw)
    length_manifest["shards"] = [dict(item) for item in raw["shards"]]
    length_manifest["shards"][0]["num_samples"] += 7
    length_path = cases / "length_mismatch.json"
    atomic_write_json(length_path, length_manifest, pretty=True)
    wb = EvaluationWorkbench(root, length_path.relative_to(root), workspace["config"], state_dir="state_length")
    report = wb.run()["report"]
    clean = next(item for item in report["shards"] if item["shard_id"] == "clean")
    error = next(item for item in clean["errors"] if item["code"] == "E_LENGTH_MISMATCH")
    assert error["observed"]["difference"] == -7

    rate_manifest = dict(raw)
    rate_manifest["shards"] = [dict(item) for item in raw["shards"]]
    rate_manifest["shards"][1]["sample_rate"] = 8000
    rate_path = cases / "rate_mismatch.json"
    atomic_write_json(rate_path, rate_manifest, pretty=True)
    wb = EvaluationWorkbench(root, rate_path.relative_to(root), workspace["config"], state_dir="state_rate")
    report = wb.run()["report"]
    low = next(item for item in report["shards"] if item["shard_id"] == "low_snr")
    error = next(item for item in low["errors"] if item["code"] == "E_SAMPLE_RATE")
    assert error["observed"]["actual_sample_rate"] == 16000
    assert error["observed"]["declared_sample_rate"] == 8000


def test_weight_change_invalidates_fusion_and_reuses_upstream(workspace):
    root = workspace["root"]
    cases = root / "test_cases"
    cases.mkdir(exist_ok=True)
    config_source = json.loads((root / workspace["config"]).read_text(encoding="utf-8"))
    config_path = cases / "weights_changing.json"
    atomic_write_json(config_path, config_source, pretty=True)
    state = "state_invalidation"
    wb = EvaluationWorkbench(root, workspace["manifest"], config_path.relative_to(root), state_dir=state)
    first = wb.run()
    first_report = (root / state / "report.json").read_bytes()
    before_clean = wb.checkpoints.latest_by_stage("clean")
    assert [before_clean[stage]["seq"] for stage in ("frame", "metrics", "fusion", "diagnostics")] == [1, 2, 3, 4]

    changed = json.loads(json.dumps(config_source))
    changed["weights"] = {"snr": 0.5, "spectral": 0.3, "clipping": 0.2}
    atomic_write_json(config_path, changed, pretty=True)
    changed_wb = EvaluationWorkbench(root, workspace["manifest"], config_path.relative_to(root), state_dir=state)
    status = changed_wb.status()
    affected = {item["shard_id"] for item in status["invalidations"]["events"]}
    assert affected == {"clean", "low_snr", "clipped", "silent"}
    clean_status = next(item for item in status["shards"] if item["shard_id"] == "clean")
    assert clean_status["earliest_invalid_stage"] == "fusion"
    assert "metric_weights_changed" in clean_status["invalidation_reasons"]
    assert clean_status["stages"]["frame"]["state"] == "valid"
    assert clean_status["stages"]["metrics"]["state"] == "valid"
    assert clean_status["stages"]["fusion"]["state"] == "stale"
    with pytest.raises(AudioQError) as caught:
        changed_wb.build_report()
    assert caught.value.code == "E_STALE_CHECKPOINT"

    rebuilt = changed_wb.run()
    after_clean = changed_wb.checkpoints.latest_by_stage("clean")
    assert after_clean["frame"]["seq"] == 1
    assert after_clean["metrics"]["seq"] == 2
    assert after_clean["fusion"]["seq"] == 5
    assert after_clean["diagnostics"]["seq"] == 6
    assert (root / state / "report.json").read_bytes() != first_report
    assert rebuilt["report"]["report_fingerprint"] != first["report"]["report_fingerprint"]
    settled = changed_wb.status()
    assert settled["invalidations"]["events"] == []
    last = settled["last_persisted_invalidation"]
    assert last is not None
    assert {item["shard_id"] for item in last["events"]} == affected

    # Reverting to the first configuration creates fresh downstream evidence;
    # it must not return the historical seq 3/4 artifacts idempotently.
    atomic_write_json(config_path, config_source, pretty=True)
    reverted_wb = EvaluationWorkbench(root, workspace["manifest"], config_path.relative_to(root), state_dir=state)
    reverted_wb.run()
    reverted_clean = reverted_wb.checkpoints.latest_by_stage("clean")
    assert reverted_clean["frame"]["seq"] == 1
    assert reverted_clean["metrics"]["seq"] == 2
    assert reverted_clean["fusion"]["seq"] == 7
    assert reverted_clean["diagnostics"]["seq"] == 8


def test_reference_change_invalidates_fusion_and_schema_change_is_observable(workspace):
    root = workspace["root"]
    cases = root / "test_cases"
    cases.mkdir(exist_ok=True)
    manifest = json.loads((root / workspace["manifest"]).read_text(encoding="utf-8"))
    state = "state_reference"
    wb = EvaluationWorkbench(root, workspace["manifest"], workspace["config"], state_dir=state)
    wb.run()

    changed_manifest = json.loads(json.dumps(manifest))
    changed_manifest["shards"][0]["reference"]["target_mos"] = 4.9
    manifest_path = cases / "reference_changed.json"
    atomic_write_json(manifest_path, changed_manifest, pretty=True)
    changed = EvaluationWorkbench(root, manifest_path.relative_to(root), workspace["config"], state_dir=state)
    status = changed.status()
    clean = next(item for item in status["shards"] if item["shard_id"] == "clean")
    assert clean["earliest_invalid_stage"] == "fusion"
    assert "reference_interval_changed" in clean["invalidation_reasons"]
    low = next(item for item in status["shards"] if item["shard_id"] == "low_snr")
    assert low["earliest_invalid_stage"] is None

    # A structure-version-only change invalidates fusion/diagnostics from the semantic model.
    config = json.loads((root / workspace["config"]).read_text(encoding="utf-8"))
    config["schema_version"] = 1
    config["weights_percent"] = {
        "snr": int(round(config["weights"]["snr"] * 100)),
        "spectral": int(round(config["weights"]["spectral"] * 100)),
        "clipping": int(round(config["weights"]["clipping"] * 100)),
    }
    config["metrics"].pop("snr_threshold_db", None)
    config["downgrade_restore"] = {"metrics.snr_threshold_db": 8.0}
    del config["weights"]
    v1_path = cases / "v1_changed.json"
    atomic_write_json(v1_path, config, pretty=True)
    v1_wb = EvaluationWorkbench(root, workspace["manifest"], v1_path.relative_to(root), state_dir="state_v1")
    v1_wb.run()
    v2_wb = EvaluationWorkbench(root, workspace["manifest"], workspace["config"], state_dir="state_v1")
    version_status = v2_wb.status()
    clean_v1 = next(item for item in version_status["shards"] if item["shard_id"] == "clean")
    assert clean_v1["earliest_invalid_stage"] == "fusion"
    assert "structure_version_changed" in clean_v1["invalidation_reasons"]


def test_stop_request_before_run_and_resume(workspace):
    wb = _workbench(workspace, "state_stop_marker")
    wb.request_stop(reason="before-run")
    stopped = wb.run(write_report=False)
    assert stopped["status"] == "stopped"
    assert stopped["completed_count"] == 0
    assert stopped["stop_reason"] == "before-run"
    resumed = wb.resume()
    assert resumed["status"] == "complete"
    assert not wb.checkpoints.stop_requested()


def test_zero_sample_wav_is_empty_audio_even_with_zero_declared_count(workspace):
    import wave

    root = workspace["root"]
    cases = root / "test_cases"
    cases.mkdir(exist_ok=True)
    empty_wav = cases / "zero_samples.wav"
    with wave.open(str(empty_wav), "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(16000)
        wav.writeframes(b"")
    raw = json.loads((root / workspace["manifest"]).read_text(encoding="utf-8"))
    manifest = dict(raw)
    manifest["shards"] = [dict(item) for item in raw["shards"]]
    manifest["shards"][0] = dict(manifest["shards"][0])
    manifest["shards"][0]["path"] = empty_wav.relative_to(root).as_posix()
    manifest["shards"][0]["num_samples"] = 0
    manifest_path = cases / "zero_manifest.json"
    atomic_write_json(manifest_path, manifest, pretty=True)
    wb = EvaluationWorkbench(root, manifest_path.relative_to(root), workspace["config"], state_dir="state_zero")
    report = wb.run()["report"]
    clean = next(item for item in report["shards"] if item["shard_id"] == "clean")
    assert "E_EMPTY_AUDIO" in [item["code"] for item in clean["errors"]]


def test_stale_report_file_is_removed_before_a_changed_stopped_run(workspace):
    root = workspace["root"]
    cases = root / "test_cases"
    cases.mkdir(exist_ok=True)
    source = json.loads((root / workspace["config"]).read_text(encoding="utf-8"))
    config_path = cases / "report_invalidation.json"
    atomic_write_json(config_path, source, pretty=True)
    state = "state_stale_report"
    full = EvaluationWorkbench(root, workspace["manifest"], config_path.relative_to(root), state_dir=state)
    full.run()
    report_path = root / state / "report.json"
    assert report_path.exists()

    changed = json.loads(json.dumps(source))
    changed["weights"] = {"snr": 0.5, "spectral": 0.3, "clipping": 0.2}
    atomic_write_json(config_path, changed, pretty=True)
    stopped = EvaluationWorkbench(root, workspace["manifest"], config_path.relative_to(root), state_dir=state)
    result = stopped.run(stop_after_shards=1, write_report=False)
    assert result["status"] == "stopped"
    assert not report_path.exists()
    resumed = stopped.resume()
    assert resumed["status"] == "complete"
    assert report_path.exists()
