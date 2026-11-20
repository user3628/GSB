from __future__ import annotations

import hashlib
import json
from pathlib import Path


from qoplib.audit import export_audit_report, query_fingerprints, query_status
from qoplib.errors import E_NUMERIC_INPUT
from qoplib.runner import request_stop, run


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _artifact_hashes(run_dir: Path):
    return {
        str(path.relative_to(run_dir)): _hash(path)
        for path in run_dir.rglob("*")
        if path.is_file() and ".lock" not in path.name
    }


def test_parallel_and_serial_are_byte_identical(demo_data, tmp_path):
    serial_dir = tmp_path / "serial"
    parallel_dir = tmp_path / "parallel"
    manifest = demo_data / "manifest.yaml"

    serial = run(manifest, demo_data, serial_dir, serial=True)
    parallel = run(manifest, demo_data, parallel_dir, workers=4)

    assert json.dumps(serial.summary, sort_keys=True) == \
        json.dumps(parallel.summary, sort_keys=True)
    assert _artifact_hashes(serial_dir) == _artifact_hashes(parallel_dir)


def test_reduction_order_follows_shard_index(demo_data, tmp_path):
    outcome = run(demo_data / "manifest.yaml", demo_data,
                  tmp_path / "run", workers=2)
    indices = [shard["index"] for shard in outcome.summary["shards"]]
    assert indices == sorted(indices)
    assert outcome.summary["reduction_order"] == [
        shard["shard_id"] for shard in outcome.summary["shards"]]


def test_numeric_input_error_is_structured(demo_data, tmp_path):
    outcome = run(demo_data / "manifest.yaml", demo_data,
                  tmp_path / "run", serial=True)
    codes = [error["code"] for error in outcome.summary["errors"]]
    assert E_NUMERIC_INPUT in codes
    bad = next(shard for shard in outcome.summary["shards"]
               if shard.get("error", {}).get("code") == E_NUMERIC_INPUT)
    assert bad["error"]["shard_id"] == "shard_03"
    assert bad["error"]["stage"] == "validate"


def test_stop_then_resume_matches_clean_run(demo_data, tmp_path):
    clean_dir = tmp_path / "clean"
    stop_dir = tmp_path / "stopped"
    manifest = demo_data / "manifest.yaml"
    clean = run(manifest, demo_data, clean_dir, serial=True)

    request_stop(stop_dir)
    stopped = run(manifest, demo_data, stop_dir, serial=True)
    assert stopped.stopped is True
    (stop_dir / "STOP").unlink()

    resumed = run(manifest, demo_data, stop_dir, serial=True)
    assert resumed.stopped is False
    assert json.dumps(clean.summary, sort_keys=True) == \
        json.dumps(resumed.summary, sort_keys=True)
    assert _artifact_hashes(clean_dir) == _artifact_hashes(stop_dir)


def test_resume_reuses_completed_shards(demo_data, tmp_path):
    run_dir = tmp_path / "run"
    manifest = demo_data / "manifest.yaml"
    request_stop(run_dir)
    run(manifest, demo_data, run_dir, serial=True)
    (run_dir / "STOP").unlink()
    outcome = run(manifest, demo_data, run_dir, serial=True)
    assert outcome.summary["totals"]["shards_completed"] == 3
    status = query_status(run_dir)
    assert status["latest_sequence"] == 12
    assert len(query_fingerprints(run_dir)) == 13


def test_audit_report_export(demo_data, tmp_path):
    run_dir = tmp_path / "run"
    run(demo_data / "manifest.yaml", demo_data, run_dir, serial=True)
    report_path = export_audit_report(run_dir)
    report = json.loads(report_path.read_text())
    assert report["report_fingerprint"]
    stages = {(row["shard_id"], row["stage"]) for row in
              report["stage_states"] if row["present"]}
    assert ("shard_00", "verify") in stages
    assert ("shard_03", "verify") not in stages


def test_tight_budget_records_unprocessed_shards(demo_data, tmp_path):
    outcome = run(demo_data / "manifest_tight.yaml", demo_data,
                  tmp_path / "run", serial=True)
    rejected = [shard for shard in outcome.summary["shards"]
                if shard["status"] == "rejected"]
    assert rejected
    assert "budget_detail" in rejected[0]
    assert outcome.summary["totals"]["shards_rejected"] == len(rejected)


def test_rerun_is_idempotent_and_byte_stable(demo_data, tmp_path):
    run_dir = tmp_path / "run"
    manifest = demo_data / "manifest.yaml"
    run(manifest, demo_data, run_dir, serial=True)
    first = _artifact_hashes(run_dir)
    run(manifest, demo_data, run_dir, serial=True)
    second = _artifact_hashes(run_dir)
    assert first == second
