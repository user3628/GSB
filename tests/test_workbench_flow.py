from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import numpy as np
import pytest

from conv_workbench.engine import Workbench, request_stop
from conv_workbench.errors import InvalidStageError


def _read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _set_wide_ranges(workspace: Path, shard_id: str, start: int = 0) -> None:
    path = workspace / "expectations.json"
    document = _read_json(path)
    stages = document["shards"][shard_id]["stages"]
    for stage in stages[start:]:
        stage["range"] = [-1000.0, 1000.0]
        stage["tolerance"] = 1e-6
    _write_json(path, document)


def _assert_complete(wb: Workbench, expected_shards: int = 3) -> None:
    status = wb.status()
    assert len(status["shards"]) == expected_shards
    for shard in status["shards"].values():
        assert shard["complete"] is True
        seqs = [stage["checkpoint"]["checkpoint_seq"] for stage in shard["stages"]]
        assert seqs == list(range(1, len(shard["stages"]) + 1))


def test_full_run_status_and_fixed_digests(demo_workspace: Path, tmp_path: Path):
    workbench = Workbench(demo_workspace, tmp_path / "run")
    first = workbench.run()
    assert first["status"] == "complete"
    assert set(first["final_digests"]) == {"small_odd_pad", "medium_odd_dilated", "boundary_chain"}
    _assert_complete(workbench)
    trace = _read_json(workbench.run_dir / "outputs" / "trace.json")
    for shard_id, info in first["outputs"].items():
        output = np.load(workbench.run_dir / info["path"], allow_pickle=False)
        assert list(output.shape) == info["shape"]
        assert trace["shards"][shard_id]["final_output"]["artifact_digest"] == info["artifact_digest"]

    # A second invocation reuses every checkpoint and produces identical bytes.
    journal_before = {
        shard_id: (workbench.run_dir / "checkpoints" / shard_id / "checkpoints.jsonl").read_bytes()
        for shard_id in first["outputs"]
    }
    second = workbench.run()
    assert second["executed_stages"] == 0
    assert second["final_digests"] == first["final_digests"]
    for shard_id, content in journal_before.items():
        assert (workbench.run_dir / "checkpoints" / shard_id / "checkpoints.jsonl").read_bytes() == content


def test_stop_then_resume_is_byte_identical_to_clean_run(demo_workspace: Path, tmp_path: Path):
    interrupted = Workbench(demo_workspace, tmp_path / "interrupted")
    stopped = interrupted.run(stop_after=2)
    assert stopped["status"] == "stopped"
    assert stopped["executed_stages"] == 2
    resumed = interrupted.run()
    assert resumed["status"] == "complete"
    assert resumed["executed_stages"] == 7
    _assert_complete(interrupted)

    clean = Workbench(demo_workspace, tmp_path / "clean")
    clean.run()
    for shard_id in resumed["outputs"]:
        assert (interrupted.run_dir / "outputs" / f"{shard_id}.npy").read_bytes() == (
            clean.run_dir / "outputs" / f"{shard_id}.npy"
        ).read_bytes()
        assert (interrupted.run_dir / "checkpoints" / shard_id / "checkpoints.jsonl").read_bytes() == (
            clean.run_dir / "checkpoints" / shard_id / "checkpoints.jsonl"
        ).read_bytes()
    assert (interrupted.run_dir / "outputs" / "trace.json").read_bytes() == (
        clean.run_dir / "outputs" / "trace.json"
    ).read_bytes()


def test_external_stop_request_completes_stage_and_then_resumes(demo_workspace: Path, tmp_path: Path):
    run_dir = tmp_path / "external_stop"
    workbench = Workbench(demo_workspace, run_dir)
    result_holder: dict[str, object] = {}
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            result_holder["result"] = workbench.run(
                shard_ids=["small_odd_pad"],
                delay_between_stages=0.25,
                install_signal_handlers=False,
            )
        except BaseException as exc:  # surfaced below
            errors.append(exc)

    thread = threading.Thread(target=worker)
    thread.start()
    journal = run_dir / "checkpoints" / "small_odd_pad" / "checkpoints.jsonl"
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline and not journal.is_file():
        time.sleep(0.01)
    request_stop(run_dir, "pytest external stop")
    thread.join(timeout=5.0)
    assert not thread.is_alive()
    assert not errors
    stopped = result_holder["result"]
    assert stopped["status"] == "stopped"
    assert stopped["report"]["reason"] == "stop_request"
    assert stopped["report"]["stop_request"]["reason"] == "pytest external stop"
    assert not (run_dir / "STOP_REQUEST.json").exists()
    assert stopped["executed_stages"] <= 2

    resumed = workbench.run(shard_ids=["small_odd_pad"])
    assert resumed["status"] == "complete"
    clean = Workbench(demo_workspace, tmp_path / "external_stop_clean")
    clean.run(shard_ids=["small_odd_pad"])
    assert (run_dir / "outputs" / "small_odd_pad.npy").read_bytes() == (
        clean.run_dir / "outputs" / "small_odd_pad.npy"
    ).read_bytes()


def test_parameter_change_invalidates_earliest_stage_and_rebuilds_downstream(
    copied_demo_workspace: Path, tmp_path: Path
):
    workbench = Workbench(copied_demo_workspace, tmp_path / "run")
    baseline = workbench.run()
    baseline_status = workbench.status("medium_odd_dilated")["shards"]["medium_odd_dilated"]
    stage0_before = baseline_status["stages"][0]["checkpoint"]

    manifest_path = copied_demo_workspace / "manifest.json"
    manifest = _read_json(manifest_path)
    medium = next(shard for shard in manifest["shards"] if shard["id"] == "medium_odd_dilated")
    changed = medium["operators"][1]["params"]
    changed["mode"] = "bilinear"
    changed["align_corners"] = False
    _write_json(manifest_path, manifest)
    _set_wide_ranges(copied_demo_workspace, "medium_odd_dilated", start=1)

    result = workbench.run(shard_ids=["medium_odd_dilated"])
    assert result["status"] == "complete"
    assert result["reused_stages"]["medium_odd_dilated"] == [0]
    assert len(result["invalidations"]) == 1
    event = result["invalidations"][0]
    assert event["earliest_invalid_stage_index"] == 1
    assert event["reason"] in {"alignment_mode_changed", "interpolation_parameters_changed"}
    assert event["affected_stages"] == [1, 2]
    assert event["old_op_fingerprint"] != event["new_op_fingerprint"]
    assert event["old_chain_prefix"] != event["new_chain_prefix"]
    assert result["final_digests"]["medium_odd_dilated"] != baseline["final_digests"]["medium_odd_dilated"]

    after = workbench.status("medium_odd_dilated")["shards"]["medium_odd_dilated"]
    assert after["stages"][0]["checkpoint"] == stage0_before
    assert [stage["checkpoint"]["checkpoint_seq"] for stage in after["stages"]] == [1, 2, 3]
    assert after["invalidations"][-1]["earliest_invalid_stage_index"] == 1


def test_tensor_content_change_invalidates_stage_zero(copied_demo_workspace: Path, tmp_path: Path):
    workbench = Workbench(copied_demo_workspace, tmp_path / "tensor_change")
    workbench.run(shard_ids=["boundary_chain"])
    tensor_path = copied_demo_workspace / "tensors" / "boundary_chain.npy"
    tensor = np.load(tensor_path, allow_pickle=False)
    np.save(tensor_path, tensor + 0.125, allow_pickle=False)
    _set_wide_ranges(copied_demo_workspace, "boundary_chain", start=0)
    result = workbench.run(shard_ids=["boundary_chain"])
    assert result["reused_stages"]["boundary_chain"] == []
    assert result["invalidations"][0]["earliest_invalid_stage_index"] == 0
    assert result["invalidations"][0]["reason"] == "source_tensor_changed"


def test_stage_request_without_predecessor_is_invalid(demo_workspace: Path, tmp_path: Path):
    workbench = Workbench(demo_workspace, tmp_path / "missing_predecessor")
    with pytest.raises(InvalidStageError) as raised:
        workbench.execute_stage("medium_odd_dilated", 2)
    assert raised.value.detail["operator_index"] == 2
    assert raised.value.detail["required_operator_index"] == 1
    assert not (workbench.run_dir / "checkpoints" / "medium_odd_dilated" / "checkpoints.jsonl").exists()

    first = workbench.execute_stage("medium_odd_dilated", 0)
    assert first["executed"] is True
    second = workbench.execute_stage("medium_odd_dilated", 1)
    assert second["executed"] is True
    third = workbench.execute_stage("medium_odd_dilated", 2)
    assert third["executed"] is True
    reused = workbench.execute_stage("medium_odd_dilated", 1)
    assert reused["executed"] is False

