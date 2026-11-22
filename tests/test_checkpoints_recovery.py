from __future__ import annotations

import multiprocessing
import shutil
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from cabin_voice.engine import Engine
from cabin_voice.errors import CabinVoiceError
from cabin_voice.storage import CheckpointLog, Workspace


def _record_body(run: dict[str, Any], position: int, expected: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "build_id": run["build_id"],
        "seq": position,
        "position": position,
        "chunk_id": expected["chunk_id"],
        "chunk_index": expected["chunk_index"],
        "stage": expected["stage"],
        "stage_index": expected["stage_index"],
        "content_fingerprint": "test",
        "input_fingerprint": "test",
        "config_version": "test",
        "edit_version": 0,
        "fingerprint_components": {},
        "artifact_summary": "test",
        "artifact": {},
    }


def _publish_worker(root: str, result_queue) -> None:
    try:
        workspace = Workspace(root)
        log = CheckpointLog(workspace)
        run = log.load_run()
        expected = run["plan"][0]
        log.publish(_record_body(run, 1, expected), expected_position=1)
        result_queue.put(("ok", None))
    except CabinVoiceError as exc:
        result_queue.put(("error", exc.code))
    except Exception as exc:  # pragma: no cover - diagnostic path
        result_queue.put(("unexpected", repr(exc)))


def test_stop_and_resume_matches_full(tmp_path: Path, workspace: Workspace) -> None:
    reference = Engine(workspace).build(mode="full")
    interrupted_workspace = Workspace(tmp_path / "interrupted").ensure()
    interrupted_workspace.import_bundle(workspace.manifest_path)
    interrupted = Engine(interrupted_workspace).build(mode="full", stop_after_units=5)
    assert interrupted["status"] == "stopped"
    assert interrupted["last_seq"] == 5
    assert not interrupted_workspace.final_path.exists()
    resumed = Engine(interrupted_workspace).build(mode="incremental", resume=True)
    assert resumed["status"] == "complete"
    assert resumed["last_seq"] == 12
    records = Engine(interrupted_workspace).checkpoints.load_consistent()["prefix"]
    assert [record["seq"] for record in records] == list(range(1, 13))
    assert workspace.read_final_bytes() == interrupted_workspace.read_final_bytes()
    assert reference["final_digest"] == resumed["final_digest"]


def test_recovery_ignores_corrupt_and_duplicate_records(tmp_path: Path, full_workspace: Workspace) -> None:
    interrupted_workspace = Workspace(tmp_path / "corrupt").ensure()
    interrupted_workspace.import_bundle(full_workspace.manifest_path)
    Engine(interrupted_workspace).build(mode="full", stop_after_units=7)
    checkpoint_dir = interrupted_workspace.checkpoint_dir
    (checkpoint_dir / "000000000003.json").write_text("{half-written", encoding="utf-8")
    shutil.copyfile(checkpoint_dir / "000000000001.json", checkpoint_dir / "duplicate-seq-one.json")
    scan = Engine(interrupted_workspace).checkpoints.load_consistent()
    assert scan["prefix"] == []
    assert {item["reason"] for item in scan["invalid"]} >= {"unreadable_json", "duplicate_seq"}
    resumed = Engine(interrupted_workspace).build(mode="incremental", resume=True)
    assert resumed["status"] == "complete"
    records = Engine(interrupted_workspace).checkpoints.load_consistent()["prefix"]
    assert [record["seq"] for record in records] == list(range(1, 13))
    assert full_workspace.read_final_bytes() == interrupted_workspace.read_final_bytes()


def test_concurrent_publishers_only_allow_contiguous_winner(tmp_path: Path, workspace: Workspace) -> None:
    concurrent = Workspace(tmp_path / "concurrent").ensure()
    concurrent.import_bundle(workspace.manifest_path)
    Engine(concurrent).build(mode="full", stop_after_units=0)
    context = multiprocessing.get_context("fork")
    queue = context.Queue()
    processes = [context.Process(target=_publish_worker, args=(str(concurrent.root), queue)) for _ in range(2)]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=20)
        assert process.exitcode == 0
    outcomes = sorted(queue.get(timeout=5) for _ in processes)
    assert outcomes == [("error", "E_STALE_CHECKPOINT"), ("ok", None)]
    scan = CheckpointLog(concurrent).load_consistent()
    assert len(scan["prefix"]) == 1
    log = CheckpointLog(concurrent)
    run = log.load_run()
    wrong = _record_body(run, 2, run["plan"][1])
    wrong["chunk_id"] = run["plan"][5]["chunk_id"]
    with pytest.raises(CabinVoiceError) as error:
        log.publish(wrong, expected_position=2)
    assert error.value.code == "E_STALE_CHECKPOINT"


def test_forced_kill_recovery_uses_only_persisted_records(tmp_path: Path, full_workspace: Workspace) -> None:
    interrupted = Workspace(tmp_path / "forced").ensure()
    interrupted.import_bundle(full_workspace.manifest_path)
    Engine(interrupted).build(mode="full", stop_after_units=9)
    # Simulate a process disappearing after atomic checkpoint writes but before finalisation.
    assert not interrupted.final_path.exists()
    resumed = Engine(interrupted).build(mode="incremental", resume=True)
    assert resumed["status"] == "complete"
    assert full_workspace.read_final_bytes() == interrupted.read_final_bytes()


def test_graceful_stop_flag_finishes_current_stage_and_resumes(
    tmp_path: Path, full_workspace: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    graceful = Workspace(tmp_path / "graceful").ensure()
    graceful.import_bundle(full_workspace.manifest_path)
    import cabin_voice.engine as engine_module

    original = engine_module.detect_chunk

    def slow_detect(*args, **kwargs):
        time.sleep(0.08)
        return original(*args, **kwargs)

    monkeypatch.setattr(engine_module, "detect_chunk", slow_detect)
    result_holder: dict[str, Any] = {}

    def run_build() -> None:
        result_holder["result"] = Engine(graceful).build(mode="full")

    thread = threading.Thread(target=run_build)
    thread.start()
    deadline = time.time() + 10
    while time.time() < deadline:
        scan = Engine(graceful).checkpoints.load_consistent()
        if scan["prefix"]:
            graceful.request_stop()
            break
        time.sleep(0.01)
    thread.join(timeout=20)
    assert not thread.is_alive()
    result = result_holder["result"]
    assert result["status"] == "stopped"
    assert 0 < result["last_seq"] < 12
    before_resume = result["last_seq"]
    # The unit in flight at stop-request time was completed and atomically published.
    scan = Engine(graceful).checkpoints.load_consistent()
    assert scan["prefix"][-1]["seq"] == before_resume
    resumed = Engine(graceful).build(mode="incremental", resume=True)
    assert resumed["status"] == "complete"
    assert full_workspace.read_final_bytes() == graceful.read_final_bytes()
