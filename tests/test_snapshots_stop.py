from __future__ import annotations

import json
from pathlib import Path

import pytest

from cabin_voice.errors import CabinError


def test_snapshot_chain_is_contiguous_and_atomic(ready_service) -> None:
    service = ready_service
    verification = service.verify_snapshots()
    assert verification["status"] == "verified"
    assert verification["seq"] == 3
    snapshot_files = sorted(service.workspace.snapshots.records_dir.glob("*.json"))
    assert [path.name for path in snapshot_files] == [
        "00000001-import.json",
        "00000002-vad.json",
        "00000003-arbitrate.json",
        "head.json",
    ]
    assert not list(service.workspace.snapshots.records_dir.glob("*.tmp-*"))


def test_snapshot_rollback_is_distinguished(ready_service) -> None:
    service = ready_service
    last_record = service.workspace.snapshots.records_dir / "00000003-arbitrate.json"
    last_record.unlink()
    with pytest.raises(CabinError) as caught:
        service.verify_snapshots()
    assert caught.value.code == "E_SNAPSHOT_ROLLBACK"
    assert caught.value.details["highest_record_seq"] == 2
    assert caught.value.details["head_seq"] == 3
    with pytest.raises(CabinError) as append_caught:
        service.run(force_full=True, verify=False)
    assert append_caught.value.code == "E_SNAPSHOT_ROLLBACK"


def test_snapshot_tamper_is_distinguished(ready_service) -> None:
    service = ready_service
    record_path = service.workspace.snapshots.records_dir / "00000002-vad.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    record["artifact"]["tampered"] = True
    record_path.write_text(json.dumps(record, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(CabinError) as caught:
        service.verify_snapshots()
    assert caught.value.code == "E_SNAPSHOT_TAMPERED"
    assert caught.value.details["seq"] == 2


def test_snapshot_fingerprint_change_is_tamper(ready_service) -> None:
    service = ready_service
    state = service.workspace.load_state()
    original = state["stages"]["vad"]["artifact_digest"]
    state["stages"]["vad"]["artifact_digest"] = "0" * 64
    service.workspace.save_state(state)
    with pytest.raises(CabinError) as caught:
        service.verify_snapshots()
    assert caught.value.code == "E_SNAPSHOT_TAMPERED"
    assert caught.value.details["stage"] == "vad"
    assert caught.value.details["expected_artifact_digest"] == "0" * 64
    assert caught.value.details["snapshot_artifact_digest"] == original


def test_stop_then_resume_preserves_result(ready_service) -> None:
    service = ready_service
    before = service.versions()["result_digest"]
    service.stop("test")
    stopped = service.run(verify=True)
    assert stopped["status"] == "stopped"
    assert stopped["stop_requested"] is True
    resumed = service.resume(verify=True)
    assert resumed["status"] == "completed"
    assert resumed["result_digest"] == before
    assert resumed["result_digest"] == resumed["verified_full_digest"]


def test_stop_between_shards_leaves_no_partial_vad_stage(service, monkeypatch) -> None:
    workspace = service.workspace
    original = workspace.is_stop_requested
    calls = {"count": 0}

    def scripted_stop() -> bool:
        calls["count"] += 1
        return calls["count"] >= 3

    monkeypatch.setattr(workspace, "is_stop_requested", scripted_stop)
    result = service.run(verify=True)
    assert result["status"] == "stopped"
    state = workspace.load_state()
    assert "vad" not in state["stages"]
    monkeypatch.setattr(workspace, "is_stop_requested", original)
    resumed = service.resume(verify=True)
    assert resumed["status"] == "completed"
    assert resumed["result_digest"] == resumed["verified_full_digest"]
    assert workspace.load_state()["stages"]["vad"]["artifact_digest"]


def test_export_writes_all_views(ready_service) -> None:
    service = ready_service
    exported = service.export("result.json")
    assert exported["result_digest"]
    for name in ("result.json", "activity.json", "tdoa.json", "assignments.json"):
        path: Path = service.workspace.exports_dir / name
        assert path.exists()
        assert path.stat().st_size > 0
