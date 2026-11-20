from __future__ import annotations

import json
import os
import shutil

import pytest

from transcript_workbench.atomic import atomic_write_bytes
from transcript_workbench.config import digest_value
from transcript_workbench.errors import WorkbenchError
from transcript_workbench.workspace import Workspace
from tests.conftest import first_word


def _edit_once(workspace: Workspace, text: str = "edited") -> int:
    state = workspace.load_state()
    word = first_word(state)
    result = workspace.edit(
        [{"op": "update_word", "entry_id": word.entry_id, "text": text}],
        expected_version=state.version,
    )
    return result["version"]


def _reseal(snapshot: dict) -> dict:
    body = {key: value for key, value in snapshot.items() if key != "snapshot_digest"}
    body["snapshot_digest"] = digest_value(body)
    return body


def test_rollback_snapshot_is_rejected_and_not_overwritten(boot_workspace):
    version_one_bytes = boot_workspace.snapshot_path.read_bytes()
    assert _edit_once(boot_workspace) == 2
    boot_workspace.snapshot_path.write_bytes(version_one_bytes)
    with pytest.raises(WorkbenchError) as raised:
        boot_workspace.load_state()
    assert raised.value.code == "E_STALE_SNAPSHOT"
    assert raised.value.details["reason"] == "rollback"
    assert boot_workspace.snapshot_path.read_bytes() == version_one_bytes
    assert (boot_workspace.versions_dir / "v00000002.json").exists()


def test_tampered_current_snapshot_is_distinguished(boot_workspace):
    snapshot = json.loads(boot_workspace.snapshot_path.read_text(encoding="utf-8"))
    snapshot["words"][0]["text"] += "-tampered"
    tampered_bytes = (json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n").encode()
    boot_workspace.snapshot_path.write_bytes(tampered_bytes)
    with pytest.raises(WorkbenchError) as raised:
        boot_workspace.load_state()
    assert raised.value.code == "E_STALE_SNAPSHOT"
    assert raised.value.details["reason"] == "tamper"
    assert boot_workspace.snapshot_path.read_bytes() == tampered_bytes


def test_normal_advance_is_accepted_from_internal_fields_not_mtime(boot_workspace):
    snapshot = json.loads(boot_workspace.snapshot_path.read_text(encoding="utf-8"))
    previous_digest = snapshot["snapshot_digest"]
    body = {key: value for key, value in snapshot.items() if key != "snapshot_digest"}
    body["version"] = 2
    body["parent_version"] = 1
    body["parent_snapshot_digest"] = previous_digest
    body["kind"] = "external-normal-advance"
    next_snapshot = _reseal(body)
    boot_workspace.snapshot_path.write_text(
        json.dumps(next_snapshot, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.utime(boot_workspace.snapshot_path, (1, 1))
    assert boot_workspace.load_state().version == 2


def test_version_gap_and_broken_parent_chain_are_rejected(boot_workspace):
    snapshot = json.loads(boot_workspace.snapshot_path.read_text(encoding="utf-8"))
    previous_digest = snapshot["snapshot_digest"]
    gap_body = {key: value for key, value in snapshot.items() if key != "snapshot_digest"}
    gap_body["version"] = 5
    gap_body["parent_version"] = 4
    gap_body["parent_snapshot_digest"] = previous_digest
    boot_workspace.snapshot_path.write_text(
        json.dumps(_reseal(gap_body), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(WorkbenchError) as raised:
        boot_workspace.load_state()
    assert raised.value.code == "E_STALE_SNAPSHOT"
    assert raised.value.details["reason"] == "version_gap"

    broken_body = {key: value for key, value in snapshot.items() if key != "snapshot_digest"}
    broken_body["version"] = 2
    broken_body["parent_version"] = 1
    broken_body["parent_snapshot_digest"] = "0" * 64
    boot_workspace.snapshot_path.write_text(
        json.dumps(_reseal(broken_body), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(WorkbenchError) as raised:
        boot_workspace.load_state()
    assert raised.value.code == "E_STALE_SNAPSHOT"
    assert raised.value.details["reason"] == "broken_chain"


def test_changed_input_audio_fingerprint_is_stale(boot_workspace):
    chunk = boot_workspace.load_state().chunks["chunk-02"]
    with open(chunk.path, "ab") as handle:
        handle.write(b"tamper")
    with pytest.raises(WorkbenchError) as raised:
        boot_workspace.load_state()
    assert raised.value.code == "E_STALE_SNAPSHOT"
    assert raised.value.details["reason"] == "input_fingerprint_mismatch"
    assert raised.value.details["chunk_id"] == "chunk-02"


def test_atomic_writer_never_leaves_partial_destination(tmp_path, monkeypatch):
    destination = tmp_path / "snapshot.json"
    destination.write_bytes(b"old-consistent")

    def fail_replace(source, target):
        raise OSError("simulated crash before replace")

    monkeypatch.setattr(os, "replace", fail_replace)
    with pytest.raises(OSError):
        atomic_write_bytes(destination, b"new-partial")
    assert destination.read_bytes() == b"old-consistent"
    assert list(tmp_path.glob(".snapshot.json.tmp-*")) == []


def test_stop_drains_new_edits_and_recover_restores_latest(boot_workspace):
    assert _edit_once(boot_workspace) == 2
    stopped = boot_workspace.request_stop()
    assert stopped["version"] == 2
    with pytest.raises(WorkbenchError) as raised:
        boot_workspace.edit([], expected_version=2)
    assert raised.value.code == "E_STOPPING"
    recovered = boot_workspace.recover()
    assert recovered["version"] == 2
    assert recovered["replayed_versions"] == []
    resumed = boot_workspace.edit([], expected_version=2)
    assert resumed["version"] == 3


def test_restart_loads_latest_committed_snapshot(boot_workspace):
    assert _edit_once(boot_workspace) == 2
    restarted = Workspace(boot_workspace.root)
    assert restarted.load_state().version == 2
    assert restarted.recover()["version"] == 2


def test_snapshot_contains_required_fingerprint_and_summary_fields(boot_workspace):
    snapshot = json.loads(boot_workspace.snapshot_path.read_text(encoding="utf-8"))
    for key in (
        "version",
        "input_fingerprint",
        "format_fingerprint",
        "timeline_digest",
        "snapshot_digest",
        "chunks",
        "words",
    ):
        assert key in snapshot
    assert snapshot["version"] == 1
    assert snapshot["input_fingerprint"]
    assert snapshot["format_fingerprint"]
    assert snapshot["timeline_digest"]


def test_changed_format_configuration_is_rejected_as_stale(boot_workspace):
    changed = Workspace(
        boot_workspace.root,
        config=boot_workspace.config.__class__(max_gap_ms=2000),
    )
    with pytest.raises(WorkbenchError) as raised:
        changed.load_state()
    assert raised.value.code == "E_STALE_SNAPSHOT"
    assert raised.value.details["reason"] == "format_fingerprint_mismatch"


def test_missing_archive_is_repaired_from_committed_current_snapshot(boot_workspace):
    assert _edit_once(boot_workspace) == 2
    archive = boot_workspace.versions_dir / "v00000002.json"
    archive.unlink()
    assert boot_workspace.load_state().version == 2
    assert archive.exists()
    assert boot_workspace.load_version(2).version == 2
