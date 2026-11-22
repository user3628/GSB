from __future__ import annotations

import shutil
from pathlib import Path

import numpy as np
import pytest

from cabin_voice.engine import Engine
from cabin_voice.errors import CabinVoiceError
from cabin_voice.storage import Workspace
from cabin_voice.timeline import EditModel
from cabin_voice.util import atomic_write_bytes


def _copy_workspace(source: Workspace, target: Path) -> Workspace:
    destination = Workspace(target).ensure()
    shutil.copytree(source.root, destination.root, dirs_exist_ok=True)
    return destination


def test_priority_change_reuses_detection_and_delay(full_workspace: Workspace) -> None:
    before = full_workspace.read_final_bytes()
    manifest = full_workspace.require_manifest()
    edits = EditModel(full_workspace.edits_path, manifest)
    edits.set_priority("passenger", 96)
    result = Engine(full_workspace).build(mode="incremental", verify=True)
    assert result["status"] == "complete"
    assert result["verification"]["equal"] is True
    for chunk_report in result["invalidation"]["chunks"].values():
        assert chunk_report["earliest_stage"] == "attribution"
        assert chunk_report["reason_codes"] == ["ZONE_PRIORITY_CHANGED"]
        assert chunk_report["sources"]["detection"] == "cache"
        assert chunk_report["sources"]["delay"] == "cache"
        assert chunk_report["sources"]["attribution"] == "computed"
    assert full_workspace.read_final_bytes() != before


def test_frame_config_change_invalidates_from_detection(full_workspace: Workspace) -> None:
    config = full_workspace.load_config()
    config["detection"]["frame_ms"] = 30.0
    full_workspace.save_config(config)
    result = Engine(full_workspace).build(mode="incremental", verify=True)
    assert result["verification"]["equal"] is True
    for chunk_report in result["invalidation"]["chunks"].values():
        assert chunk_report["earliest_stage"] == "detection"
        assert chunk_report["reason_codes"] == ["FRAME_OR_HYSTERESIS_CONFIG_CHANGED"]
        assert chunk_report["sources"]["detection"] == "computed"


def test_sample_replacement_invalidates_only_sample_dependent_chain(
    full_workspace: Workspace, tmp_path: Path
) -> None:
    manifest = full_workspace.require_manifest()
    chunk = manifest["chunks"][0]
    source_wav = full_workspace.chunk_wav_path(chunk)
    import wave

    with wave.open(str(source_wav), "rb") as handle:
        params = handle.getparams()
        data = np.frombuffer(handle.readframes(handle.getnframes()), dtype="<i2").copy()
    altered = np.clip((data.astype(np.float64) * 0.97).round(), -32768, 32767).astype("<i2")
    replacement = tmp_path / "replacement.wav"
    with wave.open(str(replacement), "wb") as handle:
        handle.setparams(params)
        handle.writeframes(altered.tobytes())
    full_workspace.replace_chunk_sample_file("chunk_001", replacement)
    result = Engine(full_workspace).build(mode="incremental", verify=True)
    assert result["verification"]["equal"] is True
    first = result["invalidation"]["chunks"]["chunk_001"]
    assert first["earliest_stage"] == "detection"
    assert "SAMPLE_CONTENT_CHANGED" in first["reason_codes"]
    assert result["invalidation"]["chunks"]["chunk_002"]["earliest_stage"] == "summary"
    assert result["invalidation"]["chunks"]["chunk_002"]["sources"]["detection"] == "cache"


def test_edit_sequence_is_byte_deterministic(tmp_path: Path, workspace: Workspace) -> None:
    second = _copy_workspace(workspace, tmp_path / "second")
    first_manifest = workspace.require_manifest()
    second_manifest = second.require_manifest()
    first_edits = EditModel(workspace.edits_path, first_manifest)
    second_edits = EditModel(second.edits_path, second_manifest)
    for edits in (first_edits, second_edits):
        edits.set_priority("rear_left", 88)
        edits.split_activity("chunk_001", "driver", 500)
        edits.merge_activity("chunk_001", "driver", 200, 500)
    assert first_edits.state_bytes() == second_edits.state_bytes()
    assert [entry["edit_version"] for entry in first_edits.state["trail"]] == [1, 2, 3]
    assert all(entry["before_summary"] != entry["after_summary"] for entry in first_edits.state["trail"])
    assert first_edits.state["trail"][1]["operation"] == "split_activity"
    assert first_edits.state["trail"][2]["operation"] == "merge_activity"
    first_result = Engine(workspace).build(mode="full")
    second_result = Engine(second).build(mode="full")
    assert first_result["final_digest"] == second_result["final_digest"]
    assert workspace.read_final_bytes() == second.read_final_bytes()
    final = Engine(workspace).result()
    assert len(final["activity_intervals"]["chunk_001"]["driver"]) == 1


def test_invalidation_report_is_observable(full_workspace: Workspace) -> None:
    manifest = full_workspace.require_manifest()
    EditModel(full_workspace.edits_path, manifest).set_hits(
        "chunk_003", "rear_right", [{"term": "play_music", "strength": 0.95, "at_ms": 735}]
    )
    Engine(full_workspace).build(mode="incremental", verify=True)
    fingerprints = Engine(full_workspace).fingerprints()
    assert fingerprints["invalidation"]["chunks"]["chunk_003"]["earliest_stage"] == "attribution"
    assert fingerprints["chunks"]["chunk_003"]["attribution"]["status"] == "published"
    assert "lexicon_hits" in fingerprints["chunks"]["chunk_003"]["attribution"]["components"]


def test_mismatch_reports_first_difference(full_workspace: Workspace) -> None:
    final = full_workspace.read_final_bytes()
    assert final is not None
    tampered = bytearray(final)
    tampered[len(tampered) // 2] ^= 0x01
    atomic_write_bytes(full_workspace.final_path, bytes(tampered))
    with pytest.raises(CabinVoiceError) as error:
        Engine(full_workspace).verify_full_consistency()
    assert error.value.code == "E_INCREMENTAL_MISMATCH"
    difference = error.value.details["first_difference"]
    assert difference["byte_offset"] == len(tampered) // 2
    assert "left_hex" in difference and "right_hex" in difference
