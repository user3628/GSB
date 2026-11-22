from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from cabin_voice.config import default_config
from cabin_voice.engine import Engine
from cabin_voice.errors import CabinVoiceError
from cabin_voice.storage import Workspace


def _reference(manifest: dict, chunk_id: str, zone_id: str) -> list[list[int]]:
    chunk = next(chunk for chunk in manifest["chunks"] if chunk["chunk_id"] == chunk_id)
    segment = next(segment for segment in chunk["segments"] if segment["zone_id"] == zone_id)
    return segment["reference_active_ms"]


def test_vad_respects_reference_and_marks_overlaps(full_workspace: Workspace) -> None:
    result = Engine(full_workspace).result()
    manifest = full_workspace.require_manifest()
    for chunk in manifest["chunks"]:
        chunk_id = chunk["chunk_id"]
        for segment in chunk["segments"]:
            zone_id = segment["zone_id"]
            detected = result["activity_intervals"][chunk_id][zone_id]
            reference = _reference(manifest, chunk_id, zone_id)
            assert len(detected) == len(reference), (chunk_id, zone_id, detected, reference)
            for actual, expected in zip(detected, reference):
                assert abs(actual["start_ms"] - expected[0]) <= 30
                assert abs(actual["end_ms"] - expected[1]) <= 30
    overlap = [marker for marker in result["overlap_markers"] if marker["chunk_id"] == "chunk_002"]
    assert any(set(marker["zones"]) == {"driver", "passenger"} for marker in overlap)
    assert result["summary"]["counts"]["overlap_regions"] >= 5


def test_overlapping_same_term_requires_review_with_evidence(full_workspace: Workspace) -> None:
    result = Engine(full_workspace).result()
    event = next(
        command for command in result["command_attributions"] if command["event_id"] == "chunk_002:open_window"
    )
    assert event["status"] == "needs_review"
    assert event["responsible_zone"] is None
    assert set(event["review"]["retained_zones"]) == {"driver", "passenger"}
    assert len(event["review"]["evidence"]) == 2
    assert event["score_gap"] < 0.20
    assigned = [command for command in result["command_attributions"] if command["status"] == "assigned"]
    assert len(assigned) >= 8
    assert all(command["responsible_zone"] for command in assigned)


@pytest.mark.parametrize(
    "path,value,parameter",
    [
        ("detection.enter_threshold_db", -60.0, "detection.enter_threshold_db"),
        ("detection.hop_ms", 0.0, "detection.hop_ms"),
    ],
)
def test_invalid_config_identifies_zone_and_parameter(
    workspace: Workspace, path: str, value: float, parameter: str
) -> None:
    config = default_config()
    cursor = config
    parts = path.split(".")
    for part in parts[:-1]:
        cursor = cursor[part]
    cursor[parts[-1]] = value
    workspace.save_config(config)
    with pytest.raises(CabinVoiceError) as error:
        Engine(workspace).build(mode="full")
    assert error.value.code == "E_INVALID_CONFIG"
    assert error.value.details["zone_id"] in {"driver", "passenger", "rear_left", "rear_right"}
    assert error.value.details["parameter"] == parameter
    assert not workspace.run_path.exists()


def test_bad_offset_injection_returns_business_code(tmp_path: Path, generated_demo: dict) -> None:
    ws = Workspace(tmp_path / "bad_offset").ensure()
    with pytest.raises(CabinVoiceError) as error:
        ws.import_bundle(generated_demo["invalid_offset_manifest"])
    assert error.value.code == "E_ZONE_OFFSET_OUT_OF_BOUNDS"
    assert error.value.details["parameter"] == "offset_samples"
    assert error.value.details["zone_id"] == "driver"


def test_missing_lexicon_reference_returns_business_code(tmp_path: Path, generated_demo: dict) -> None:
    source = Path(generated_demo["manifest"])
    manifest = json.loads(source.read_text(encoding="utf-8"))
    manifest["chunks"][0]["segments"][0]["lexicon_hits"][0]["term"] = "not_in_catalog"
    broken = tmp_path / "missing_lexicon.json"
    broken.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    for audio in (Path(generated_demo["output"]) / "audio").glob("*.wav"):
        shutil.copyfile(audio, tmp_path / audio.name)
    shutil.copytree(Path(generated_demo["output"]) / "audio", tmp_path / "audio", dirs_exist_ok=True)
    ws = Workspace(tmp_path / "missing_workspace").ensure()
    with pytest.raises(CabinVoiceError) as error:
        ws.import_bundle(broken)
    assert error.value.code == "E_LEXICON_REFERENCE_MISSING"
    assert error.value.details["term"] == "not_in_catalog"


def test_absent_lexicon_returns_missing_reference_code(tmp_path: Path, generated_demo: dict) -> None:
    source = Path(generated_demo["manifest"])
    manifest = json.loads(source.read_text(encoding="utf-8"))
    manifest.pop("lexicon")
    broken = tmp_path / "absent_lexicon.json"
    broken.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    shutil.copytree(Path(generated_demo["output"]) / "audio", tmp_path / "audio", dirs_exist_ok=True)
    ws = Workspace(tmp_path / "absent_workspace").ensure()
    with pytest.raises(CabinVoiceError) as error:
        ws.import_bundle(broken)
    assert error.value.code == "E_LEXICON_REFERENCE_MISSING"
    assert error.value.details["parameter"] == "lexicon"


def test_nonfinite_hit_score_returns_business_code(tmp_path: Path, generated_demo: dict) -> None:
    source = Path(generated_demo["manifest"])
    manifest = json.loads(source.read_text(encoding="utf-8"))
    manifest["chunks"][0]["segments"][0]["lexicon_hits"][0]["strength"] = float("nan")
    broken = tmp_path / "nonfinite.json"
    broken.write_text(json.dumps(manifest, ensure_ascii=False, allow_nan=True), encoding="utf-8")
    shutil.copytree(Path(generated_demo["output"]) / "audio", tmp_path / "audio", dirs_exist_ok=True)
    ws = Workspace(tmp_path / "nonfinite_workspace").ensure()
    with pytest.raises(CabinVoiceError) as error:
        ws.import_bundle(broken)
    assert error.value.code == "E_NONFINITE_SCORE"
