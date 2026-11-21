from __future__ import annotations

import json
from pathlib import Path

import pytest

from cabin_voice.canonical import canonical_bytes, digest_obj
from cabin_voice.config import VadConfig
from cabin_voice.errors import CabinError
from cabin_voice.manifest import load_manifest
from cabin_voice.projection import build_activity_projection, effective_activity_map
from cabin_voice.vad import compute_vad


def _write_manifest(path: Path, value: dict, data_root: str) -> Path:
    target = path / "broken-manifest.json"
    for shard in value["shards"]:
        wav = Path(shard["wav"])
        if not wav.is_absolute():
            shard["wav"] = str((Path(data_root) / wav).resolve())
    target.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def test_manifest_validation_rejects_bad_total_samples(demo_dataset: dict[str, str], tmp_path: Path) -> None:
    manifest = json.loads(Path(demo_dataset["manifest_path"]).read_text(encoding="utf-8"))
    manifest["shards"][0]["total_samples"] += 1
    broken = _write_manifest(tmp_path, manifest, demo_dataset["data_root"])
    with pytest.raises(CabinError) as caught:
        load_manifest(broken)
    assert caught.value.code == "E_IMPORT_INVALID"
    assert caught.value.details["shard_id"] == manifest["shards"][0]["shard_id"]
    assert "total_samples" in caught.value.message


def test_manifest_validation_rejects_offset_sum_mismatch(demo_dataset: dict[str, str], tmp_path: Path) -> None:
    manifest = json.loads(Path(demo_dataset["manifest_path"]).read_text(encoding="utf-8"))
    manifest["shards"][1]["zones"][-1]["end_sample"] -= 10
    broken = _write_manifest(tmp_path, manifest, demo_dataset["data_root"])
    with pytest.raises(CabinError) as caught:
        load_manifest(broken)
    assert caught.value.code == "E_IMPORT_INVALID"
    assert caught.value.details["shard_id"] == manifest["shards"][1]["shard_id"]


def test_manifest_validation_rejects_annotation_beyond_duration_bound(
    demo_dataset: dict[str, str], tmp_path: Path
) -> None:
    manifest = json.loads(Path(demo_dataset["manifest_path"]).read_text(encoding="utf-8"))
    manifest["shards"][2]["reference_activity_ms"]["driver"].append([1990, 2100])
    broken = _write_manifest(tmp_path, manifest, demo_dataset["data_root"])
    with pytest.raises(CabinError) as caught:
        load_manifest(broken)
    assert caught.value.code == "E_IMPORT_INVALID"
    assert caught.value.details["end_ms"] == 2100
    assert caught.value.details["zone_id"] == "driver"


def test_vad_is_byte_deterministic_and_records_padding(demo_dataset: dict[str, str]) -> None:
    manifest = load_manifest(demo_dataset["manifest_path"])
    first = compute_vad(manifest, VadConfig())
    second = compute_vad(manifest, VadConfig())
    assert canonical_bytes(first) == canonical_bytes(second)
    assert digest_obj(first) == digest_obj(second)
    for shard in first["shards"].values():
        for zone in shard["zones"].values():
            assert zone["padded_frame_count"] >= 1
            assert zone["padding_samples"] > 0
            assert len(zone["frames"]) == zone["frame_count"]
            assert zone["frame_count"] == len(zone["frames"])


def test_vad_detects_silence_and_two_zone_overlap(ready_service) -> None:
    state = ready_service.workspace.load_state()
    timeline = build_activity_projection(state)
    shard1 = timeline["shard-001"]
    assert shard1["zones"]["driver"][0]["start_ms"] <= 230
    assert shard1["zones"]["driver"][0]["end_ms"] >= 610
    assert shard1["zones"]["passenger"][0]["start_ms"] <= 210
    assert shard1["zones"]["passenger"][0]["end_ms"] >= 610
    assert shard1["zones"]["rear_right"] == []
    overlaps = shard1["overlaps"]
    assert overlaps
    assert overlaps[0]["zones"] == ["driver", "passenger"]
    assert overlaps[0]["duration_ms"] > 300
    for shard in state["stages"]["vad"]["artifact"]["shards"].values():
        for zone in shard["zones"].values():
            assert zone["reference_overlap_ratio"] >= 0.90
    activity = effective_activity_map(state)
    assert activity["shard-001"]["driver"]
    assert activity["shard-001"]["rear_right"] == []
