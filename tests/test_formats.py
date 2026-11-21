from __future__ import annotations

import copy
import json
from pathlib import Path

import numpy as np
import pytest
import soundfile as sf

from tonelab.config import Config
from tonelab.errors import ToneLabError
from tonelab.formats import load_manifest


@pytest.mark.parametrize("fmt", ["json_v2", "wav", "units", "json_v1"])
def test_all_external_formats_roundtrip(fmt: str, baseline) -> None:
    workspace, engine, _report = baseline
    result = engine.roundtrip(fmt)
    assert result["ok"] is True
    assert result["differences"] == []
    if fmt == "json_v2":
        assert result["stable_export"] is True
        assert result["first_sha256"] == result["second_sha256"]
    if fmt == "wav":
        assert result["stable_export"] is True
        assert all(item["sha256"] for item in result["files"].values())
    if fmt == "units":
        assert result["stable_export"] is True
    if fmt == "json_v1":
        assert result["stable_manifest"] is True
        assert result["pcm_equal"] is True
        assert result["max_normalized_pitch_delta"] <= result["pitch_percent_rounding_error_bound"] + 1e-12


def test_wav_export_preserves_declared_header(baseline, tmp_path: Path) -> None:
    _workspace, engine, _report = baseline
    output = tmp_path / "wav"
    result = engine.export("wav", output)
    for item in result["files"].values():
        info = sf.info(item["wav"])
        assert info.samplerate == 16000
        assert info.channels == 1
        assert info.subtype == "PCM_16"
        assert item["sample_rate"] == 16000
        assert item["channels"] == 1
        assert item["bit_depth"] == 16


@pytest.mark.parametrize(
    "sample_rate,channels",
    [(8000, 1), (16000, 2)],
)
def test_wav_format_mismatch_has_shard_and_declared_actual(
    demo_data: Path,
    tmp_path: Path,
    sample_rate: int,
    channels: int,
) -> None:
    manifest = json.loads((demo_data / "manifest_v2.json").read_text(encoding="utf-8"))
    pcm, _ = sf.read(str(demo_data / manifest["shards"][0]["wav"]), dtype="int16", always_2d=False)
    changed = tmp_path / "changed.wav"
    if channels == 2:
        data = np.column_stack([pcm, pcm])
    else:
        data = pcm
    sf.write(str(changed), data, sample_rate, subtype="PCM_16", format="WAV")
    manifest["shards"][0]["wav"] = "changed.wav"
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(ToneLabError) as captured:
        load_manifest(path, Config())
    error = captured.value
    assert error.code == "E_FORMAT_MISMATCH"
    assert error.shard_id == manifest["shards"][0]["shard_id"]
    assert error.context["declared"]["sample_rate"] == 16000
    assert error.context["declared"]["channels"] == 1
    assert error.context["actual"]["sample_rate"] == sample_rate
    assert error.context["actual"]["channels"] == channels


def test_manifest_total_upper_bound_is_checked(demo_data: Path, tmp_path: Path) -> None:
    manifest = json.loads((demo_data / "manifest_v1.json").read_text(encoding="utf-8"))
    manifest["shards"][0]["wav"] = str((demo_data / manifest["shards"][0]["wav"]).resolve())
    manifest["declared_total_samples"] -= 1
    path = tmp_path / "bad-total.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(ToneLabError) as captured:
        load_manifest(path, Config())
    assert captured.value.code == "E_MANIFEST_MISMATCH"
    assert captured.value.context["declared_total_samples"] == manifest["declared_total_samples"]
    assert captured.value.context["actual_total_samples"] > captured.value.context["declared_total_samples"]


def test_declared_total_is_an_upper_bound_by_default(demo_data: Path, tmp_path: Path) -> None:
    manifest = json.loads((demo_data / "manifest_v1.json").read_text(encoding="utf-8"))
    manifest["shards"][0]["wav"] = str((demo_data / manifest["shards"][0]["wav"]).resolve())
    manifest["declared_total_samples"] += 4096
    path = tmp_path / "upper-bound.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    project = load_manifest(path, Config())
    assert project.total_samples < project.declared_total_samples


def test_exact_total_policy_is_configurable(demo_data: Path, tmp_path: Path) -> None:
    manifest = json.loads((demo_data / "manifest_v1.json").read_text(encoding="utf-8"))
    manifest["shards"][0]["wav"] = str((demo_data / manifest["shards"][0]["wav"]).resolve())
    manifest["declared_total_samples"] += 4096
    path = tmp_path / "exact-total.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(ToneLabError) as captured:
        load_manifest(path, Config({"manifest": {"exact_total_samples": True}}))
    assert captured.value.code == "E_MANIFEST_MISMATCH"


def test_per_shard_declared_sample_count_is_an_upper_bound(demo_data: Path, tmp_path: Path) -> None:
    manifest = json.loads((demo_data / "manifest_v1.json").read_text(encoding="utf-8"))
    manifest["shards"][0]["wav"] = str((demo_data / manifest["shards"][0]["wav"]).resolve())
    manifest["shards"][0]["sample_count"] -= 1
    manifest["declared_total_samples"] -= 1
    path = tmp_path / "bad-shard-count.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(ToneLabError) as captured:
        load_manifest(path, Config())
    assert captured.value.code == "E_MANIFEST_MISMATCH"
    assert captured.value.shard_id.startswith("v1-")
    assert captured.value.context["actual_sample_count"] > captured.value.context["declared_sample_count"]


def test_project_json_v2_embeds_pcm_and_metadata(baseline, tmp_path: Path) -> None:
    _workspace, engine, _report = baseline
    path = tmp_path / "project.json"
    engine.export("json_v2", path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["format"] == "tonelab.sound-project"
    assert payload["structure_version"] == 2
    assert len(payload["shards"]) == 4
    assert any(shard["source_structure_version"] == 1 for shard in payload["shards"])
    assert all(shard["pcm_encoding"] == "int16le-base64" and shard["pcm_b64"] for shard in payload["shards"])

