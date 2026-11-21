from __future__ import annotations

import json
from pathlib import Path

import pytest

from tonelab.config import Config
from tonelab.errors import ToneLabError
from tonelab.formats import load_manifest


def test_v1_stable_id_is_repeatable(demo_data: Path) -> None:
    first = load_manifest(demo_data / "manifest_v1.json", Config())
    second = load_manifest(demo_data / "manifest_v1.json", Config())
    assert first.shards[0].shard_id == second.shards[0].shard_id
    assert first.shards[0].shard_id.startswith("v1-")
    assert first.shards[0].source_structure_version == 1
    assert first.shards[0].structure_version == 2


def test_v1_stable_id_does_not_depend_on_manifest_filename(demo_data: Path, tmp_path: Path) -> None:
    manifest = json.loads((demo_data / "manifest_v1.json").read_text(encoding="utf-8"))
    manifest["shards"][0]["wav"] = str((demo_data / manifest["shards"][0]["wav"]).resolve())
    renamed = tmp_path / "renamed-legacy-manifest.json"
    renamed.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    original = load_manifest(demo_data / "manifest_v1.json", Config())
    copied = load_manifest(renamed, Config())
    assert original.shards[0].shard_id == copied.shards[0].shard_id


def test_v1_configurable_separator_prefix_formants_and_cents(demo_data: Path, tmp_path: Path) -> None:
    manifest = json.loads((demo_data / "manifest_v1.json").read_text(encoding="utf-8"))
    manifest["shards"][0]["wav"] = str((demo_data / manifest["shards"][0]["wav"]).resolve())
    manifest["shards"][0]["units"] = "旧,版,legacy,停顿"
    manifest["shards"][0]["pitch_offsets_percent"] = [0, 100, -100]
    path = tmp_path / "legacy-comma.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    config = Config(
        {
            "v1_compat": {
                "unit_separator": ",",
                "stable_id_prefix": "legacy-",
                "pitch_percent_mode": "cents",
                "pitch_percent_scale": 0.01,
                "default_formants": {
                    "fallback": [611.0, 1777.0, 2888.0],
                    "pause": [322.0, 933.0, 1888.0],
                },
            }
        }
    )
    project = load_manifest(path, config)
    shard = project.shards[0]
    assert shard.shard_id.startswith("legacy-")
    assert [unit.symbol for unit in shard.units] == ["旧", "版", "legacy", "停顿"]
    assert shard.units[0].formants_hz == [611.0, 1777.0, 2888.0]
    assert shard.units[3].formants_hz == [322.0, 933.0, 1888.0]
    assert shard.pitch_curve[1][1] == pytest.approx(2 ** (1.0 / 12.0), rel=0.0, abs=1e-12)
    assert shard.pitch_curve[2][1] == pytest.approx(2 ** (-1.0 / 12.0), rel=0.0, abs=1e-12)
    assert {round(unit.start_ms, 6) for unit in shard.units} == {
        0.0,
        round(shard.duration_ms / 4, 6),
        round(shard.duration_ms / 2, 6),
        round(shard.duration_ms * 3 / 4, 6),
    }


def test_v1_non_integer_percent_is_rejected(demo_data: Path, tmp_path: Path) -> None:
    manifest = json.loads((demo_data / "manifest_v1.json").read_text(encoding="utf-8"))
    manifest["shards"][0]["wav"] = str((demo_data / manifest["shards"][0]["wav"]).resolve())
    manifest["shards"][0]["pitch_offsets_percent"][1] = 1.5
    path = tmp_path / "bad-percent.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(ToneLabError) as captured:
        load_manifest(path, Config())
    assert captured.value.code == "E_NUMERIC"
    assert captured.value.shard_id
    assert captured.value.position["kind"] == "pitch_offset_percent"


def test_upgraded_v1_shard_reexports_as_stable_v1(baseline) -> None:
    _workspace, engine, _report = baseline
    result = engine.roundtrip("json_v1")
    assert result["stable_manifest"] is True
    assert result["pcm_equal"] is True
    assert result["first_sha256"] == result["second_sha256"]


def test_v2_pitch_curve_is_optional(demo_data: Path, tmp_path: Path) -> None:
    manifest = json.loads((demo_data / "manifest_v2.json").read_text(encoding="utf-8"))
    for shard in manifest["shards"]:
        shard["wav"] = str((demo_data / shard["wav"]).resolve())
    manifest["shards"][0].pop("pitch_curve")
    path = tmp_path / "optional-pitch.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    project = load_manifest(path, Config())
    assert project.shards[0].pitch_curve == [(0.0, 1.0)]

