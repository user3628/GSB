from __future__ import annotations

import pytest

from tonelab.config import Config
from tonelab.engine import BuildEngine
from tonelab.errors import ToneLabError


def test_noop_incremental_matches_full(baseline) -> None:
    workspace, engine, full = baseline
    incremental = engine.build("incremental")
    assert incremental["deterministic_fingerprint"] == full["deterministic_fingerprint"]
    assert all(not stages for stages in incremental["recomputed_stages"].values())
    assert incremental["checkpoint"]["last_sequence"] == full["checkpoint"]["last_sequence"]


@pytest.mark.parametrize("kind", ["sample", "unit", "upgrade", "pitch"])
def test_incremental_mutations_are_byte_identical_to_full(kind: str, isolated_workspace) -> None:
    workspace, engine, full = isolated_workspace
    project = workspace.load_project()
    if kind == "sample":
        mutation = {"kind": "sample", "shard_id": project.shards[0].shard_id, "shift": 5}
        expected_recomputed = {"feature", "model", "quantize", "export"}
        expected_reused: set[str] = set()
    elif kind == "unit":
        mutation = {
            "kind": "unit",
            "shard_id": project.shards[1].shard_id,
            "unit_index": 1,
            "end_ms": project.shards[1].units[1].end_ms - 5.0,
        }
        expected_recomputed = {"model", "quantize", "export"}
        expected_reused = {"feature"}
    elif kind == "upgrade":
        shard_id = next(shard.shard_id for shard in project.shards if shard.source_structure_version == 1)
        mutation = {"kind": "upgrade", "shard_id": shard_id}
        expected_recomputed = {"model", "quantize", "export"}
        expected_reused = {"feature"}
    else:
        mutation = {
            "kind": "pitch",
            "shard_id": project.shards[2].shard_id,
            "point_index": 2,
            "value": 1.01,
        }
        expected_recomputed = {"model", "quantize", "export"}
        expected_reused = {"feature"}
    result = engine.consistency_check(mutation)
    assert result["ok"] is True
    assert result["differences"] == []
    assert result["incremental_fingerprint"] == result["full_fingerprint"]
    assert set(result["incremental_recomputed"][mutation["shard_id"]]) == expected_recomputed
    assert set(result["incremental_reused"][mutation["shard_id"]]) == expected_reused
    assert result["compared"]["stages"] == 16
    assert result["compared"]["export_files"] > 0


def test_incremental_mismatch_is_detected(monkeypatch, isolated_workspace) -> None:
    _workspace, engine, _full = isolated_workspace
    original = BuildEngine._build_project_exports
    calls = {"count": 0}

    def tampered(self, project):
        result = original(self, project)
        calls["count"] += 1
        if calls["count"] == 2:
            first_key = sorted(result)[0]
            result[first_key] = {
                **result[first_key],
                "sha256": "0" * 64,
            }
        return result

    monkeypatch.setattr(BuildEngine, "_build_project_exports", tampered)
    project = engine.workspace.load_project()
    with pytest.raises(ToneLabError) as captured:
        engine.consistency_check({"kind": "sample", "shard_id": project.shards[0].shard_id, "shift": 1})
    error = captured.value
    assert error.code == "E_INCREMENTAL_MISMATCH"
    assert error.context["differences"]
    assert error.context["incremental_fingerprint"] != error.context["full_fingerprint"]


def test_structure_version_and_quantization_bits_invalidate_model_or_quantize(isolated_workspace) -> None:
    workspace, engine, _full = isolated_workspace
    project = workspace.load_project()
    v1_shard = next(shard.shard_id for shard in project.shards if shard.source_structure_version == 1)
    result = engine.consistency_check({"kind": "upgrade", "shard_id": v1_shard})
    assert result["incremental_recomputed"][v1_shard][0] == "model"
    # A quantize-only config change must preserve feature and model artifacts.
    new_config = Config(
        {
            **workspace.config.data,
            "quantization": {
                **workspace.config.data["quantization"],
                "pitch_bits": 13,
                "pitch_scale": 4096,
            },
        }
    )
    assert new_config.version != workspace.config.version
    workspace.config = new_config
    workspace._write_config()
    changed = BuildEngine(workspace).build("incremental")
    for shard_id, stages in changed["recomputed_stages"].items():
        assert stages == ["quantize", "export"], shard_id
        assert changed["reused_stages"][shard_id] == ["feature", "model"]


def test_pitch_time_and_unit_formant_editing(isolated_workspace) -> None:
    workspace, engine, _full = isolated_workspace
    project = workspace.load_project()
    shard_id = project.shards[0].shard_id
    pitch_result = engine.edit_pitch_point(shard_id, 1, time=0.01, value=0.99)
    assert pitch_result["new"] == (0.01, 0.99)
    unit_result = engine.edit_unit(
        shard_id,
        0,
        formants_hz=[450.0, 1800.0, 2800.0],
        formant_ranges_hz=[[420.0, 480.0], [1700.0, 1900.0], [2700.0, 2900.0]],
    )
    assert unit_result["new"]["formants_hz"] == [450.0, 1800.0, 2800.0]
    assert unit_result["new"]["formant_ranges_hz"][0] == [420.0, 480.0]
    rebuilt = engine.build("incremental")
    assert rebuilt["recomputed_stages"][shard_id] == ["model", "quantize", "export"]
    assert rebuilt["reused_stages"][shard_id] == ["feature"]

