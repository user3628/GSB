from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from multiscale_fusion.engine import FusionEngine
from multiscale_fusion.errors import FusionError
from multiscale_fusion.util import load_npy, pretty_json_bytes, sha256_file, write_bytes, write_json, write_npy
from multiscale_fusion.workspace import import_bundle

from .conftest import key_artifacts, layer_dict, manifest_dict, write_manifest_dict


def _full_rebuild_workspace(source: Path, target: Path) -> Path:
    import_bundle(source / "input", target, force=True)
    report = target / "input" / "import_report.json"
    if report.exists():
        report.unlink()
    FusionEngine(target).run("demo_fpn")
    return target


def test_attention_change_invalidates_layer_and_downstream(workspace: Path, tmp_path: Path) -> None:
    engine = FusionEngine(workspace)
    engine.run("demo_fpn")
    data = manifest_dict(workspace)
    layer_dict(data, "p4")["attention"]["weights"][0] = 0.123456
    write_manifest_dict(workspace, data)
    engine = FusionEngine(workspace)
    result = engine.run("demo_fpn")
    assert result["status"] == "completed"
    assert result["reused_layers"] == ["p5"]
    assert result["computed_layers"] == ["p4", "p3"]
    assert result["invalidation"]["earliest_affected_layer"] == "p4"
    assert result["invalidation"]["affected_layers"] == ["p4", "p3"]
    assert "attention_changed" in result["invalidation"]["reasons"]["p4"]
    assert result["invalidation"]["reasons"]["p3"] == ["dependency_invalidated"]
    assert engine.verify_incremental("demo_fpn")["ok"] is True

    full = _full_rebuild_workspace(workspace, tmp_path / "full")
    assert key_artifacts(workspace) == key_artifacts(full)


def test_p3_only_change_reuses_both_deeper_sources(workspace: Path, tmp_path: Path) -> None:
    engine = FusionEngine(workspace)
    engine.run("demo_fpn")
    data = manifest_dict(workspace)
    layer_dict(data, "p3")["fusion"]["coeffs"]["lateral"] = 0.41
    layer_dict(data, "p3")["fusion"]["coeffs"]["topdown"] = 0.59
    write_manifest_dict(workspace, data)
    engine = FusionEngine(workspace)
    result = engine.run("demo_fpn")
    assert result["reused_layers"] == ["p5", "p4"]
    assert result["computed_layers"] == ["p3"]
    assert result["invalidation"]["earliest_affected_layer"] == "p3"
    assert result["invalidation"]["affected_layers"] == ["p3"]
    full = _full_rebuild_workspace(workspace, tmp_path / "full")
    assert key_artifacts(workspace) == key_artifacts(full)


def test_sensitivity_only_change_reuses_all_tensors(workspace: Path, tmp_path: Path) -> None:
    engine = FusionEngine(workspace)
    engine.run("demo_fpn")
    before = key_artifacts(workspace)
    data = manifest_dict(workspace)
    data["tasks"][0]["sensitivity"]["amplitude"] = 0.0015
    write_manifest_dict(workspace, data)
    engine = FusionEngine(workspace)
    result = engine.run("demo_fpn")
    assert result["reused_layers"] == ["p5", "p4", "p3"]
    assert result["computed_layers"] == []
    assert result["invalidation"]["earliest_affected_stage"] == "sensitivity"
    assert result["invalidation"]["affected_layers"] == []
    assert result["invalidation"]["sensitivity_invalidated"] is True
    assert result["head_sequence"] == 3
    after = key_artifacts(workspace)
    assert after["p3.npy"] == before["p3.npy"]
    assert after["p4.npy"] == before["p4.npy"]
    assert after["p5.npy"] == before["p5.npy"]
    assert after["reports/sensitivity.json"] != before["reports/sensitivity.json"]
    full = _full_rebuild_workspace(workspace, tmp_path / "full")
    assert after == key_artifacts(full)


def test_quant_bits_change_invalidates_from_changed_layer(workspace: Path, tmp_path: Path) -> None:
    engine = FusionEngine(workspace)
    engine.run("demo_fpn")
    data = manifest_dict(workspace)
    layer_dict(data, "p4")["quantization"]["bits"] = 10
    write_manifest_dict(workspace, data)
    engine = FusionEngine(workspace)
    result = engine.run("demo_fpn")
    assert result["reused_layers"] == ["p5"]
    assert result["computed_layers"] == ["p4", "p3"]
    assert "quantization_changed" in result["invalidation"]["reasons"]["p4"]
    full = _full_rebuild_workspace(workspace, tmp_path / "full")
    assert key_artifacts(workspace) == key_artifacts(full)


def test_input_shard_change_marks_reference_mismatch_but_rebuild_matches(workspace: Path, tmp_path: Path) -> None:
    engine = FusionEngine(workspace)
    engine.run("demo_fpn")
    shard = workspace / "input" / "shards" / "p4.npy"
    values = load_npy(shard, layer="p4")
    values[0, 0, 0, 0] += 13.25
    write_npy(shard, values)
    engine = FusionEngine(workspace)
    result = engine.run("demo_fpn")
    assert result["reused_layers"] == ["p5"]
    assert result["computed_layers"] == ["p4", "p3"]
    assert result["quantization"]["layers"]["p4"]["reference_error_within_bound"] is False
    assert engine.verify_incremental("demo_fpn")["ok"] is True
    full = _full_rebuild_workspace(workspace, tmp_path / "full")
    assert key_artifacts(workspace) == key_artifacts(full)


def test_consistent_corruption_is_localized_as_incremental_mismatch(engine: FusionEngine) -> None:
    engine.run("demo_fpn")
    store = engine.store("demo_fpn")
    state = store.scan()
    record = state.latest_for_layer("p3")
    assert record is not None
    raw_path = store.artifacts_dir / record["stage_version"] / "raw.npy"
    values = load_npy(raw_path, layer="p3")
    values.flat[3] += 1.0
    write_npy(raw_path, values)
    new_digest = sha256_file(raw_path)
    public = {key: value for key, value in record.items() if not key.startswith("_")}
    public["artifact_summary"]["files"]["raw.npy"] = new_digest
    record_path = Path(record["_path"])
    write_bytes(record_path, pretty_json_bytes(public))
    new_record_digest = store.record_digest(public)
    write_json(
        store.head_path,
        {
            "schema_version": 1,
            "task_id": "demo_fpn",
            "sequence": record["sequence"],
            "record_digest": new_record_digest,
        },
    )
    assert store.verify()["classification"] == "normal"
    with pytest.raises(FusionError) as excinfo:
        engine.verify_incremental("demo_fpn")
    error = excinfo.value
    assert error.code == "E_INCREMENTAL_MISMATCH"
    assert error.layer == "p3"
    assert error.index == 3
    assert error.details["reason"] == "raw_tensor_diff"
