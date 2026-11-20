from __future__ import annotations

import json
from pathlib import Path

import pytest

from conv_workbench.engine import Workbench
from conv_workbench.errors import InvalidStageError, ShapeMismatchError, WorkbenchError


def _read(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _write(path: Path, value) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_unknown_operator_prevents_any_partial_chain_execution(copied_demo_workspace: Path, tmp_path: Path):
    manifest_path = copied_demo_workspace / "manifest.json"
    manifest = _read(manifest_path)
    small = next(shard for shard in manifest["shards"] if shard["id"] == "small_odd_pad")
    small["operators"][1] = {"name": "not_an_operator", "params": {}}
    _write(manifest_path, manifest)
    workbench = Workbench(copied_demo_workspace, tmp_path / "run")
    with pytest.raises(InvalidStageError) as raised:
        workbench.run()
    assert raised.value.detail["operator_index"] == 1
    assert raised.value.detail["shard_id"] == "small_odd_pad"
    assert not (workbench.run_dir / "checkpoints").exists()
    assert not (workbench.run_dir / "artifacts").exists()


def test_invalid_convolution_parameters_return_invalid_stage(copied_demo_workspace: Path, tmp_path: Path):
    manifest_path = copied_demo_workspace / "manifest.json"
    manifest = _read(manifest_path)
    small = next(shard for shard in manifest["shards"] if shard["id"] == "small_odd_pad")
    small["operators"][0] = {
        "name": "conv2d",
        "params": {"stride": [1, 1], "padding": "same", "dilation": [1, 1]},
    }
    _write(manifest_path, manifest)
    with pytest.raises(InvalidStageError) as raised:
        Workbench(copied_demo_workspace, tmp_path / "run").run()
    assert raised.value.code == "E_INVALID_STAGE"
    assert raised.value.detail["operator_index"] == 0
    assert "weights" in raised.value.detail["reason"]


def test_out_of_bounds_scale_returns_invalid_stage(copied_demo_workspace: Path, tmp_path: Path):
    manifest_path = copied_demo_workspace / "manifest.json"
    manifest = _read(manifest_path)
    small = next(shard for shard in manifest["shards"] if shard["id"] == "small_odd_pad")
    small["operators"][1]["params"]["scale"] = [1.0, 129.0]
    _write(manifest_path, manifest)
    with pytest.raises(InvalidStageError) as raised:
        Workbench(copied_demo_workspace, tmp_path / "run").run()
    assert raised.value.detail["operator_index"] == 1
    assert raised.value.detail["scale"] == [1.0, 129.0]


def test_declared_shape_mismatch_reports_shard_and_block(copied_demo_workspace: Path, tmp_path: Path):
    path = copied_demo_workspace / "expectations.json"
    document = _read(path)
    document["shards"]["small_odd_pad"]["stages"][0]["shape"] = [2, 99, 99, 5]
    _write(path, document)
    with pytest.raises(ShapeMismatchError) as raised:
        Workbench(copied_demo_workspace, tmp_path / "run").run(shard_ids=["small_odd_pad"])
    assert raised.value.code == "E_SHAPE_MISMATCH"
    assert raised.value.detail["shard_id"] == "small_odd_pad"
    assert "block" in raised.value.detail
    assert raised.value.detail["actual_shape"] == [2, 5, 3, 5]


def test_declared_value_range_mismatch_reports_block_position(copied_demo_workspace: Path, tmp_path: Path):
    path = copied_demo_workspace / "expectations.json"
    document = _read(path)
    document["shards"]["small_odd_pad"]["stages"][0]["range"] = [0.0, 0.000001]
    _write(path, document)
    with pytest.raises(ShapeMismatchError) as raised:
        Workbench(copied_demo_workspace, tmp_path / "run").run(shard_ids=["small_odd_pad"])
    detail = raised.value.detail
    assert detail["shard_id"] == "small_odd_pad"
    assert detail["operator_index"] == 0
    assert "output_index" in detail and "block" in detail
    assert len(detail["output_index"]) == 4


def test_partial_chain_export_returns_invalid_stage(demo_workspace: Path, tmp_path: Path):
    workbench = Workbench(demo_workspace, tmp_path / "partial")
    stopped = workbench.run(shard_ids=["small_odd_pad"], stop_after=1)
    assert stopped["status"] == "stopped"
    with pytest.raises(InvalidStageError) as raised:
        workbench.export(shard_ids=["small_odd_pad"])
    assert raised.value.detail["completed_stages"] == 1
    assert raised.value.detail["required_stages"] == 2


def test_reused_checkpoint_is_rechecked_against_current_expectations(copied_demo_workspace: Path, tmp_path: Path):
    workbench = Workbench(copied_demo_workspace, tmp_path / "recheck")
    workbench.run(shard_ids=["small_odd_pad"])
    path = copied_demo_workspace / "expectations.json"
    document = _read(path)
    document["shards"]["small_odd_pad"]["stages"][0]["range"] = [0.0, 0.000001]
    _write(path, document)
    with pytest.raises(ShapeMismatchError) as raised:
        workbench.run(shard_ids=["small_odd_pad"])
    assert raised.value.detail["shard_id"] == "small_odd_pad"
    assert raised.value.detail["operator_index"] == 0

