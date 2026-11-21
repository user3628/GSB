from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

from qpipe.errors import Interrupted, InvalidParam, NumericInput
from qpipe.pipeline import Pipeline
from qpipe.verifier import verify_run


def test_numeric_zero_channel_and_unsupported_bits(
    tmp_path: Path, samples_dir: Path
) -> None:
    pipeline = Pipeline(tmp_path, state_dir=".errors")
    pipeline.import_config(
        manifest_path=samples_dir / "operator_manifest_nan.json",
        shapes_path=samples_dir / "shape_declarations.json",
        version="nan",
    )
    with pytest.raises(NumericInput) as numeric:
        pipeline.run(run_id="nan", version="nan")
    assert numeric.value.code == "E_NUMERIC_INPUT"
    assert numeric.value.layer == "l0"
    assert numeric.value.parameter == "activation"

    pipeline.import_config(
        manifest_path=samples_dir / "operator_manifest_zero_channel.json",
        shapes_path=samples_dir / "shape_declarations.json",
        version="zero",
    )
    with pytest.raises(InvalidParam) as zero:
        pipeline.run(run_id="zero", version="zero")
    assert zero.value.code == "E_INVALID_PARAM"
    assert zero.value.layer == "l0"
    assert zero.value.parameter == "weight.quantization.scale"

    pipeline.import_config(
        manifest_path=samples_dir / "operator_manifest_unsupported_bits.json",
        shapes_path=samples_dir / "shape_declarations.json",
        version="bits",
    )
    with pytest.raises(InvalidParam) as bits:
        pipeline.run(run_id="bits", version="bits")
    assert bits.value.code == "E_INVALID_PARAM"
    assert bits.value.layer == "l0"
    assert bits.value.parameter == "weight.quantization.bits"

    env = dict(**__import__("os").environ)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    process = subprocess.run(
        [
            sys.executable,
            "-m",
            "qpipe",
            "run",
            "--workspace",
            str(tmp_path),
            "--state-dir",
            ".errors",
            "--version",
            "nan",
            "--run-id",
            "nan-cli",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert process.returncode == 3
    assert '"code": "E_NUMERIC_INPUT"' in process.stdout


def test_graceful_stop_resume_byte_identical(pipeline: Pipeline) -> None:
    full = pipeline.run(run_id="full")
    with pytest.raises(Interrupted) as caught:
        pipeline.run(run_id="resumed", stop_after_units=5)
    assert caught.value.details["completed_layer_units"] == 5
    state = json.loads(
        (pipeline.root / "runs" / "resumed" / "run_state.json").read_text()
    )
    assert state["status"] == "interrupted"
    resumed = pipeline.run(run_id="resumed", resume=True)
    assert resumed.final_digest == full.final_digest
    for name in ("result.json", "error_report.json", "layer_errors.json", "SHA256SUMS"):
        assert (Path(full.output_dir) / name).read_bytes() == (
            Path(resumed.output_dir) / name
        ).read_bytes()
    verification = verify_run(
        root=pipeline.root, run_id="resumed", compare_run_id="full"
    )
    assert verification["ok"] is True
    assert verification["byte_identical"] is True
    assert verification["checkpoint_count"] == 20


def test_incremental_invalidation_only_rebuilds_downstream(
    pipeline: Pipeline, samples_dir: Path
) -> None:
    baseline = pipeline.run(run_id="baseline")
    assert baseline.status == "pass"
    manifest = json.loads((samples_dir / "operator_manifest.json").read_text())
    manifest["layers"][3]["tolerance"]["max_abs"] = 5.0
    tolerance_path = samples_dir / "operator_manifest_tolerance.json"
    tolerance_path.write_text(json.dumps(manifest), encoding="utf-8")
    pipeline.import_config(
        manifest_path=tolerance_path,
        shapes_path=samples_dir / "shape_declarations.json",
        version="v2",
    )
    status = pipeline.status(version="v2")
    assert status["invalidation"]["earliest_stage"] == "validation"
    reasons = {(item["stage"], item["reason"], item["layer"]) for item in status["invalidation"]["reasons"]}
    assert ("validation", "layer_tolerance_changed", "l3") in reasons
    pipeline.run(run_id="incremental", version="v2")
    state = json.loads(
        (pipeline.root / "runs" / "incremental" / "run_state.json").read_text()
    )
    assert state["stage_sources"] == {
        "calibration": "cache",
        "forward": "cache",
        "attribution": "cache",
        "validation": "recomputed",
    }

    weight_file = samples_dir / "weights" / "shard_00.npz"
    with np.load(weight_file, allow_pickle=False) as shard:
        changed = np.array(shard["weight"], copy=True)
    changed[0, 0] += 0.01
    np.savez(weight_file, weight=changed)
    pipeline.import_config(
        manifest_path=samples_dir / "operator_manifest.json",
        shapes_path=samples_dir / "shape_declarations.json",
        version="v3",
    )
    status = pipeline.status(version="v3")
    assert status["invalidation"]["earliest_stage"] == "calibration"
    assert any(
        item["reason"] == "shard_content_changed"
        for item in status["invalidation"]["reasons"]
    )
    pipeline.run(run_id="weight-change", version="v3")
    state = json.loads(
        (pipeline.root / "runs" / "weight-change" / "run_state.json").read_text()
    )
    assert state["stage_sources"]["calibration"] == "recomputed"
