from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from qpipe.checkpoints import load_cached_stage
from qpipe.config import compute_stage_fingerprints, load_current_config
from qpipe.errors import ToleranceExceeded
from qpipe.errors import InvalidStage
from qpipe.pipeline import Pipeline
from qpipe.samples import generate_samples
from qpipe.util import digest_obj


def _cached(pipeline: Pipeline, stage: str) -> dict:
    config, _ = load_current_config(pipeline.root)
    fingerprints = compute_stage_fingerprints(config)
    cached = load_cached_stage(
        pipeline.root, stage, fingerprints["stages"][stage]
    )
    assert cached is not None
    return cached


def test_samples_and_pipeline_are_byte_reproducible(tmp_path: Path) -> None:
    samples_a = tmp_path / "samples-a"
    samples_b = tmp_path / "samples-b"
    generate_samples(samples_a, clean=True)
    generate_samples(samples_b, clean=True)
    files_a = sorted(path.relative_to(samples_a) for path in samples_a.rglob("*") if path.is_file())
    files_b = sorted(path.relative_to(samples_b) for path in samples_b.rglob("*") if path.is_file())
    assert files_a == files_b
    for relative in files_a:
        assert (samples_a / relative).read_bytes() == (samples_b / relative).read_bytes()

    outputs: list[dict[str, bytes]] = []
    for state_name in (".state-a", ".state-b"):
        pipeline = Pipeline(tmp_path, state_dir=state_name)
        pipeline.import_config(
            manifest_path=samples_a / "operator_manifest.json",
            shapes_path=samples_a / "shape_declarations.json",
            version="v1",
        )
        outcome = pipeline.run(run_id="full")
        assert outcome.status == "pass"
        output_dir = Path(outcome.output_dir)
        outputs.append(
            {path.name: path.read_bytes() for path in output_dir.iterdir() if path.is_file()}
        )
    assert outputs[0] == outputs[1]


def test_calibration_covers_extrema_and_forward_counts_overflow(
    pipeline: Pipeline,
) -> None:
    outcome = pipeline.run(run_id="stages")
    assert outcome.status == "pass"
    calibration = _cached(pipeline, "calibration")["payload"]
    forward = _cached(pipeline, "forward")["payload"]
    for layer_result in calibration["layers"].values():
        for role in ("weight", "activation"):
            params = layer_result[role]
            scales = params["scale"] if isinstance(params["scale"], list) else [params["scale"]]
            observed_min = (
                params["observed_min"]
                if isinstance(params["observed_min"], list)
                else [params["observed_min"]]
            )
            observed_max = (
                params["observed_max"]
                if isinstance(params["observed_max"], list)
                else [params["observed_max"]]
            )
            representable_min = (
                params["representable_min"]
                if isinstance(params["representable_min"], list)
                else [params["representable_min"]]
            )
            representable_max = (
                params["representable_max"]
                if isinstance(params["representable_max"], list)
                else [params["representable_max"]]
            )
            assert all(scale > 0 for scale in scales)
            for obs_min, obs_max, rep_min, rep_max in zip(
                observed_min,
                observed_max,
                representable_min,
                representable_max,
                strict=True,
            ):
                assert rep_min <= obs_min + 1e-12
                assert rep_max >= obs_max - 1e-12

    for layer_result in forward["layers"].values():
        accumulator = layer_result["accumulator"]
        assert accumulator["accumulator_mode"] == "fixed_width_integer"
        assert accumulator["overflow_clamp_count"] >= 0
        assert accumulator["accumulator_sha256"]
    assert forward["layers"]["l2"]["accumulator"]["overflow_clamp_count"] > 0


def test_attribution_sums_and_review_flags(pipeline: Pipeline) -> None:
    outcome = pipeline.run(run_id="attribution")
    assert outcome.status == "pass"
    attribution = _cached(pipeline, "attribution")["payload"]
    deviation = attribution["end_to_end"]["absolute_l1_deviation"]
    consistency = attribution["contribution_consistency"]
    tolerance = consistency["declared_tolerance"]
    assert abs(consistency["first_order_sensitivity_sum"] - deviation) <= tolerance
    assert abs(consistency["layer_perturbation_replay_sum"] - deviation) <= tolerance
    assert consistency["consistent"] is True
    for layer in attribution["layers"].values():
        method_a = layer["attribution"]["first_order_sensitivity"]
        method_b = layer["attribution"]["layer_perturbation_replay"]
        assert method_a["basis"] and method_b["basis"]
        assert method_a["rank"] >= 1 and method_b["rank"] >= 1
    assert attribution["ranking"]["review_required_layers"] == ["l2", "l3"]
    result = outcome.result
    assert result["closest_to_tolerance"]["absolute_margin"] >= 0


def test_tolerance_failure_has_structured_report(
    tmp_path: Path, samples_dir: Path
) -> None:
    import json

    manifest = json.loads((samples_dir / "operator_manifest.json").read_text())
    manifest["layers"][0]["tolerance"] = {"max_abs": 1e-12, "max_rel": 0.0}
    manifest["end_to_end_tolerance"] = {"max_abs": 1e-12, "max_rel": 0.0}
    path = samples_dir / "operator_manifest_tight.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    pipeline = Pipeline(tmp_path, state_dir=".tight-state")
    pipeline.import_config(
        manifest_path=path,
        shapes_path=samples_dir / "shape_declarations.json",
        version="tight",
    )
    with pytest.raises(ToleranceExceeded) as caught:
        pipeline.run(run_id="tight")
    exc = caught.value
    report = exc.details["error_report"]
    assert report["code"] == "E_TOLERANCE_EXCEEDED"
    assert report["layer"] is not None
    assert report["max_deviation"] > report["limit"]
    assert report["out_of_tolerance_positions"]
    assert Path(exc.details["output_dir"]).is_dir()


def test_staged_execution_requires_order_and_matches_full(
    pipeline: Pipeline,
) -> None:
    with pytest.raises(InvalidStage):
        pipeline.run_stage(stage="forward", run_id="staged")
    for stage in ("calibration", "forward", "attribution"):
        result = pipeline.run_stage(stage=stage, run_id="staged")
        assert result["status"] == "stage_complete"
    validation = pipeline.run_stage(stage="validation", run_id="staged")
    assert validation["status"] == "pass"
    full = pipeline.run(run_id="full-after-staged")
    assert validation["final_digest"] == full.final_digest
    for name in ("result.json", "error_report.json", "layer_errors.json", "SHA256SUMS"):
        assert (Path(validation["output_dir"]) / name).read_bytes() == (
            Path(full.output_dir) / name
        ).read_bytes()
