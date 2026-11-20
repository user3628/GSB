"""Per-shard deterministic pipeline and versioned incremental invalidation."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np

from .checkpoint import (
    PIPELINE_STAGES,
    CheckpointRecord,
    CheckpointStore,
    build_record,
    deterministic_sequence,
)
from .errors import (
    E_NUMERIC_INPUT,
    InvalidStageError,
    StructuredError,
)
from .fingerprints import combine_fingerprints, fingerprint_array
from .io_util import load_npz, save_npz_deterministic
from .manifest import Manifest, ShardSpec, manifest_config_fingerprint
from .ops import run_linear_chain
from .quantization import (
    has_non_finite,
    non_finite_summary,
    quantize,
)


@dataclass
class StageOutcome:
    stage: str
    reused: bool
    stage_fingerprint: str
    product_summary: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ShardRunResult:
    shard_id: str
    shard_index: int
    status: str
    stages: List[StageOutcome]
    final_summary: Dict[str, Any]
    invalidations: List[Dict[str, Any]]
    error: Optional[Dict[str, Any]] = None
    sequence: Optional[int] = None
    pending_records: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "shard_id": self.shard_id,
            "shard_index": self.shard_index,
            "status": self.status,
            "sequence": self.sequence,
            "stages": [item.__dict__ for item in self.stages],
            "invalidations": self.invalidations,
            "final_summary": self.final_summary,
            "error": self.error,
            "pending_records": self.pending_records,
        }


def _weight_names(shard: ShardSpec) -> List[str]:
    names: List[str] = []
    for layer in shard.layers:
        if layer.weight not in names:
            names.append(layer.weight)
    return names


def input_fingerprint(arrays: Dict[str, np.ndarray]) -> str:
    parts = []
    for name in sorted(arrays):
        parts.append(f"{name}:{fingerprint_array(arrays[name])}")
    return combine_fingerprints(parts)


def expected_stage_fingerprints(
    shard: ShardSpec, arrays: Dict[str, np.ndarray],
    manifest_fingerprint: str,
) -> Dict[str, str]:
    in_fp = input_fingerprint(arrays)
    # validate only depends on input bytes plus the shape declaration, not
    # on quantization settings: changing bitwidth/mode must reuse validate.
    validate_fp = combine_fingerprints([
        "validate", shard.shard_id, in_fp,
        f"shape={shard.expected_output_shape!r}:batch={shard.batch}:"
        f"features={shard.input_features}",
    ])
    quant_parts = ["quantize", validate_fp, manifest_fingerprint]
    for layer in shard.layers:
        quant_parts.append(layer.weight_config().__repr__())
        quant_parts.append(layer.activation_config().__repr__())
    quant_fp = combine_fingerprints(quant_parts)
    linear_fp = combine_fingerprints([
        "linear", quant_fp,
        combine_fingerprints(layer.name for layer in shard.layers),
    ])
    verify_fp = combine_fingerprints([
        "verify", linear_fp,
        combine_fingerprints(
            f"{layer.name}:{layer.rel_tol}:{layer.abs_tol}:"
            f"{layer.sample_positions}" for layer in shard.layers),
    ])
    return {
        "validate": validate_fp,
        "quantize": quant_fp,
        "linear": linear_fp,
        "verify": verify_fp,
    }




def run_shard_pipeline(
    shard: ShardSpec,
    data_dir: Path,
    manifest: Manifest,
    store: CheckpointStore,
    run_fingerprint: str,
    *,
    check_tolerance: bool = True,
    auto_commit: bool = True,
) -> ShardRunResult:
    """Execute (or resume) one shard with stage-level cache reuse.

    When ``auto_commit`` is True (direct library use) each new stage record
    is committed immediately.  The parallel runner sets it False and funnels
    the returned ``pending_records`` through its deterministic publisher.
    """

    arrays = load_npz(Path(data_dir) / shard.npz_file)
    in_fp = input_fingerprint(arrays)
    expected_fps = expected_stage_fingerprints(
        shard, arrays, manifest_config_fingerprint(manifest))
    invalidations: List[Dict[str, Any]] = []
    outcomes: List[StageOutcome] = []
    pending_records: List[Dict[str, Any]] = []
    sequence: Optional[int] = None
    cascade_invalid = False

    def _emit(record: CheckpointRecord) -> None:
        # A rebuilt stage may still hold stale on-disk artifacts from the
        # previous version; clear the stage directory and counter entry
        # immediately before republishing the replacement.
        stage_path = store.stage_record_path(record.shard_id, record.stage)
        if stage_path.exists():
            store.forget_committed(record.shard_id, record.stage)
            stage_dir = stage_path.parent
            for child in stage_dir.iterdir():
                child.unlink()
        if auto_commit:
            store.commit(record)
        else:
            pending_records.append(record.to_dict())

    # ---- stage 1: validate -------------------------------------------
    validate_summary = {
        "input_fingerprint": in_fp,
        "arrays": sorted(arrays),
        "array_shapes": {name: list(arrays[name].shape) for name in
                         sorted(arrays)},
    }
    non_finite = {name: non_finite_summary(array) for name, array in
                  arrays.items() if has_non_finite(array)}
    if non_finite:
        validate_summary["non_finite"] = non_finite
    reused = _try_reuse(store, shard.shard_id, "validate",
                        expected_fps["validate"], invalidations)
    if reused is None:
        sequence = deterministic_sequence(shard.index, "validate")
        record = build_record(
            sequence, shard.shard_id, shard.index, "validate",
            in_fp, manifest.config_version, expected_fps["validate"],
            validate_summary, run_fingerprint)
        _emit(record)
        save_npz_deterministic(
            store.stage_artifact_path(shard.shard_id, "validate",
                                      "validated_input.npz"),
            arrays,
        )
        outcomes.append(StageOutcome("validate", False,
                                     expected_fps["validate"],
                                     validate_summary))
    else:
        outcomes.append(reused)

    if non_finite:
        error = StructuredError(
            code=E_NUMERIC_INPUT,
            message=(f"shard {shard.shard_id} contains NaN/Inf inputs; "
                     "IEEE propagation would poison downstream stages"),
            shard_id=shard.shard_id,
            stage="validate",
            details={"non_finite": non_finite,
                     "shard_id": shard.shard_id, "stage": "validate"},
        )
        return ShardRunResult(
            shard_id=shard.shard_id, shard_index=shard.index,
            status="rejected_input", stages=outcomes,
            final_summary={"input_fingerprint": in_fp},
            invalidations=invalidations, error=error.to_dict(),
            sequence=sequence, pending_records=pending_records,
        )

    # ---- stage 2: quantize -------------------------------------------
    quant_summary: Dict[str, Any] = {"layers": [], "clamp_total": 0}
    quantized_weights: Dict[str, np.ndarray] = {}
    quant_params: Dict[str, Dict[str, np.ndarray]] = {}
    for layer in shard.layers:
        weight = arrays[layer.weight].astype(np.float64)
        w_config = layer.weight_config()
        qw = quantize(weight, w_config)
        quantized_weights[layer.weight] = qw.qvalues
        quant_params[layer.weight] = {"scale": qw.scale,
                                      "zero_point": qw.zero_point}
        quant_summary["layers"].append({
            "layer": layer.name,
            "weight": layer.weight,
            "qmin": qw.qmin,
            "qmax": qw.qmax,
            "clamp_count": qw.clamp_count,
            "low_clamps": qw.low_clamps,
            "high_clamps": qw.high_clamps,
            "clamp_positions": qw.clamp_positions,
        })
        quant_summary["clamp_total"] += qw.clamp_count
    activation = arrays["activation"].astype(np.float64)
    first_act_config = shard.layers[0].activation_config()
    qx = quantize(activation, first_act_config)
    quant_summary["activation"] = qx.to_audit()
    quant_summary["activation"]["clamp_positions"] = qx.clamp_positions
    quant_summary["clamp_total"] += qx.clamp_count

    reused = _try_reuse(store, shard.shard_id, "quantize",
                        expected_fps["quantize"], invalidations,
                        force_invalid=cascade_invalid)
    if reused is None:
        cascade_invalid = True
        sequence = deterministic_sequence(shard.index, "quantize")
        save_npz_deterministic(
            store.stage_artifact_path(shard.shard_id, "quantize",
                                      "quantized_weights.npz"),
            {name: value for name, value in quantized_weights.items()},
        )
        record = build_record(
            sequence, shard.shard_id, shard.index, "quantize",
            in_fp, manifest.config_version, expected_fps["quantize"],
            quant_summary, run_fingerprint)
        _emit(record)
        outcomes.append(StageOutcome("quantize", False,
                                     expected_fps["quantize"], quant_summary))
    else:
        outcomes.append(reused)

    # ---- stage 3: linear chain ---------------------------------------
    weights = {name: arrays[name].astype(np.float64)
               for name in _weight_names(shard)}
    chain = run_linear_chain(
        activation, weights, shard.layers, shard.shard_id,
        check_tolerance=check_tolerance)
    linear_summary = {
        "output_shape": list(chain["final_output"].shape),
        "output_fingerprint": fingerprint_array(chain["final_output"]),
        "reference_fingerprint": fingerprint_array(
            chain["reference_output"]),
    }
    reused = _try_reuse(store, shard.shard_id, "linear",
                        expected_fps["linear"], invalidations,
                        force_invalid=cascade_invalid)
    if reused is None:
        cascade_invalid = True
        sequence = deterministic_sequence(shard.index, "linear")
        save_npz_deterministic(
            store.stage_artifact_path(shard.shard_id, "linear",
                                      "layer_outputs.npz"),
            {name: value for name, value in chain["outputs"].items()},
        )
        record = build_record(
            sequence, shard.shard_id, shard.index, "linear",
            in_fp, manifest.config_version, expected_fps["linear"],
            linear_summary, run_fingerprint)
        _emit(record)
        outcomes.append(StageOutcome("linear", False,
                                     expected_fps["linear"], linear_summary))
    else:
        outcomes.append(reused)

    # ---- stage 4: verify ---------------------------------------------
    verify_summary = {
        "layer_reports": chain["layer_reports"],
        "tightest_margin_layer": chain["tightest_margin_layer"],
        "expected_output_shape": shard.expected_output_shape,
        "shape_matches": list(chain["final_output"].shape) ==
        list(shard.expected_output_shape),
    }
    reused = _try_reuse(store, shard.shard_id, "verify",
                        expected_fps["verify"], invalidations,
                        force_invalid=cascade_invalid)
    if reused is None:
        cascade_invalid = True
        sequence = deterministic_sequence(shard.index, "verify")
        save_npz_deterministic(
            store.stage_artifact_path(shard.shard_id, "verify",
                                      "final_output.npz"),
            {"final_output": chain["final_output"],
             "reference_output": chain["reference_output"]},
        )
        record = build_record(
            sequence, shard.shard_id, shard.index, "verify",
            in_fp, manifest.config_version, expected_fps["verify"],
            verify_summary, run_fingerprint)
        _emit(record)
        outcomes.append(StageOutcome("verify", False,
                                     expected_fps["verify"], verify_summary))
    else:
        outcomes.append(reused)

    final_summary = {
        "input_fingerprint": in_fp,
        "output_fingerprint": linear_summary["output_fingerprint"],
        "output_shape": linear_summary["output_shape"],
        "clamp_total": quant_summary["clamp_total"],
        "layer_count": len(shard.layers),
        "tightest_margin_layer": chain["tightest_margin_layer"],
        "layer_reports": chain["layer_reports"],
        "quant_summary": quant_summary,
    }
    return ShardRunResult(
        shard_id=shard.shard_id, shard_index=shard.index, status="completed",
        stages=outcomes, final_summary=final_summary,
        invalidations=invalidations, sequence=sequence,
        pending_records=pending_records,
    )


def _try_reuse(store: CheckpointStore, shard_id: str, stage: str,
               expected_fp: str,
               invalidations: List[Dict[str, Any]],
               force_invalid: bool = False,
               reason: str = "stage_fingerprint_changed"
               ) -> Optional[StageOutcome]:
    record = store.load_stage_record(shard_id, stage)
    if record is None:
        return None
    if force_invalid or record["stage_fingerprint"] != expected_fp:
        invalidations.append({
            "shard_id": shard_id,
            "stage": stage,
            "reason": ("upstream_stage_invalidated" if force_invalid
                       else reason),
            "old_fingerprint": record["stage_fingerprint"],
            "new_fingerprint": expected_fp,
        })
        return None
    summary = dict(record["product_summary"])
    summary["sequence"] = record["sequence"]
    return StageOutcome(stage=stage, reused=True,
                        stage_fingerprint=expected_fp, product_summary=summary)


def require_prerequisite(store: CheckpointStore, shard_id: str,
                         target_stage: str) -> None:
    """Guard used by callers that jump directly to a later stage."""

    order = PIPELINE_STAGES.index(target_stage)
    for stage in PIPELINE_STAGES[:order]:
        if store.load_stage_record(shard_id, stage) is None:
            raise InvalidStageError(
                f"stage {target_stage} requested without prerequisite "
                f"stage {stage} for shard {shard_id}",
                details={"shard_id": shard_id, "target_stage": target_stage,
                         "missing_stage": stage},
            )
