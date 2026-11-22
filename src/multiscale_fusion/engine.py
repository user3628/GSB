from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import numpy as np

from .attention import normalize_attention
from .checkpoint import CheckpointState, CheckpointStore
from .errors import FusionError
from .kernels import build_conv_params, conv_output_hw
from .manifest import LayerSpec, Manifest, TaskSpec
from .pipeline import FusionResult, LayerOutput, compute_layer_output, run_fusion
from .quantization import QuantizationResult, quantize_symmetric
from .sensitivity import assert_sensitivity_report_valid, compute_sensitivity_report
from .util import (
    array_bytes,
    canonical_json_bytes,
    load_npy,
    pretty_json_bytes,
    read_json,
    resolve_under,
    sha256_array,
    sha256_bytes,
    sha256_file,
    sha256_json,
    stable_rng,
    write_bytes,
    write_json,
    write_npy,
)
from .workspace import load_workspace_manifest


ALGORITHM_VERSION = "multiscale-fusion/v1"


@dataclass
class LayerPlan:
    layer: LayerSpec
    input_path: Path
    input_bytes_hash: str
    input_array: np.ndarray
    conv_params: tuple[np.ndarray, np.ndarray, dict[str, Any]]
    reference_path: Path
    reference_bytes_hash: str
    reference_array: np.ndarray
    component_fingerprints: dict[str, str]
    own_fingerprint: str
    dependency_layer: str | None = None
    dependency_version: str | None = None
    stage_version: str = ""


@dataclass
class TaskPlan:
    task: TaskSpec
    layers: dict[str, LayerPlan]
    processing_order: tuple[str, ...]
    final_reference_path: Path
    final_reference_hash: str
    final_reference_array: np.ndarray
    sensitivity_version: str
    overall_version: str


@dataclass
class StageMaterial:
    raw: np.ndarray
    metadata: dict[str, Any]
    record: dict[str, Any]
    reused: bool
    quant_arrays: dict[str, np.ndarray]


class FusionEngine:
    """Library entry point for deterministic pyramid fusion and recovery."""

    def __init__(self, workspace: str | Path, *, manifest: Manifest | None = None, lock_timeout: float = 20.0) -> None:
        self.workspace = Path(workspace).resolve()
        self.input_dir = self.workspace / "input"
        self.manifest = manifest or load_workspace_manifest(self.workspace)
        self.lock_timeout = lock_timeout

    def _resolve_task(self, task_id: str | None) -> TaskSpec:
        if task_id is None:
            if len(self.manifest.tasks) != 1:
                raise FusionError.invalid_stage(
                    "task_id is required when the manifest contains multiple tasks",
                    details={"available_tasks": [task.id for task in self.manifest.tasks]},
                )
            return self.manifest.tasks[0]
        return self.manifest.by_id(task_id)

    def store(self, task: TaskSpec | str) -> CheckpointStore:
        task_id = task if isinstance(task, str) else task.id
        return CheckpointStore(self.workspace, task_id, lock_timeout=self.lock_timeout)

    def _build_plan(self, task: TaskSpec) -> TaskPlan:
        layers: dict[str, LayerPlan] = {}
        for layer in task.layers:
            input_path = resolve_under(self.input_dir, layer.input, field_name=f"layers.{layer.name}.input")
            if not input_path.is_file():
                raise FusionError.invalid_stage(
                    f"input shard for layer {layer.name!r} is missing",
                    layer=layer.name,
                    path=str(input_path),
                )
            input_array = load_npy(input_path, layer=layer.name)
            if input_array.ndim != 4:
                raise FusionError.shape_mismatch(layer.name, [0, 0, 0, 0], input_array.shape, stage="input")
            if not np.issubdtype(input_array.dtype, np.number):
                raise FusionError.invalid_param(layer.name, None, "input.dtype", "must be numeric", dtype=str(input_array.dtype))
            if not np.all(np.isfinite(input_array)):
                count = int(np.count_nonzero(~np.isfinite(input_array)))
                raise FusionError.numeric(layer.name, "non_finite_input", non_finite_count=count)
            input_array = np.asarray(input_array, dtype=np.float64)
            in_channels = int(input_array.shape[1])
            conv_params = build_conv_params(layer.name, in_channels, layer.lateral, task.seed)
            oh, ow = conv_output_hw(
                (input_array.shape[2], input_array.shape[3]),
                layer.lateral.kernel_size,
                layer.lateral.stride,
                layer.lateral.padding,
            )
            actual_lateral_shape = (int(input_array.shape[0]), layer.lateral.out_channels, oh, ow)
            if actual_lateral_shape != layer.expected_output_shape:
                raise FusionError.shape_mismatch(
                    layer.name,
                    layer.expected_output_shape,
                    actual_lateral_shape,
                    stage="lateral_shape_plan",
                )
            try:
                normalize_attention(layer.attention, layer.lateral.out_channels, layer=layer.name)
            except FusionError:
                raise
            if layer.reference is None:
                raise FusionError.invalid_stage(
                    f"layer {layer.name!r} has no high-precision reference path",
                    layer=layer.name,
                )
            reference_path = resolve_under(self.input_dir, layer.reference, field_name=f"layers.{layer.name}.reference")
            if not reference_path.is_file():
                raise FusionError.invalid_stage(
                    f"reference output for layer {layer.name!r} is missing",
                    layer=layer.name,
                    path=str(reference_path),
                )
            reference_array = load_npy(reference_path, layer=layer.name)
            if tuple(reference_array.shape) != layer.expected_output_shape:
                raise FusionError.shape_mismatch(
                    layer.name,
                    layer.expected_output_shape,
                    reference_array.shape,
                    stage="reference_shape",
                )
            if not np.all(np.isfinite(reference_array)):
                count = int(np.count_nonzero(~np.isfinite(reference_array)))
                raise FusionError.numeric(layer.name, "non_finite_reference", non_finite_count=count)
            reference_array = np.asarray(reference_array, dtype=np.float64)
            weight, bias, conv_meta = conv_params
            components = {
                "input": sha256_file(input_path),
                "attention": sha256_json(layer.attention.to_dict()),
                "fusion": sha256_json(layer.fusion.to_dict()),
                "lateral": sha256_json(
                    {
                        "spec": layer.lateral.to_dict(),
                        "weight_sha256": sha256_array(weight),
                        "bias_sha256": sha256_array(bias),
                        "generated": conv_meta.get("seed_material"),
                    }
                ),
                "upsample": sha256_json(layer.upsample.to_dict() if layer.upsample is not None else None),
                "quantization": sha256_json(layer.quantization.to_dict()),
                "reference": sha256_file(reference_path),
                "output_contract": sha256_json(
                    {
                        "expected_output_shape": list(layer.expected_output_shape),
                        "allowed_range": list(layer.allowed_range),
                    }
                ),
                "config": sha256_json(
                    {
                        "task_seed": task.seed,
                        "layer": layer.to_dict(),
                        "algorithm": ALGORITHM_VERSION,
                        "stage_schema": 1,
                    }
                ),
            }
            own_fingerprint = sha256_json(components)
            layers[layer.name] = LayerPlan(
                layer=layer,
                input_path=input_path,
                input_bytes_hash=sha256_file(input_path),
                input_array=input_array,
                conv_params=conv_params,
                reference_path=reference_path,
                reference_bytes_hash=sha256_file(reference_path),
                reference_array=reference_array,
                component_fingerprints=components,
                own_fingerprint=own_fingerprint,
            )

        previous_layer: str | None = None
        previous_version: str | None = None
        for layer in task.processing_layers:
            plan = layers[layer.name]
            plan.dependency_layer = previous_layer
            plan.dependency_version = previous_version
            plan.stage_version = sha256_json({"own": plan.own_fingerprint, "dependency": previous_version})
            if previous_layer is not None:
                upstream = layers[previous_layer]
                if layer.upsample is None:
                    raise FusionError.invalid_param(layer.name, None, "upsample", "missing top-down connection")
                base_h = upstream.layer.expected_output_shape[2] * layer.upsample.scale
                base_w = upstream.layer.expected_output_shape[3] * layer.upsample.scale
                target = layer.expected_output_shape
                if (
                    target[0] != upstream.layer.expected_output_shape[0]
                    or target[1] != upstream.layer.expected_output_shape[1]
                    or abs(target[2] - base_h) > layer.upsample.max_tail
                    or abs(target[3] - base_w) > layer.upsample.max_tail
                ):
                    raise FusionError.shape_mismatch(
                        layer.name,
                        target,
                        [target[0], target[1], base_h, base_w],
                        stage="upsample_plan_alignment",
                    )
            previous_layer = layer.name
            previous_version = plan.stage_version

        if task.final_reference is None:
            raise FusionError.invalid_stage("task.final_reference is required for final quantization", details={"task_id": task.id})
        final_reference_path = resolve_under(self.input_dir, task.final_reference, field_name="task.final_reference")
        if not final_reference_path.is_file():
            raise FusionError.invalid_stage(
                "final reference output is missing",
                details={"task_id": task.id, "path": str(final_reference_path)},
            )
        final_reference_array = load_npy(final_reference_path, layer="final")
        if tuple(final_reference_array.shape) != task.finest_layer.expected_output_shape:
            raise FusionError.shape_mismatch(
                "final",
                task.finest_layer.expected_output_shape,
                final_reference_array.shape,
                stage="final_reference_shape",
            )
        if not np.all(np.isfinite(final_reference_array)):
            count = int(np.count_nonzero(~np.isfinite(final_reference_array)))
            raise FusionError.numeric("final", "non_finite_reference", non_finite_count=count)
        final_reference_array = np.asarray(final_reference_array, dtype=np.float64)
        processing_order = tuple(plan.name for plan in task.processing_layers)
        sensitivity_version = sha256_json(
            {
                "task_id": task.id,
                "sensitivity": task.sensitivity.to_dict(),
                "stages": {name: layers[name].stage_version for name in processing_order},
            }
        )
        overall_version = sha256_json(
            {
                "task": task.to_dict(),
                "stages": {name: layers[name].stage_version for name in processing_order},
                "sensitivity_version": sensitivity_version,
                "final_reference": sha256_file(final_reference_path),
            }
        )
        return TaskPlan(
            task=task,
            layers=layers,
            processing_order=processing_order,
            final_reference_path=final_reference_path,
            final_reference_hash=sha256_file(final_reference_path),
            final_reference_array=final_reference_array,
            sensitivity_version=sensitivity_version,
            overall_version=overall_version,
        )

    @staticmethod
    def _component_reasons(record: Mapping[str, Any], plan: LayerPlan) -> list[str]:
        reasons: list[str] = []
        stored = record.get("component_fingerprints") or {}
        for name, current in sorted(plan.component_fingerprints.items()):
            if stored.get(name) != current:
                reasons.append(f"{name}_changed")
        if record.get("dependency_version") != plan.dependency_version:
            if not reasons:
                reasons.append("dependency_invalidated")
            elif "dependency_invalidated" not in reasons:
                reasons.append("dependency_invalidated")
        return reasons or ["stage_version_changed"]

    def _incremental_plan(self, plan: TaskPlan, store: CheckpointStore, state: CheckpointState) -> dict[str, Any]:
        affected: list[str] = []
        reused: list[str] = []
        reasons: dict[str, list[str]] = {}
        records: dict[str, int | None] = {}
        for layer_name in plan.processing_order:
            layer_plan = plan.layers[layer_name]
            record = state.latest_for_layer(layer_name)
            records[layer_name] = int(record["sequence"]) if record else None
            if record is not None and record.get("stage_version") == layer_plan.stage_version:
                reused.append(layer_name)
            else:
                affected.append(layer_name)
                if record is None:
                    reasons[layer_name] = ["cold_start"] if not state.records else ["missing_checkpoint"]
                else:
                    reasons[layer_name] = self._component_reasons(record, layer_plan)
        sensitivity_report = store.read_report("sensitivity.json")
        sensitivity_current = bool(
            sensitivity_report
            and sensitivity_report.get("sensitivity_version") == plan.sensitivity_version
            and not affected
        )
        earliest = affected[0] if affected else None
        return {
            "schema_version": 1,
            "task_id": plan.task.id,
            "overall_version": plan.overall_version,
            "sensitivity_version": plan.sensitivity_version,
            "earliest_affected_layer": earliest,
            "earliest_affected_stage": earliest if earliest is not None else ("sensitivity" if not sensitivity_current else None),
            "affected_layers": affected,
            "reused_layers": reused,
            "reasons": reasons,
            "checkpoint_sequences": records,
            "sensitivity_invalidated": not sensitivity_current,
            "sensitivity_report_current": sensitivity_current,
            "mode": "cold_start" if not state.records else ("no_change" if not affected and sensitivity_current else "incremental"),
            "stage_versions": {name: plan.layers[name].stage_version for name in plan.processing_order},
        }

    def _read_stage_metadata(self, store: CheckpointStore, record: Mapping[str, Any]) -> dict[str, Any]:
        path = store.load_artifact(record, "metadata.json")
        value = read_json(path)
        if not isinstance(value, dict):
            raise FusionError.incremental_mismatch(
                str(record.get("layer")),
                reason="metadata_not_object",
                artifact="metadata.json",
            )
        return value

    def _load_stage(self, store: CheckpointStore, record: Mapping[str, Any]) -> StageMaterial:
        raw_path = store.load_artifact(record, "raw.npy")
        raw = load_npy(raw_path, layer=str(record.get("layer")))
        metadata = self._read_stage_metadata(store, record)
        quant_arrays = {
            "quantized.npy": load_npy(store.load_artifact(record, "quantized.npy"), layer=str(record.get("layer"))),
            "dequantized.npy": load_npy(store.load_artifact(record, "dequantized.npy"), layer=str(record.get("layer"))),
            "error_bound.npy": load_npy(store.load_artifact(record, "error_bound.npy"), layer=str(record.get("layer"))),
            "abs_reference_error.npy": load_npy(store.load_artifact(record, "abs_reference_error.npy"), layer=str(record.get("layer"))),
        }
        return StageMaterial(raw=np.asarray(raw, dtype=np.float64), metadata=metadata, record=dict(record), reused=True, quant_arrays=quant_arrays)

    def _stage_metadata(
        self,
        plan: TaskPlan,
        layer_plan: LayerPlan,
        output: LayerOutput,
        quant: QuantizationResult,
    ) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "task_id": plan.task.id,
            "layer": layer_plan.layer.name,
            "stage_version": layer_plan.stage_version,
            "own_fingerprint": layer_plan.own_fingerprint,
            "dependency_layer": layer_plan.dependency_layer,
            "dependency_version": layer_plan.dependency_version,
            "component_fingerprints": dict(sorted(layer_plan.component_fingerprints.items())),
            "reference": layer_plan.layer.reference,
            "diagnostics": output.diagnostics(),
            "quantization": quant.metadata,
        }

    def _compute_and_publish_stage(
        self,
        task: TaskSpec,
        plan: TaskPlan,
        store: CheckpointStore,
        layer_name: str,
        previous_raw: np.ndarray | None,
        *,
        stage_delay: float,
    ) -> StageMaterial:
        layer_plan = plan.layers[layer_name]
        layer = layer_plan.layer
        output = compute_layer_output(
            task,
            layer,
            layer_plan.input_array,
            layer_plan.conv_params,
            previous_raw,
        )
        quant = quantize_symmetric(
            output.raw,
            layer.quantization.bits,
            layer=layer.name,
            reference=layer_plan.reference_array,
            scale_override=layer.quantization.scale_override,
        )
        metadata = self._stage_metadata(plan, layer_plan, output, quant)
        files: dict[str, bytes] = {
            "raw.npy": array_bytes(output.raw),
            "metadata.json": pretty_json_bytes(metadata),
            **{name: array_bytes(array) for name, array in quant.artifact_arrays().items()},
        }
        if stage_delay > 0:
            time.sleep(stage_delay)
        prerequisites: list[tuple[str, str]] = []
        if layer_plan.dependency_layer is not None and layer_plan.dependency_version is not None:
            prerequisites.append((layer_plan.dependency_layer, layer_plan.dependency_version))
        record = store.publish_stage(
            layer=layer.name,
            stage_index=list(plan.processing_order).index(layer.name),
            stage_version=layer_plan.stage_version,
            own_fingerprint=layer_plan.own_fingerprint,
            dependency_version=layer_plan.dependency_version,
            component_fingerprints=layer_plan.component_fingerprints,
            artifact_files=files,
            prerequisites=prerequisites,
        )
        publication = record.get("publication")
        if publication == "reused":
            return self._load_stage(store, record)
        return StageMaterial(
            raw=output.raw,
            metadata=metadata,
            record=record,
            reused=False,
            quant_arrays=quant.artifact_arrays(),
        )

    def _sensitivity_report(
        self,
        plan: TaskPlan,
        raw_by_layer: Mapping[str, np.ndarray],
        store: CheckpointStore,
        *,
        recompute: bool,
    ) -> dict[str, Any]:
        existing = store.read_report("sensitivity.json")
        if not recompute and existing and existing.get("sensitivity_version") == plan.sensitivity_version:
            return existing
        final_layer = plan.task.finest_layer.name
        baseline_final = raw_by_layer[final_layer]
        baseline = FusionResult(
            task_id=plan.task.id,
            layer_outputs={},
            processing_order=plan.processing_order,
            final=baseline_final,
            final_sha256=sha256_array(baseline_final),
        )
        report = compute_sensitivity_report(
            plan.task,
            {name: plan.layers[name].input_array for name in plan.processing_order},
            {name: plan.layers[name].conv_params for name in plan.processing_order},
            baseline,
        )
        report["sensitivity_version"] = plan.sensitivity_version
        report["overall_version"] = plan.overall_version
        assert_sensitivity_report_valid(report)
        store.write_report("sensitivity.json", report)
        return report

    def _quantization_report(
        self,
        plan: TaskPlan,
        raw_by_layer: Mapping[str, np.ndarray],
        stages: Mapping[str, StageMaterial],
    ) -> tuple[dict[str, Any], QuantizationResult]:
        layer_reports: dict[str, Any] = {}
        for layer in plan.task.layers:
            material = stages[layer.name]
            layer_reports[layer.name] = {
                **material.metadata["quantization"],
                "error_bound_artifact": f"quantization/{layer.name}_error_bound.npy",
                "abs_reference_error_artifact": f"quantization/{layer.name}_abs_reference_error.npy",
            }
        final_layer = plan.task.finest_layer
        final_quant = quantize_symmetric(
            raw_by_layer[final_layer.name],
            final_layer.quantization.bits,
            layer="final",
            reference=plan.final_reference_array,
            scale_override=final_layer.quantization.scale_override,
        )
        report = {
            "schema_version": 1,
            "task_id": plan.task.id,
            "overall_version": plan.overall_version,
            "error_bound_definition": "0.5 * explicit_scale + numeric_reference_slack",
            "layers": layer_reports,
            "final": {
                **final_quant.metadata,
                "source_layer": final_layer.name,
                "final_reference_sha256": plan.final_reference_hash,
                "error_bound_artifact": "quantization/final_error_bound.npy",
                "abs_reference_error_artifact": "quantization/final_abs_reference_error.npy",
                "quantized_artifact": "quantization/final_quantized.npy",
                "dequantized_artifact": "quantization/final_dequantized.npy",
            },
        }
        return report, final_quant

    def _export_completed(
        self,
        plan: TaskPlan,
        store: CheckpointStore,
        stages: Mapping[str, StageMaterial],
        raw_by_layer: Mapping[str, np.ndarray],
        sensitivity: dict[str, Any],
    ) -> dict[str, Any]:
        output_dir = self.workspace / "outputs" / plan.task.id
        for layer in plan.task.layers:
            write_npy(output_dir / f"{layer.name}.npy", raw_by_layer[layer.name])
        write_npy(output_dir / "final.npy", raw_by_layer[plan.task.finest_layer.name])
        quant_report, final_quant = self._quantization_report(plan, raw_by_layer, stages)
        for layer_name, material in stages.items():
            for array_name, array in material.quant_arrays.items():
                write_npy(output_dir / "quantization" / f"{layer_name}_{array_name}", array)
        for array_name, array in final_quant.artifact_arrays().items():
            write_npy(output_dir / "quantization" / f"final_{array_name}", array)
        write_json(output_dir / "reports" / "quantization.json", quant_report)
        write_json(output_dir / "reports" / "sensitivity.json", sensitivity)
        store.write_report("quantization.json", quant_report)
        store.write_report("sensitivity.json", sensitivity)
        return {
            "output_dir": str(output_dir),
            "quantization_report": quant_report,
            "final_quantization": final_quant.metadata,
        }

    def run(
        self,
        task_id: str | None = None,
        *,
        max_stages: int | None = None,
        stop_after_layer: str | None = None,
        stage_delay: float = 0.0,
        crash_after_layer: str | None = None,
        resume: bool = False,
        verify_full: bool = False,
    ) -> dict[str, Any]:
        task = self._resolve_task(task_id)
        plan = self._build_plan(task)
        store = self.store(task)
        if resume:
            store.clear_stop()
        state = store.scan(repair_head=True)
        invalidation = self._incremental_plan(plan, store, state)
        store.write_report("incremental.json", invalidation)
        stop_limit = len(plan.processing_order)
        if max_stages is not None:
            stop_limit = max(0, min(stop_limit, int(max_stages)))
        if stop_after_layer is not None:
            if stop_after_layer not in plan.processing_order:
                raise FusionError.invalid_stage(f"unknown stop_after layer {stop_after_layer!r}", layer=stop_after_layer)
            stop_limit = min(stop_limit, list(plan.processing_order).index(stop_after_layer) + 1)

        raw_by_layer: dict[str, np.ndarray] = {}
        stages: dict[str, StageMaterial] = {}
        reused: list[str] = []
        computed: list[str] = []
        status = "completed"
        if store.stop_requested():
            status = "stopped"
        else:
            for index, layer_name in enumerate(plan.processing_order):
                if index >= stop_limit:
                    status = "partial"
                    break
                if index > 0 and store.stop_requested():
                    status = "stopped"
                    break
                state = store.scan(repair_head=True)
                current = state.latest_for_layer(layer_name)
                layer_plan = plan.layers[layer_name]
                if current is not None and current.get("stage_version") == layer_plan.stage_version:
                    material = self._load_stage(store, current)
                    reused.append(layer_name)
                else:
                    previous_raw = None
                    if layer_plan.dependency_layer is not None:
                        previous_raw = raw_by_layer.get(layer_plan.dependency_layer)
                        if previous_raw is None:
                            dep_record = state.latest_for_layer(layer_plan.dependency_layer)
                            if dep_record is None or dep_record.get("stage_version") != layer_plan.dependency_version:
                                raise FusionError.invalid_stage(
                                    f"missing prerequisite stage {layer_plan.dependency_layer!r}",
                                    layer=layer_name,
                                    prerequisite_layer=layer_plan.dependency_layer,
                                )
                            previous_raw = self._load_stage(store, dep_record).raw
                    material = self._compute_and_publish_stage(
                        task,
                        plan,
                        store,
                        layer_name,
                        previous_raw,
                        stage_delay=stage_delay,
                    )
                    if material.reused:
                        reused.append(layer_name)
                    else:
                        computed.append(layer_name)
                stages[layer_name] = material
                raw_by_layer[layer_name] = material.raw
                if crash_after_layer == layer_name:
                    os._exit(137)
                if store.stop_requested():
                    status = "stopped"
                    break

        result: dict[str, Any] = {
            "ok": status in {"completed", "stopped", "partial"},
            "task_id": task.id,
            "status": status,
            "processing_order": list(plan.processing_order),
            "reused_layers": reused,
            "computed_layers": computed,
            "completed_layers": [name for name in plan.processing_order if name in raw_by_layer],
            "invalidation": invalidation,
            "overall_version": plan.overall_version,
            "sensitivity_version": plan.sensitivity_version,
        }
        if status == "completed":
            sensitivity = self._sensitivity_report(plan, raw_by_layer, store, recompute=invalidation["sensitivity_invalidated"])
            exported = self._export_completed(plan, store, stages, raw_by_layer, sensitivity)
            final_state = store.scan(repair_head=True)
            run_report = {
                "schema_version": 1,
                "task_id": task.id,
                "status": "completed",
                "overall_version": plan.overall_version,
                "sensitivity_version": plan.sensitivity_version,
                "processing_order": list(plan.processing_order),
                "computed_layers": computed,
                "reused_layers": reused,
                "head_sequence": final_state.head_sequence,
                "head_digest": final_state.head_digest,
                "output_sha256": {name: sha256_array(raw_by_layer[name]) for name in plan.processing_order},
                "sensitivity_ranking": sensitivity["replay_ranking"],
                "review_required_layers": sensitivity["review_required_layers"],
                "output_dir": exported["output_dir"],
            }
            store.write_report("run.json", run_report)
            result.update(
                {
                    "final_sha256": sha256_array(raw_by_layer[plan.task.finest_layer.name]),
                    "sensitivity": sensitivity,
                    "quantization": exported["quantization_report"],
                    "final_quantization": exported["final_quantization"],
                    "output_dir": exported["output_dir"],
                    "head_sequence": final_state.head_sequence,
                }
            )
        if "head_sequence" not in result:
            current_state = store.scan(repair_head=True)
            result["head_sequence"] = current_state.head_sequence
            result["head_digest"] = current_state.head_digest
        if verify_full:
            if status == "completed":
                result["verification"] = self.verify_incremental(task.id)
            else:
                result["verification"] = {
                    "ok": False,
                    "skipped": True,
                    "reason": "task_did_not_complete",
                    "status": status,
                }
        return result

    def resume(
        self,
        task_id: str | None = None,
        *,
        stage_delay: float = 0.0,
        crash_after_layer: str | None = None,
        verify_full: bool = False,
    ) -> dict[str, Any]:
        return self.run(
            task_id,
            stage_delay=stage_delay,
            crash_after_layer=crash_after_layer,
            resume=True,
            verify_full=verify_full,
        )

    def run_until(self, layer: str, task_id: str | None = None, *, stage_delay: float = 0.0) -> dict[str, Any]:
        task = self._resolve_task(task_id)
        if layer not in {item.name for item in task.processing_layers}:
            raise FusionError.invalid_stage(f"unknown target layer {layer!r}", layer=layer)
        return self.run(task.id, stop_after_layer=layer, stage_delay=stage_delay)

    def stop(self, task_id: str | None = None, *, reason: str = "cli_request") -> dict[str, Any]:
        task = self._resolve_task(task_id)
        path = self.store(task).write_stop(reason=reason)
        return {"ok": True, "task_id": task.id, "stop_requested": True, "marker": str(path)}

    def status(self, task_id: str | None = None) -> dict[str, Any]:
        task = self._resolve_task(task_id)
        plan = self._build_plan(task)
        store = self.store(task)
        state = store.scan(repair_head=False)
        layers: dict[str, Any] = {}
        for layer_name in plan.processing_order:
            layer_plan = plan.layers[layer_name]
            record = state.latest_for_layer(layer_name)
            current = bool(record and record.get("stage_version") == layer_plan.stage_version)
            layers[layer_name] = {
                "stage_version": layer_plan.stage_version,
                "current": current,
                "record_sequence": int(record["sequence"]) if record else None,
                "record_stage_version": record.get("stage_version") if record else None,
                "record_layer": record.get("layer") if record else None,
                "artifact": (
                    str(store.artifacts_dir / record["stage_version"] / "raw.npy") if current and record else None
                ),
                "artifact_sha256": (
                    record.get("artifact_summary", {}).get("files", {}).get("raw.npy") if current and record else None
                ),
            }
        sensitivity_report = store.read_report("sensitivity.json")
        sensitivity_current = bool(
            sensitivity_report and sensitivity_report.get("sensitivity_version") == plan.sensitivity_version
        )
        return {
            "ok": True,
            "task_id": task.id,
            "overall_version": plan.overall_version,
            "sensitivity_version": plan.sensitivity_version,
            "head_sequence": state.head_sequence,
            "head_digest": state.head_digest,
            "head_repair_pending": state.head_repair_needed,
            "stop_requested": store.stop_requested(),
            "checkpoint_records": len(state.records),
            "layers": layers,
            "sensitivity": {
                "current": sensitivity_current,
                "ranking": sensitivity_report.get("replay_ranking") if sensitivity_current and sensitivity_report else None,
            },
            "incremental": store.read_report("incremental.json"),
        }

    def sensitivity(self, task_id: str | None = None) -> dict[str, Any]:
        task = self._resolve_task(task_id)
        plan = self._build_plan(task)
        report = self.store(task).read_report("sensitivity.json")
        if not report:
            raise FusionError.invalid_stage("sensitivity report is unavailable; run the task first", details={"task_id": task.id})
        current = report.get("sensitivity_version") == plan.sensitivity_version
        return {"ok": True, "task_id": task.id, "current": current, "report": report}

    def verify_checkpoints(self, task_id: str | None = None) -> dict[str, Any]:
        task = self._resolve_task(task_id)
        return self.store(task).verify()

    def verify_incremental(self, task_id: str | None = None) -> dict[str, Any]:
        task = self._resolve_task(task_id)
        plan = self._build_plan(task)
        store = self.store(task)
        state = store.scan(repair_head=True)
        fresh = run_fusion(
            task,
            {name: plan.layers[name].input_array for name in plan.processing_order},
            {name: plan.layers[name].conv_params for name in plan.processing_order},
        )
        for layer_name in plan.processing_order:
            layer_plan = plan.layers[layer_name]
            record = state.latest_for_layer(layer_name)
            if record is None or record.get("stage_version") != layer_plan.stage_version:
                raise FusionError.invalid_stage(
                    f"layer {layer_name!r} does not have a current checkpoint",
                    layer=layer_name,
                )
            material = self._load_stage(store, record)
            expected = fresh.layer_outputs[layer_name].raw
            if array_bytes(material.raw) != array_bytes(expected):
                mismatch = np.flatnonzero(np.asarray(material.raw != expected).ravel())
                index = int(mismatch[0]) if mismatch.size else None
                raise FusionError.incremental_mismatch(
                    layer_name,
                    index=index,
                    reason="raw_tensor_diff",
                    expected=sha256_array(expected),
                    actual=sha256_array(material.raw),
                )
            fresh_quant = quantize_symmetric(
                expected,
                layer_plan.layer.quantization.bits,
                layer=layer_name,
                reference=layer_plan.reference_array,
                scale_override=layer_plan.layer.quantization.scale_override,
            )
            if canonical_json_bytes(fresh_quant.metadata) != canonical_json_bytes(material.metadata.get("quantization")):
                raise FusionError.incremental_mismatch(
                    layer_name,
                    reason="quantization_metadata_diff",
                    expected=sha256_json(fresh_quant.metadata),
                    actual=sha256_json(material.metadata.get("quantization")),
                )
        raw_by_layer = {name: fresh.layer_outputs[name].raw for name in plan.processing_order}
        stages = {
            name: self._load_stage(store, state.latest_for_layer(name))
            for name in plan.processing_order
        }
        quant_report, _ = self._quantization_report(plan, raw_by_layer, stages)
        stored_quant_report = store.read_report("quantization.json")
        if stored_quant_report is None or canonical_json_bytes(quant_report) != canonical_json_bytes(stored_quant_report):
            raise FusionError.incremental_mismatch(
                "quantization",
                reason="quantization_report_diff",
                expected=sha256_json(quant_report),
                actual=sha256_json(stored_quant_report),
            )
        baseline = FusionResult(
            task_id=task.id,
            layer_outputs={},
            processing_order=plan.processing_order,
            final=fresh.final,
            final_sha256=fresh.final_sha256,
        )
        fresh_sensitivity = compute_sensitivity_report(
            task,
            {name: plan.layers[name].input_array for name in plan.processing_order},
            {name: plan.layers[name].conv_params for name in plan.processing_order},
            baseline,
        )
        fresh_sensitivity["sensitivity_version"] = plan.sensitivity_version
        fresh_sensitivity["overall_version"] = plan.overall_version
        stored_sensitivity = store.read_report("sensitivity.json")
        if stored_sensitivity is None or canonical_json_bytes(fresh_sensitivity) != canonical_json_bytes(stored_sensitivity):
            raise FusionError.incremental_mismatch(
                "sensitivity",
                reason="sensitivity_report_diff",
                expected=sha256_json(fresh_sensitivity),
                actual=sha256_json(stored_sensitivity),
            )
        return {
            "ok": True,
            "task_id": task.id,
            "overall_version": plan.overall_version,
            "layers_verified": list(plan.processing_order),
            "head_sequence": state.head_sequence,
            "result": "incremental_matches_full_rebuild",
        }
