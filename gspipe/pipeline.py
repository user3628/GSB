"""End-to-end four-stage pipeline orchestration."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import numpy as np

from .artifacts import ArtifactRef, ArtifactStore
from .cache import ExecutionCache, ExecutionCacheHit, QueryCache
from .canonical import canonical_json_bytes, normalize_instruction, sha256_bytes, sha256_json
from .checkpoints import CheckpointState, CheckpointStore, build_checkpoint_record
from .config import PipelineConfig
from .errors import (
    E_ARTIFACT_CORRUPT,
    E_INVALID_CHAIN,
    E_INVALID_STAGE,
    E_RESULT_UNAVAILABLE,
    E_TASK_NOT_FOUND,
    PipelineError,
    internal_error,
    invalid_stage,
)
from .fsutil import atomic_write_bytes, atomic_write_json, ensure_dir
from .invalidations import InvalidationStore
from .metrics import MetricsStore
from .models import TaskSpec
from .ops import apply_operator, estimate_chain_steps, operator_steps
from .parser import Operator, ParsedChain, parse_instruction
from .pngio import decode_rgb_png, encode_rgb_png, pixel_fingerprint
from .sample_data import generate_samples
from .scheduler import SchedulingPlan, TaskEstimate, build_schedule
from .stages import STAGE_INDEX, STAGES, normalize_stage_list
from .workspace import InputShard, Workspace


ProgressCallback = Callable[[dict[str, Any]], None]


_UPSTREAM_KEYS = {"parse_output", "execute_output", "verify_output"}

_COMPONENT_REASONS = {
    "input_pixels": "input_pixels_changed",
    "input_size": "input_shape_changed",
    "instruction": "instruction_changed",
    "allowed_ops": "allowed_ops_changed",
    "parse_config_version": "parse_config_changed",
    "parse_output": "upstream_parse_changed",
    "kernel_config_version": "kernel_version_changed",
    "scheduler_config_version": "scheduling_budget_changed",
    "latency_budget_ms": "latency_budget_changed",
    "execute_output": "upstream_execute_changed",
    "expected_output": "expected_output_changed",
    "expected_summary": "expected_summary_changed",
    "verify_config_version": "verify_config_changed",
    "verify_output": "upstream_verify_changed",
    "export_config_version": "export_config_changed",
}


def _parse_artifact(
    task: TaskSpec,
    shard: InputShard,
    normalized_instruction: str,
    parsed: ParsedChain | None,
    error: dict[str, Any] | None,
) -> dict[str, Any]:
    artifact: dict[str, Any] = {
        "schema": "gspipe-parse-artifact-v1",
        "task_id": task.task_id,
        "status": "ERROR" if error else "OK",
        "normalized_instruction": normalized_instruction,
        "allowed_ops": list(task.allowed_ops),
        "input_size": [int(shard.image.shape[1]), int(shard.image.shape[0])],
        "input_pixels": shard.info["pixel_sha256"],
        "error": error,
    }
    if parsed is not None:
        artifact.update(
            {
                "operators": [operator.to_dict() for operator in parsed.operators],
                "chain_summary": parsed.chain_summary,
                "output_size": [int(parsed.output_size[0]), int(parsed.output_size[1])],
            }
        )
    else:
        artifact.update({"operators": [], "chain_summary": None, "output_size": None})
    return artifact


def _execute_status_from_verify(verify_status: str) -> str:
    if verify_status == "VERIFIED":
        return "PROCESSED"
    if verify_status in {"MISMATCH", "VERIFIED_WITH_MISMATCH"}:
        return "MISMATCH"
    return "UNPROCESSED"


@dataclass
class TaskInspection:
    task: TaskSpec
    shard: InputShard
    state: CheckpointState
    normalized_instruction: str
    parsed: ParsedChain | None
    parse_error: dict[str, Any] | None
    parse_artifact: dict[str, Any]
    parse_artifact_digest: str
    components: dict[str, dict[str, Any]]
    fingerprints: dict[str, str]
    config_versions: dict[str, str]
    reuse: dict[str, bool]
    earliest_stage: str | None
    changed_components: list[str]
    reasons: list[str]
    target_generation: int
    invalidation_event: dict[str, Any] | None = None

    @property
    def task_id(self) -> str:
        return self.task.task_id

    @property
    def chain_steps(self) -> int:
        if self.parsed is None:
            return 0
        steps, _ = estimate_chain_steps(self.parsed.operators, self.parsed.input_size)
        return steps


@dataclass
class ExecutionOutcome:
    task_id: str
    artifact: dict[str, Any]
    artifact_ref: ArtifactRef | None
    metrics: dict[str, Any]
    error: dict[str, Any] | None = None


class Pipeline:
    def __init__(
        self,
        workspace: str | Path,
        *,
        config: PipelineConfig | None = None,
        on_progress: ProgressCallback | None = None,
    ) -> None:
        self.workspace = Workspace(workspace)
        self.workspace.ensure_layout()
        self.config = config if config is not None else self.workspace.load_config()
        self.artifacts = ArtifactStore(self.workspace.root)
        self.checkpoints = CheckpointStore(self.workspace.root, self.artifacts)
        self.execution_cache = ExecutionCache(self.workspace.root, self.artifacts)
        self.query_cache = QueryCache(self.workspace.root)
        self.invalidations = InvalidationStore(self.workspace.root)
        self.metrics = MetricsStore(self.workspace.root)
        self.on_progress = on_progress

    def _emit(self, event: dict[str, Any]) -> None:
        if self.on_progress is not None:
            self.on_progress(event)

    def _load_inputs(self) -> tuple[list[TaskSpec], dict[str, InputShard]]:
        tasks = self.workspace.load_tasks()
        if len(tasks) > int(self.config.limits["max_tasks"]):
            raise PipelineError(
                "E_INVALID_TASKS",
                "Task count exceeds configured limit",
                details={"task_count": len(tasks), "max_tasks": self.config.limits["max_tasks"]},
            )
        shards: dict[str, InputShard] = {}
        for task in tasks:
            if task.input_filename not in shards:
                shards[task.input_filename] = self.workspace.decode_shard(task.input_filename, self.config)
        return tasks, shards

    def _inspect_task(self, task: TaskSpec, shard: InputShard) -> TaskInspection:
        normalized = normalize_instruction(task.instruction)
        parsed: ParsedChain | None = None
        parse_error: dict[str, Any] | None = None
        try:
            parsed = parse_instruction(
                task.task_id,
                task.instruction,
                task.allowed_ops,
                (int(shard.image.shape[1]), int(shard.image.shape[0])),
                max_chain_length=int(self.config.parse["max_chain_length"]),
                max_image_side=int(self.config.limits["max_image_side"]),
                max_image_pixels=int(self.config.limits["max_image_pixels"]),
            )
        except PipelineError as exc:
            if exc.code != E_INVALID_CHAIN:
                raise
            parse_error = exc.to_dict()
        parse_artifact = _parse_artifact(task, shard, normalized, parsed, parse_error)
        parse_digest = sha256_json(parse_artifact)
        parse_components = {
            "input_pixels": shard.info["pixel_sha256"],
            "input_size": [int(shard.image.shape[1]), int(shard.image.shape[0])],
            "instruction": normalized,
            "allowed_ops": list(task.allowed_ops),
            "parse_config_version": self.config.parse_version,
        }
        execute_components = {
            "parse_output": parse_digest,
            "kernel_config_version": self.config.kernel_version,
            "scheduler_config_version": self.config.scheduler_version,
            "latency_budget_ms": int(task.latency_budget_ms),
        }
        components: dict[str, dict[str, Any]] = {"parse": parse_components, "execute": execute_components}
        fingerprints: dict[str, str] = {
            "parse": sha256_json(parse_components),
            "execute": sha256_json(execute_components),
        }
        config_versions = {
            "parse": self.config.parse_version,
            "execute": self.config.execute_version,
            "verify": self.config.verify_version,
            "export": self.config.export_version,
        }
        state = self.checkpoints.state(task.task_id)
        reuse: dict[str, bool] = {stage: False for stage in STAGES}

        parse_record = state.latest("parse")
        reuse["parse"] = bool(
            parse_record is not None
            and state.is_valid("parse")
            and parse_record.get("input_fingerprint") == fingerprints["parse"]
            and parse_record.get("fingerprint_components") == parse_components
            and parse_record.get("artifact_digest") == parse_digest
        )

        execute_record = state.latest("execute")
        if reuse["parse"]:
            reuse["execute"] = bool(
                execute_record is not None
                and state.is_valid("execute")
                and execute_record.get("input_fingerprint") == fingerprints["execute"]
                and execute_record.get("fingerprint_components") == execute_components
            )

        if reuse["execute"]:
            execute_artifact_digest = str(execute_record["artifact_digest"])
            verify_components = {
                "execute_output": execute_artifact_digest,
                "expected_output": [int(task.expected_width), int(task.expected_height)],
                "expected_summary": task.expected_summary,
                "verify_config_version": self.config.verify_version,
            }
            components["verify"] = verify_components
            fingerprints["verify"] = sha256_json(verify_components)
            verify_record = state.latest("verify")
            reuse["verify"] = bool(
                verify_record is not None
                and state.is_valid("verify")
                and verify_record.get("input_fingerprint") == fingerprints["verify"]
                and verify_record.get("fingerprint_components") == verify_components
            )
            if reuse["verify"]:
                verify_artifact_digest = str(verify_record["artifact_digest"])
                export_components = {
                    "verify_output": verify_artifact_digest,
                    "export_config_version": self.config.export_version,
                }
                components["export"] = export_components
                fingerprints["export"] = sha256_json(export_components)
                export_record = state.latest("export")
                reuse["export"] = bool(
                    export_record is not None
                    and state.is_valid("export")
                    and export_record.get("input_fingerprint") == fingerprints["export"]
                    and export_record.get("fingerprint_components") == export_components
                )

        earliest_stage: str | None = None
        changed_components: list[str] = []
        reasons: list[str] = []
        for stage in STAGES:
            if reuse[stage]:
                continue
            earliest_stage = stage
            record = state.latest(stage)
            expected_components = components.get(stage)
            if record is not None and expected_components is not None:
                old_components = record.get("fingerprint_components")
                if isinstance(old_components, dict):
                    changed_components = sorted(
                        key
                        for key in set(old_components) | set(expected_components)
                        if old_components.get(key) != expected_components.get(key)
                    )
                artifact_ok = self.artifacts.exists(
                    str(record.get("artifact_digest", "")), str(record.get("artifact_type", "json"))
                )
                if changed_components:
                    reasons = [_COMPONENT_REASONS.get(key, f"component_changed:{key}") for key in changed_components]
                elif not artifact_ok:
                    reasons = ["checkpoint_artifact_missing"]
                elif not state.is_valid(stage):
                    reasons = ["upstream_dependency_invalidated"]
                else:
                    reasons = ["checkpoint_changed"]
            elif record is None:
                reasons = ["checkpoint_missing"]
            else:
                reasons = ["upstream_dependency_invalidated"]
            break

        target_generation = state.generation
        if earliest_stage is not None:
            if not state.records:
                target_generation = 0
            else:
                latest_record = max(state.records, key=lambda item: int(item["seq"]))
                latest_index = STAGE_INDEX[str(latest_record["stage"])]
                earliest_index = STAGE_INDEX[earliest_stage]
                if latest_index >= earliest_index:
                    target_generation = state.generation + 1
                else:
                    target_generation = state.generation

        return TaskInspection(
            task=task,
            shard=shard,
            state=state,
            normalized_instruction=normalized,
            parsed=parsed,
            parse_error=parse_error,
            parse_artifact=parse_artifact,
            parse_artifact_digest=parse_digest,
            components=components,
            fingerprints=fingerprints,
            config_versions=config_versions,
            reuse=reuse,
            earliest_stage=earliest_stage,
            changed_components=changed_components,
            reasons=reasons,
            target_generation=target_generation,
        )

    def inspect(self) -> list[TaskInspection]:
        tasks, shards = self._load_inputs()
        return [self._inspect_task(task, shards[task.input_filename]) for task in tasks]

    def _record_invalidation(self, inspection: TaskInspection) -> None:
        if inspection.earliest_stage is None or not inspection.state.records:
            return
        earliest = inspection.earliest_stage
        prior = inspection.state.latest(earliest)
        if prior is None:
            # A missing stage in a partially built chain is not a version change.
            return
        new_fingerprint = inspection.fingerprints.get(earliest)
        event = {
            "schema": "gspipe-invalidation-v1",
            "task_id": inspection.task_id,
            "earliest_stage": earliest,
            "reasons": list(inspection.reasons),
            "changed_components": list(inspection.changed_components),
            "old_generation": int(prior.get("generation", 0)),
            "new_generation": int(inspection.target_generation),
            "old_fingerprint": prior.get("input_fingerprint"),
            "new_fingerprint": new_fingerprint,
            "old_artifact_digest": prior.get("artifact_digest"),
            "detected_seq": int(inspection.state.last_seq),
        }
        inspection.invalidation_event = self.invalidations.record(event)

    def _validate_requested_stages(
        self, requested: list[str], inspections: Sequence[TaskInspection]
    ) -> int:
        start_index = STAGE_INDEX[requested[0]]
        expected_segment = list(STAGES[start_index : start_index + len(requested)])
        if requested != expected_segment:
            raise invalid_stage(
                requested[0],
                "non_contiguous_stage_request",
                "Requested stages must be a contiguous canonical segment such as parse,execute or verify,export",
                requested=requested,
                expected=expected_segment,
            )
        for inspection in inspections:
            if inspection.earliest_stage is None:
                continue
            earliest_index = STAGE_INDEX[inspection.earliest_stage]
            if earliest_index < start_index:
                raise invalid_stage(
                    inspection.earliest_stage,
                    "missing_or_stale_predecessor",
                    "A requested stage has an invalid or missing predecessor; include the earlier stage in this run",
                    task_id=inspection.task_id,
                    requested_start=requested[0],
                    earliest_invalid_stage=inspection.earliest_stage,
                )
        return start_index

    def _dependencies_for(self, state: CheckpointState, stage: str) -> dict[str, str]:
        index = STAGE_INDEX[stage]
        if index == 0:
            return {}
        prior_stage = STAGES[index - 1]
        prior = state.latest(prior_stage)
        if prior is None or not state.is_valid(prior_stage):
            raise invalid_stage(
                stage,
                "missing_predecessor",
                f"Cannot publish {stage}: predecessor {prior_stage} is missing or invalid",
                predecessor=prior_stage,
            )
        return {prior_stage: str(prior["artifact_digest"])}

    def _publish_stage(
        self,
        inspection: TaskInspection,
        stage: str,
        artifact: Mapping[str, Any],
        components: Mapping[str, Any],
        *,
        artifact_ref: ArtifactRef | None = None,
    ) -> dict[str, Any]:
        ref = artifact_ref or self.artifacts.put_json(artifact)
        state = self.checkpoints.state(inspection.task_id)
        dependencies = self._dependencies_for(state, stage)
        generation = inspection.target_generation if state.records else 0
        input_fingerprint = sha256_json(components)
        record = build_checkpoint_record(
            task_id=inspection.task_id,
            stage=stage,
            seq=state.last_seq + 1,
            generation=generation,
            input_fingerprint=input_fingerprint,
            config_version=inspection.config_versions[stage],
            fingerprint_components=components,
            artifact_digest=ref.digest,
            artifact_type=ref.kind,
            artifact_path=ref.relative_path,
            dependencies=dependencies,
        )
        published = self.checkpoints.publish(inspection.task_id, record)
        inspection.state = self.checkpoints.state(inspection.task_id)
        inspection.components[stage] = dict(components)
        inspection.fingerprints[stage] = input_fingerprint
        self._emit(
            {
                "event": "checkpoint_published",
                "task_id": inspection.task_id,
                "stage": stage,
                "seq": published["seq"],
                "generation": published["generation"],
                "artifact_digest": published["artifact_digest"],
            }
        )
        return published

    def _run_parse_stage(self, inspections: Sequence[TaskInspection]) -> tuple[dict[str, Any], bool]:
        written = 0
        reused = 0
        errors: list[dict[str, Any]] = []
        stopped = False
        for inspection in inspections:
            if self.workspace.stop_requested():
                stopped = True
                break
            if inspection.reuse["parse"]:
                reused += 1
                continue
            self._publish_stage(
                inspection,
                "parse",
                inspection.parse_artifact,
                inspection.components["parse"],
            )
            written += 1
            if inspection.parse_error is not None:
                errors.append(
                    {
                        "task_id": inspection.task_id,
                        "stage": "parse",
                        "error": inspection.parse_error,
                    }
                )
        return {"stage": "parse", "checkpoints_written": written, "reused": reused, "errors": errors}, stopped

    def _base_execute_artifact(
        self,
        inspection: TaskInspection,
        *,
        status: str,
        reason: str | None,
        scheduler: Mapping[str, Any] | None = None,
        error: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        chain_summary = inspection.parsed.chain_summary if inspection.parsed is not None else None
        output_size = (
            [int(inspection.parsed.output_size[0]), int(inspection.parsed.output_size[1])]
            if inspection.parsed is not None
            else None
        )
        artifact: dict[str, Any] = {
            "schema": "gspipe-execute-artifact-v1",
            "task_id": inspection.task_id,
            "status": status,
            "reason": reason,
            "chain_summary": chain_summary,
            "output_size": output_size,
            "estimated_steps": inspection.chain_steps,
            "steps_used": 0,
            "latency_budget_ms": int(inspection.task.latency_budget_ms),
            "chunks": [],
            "scheduler": dict(scheduler or {}),
            "parse_error": inspection.parse_error,
            "error": error,
        }
        return artifact

    def _execute_one(
        self,
        inspection: TaskInspection,
        scheduled: Any,
        plan: SchedulingPlan,
    ) -> ExecutionOutcome:
        if inspection.parsed is None:
            artifact = self._base_execute_artifact(
                inspection,
                status="SKIPPED",
                reason="INVALID_CHAIN",
                scheduler={"lane": scheduled.lane, "estimated_steps": scheduled.estimated_steps},
            )
            return ExecutionOutcome(inspection.task_id, artifact, None, {"elapsed_ns": 0, "cache_hit": False})

        parsed = inspection.parsed
        chain_payload = [operator.to_dict() for operator in parsed.operators]
        cache_key = self.execution_cache.key_for(
            input_pixels=inspection.shard.info["pixel_sha256"],
            chain=chain_payload,
            kernel_version=self.config.kernel_version,
        )
        started_ns = time.perf_counter_ns()
        cache_hit: ExecutionCacheHit | None = None
        if self.config.cache_enabled:
            cache_hit = self.execution_cache.get(cache_key)

        runtime_chunks: list[dict[str, Any]] = []
        if cache_hit is not None:
            metadata = cache_hit.metadata
            deterministic_chunks = list(metadata.get("chunks", []))
            output_size = [int(metadata["output_size"][0]), int(metadata["output_size"][1])]
            output_pixel_digest = str(metadata["output_pixel_digest"])
            output_png_digest = str(metadata["output_png_digest"])
            png_bytes = cache_hit.png_bytes
            for chunk in deterministic_chunks:
                runtime_chunks.append(
                    {
                        "index": int(chunk["index"]),
                        "operator": chunk["operator"],
                        "estimated_steps": int(chunk["estimated_steps"]),
                        "cumulative_steps": int(chunk["cumulative_steps"]),
                        "elapsed_ms": 0.0,
                    }
                )
        else:
            image = np.array(inspection.shard.image, dtype=np.uint8, copy=True, order="C")
            deterministic_chunks = []
            cumulative_steps = 0
            for index, operator in enumerate(parsed.operators, start=1):
                before_size = (int(image.shape[1]), int(image.shape[0]))
                chunk_started = time.perf_counter_ns()
                image = apply_operator(image, operator.name, operator.params, task_id=inspection.task_id)
                chunk_elapsed_ns = time.perf_counter_ns() - chunk_started
                steps = operator_steps(operator.name, operator.params, before_size)
                cumulative_steps += steps
                after_size = (int(image.shape[1]), int(image.shape[0]))
                chunk = {
                    "index": index,
                    "operator": operator.name,
                    "params": dict(sorted(operator.params.items())),
                    "estimated_steps": steps,
                    "cumulative_steps": cumulative_steps,
                    "output_size": [after_size[0], after_size[1]],
                    "output_pixel_digest": pixel_fingerprint(image),
                }
                deterministic_chunks.append(chunk)
                runtime_chunks.append(
                    {
                        "index": index,
                        "operator": operator.name,
                        "estimated_steps": steps,
                        "cumulative_steps": cumulative_steps,
                        "elapsed_ms": round(chunk_elapsed_ns / 1_000_000.0, 6),
                    }
                )
            png_bytes = encode_rgb_png(image)
            output_png_digest = sha256_bytes(png_bytes)
            output_pixel_digest = pixel_fingerprint(image)
            output_size = [int(image.shape[1]), int(image.shape[0])]
            png_ref = self.artifacts.put_png(png_bytes)
            if png_ref.digest != output_png_digest:
                raise PipelineError(E_ARTIFACT_CORRUPT, "PNG artifact digest mismatch")
            metadata = {
                "schema": "gspipe-execution-cache-v1",
                "chain_summary": parsed.chain_summary,
                "output_size": output_size,
                "output_png_digest": output_png_digest,
                "output_pixel_digest": output_pixel_digest,
                "chunks": deterministic_chunks,
            }
            if self.config.cache_enabled:
                self.execution_cache.put(cache_key, metadata, png_bytes)

        elapsed_ns = time.perf_counter_ns() - started_ns
        latency_budget_ms = int(inspection.task.latency_budget_ms)
        within_latency = elapsed_ns <= latency_budget_ms * 1_000_000
        status = "PROCESSED" if within_latency else "UNPROCESSED"
        reason = None if within_latency else "LATENCY_BUDGET_EXCEEDED"
        artifact: dict[str, Any] = {
            "schema": "gspipe-execute-artifact-v1",
            "task_id": inspection.task_id,
            "status": status,
            "reason": reason,
            "chain_summary": parsed.chain_summary,
            "output_size": output_size if within_latency else None,
            "computed_output_size": output_size,
            "estimated_steps": scheduled.estimated_steps,
            "steps_used": sum(int(chunk["estimated_steps"]) for chunk in deterministic_chunks),
            "latency_budget_ms": latency_budget_ms,
            "chunks": deterministic_chunks,
            "output_png_digest": output_png_digest if within_latency else None,
            "computed_output_png_digest": output_png_digest,
            "output_pixel_digest": output_pixel_digest if within_latency else None,
            "computed_output_pixel_digest": output_pixel_digest,
            "cache_key": cache_key,
            "scheduler": {
                "lane": scheduled.lane,
                "estimated_steps": scheduled.estimated_steps,
                "total_step_budget": plan.total_step_budget,
                "worker_step_quota": plan.worker_step_quota,
            },
            "parse_error": None,
            "error": None,
        }
        ref = self.artifacts.put_json(artifact)
        metrics = {
            "schema": "gspipe-execution-metrics-v1",
            "task_id": inspection.task_id,
            "artifact_digest": ref.digest,
            "lane": scheduled.lane,
            "estimated_steps": scheduled.estimated_steps,
            "steps_used": artifact["steps_used"],
            "elapsed_ns": elapsed_ns,
            "elapsed_ms": round(elapsed_ns / 1_000_000.0, 6),
            "latency_budget_ms": latency_budget_ms,
            "latency_remaining_ms": round(latency_budget_ms - elapsed_ns / 1_000_000.0, 6),
            "within_latency_budget": within_latency,
            "cache_hit": cache_hit is not None,
            "cache_key": cache_key,
            "chunks": runtime_chunks,
        }
        self.metrics.write_execution(inspection.task_id, ref.digest, metrics)
        return ExecutionOutcome(inspection.task_id, artifact, ref, metrics)

    def _execute_error_outcome(self, inspection: TaskInspection, exc: BaseException, lane: int) -> ExecutionOutcome:
        structured = exc.to_dict() if isinstance(exc, PipelineError) else internal_error(exc).to_dict()
        artifact = self._base_execute_artifact(
            inspection,
            status="ERROR",
            reason="EXECUTION_ERROR",
            scheduler={"lane": lane, "estimated_steps": inspection.chain_steps},
            error=structured,
        )
        ref = self.artifacts.put_json(artifact)
        metrics = {
            "schema": "gspipe-execution-metrics-v1",
            "task_id": inspection.task_id,
            "artifact_digest": ref.digest,
            "lane": lane,
            "estimated_steps": inspection.chain_steps,
            "steps_used": 0,
            "elapsed_ns": 0,
            "elapsed_ms": 0.0,
            "latency_budget_ms": int(inspection.task.latency_budget_ms),
            "within_latency_budget": False,
            "cache_hit": False,
            "error": structured,
            "chunks": [],
        }
        self.metrics.write_execution(inspection.task_id, ref.digest, metrics)
        return ExecutionOutcome(inspection.task_id, artifact, ref, metrics, error=structured)

    def _run_execute_stage(
        self,
        inspections: Sequence[TaskInspection],
        *,
        parallel: bool = True,
    ) -> tuple[dict[str, Any], bool, SchedulingPlan | None]:
        candidates = [inspection for inspection in inspections if not inspection.reuse["execute"]]
        if not candidates:
            return {"stage": "execute", "checkpoints_written": 0, "scheduler": None}, False, None

        outcomes: dict[str, ExecutionOutcome] = {}
        errors: list[dict[str, Any]] = []
        valid_candidates: list[TaskInspection] = []
        for inspection in candidates:
            if inspection.parsed is None:
                artifact = self._base_execute_artifact(
                    inspection,
                    status="SKIPPED",
                    reason="INVALID_CHAIN",
                    scheduler={"lane": None, "estimated_steps": 0},
                )
                ref = self.artifacts.put_json(artifact)
                outcomes[inspection.task_id] = ExecutionOutcome(
                    inspection.task_id,
                    artifact,
                    ref,
                    {"elapsed_ns": 0, "cache_hit": False, "latency_budget_ms": inspection.task.latency_budget_ms},
                )
                errors.append({"task_id": inspection.task_id, "stage": "execute", "error": inspection.parse_error})
            else:
                valid_candidates.append(inspection)

        plan: SchedulingPlan | None = None
        scheduled_by_id: dict[str, Any] = {}
        if valid_candidates:
            plan = build_schedule(
                [TaskEstimate(inspection.task_id, inspection.chain_steps) for inspection in valid_candidates],
                dict(self.config.scheduler),
            )
            for scheduled in plan.accepted:
                scheduled_by_id[scheduled.task_id] = scheduled
            for rejected in plan.rejected:
                inspection = next(item for item in valid_candidates if item.task_id == rejected.task_id)
                artifact = self._base_execute_artifact(
                    inspection,
                    status="UNPROCESSED",
                    reason=rejected.reason,
                    scheduler={
                        "rejected": True,
                        "estimated_steps": inspection.chain_steps,
                        **rejected.details,
                    },
                )
                ref = self.artifacts.put_json(artifact)
                outcomes[inspection.task_id] = ExecutionOutcome(
                    inspection.task_id,
                    artifact,
                    ref,
                    {
                        "elapsed_ns": 0,
                        "cache_hit": False,
                        "latency_budget_ms": inspection.task.latency_budget_ms,
                    },
                )

        stopped = False
        if plan is not None and plan.accepted:
            by_id = {inspection.task_id: inspection for inspection in valid_candidates}
            if not parallel:
                for scheduled in plan.accepted:
                    if self.workspace.stop_requested():
                        stopped = True
                        break
                    inspection = by_id[scheduled.task_id]
                    try:
                        outcome = self._execute_one(inspection, scheduled, plan)
                    except BaseException as exc:  # structured execution error, not a thread timeout
                        outcome = self._execute_error_outcome(inspection, exc, scheduled.lane)
                    outcomes[outcome.task_id] = outcome
                    if outcome.error is not None:
                        errors.append({"task_id": outcome.task_id, "stage": "execute", "error": outcome.error})
            else:
                results_lock = threading.Lock()
                thread_errors: list[BaseException] = []

                def lane_worker(lane_tasks: Sequence[Any]) -> None:
                    for scheduled in lane_tasks:
                        if self.workspace.stop_requested():
                            return
                        inspection = by_id[scheduled.task_id]
                        try:
                            outcome = self._execute_one(inspection, scheduled, plan)
                        except BaseException as exc:
                            outcome = self._execute_error_outcome(inspection, exc, scheduled.lane)
                        with results_lock:
                            outcomes[outcome.task_id] = outcome
                            if outcome.error is not None:
                                errors.append(
                                    {"task_id": outcome.task_id, "stage": "execute", "error": outcome.error}
                                )

                threads: list[threading.Thread] = []
                for lane, lane_tasks in plan.lanes.items():
                    thread = threading.Thread(
                        target=lane_worker,
                        args=(lane_tasks,),
                        name=f"gspipe-lane-{lane}",
                        daemon=True,
                    )
                    threads.append(thread)
                    thread.start()
                for thread in threads:
                    thread.join()
                if self.workspace.stop_requested() and any(
                    scheduled.task_id not in outcomes for scheduled in plan.accepted
                ):
                    stopped = True

        written = 0
        for inspection in sorted(candidates, key=lambda item: item.task_id):
            outcome = outcomes.get(inspection.task_id)
            if outcome is None:
                continue
            self._publish_stage(
                inspection,
                "execute",
                outcome.artifact,
                inspection.components["execute"],
                artifact_ref=outcome.artifact_ref,
            )
            written += 1

        stage_report = {
            "stage": "execute",
            "checkpoints_written": written,
            "scheduler": plan.to_dict() if plan is not None else None,
            "executed": [outcome.artifact["task_id"] for outcome in outcomes.values() if outcome.artifact["status"] == "PROCESSED"],
            "unprocessed": [
                {"task_id": outcome.task_id, "reason": outcome.artifact.get("reason")}
                for outcome in sorted(outcomes.values(), key=lambda item: item.task_id)
                if outcome.artifact["status"] != "PROCESSED"
            ],
            "errors": errors,
            "stopped_before_all_tasks": stopped,
        }
        return stage_report, stopped, plan

    def _run_verify_stage(self, inspections: Sequence[TaskInspection]) -> tuple[dict[str, Any], bool]:
        written = 0
        mismatched: list[str] = []
        errors: list[dict[str, Any]] = []
        stopped = False
        for inspection in inspections:
            if self.workspace.stop_requested():
                stopped = True
                break
            if inspection.reuse["verify"]:
                continue
            state = self.checkpoints.state(inspection.task_id)
            execute_record = state.latest("execute")
            if execute_record is None or not state.is_valid("execute"):
                raise invalid_stage(
                    "verify",
                    "missing_predecessor",
                    "verify requires a current execute checkpoint",
                    task_id=inspection.task_id,
                )
            execute_digest = str(execute_record["artifact_digest"])
            execute_artifact = self.artifacts.get_json(execute_digest)
            components = {
                "execute_output": execute_digest,
                "expected_output": [int(inspection.task.expected_width), int(inspection.task.expected_height)],
                "expected_summary": inspection.task.expected_summary,
                "verify_config_version": self.config.verify_version,
            }
            verify_status: str
            reason: str | None = None
            output_size: list[int] | None = None
            output_png_digest: str | None = None
            summary_match = False
            size_match = False
            artifact_error: dict[str, Any] | None = None
            if execute_artifact.get("status") == "PROCESSED":
                try:
                    output_png_digest = str(execute_artifact["output_png_digest"])
                    png_bytes = self.artifacts.get_png(output_png_digest)
                    output_image = decode_rgb_png(png_bytes, source=f"{inspection.task_id}:execute-output")
                    output_size = [int(output_image.shape[1]), int(output_image.shape[0])]
                    expected_pixel_digest = execute_artifact.get("output_pixel_digest")
                    if pixel_fingerprint(output_image) != expected_pixel_digest:
                        raise PipelineError(
                            E_ARTIFACT_CORRUPT,
                            "Execution output pixel digest does not match its artifact metadata",
                            task_id=inspection.task_id,
                            stage="verify",
                        )
                    expected_size = [int(inspection.task.expected_width), int(inspection.task.expected_height)]
                    size_match = output_size == expected_size
                    actual_summary = execute_artifact.get("chain_summary")
                    summary_match = actual_summary == inspection.task.expected_summary
                    verify_status = "VERIFIED" if size_match and summary_match else "MISMATCH"
                except PipelineError as exc:
                    verify_status = "ERROR"
                    reason = "EXECUTION_ARTIFACT_ERROR"
                    artifact_error = exc.to_dict()
                    errors.append({"task_id": inspection.task_id, "stage": "verify", "error": artifact_error})
            else:
                verify_status = "SKIPPED"
                reason = str(execute_artifact.get("reason") or "EXECUTION_NOT_PROCESSED")

            if verify_status == "MISMATCH":
                mismatched.append(inspection.task_id)
            actual_summary = (
                execute_artifact.get("chain_summary") if execute_artifact.get("status") == "PROCESSED" else None
            )
            artifact = {
                "schema": "gspipe-verify-artifact-v1",
                "task_id": inspection.task_id,
                "status": verify_status,
                "reason": reason,
                "execute_artifact_digest": execute_digest,
                "expected_output": [int(inspection.task.expected_width), int(inspection.task.expected_height)],
                "actual_output_size": output_size,
                "expected_summary": inspection.task.expected_summary,
                "actual_summary": actual_summary,
                "summary_match": summary_match,
                "size_match": size_match,
                "output_png_digest": output_png_digest if verify_status in {"VERIFIED", "MISMATCH"} else None,
                "error": artifact_error,
            }
            self._publish_stage(inspection, "verify", artifact, components)
            written += 1
        report = {
            "stage": "verify",
            "checkpoints_written": written,
            "mismatched": mismatched,
            "errors": errors,
        }
        return report, stopped

    def _task_export_entry(self, inspection: TaskInspection, verify_artifact: Mapping[str, Any]) -> dict[str, Any]:
        verify_status = str(verify_artifact.get("status"))
        if verify_status == "VERIFIED":
            status = "PROCESSED"
        elif verify_status == "MISMATCH":
            status = "MISMATCH"
        else:
            status = "UNPROCESSED"
        output_size = verify_artifact.get("actual_output_size")
        return {
            "task_id": inspection.task_id,
            "status": status,
            "input": inspection.task.input_filename,
            "instruction": inspection.normalized_instruction,
            "edit_summary": inspection.parsed.chain_summary if inspection.parsed is not None else None,
            "expected_summary": inspection.task.expected_summary,
            "summary_match": bool(verify_artifact.get("summary_match", False)),
            "size_match": bool(verify_artifact.get("size_match", False)),
            "output_width": int(output_size[0]) if output_size else None,
            "output_height": int(output_size[1]) if output_size else None,
            "output_sha256": verify_artifact.get("output_png_digest"),
            "latency_budget_ms": int(inspection.task.latency_budget_ms),
            "reason": verify_artifact.get("reason"),
        }

    def _run_export_stage(self, inspections: Sequence[TaskInspection]) -> tuple[dict[str, Any], bool]:
        written = 0
        stopped = False
        for inspection in inspections:
            if self.workspace.stop_requested():
                stopped = True
                break
            if inspection.reuse["export"]:
                continue
            state = self.checkpoints.state(inspection.task_id)
            verify_record = state.latest("verify")
            if verify_record is None or not state.is_valid("verify"):
                raise invalid_stage(
                    "export",
                    "missing_predecessor",
                    "export requires a current verify checkpoint",
                    task_id=inspection.task_id,
                )
            verify_digest = str(verify_record["artifact_digest"])
            verify_artifact = self.artifacts.get_json(verify_digest)
            components = {
                "verify_output": verify_digest,
                "export_config_version": self.config.export_version,
            }
            entry = self._task_export_entry(inspection, verify_artifact)
            artifact = {
                "schema": "gspipe-export-artifact-v1",
                "task_id": inspection.task_id,
                **entry,
                "verify_artifact_digest": verify_digest,
            }
            self._publish_stage(inspection, "export", artifact, components)
            written += 1
        report = {"stage": "export", "checkpoints_written": written}
        return report, stopped

    def _build_export_bundle(self, inspections: Sequence[TaskInspection]) -> dict[str, Any]:
        summary_tasks: list[dict[str, Any]] = []
        manifest_tasks: list[dict[str, Any]] = []
        result_blobs: dict[str, bytes] = {}
        for inspection in sorted(inspections, key=lambda item: item.task_id):
            state = self.checkpoints.state(inspection.task_id)
            verify_record = state.latest("verify")
            export_record = state.latest("export")
            if verify_record is None or not state.is_valid("verify"):
                raise invalid_stage(
                    "export",
                    "missing_predecessor",
                    "Cannot build export bundle without a current verify checkpoint",
                    task_id=inspection.task_id,
                )
            if export_record is None or not state.is_valid("export"):
                raise invalid_stage(
                    "export",
                    "missing_export_checkpoint",
                    "Cannot build export bundle without a current export checkpoint",
                    task_id=inspection.task_id,
                )
            verify_artifact = self.artifacts.get_json(str(verify_record["artifact_digest"]))
            entry = self._task_export_entry(inspection, verify_artifact)
            summary_tasks.append(entry)
            result_file: str | None = None
            if entry["status"] in {"PROCESSED", "MISMATCH"} and entry["output_sha256"]:
                result_file = f"results/{inspection.task_id}.png"
                result_blobs[result_file] = self.artifacts.get_png(str(entry["output_sha256"]))
            manifest_tasks.append({**entry, "result_file": result_file})

        summary = {"schema": "gspipe-edit-summary-v1", "tasks": summary_tasks}
        summary_digest = sha256_json(summary)
        manifest = {
            "schema": "gspipe-export-manifest-v1",
            "summary_digest": summary_digest,
            "task_count": len(manifest_tasks),
            "tasks": manifest_tasks,
        }
        ensure_dir(self.workspace.exports_dir)
        bundle_dir = self.workspace.exports_dir / summary_digest
        bundle_matches = False
        if bundle_dir.exists():
            expected_manifest_bytes = canonical_json_bytes(manifest) + b"\n"
            expected_summary_bytes = canonical_json_bytes(summary) + b"\n"
            try:
                bundle_matches = (
                    (bundle_dir / "manifest.json").read_bytes() == expected_manifest_bytes
                    and (bundle_dir / "summary.json").read_bytes() == expected_summary_bytes
                    and all((bundle_dir / relative).read_bytes() == png_bytes for relative, png_bytes in result_blobs.items())
                )
            except OSError:
                bundle_matches = False
            if not bundle_matches:
                shutil.rmtree(bundle_dir, ignore_errors=True)
        if not bundle_matches:
            temp_dir = Path(tempfile.mkdtemp(prefix=f".{summary_digest}.tmp-", dir=self.workspace.exports_dir))
            try:
                summary_path = temp_dir / "summary.json"
                manifest_path = temp_dir / "manifest.json"
                summary_path.write_bytes(canonical_json_bytes(summary) + b"\n")
                manifest_path.write_bytes(canonical_json_bytes(manifest) + b"\n")
                results_dir = temp_dir / "results"
                ensure_dir(results_dir)
                for relative, png_bytes in sorted(result_blobs.items()):
                    target = temp_dir / relative
                    ensure_dir(target.parent)
                    target.write_bytes(png_bytes)
                try:
                    os.replace(str(temp_dir), str(bundle_dir))
                except OSError:
                    if bundle_dir.exists():
                        shutil.rmtree(temp_dir, ignore_errors=True)
                    else:
                        raise
            except BaseException:
                shutil.rmtree(temp_dir, ignore_errors=True)
                raise
        latest = {
            "schema": "gspipe-latest-export-v1",
            "summary_digest": summary_digest,
            "bundle_dir": bundle_dir.relative_to(self.workspace.root).as_posix(),
            "task_count": len(summary_tasks),
        }
        atomic_write_json(self.workspace.state_dir / "latest_export.json", latest)
        counts: dict[str, int] = {}
        for entry in summary_tasks:
            counts[str(entry["status"])] = counts.get(str(entry["status"]), 0) + 1
        return {
            **latest,
            "summary": summary,
            "manifest": manifest,
            "status_counts": dict(sorted(counts.items())),
        }

    def run(
        self,
        stages: str | list[str] | tuple[str, ...] | None = None,
        *,
        resume: bool = False,
        parallel: bool = True,
    ) -> dict[str, Any]:
        requested = normalize_stage_list(stages)
        started_ns = time.perf_counter_ns()
        with self.workspace.run_lock():
            if resume:
                self.workspace.clear_stop()
            if self.workspace.stop_requested():
                report = {
                    "ok": True,
                    "status": "STOPPED",
                    "workspace": str(self.workspace.root),
                    "requested_stages": requested,
                    "reason": "stop_requested",
                    "checkpoints_written": 0,
                }
                self.metrics.write_last_run(report)
                return report

            inspections = self.inspect()
            self._validate_requested_stages(requested, inspections)
            for inspection in inspections:
                self._record_invalidation(inspection)

            stage_reports: list[dict[str, Any]] = []
            errors: list[dict[str, Any]] = []
            stop_observed = False
            scheduler_report: dict[str, Any] | None = None
            checkpoints_written = 0

            for stage in requested:
                if self.workspace.stop_requested():
                    stop_observed = True
                    break
                if stage == "parse":
                    report, stopped = self._run_parse_stage(inspections)
                elif stage == "execute":
                    report, stopped, plan = self._run_execute_stage(inspections, parallel=parallel)
                    scheduler_report = report.get("scheduler")
                elif stage == "verify":
                    report, stopped = self._run_verify_stage(inspections)
                elif stage == "export":
                    report, stopped = self._run_export_stage(inspections)
                else:
                    raise invalid_stage(stage, "unknown_stage", f"Unknown stage {stage!r}")
                stage_reports.append(report)
                checkpoints_written += int(report.get("checkpoints_written", 0))
                errors.extend(report.get("errors", []))
                if stopped or self.workspace.stop_requested():
                    stop_observed = True
                    break

            bundle: dict[str, Any] | None = None
            if not stop_observed and "export" in requested:
                all_current = True
                for inspection in inspections:
                    state = self.checkpoints.state(inspection.task_id)
                    if not state.is_valid("export"):
                        all_current = False
                        break
                if all_current:
                    bundle = self._build_export_bundle(inspections)

            status = "STOPPED" if stop_observed else "COMPLETED"
            elapsed_ns = time.perf_counter_ns() - started_ns
            invalidations = [
                inspection.invalidation_event
                for inspection in inspections
                if inspection.invalidation_event is not None
            ]
            final_inspections = self.inspect()
            pending_current = [
                {"task_id": item.task_id, "reasons": list(item.reasons), "earliest_stage": item.earliest_stage}
                for item in final_inspections
                if item.earliest_stage is not None
            ]
            report = {
                "ok": True,
                "status": status,
                "workspace": str(self.workspace.root),
                "requested_stages": requested,
                "stages": stage_reports,
                "checkpoints_written": checkpoints_written,
                "scheduler": scheduler_report,
                "invalidations": invalidations,
                "errors": errors,
                "pending_tasks": pending_current,
                "summary_digest": bundle["summary_digest"] if bundle is not None else None,
                "export_dir": bundle["bundle_dir"] if bundle is not None else None,
                "status_counts": bundle["status_counts"] if bundle is not None else None,
                "parallel": bool(parallel),
                "elapsed_ms": round(elapsed_ns / 1_000_000.0, 6),
            }
            self.metrics.write_last_run(report)
            return report

    def _inspection_for(self, task_id: str) -> TaskInspection:
        inspections = self.inspect()
        for inspection in inspections:
            if inspection.task_id == task_id:
                return inspection
        raise PipelineError(
            E_TASK_NOT_FOUND,
            f"Unknown task_id {task_id!r}",
            task_id=task_id,
        )

    def validate_task(self, task_id: str) -> dict[str, Any]:
        inspection = self._inspection_for(task_id)
        if inspection.parse_error is not None:
            error = inspection.parse_error
            raise PipelineError(
                str(error.get("code", E_INVALID_CHAIN)),
                str(error.get("message", "Invalid operator chain")),
                task_id=error.get("task_id", task_id),
                operator=error.get("operator"),
                stage=error.get("stage", "parse"),
                details=error.get("details"),
                context=error.get("context"),
            )
        assert inspection.parsed is not None
        return {
            "ok": True,
            "task_id": task_id,
            "normalized_instruction": inspection.normalized_instruction,
            "operators": [operator.to_dict() for operator in inspection.parsed.operators],
            "chain_summary": inspection.parsed.chain_summary,
            "input_size": list(inspection.parsed.input_size),
            "output_size": list(inspection.parsed.output_size),
            "parse_artifact_digest": inspection.parse_artifact_digest,
            "parse_input_fingerprint": inspection.fingerprints["parse"],
        }

    def _query_payload(self, inspection: TaskInspection) -> tuple[str, dict[str, Any]]:
        task = inspection.task
        state = self.checkpoints.state(task.task_id)
        invalidations = self.invalidations.load(task.task_id)
        latest_execute = state.latest("execute")
        execute_artifact: dict[str, Any] | None = None
        execution_metrics: dict[str, Any] | None = None
        if latest_execute is not None:
            try:
                execute_artifact = self.artifacts.get_json(str(latest_execute["artifact_digest"]))
            except PipelineError:
                execute_artifact = None
            execution_metrics = self.metrics.read_execution(task.task_id, str(latest_execute["artifact_digest"]))

        latest_verify = state.latest("verify")
        verify_artifact: dict[str, Any] | None = None
        if latest_verify is not None:
            try:
                verify_artifact = self.artifacts.get_json(str(latest_verify["artifact_digest"]))
            except PipelineError:
                verify_artifact = None

        stages: dict[str, Any] = {}
        for stage in STAGES:
            record = state.latest(stage)
            checkpoint = None
            if record is not None:
                checkpoint = {
                    "seq": int(record.get("seq", 0)),
                    "generation": int(record.get("generation", 0)),
                    "input_fingerprint": record.get("input_fingerprint"),
                    "config_version": record.get("config_version"),
                    "artifact_digest": record.get("artifact_digest"),
                    "artifact_path": record.get("artifact_path"),
                    "dependencies": record.get("dependencies", {}),
                    "state_valid": state.is_valid(stage),
                }
            stages[stage] = {
                "checkpoint": checkpoint,
                "reused_for_current_version": inspection.reuse[stage],
                "current_input_fingerprint": inspection.fingerprints.get(stage),
                "current_config_version": inspection.config_versions[stage],
                "current_fingerprint_components": inspection.components.get(stage),
                "needs_rebuild": (not inspection.reuse[stage]),
            }

        if inspection.parse_error is not None:
            task_status = "INVALID_CHAIN"
        elif inspection.earliest_stage is not None:
            task_status = "PENDING_REBUILD"
        elif verify_artifact is not None and verify_artifact.get("status") == "VERIFIED":
            task_status = "PROCESSED"
        elif verify_artifact is not None and verify_artifact.get("status") == "MISMATCH":
            task_status = "MISMATCH"
        elif execute_artifact is not None and execute_artifact.get("status") == "PROCESSED":
            task_status = "PROCESSED_UNVERIFIED"
        else:
            task_status = "UNPROCESSED"

        version_fingerprint = sha256_json(
            {
                "schema": "gspipe-task-version-v1",
                "task": task.to_dict(),
                "input_pixels": inspection.shard.info["pixel_sha256"],
                "config_versions": inspection.config_versions,
                "stage_fingerprints": dict(sorted(inspection.fingerprints.items())),
            }
        )
        metrics_digest = sha256_json(execution_metrics) if execution_metrics is not None else None
        record_identity = [
            {
                "seq": int(record.get("seq", 0)),
                "stage": record.get("stage"),
                "generation": int(record.get("generation", 0)),
                "input_fingerprint": record.get("input_fingerprint"),
                "artifact_digest": record.get("artifact_digest"),
            }
            for record in state.records
        ]
        invalidation_identity = [
            {"event_id": event.get("event_id"), "detected_seq": event.get("detected_seq")}
            for event in invalidations
        ]
        cache_key = sha256_json(
            {
                "schema": "gspipe-query-identity-v1",
                "task": task.to_dict(),
                "input_pixels": inspection.shard.info["pixel_sha256"],
                "config_version": self.config.config_version,
                "records": record_identity,
                "metrics_digest": metrics_digest,
                "invalidations": invalidation_identity,
            }
        )
        execution_payload = None
        if execute_artifact is not None:
            execution_payload = {
                "status": execute_artifact.get("status"),
                "reason": execute_artifact.get("reason"),
                "chain_summary": execute_artifact.get("chain_summary"),
                "output_size": execute_artifact.get("output_size"),
                "output_sha256": execute_artifact.get("output_png_digest"),
                "steps_used": execute_artifact.get("steps_used"),
                "latency_budget_ms": execute_artifact.get("latency_budget_ms"),
                "chunks": execute_artifact.get("chunks", []),
                "error": execute_artifact.get("error"),
            }
        payload = {
            "schema": "gspipe-task-query-v1",
            "cache_key": cache_key,
            "task_id": task.task_id,
            "status": task_status,
            "input": task.input_filename,
            "instruction": inspection.normalized_instruction,
            "version_fingerprint": version_fingerprint,
            "config_version": self.config.config_version,
            "stage_fingerprints": {
                stage: inspection.fingerprints.get(stage) for stage in STAGES
            },
            "stages": stages,
            "earliest_invalid_stage": inspection.earliest_stage,
            "invalidation_reasons": list(inspection.reasons),
            "changed_components": list(inspection.changed_components),
            "invalidations": invalidations,
            "execution": execution_payload,
            "runtime_metrics": execution_metrics,
            "latency_budget_ms": int(task.latency_budget_ms),
            "scheduler_step_budget": self.config.scheduler,
        }
        return cache_key, payload

    def query_task(self, task_id: str, *, force_recompute: bool = False) -> tuple[bytes, bool]:
        inspection = self._inspection_for(task_id)
        cache_key, payload = self._query_payload(inspection)
        if not force_recompute:
            cached = self.query_cache.get(cache_key)
            if cached is not None:
                return cached, True
        data = canonical_json_bytes(payload) + b"\n"
        self.query_cache.put(cache_key, data)
        return data, False

    def status(self) -> dict[str, Any]:
        inspections = self.inspect()
        tasks: list[dict[str, Any]] = []
        cache_hits = 0
        for inspection in inspections:
            data, hit = self.query_task(inspection.task_id)
            cache_hits += int(hit)
            tasks.append(json.loads(data.decode("utf-8")))
        return {
            "ok": True,
            "workspace": str(self.workspace.root),
            "config_version": self.config.config_version,
            "task_count": len(tasks),
            "tasks": tasks,
            "query_cache_hits": cache_hits,
            "last_run": self.metrics.read_last_run(),
        }

    def fingerprints(self) -> dict[str, Any]:
        inspections = self.inspect()
        return {
            "ok": True,
            "config_version": self.config.config_version,
            "tasks": [
                {
                    "task_id": inspection.task_id,
                    "version_fingerprint": sha256_json(
                        {
                            "task": inspection.task.to_dict(),
                            "input_pixels": inspection.shard.info["pixel_sha256"],
                            "stage_fingerprints": dict(sorted(inspection.fingerprints.items())),
                        }
                    ),
                    "stage_fingerprints": {
                        stage: inspection.fingerprints.get(stage) for stage in STAGES
                    },
                    "earliest_invalid_stage": inspection.earliest_stage,
                    "invalidation_reasons": inspection.reasons,
                }
                for inspection in inspections
            ],
        }

    def get_result_png(self, task_id: str) -> bytes:
        inspection = self._inspection_for(task_id)
        if not inspection.reuse["execute"]:
            raise PipelineError(
                E_RESULT_UNAVAILABLE,
                "Task execution result is stale or not current",
                task_id=task_id,
                stage="execute",
                details={"earliest_invalid_stage": inspection.earliest_stage},
            )
        state = self.checkpoints.state(task_id)
        execute_record = state.latest("execute")
        if execute_record is None:
            raise PipelineError(E_RESULT_UNAVAILABLE, "Task has no execute checkpoint", task_id=task_id)
        execute_artifact = self.artifacts.get_json(str(execute_record["artifact_digest"]))
        if execute_artifact.get("status") != "PROCESSED" or not execute_artifact.get("output_png_digest"):
            raise PipelineError(
                E_RESULT_UNAVAILABLE,
                "Task was not processed into an exportable image",
                task_id=task_id,
                details={"status": execute_artifact.get("status"), "reason": execute_artifact.get("reason")},
            )
        return self.artifacts.get_png(str(execute_artifact["output_png_digest"]))

    def latest_export(self) -> dict[str, Any] | None:
        path = self.workspace.state_dir / "latest_export.json"
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(value, dict):
            return None
        summary_path = self.workspace.root / str(value.get("bundle_dir", "")) / "summary.json"
        manifest_path = self.workspace.root / str(value.get("bundle_dir", "")) / "manifest.json"
        try:
            value["summary"] = json.loads(summary_path.read_text(encoding="utf-8"))
            value["manifest"] = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return value

    def export(self, *, parallel: bool = True) -> dict[str, Any]:
        report = self.run(["export"], parallel=parallel)
        latest = self.latest_export()
        return {
            "ok": True,
            "run": report,
            "export": latest,
        }
