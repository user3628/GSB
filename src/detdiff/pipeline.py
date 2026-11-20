"""Fingerprint-driven incremental build, diagnostics and consistency checks."""

from __future__ import annotations

import contextlib
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import numpy as np
import yaml

from .artifacts import (
    embedding_artifact,
    embedding_from_bytes,
    quantize_artifact,
    quantize_from_bytes,
    sample_artifact,
    schedule_artifact,
    schedule_from_bytes,
    sample_from_bytes,
)
from .checkpoints import CheckpointStore
from .config import STAGE_NAMES, ShardConfig, WorkbenchConfig, load_config
from .encoding import encode_prompt, schedule_weights
from .errors import BusinessError, ErrorCode
from .quantization import quantize_to_png
from .sampling import load_reference_tensor, run_sampling
from .util import (
    atomic_write_bytes,
    exclusive_file_lock,
    sha256_bytes,
    sha256_file,
)
from .versions import (
    changed_meta_keys,
    encode_meta,
    input_fingerprint,
    quantize_meta,
    sample_meta,
    schedule_meta,
)


@dataclass(frozen=True)
class ReferenceInput:
    tensor: np.ndarray
    sha256: str
    path: str


@dataclass(frozen=True)
class StageOutput:
    artifact: bytes
    input_fingerprint: str
    input_meta: dict[str, Any]
    output_sha256: str | None = None


@dataclass(frozen=True)
class ComputedShard:
    stages: dict[str, StageOutput]
    png_bytes: bytes


@dataclass(frozen=True)
class BuildHooks:
    stop_after_stage: str | None = None
    pause_file: Path | None = None
    should_stop: Callable[[], bool] | None = None


@dataclass
class BuildReport:
    ok: bool
    config_version: str
    rebuilt: dict[str, list[str]] = field(default_factory=dict)
    reused: dict[str, list[str]] = field(default_factory=dict)
    invalid_reasons: dict[str, dict[str, str]] = field(default_factory=dict)
    published: list[dict[str, Any]] = field(default_factory=list)
    output_sha256: dict[str, str] = field(default_factory=dict)
    consistency_verified: bool = False
    interrupted: bool = False
    interrupted_after: dict[str, str] | None = None
    stop_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "config_version": self.config_version,
            "rebuilt": self.rebuilt,
            "reused": self.reused,
            "invalid_reasons": self.invalid_reasons,
            "published": self.published,
            "output_sha256": self.output_sha256,
            "consistency_verified": self.consistency_verified,
            "interrupted": self.interrupted,
            "interrupted_after": self.interrupted_after,
            "stop_reason": self.stop_reason,
        }


class BuildStopped(Exception):
    def __init__(self, report: BuildReport):
        super().__init__(report.stop_reason or "build stopped")
        self.report = report


@dataclass
class StageView:
    stage: str
    status: str
    invalid: bool
    reason: str | None
    record: Any
    expected_input_fingerprint: str | None
    expected_input_meta: dict[str, Any] | None
    changed_fields: list[str]
    invalidated_by: str | None
    trajectory: list[dict[str, Any]]
    clamp_counts: dict[str, int] | None
    current_output_sha256: str | None
    config_version_matches: bool

    def to_dict(self) -> dict[str, Any]:
        record = self.record
        return {
            "stage": self.stage,
            "status": self.status,
            "invalid": self.invalid,
            "reason": self.reason,
            "checkpoint_exists": record is not None,
            "sequence": record.sequence if record is not None else None,
            "input_fingerprint": record.input_fingerprint if record is not None else None,
            "expected_input_fingerprint": self.expected_input_fingerprint,
            "config_version": record.config_version if record is not None else None,
            "config_version_matches": self.config_version_matches,
            "artifact_sha256": record.artifact_sha256 if record is not None else None,
            "artifact_path": record.artifact_path if record is not None else None,
            "output_sha256": record.output_sha256 if record is not None else None,
            "current_output_sha256": self.current_output_sha256,
            "changed_fields": self.changed_fields,
            "invalidated_by": self.invalidated_by,
            "trajectory": self.trajectory,
            "trajectory_length": len(self.trajectory),
            "clamp_counts": self.clamp_counts,
        }


@dataclass
class ShardDiagnostic:
    shard: ShardConfig
    stages: list[StageView]
    png_sha256: str | None
    output_exists: bool
    reference_sha256: str | None
    reference_error: str | None

    @property
    def earliest_invalid(self) -> str | None:
        for stage in self.stages:
            if stage.invalid:
                return stage.stage
        return None

    def to_dict(self) -> dict[str, Any]:
        invalid_stages = [stage.stage for stage in self.stages if stage.invalid]
        reusable_stages = [stage.stage for stage in self.stages if not stage.invalid]
        return {
            "id": self.shard.id,
            "prompt": self.shard.prompt,
            "guidance_scale": self.shard.guidance_scale,
            "steps": self.shard.steps,
            "seed": self.shard.seed,
            "reference": self.shard.reference,
            "reference_sha256": self.reference_sha256,
            "reference_error": self.reference_error,
            "blend_strength": self.shard.blend_strength,
            "earliest_invalid": self.earliest_invalid,
            "rebuild_plan": invalid_stages,
            "reuse_plan": reusable_stages,
            "output_exists": self.output_exists,
            "png_sha256": self.png_sha256,
            "stages": [stage.to_dict() for stage in self.stages],
        }


def build_workspace(
    root: Path,
    *,
    config_path: Path | None = None,
    hooks: BuildHooks | None = None,
    verify_consistency: bool = True,
) -> BuildReport:
    """Build all invalid stage chains under the workspace transaction lock.

    Publication is allowed only after a fresh in-memory full build has produced
    byte-identical stage artifacts and PNG payloads for every shard.
    """

    root = root.resolve()
    config_path = (config_path or (root / "config.yaml")).resolve()
    store = CheckpointStore(root)
    hooks = hooks or BuildHooks()
    with exclusive_file_lock(store.workspace_lock_path()):
        config = load_config(config_path)
        references = load_all_references(config)
        diagnostics = inspect_workspace(config, references=references)
        report = BuildReport(ok=False, config_version=config.config_version)

        plans: dict[str, dict[str, StageOutput]] = {}
        candidates: dict[str, dict[str, bytes]] = {}
        reasons: dict[str, dict[str, str]] = {}
        rebuilt: dict[str, list[str]] = {}
        reused: dict[str, list[str]] = {}

        for diagnostic in diagnostics:
            shard = diagnostic.shard
            stage_map: dict[str, StageOutput] = {}
            candidate: dict[str, bytes] = {}
            rebuilt_stages: list[str] = []
            reused_stages: list[str] = []
            reasons_for_shard: dict[str, str] = {}
            for view in diagnostic.stages:
                if view.invalid:
                    rebuilt_stages.append(view.stage)
                    if view.reason:
                        reasons_for_shard[view.stage] = view.reason
                    output = _compute_stage(
                        shard,
                        config,
                        view.stage,
                        previous=stage_map,
                        reference=references.get(shard.id),
                    )
                    stage_map[view.stage] = output
                    candidate[view.stage] = output.artifact
                else:
                    reused_stages.append(view.stage)
                    blob = store.load_artifact(view.record)
                    stage_map[view.stage] = StageOutput(
                        artifact=blob,
                        input_fingerprint=view.record.input_fingerprint,
                        input_meta=view.record.input_meta,
                        output_sha256=view.record.output_sha256,
                    )
                    candidate[view.stage] = blob
            plans[shard.id] = stage_map
            candidates[shard.id] = candidate
            if rebuilt_stages:
                rebuilt[shard.id] = rebuilt_stages
            if reused_stages:
                reused[shard.id] = reused_stages
            if reasons_for_shard:
                reasons[shard.id] = reasons_for_shard

        report.rebuilt = rebuilt
        report.reused = reused
        report.invalid_reasons = reasons
        has_rebuilds = any(rebuilt.values())
        if has_rebuilds and verify_consistency:
            fresh: dict[str, ComputedShard] = {}
            for shard in config.shards:
                fresh[shard.id] = compute_shard_from_scratch(shard, config, references.get(shard.id))
            _assert_candidate_matches_fresh(candidates, fresh)
            report.consistency_verified = True

        if not has_rebuilds:
            report.ok = True
            report.output_sha256 = _current_output_hashes(config, store)
            return report

        stop_after_used = False
        for shard in config.shards:
            if shard.id not in rebuilt:
                continue
            for stage in rebuilt[shard.id]:
                if _stop_requested(hooks):
                    report.interrupted = True
                    report.stop_reason = _stop_reason(hooks)
                    raise BuildStopped(report)
                output = plans[shard.id][stage]
                expected_sequence = store.next_sequence(shard.id)
                output_sha = None
                if stage == "quantize":
                    png, _ = quantize_from_bytes(output.artifact)
                    output_sha = sha256_bytes(png)
                record = store.publish(
                    shard_id=shard.id,
                    stage=stage,
                    expected_sequence=expected_sequence,
                    input_fingerprint=output.input_fingerprint,
                    config_version=config.config_version,
                    input_meta=output.input_meta,
                    artifact_bytes=output.artifact,
                    output_sha256=output_sha,
                )
                if stage == "quantize":
                    png, _ = quantize_from_bytes(output.artifact)
                    atomic_write_bytes(store.output_path(shard.id), png)
                    report.output_sha256[shard.id] = output_sha or sha256_bytes(png)
                report.published.append(
                    {
                        "shard": shard.id,
                        "stage": stage,
                        "sequence": record.sequence,
                        "input_fingerprint": record.input_fingerprint,
                        "artifact_sha256": record.artifact_sha256,
                    }
                )
                if hooks.stop_after_stage == stage and not stop_after_used:
                    stop_after_used = True
                    report.interrupted = True
                    report.interrupted_after = {"shard": shard.id, "stage": stage}
                    report.stop_reason = f"injected stop after {shard.id}/{stage}"
                    raise BuildStopped(report)
                if _stop_requested(hooks):
                    report.interrupted = True
                    report.interrupted_after = {"shard": shard.id, "stage": stage}
                    report.stop_reason = _stop_reason(hooks)
                    raise BuildStopped(report)

        report.ok = True
        for shard in config.shards:
            output = store.output_path(shard.id)
            if output.is_file():
                report.output_sha256[shard.id] = sha256_file(output)
        return report


def compute_shard_from_scratch(
    shard: ShardConfig,
    config: WorkbenchConfig,
    reference: ReferenceInput | None,
) -> ComputedShard:
    outputs: dict[str, StageOutput] = {}
    for stage in STAGE_NAMES:
        outputs[stage] = _compute_stage(shard, config, stage, previous=outputs, reference=reference)
    png, _ = quantize_from_bytes(outputs["quantize"].artifact)
    return ComputedShard(stages=outputs, png_bytes=png)


def _compute_stage(
    shard: ShardConfig,
    config: WorkbenchConfig,
    stage: str,
    *,
    previous: dict[str, StageOutput],
    reference: ReferenceInput | None,
) -> StageOutput:
    if stage == "encode":
        embedding, normalized_prompt = encode_prompt(shard.prompt, config.global_config.encoder)
        artifact = embedding_artifact(shard.id, normalized_prompt, embedding)
        meta = encode_meta(shard, config.global_config.encoder)
        return StageOutput(artifact, input_fingerprint(stage, meta), meta)

    if stage == "schedule":
        embedding, _ = embedding_from_bytes(previous["encode"].artifact)
        weights = schedule_weights(shard.guidance_scale, shard.steps, shard_id=shard.id)
        artifact = schedule_artifact(shard.id, weights)
        meta = schedule_meta(shard, encode_artifact_sha256=sha256_bytes(previous["encode"].artifact))
        return StageOutput(artifact, input_fingerprint(stage, meta), meta)

    if stage == "sample":
        embedding, _ = embedding_from_bytes(previous["encode"].artifact)
        weights, _ = schedule_from_bytes(previous["schedule"].artifact)
        shape = (
            config.global_config.image.height,
            config.global_config.image.width,
            config.global_config.image.channels,
        )
        reference_tensor = reference.tensor if reference is not None else None
        tensor, trajectory = run_sampling(
            embedding,
            weights,
            shard.seed,
            shape,
            reference_tensor=reference_tensor,
            blend_strength=shard.blend_strength,
            shard_id=shard.id,
        )
        artifact = sample_artifact(shard.id, tensor, trajectory)
        meta = sample_meta(
            shard,
            config.global_config,
            schedule_artifact_sha256=sha256_bytes(previous["schedule"].artifact),
            reference_sha256=reference.sha256 if reference is not None else None,
        )
        return StageOutput(artifact, input_fingerprint(stage, meta), meta)

    if stage == "quantize":
        tensor, _ = sample_from_bytes(previous["sample"].artifact)
        quantized = quantize_to_png(tensor, compression_level=config.global_config.quantize.png_compression)
        artifact = quantize_artifact(
            shard.id,
            quantized.png_bytes,
            shape=tensor.shape,
            pre_clamp_count=quantized.pre_clamp_count,
            post_clamp_count=quantized.post_clamp_count,
        )
        meta = quantize_meta(
            shard,
            config.global_config.quantize,
            sample_artifact_sha256=sha256_bytes(previous["sample"].artifact),
        )
        output_sha = sha256_bytes(quantized.png_bytes)
        return StageOutput(artifact, input_fingerprint(stage, meta), meta, output_sha)

    raise BusinessError(ErrorCode.E_INVALID_STAGE, f"unknown stage: {stage}", field="stage")


def load_all_references(config: WorkbenchConfig) -> dict[str, ReferenceInput | None]:
    shape = (
        config.global_config.image.height,
        config.global_config.image.width,
        config.global_config.image.channels,
    )
    result: dict[str, ReferenceInput | None] = {}
    for shard in config.shards:
        if shard.reference is None:
            result[shard.id] = None
            continue
        path = config.resolve(shard.reference)
        tensor, digest = load_reference_tensor(path, shape, shard_id=shard.id)
        result[shard.id] = ReferenceInput(tensor=tensor, sha256=digest, path=str(path))
    return result


def inspect_workspace(
    config: WorkbenchConfig | None = None,
    *,
    root: Path | None = None,
    references: dict[str, ReferenceInput | None] | None = None,
) -> list[ShardDiagnostic]:
    """Read-only diagnostics. No directories or checkpoints are created."""

    if config is None:
        if root is None:
            raise ValueError("config or root is required")
        config = load_config(root / "config.yaml")
    store = CheckpointStore(config.root)
    if references is None:
        references = _safe_reference_inputs(config)
    shape = (
        config.global_config.image.height,
        config.global_config.image.width,
        config.global_config.image.channels,
    )
    result: list[ShardDiagnostic] = []
    for shard in config.shards:
        reference = references.get(shard.id)
        reference_error = None
        if shard.reference is not None and reference is None:
            try:
                load_reference_tensor(config.resolve(shard.reference), shape, shard_id=shard.id)
            except BusinessError as exc:
                reference_error = exc.message
            reference = None
        views = _inspect_shard(config, store, shard, reference, reference_error)
        output = store.output_path(shard.id)
        png_sha = sha256_file(output) if output.is_file() else None
        result.append(
            ShardDiagnostic(
                shard=shard,
                stages=views,
                png_sha256=png_sha,
                output_exists=output.is_file(),
                reference_sha256=reference.sha256 if reference is not None else None,
                reference_error=reference_error,
            )
        )
    return result


def _inspect_shard(
    config: WorkbenchConfig,
    store: CheckpointStore,
    shard: ShardConfig,
    reference: ReferenceInput | None,
    reference_error: str | None,
) -> list[StageView]:
    records = store.read_all_records(shard.id)
    views: list[StageView] = []
    upstream_valid = True
    upstream_artifact_sha: str | None = None
    earliest_invalid: str | None = None
    earliest_reason: str | None = None

    for stage in STAGE_NAMES:
        record = records.get(stage)
        expected_meta: dict[str, Any] | None = None
        expected_fp: str | None = None
        changed: list[str] = []
        reason: str | None = None
        invalidated_by: str | None = None

        if upstream_valid:
            if stage == "encode":
                expected_meta = encode_meta(shard, config.global_config.encoder)
            elif stage == "schedule":
                expected_meta = schedule_meta(shard, encode_artifact_sha256=upstream_artifact_sha or "")
            elif stage == "sample":
                if reference_error is not None:
                    reason = reference_error
                    invalidated_by = earliest_invalid or stage
                else:
                    expected_meta = sample_meta(
                        shard,
                        config.global_config,
                        schedule_artifact_sha256=upstream_artifact_sha or "",
                        reference_sha256=reference.sha256 if reference is not None else None,
                    )
            elif stage == "quantize":
                expected_meta = quantize_meta(
                    shard,
                    config.global_config.quantize,
                    sample_artifact_sha256=upstream_artifact_sha or "",
                )
            if expected_meta is not None:
                expected_fp = input_fingerprint(stage, expected_meta)

            if reason is None:
                if record is None:
                    reason = "checkpoint is missing"
                elif record.input_fingerprint != expected_fp:
                    changed = changed_meta_keys(record.input_meta, expected_meta)
                    detail = ", ".join(changed) if changed else "input fingerprint"
                    reason = f"input fingerprint changed: {detail}"
                else:
                    valid, verify_reason = store.verify_record(record)
                    if not valid:
                        reason = f"checkpoint artifact is invalid: {verify_reason}"
        else:
            reason = f"upstream stage {earliest_invalid} is invalid"
            invalidated_by = earliest_invalid

        invalid = reason is not None
        if invalid:
            if earliest_invalid is None:
                earliest_invalid = stage
                earliest_reason = reason
            if invalidated_by is None:
                invalidated_by = earliest_invalid

        trajectory: list[dict[str, Any]] = []
        clamp_counts: dict[str, int] | None = None
        current_output_sha = None
        if record is not None:
            with contextlib.suppress(ValueError, OSError):
                blob = store.load_artifact(record)
                if stage == "sample":
                    _, header = sample_from_bytes(blob)
                    trajectory = header.get("trajectory", [])
                elif stage == "quantize":
                    _, header = quantize_from_bytes(blob)
                    clamp_counts = {
                        "pre": int(header.get("pre_clamp_count", 0)),
                        "post": int(header.get("post_clamp_count", 0)),
                    }
        if stage == "quantize":
            output = store.output_path(shard.id)
            current_output_sha = sha256_file(output) if output.is_file() else None

        views.append(
            StageView(
                stage=stage,
                status="invalid" if invalid else "valid",
                invalid=invalid,
                reason=reason,
                record=record,
                expected_input_fingerprint=expected_fp,
                expected_input_meta=expected_meta,
                changed_fields=changed,
                invalidated_by=invalidated_by,
                trajectory=trajectory,
                clamp_counts=clamp_counts,
                current_output_sha256=current_output_sha,
                config_version_matches=not invalid,
            )
        )
        if invalid:
            upstream_valid = False
            upstream_artifact_sha = None
        else:
            upstream_valid = True
            upstream_artifact_sha = record.artifact_sha256 if record is not None else None
    return views


def _safe_reference_inputs(config: WorkbenchConfig) -> dict[str, ReferenceInput | None]:
    result: dict[str, ReferenceInput | None] = {}
    shape = (
        config.global_config.image.height,
        config.global_config.image.width,
        config.global_config.image.channels,
    )
    for shard in config.shards:
        if shard.reference is None:
            result[shard.id] = None
            continue
        try:
            tensor, digest = load_reference_tensor(config.resolve(shard.reference), shape, shard_id=shard.id)
        except BusinessError:
            result[shard.id] = None
        else:
            result[shard.id] = ReferenceInput(tensor, digest, str(config.resolve(shard.reference)))
    return result


def _assert_candidate_matches_fresh(
    candidates: dict[str, dict[str, bytes]],
    fresh: dict[str, ComputedShard],
) -> None:
    differences: list[dict[str, Any]] = []
    for shard_id, stages in fresh.items():
        for stage in STAGE_NAMES:
            candidate = candidates[shard_id][stage]
            expected = stages.stages[stage].artifact
            if candidate != expected:
                differences.append(
                    {
                        "shard": shard_id,
                        "stage": stage,
                        "candidate_sha256": sha256_bytes(candidate),
                        "fresh_sha256": sha256_bytes(expected),
                    }
                )
        candidate_png, _ = quantize_from_bytes(candidates[shard_id]["quantize"])
        if candidate_png != stages.png_bytes:
            differences.append(
                {
                    "shard": shard_id,
                    "stage": "png",
                    "candidate_sha256": sha256_bytes(candidate_png),
                    "fresh_sha256": sha256_bytes(stages.png_bytes),
                }
            )
    if differences:
        raise BusinessError(
            ErrorCode.E_INCONSISTENT_STATE,
            "incremental candidate is not byte-identical to a fresh full build",
            details={"differences": differences[:20], "difference_count": len(differences)},
        )


def verify_against_full(root: Path, *, config_path: Path | None = None) -> dict[str, Any]:
    """Perform a real from-zero build in a temporary workspace and compare."""

    root = root.resolve()
    config_path = (config_path or (root / "config.yaml")).resolve()
    config = load_config(config_path)
    with tempfile.TemporaryDirectory(prefix="detdiff-full-") as temp:
        shadow = Path(temp)
        _prepare_shadow_workspace(config, config_path, shadow)
        build_workspace(shadow, config_path=shadow / "config.yaml", verify_consistency=False)
        current = published_manifest(config)
        full = published_manifest(load_config(shadow / "config.yaml"))
        files: dict[str, dict[str, str | None]] = {}
        for key in sorted(set(current) | set(full)):
            if current.get(key) != full.get(key):
                files[key] = {"incremental": current.get(key), "full": full.get(key)}
        if files:
            raise BusinessError(
                ErrorCode.E_INCONSISTENT_STATE,
                "incremental publication differs from a from-zero full build",
                details={"files": files},
            )
        return {
            "ok": True,
            "files_compared": len(current),
            "manifest": current,
        }


def published_manifest(config: WorkbenchConfig) -> dict[str, str]:
    store = CheckpointStore(config.root)
    result: dict[str, str] = {}
    for shard in config.shards:
        records = store.read_all_records(shard.id)
        for stage in STAGE_NAMES:
            record = records.get(stage)
            key = f"state/{shard.id}/{stage}"
            result[key] = record.artifact_sha256 if record is not None else "<missing>"
            if stage == "quantize":
                output = store.output_path(shard.id)
                result[f"outputs/{shard.id}.png"] = sha256_file(output) if output.is_file() else "<missing>"
    return result


def _prepare_shadow_workspace(config: WorkbenchConfig, config_path: Path, shadow: Path) -> None:
    shadow.mkdir(parents=True, exist_ok=True)
    raw = {
        "schema_version": config.schema_version,
        "global": config.global_config.as_dict(),
        "shards": [],
    }
    import copy

    for shard in config.shards:
        item = shard.as_dict()
        if shard.reference is not None:
            item["reference"] = str(config.resolve(shard.reference))
        if shard.source_image is not None:
            item["source_image"] = str(config.resolve(shard.source_image))
        raw["shards"].append(item)
    (shadow / "config.yaml").write_text(
        yaml.safe_dump(copy.deepcopy(raw), allow_unicode=True, sort_keys=True),
        encoding="utf-8",
    )


def _current_output_hashes(config: WorkbenchConfig, store: CheckpointStore) -> dict[str, str]:
    result: dict[str, str] = {}
    for shard in config.shards:
        output = store.output_path(shard.id)
        if output.is_file():
            result[shard.id] = sha256_file(output)
    return result


def _stop_requested(hooks: BuildHooks) -> bool:
    if hooks.pause_file is not None and hooks.pause_file.exists():
        return True
    if hooks.should_stop is not None and hooks.should_stop():
        return True
    return False


def _stop_reason(hooks: BuildHooks) -> str:
    if hooks.pause_file is not None and hooks.pause_file.exists():
        return f"pause file detected: {hooks.pause_file}"
    if hooks.should_stop is not None and hooks.should_stop():
        return "stop signal received"
    return "build stopped"

