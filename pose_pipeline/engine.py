"""Pipeline orchestration, incremental invalidation, and resumable builds."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from .assembly import assemble_poses
from .config import PipelineConfig
from .constants import (
    ASSEMBLY,
    DECODE,
    ERROR_NOT_FOUND,
    PREPROCESS,
    SCHEMA_VERSION,
    STAGE_INDEX,
    STAGES,
    VALIDATION,
    normalize_stage,
)
from .errors import PipelineError, invalid_stage
from .models import Manifest, ShardSpec
from .peaks import decode_peaks
from .preprocess import preprocess_shard
from .store import CheckpointStore
from .util import (
    atomic_copy_file,
    atomic_write_bytes,
    atomic_write_json,
    atomic_write_pretty_json,
    digest_json,
    file_sha256,
    pretty_json_bytes,
    read_json_file,
    safe_unlink,
    sha256_bytes,
)
from .validation import build_final_report, validate_shard


class PipelineEngine:
    """A local, deterministic four-stage pose pipeline."""

    def __init__(
        self,
        workspace: str | Path,
        config: PipelineConfig | dict[str, Any] | str | Path | None = None,
    ) -> None:
        self.workspace = Path(workspace).resolve()
        if isinstance(config, PipelineConfig):
            self.config = config
        elif isinstance(config, dict):
            self.config = PipelineConfig.from_mapping(config)
        elif isinstance(config, (str, Path)):
            self.config = PipelineConfig.from_file(config)
        elif config is None:
            self.config = PipelineConfig()
        else:
            raise TypeError("config must be PipelineConfig, mapping, path, or None")
        self.store = CheckpointStore(self.workspace)
        self.store.ensure()
        self.manifest_path = self.store.inputs_dir / "manifest.json"
        self.input_index_path = self.store.state_dir / "input_index.json"
        self.config_snapshot_path = self.store.state_dir / "config_snapshot.json"
        self.invalidations_path = self.store.state_dir / "invalidations.json"
        self.errors_path = self.store.state_dir / "errors.json"
        self.exports_state_path = self.store.state_dir / "exports.json"
        self.run_status_path = self.store.state_dir / "run_status.json"

    # ------------------------------------------------------------------ import
    def import_samples(self, manifest_path: str | Path) -> dict[str, Any]:
        source_path = Path(manifest_path).resolve()
        try:
            raw = read_json_file(source_path)
        except (OSError, ValueError) as exc:
            raise PipelineError(
                "E_INVALID_MANIFEST", f"cannot read source manifest: {exc}"
            ) from exc
        if not isinstance(raw, dict):
            raise PipelineError("E_INVALID_MANIFEST", "manifest must be a JSON object")
        source_manifest = Manifest.from_mapping(raw, base_dir=source_path.parent)
        self.store.ensure()
        rewritten = copy.deepcopy(raw)
        shards_raw = rewritten.get("shards")
        if not isinstance(shards_raw, list):
            raise PipelineError("E_INVALID_MANIFEST", "manifest.shards must be an array")
        source_by_id = source_manifest.shard_map()
        images_dir = self.store.inputs_dir / "images"
        images_dir.mkdir(parents=True, exist_ok=True)
        copied: list[dict[str, Any]] = []
        for raw_shard in shards_raw:
            shard_id = str(raw_shard.get("shard_id") or raw_shard.get("id"))
            shard = source_by_id[shard_id]
            source_image = shard.image_path
            if not source_image.is_file():
                raise PipelineError(
                    "E_INVALID_MANIFEST",
                    f"missing source image for {shard_id}: {source_image}",
                )
            destination = images_dir / f"{shard_id}.png"
            atomic_copy_file(source_image, destination)
            raw_shard["image"] = f"images/{shard_id}.png"
            copied.append(
                {
                    "shard_id": shard_id,
                    "source": str(source_image),
                    "destination": str(destination),
                    "sha256": file_sha256(destination),
                }
            )
        atomic_write_pretty_json(self.manifest_path, rewritten)
        ground_truth = source_path.parent / "ground_truth.json"
        if ground_truth.is_file():
            atomic_copy_file(ground_truth, self.store.inputs_dir / "ground_truth.json")
        manifest = Manifest.from_file(self.manifest_path)
        return {
            "ok": True,
            "workspace": str(self.workspace),
            "manifest": str(self.manifest_path),
            "manifest_input_fingerprint": manifest.input_fingerprint(),
            "shards": copied,
        }

    # --------------------------------------------------------------- inspection
    def load_manifest(self) -> Manifest:
        if not self.manifest_path.is_file():
            raise PipelineError(
                ERROR_NOT_FOUND,
                "workspace has no imported manifest; call import first",
                details={"manifest_path": str(self.manifest_path)},
            )
        return Manifest.from_file(self.manifest_path)

    def _image_hashes(self, manifest: Manifest) -> dict[str, str]:
        hashes: dict[str, str] = {}
        for shard in manifest.shards:
            if not shard.image_path.is_file():
                raise PipelineError(
                    ERROR_NOT_FOUND,
                    f"image for shard {shard.shard_id} is missing",
                    stage=PREPROCESS,
                    shard_id=shard.shard_id,
                    details={"image": str(shard.image_path)},
                )
            hashes[shard.shard_id] = file_sha256(shard.image_path)
        return hashes

    def _current_input_index(
        self, manifest: Manifest, image_hashes: dict[str, str]
    ) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "coordinate_convention": manifest.coordinate_convention,
            "config_versions": {
                stage: self.config.stage_version(stage) for stage in STAGES
            },
            "shards": {
                shard.shard_id: shard.input_index(image_hashes[shard.shard_id])
                for shard in manifest.shards
            },
        }

    def _load_optional_json(self, path: Path) -> dict[str, Any] | None:
        try:
            value = read_json_file(path)
        except (OSError, ValueError, UnicodeDecodeError):
            return None
        return value if isinstance(value, dict) else None

    def _preprocess_fingerprint(self, shard: ShardSpec, image_sha256: str) -> str:
        return digest_json(
            {
                "schema_version": SCHEMA_VERSION,
                "stage": PREPROCESS,
                "coordinate_convention": "normalized_topleft_xy",
                "image_sha256": image_sha256,
                "declared_dimensions": [shard.width, shard.height],
                "generation": shard.generation,
                "truth": shard.truth_core_mapping(),
                "keypoint_names": list(shard.keypoints),
                "config_version": self.config.stage_version(PREPROCESS),
            }
        )

    def _decode_fingerprint(self, preprocess_artifact: dict[str, Any]) -> str:
        return digest_json(
            {
                "schema_version": SCHEMA_VERSION,
                "stage": DECODE,
                "upstream_artifact_digest": preprocess_artifact["artifact_digest"],
                "config_version": self.config.stage_version(DECODE),
            }
        )

    def _assembly_fingerprint(
        self, shard: ShardSpec, decode_artifact: dict[str, Any]
    ) -> str:
        return digest_json(
            {
                "schema_version": SCHEMA_VERSION,
                "stage": ASSEMBLY,
                "upstream_artifact_digest": decode_artifact["artifact_digest"],
                "keypoint_names": list(shard.keypoints),
                "skeleton": [list(edge) for edge in shard.skeleton],
                "image_dimensions": [shard.width, shard.height],
                "coordinate_convention": "normalized_topleft_xy",
                "config_version": self.config.stage_version(ASSEMBLY),
            }
        )

    def _validation_fingerprint(
        self, shard: ShardSpec, assembly_artifact: dict[str, Any]
    ) -> str:
        return digest_json(
            {
                "schema_version": SCHEMA_VERSION,
                "stage": VALIDATION,
                "upstream_artifact_digest": assembly_artifact["artifact_digest"],
                "truth": shard.truth_core_mapping(),
                "keypoint_names": list(shard.keypoints),
                "coordinate_convention": "normalized_topleft_xy",
                "source_label": shard.source_label,
                "expected_confidence": shard.expected_confidence.to_mapping(),
                "instance_expected_confidence": {
                    instance.instance_id: instance.expected_confidence_mapping()
                    for instance in shard.instances
                },
                "config_version": self.config.stage_version(VALIDATION),
            }
        )

    def _inspect(self, manifest: Manifest | None = None) -> dict[str, Any]:
        manifest = manifest or self.load_manifest()
        image_hashes = self._image_hashes(manifest)
        current_index = self._current_input_index(manifest, image_hashes)
        old_index = self._load_optional_json(self.input_index_path) or {}
        old_config = self._load_optional_json(self.config_snapshot_path) or {}
        reasons = self._change_reasons(old_index, current_index, old_config)
        records_by_stage = self.store.latest_by_stage()
        shard_results: dict[str, Any] = {}
        affected: list[dict[str, Any]] = []
        unchanged: list[str] = []
        units: list[dict[str, Any]] = []

        for shard in sorted(manifest.shards, key=lambda item: item.shard_id):
            stage_status: dict[str, dict[str, Any]] = {}
            shard_reasons = reasons.get(shard.shard_id, {})
            invalid_from: str | None = None
            upstream_invalid = False
            current_stage: str | None = None
            for stage in STAGES:
                record = records_by_stage.get((shard.shard_id, stage))
                status = "missing"
                artifact: dict[str, Any] | None = None
                expected_fingerprint: str | None = None
                if upstream_invalid:
                    status = "blocked"
                elif stage == PREPROCESS:
                    expected_fingerprint = self._preprocess_fingerprint(
                        shard, image_hashes[shard.shard_id]
                    )
                else:
                    previous_stage = STAGES[STAGE_INDEX[stage] - 2]
                    previous_status = stage_status[previous_stage]
                    previous_artifact = previous_status.get("_artifact")
                    if previous_status["status"] != "current" or previous_artifact is None:
                        status = "blocked"
                    elif stage == DECODE:
                        expected_fingerprint = self._decode_fingerprint(previous_artifact)
                    elif stage == ASSEMBLY:
                        expected_fingerprint = self._assembly_fingerprint(
                            shard, previous_artifact
                        )
                    else:
                        expected_fingerprint = self._validation_fingerprint(
                            shard, previous_artifact
                        )
                if status != "blocked":
                    if record is None:
                        status = "missing"
                    else:
                        loaded = self.store.load_artifact(record)
                        if loaded is None:
                            status = "unavailable"
                        elif str(record.get("input_fingerprint")) != expected_fingerprint:
                            status = "stale"
                        else:
                            status = "current"
                            artifact = loaded
                if status != "current":
                    upstream_invalid = True
                    if invalid_from is None:
                        invalid_from = stage
                    if record is None:
                        shard_reasons.setdefault(stage, []).append(
                            {"code": "missing_stage_checkpoint", "stage": stage}
                        )
                    elif record is not None and status in {"stale", "unavailable"}:
                        shard_reasons.setdefault(stage, []).append(
                            {"code": f"{status}_stage_checkpoint", "stage": stage}
                        )
                    elif status == "blocked":
                        shard_reasons.setdefault(stage, []).append(
                            {"code": "upstream_stage_invalidated", "stage": stage}
                        )
                else:
                    current_stage = stage
                stage_status[stage] = {
                    "status": status,
                    "sequence": int(record["sequence"]) if record else None,
                    "shard_sequence": int(record["shard_sequence"]) if record else None,
                    "input_fingerprint": expected_fingerprint,
                    "recorded_input_fingerprint": record.get("input_fingerprint") if record else None,
                    "config_version": record.get("config_version") if record else None,
                    "artifact_digest": record.get("artifact_digest") if record else None,
                    "record_digest": record.get("record_digest") if record else None,
                    "_artifact": artifact,
                }
            for stage in STAGES:
                if stage_status[stage]["status"] != "current":
                    units.append(
                        {
                            "shard_id": shard.shard_id,
                            "stage": stage,
                            "stage_index": STAGE_INDEX[stage],
                        }
                    )
            serializable_statuses = {
                stage: {key: value for key, value in payload.items() if key != "_artifact"}
                for stage, payload in stage_status.items()
            }
            result = {
                "source_label": shard.source_label,
                "earliest_affected_stage": invalid_from,
                "reasons": [
                    reason
                    for stage in STAGES
                    for reason in shard_reasons.get(stage, [])
                ],
                "current_stage": current_stage,
                "next_stage": (
                    STAGES[STAGE_INDEX[current_stage]]
                    if current_stage is not None and STAGE_INDEX[current_stage] < 4
                    else None
                ),
                "complete": current_stage == VALIDATION,
                "stages": serializable_statuses,
            }
            shard_results[shard.shard_id] = result
            if invalid_from is not None:
                affected.append(
                    {
                        "shard_id": shard.shard_id,
                        "source_label": shard.source_label,
                        "earliest_stage": invalid_from,
                        "affected_stages": list(
                            STAGES[STAGE_INDEX[invalid_from] - 1 :]
                        ),
                        "reasons": result["reasons"],
                        "latest_fingerprints": {
                            stage: stage_status[stage]["input_fingerprint"]
                            for stage in STAGES
                        },
                    }
                )
            else:
                unchanged.append(shard.shard_id)

        current_ids = set(shard_results)
        removed = sorted(set((old_index.get("shards") or {}).keys()) - current_ids)
        for shard_id in removed:
            affected.append(
                {
                    "shard_id": shard_id,
                    "source_label": None,
                    "earliest_stage": PREPROCESS,
                    "affected_stages": list(STAGES),
                    "reasons": [{"code": "shard_removed", "stage": PREPROCESS}],
                    "latest_fingerprints": {stage: None for stage in STAGES},
                }
            )
        units.sort(key=lambda unit: (unit["stage_index"], unit["shard_id"]))
        complete = bool(shard_results) and all(
            result["complete"] for result in shard_results.values()
        )
        earliest = None
        for item in affected:
            stage = item["earliest_stage"]
            if stage is not None and (
                earliest is None or STAGE_INDEX[stage] < STAGE_INDEX[earliest]
            ):
                earliest = stage
        invalidation_report = {
            "schema_version": SCHEMA_VERSION,
            "status": "planned",
            "manifest_input_fingerprint": manifest.input_fingerprint(),
            "config_version": self.config.config_version(),
            "earliest_stage": earliest,
            "affected_shards": affected,
            "affected_shard_ids": [item["shard_id"] for item in affected],
            "unchanged_shards": unchanged,
            "latest_fingerprints": {
                shard_id: {
                    stage: payload["input_fingerprint"]
                    for stage, payload in result["stages"].items()
                }
                for shard_id, result in sorted(shard_results.items())
            },
            "planned_units": units,
            "checkpoint_head": self.store.head(),
        }
        return {
            "manifest": manifest,
            "image_hashes": image_hashes,
            "current_index": current_index,
            "shards": shard_results,
            "units": units,
            "affected": affected,
            "unchanged": unchanged,
            "complete": complete,
            "earliest_stage": earliest,
            "invalidation_report": invalidation_report,
        }

    def _change_reasons(
        self,
        old_index: dict[str, Any],
        current_index: dict[str, Any],
        old_config: dict[str, Any],
    ) -> dict[str, dict[str, list[dict[str, Any]]]]:
        result: dict[str, dict[str, list[dict[str, Any]]]] = {}
        old_shards = old_index.get("shards") if isinstance(old_index.get("shards"), dict) else {}
        if not old_shards:
            for shard_id in current_index["shards"]:
                result.setdefault(shard_id, {}).setdefault(PREPROCESS, []).append(
                    {"code": "new_shard", "stage": PREPROCESS}
                )
        for shard_id, new_meta in sorted(current_index["shards"].items()):
            old_meta = old_shards.get(shard_id)
            if not isinstance(old_meta, dict):
                result.setdefault(shard_id, {}).setdefault(PREPROCESS, []).append(
                    {"code": "new_shard", "stage": PREPROCESS}
                )
                continue
            field_stage = (
                ("image_sha256", PREPROCESS, "image_content_changed"),
                ("width", PREPROCESS, "image_dimensions_changed"),
                ("height", PREPROCESS, "image_dimensions_changed"),
                ("truth_digest", PREPROCESS, "ground_truth_changed"),
                ("visibility_digest", PREPROCESS, "visibility_changed"),
                ("generation_digest", PREPROCESS, "generation_parameters_changed"),
                ("keypoints", PREPROCESS, "keypoint_definition_changed"),
                ("skeleton_digest", ASSEMBLY, "skeleton_definition_changed"),
                ("source_label", VALIDATION, "source_label_changed"),
                ("expected_confidence_digest", VALIDATION, "expected_confidence_changed"),
            )
            for field, stage, code in field_stage:
                if old_meta.get(field) != new_meta.get(field):
                    result.setdefault(shard_id, {}).setdefault(stage, []).append(
                        {
                            "code": code,
                            "stage": stage,
                            "field": field,
                            "before": old_meta.get(field),
                            "after": new_meta.get(field),
                        }
                    )
        if old_index.get("coordinate_convention") != current_index.get("coordinate_convention"):
            for shard_id in current_index["shards"]:
                result.setdefault(shard_id, {}).setdefault(VALIDATION, []).append(
                    {"code": "coordinate_convention_changed", "stage": VALIDATION}
                )
        old_versions = old_index.get("config_versions") or old_config.get("stage_versions") or {}
        new_versions = current_index["config_versions"]
        for stage in STAGES:
            old_version = old_versions.get(stage) if isinstance(old_versions, dict) else None
            if old_version != new_versions[stage]:
                for shard_id in current_index["shards"]:
                    result.setdefault(shard_id, {}).setdefault(stage, []).append(
                        {
                            "code": f"{stage}_config_changed",
                            "stage": stage,
                            "before": old_version,
                            "after": new_versions[stage],
                        }
                    )
        return result

    # ---------------------------------------------------------------- execution
    def _record_run_status(self, value: dict[str, Any]) -> None:
        atomic_write_json(self.run_status_path, value)

    def _save_input_state(self, inspection: dict[str, Any], status: str) -> None:
        atomic_write_json(self.input_index_path, inspection["current_index"])
        atomic_write_json(
            self.config_snapshot_path,
            {
                "schema_version": SCHEMA_VERSION,
                "config": self.config.to_mapping(),
                "config_version": self.config.config_version(),
                "stage_versions": {
                    stage: self.config.stage_version(stage) for stage in STAGES
                },
            },
        )
        report = copy.deepcopy(inspection["invalidation_report"])
        report["status"] = status
        atomic_write_json(self.invalidations_path, report)

    def _save_error(self, error: PipelineError) -> None:
        prior = self._load_optional_json(self.errors_path) or {"errors": []}
        errors = list(prior.get("errors", []))
        errors.append(error.to_dict())
        payload = {
            "schema_version": SCHEMA_VERSION,
            "errors": errors,
            "latest": error.to_dict(),
        }
        atomic_write_json(self.errors_path, payload)

    def _load_upstream_artifact(
        self, shard_id: str, previous_stage: str
    ) -> dict[str, Any] | None:
        records = self.store.latest_by_stage()
        record = records.get((shard_id, previous_stage))
        if record is None:
            return None
        return self.store.load_artifact(record)

    def _compute_current_fingerprint(
        self, shard: ShardSpec, stage: str, image_sha256: str
    ) -> tuple[str, dict[str, Any] | None]:
        if stage == PREPROCESS:
            return self._preprocess_fingerprint(shard, image_sha256), None
        previous_stage = STAGES[STAGE_INDEX[stage] - 2]
        upstream = self._load_upstream_artifact(shard.shard_id, previous_stage)
        if upstream is None:
            raise invalid_stage(
                f"missing prerequisite artifact for {shard.shard_id}/{stage}",
                shard_id=shard.shard_id,
                current_stage=None,
                details={"required_stage": previous_stage, "requested_stage": stage},
            )
        if stage == DECODE:
            fingerprint = self._decode_fingerprint(upstream)
        elif stage == ASSEMBLY:
            fingerprint = self._assembly_fingerprint(shard, upstream)
        elif stage == VALIDATION:
            fingerprint = self._validation_fingerprint(shard, upstream)
        else:
            raise ValueError(f"unknown stage: {stage}")
        return fingerprint, upstream

    def _is_stage_current(
        self, shard: ShardSpec, stage: str, fingerprint: str
    ) -> dict[str, Any] | None:
        record = self.store.latest_by_stage().get((shard.shard_id, stage))
        if record is None:
            return None
        if record.get("input_fingerprint") != fingerprint:
            return None
        return self.store.load_artifact(record)

    def _execute_stage(
        self, shard: ShardSpec, stage: str, image_sha256: str
    ) -> dict[str, Any]:
        fingerprint, upstream = self._compute_current_fingerprint(
            shard, stage, image_sha256
        )
        existing = self._is_stage_current(shard, stage, fingerprint)
        if existing is not None:
            return {
                "status": "reused",
                "shard_id": shard.shard_id,
                "stage": stage,
                "artifact_digest": existing["artifact_digest"],
            }
        if stage == PREPROCESS:
            artifact = preprocess_shard(
                shard,
                self.config,
                input_fingerprint=fingerprint,
            )
        elif stage == DECODE:
            assert upstream is not None
            artifact = decode_peaks(
                upstream,
                self.config,
                input_fingerprint=fingerprint,
            )
        elif stage == ASSEMBLY:
            assert upstream is not None
            artifact = assemble_poses(
                upstream,
                shard_id=shard.shard_id,
                keypoint_names=shard.keypoints,
                skeleton=shard.skeleton,
                width=shard.width,
                height=shard.height,
                coordinate_convention="normalized_topleft_xy",
                config=self.config,
                input_fingerprint=fingerprint,
            )
        elif stage == VALIDATION:
            assert upstream is not None
            artifact = validate_shard(
                upstream,
                shard,
                self.config,
                input_fingerprint=fingerprint,
            )
        else:
            raise ValueError(f"unknown stage: {stage}")
        self.store.write_artifact(artifact)
        expected_sequence = self.store.next_sequence()
        try:
            record = self.store.publish(
                shard_id=shard.shard_id,
                stage=stage,
                input_fingerprint=fingerprint,
                config_version=str(artifact["config_version"]),
                artifact_digest=str(artifact["artifact_digest"]),
                upstream_digest=(
                    str(artifact.get("upstream_digest"))
                    if artifact.get("upstream_digest") is not None
                    else None
                ),
                expected_sequence=expected_sequence,
                expected_shard_id=shard.shard_id,
                expected_stage=stage,
                artifact_meta={
                    "candidate_count": artifact.get("candidate_count"),
                    "instance_count": artifact.get("instance_count"),
                },
            )
        except PipelineError as exc:
            self.store.record_attempt(
                shard_id=shard.shard_id,
                stage=stage,
                artifact_digest=str(artifact["artifact_digest"]),
                status="rejected",
                sequence_expected=expected_sequence,
                error=exc.to_dict(),
            )
            raise
        self.store.record_attempt(
            shard_id=shard.shard_id,
            stage=stage,
            artifact_digest=str(artifact["artifact_digest"]),
            status="committed",
            sequence_expected=record["sequence"],
        )
        return {
            "status": "committed",
            "shard_id": shard.shard_id,
            "stage": stage,
            "sequence": record["sequence"],
            "artifact_digest": artifact["artifact_digest"],
        }

    def build(
        self,
        *,
        to_stage: str | int | None = None,
        stop_after_units: int | None = None,
        clear_stop: bool = False,
        strict_shard_id: str | None = None,
        strict_stage: str | int | None = None,
    ) -> dict[str, Any]:
        if clear_stop:
            self.store.clear_stop()
        try:
            target_stage = normalize_stage(to_stage, default=VALIDATION)
        except ValueError as exc:
            raise invalid_stage(str(exc)) from exc
        if strict_shard_id is not None or strict_stage is not None:
            if strict_shard_id is None or strict_stage is None:
                raise invalid_stage("strict builds require both shard_id and stage")
            return self._build_strict(strict_shard_id, strict_stage)
        inspection = self._inspect()
        initial_report = copy.deepcopy(inspection["invalidation_report"])
        self._save_input_state(inspection, "planned")
        if inspection["units"]:
            self._invalidate_exports("incremental rebuild pending")
        target_index = STAGE_INDEX[target_stage]
        planned_units = [
            unit for unit in inspection["units"] if unit["stage_index"] <= target_index
        ]
        completed: list[dict[str, Any]] = []
        if stop_after_units is not None and stop_after_units < 0:
            raise invalid_stage("stop_after_units must be non-negative")
        stopped = self.store.stop_requested()
        status = "stopped_before_unit" if stopped else "running"
        self._record_run_status(
            {
                "schema_version": SCHEMA_VERSION,
                "status": status,
                "target_stage": target_stage,
                "planned_units": len(planned_units),
                "completed_units": 0,
            }
        )
        manifest = inspection["manifest"]
        shard_map = manifest.shard_map()
        for unit in planned_units:
            if self.store.stop_requested():
                status = "stopped_before_unit" if not completed else "stopped"
                stopped = True
                break
            if stop_after_units is not None and len(completed) >= stop_after_units:
                status = "stopped_after_unit_limit"
                stopped = True
                break
            try:
                result = self._execute_stage(
                    shard_map[unit["shard_id"]],
                    unit["stage"],
                    inspection["image_hashes"][unit["shard_id"]],
                )
            except PipelineError as exc:
                self._save_error(exc)
                failed_report = initial_report
                failed_report["status"] = "failed"
                failed_report["error"] = exc.to_dict()
                failed_report["completed_units"] = completed
                failed_report["completed_unit_count"] = len(completed)
                failed_report["pipeline_complete"] = False
                failed_report["checkpoint_head"] = self.store.head()
                atomic_write_json(self.invalidations_path, failed_report)
                self._record_run_status(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "status": "failed",
                        "target_stage": target_stage,
                        "planned_units": len(planned_units),
                        "completed_units": len(completed),
                        "error": exc.to_dict(),
                    }
                )
                raise
            completed.append(result)
            self._record_run_status(
                {
                    "schema_version": SCHEMA_VERSION,
                    "status": "running",
                    "target_stage": target_stage,
                    "planned_units": len(planned_units),
                    "completed_units": len(completed),
                    "last_completed": result,
                }
            )
        if not stopped:
            status = "completed_to_stage" if target_index < 4 else "completed"
        final_inspection = self._inspect()
        if final_inspection["complete"]:
            self._write_exports(final_inspection)
            if status == "completed_to_stage":
                status = "completed"
        else:
            self._invalidate_exports("pipeline is not complete")
        report = initial_report
        report["status"] = status
        report["completed_units"] = completed
        report["completed_unit_count"] = len(completed)
        report["pipeline_complete"] = final_inspection["complete"]
        report["latest_fingerprints"] = final_inspection["invalidation_report"][
            "latest_fingerprints"
        ]
        report["final_checkpoint_head"] = self.store.head()
        atomic_write_json(self.invalidations_path, report)
        self._record_run_status(
            {
                "schema_version": SCHEMA_VERSION,
                "status": status,
                "target_stage": target_stage,
                "planned_units": len(planned_units),
                "completed_units": len(completed),
                "pipeline_complete": final_inspection["complete"],
            }
        )
        return {
            "ok": True,
            "status": status,
            "workspace": str(self.workspace),
            "target_stage": target_stage,
            "completed_units": completed,
            "planned_unit_count": len(planned_units),
            "pipeline_complete": final_inspection["complete"],
            "invalidation": report,
            "fingerprints": self._fingerprint_payload(final_inspection),
        }

    def _build_strict(self, shard_id: str, stage: str | int) -> dict[str, Any]:
        try:
            target_stage = normalize_stage(stage)
        except ValueError as exc:
            raise invalid_stage(str(exc), shard_id=shard_id) from exc
        inspection = self._inspect()
        shard_result = inspection["shards"].get(shard_id)
        if shard_result is None:
            raise PipelineError(
                ERROR_NOT_FOUND, f"unknown shard: {shard_id}", shard_id=shard_id
            )
        current_stage = shard_result["current_stage"]
        current_index = STAGE_INDEX[current_stage] if current_stage else 0
        target_index = STAGE_INDEX[target_stage]
        if target_index > current_index + 1:
            raise invalid_stage(
                "requested stage jump is not allowed; build prerequisites first",
                shard_id=shard_id,
                current_stage=current_stage,
                details={"requested_stage": target_stage},
            )
        if target_index <= current_index:
            return {
                "ok": True,
                "status": "already_current",
                "shard_id": shard_id,
                "stage": target_stage,
                "current_stage": current_stage,
            }
        self._save_input_state(inspection, "planned")
        shard = inspection["manifest"].shard_map()[shard_id]
        try:
            result = self._execute_stage(
                shard, target_stage, inspection["image_hashes"][shard_id]
            )
        except PipelineError as exc:
            self._save_error(exc)
            failed_report = inspection["invalidation_report"]
            failed_report["status"] = "failed"
            failed_report["error"] = exc.to_dict()
            failed_report["checkpoint_head"] = self.store.head()
            atomic_write_json(self.invalidations_path, failed_report)
            raise
        final_inspection = self._inspect()
        if final_inspection["complete"]:
            self._write_exports(final_inspection)
        return {
            "ok": True,
            "status": "committed",
            "shard_id": shard_id,
            "stage": target_stage,
            "result": result,
            "pipeline_complete": final_inspection["complete"],
        }

    # --------------------------------------------------------------- public API
    def request_stop(self) -> dict[str, Any]:
        self.store.request_stop()
        return {
            "ok": True,
            "stop_requested": True,
            "message": "the current atomic shard-stage unit will finish before stopping",
        }

    def clear_stop(self) -> dict[str, Any]:
        self.store.clear_stop()
        return {"ok": True, "stop_requested": False}

    def resume(
        self,
        *,
        to_stage: str | int | None = None,
        stop_after_units: int | None = None,
    ) -> dict[str, Any]:
        self.store.clear_stop()
        return self.build(to_stage=to_stage, stop_after_units=stop_after_units)

    def status(self) -> dict[str, Any]:
        inspection = self._inspect()
        store_scan = self.store.scan()
        exports_state = self._load_optional_json(self.exports_state_path) or {
            "valid": False,
            "reason": "not_generated",
        }
        if not inspection["complete"]:
            exports_state = {
                "schema_version": SCHEMA_VERSION,
                "valid": False,
                "stale": True,
                "reason": "pipeline_incomplete_or_invalidated",
                "prior_exports": exports_state,
            }
        return {
            "ok": True,
            "workspace": str(self.workspace),
            "manifest_input_fingerprint": inspection["manifest"].input_fingerprint(),
            "config_version": self.config.config_version(),
            "stop_requested": self.store.stop_requested(),
            "pipeline_complete": inspection["complete"],
            "earliest_affected_stage": inspection["earliest_stage"],
            "affected_shards": [item["shard_id"] for item in inspection["affected"]],
            "planned_units": inspection["units"],
            "shards": inspection["shards"],
            "checkpoint": {
                "head": store_scan.head,
                "diagnostics": store_scan.diagnostics,
            },
            "exports": exports_state,
            "run_status": self._load_optional_json(self.run_status_path)
            or {"status": "never_run"},
            "errors": self._load_optional_json(self.errors_path),
        }

    def _fingerprint_payload(self, inspection: dict[str, Any]) -> dict[str, Any]:
        shards: dict[str, Any] = {}
        for shard_id, result in sorted(inspection["shards"].items()):
            shards[shard_id] = {
                stage: {
                    "status": payload["status"],
                    "input_fingerprint": payload["input_fingerprint"],
                    "config_version": payload["config_version"]
                    or self.config.stage_version(stage),
                    "artifact_digest": payload["artifact_digest"],
                }
                for stage, payload in result["stages"].items()
            }
        payload = {
            "schema_version": SCHEMA_VERSION,
            "manifest_input_fingerprint": inspection["manifest"].input_fingerprint(),
            "config_version": self.config.config_version(),
            "shards": shards,
        }
        payload["version_fingerprint"] = digest_json(payload)
        return payload

    def fingerprints(self) -> dict[str, Any]:
        return {"ok": True, **self._fingerprint_payload(self._inspect())}

    def invalidations(self, *, refresh: bool = False) -> dict[str, Any]:
        saved = self._load_optional_json(self.invalidations_path)
        if saved is not None and not refresh:
            return {"ok": True, **saved}
        inspection = self._inspect()
        return {"ok": True, **inspection["invalidation_report"]}

    def poses(self) -> dict[str, Any]:
        self._require_current_complete("pose export")
        path = self.store.exports_dir / "poses.json"
        try:
            value = read_json_file(path)
        except (OSError, ValueError, UnicodeDecodeError) as exc:
            raise PipelineError(
                ERROR_NOT_FOUND,
                "pose export is not available; run a complete build first",
                details={"path": str(path)},
            ) from exc
        claimed = value.get("export_digest")
        payload = dict(value)
        payload.pop("export_digest", None)
        if claimed != digest_json(payload):
            raise PipelineError(
                ERROR_NOT_FOUND,
                "pose export failed integrity verification",
                details={"path": str(path)},
            )
        return value

    def report(self) -> dict[str, Any]:
        self._require_current_complete("verification report")
        path = self.store.exports_dir / "report.json"
        try:
            value = read_json_file(path)
        except (OSError, ValueError, UnicodeDecodeError) as exc:
            raise PipelineError(
                ERROR_NOT_FOUND,
                "verification report is not available; run a complete build first",
                details={"path": str(path)},
            ) from exc
        claimed = value.get("report_digest")
        payload = dict(value)
        payload.pop("report_digest", None)
        if claimed != digest_json(payload):
            raise PipelineError(
                ERROR_NOT_FOUND,
                "verification report failed integrity verification",
                details={"path": str(path)},
            )
        return value

    def _require_current_complete(self, label: str) -> None:
        inspection = self._inspect()
        if not inspection["complete"]:
            raise PipelineError(
                ERROR_NOT_FOUND,
                f"{label} is stale or unavailable; rebuild the invalidated stages first",
                details={
                    "reason": "pipeline_incomplete_or_invalidated",
                    "planned_units": len(inspection["units"]),
                },
            )

    def keypoint_errors(self) -> dict[str, Any]:
        inspection = self._inspect()
        if not inspection["complete"]:
            raise invalid_stage(
                "keypoint errors require a complete validation stage",
                details={"planned_units": len(inspection["units"])},
            )
        rows: list[dict[str, Any]] = []
        for shard in sorted(inspection["manifest"].shards, key=lambda item: item.shard_id):
            record = self.store.latest_by_stage().get((shard.shard_id, VALIDATION))
            summary = self.store.load_artifact(record) if record else None
            if summary is None:
                raise invalid_stage(
                    "validation artifact is unavailable",
                    shard_id=shard.shard_id,
                )
            for instance in summary["instances"]:
                for keypoint in instance["keypoints"]:
                    if keypoint["status"] != "matched":
                        continue
                    rows.append(
                        {
                            "shard_id": shard.shard_id,
                            "source_label": summary["source_label"],
                            "truth_instance_id": instance["truth_instance_id"],
                            "predicted_instance_id": instance["predicted_instance_id"],
                            **keypoint,
                        }
                    )
        rows.sort(
            key=lambda row: (
                row["shard_id"],
                row["truth_instance_id"],
                row["keypoint"],
            )
        )
        return {
            "ok": True,
            "coordinate_convention": "normalized_topleft_xy",
            "keypoints": rows,
            "count": len(rows),
        }

    def export(self, out_dir: str | Path | None = None) -> dict[str, Any]:
        inspection = self._inspect()
        if not inspection["complete"]:
            raise invalid_stage(
                "cannot export before all shards reach validation",
                details={"planned_units": len(inspection["units"])},
            )
        self._write_exports(inspection)
        report = self.report()
        poses = self.poses()
        result: dict[str, Any] = {
            "ok": True,
            "report_digest": report["report_digest"],
            "report": report,
            "poses": poses,
            "paths": {
                "report": str(self.store.exports_dir / "report.json"),
                "poses": str(self.store.exports_dir / "poses.json"),
            },
        }
        if out_dir is not None:
            destination = Path(out_dir).resolve()
            destination.mkdir(parents=True, exist_ok=True)
            report_bytes = pretty_json_bytes(report)
            poses_bytes = pretty_json_bytes(poses)
            atomic_write_bytes(destination / "report.json", report_bytes)
            atomic_write_bytes(destination / "poses.json", poses_bytes)
            result["paths"]["export_report"] = str(destination / "report.json")
            result["paths"]["export_poses"] = str(destination / "poses.json")
        return result

    def uncommitted_artifacts(self) -> dict[str, Any]:
        return {
            "ok": True,
            "artifacts": self.store.list_uncommitted_artifacts(),
            "note": "uncommitted artifacts are readable but never used by summaries",
        }

    # ------------------------------------------------------------------ exports
    def _write_exports(self, inspection: dict[str, Any]) -> None:
        manifest = inspection["manifest"]
        versions = self._fingerprint_payload(inspection)
        summaries: list[dict[str, Any]] = []
        for shard in sorted(manifest.shards, key=lambda item: item.shard_id):
            record = self.store.latest_by_stage().get((shard.shard_id, VALIDATION))
            if record is None:
                self._invalidate_exports(f"validation missing for {shard.shard_id}")
                return
            summary = self.store.load_artifact(record)
            if summary is None:
                self._invalidate_exports(f"validation artifact unavailable for {shard.shard_id}")
                return
            summaries.append(summary)
        poses, report = build_final_report(
            summaries,
            manifest_input_fingerprint=manifest.input_fingerprint(),
            config=self.config,
            version_fingerprints=versions,
        )
        poses_bytes = pretty_json_bytes(poses)
        report_bytes = pretty_json_bytes(report)
        atomic_write_bytes(self.store.exports_dir / "poses.json", poses_bytes)
        atomic_write_bytes(self.store.exports_dir / "report.json", report_bytes)
        state = {
            "schema_version": SCHEMA_VERSION,
            "valid": True,
            "poses_digest": sha256_bytes(poses_bytes),
            "report_digest": report["report_digest"],
            "report_file_sha256": sha256_bytes(report_bytes),
            "paths": {
                "poses": "exports/poses.json",
                "report": "exports/report.json",
            },
        }
        atomic_write_json(self.exports_state_path, state)

    def _invalidate_exports(self, reason: str) -> None:
        safe_unlink(self.store.exports_dir / "poses.json")
        safe_unlink(self.store.exports_dir / "report.json")
        atomic_write_json(
            self.exports_state_path,
            {
                "schema_version": SCHEMA_VERSION,
                "valid": False,
                "reason": reason,
                "paths": {
                    "poses": "exports/poses.json",
                    "report": "exports/report.json",
                },
            },
        )

    def errors(self) -> dict[str, Any]:
        errors = self._load_optional_json(self.errors_path)
        return {"ok": True, "errors": errors or {"errors": [], "latest": None}}
