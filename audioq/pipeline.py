"""Four-stage quality assessment, reuse, invalidation and report generation."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .audio import evaluate_metrics, frame_audio, read_pcm_wav
from .canonical import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_json_bytes,
    fingerprint,
    read_json,
    sha256_bytes,
    sha256_json,
)
from .checkpoints import STAGES, CheckpointManager
from .errors import AudioQError
from .fusion import fuse_metrics
from .models import EvaluationConfig, EvaluationModel, ReferenceConfig, load_config_file


REPORT_SCHEMA_VERSION = 1
STAGE_ALGORITHM_VERSIONS = {
    "frame": "hann-frame-v1",
    "metrics": "snr-spectral-flatness-clipping-v1",
    "fusion": "weighted-log-normalized-decimal-v1",
    "diagnostics": "four-stage-diagnostics-v1",
}


def _implementation_sha(filename: str) -> str:
    return sha256_bytes((Path(__file__).with_name(filename)).read_bytes())


_IMPLEMENTATION_SHAS = {
    "audio": _implementation_sha("audio.py"),
    "fusion": _implementation_sha("fusion.py"),
    "pipeline": _implementation_sha("pipeline.py"),
}


@dataclass(frozen=True)
class ShardSpec:
    shard_id: str
    path: Path
    relative_path: str
    declared_sample_count: int
    declared_sample_rate: int
    language: str
    order: int
    reference: ReferenceConfig

    def reference_dict(self) -> dict[str, float]:
        return self.reference.to_dict()


def _positive_int(value: Any, field: str, *, shard_id: str | None = None, allow_zero: bool = False) -> int:
    minimum = 0 if allow_zero else 1
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise AudioQError(
            "E_MANIFEST",
            f"manifest field {field} must be a {'non-negative' if allow_zero else 'positive'} integer",
            shard_id=shard_id,
            observed={field: value},
        )
    return value


def _error_artifact(stage: str, shard_id: str, exc: AudioQError, **extra: Any) -> dict[str, Any]:
    if exc.shard_id is None:
        exc.shard_id = shard_id
    if exc.stage is None:
        exc.stage = stage
    return {
        "status": "error",
        "shard_id": shard_id,
        "stage": stage,
        "errors": [exc.to_dict()],
        **extra,
    }


class EvaluationWorkbench:
    def __init__(
        self,
        root: Path | str,
        manifest_path: Path | str,
        config_path: Path | str,
        state_dir: Path | str = "work/state",
        *,
        normalize_policy: str = "normalize",
        snr_threshold_rule: str = "fixed",
        default_snr_threshold_db: float = 8.0,
    ) -> None:
        self.root = Path(root).resolve()
        self.manifest_path = self._resolve_local_path(manifest_path)
        self.config_path = self._resolve_local_path(config_path)
        state = Path(state_dir)
        if state.is_absolute():
            self.state_dir = state
        else:
            self.state_dir = self.root / state
        self.config = load_config_file(
            self.config_path,
            normalize_policy=normalize_policy,
            snr_threshold_rule=snr_threshold_rule,
            default_snr_threshold_db=default_snr_threshold_db,
        )
        self.manifest_raw = read_json(self.manifest_path)
        self.shards = self._parse_manifest(self.manifest_raw)
        self.manifest_fingerprint = self._manifest_fingerprint()
        self.checkpoints = CheckpointManager(self.state_dir)
        self._source_sha_cache: dict[str, str] = {}

    def _resolve_local_path(self, path: Path | str) -> Path:
        candidate = Path(path)
        if not candidate.is_absolute():
            candidate = self.root / candidate
        resolved = candidate.resolve()
        try:
            resolved.relative_to(self.root)
        except ValueError as exc:
            raise AudioQError(
                "E_MANIFEST",
                "workspace paths must be relative and remain inside the workspace",
                observed={"path": str(path)},
            ) from exc
        return resolved

    def _relative(self, path: Path) -> str:
        try:
            return path.resolve().relative_to(self.root).as_posix()
        except ValueError:
            return str(path)

    def _parse_manifest(self, value: Any) -> list[ShardSpec]:
        if not isinstance(value, Mapping):
            raise AudioQError("E_MANIFEST", "manifest must be a JSON object")
        shards_raw = value.get("shards")
        if not isinstance(shards_raw, list) or not shards_raw:
            raise AudioQError("E_MANIFEST", "manifest must contain a non-empty shards array")
        shards: list[ShardSpec] = []
        ids: set[str] = set()
        orders: set[int] = set()
        for item in shards_raw:
            if not isinstance(item, Mapping):
                raise AudioQError("E_MANIFEST", "each shard entry must be an object")
            shard_id = item.get("shard_id", item.get("id"))
            if not isinstance(shard_id, str) or not shard_id.strip():
                raise AudioQError("E_MANIFEST", "each shard needs a non-empty shard_id")
            if shard_id in ids:
                raise AudioQError("E_MANIFEST", "duplicate shard_id", shard_id=shard_id)
            ids.add(shard_id)
            path_value = item.get("path")
            if not isinstance(path_value, str) or not path_value:
                raise AudioQError("E_MANIFEST", "each shard needs a relative WAV path", shard_id=shard_id)
            path_obj = Path(path_value)
            if path_obj.is_absolute():
                raise AudioQError(
                    "E_MANIFEST",
                    "shard paths must be relative to the workspace",
                    shard_id=shard_id,
                    observed={"path": path_value},
                )
            path = self._resolve_local_path(path_obj)
            declared_count = _positive_int(
                item.get("num_samples", item.get("sample_count")),
                "num_samples",
                shard_id=shard_id,
                allow_zero=True,
            )
            declared_rate = _positive_int(item.get("sample_rate"), "sample_rate", shard_id=shard_id)
            language = item.get("language")
            if not isinstance(language, str) or not language.strip():
                raise AudioQError("E_MANIFEST", "each shard needs a language label", shard_id=shard_id)
            order = item.get("order")
            if isinstance(order, bool) or not isinstance(order, int):
                raise AudioQError("E_MANIFEST", "each shard needs an integer order", shard_id=shard_id)
            if order in orders:
                raise AudioQError("E_MANIFEST", "duplicate shard order", shard_id=shard_id, observed={"order": order})
            orders.add(order)
            reference_raw = item.get("reference", item.get("reference_interval"))
            if not isinstance(reference_raw, Mapping):
                raise AudioQError(
                    "E_MANIFEST",
                    "each shard needs a MOS reference interval",
                    shard_id=shard_id,
                )
            try:
                low = float(reference_raw.get("min_mos", reference_raw.get("min")))
                high = float(reference_raw.get("max_mos", reference_raw.get("max")))
                target_raw = reference_raw.get("target_mos", reference_raw.get("target"))
                reference = ReferenceConfig(
                    min_mos=low,
                    max_mos=high,
                    target_mos=(low + high) / 2.0 if target_raw is None else float(target_raw),
                )
            except (TypeError, ValueError) as exc:
                raise AudioQError(
                    "E_MANIFEST",
                    "reference interval values must be numeric",
                    shard_id=shard_id,
                    observed={"reference": dict(reference_raw)},
                ) from exc
            shards.append(
                ShardSpec(
                    shard_id=shard_id,
                    path=path,
                    relative_path=path_obj.as_posix(),
                    declared_sample_count=declared_count,
                    declared_sample_rate=declared_rate,
                    language=language,
                    order=order,
                    reference=reference,
                )
            )
        return sorted(shards, key=lambda item: (item.order, item.shard_id))

    def _manifest_fingerprint(self) -> str:
        semantic = {
            "manifest_version": self.manifest_raw.get("manifest_version", 1),
            "evaluation_id": self.manifest_raw.get("evaluation_id", self.config.evaluation_id),
            "shards": [
                {
                    "shard_id": shard.shard_id,
                    "path": shard.relative_path,
                    "num_samples": shard.declared_sample_count,
                    "sample_rate": shard.declared_sample_rate,
                    "language": shard.language,
                    "order": shard.order,
                    "reference": shard.reference_dict(),
                }
                for shard in self.shards
            ],
        }
        return fingerprint(semantic)

    @property
    def evaluation_id(self) -> str:
        return str(self.manifest_raw.get("evaluation_id", self.config.evaluation_id))

    def _source_sha(self, shard: ShardSpec) -> str:
        key = shard.shard_id
        if key not in self._source_sha_cache:
            try:
                data = shard.path.read_bytes()
                self._source_sha_cache[key] = sha256_bytes(data)
            except OSError:
                self._source_sha_cache[key] = "missing:" + sha256_bytes(shard.relative_path.encode("utf-8"))
        return self._source_sha_cache[key]

    def expected_fingerprints(self, shard: ShardSpec) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
        source_sha = self._source_sha(shard)
        frame_components = {
            "algorithm_version": STAGE_ALGORITHM_VERSIONS["frame"],
            "implementation_sha256": _IMPLEMENTATION_SHAS["audio"],
            "source_file_sha256": source_sha,
            "source_path": shard.relative_path,
            "declared_num_samples": shard.declared_sample_count,
            "declared_sample_rate": shard.declared_sample_rate,
            "frame": self.config.frame.to_dict(),
        }
        frame_fp = fingerprint({"stage": "frame", "inputs": frame_components})
        metrics_components = {
            "algorithm_version": STAGE_ALGORITHM_VERSIONS["metrics"],
            "implementation_sha256": _IMPLEMENTATION_SHAS["audio"],
            "upstream_frame_fingerprint": frame_fp,
            "metrics": self.config.metrics.to_dict(),
            "score_decimals": self.config.fusion.decimals,
        }
        metrics_fp = fingerprint({"stage": "metrics", "inputs": metrics_components})
        fusion_components = {
            "algorithm_version": STAGE_ALGORITHM_VERSIONS["fusion"],
            "implementation_sha256": _IMPLEMENTATION_SHAS["fusion"],
            "upstream_metrics_fingerprint": metrics_fp,
            "weights": {name: self.config.weights[name] for name in ("snr", "spectral", "clipping")},
            "reference": shard.reference_dict(),
            "fusion": self.config.fusion.to_dict(),
            "schema_version": self.config.schema_version,
        }
        fusion_fp = fingerprint({"stage": "fusion", "inputs": fusion_components})
        diagnostics_components = {
            "algorithm_version": STAGE_ALGORITHM_VERSIONS["diagnostics"],
            "implementation_sha256": _IMPLEMENTATION_SHAS["pipeline"],
            "upstream_fusion_fingerprint": fusion_fp,
            "language": shard.language,
            "order": shard.order,
            "evaluation_id": self.evaluation_id,
            "report_schema_version": REPORT_SCHEMA_VERSION,
        }
        diagnostics_fp = fingerprint({"stage": "diagnostics", "inputs": diagnostics_components})
        return (
            {
                "frame": frame_components,
                "metrics": metrics_components,
                "fusion": fusion_components,
                "diagnostics": diagnostics_components,
            },
            {
                "frame": frame_fp,
                "metrics": metrics_fp,
                "fusion": fusion_fp,
                "diagnostics": diagnostics_fp,
            },
        )

    def _invalidation_for_shard(self, shard: ShardSpec) -> dict[str, Any] | None:
        components, fingerprints = self.expected_fingerprints(shard)
        scan = self.checkpoints.scan_shard(shard.shard_id, repair=False)
        latest = scan.latest_by_stage
        earliest: str | None = None
        reasons: list[str] = []
        for stage in STAGES:
            record = latest.get(stage)
            if record is None:
                earliest = stage
                reasons = ["missing_stage_checkpoint"]
                break
            if record["input_fingerprint"] != fingerprints[stage]:
                earliest = stage
                old_components = record.get("version_components", {})
                if not old_components:
                    reasons = ["version_fingerprint_changed"]
                else:
                    for key in sorted(set(old_components) | set(components[stage])):
                        if old_components.get(key) != components[stage].get(key):
                            reasons.append(self._reason_for_component(stage, key))
                reasons = sorted(set(reasons)) or ["version_fingerprint_changed"]
                break
        if earliest is None:
            return None
        affected = list(STAGES[STAGES.index(earliest) :])
        return {
            "shard_id": shard.shard_id,
            "earliest_stage": earliest,
            "affected_stages": affected,
            "reasons": reasons,
            "previous_input_fingerprints": {
                stage: latest[stage]["input_fingerprint"] for stage in latest
            },
            "current_input_fingerprints": fingerprints,
            "rebuilt_version_fingerprint": fingerprints["diagnostics"],
        }

    @staticmethod
    def _reason_for_component(stage: str, key: str) -> str:
        if stage == "frame":
            if key == "source_file_sha256":
                return "shard_content_changed"
            if key.startswith("frame"):
                return "frame_parameters_changed"
            if key.startswith("declared"):
                return "manifest_metadata_changed"
        if stage == "metrics":
            if key == "metrics.snr_threshold_db":
                return "snr_threshold_changed"
            if key.startswith("metrics"):
                return "metric_parameters_changed"
            if key == "upstream_frame_fingerprint":
                return "upstream_frame_changed"
        if stage == "fusion":
            if key == "weights":
                return "metric_weights_changed"
            if key == "reference":
                return "reference_interval_changed"
            if key == "schema_version":
                return "structure_version_changed"
            if key == "fusion":
                return "fusion_parameters_changed"
            if key == "upstream_metrics_fingerprint":
                return "upstream_metrics_changed"
        if stage == "diagnostics":
            if key == "language":
                return "language_changed"
            if key == "order":
                return "order_changed"
            if key == "evaluation_id":
                return "evaluation_id_changed"
            if key == "upstream_fusion_fingerprint":
                return "upstream_fusion_changed"
        return f"{key}_changed"

    def current_invalidations(self) -> dict[str, Any]:
        events = [event for shard in self.shards if (event := self._invalidation_for_shard(shard)) is not None]
        return {
            "config_fingerprint": self.config.version_fingerprint,
            "manifest_fingerprint": self.manifest_fingerprint,
            "affected_shards": [event["shard_id"] for event in events],
            "events": events,
        }

    def _persist_invalidations(self, payload: Mapping[str, Any]) -> None:
        if payload.get("events"):
            atomic_write_json(self.state_dir / "last_invalidation.json", payload, pretty=True)

    def _compute_frame_artifact(self, shard: ShardSpec) -> dict[str, Any]:
        actual_count: int | None = None
        actual_rate: int | None = None
        content_sha: str | None = None
        try:
            audio = read_pcm_wav(shard.path, shard_id=shard.shard_id)
            actual_count = audio.sample_count
            actual_rate = audio.sample_rate
            content_sha = audio.content_sha256
            if actual_count == 0:
                raise AudioQError(
                    "E_EMPTY_AUDIO",
                    "WAV contains no samples",
                    shard_id=shard.shard_id,
                    stage="frame",
                    observed={"actual_num_samples": 0, "declared_num_samples": shard.declared_sample_count},
                )
            if actual_rate != shard.declared_sample_rate:
                raise AudioQError(
                    "E_SAMPLE_RATE",
                    "WAV sample rate differs from the manifest",
                    shard_id=shard.shard_id,
                    stage="frame",
                    observed={
                        "actual_sample_rate": actual_rate,
                        "declared_sample_rate": shard.declared_sample_rate,
                    },
                )
            if actual_count != shard.declared_sample_count:
                raise AudioQError(
                    "E_LENGTH_MISMATCH",
                    "WAV sample count differs from the manifest",
                    shard_id=shard.shard_id,
                    stage="frame",
                    observed={
                        "actual_num_samples": actual_count,
                        "declared_num_samples": shard.declared_sample_count,
                        "difference": actual_count - shard.declared_sample_count,
                    },
                )
            artifact = frame_audio(audio, self.config.frame, shard_id=shard.shard_id)
            artifact["declared_num_samples"] = shard.declared_sample_count
            artifact["declared_sample_rate"] = shard.declared_sample_rate
            return artifact
        except AudioQError as exc:
            return _error_artifact(
                "frame",
                shard.shard_id,
                exc,
                sample_count=actual_count,
                sample_rate=actual_rate,
                declared_num_samples=shard.declared_sample_count,
                declared_sample_rate=shard.declared_sample_rate,
                content_sha256=content_sha,
            )

    def _compute_metrics_artifact(self, shard: ShardSpec, frame: Mapping[str, Any]) -> dict[str, Any]:
        if frame.get("status") != "ok":
            return {
                "status": "error",
                "shard_id": shard.shard_id,
                "stage": "metrics",
                "scores": {"snr": None, "spectral": None, "clipping": None},
                "upstream_error_codes": [item["code"] for item in frame.get("errors", [])],
                "errors": [],
            }
        try:
            audio = read_pcm_wav(shard.path, shard_id=shard.shard_id)
            return evaluate_metrics(
                audio,
                frame,
                self.config.metrics,
                decimals=self.config.fusion.decimals,
                shard_id=shard.shard_id,
            )
        except AudioQError as exc:
            return _error_artifact(
                "metrics",
                shard.shard_id,
                exc,
                scores={"snr": None, "spectral": None, "clipping": None},
            )

    def _compute_fusion_artifact(
        self,
        shard: ShardSpec,
        metrics: Mapping[str, Any],
    ) -> dict[str, Any]:
        try:
            result = fuse_metrics(metrics, self.config, shard.reference, shard_id=shard.shard_id)
            if metrics.get("status") != "ok":
                result["status"] = "degraded"
                result["upstream_error_codes"] = [
                    item.get("code") for item in metrics.get("errors", []) if isinstance(item, Mapping)
                ]
            return result
        except AudioQError as exc:
            if exc.stage is None:
                exc.stage = "fusion"
            return _error_artifact(
                "fusion",
                shard.shard_id,
                exc,
                fused_score=None,
                grade="unknown",
                reference={**shard.reference_dict(), "out_of_range": None, "deviation": None},
            )

    @staticmethod
    def _unique_errors(artifacts: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        seen: set[str] = set()
        for artifact in artifacts:
            for error in artifact.get("errors", []):
                if not isinstance(error, Mapping):
                    continue
                key = json.dumps(dict(error), sort_keys=True, ensure_ascii=False)
                if key not in seen:
                    seen.add(key)
                    result.append(dict(error))
        return result

    def _compute_diagnostics_artifact(
        self,
        shard: ShardSpec,
        frame: Mapping[str, Any],
        metrics: Mapping[str, Any],
        fusion: Mapping[str, Any],
        fingerprints: Mapping[str, str],
    ) -> dict[str, Any]:
        errors = self._unique_errors([frame, metrics, fusion])
        fused = fusion.get("fused_score")
        if fused is None:
            status = "error"
        elif errors:
            status = "degraded"
        else:
            status = "ok"
        audio = {
            "path": shard.relative_path,
            "declared_num_samples": shard.declared_sample_count,
            "declared_sample_rate": shard.declared_sample_rate,
            "actual_num_samples": frame.get("sample_count"),
            "actual_sample_rate": frame.get("sample_rate"),
            "content_sha256": frame.get("content_sha256"),
            "frame_count": frame.get("frame_count"),
            "duration_seconds": frame.get("duration_seconds"),
        }
        internal_model = EvaluationModel(
            shard_id=shard.shard_id,
            sample_count=int(frame.get("sample_count") or 0),
            sample_rate=int(frame.get("sample_rate") or shard.declared_sample_rate),
            metric_scores=metrics.get("scores", {}),
            fused_score=fusion.get("fused_score"),
            schema_version=self.config.schema_version,
            version_fingerprint=fingerprints["diagnostics"],
            errors=errors,
            extra={
                "stage_fingerprints": dict(fingerprints),
                "audio_path": shard.relative_path,
            },
        )
        return {
            "status": status,
            "shard_id": shard.shard_id,
            "order": shard.order,
            "language": shard.language,
            "audio": audio,
            "metrics": metrics,
            "fusion": fusion,
            "errors": errors,
            "internal_model": internal_model.to_dict(),
            "stage_fingerprints": dict(fingerprints),
            "version_fingerprint": fingerprints["diagnostics"],
            "report_schema_version": REPORT_SCHEMA_VERSION,
        }

    def _ensure_stage(
        self,
        shard: ShardSpec,
        stage: str,
        input_fingerprint: str,
        artifact: Mapping[str, Any],
        components: Mapping[str, Any],
        prerequisites: Mapping[str, Mapping[str, Any]],
    ) -> dict[str, Any]:
        prereq_digests = {name: record["artifact_digest"] for name, record in prerequisites.items()}
        for attempt in range(3):
            latest = self.checkpoints.latest_by_stage(shard.shard_id, repair=True)
            record = latest.get(stage)
            if record is not None and record["input_fingerprint"] == input_fingerprint:
                if record["artifact_digest"] == sha256_json(dict(artifact)):
                    return record
            try:
                return self.checkpoints.publish_checkpoint(
                    shard_id=shard.shard_id,
                    stage=stage,
                    input_fingerprint=input_fingerprint,
                    config_fingerprint=self.config.version_fingerprint,
                    artifact=artifact,
                    version_components=components,
                    prerequisites=prereq_digests,
                    allow_idempotent=True,
                )
            except AudioQError as exc:
                if exc.code != "E_STALE_CHECKPOINT" or attempt == 2:
                    raise
                refreshed = self.checkpoints.latest_by_stage(shard.shard_id, repair=True)
                refreshed_record = refreshed.get(stage)
                if (
                    refreshed_record is not None
                    and refreshed_record["input_fingerprint"] == input_fingerprint
                ):
                    return refreshed_record
        raise AudioQError(
            "E_STALE_CHECKPOINT",
            "checkpoint publication did not converge",
            shard_id=shard.shard_id,
            stage=stage,
        )

    def evaluate_shard(self, shard: ShardSpec) -> dict[str, Any]:
        components, fingerprints = self.expected_fingerprints(shard)
        frame = self._compute_frame_artifact(shard)
        frame_record = self._ensure_stage(
            shard,
            "frame",
            fingerprints["frame"],
            frame,
            components["frame"],
            {},
        )
        metrics = self._compute_metrics_artifact(shard, frame)
        metrics_record = self._ensure_stage(
            shard,
            "metrics",
            fingerprints["metrics"],
            metrics,
            components["metrics"],
            {"frame": frame_record},
        )
        fusion = self._compute_fusion_artifact(shard, metrics)
        fusion_record = self._ensure_stage(
            shard,
            "fusion",
            fingerprints["fusion"],
            fusion,
            components["fusion"],
            {"frame": frame_record, "metrics": metrics_record},
        )
        diagnostics = self._compute_diagnostics_artifact(shard, frame, metrics, fusion, fingerprints)
        diagnostics_record = self._ensure_stage(
            shard,
            "diagnostics",
            fingerprints["diagnostics"],
            diagnostics,
            components["diagnostics"],
            {"frame": frame_record, "metrics": metrics_record, "fusion": fusion_record},
        )
        return diagnostics_record["artifact"]

    def _load_current_diagnostics(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for shard in self.shards:
            _, fingerprints = self.expected_fingerprints(shard)
            latest = self.checkpoints.latest_by_stage(shard.shard_id, repair=False)
            record = latest.get("diagnostics")
            if record is None:
                raise AudioQError(
                    "E_INVALID_STAGE",
                    "diagnostics checkpoint is missing",
                    shard_id=shard.shard_id,
                    stage="diagnostics",
                )
            if record["input_fingerprint"] != fingerprints["diagnostics"]:
                raise AudioQError(
                    "E_STALE_CHECKPOINT",
                    "refusing to export a stale diagnostics artifact",
                    shard_id=shard.shard_id,
                    stage="diagnostics",
                    observed={
                        "stored_fingerprint": record["input_fingerprint"],
                        "current_fingerprint": fingerprints["diagnostics"],
                    },
                )
            result.append(dict(record["artifact"]))
        return result

    def build_report(self) -> dict[str, Any]:
        shards = self._load_current_diagnostics()
        statuses = [shard["status"] for shard in shards]
        scores = [float(shard["fusion"]["fused_score"]) for shard in shards if shard["fusion"].get("fused_score") is not None]
        errors = [error for shard in shards for error in shard.get("errors", [])]
        error_codes = sorted({str(error.get("code")) for error in errors})
        grade_counts: dict[str, int] = {}
        for shard in shards:
            grade = str(shard["fusion"].get("grade", "unknown"))
            grade_counts[grade] = grade_counts.get(grade, 0) + 1
        out_of_range = [
            shard["shard_id"]
            for shard in shards
            if shard["fusion"].get("reference", {}).get("out_of_range") is True
        ]
        summary = {
            "shard_count": len(shards),
            "status_counts": {name: statuses.count(name) for name in sorted(set(statuses))},
            "grade_counts": {name: grade_counts[name] for name in sorted(grade_counts)},
            "error_shard_count": sum(1 for shard in shards if shard.get("errors")),
            "error_codes": error_codes,
            "out_of_range_shards": out_of_range,
            "mean_fused_score": (
                round(math.fsum(scores) / len(scores), self.config.fusion.decimals) if scores else None
            ),
        }
        payload: dict[str, Any] = {
            "report_schema_version": REPORT_SCHEMA_VERSION,
            "evaluation_id": self.evaluation_id,
            "config": {
                "path": self._relative(self.config_path),
                "schema_version": self.config.schema_version,
                "version_fingerprint": self.config.version_fingerprint,
            },
            "manifest": {
                "path": self._relative(self.manifest_path),
                "fingerprint": self.manifest_fingerprint,
            },
            "shards": shards,
            "summary": summary,
        }
        payload["report_fingerprint"] = fingerprint(payload)
        return payload

    def write_report(self, path: Path | str | None = None) -> tuple[dict[str, Any], bytes, Path]:
        report = self.build_report()
        data = canonical_json_bytes(report, pretty=True)
        if path is None:
            target = self.state_dir / "report.json"
        else:
            candidate = Path(path)
            target = candidate if candidate.is_absolute() else self.root / candidate
        atomic_write_bytes(target, data)
        return report, data, target

    def status(self) -> dict[str, Any]:
        invalidation = self.current_invalidations()
        shard_statuses: list[dict[str, Any]] = []
        for shard in self.shards:
            components, fingerprints = self.expected_fingerprints(shard)
            scan = self.checkpoints.scan_shard(shard.shard_id, repair=False)
            latest = scan.latest_by_stage
            stages: dict[str, Any] = {}
            earliest: str | None = None
            for stage in STAGES:
                record = latest.get(stage)
                if record is None:
                    state = "missing"
                elif record["input_fingerprint"] == fingerprints[stage]:
                    state = "valid"
                else:
                    state = "stale"
                if state != "valid" and earliest is None:
                    earliest = stage
                stages[stage] = {
                    "state": state,
                    "seq": record["seq"] if record else None,
                    "input_fingerprint": record["input_fingerprint"] if record else None,
                    "expected_input_fingerprint": fingerprints[stage],
                    "artifact_digest": record["artifact_digest"] if record else None,
                }
            event = next((item for item in invalidation["events"] if item["shard_id"] == shard.shard_id), None)
            diagnostic_record = latest.get("diagnostics")
            diagnostic_summary = None
            if diagnostic_record is not None:
                diagnostic_artifact = diagnostic_record.get("artifact", {})
                fusion_artifact = diagnostic_artifact.get("fusion", {})
                diagnostic_summary = {
                    "status": diagnostic_artifact.get("status"),
                    "fused_score": fusion_artifact.get("fused_score"),
                    "grade": fusion_artifact.get("grade"),
                    "errors": diagnostic_artifact.get("errors", []),
                    "version_fingerprint": diagnostic_artifact.get("version_fingerprint"),
                    "is_current": diagnostic_record["input_fingerprint"] == fingerprints["diagnostics"],
                }
            shard_statuses.append(
                {
                    "shard_id": shard.shard_id,
                    "order": shard.order,
                    "language": shard.language,
                    "path": shard.relative_path,
                    "expected_stage_fingerprints": fingerprints,
                    "expected_components": components,
                    "stages": stages,
                    "consistent_last_seq": scan.last_seq,
                    "earliest_invalid_stage": earliest,
                    "affected_stages": event["affected_stages"] if event else [],
                    "invalidation_reasons": event["reasons"] if event else [],
                    "invalid_records": scan.invalid,
                    "last_diagnostics": diagnostic_summary,
                }
            )
        last_persisted = None
        invalidation_path = self.state_dir / "last_invalidation.json"
        if invalidation_path.exists():
            try:
                last_persisted = read_json(invalidation_path)
            except AudioQError:
                last_persisted = {"unreadable": True}
        return {
            "ok": True,
            "evaluation_id": self.evaluation_id,
            "config": {
                "path": self._relative(self.config_path),
                "schema_version": self.config.schema_version,
                "version_fingerprint": self.config.version_fingerprint,
            },
            "manifest": {
                "path": self._relative(self.manifest_path),
                "fingerprint": self.manifest_fingerprint,
            },
            "shards": shard_statuses,
            "invalidations": invalidation,
            "last_persisted_invalidation": last_persisted,
            "checkpoints": self.checkpoints.summary([shard.shard_id for shard in self.shards]),
            "stop_requested": self.checkpoints.stop_requested(),
            "stop_request": self.checkpoints.stop_payload(),
        }

    def run(
        self,
        *,
        stop_after_shards: int | None = None,
        clear_stop: bool = False,
        write_report: bool = True,
    ) -> dict[str, Any]:
        if stop_after_shards is not None and (
            isinstance(stop_after_shards, bool) or not isinstance(stop_after_shards, int) or stop_after_shards < 0
        ):
            raise AudioQError("E_CONFIG", "stop_after_shards must be a non-negative integer")
        if clear_stop:
            self.checkpoints.clear_stop()
        self._source_sha_cache.clear()
        before = self.current_invalidations()
        if before.get("events"):
            try:
                (self.state_dir / "report.json").unlink()
            except FileNotFoundError:
                pass
        completed: list[str] = []
        stopped = False
        stop_reason: str | None = None
        for shard in self.shards:
            if self.checkpoints.stop_requested():
                stopped = True
                stop_reason = str((self.checkpoints.stop_payload() or {}).get("reason", "stop_request"))
                break
            if stop_after_shards is not None and len(completed) >= stop_after_shards:
                stopped = True
                stop_reason = "stop_after_shards"
                break
            self.evaluate_shard(shard)
            completed.append(shard.shard_id)
        self._persist_invalidations(before)
        report: dict[str, Any] | None = None
        report_data: bytes | None = None
        report_path: Path | None = None
        if not stopped and len(completed) == len(self.shards):
            if write_report:
                report, report_data, report_path = self.write_report()
            else:
                report = self.build_report()
                report_data = canonical_json_bytes(report, pretty=True)
        return {
            "ok": True,
            "status": "stopped" if stopped else "complete",
            "stopped": stopped,
            "stop_reason": stop_reason,
            "completed_shards": completed,
            "completed_count": len(completed),
            "total_shards": len(self.shards),
            "invalidations": before,
            "report": report,
            "report_path": self._relative(report_path) if report_path else None,
            "report_sha256": sha256_bytes(report_data) if report_data is not None else None,
            "status_snapshot": self.status(),
        }

    def resume(self, *, stop_after_shards: int | None = None, write_report: bool = True) -> dict[str, Any]:
        return self.run(stop_after_shards=stop_after_shards, clear_stop=True, write_report=write_report)

    def request_stop(self, reason: str = "user_request") -> dict[str, Any]:
        return self.checkpoints.request_stop(reason=reason)
