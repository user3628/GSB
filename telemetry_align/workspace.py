"""Workspace orchestration: incremental planning, stages, stop/resume and queries."""

from __future__ import annotations

import copy
import uuid
from pathlib import Path
from typing import Any

import yaml

from .canonical import (
    FileLock,
    atomic_write_bytes,
    atomic_write_json,
    atomic_write_text,
    digest_obj,
    read_json,
    resolve_inside,
    safe_relpath,
    sha256_bytes,
    sha256_file,
)
from .checkpoint import CheckpointStore
from .config import ConfigBundle, load_config, parse_config, semantic_equal
from .errors import TelemetryError
from .fingerprints import (
    STAGE_DEPENDENCIES,
    STAGE_RANK,
    STAGES,
    fusion_stage_fingerprint,
    grid_fingerprint,
    normalize_source_fingerprint,
    normalize_stage_fingerprint,
    overall_fingerprint,
    resample_source_fingerprint,
    resample_stage_fingerprint,
    summary_stage_fingerprint,
)
from .fusion import fuse_alignment
from .normalize import build_grid, parse_and_normalize_source
from .resample import resample_source
from .samples import generate_demo, mutate_source
from .summary import build_summary


def _run_id() -> str:
    return uuid.uuid4().hex


def _artifact_ref(path: str, artifact_digest: str, fingerprint: str) -> dict[str, Any]:
    return {
        "artifact_path": path,
        "artifact_digest": artifact_digest,
        "fingerprint": fingerprint,
    }


class AlignmentWorkspace:
    def __init__(self, root: Path | str) -> None:
        self.root = Path(root).resolve()
        self.manifest_path = self.root / "workspace.json"
        self.state_dir = self.root / "state"
        self.store = CheckpointStore(self.root)
        self.run_lock_path = self.state_dir / "run.lock"
        self.run_state_path = self.state_dir / "run.json"
        self.stop_path = self.state_dir / "stop.json"
        self.invalidation_path = self.state_dir / "invalidations.json"

    # ------------------------------------------------------------ workspace IO
    def init_demo(self, *, reset: bool = True) -> dict[str, Any]:
        manifest = generate_demo(self.root, reset=reset)
        return {"manifest": manifest, "status": self.status()}

    def load_manifest(self) -> dict[str, Any]:
        if not self.manifest_path.exists():
            raise TelemetryError(
                "E_NOT_FOUND",
                "工作区未初始化 / workspace is not initialised",
                {"root": str(self.root)},
            )
        try:
            manifest = read_json(self.manifest_path)
        except Exception as exc:
            raise TelemetryError(
                "E_CONFIG_INVALID",
                "工作区清单损坏 / workspace manifest is corrupt",
                {"path": str(self.manifest_path), "reason": str(exc)},
            ) from exc
        if not isinstance(manifest.get("sources"), dict) or not isinstance(
            manifest.get("configs"), dict
        ):
            raise TelemetryError(
                "E_CONFIG_INVALID",
                "工作区清单缺少 sources/configs / manifest is incomplete",
                {"manifest": manifest},
            )
        return manifest

    @staticmethod
    def _config_name(config_name: str | None, manifest: dict[str, Any]) -> str:
        name = config_name or manifest.get("active_config")
        if not name:
            raise TelemetryError(
                "E_CONFIG_INVALID",
                "未指定活动配置 / active config is not specified",
                {"available": sorted(manifest.get("configs", {}))},
            )
        if name not in manifest.get("configs", {}):
            raise TelemetryError(
                "E_NOT_FOUND",
                "配置不存在 / config not found",
                {"config": name, "available": sorted(manifest.get("configs", {}))},
            )
        return str(name)

    def load_config_bundle(
        self,
        config_name: str | None = None,
        *,
        manifest: dict[str, Any] | None = None,
    ) -> tuple[str, ConfigBundle]:
        manifest = manifest or self.load_manifest()
        name = self._config_name(config_name, manifest)
        path = resolve_inside(self.root, manifest["configs"][name])
        config = load_config(path, policy_override=manifest.get("v1_policy"))
        self._validate_source_coverage(config, manifest)
        return name, config

    @staticmethod
    def _validate_source_coverage(config: ConfigBundle, manifest: dict[str, Any]) -> None:
        configured = set(config.source_ids)
        available = set(manifest["sources"])
        if configured != available:
            raise TelemetryError(
                "E_CONFIG_INVALID",
                "配置源集合与导入分片不一致 / config sources do not match imported shards",
                {
                    "config_sources": sorted(configured),
                    "imported_sources": sorted(available),
                    "missing_shards": sorted(configured - available),
                    "unconfigured_shards": sorted(available - configured),
                },
            )

    def _source_paths(self, manifest: dict[str, Any]) -> dict[str, Path]:
        return {
            source_id: resolve_inside(self.root, relative_path)
            for source_id, relative_path in sorted(manifest["sources"].items())
        }

    def _read_sources(self, manifest: dict[str, Any]) -> tuple[dict[str, bytes], dict[str, str]]:
        bytes_by_source: dict[str, bytes] = {}
        digests: dict[str, str] = {}
        for source_id, path in self._source_paths(manifest).items():
            try:
                data = path.read_bytes()
            except FileNotFoundError as exc:
                raise TelemetryError(
                    "E_NOT_FOUND",
                    "源分片文件不存在 / source shard is missing",
                    {"source_id": source_id, "path": str(path)},
                ) from exc
            bytes_by_source[source_id] = data
            digests[source_id] = sha256_bytes(data)
        return bytes_by_source, digests

    # --------------------------------------------------------------- planning
    def plan(self, config: ConfigBundle, raw_digests: dict[str, str]) -> dict[str, Any]:
        grid = config.effective["grid"]
        count = ((grid["end_ms"] - grid["start_ms"]) // grid["step_ms"]) + 1
        grid_definition = {
            "step_ms": grid["step_ms"],
            "start_ms": grid["start_ms"],
            "end_ms": grid["end_ms"],
            "count": count,
        }
        grid_fp = grid_fingerprint(grid_definition)
        normalize_sources = {
            source_id: normalize_source_fingerprint(
                raw_digests[source_id], config.normalize_source_slice(source_id)
            )
            for source_id in config.source_ids
        }
        normalize_fp = normalize_stage_fingerprint(grid_definition, normalize_sources)
        resample_sources = {
            source_id: resample_source_fingerprint(
                normalize_sources[source_id],
                grid_fp,
                config.resample_source_slice(source_id),
            )
            for source_id in config.source_ids
        }
        resample_fp = resample_stage_fingerprint(grid_fp, resample_sources)
        fusion_fp = fusion_stage_fingerprint(resample_fp, config.fusion_slice())
        summary_fp = summary_stage_fingerprint(fusion_fp, config.summary_slice())
        stage_fingerprints = {
            "normalize": normalize_fp,
            "resample": resample_fp,
            "fusion": fusion_fp,
            "summary": summary_fp,
        }
        return {
            "grid_definition": grid_definition,
            "grid_fingerprint": grid_fp,
            "normalize_source_fingerprints": normalize_sources,
            "resample_source_fingerprints": resample_sources,
            "stage_fingerprints": stage_fingerprints,
            "overall_fingerprint": overall_fingerprint(
                stage_fingerprints, config.semantic_fingerprint
            ),
            "raw_digests": {key: raw_digests[key] for key in sorted(raw_digests)},
        }

    def _context(self, config: ConfigBundle, plan: dict[str, Any]) -> dict[str, Any]:
        return {
            "config_version": config.schema_version,
            "effective_config_version": config.effective_schema_version,
            "config_fingerprint": config.config_fingerprint,
            "semantic_fingerprint": config.semantic_fingerprint,
            "semantic_config": config.semantic,
            "source_digests": plan["raw_digests"],
            "stage_fingerprints": plan["stage_fingerprints"],
            "overall_fingerprint": plan["overall_fingerprint"],
        }

    def _invalidation_report(
        self,
        config: ConfigBundle,
        plan: dict[str, Any],
        head: dict[str, Any] | None,
    ) -> dict[str, Any]:
        reasons: list[dict[str, Any]] = []
        prev_context = head.get("context") if head else None
        if head is None:
            reasons.append(
                {
                    "stage": "normalize",
                    "code": "initial_run",
                    "message": "尚无检查点，需从归一化阶段构建 / no checkpoint exists",
                }
            )
        elif not isinstance(prev_context, dict):
            reasons.append(
                {
                    "stage": "normalize",
                    "code": "legacy_context_missing",
                    "message": "旧检查点缺少上下文，保守重建 / checkpoint context is missing",
                }
            )
        else:
            previous_sources = prev_context.get("source_digests", {})
            for source_id in config.source_ids:
                if previous_sources.get(source_id) != plan["raw_digests"][source_id]:
                    reasons.append(
                        {
                            "stage": "normalize",
                            "code": "source_content_changed",
                            "source_id": source_id,
                            "old_digest": previous_sources.get(source_id),
                            "new_digest": plan["raw_digests"][source_id],
                        }
                    )
            previous_semantic = prev_context.get("semantic_config")
            if not isinstance(previous_semantic, dict):
                reasons.append(
                    {
                        "stage": "normalize",
                        "code": "semantic_config_missing",
                        "message": "旧检查点缺少语义配置 / semantic config is unavailable",
                    }
                )
            else:
                current = config.semantic
                if previous_semantic.get("grid") != current["grid"]:
                    reasons.append(
                        {
                            "stage": "normalize",
                            "code": "grid_definition_changed",
                            "old": previous_semantic.get("grid"),
                            "new": current["grid"],
                        }
                    )
                old_sources = previous_semantic.get("sources", {})
                for source_id in config.source_ids:
                    old_source = old_sources.get(source_id, {})
                    new_source = current["sources"][source_id]
                    changed = [
                        field
                        for field in ("channel_mapping", "valid_ranges", "quality_accept")
                        if old_source.get(field) != new_source.get(field)
                    ]
                    if changed:
                        reasons.append(
                            {
                                "stage": "normalize",
                                "code": "source_normalization_config_changed",
                                "source_id": source_id,
                                "fields": changed,
                            }
                        )
                    if old_source.get("priority") != new_source.get("priority"):
                        reasons.append(
                            {
                                "stage": "fusion",
                                "code": "source_priority_changed",
                                "source_id": source_id,
                                "old": old_source.get("priority"),
                                "new": new_source.get("priority"),
                            }
                        )
                if previous_semantic.get("tolerance_ms") != current["tolerance_ms"]:
                    reasons.append(
                        {
                            "stage": "resample",
                            "code": "tolerance_changed",
                            "old": previous_semantic.get("tolerance_ms"),
                            "new": current["tolerance_ms"],
                        }
                    )
                if previous_semantic.get("resample") != current["resample"]:
                    reasons.append(
                        {
                            "stage": "resample",
                            "code": "resample_config_changed",
                            "old": previous_semantic.get("resample"),
                            "new": current["resample"],
                        }
                    )
                if previous_semantic.get("interpolation") != current["interpolation"]:
                    reasons.append(
                        {
                            "stage": "resample",
                            "code": "interpolation_changed",
                            "old": previous_semantic.get("interpolation"),
                            "new": current["interpolation"],
                        }
                    )
                if previous_semantic.get("fusion") != current["fusion"]:
                    reasons.append(
                        {
                            "stage": "fusion",
                            "code": "fusion_config_changed",
                            "old": previous_semantic.get("fusion"),
                            "new": current["fusion"],
                        }
                    )
                if previous_semantic.get("channels") != current["channels"]:
                    reasons.append(
                        {
                            "stage": "fusion",
                            "code": "channel_labels_changed",
                            "old": previous_semantic.get("channels"),
                            "new": current["channels"],
                        }
                    )
                if previous_semantic.get("export") != current["export"]:
                    reasons.append(
                        {
                            "stage": "summary",
                            "code": "export_config_changed",
                            "old": previous_semantic.get("export"),
                            "new": current["export"],
                        }
                    )
                if prev_context.get("config_fingerprint") != config.config_fingerprint and not reasons:
                    reasons.append(
                        {
                            "stage": None,
                            "code": "config_schema_or_provenance_changed",
                            "message": "结构版本或元数据变化，但有效语义相同 / schema or metadata changed with identical semantics",
                        }
                    )
        stage_reasons = [
            reason["stage"] for reason in reasons if reason.get("stage") in STAGE_RANK
        ]
        earliest = (
            min(stage_reasons, key=lambda stage: STAGE_RANK[stage])
            if stage_reasons
            else None
        )
        affected = (
            list(STAGES[STAGE_RANK[earliest] - 1 :]) if earliest is not None else []
        )
        return {
            "schema_version": 1,
            "config_fingerprint": config.config_fingerprint,
            "semantic_fingerprint": config.semantic_fingerprint,
            "source_digests": plan["raw_digests"],
            "stage_fingerprints": plan["stage_fingerprints"],
            "earliest_stage": earliest,
            "affected_stages": affected,
            "reasons": reasons,
        }

    def _stage_fresh(
        self, stage: str, plan: dict[str, Any], catalog: dict[str, Any]
    ) -> bool:
        entry = catalog.get(stage)
        if not isinstance(entry, dict):
            return False
        if entry.get("stage_fingerprint") != plan["stage_fingerprints"][stage]:
            return False
        if stage == "normalize":
            if entry.get("grid", {}).get("fingerprint") != plan["grid_fingerprint"]:
                return False
            for source_id, fp in plan["normalize_source_fingerprints"].items():
                if entry.get("sources", {}).get(source_id, {}).get("fingerprint") != fp:
                    return False
        if stage == "resample":
            for source_id, fp in plan["resample_source_fingerprints"].items():
                if entry.get("sources", {}).get(source_id, {}).get("fingerprint") != fp:
                    return False
        return True

    def _freshness(
        self, plan: dict[str, Any], head: dict[str, Any] | None
    ) -> dict[str, bool]:
        catalog = head.get("catalog", {}) if head else {}
        return {
            stage: self._stage_fresh(stage, plan, catalog) for stage in STAGES
        }

    # ----------------------------------------------------------- artifact I/O
    def _write_artifact(self, relative_path: str, value: Any) -> dict[str, Any]:
        path = self.root / relative_path
        atomic_write_json(path, value)
        return _artifact_ref(relative_path, sha256_file(path), value["fingerprint"])

    def _read_artifact(
        self,
        entry: dict[str, Any],
        *,
        expected_fingerprint: str,
        label: str,
    ) -> dict[str, Any]:
        path_value = entry.get("artifact_path")
        if not path_value:
            raise TelemetryError(
                "E_INVALID_STAGE",
                "产物条目缺少路径 / artifact entry lacks path",
                {"artifact": label},
            )
        path = resolve_inside(self.root, path_value)
        if not path.exists():
            raise TelemetryError(
                "E_INVALID_STAGE",
                "阶段产物不存在 / stage artifact is missing",
                {"artifact": label, "path": path_value},
            )
        expected_digest = entry.get("artifact_digest")
        if expected_digest and sha256_file(path) != expected_digest:
            raise TelemetryError(
                "E_STALE_CHECKPOINT",
                "阶段产物摘要不匹配 / stage artifact digest mismatch",
                {
                    "kind": "tampered",
                    "artifact": label,
                    "path": path_value,
                    "expected_digest": expected_digest,
                    "actual_digest": sha256_file(path),
                },
            )
        data = read_json(path)
        if data.get("fingerprint") != expected_fingerprint:
            raise TelemetryError(
                "E_INVALID_STAGE",
                "旧版本产物不可复用 / stale artifact cannot be reused",
                {
                    "artifact": label,
                    "expected_fingerprint": expected_fingerprint,
                    "found_fingerprint": data.get("fingerprint"),
                },
            )
        return data

    # --------------------------------------------------------------- stage 1
    def _execute_normalize(
        self,
        config: ConfigBundle,
        plan: dict[str, Any],
        source_bytes: dict[str, bytes],
        previous_catalog: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any], list[str], list[str]]:
        grid = build_grid(config.effective, config.effective["tolerance_ms"])
        previous_normalize = previous_catalog.get("normalize", {})
        previous_sources = previous_normalize.get("sources", {})
        previous_grid = previous_normalize.get("grid", {})
        source_artifacts: dict[str, dict[str, Any]] = {}
        source_refs: dict[str, Any] = {}
        cleaning_reports: dict[str, dict[str, Any]] = {}
        reused: list[str] = []
        rebuilt: list[str] = []
        errors: list[TelemetryError] = []

        for source_id in config.source_ids:
            fingerprint = plan["normalize_source_fingerprints"][source_id]
            cached = previous_sources.get(source_id)
            if isinstance(cached, dict) and cached.get("fingerprint") == fingerprint:
                artifact = self._read_artifact(
                    cached,
                    expected_fingerprint=fingerprint,
                    label=f"normalize:{source_id}",
                )
                source_artifacts[source_id] = artifact
                source_refs[source_id] = {
                    "artifact_path": cached["artifact_path"],
                    "artifact_digest": cached["artifact_digest"],
                    "fingerprint": fingerprint,
                }
                cleaning_reports[source_id] = artifact["cleaning_report"]
                reused.append(f"normalize:{source_id}")
                continue
            try:
                normalized, cleaning_report = parse_and_normalize_source(
                    source_id,
                    source_bytes[source_id],
                    config.effective["sources"][source_id],
                )
            except TelemetryError as exc:
                if exc.code in {"E_TIME_INVALID", "E_SOURCE_INVALID"}:
                    errors.append(exc)
                    continue
                raise
            artifact = {
                "schema_version": 1,
                "source_id": source_id,
                "raw_digest": plan["raw_digests"][source_id],
                "fingerprint": fingerprint,
                "records": normalized["records"],
                "cleaning_report": cleaning_report,
            }
            source_artifacts[source_id] = artifact
            cleaning_reports[source_id] = cleaning_report

        if errors:
            code = (
                "E_TIME_INVALID"
                if any(error.code == "E_TIME_INVALID" for error in errors)
                else "E_SOURCE_INVALID"
            )
            raise TelemetryError(
                code,
                "一个或多个源分片校验失败 / one or more shards failed validation",
                {"sources": [error.details for error in errors]},
            )

        for source_id in config.source_ids:
            if source_id in source_refs:
                continue
            fingerprint = plan["normalize_source_fingerprints"][source_id]
            relative_path = f"artifacts/normalize/sources/{source_id}-{fingerprint}.json"
            source_refs[source_id] = self._write_artifact(
                relative_path, source_artifacts[source_id]
            )
            rebuilt.append(f"normalize:{source_id}")

        if previous_grid.get("fingerprint") == plan["grid_fingerprint"]:
            grid_entry = {
                "artifact_path": previous_grid["artifact_path"],
                "artifact_digest": previous_grid["artifact_digest"],
                "fingerprint": plan["grid_fingerprint"],
            }
            reused.append("normalize:grid")
        else:
            relative_path = f"artifacts/normalize/grid-{plan['grid_fingerprint']}.json"
            grid_entry = self._write_artifact(relative_path, grid)
            rebuilt.append("normalize:grid")

        stage_fingerprint = plan["stage_fingerprints"]["normalize"]
        manifest = {
            "schema_version": 1,
            "stage": "normalize",
            "fingerprint": stage_fingerprint,
            "grid_fingerprint": plan["grid_fingerprint"],
            "grid": grid_entry,
            "sources": {
                source_id: source_refs[source_id] for source_id in sorted(source_refs)
            },
            "cleaning_reports": cleaning_reports,
        }
        relative_path = f"artifacts/normalize/manifest-{stage_fingerprint}.json"
        manifest_ref = self._write_artifact(relative_path, manifest)
        catalog_entry = {
            "stage_fingerprint": stage_fingerprint,
            "artifact_path": relative_path,
            "artifact_digest": manifest_ref["artifact_digest"],
            "grid": grid_entry,
            "sources": source_refs,
        }
        bundle = {
            "grid": grid,
            "sources": source_artifacts,
            "cleaning_reports": cleaning_reports,
            "manifest": manifest,
        }
        return catalog_entry, bundle, reused, rebuilt

    # --------------------------------------------------------------- stage 2
    def _read_grid(
        self, normalize_entry: dict[str, Any], expected_fingerprint: str
    ) -> dict[str, Any]:
        grid_entry = normalize_entry.get("grid")
        if not isinstance(grid_entry, dict):
            raise TelemetryError(
                "E_INVALID_STAGE",
                "归一化阶段缺少网格产物 / normalize stage lacks grid artifact",
                {},
            )
        if grid_entry.get("fingerprint") != expected_fingerprint:
            raise TelemetryError(
                "E_INVALID_STAGE",
                "网格产物指纹不匹配 / grid fingerprint mismatch",
                {
                    "expected": expected_fingerprint,
                    "found": grid_entry.get("fingerprint"),
                },
            )
        return self._read_artifact(
            grid_entry,
            expected_fingerprint=expected_fingerprint,
            label="normalize:grid",
        )

    def _execute_resample(
        self,
        config: ConfigBundle,
        plan: dict[str, Any],
        normalize_entry: dict[str, Any],
        previous_catalog: dict[str, Any],
        normalize_bundle: dict[str, Any] | None,
    ) -> tuple[dict[str, Any], dict[str, Any], list[str], list[str]]:
        grid = (
            normalize_bundle["grid"]
            if normalize_bundle is not None
            else self._read_grid(normalize_entry, plan["grid_fingerprint"])
        )
        normalized_sources: dict[str, dict[str, Any]] = {}
        for source_id in config.source_ids:
            expected = plan["normalize_source_fingerprints"][source_id]
            if normalize_bundle is not None:
                artifact = normalize_bundle["sources"][source_id]
            else:
                entry = normalize_entry.get("sources", {}).get(source_id)
                if not isinstance(entry, dict):
                    raise TelemetryError(
                        "E_INVALID_STAGE",
                        "归一化源产物缺失 / normalized source artifact is missing",
                        {"source_id": source_id},
                    )
                artifact = self._read_artifact(
                    entry,
                    expected_fingerprint=expected,
                    label=f"normalize:{source_id}",
                )
            if artifact.get("fingerprint") != expected:
                raise TelemetryError(
                    "E_INVALID_STAGE",
                    "归一化源产物已过期 / normalized source artifact is stale",
                    {"source_id": source_id},
                )
            normalized_sources[source_id] = artifact

        previous_resample = previous_catalog.get("resample", {})
        previous_sources = previous_resample.get("sources", {})
        resample_artifacts: dict[str, dict[str, Any]] = {}
        source_refs: dict[str, Any] = {}
        reused: list[str] = []
        rebuilt: list[str] = []
        for source_id in config.source_ids:
            fingerprint = plan["resample_source_fingerprints"][source_id]
            cached = previous_sources.get(source_id)
            if isinstance(cached, dict) and cached.get("fingerprint") == fingerprint:
                artifact = self._read_artifact(
                    cached,
                    expected_fingerprint=fingerprint,
                    label=f"resample:{source_id}",
                )
                source_refs[source_id] = {
                    "artifact_path": cached["artifact_path"],
                    "artifact_digest": cached["artifact_digest"],
                    "fingerprint": fingerprint,
                }
                resample_artifacts[source_id] = artifact
                reused.append(f"resample:{source_id}")
                continue
            artifact = resample_source(
                normalized_sources[source_id], grid, config.effective, source_id
            )
            artifact["fingerprint"] = fingerprint
            artifact["normalize_fingerprint"] = plan["normalize_source_fingerprints"][source_id]
            relative_path = f"artifacts/resample/{source_id}-{fingerprint}.json"
            source_refs[source_id] = self._write_artifact(relative_path, artifact)
            resample_artifacts[source_id] = artifact
            rebuilt.append(f"resample:{source_id}")

        stage_fingerprint = plan["stage_fingerprints"]["resample"]
        manifest = {
            "schema_version": 1,
            "stage": "resample",
            "fingerprint": stage_fingerprint,
            "grid_fingerprint": plan["grid_fingerprint"],
            "tolerance_ms": config.effective["tolerance_ms"],
            "interpolation": config.effective["interpolation"],
            "resample": config.effective["resample"],
            "sources": {
                source_id: source_refs[source_id] for source_id in sorted(source_refs)
            },
        }
        relative_path = f"artifacts/resample/manifest-{stage_fingerprint}.json"
        manifest_ref = self._write_artifact(relative_path, manifest)
        catalog_entry = {
            "stage_fingerprint": stage_fingerprint,
            "artifact_path": relative_path,
            "artifact_digest": manifest_ref["artifact_digest"],
            "sources": source_refs,
        }
        bundle = {"grid": grid, "sources": resample_artifacts, "manifest": manifest}
        return catalog_entry, bundle, reused, rebuilt

    def _load_resample_bundle(
        self,
        config: ConfigBundle,
        plan: dict[str, Any],
        resample_entry: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        artifacts: dict[str, dict[str, Any]] = {}
        for source_id in config.source_ids:
            fingerprint = plan["resample_source_fingerprints"][source_id]
            entry = resample_entry.get("sources", {}).get(source_id)
            if not isinstance(entry, dict):
                raise TelemetryError(
                    "E_INVALID_STAGE",
                    "重采样源产物缺失 / resampled source artifact is missing",
                    {"source_id": source_id},
                )
            artifacts[source_id] = self._read_artifact(
                entry,
                expected_fingerprint=fingerprint,
                label=f"resample:{source_id}",
            )
        return artifacts

    # --------------------------------------------------------------- stage 3
    def _execute_fusion(
        self,
        config: ConfigBundle,
        plan: dict[str, Any],
        normalize_entry: dict[str, Any],
        resample_entry: dict[str, Any],
        resample_bundle: dict[str, Any] | None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        grid = (
            resample_bundle["grid"]
            if resample_bundle is not None
            else self._read_grid(normalize_entry, plan["grid_fingerprint"])
        )
        resample_artifacts = (
            resample_bundle["sources"]
            if resample_bundle is not None
            else self._load_resample_bundle(config, plan, resample_entry)
        )
        fusion = fuse_alignment(grid, resample_artifacts, config.effective)
        stage_fingerprint = plan["stage_fingerprints"]["fusion"]
        fusion["fingerprint"] = stage_fingerprint
        fusion["source_resample_fingerprints"] = plan["resample_source_fingerprints"]
        relative_path = f"artifacts/fusion/fusion-{stage_fingerprint}.json"
        digest = atomic_write_json(self.root / relative_path, fusion)
        entry = {
            "stage_fingerprint": stage_fingerprint,
            "artifact_path": relative_path,
            "artifact_digest": digest,
        }
        return entry, fusion

    # --------------------------------------------------------------- stage 4
    def _execute_summary(
        self,
        config: ConfigBundle,
        plan: dict[str, Any],
        normalize_entry: dict[str, Any],
        fusion_entry: dict[str, Any],
        fusion_bundle: dict[str, Any] | None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        fusion_path = resolve_inside(self.root, fusion_entry["artifact_path"])
        fusion_digest = sha256_file(fusion_path)
        if fusion_bundle is None:
            fusion = self._read_artifact(
                fusion_entry,
                expected_fingerprint=plan["stage_fingerprints"]["fusion"],
                label="fusion",
            )
        else:
            fusion = fusion_bundle
        grid = self._read_grid(normalize_entry, plan["grid_fingerprint"])
        cleaning_reports: dict[str, dict[str, Any]] = {}
        for source_id in config.source_ids:
            entry = normalize_entry.get("sources", {}).get(source_id)
            if not isinstance(entry, dict):
                raise TelemetryError(
                    "E_INVALID_STAGE",
                    "摘要缺少归一化源产物 / summary lacks normalized source artifact",
                    {"source_id": source_id},
                )
            artifact = self._read_artifact(
                entry,
                expected_fingerprint=plan["normalize_source_fingerprints"][source_id],
                label=f"normalize:{source_id}",
            )
            cleaning_reports[source_id] = artifact["cleaning_report"]
        stage_fingerprint = plan["stage_fingerprints"]["summary"]
        summary = build_summary(
            config=config,
            grid=grid,
            fusion=fusion,
            fusion_artifact_digest=fusion_digest,
            cleaning_reports=cleaning_reports,
            source_fingerprints={
                "normalize": plan["normalize_source_fingerprints"],
                "resample": plan["resample_source_fingerprints"],
            },
            stage_fingerprints=plan["stage_fingerprints"],
        )
        summary["fingerprint"] = stage_fingerprint
        relative_path = f"artifacts/summary/summary-{stage_fingerprint}.json"
        digest = atomic_write_json(self.root / relative_path, summary)
        entry = {
            "stage_fingerprint": stage_fingerprint,
            "artifact_path": relative_path,
            "artifact_digest": digest,
        }
        return entry, summary

    # ---------------------------------------------------------- state helpers
    def _read_run_state(self) -> dict[str, Any] | None:
        try:
            return read_json(self.run_state_path)
        except FileNotFoundError:
            return None
        except Exception:
            return None

    def _write_run_state(self, value: dict[str, Any]) -> None:
        atomic_write_json(self.run_state_path, value)

    def _read_stop_state(self) -> dict[str, Any]:
        try:
            data = read_json(self.stop_path)
            if isinstance(data, dict):
                return data
        except FileNotFoundError:
            pass
        except Exception:
            pass
        return {"requested": False, "after_stage": None, "last_stop": None}

    def request_stop(self, after_stage: str | None = None) -> dict[str, Any]:
        if after_stage is not None and after_stage not in STAGE_RANK:
            raise TelemetryError(
                "E_INVALID_STAGE",
                "停止阶段无效 / invalid stop stage",
                {"after_stage": after_stage, "allowed": list(STAGES)},
            )
        state = self._read_stop_state()
        state["requested"] = True
        state["after_stage"] = after_stage
        atomic_write_json(self.stop_path, state)
        return state

    def _consume_stop(self, *, reason: str, stage: str, run_id: str) -> None:
        state = self._read_stop_state()
        state["requested"] = False
        state["last_stop"] = {"reason": reason, "stage": stage, "run_id": run_id}
        atomic_write_json(self.stop_path, state)

    def _write_invalidation_log(self, report: dict[str, Any], run_id: str) -> None:
        existing: list[dict[str, Any]] = []
        try:
            raw = read_json(self.invalidation_path)
            if isinstance(raw, list):
                existing = raw
        except Exception:
            existing = []
        existing.append({"run_id": run_id, **report})
        atomic_write_json(self.invalidation_path, existing[-50:])

    def _checkpoint_record(
        self,
        *,
        run_id: str,
        stage: str,
        config: ConfigBundle,
        plan: dict[str, Any],
        catalog: dict[str, Any],
        input_digest: str,
        stage_entry: dict[str, Any],
        invalidation: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "seq": self.store.next_seq(),
            "run_id": run_id,
            "stage": stage,
            "config_version": config.schema_version,
            "effective_config_version": config.effective_schema_version,
            "config_fingerprint": config.config_fingerprint,
            "semantic_config_fingerprint": config.semantic_fingerprint,
            "input_digest": input_digest,
            "artifact_digest": stage_entry["artifact_digest"],
            "fingerprint": stage_entry["stage_fingerprint"],
            "catalog": catalog,
            "prerequisites": {
                dependency: plan["stage_fingerprints"][dependency]
                for dependency in STAGE_DEPENDENCIES[stage]
            },
            "source_digests": plan["raw_digests"],
            "semantic_config": config.semantic,
            "stage_fingerprints": plan["stage_fingerprints"],
            "invalidation": invalidation,
            "context": self._context(config, plan),
        }

    def _attach_diagnostic(self, exc: TelemetryError) -> TelemetryError:
        if exc.code not in {"E_TIME_INVALID", "E_GRID_INVALID", "E_SOURCE_INVALID"}:
            return exc
        payload = {
            "schema_version": 1,
            "code": exc.code,
            "message": exc.message,
            "details": exc.details,
        }
        digest = digest_obj(payload)
        relative_path = f"reports/errors/{exc.code}-{digest}.json"
        atomic_write_json(self.root / relative_path, payload)
        details = copy.deepcopy(exc.details)
        details["diagnostics_path"] = relative_path
        return TelemetryError(exc.code, exc.message, details)

    # -------------------------------------------------------------- main align
    def align(
        self,
        *,
        config_name: str | None = None,
        resume: bool = False,
        until_stage: str | None = None,
    ) -> dict[str, Any]:
        if until_stage is not None and until_stage not in STAGE_RANK:
            raise TelemetryError(
                "E_INVALID_STAGE",
                "目标阶段无效 / invalid target stage",
                {"until_stage": until_stage, "allowed": list(STAGES)},
            )
        with FileLock(
            self.run_lock_path, blocking=False, purpose="alignment run"
        ):
            run_state: dict[str, Any] | None = None
            run_id: str | None = None
            try:
                manifest = self.load_manifest()
                name, config = self.load_config_bundle(config_name, manifest=manifest)
                source_bytes, raw_digests = self._read_sources(manifest)
                plan = self.plan(config, raw_digests)
                head = self.store.head()
                previous_catalog = (
                    copy.deepcopy(head.get("catalog", {})) if head else {}
                )
                existing_run = self._read_run_state()

                if resume and existing_run is not None:
                    context = self._context(config, plan)
                    previous_context = existing_run.get("context")
                    if not isinstance(previous_context, dict) or any(
                        previous_context.get(field) != context.get(field)
                        for field in (
                            "source_digests",
                            "semantic_fingerprint",
                            "config_fingerprint",
                        )
                    ):
                        raise TelemetryError(
                            "E_STALE_CHECKPOINT",
                            "恢复输入与停止时不一致 / resume inputs differ from stopped run",
                            {
                                "kind": "input_changed",
                                "run_id": existing_run.get("run_id"),
                            },
                        )
                    run_id = str(existing_run["run_id"])
                    run_state = existing_run
                else:
                    run_id = _run_id()

                freshness = self._freshness(plan, head)
                invalidation = self._invalidation_report(config, plan, head)
                pending_stages = [
                    stage for stage in STAGES if not freshness[stage]
                ]
                if pending_stages:
                    earliest_pending = pending_stages[0]
                    known_stages = {
                        reason.get("stage")
                        for reason in invalidation["reasons"]
                        if reason.get("stage")
                    }
                    if earliest_pending not in known_stages:
                        invalidation["reasons"].append(
                            {
                                "stage": earliest_pending,
                                "code": "artifact_missing_or_stale",
                                "message": "当前产物缺失或过期 / current artifact is missing or stale",
                            }
                        )
                        all_stages = [
                            reason["stage"]
                            for reason in invalidation["reasons"]
                            if reason.get("stage") in STAGE_RANK
                        ]
                        earliest = min(
                            all_stages, key=lambda stage: STAGE_RANK[stage]
                        )
                        invalidation["earliest_stage"] = earliest
                        invalidation["affected_stages"] = list(
                            STAGES[STAGE_RANK[earliest] - 1 :]
                        )

                if run_state is None:
                    run_state = {
                        "schema_version": 1,
                        "run_id": run_id,
                        "status": "running",
                        "config_name": name,
                        "config_version": config.schema_version,
                        "config_fingerprint": config.config_fingerprint,
                        "semantic_fingerprint": config.semantic_fingerprint,
                        "context": self._context(config, plan),
                        "completed_stages": [],
                        "executed_stages": [],
                        "reused": [],
                        "rebuilt": [],
                        "start_seq": (int(head["seq"]) + 1) if head else 1,
                        "last_seq": int(head["seq"]) if head else 0,
                        "invalidations": invalidation,
                    }
                else:
                    run_state["status"] = "running"
                    run_state["invalidations"] = invalidation

                self._write_invalidation_log(invalidation, run_id)
                if not pending_stages:
                    run_state["status"] = "completed"
                    run_state["executed_stages"] = []
                    run_state["last_result"] = {
                        "status": "noop",
                        "message": "所有阶段均为最新 / all stages are current",
                        "fingerprints": plan["stage_fingerprints"],
                    }
                    self._write_run_state(run_state)
                    if previous_catalog.get("summary"):
                        self._sync_outputs(previous_catalog)
                    return {
                        "ok": True,
                        "status": "noop",
                        "run_id": run_id,
                        "executed_stages": [],
                        "reused": run_state.get("reused", []),
                        "rebuilt": run_state.get("rebuilt", []),
                        "checkpoint_sequence": self.store.sequence(),
                        "fingerprints": plan["stage_fingerprints"],
                        "overall_fingerprint": plan["overall_fingerprint"],
                        "invalidations": invalidation,
                    }

                self._write_run_state(run_state)
                start_stage = pending_stages[0]
                current_catalog = previous_catalog
                normalize_entry = current_catalog.get("normalize")
                resample_entry = current_catalog.get("resample")
                fusion_entry = current_catalog.get("fusion")
                normalize_bundle = None
                resample_bundle = None
                fusion_bundle = None
                executed: list[str] = []
                all_reused: list[str] = list(run_state.get("reused", []))
                all_rebuilt: list[str] = list(run_state.get("rebuilt", []))
                stopped = False
                stopped_stage: str | None = None
                stop_reason: str | None = None

                for stage in pending_stages:
                    reused: list[str] = []
                    rebuilt: list[str] = []
                    if stage == "normalize":
                        stage_entry, normalize_bundle, reused, rebuilt = (
                            self._execute_normalize(
                                config, plan, source_bytes, previous_catalog
                            )
                        )
                        normalize_entry = stage_entry
                        current_catalog = {"normalize": stage_entry}
                        input_digest = digest_obj(
                            {
                                "raw_digests": plan["raw_digests"],
                                "config": config.normalize_slice(),
                            }
                        )
                    elif stage == "resample":
                        if normalize_entry is None:
                            raise TelemetryError(
                                "E_INVALID_STAGE",
                                "重采样缺少归一化前置 / resample lacks normalize prerequisite",
                                {},
                            )
                        stage_entry, resample_bundle, reused, rebuilt = (
                            self._execute_resample(
                                config,
                                plan,
                                normalize_entry,
                                previous_catalog,
                                normalize_bundle,
                            )
                        )
                        resample_entry = stage_entry
                        current_catalog = {
                            "normalize": normalize_entry,
                            "resample": stage_entry,
                        }
                        input_digest = digest_obj(
                            {
                                "normalize": plan["stage_fingerprints"]["normalize"],
                                "config": config.resample_slice(),
                            }
                        )
                    elif stage == "fusion":
                        if normalize_entry is None or resample_entry is None:
                            raise TelemetryError(
                                "E_INVALID_STAGE",
                                "融合缺少前置阶段 / fusion lacks prerequisites",
                                {},
                            )
                        stage_entry, fusion_bundle = self._execute_fusion(
                            config,
                            plan,
                            normalize_entry,
                            resample_entry,
                            resample_bundle,
                        )
                        fusion_entry = stage_entry
                        current_catalog = {
                            "normalize": normalize_entry,
                            "resample": resample_entry,
                            "fusion": stage_entry,
                        }
                        input_digest = digest_obj(
                            {
                                "resample": plan["stage_fingerprints"]["resample"],
                                "config": config.fusion_slice(),
                            }
                        )
                    elif stage == "summary":
                        if fusion_entry is None:
                            raise TelemetryError(
                                "E_INVALID_STAGE",
                                "摘要缺少融合前置 / summary lacks fusion prerequisite",
                                {},
                            )
                        stage_entry, _ = self._execute_summary(
                            config,
                            plan,
                            normalize_entry,
                            fusion_entry,
                            fusion_bundle,
                        )
                        current_catalog = {
                            "normalize": normalize_entry,
                            "resample": resample_entry,
                            "fusion": fusion_entry,
                            "summary": stage_entry,
                        }
                        input_digest = digest_obj(
                            {
                                "fusion": plan["stage_fingerprints"]["fusion"],
                                "config": config.summary_slice(),
                            }
                        )
                    else:  # pragma: no cover
                        raise TelemetryError(
                            "E_INVALID_STAGE", "未知阶段", {"stage": stage}
                        )

                    record = self._checkpoint_record(
                        run_id=run_id,
                        stage=stage,
                        config=config,
                        plan=plan,
                        catalog=current_catalog,
                        input_digest=input_digest,
                        stage_entry=stage_entry,
                        invalidation=invalidation,
                    )
                    published = self.store.publish(record)
                    executed.append(stage)
                    all_reused.extend(reused)
                    all_rebuilt.extend(rebuilt)
                    run_state["completed_stages"] = sorted(
                        set(run_state.get("completed_stages", [])) | {stage},
                        key=lambda item: STAGE_RANK[item],
                    )
                    run_state["executed_stages"] = executed
                    run_state["reused"] = all_reused
                    run_state["rebuilt"] = all_rebuilt
                    run_state["last_seq"] = published["seq"]
                    self._write_run_state(run_state)

                    stop_state = self._read_stop_state()
                    stop_requested = bool(stop_state.get("requested"))
                    stop_target = stop_state.get("after_stage")
                    target_reached = stop_requested and (
                        stop_target is None
                        or STAGE_RANK[stage] >= STAGE_RANK[stop_target]
                    )
                    until_reached = until_stage is not None and (
                        STAGE_RANK[stage] >= STAGE_RANK[until_stage]
                    )
                    last_stage_reached = stage == pending_stages[-1]
                    if target_reached or until_reached:
                        stopped = True
                        stopped_stage = stage
                        stop_reason = (
                            "stop_request" if target_reached else "until_stage"
                        )
                        if target_reached:
                            self._consume_stop(
                                reason="stop_request", stage=stage, run_id=run_id
                            )
                        break
                    if last_stage_reached:
                        break

                if stopped:
                    run_state["status"] = (
                        "stopped" if stop_reason == "stop_request" else "paused"
                    )
                    run_state["stop_reason"] = stop_reason
                    run_state["last_result"] = {
                        "status": run_state["status"],
                        "stopped_after_stage": stopped_stage,
                        "executed_stages": executed,
                    }
                else:
                    run_state["status"] = "completed"
                    run_state["last_result"] = {
                        "status": "completed",
                        "executed_stages": executed,
                    }
                if "summary" in current_catalog:
                    self._sync_outputs(current_catalog)
                self._write_run_state(run_state)
                return {
                    "ok": True,
                    "status": run_state["status"],
                    "run_id": run_id,
                    "start_stage": start_stage,
                    "executed_stages": executed,
                    "reused": all_reused,
                    "rebuilt": all_rebuilt,
                    "checkpoint_sequence": self.store.sequence(),
                    "fingerprints": plan["stage_fingerprints"],
                    "overall_fingerprint": plan["overall_fingerprint"],
                    "invalidations": invalidation,
                }
            except TelemetryError as exc:
                if run_state is not None:
                    run_state["status"] = "error"
                    run_state["error"] = {
                        "code": exc.code,
                        "details": exc.details,
                    }
                    self._write_run_state(run_state)
                raise self._attach_diagnostic(exc) from exc
            except Exception as exc:
                if run_state is not None:
                    run_state["status"] = "error"
                    run_state["error"] = {
                        "code": "E_INTERNAL",
                        "message": str(exc),
                    }
                    self._write_run_state(run_state)
                raise

    # ------------------------------------------------------------- query APIs
    def fingerprints(self, config_name: str | None = None) -> dict[str, Any]:
        manifest = self.load_manifest()
        name, config = self.load_config_bundle(config_name, manifest=manifest)
        source_bytes, raw_digests = self._read_sources(manifest)
        del source_bytes
        plan = self.plan(config, raw_digests)
        head = self.store.head()
        catalog = head.get("catalog", {}) if head else {}
        stages = {}
        for stage in STAGES:
            published = catalog.get(stage, {}).get("stage_fingerprint")
            stages[stage] = {
                "current": plan["stage_fingerprints"][stage],
                "published": published,
                "fresh": published == plan["stage_fingerprints"][stage],
            }
        return {
            "ok": True,
            "config_name": name,
            "config_version": config.schema_version,
            "effective_config_version": config.effective_schema_version,
            "config_fingerprint": config.config_fingerprint,
            "semantic_fingerprint": config.semantic_fingerprint,
            "overall_fingerprint": plan["overall_fingerprint"],
            "stages": stages,
            "sources": {
                source_id: {
                    "raw_digest": raw_digests[source_id],
                    "normalize_fingerprint": plan[
                        "normalize_source_fingerprints"
                    ][source_id],
                    "resample_fingerprint": plan[
                        "resample_source_fingerprints"
                    ][source_id],
                }
                for source_id in config.source_ids
            },
            "checkpoint_head": head,
        }

    def status(self, config_name: str | None = None) -> dict[str, Any]:
        manifest = self.load_manifest()
        name, config = self.load_config_bundle(config_name, manifest=manifest)
        source_bytes, raw_digests = self._read_sources(manifest)
        del source_bytes
        plan = self.plan(config, raw_digests)
        records = self.store.records()
        head = records[-1] if records else None
        catalog = head.get("catalog", {}) if head else {}
        freshness = self._freshness(plan, head)
        invalidation = self._invalidation_report(config, plan, head)
        stages = []
        for stage in STAGES:
            published_fp = catalog.get(stage, {}).get("stage_fingerprint")
            stages.append(
                {
                    "stage": stage,
                    "rank": STAGE_RANK[stage],
                    "state": (
                        "fresh"
                        if freshness[stage]
                        else ("pending" if published_fp is None else "stale")
                    ),
                    "current_fingerprint": plan["stage_fingerprints"][stage],
                    "published_fingerprint": published_fp,
                    "checkpoint_seq": next(
                        (
                            record["seq"]
                            for record in reversed(records)
                            if record.get("stage") == stage
                            and record.get("fingerprint") == published_fp
                        ),
                        None,
                    ),
                }
            )
        summary_data = None
        if freshness["summary"]:
            try:
                summary_entry = catalog["summary"]
                summary_data = self._read_artifact(
                    summary_entry,
                    expected_fingerprint=plan["stage_fingerprints"]["summary"],
                    label="summary",
                )
            except TelemetryError:
                summary_data = None
        invalidations = []
        try:
            log = read_json(self.invalidation_path)
            if isinstance(log, list):
                invalidations = log[-10:]
        except Exception:
            pass
        return {
            "ok": True,
            "initialized": True,
            "workspace": str(self.root),
            "workspace_id": manifest.get("workspace_id"),
            "config": {
                "name": name,
                "source_schema_version": config.schema_version,
                "effective_schema_version": config.effective_schema_version,
                "config_fingerprint": config.config_fingerprint,
                "semantic_fingerprint": config.semantic_fingerprint,
                "degraded": config.degraded,
                "provenance": config.provenance,
                "path": manifest["configs"][name],
            },
            "sources": {
                source_id: {
                    "path": manifest["sources"][source_id],
                    "digest": raw_digests[source_id],
                    "normalize_fingerprint": plan[
                        "normalize_source_fingerprints"
                    ][source_id],
                    "resample_fingerprint": plan[
                        "resample_source_fingerprints"
                    ][source_id],
                }
                for source_id in config.source_ids
            },
            "fingerprints": {
                **plan["stage_fingerprints"],
                "overall": plan["overall_fingerprint"],
            },
            "stages": stages,
            "complete": all(freshness.values()),
            "checkpoints": {
                "head_seq": int(head["seq"]) if head else None,
                "head_stage": head["stage"] if head else None,
                "record_count": len(records),
                "sequence": [int(record["seq"]) for record in records],
            },
            "run": self._read_run_state(),
            "stop": self._read_stop_state(),
            "planned_invalidation": invalidation,
            "invalidations": invalidations,
            "counts": summary_data["counts"] if summary_data else None,
            "summary_fingerprint": (
                summary_data["fingerprint"] if summary_data else None
            ),
        }

    def _current_stage_artifact(
        self, stage: str, config_name: str | None = None
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        manifest = self.load_manifest()
        _, config = self.load_config_bundle(config_name, manifest=manifest)
        source_bytes, raw_digests = self._read_sources(manifest)
        del source_bytes
        plan = self.plan(config, raw_digests)
        head = self.store.head()
        if head is None:
            raise TelemetryError(
                "E_STALE_RESULT",
                "尚无对齐结果 / no alignment result exists",
                {"stage": stage},
            )
        entry = head.get("catalog", {}).get(stage)
        expected = plan["stage_fingerprints"][stage]
        if not isinstance(entry, dict) or entry.get("stage_fingerprint") != expected:
            raise TelemetryError(
                "E_STALE_RESULT",
                "结果已过期，需要重建 / result is stale and must be rebuilt",
                {
                    "stage": stage,
                    "expected_fingerprint": expected,
                    "published_fingerprint": (
                        entry.get("stage_fingerprint")
                        if isinstance(entry, dict)
                        else None
                    ),
                },
            )
        artifact = self._read_artifact(
            entry, expected_fingerprint=expected, label=stage
        )
        return artifact, entry, plan

    def conflicts(self, config_name: str | None = None) -> dict[str, Any]:
        fusion, _, _ = self._current_stage_artifact("fusion", config_name)
        return {
            "ok": True,
            "count": len(fusion["conflicts"]),
            "conflicts": fusion["conflicts"],
            "summary": fusion["summary"],
        }

    def gaps(self, config_name: str | None = None) -> dict[str, Any]:
        fusion, _, _ = self._current_stage_artifact("fusion", config_name)
        return {
            "ok": True,
            "count": len(fusion["gaps"]),
            "gaps": fusion["gaps"],
            "point_gaps": [
                gap for gap in fusion["gaps"] if gap["kind"] == "point_gap"
            ],
        }

    def cleaning(self, config_name: str | None = None) -> dict[str, Any]:
        artifact, _, plan = self._current_stage_artifact("normalize", config_name)
        return {
            "ok": True,
            "grid_fingerprint": plan["grid_fingerprint"],
            "fingerprint": artifact["fingerprint"],
            "reports": artifact["cleaning_reports"],
        }

    def summary(self, config_name: str | None = None) -> dict[str, Any]:
        summary, _, _ = self._current_stage_artifact("summary", config_name)
        return summary

    def export_summary(
        self,
        config_name: str | None = None,
        *,
        output_path: str = "out/alignment_summary.json",
    ) -> dict[str, Any]:
        summary, entry, _ = self._current_stage_artifact("summary", config_name)
        source_path = resolve_inside(self.root, entry["artifact_path"])
        destination = resolve_inside(self.root, output_path)
        data = source_path.read_bytes()
        atomic_write_bytes(destination, data)
        return {
            "ok": True,
            "path": safe_relpath(self.root, destination),
            "digest": sha256_bytes(data),
            "summary": summary,
        }

    def _sync_outputs(self, catalog: dict[str, Any]) -> None:
        normalize_entry = catalog.get("normalize")
        if isinstance(normalize_entry, dict):
            reports: dict[str, Any] = {}
            for source_id, entry in normalize_entry.get("sources", {}).items():
                artifact = self._read_artifact(
                    entry,
                    expected_fingerprint=entry["fingerprint"],
                    label=f"normalize:{source_id}",
                )
                reports[source_id] = artifact["cleaning_report"]
            cleaning_payload = {
                "schema_version": 1,
                "normalize_fingerprint": normalize_entry["stage_fingerprint"],
                "grid_fingerprint": normalize_entry.get("grid", {}).get(
                    "fingerprint"
                ),
                "reports": reports,
            }
            atomic_write_json(
                self.root / "out" / "cleaning_report.json", cleaning_payload
            )
        summary_entry = catalog.get("summary")
        if isinstance(summary_entry, dict):
            source_path = resolve_inside(self.root, summary_entry["artifact_path"])
            atomic_write_bytes(
                self.root / "out" / "alignment_summary.json",
                source_path.read_bytes(),
            )

    # --------------------------------------------------------------- imports
    def _materialize_import(
        self,
        value: Any,
        *,
        stem: str,
        suffix: str,
        default_area: str,
    ) -> tuple[str, bytes]:
        if isinstance(value, dict):
            if "content" in value:
                content = value["content"]
                data = (
                    content.encode("utf-8")
                    if isinstance(content, str)
                    else bytes(content)
                )
                digest = sha256_bytes(data)
                relative_path = (
                    f"imports/{default_area}/{stem}-{digest[:20]}{suffix}"
                )
                atomic_write_bytes(self.root / relative_path, data)
                return relative_path, data
            if "path" in value:
                path = resolve_inside(self.root, value["path"])
                if not path.exists():
                    raise TelemetryError(
                        "E_NOT_FOUND",
                        "导入文件不存在 / import file is missing",
                        {"path": value["path"]},
                    )
                data = path.read_bytes()
                return safe_relpath(self.root, path), data
            raise TelemetryError(
                "E_CONFIG_INVALID",
                "导入项必须提供 content 或 path / import needs content or path",
                {"stem": stem},
            )
        if isinstance(value, (str, bytes)):
            if isinstance(value, str):
                candidate = self.root / value
                if candidate.exists():
                    resolved = resolve_inside(self.root, value)
                    return safe_relpath(self.root, resolved), resolved.read_bytes()
                data = value.encode("utf-8")
            else:
                data = bytes(value)
            digest = sha256_bytes(data)
            relative_path = f"imports/{default_area}/{stem}-{digest[:20]}{suffix}"
            atomic_write_bytes(self.root / relative_path, data)
            return relative_path, data
        raise TelemetryError(
            "E_CONFIG_INVALID",
            "不支持的导入值 / unsupported import value",
            {"stem": stem, "type": type(value).__name__},
        )

    def import_inputs(
        self,
        *,
        sources: dict[str, Any] | None = None,
        configs: dict[str, Any] | None = None,
        active_config: str | None = None,
        v1_policy: dict[str, Any] | None = None,
        workspace_id: str | None = None,
    ) -> dict[str, Any]:
        if sources is not None and not isinstance(sources, dict):
            raise TelemetryError(
                "E_CONFIG_INVALID", "sources 必须是映射 / sources must be a mapping", {}
            )
        if configs is not None and not isinstance(configs, dict):
            raise TelemetryError(
                "E_CONFIG_INVALID", "configs 必须是映射 / configs must be a mapping", {}
            )
        if v1_policy is not None and not isinstance(v1_policy, dict):
            raise TelemetryError(
                "E_CONFIG_INVALID", "v1_policy 必须是映射 / v1_policy must be a mapping", {}
            )
        try:
            manifest = copy.deepcopy(self.load_manifest())
        except TelemetryError:
            manifest = {
                "schema_version": 1,
                "workspace_id": workspace_id or "imported-workspace",
                "sources": {},
                "configs": {},
                "active_config": None,
            }
        if workspace_id:
            manifest["workspace_id"] = workspace_id
        if v1_policy is not None:
            manifest["v1_policy"] = copy.deepcopy(v1_policy)
        source_digests = dict(manifest.get("source_digests", {}))
        for source_id, value in sorted((sources or {}).items()):
            relative_path, data = self._materialize_import(
                value,
                stem=str(source_id),
                suffix=".jsonl",
                default_area="sources",
            )
            manifest["sources"][str(source_id)] = relative_path
            source_digests[str(source_id)] = sha256_bytes(data)
        for config_name, value in sorted((configs or {}).items()):
            relative_path, _ = self._materialize_import(
                value,
                stem=f"config-{config_name}",
                suffix=".yaml",
                default_area="configs",
            )
            manifest["configs"][str(config_name)] = relative_path
        if active_config is not None:
            manifest["active_config"] = str(active_config)
        elif not manifest.get("active_config") and manifest["configs"]:
            manifest["active_config"] = (
                "v2" if "v2" in manifest["configs"] else sorted(manifest["configs"])[0]
            )
        manifest["source_digests"] = source_digests

        if manifest.get("active_config") not in manifest["configs"]:
            raise TelemetryError(
                "E_CONFIG_INVALID",
                "活动配置不存在 / active config does not exist",
                {
                    "active_config": manifest.get("active_config"),
                    "available": sorted(manifest["configs"]),
                },
            )

        validation: dict[str, Any] = {}
        for config_name in sorted(manifest["configs"]):
            path = resolve_inside(self.root, manifest["configs"][config_name])
            config = load_config(path, policy_override=manifest.get("v1_policy"))
            validation[config_name] = {
                "source_schema_version": config.schema_version,
                "effective_schema_version": config.effective_schema_version,
                "semantic_fingerprint": config.semantic_fingerprint,
                "config_fingerprint": config.config_fingerprint,
                "degraded": config.degraded,
                "provenance": config.provenance,
            }
        atomic_write_json(self.manifest_path, manifest)
        return {"ok": True, "manifest": manifest, "configs": validation}

    def upgrade_v1_config(
        self,
        *,
        config_name: str = "v1",
        output_path: str = "out/upgraded_config_v2.yaml",
    ) -> dict[str, Any]:
        manifest = self.load_manifest()
        if config_name not in manifest["configs"]:
            raise TelemetryError(
                "E_NOT_FOUND",
                "V1 配置不存在 / V1 config not found",
                {"config": config_name},
            )
        path = resolve_inside(self.root, manifest["configs"][config_name])
        config = load_config(path, policy_override=manifest.get("v1_policy"))
        if config.schema_version != 1:
            raise TelemetryError(
                "E_CONFIG_INVALID",
                "所选配置不是结构版本 1 / selected config is not schema version 1",
                {"config": config_name, "schema_version": config.schema_version},
            )
        yaml_text = config.to_v2_yaml()
        destination = resolve_inside(self.root, output_path)
        atomic_write_text(destination, yaml_text)
        reparsed = parse_config(
            yaml.safe_load(yaml_text),
            path=safe_relpath(self.root, destination),
        )
        equivalent = semantic_equal(config, reparsed)
        if not equivalent:
            raise TelemetryError(
                "E_CONFIG_INVALID",
                "V1→V2 往返语义不一致 / V1-to-V2 round trip changed semantics",
                {"config": config_name},
            )
        return {
            "ok": True,
            "path": safe_relpath(self.root, destination),
            "source_schema_version": 1,
            "effective_schema_version": 2,
            "degraded": True,
            "provenance": config.provenance,
            "semantic_fingerprint_before": config.semantic_fingerprint,
            "semantic_fingerprint_after": reparsed.semantic_fingerprint,
            "equivalent": equivalent,
            "yaml": yaml_text,
        }

    def mutate_source_and_rebuild(
        self,
        *,
        sensor_id: str = "sensor_a",
        config_name: str | None = None,
    ) -> dict[str, Any]:
        mutation = mutate_source(self.root, sensor_id)
        result = self.align(config_name=config_name, resume=False)
        return {"ok": True, "mutation": mutation, "alignment": result}


