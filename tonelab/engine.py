"""Build engine, mutations, checkpointed execution, and consistency audits."""

from __future__ import annotations

import copy
import json
import shutil
import tempfile
from pathlib import Path
from typing import Any, Callable, Iterable

import numpy as np

from .audio import read_wav
from .config import Config
from .errors import ToneLabError
from .features import extract_features
from .fingerprints import (
    STAGES,
    plan_invalidation,
    project_version_fingerprint,
    shard_version_fingerprint,
    stage_fingerprints,
    structure_version_fingerprint,
)
from .formats import (
    apply_units_annotation,
    export_project_json_v1,
    export_project_json_v2,
    export_units_annotations,
    export_wav_bundle,
    import_project_json,
    import_wav_bundle,
    load_manifest,
    project_to_v2_dict,
)
from .models import Project, PronUnit, Shard
from .modeling import build_articulation
from .quantization import quantize_shard
from .util import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_json_bytes,
    file_lock,
    pretty_json_bytes,
    read_json,
    relative_display,
    safe_component,
    sha256_bytes,
    sha256_file,
    sha256_text,
)
from .validation import validate_project, validate_shard
from .workspace import Workspace


def _artifact_stem(shard_id: str) -> str:
    return f"{safe_component(shard_id)}-{sha256_text(shard_id)[:8]}"


def _verify_embedded_digest(artifact: dict[str, Any]) -> bool:
    stored = artifact.get("artifact_sha256")
    if not stored:
        return False
    payload = dict(artifact)
    payload.pop("artifact_sha256", None)
    return sha256_bytes(canonical_json_bytes(payload)) == stored


def _walk_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(path for path in root.rglob("*") if path.is_file())


def _file_summaries(root: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for path in _walk_files(root):
        rel = path.relative_to(root).as_posix()
        result[rel] = {
            "sha256": sha256_file(path),
            "bytes": path.stat().st_size,
        }
    return result


class BuildEngine:
    def __init__(self, workspace: Workspace) -> None:
        self.workspace = workspace
        self.config = workspace.config
        self.manifest_path = workspace.stages_dir / "fingerprints.json"

    def _load_manifest(self) -> dict[str, Any]:
        if not self.manifest_path.exists():
            return {"format": "tonelab.stage-fingerprints", "schema": 1, "shards": {}}
        data = read_json(self.manifest_path)
        if data.get("format") != "tonelab.stage-fingerprints":
            raise ToneLabError("E_INVALID_STAGE", "invalid stage fingerprint manifest")
        data.setdefault("shards", {})
        return data

    def _save_manifest(self, manifest: dict[str, Any]) -> None:
        atomic_write_json(self.manifest_path, manifest)

    def _stage_path(self, stage: str, shard_id: str) -> Path:
        if stage == "export":
            return self.workspace.stages_dir / stage / f"{_artifact_stem(shard_id)}.json"
        return self.workspace.stages_dir / stage / f"{_artifact_stem(shard_id)}.json"

    def _artifact_rel(self, path: Path) -> str:
        return path.relative_to(self.workspace.root).as_posix()

    def _write_artifact(self, path: Path, artifact: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        data = canonical_json_bytes(artifact)
        atomic_write_bytes(path, data)
        digest = sha256_bytes(data)
        summary = {
            "format": artifact.get("format"),
            "artifact_sha256": artifact.get("artifact_sha256"),
            "file_sha256": digest,
            "bytes": len(data),
        }
        for key in (
            "frame_count",
            "emitted_frame_count",
            "truncated_frames",
            "formant_count",
            "sample_count",
        ):
            if key in artifact:
                summary[key] = artifact[key]
        return digest, summary

    def _load_artifact(self, path: Path, expected_sha: str, *, shard_id: str, stage: str) -> dict[str, Any]:
        if not path.exists():
            raise ToneLabError(
                "E_INVALID_STAGE",
                "required predecessor artifact is missing",
                shard_id=shard_id,
                context={"stage": stage, "artifact": self._artifact_rel(path)},
            )
        raw = path.read_bytes()
        actual_sha = sha256_bytes(raw)
        if actual_sha != expected_sha:
            raise ToneLabError(
                "E_INVALID_STAGE",
                "artifact content no longer matches its recorded digest",
                shard_id=shard_id,
                context={
                    "stage": stage,
                    "artifact": self._artifact_rel(path),
                    "expected_sha256": expected_sha,
                    "actual_sha256": actual_sha,
                },
            )
        try:
            artifact = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise ToneLabError(
                "E_INVALID_STAGE",
                "stored stage artifact is not valid JSON",
                shard_id=shard_id,
                context={"stage": stage, "reason": str(exc)},
            ) from exc
        if not _verify_embedded_digest(artifact):
            raise ToneLabError(
                "E_INVALID_STAGE",
                "stored stage artifact embedded digest is invalid",
                shard_id=shard_id,
                context={"stage": stage, "artifact": self._artifact_rel(path)},
            )
        return artifact

    def _export_shard_stage(self, shard: Shard, quantized: dict[str, Any]) -> tuple[dict[str, Any], list[Path]]:
        stage_dir = self.workspace.stages_dir / "export" / _artifact_stem(shard.shard_id)
        if stage_dir.exists():
            shutil.rmtree(stage_dir)
        stage_dir.mkdir(parents=True, exist_ok=True)
        curve = quantized["pitch_curve"]
        time_scale = float(curve["time_scale"])
        pitch_scale = float(curve["pitch_scale"])
        exported_shard = shard.clone()
        exported_shard.pitch_curve = [
            (float(t) / time_scale, float(v) / pitch_scale)
            for t, v in zip(curve["times_q"], curve["values_q"])
        ]
        exported_shard.quantization = {
            "pitch_scale": pitch_scale,
            "formant_scale": quantized["formant_trajectory"]["formant_scale"],
            "f0_scale": quantized["f0_trajectory"]["f0_scale"],
            "pitch_error_bound": quantized["pitch_curve"]["error_bound"],
            "formant_error_bound": quantized["formant_trajectory"]["error_bound"],
            "f0_error_bound": quantized["f0_trajectory"]["error_bound"],
        }
        exported_project = Project(
            project_id=shard.shard_id,
            shards=[exported_shard],
            declared_total_samples=exported_shard.sample_count,
        )
        wav_bundle = export_wav_bundle(exported_project, stage_dir)
        units_info = export_units_annotations(exported_project, stage_dir)
        files = _file_summaries(stage_dir)
        artifact = {
            "format": "tonelab.export-stage",
            "schema": 1,
            "shard_id": shard.shard_id,
            "structure_version": shard.structure_version,
            "source_structure_version": shard.source_structure_version,
            "quantized_artifact_sha256": quantized.get("artifact_sha256"),
            "wav_format": {
                key: wav_bundle["files"][shard.shard_id][key]
                for key in ("sample_rate", "channels", "bit_depth")
            },
            "files": files,
            "wav_bundle": {
                key: wav_bundle["files"][shard.shard_id][key]
                for key in ("wav_sha256", "sidecar_sha256")
            },
            "units_file": Path(units_info["files"][shard.shard_id]).name,
        }
        artifact["artifact_sha256"] = sha256_bytes(canonical_json_bytes(artifact))
        artifact_paths = [stage_dir / name for name in files]
        return artifact, artifact_paths

    def _build_project_exports(self, project: Project) -> dict[str, dict[str, Any]]:
        build_dir = self.workspace.exports_dir / "build"
        if build_dir.exists():
            shutil.rmtree(build_dir)
        build_dir.mkdir(parents=True, exist_ok=True)
        export_project_json_v2(project, build_dir / "project_v2.json", embed_pcm=True)
        export_wav_bundle(project, build_dir / "wav")
        export_units_annotations(project, build_dir / "units")
        export_project_json_v1(project, build_dir / "v1", self.config)
        return _file_summaries(build_dir)

    def _project_with_quantized_curves(
        self,
        project: Project,
        quantized_by_shard: dict[str, dict[str, Any]],
    ) -> Project:
        exported = project.clone()
        for shard in exported.shards:
            quantized = quantized_by_shard.get(shard.shard_id)
            if quantized is None:
                raise ToneLabError(
                    "E_INVALID_STAGE",
                    "cannot build project exports without a quantized shard artifact",
                    shard_id=shard.shard_id,
                    context={"stage": "export", "required_stage": "quantize"},
                )
            curve = quantized["pitch_curve"]
            time_scale = float(curve["time_scale"])
            pitch_scale = float(curve["pitch_scale"])
            shard.pitch_curve = [
                (float(t) / time_scale, float(v) / pitch_scale)
                for t, v in zip(curve["times_q"], curve["values_q"])
            ]
            shard.quantization = {
                "pitch_scale": pitch_scale,
                "formant_scale": quantized["formant_trajectory"]["formant_scale"],
                "f0_scale": quantized["f0_trajectory"]["f0_scale"],
                "pitch_error_bound": curve["error_bound"],
                "formant_error_bound": quantized["formant_trajectory"]["error_bound"],
                "f0_error_bound": quantized["f0_trajectory"]["error_bound"],
            }
        return exported

    def _project_export_paths(self, summary: dict[str, Any]) -> list[Path]:
        build_dir = self.workspace.exports_dir / "build"
        return [build_dir / relative for relative in summary]

    def build(
        self,
        mode: str = "incremental",
        *,
        clear_stop: bool = False,
        until_stage: str | None = None,
    ) -> dict[str, Any]:
        if mode not in {"incremental", "full"}:
            raise ToneLabError("E_BAD_REQUEST", "build mode must be incremental or full", context={"mode": mode})
        if until_stage is not None and until_stage not in STAGES:
            raise ToneLabError(
                "E_BAD_REQUEST",
                "until_stage must be one of feature/model/quantize/export",
                context={"until_stage": until_stage, "allowed": list(STAGES)},
            )
        stage_limit = len(STAGES) if until_stage is None else STAGES.index(until_stage) + 1
        selected_stages = STAGES[:stage_limit]
        if clear_stop:
            self.workspace.stop.clear()
        project = self.workspace.load_project()
        validate_project(project, self.config)
        if mode == "full":
            self.workspace.clear_derived()
        manifest = self._load_manifest()
        stop_preexisting = self.workspace.stop.is_requested()
        if stop_preexisting:
            report = self._base_report(project, mode, stopped=True, next_shard=project.shards[0].shard_id if project.shards else None)
            report["stop"] = self.workspace.stop.status()
            self.workspace.write_state({"last_build": report, "invalidation": {}})
            return report
        shard_plans: dict[str, Any] = {}
        recomputed_stages: dict[str, list[str]] = {}
        reused_stages: dict[str, list[str]] = {}
        truncated_total = 0
        recomputed_export = False
        quantized_by_shard: dict[str, dict[str, Any]] = {}
        stopped = False
        next_shard: str | None = None
        for shard_index, shard in enumerate(project.shards):
            if stopped:
                next_shard = shard.shard_id
                break
            new_fps = stage_fingerprints(shard, self.config)
            old_record = manifest["shards"].get(shard.shard_id, {})
            old_fps = {
                stage: {
                    "fingerprint": old_record[stage]["fingerprint"],
                    "inputs": old_record[stage].get("inputs", {}),
                }
                for stage in STAGES
                if stage in old_record
            }
            plan = plan_invalidation(old_fps, new_fps)
            plan["shard_id"] = shard.shard_id
            plan["fingerprint"] = shard_version_fingerprint(shard)
            shard_plans[shard.shard_id] = plan
            recomputed: list[str] = []
            reused: list[str] = []
            artifacts: dict[str, dict[str, Any]] = {}
            for stage in selected_stages:
                path = self._stage_path(stage, shard.shard_id)
                old_stage = old_record.get(stage)
                must_rebuild = mode == "full" or stage in plan["invalid_stages"]
                if not must_rebuild and old_stage:
                    artifact = self._load_artifact(
                        path,
                        str(old_stage["artifact_sha256"]),
                        shard_id=shard.shard_id,
                        stage=stage,
                    )
                    artifacts[stage] = artifact
                    reused.append(stage)
                    if stage == "feature":
                        truncated_total += int(artifact.get("truncated_frames", 0))
                    if stage == "export":
                        export_dir = self.workspace.stages_dir / "export" / _artifact_stem(shard.shard_id)
                        for rel in artifact.get("files", {}).keys():
                            artifact_path = export_dir / rel
                            if not artifact_path.exists():
                                raise ToneLabError(
                                    "E_INVALID_STAGE",
                                    "export artifact referenced by stage record is missing",
                                    shard_id=shard.shard_id,
                                    context={"stage": "export", "artifact": str(artifact_path)},
                                )
                    continue
                if stage == "feature":
                    artifact = extract_features(shard, self.config)
                    digest, summary = self._write_artifact(path, artifact)
                    artifact_paths = [path]
                elif stage == "model":
                    artifact = build_articulation(shard, artifacts["feature"], self.config)
                    digest, summary = self._write_artifact(path, artifact)
                    artifact_paths = [path]
                elif stage == "quantize":
                    artifact = quantize_shard(shard, artifacts["model"], self.config)
                    digest, summary = self._write_artifact(path, artifact)
                    artifact_paths = [path]
                else:
                    artifact, data_paths = self._export_shard_stage(shard, artifacts["quantize"])
                    digest, summary = self._write_artifact(path, artifact)
                    artifact_paths = [path, *data_paths]
                    recomputed_export = True
                artifacts[stage] = artifact
                recomputed.append(stage)
                if stage == "feature":
                    truncated_total += int(artifact.get("truncated_frames", 0))
                old_record[stage] = {
                    "fingerprint": new_fps[stage]["fingerprint"],
                    "inputs": new_fps[stage]["inputs"],
                    "artifact_file": self._artifact_rel(path),
                    "artifact_sha256": digest,
                    "artifact_summary": summary,
                }
                self.workspace.checkpoints.publish(
                    stage=stage,
                    shard_id=shard.shard_id,
                    shard_fingerprint=shard_version_fingerprint(shard),
                    structure_version_fingerprint=structure_version_fingerprint(project),
                    config_version=self.config.version,
                    artifact_summary=summary,
                    artifact_paths=artifact_paths,
                    project_id=project.project_id,
                    stage_fingerprint=new_fps[stage]["fingerprint"],
                )
            manifest["shards"][shard.shard_id] = old_record
            manifest["config_version"] = self.config.version
            manifest["project_id"] = project.project_id
            self._save_manifest(manifest)
            recomputed_stages[shard.shard_id] = recomputed
            reused_stages[shard.shard_id] = reused
            if "quantize" in artifacts:
                quantized_by_shard[shard.shard_id] = artifacts["quantize"]
            if self.workspace.stop.is_requested():
                stopped = True
                if shard_index + 1 < len(project.shards):
                    next_shard = project.shards[shard_index + 1].shard_id
        # Final project-level exports are themselves export-stage products.
        previous_state = self.workspace.read_state()
        previous_exports = previous_state.get("last_build", {}).get("project_exports")
        exports_ok = False
        if previous_exports and not recomputed_export and not stopped:
            paths = self._project_export_paths(previous_exports)
            if all(path.exists() for path in paths):
                exports_ok = True
            else:
                raise ToneLabError(
                    "E_INVALID_STAGE",
                    "project export artifact is missing while its stage record is still valid",
                    context={"missing": [str(path) for path in paths if not path.exists()]},
                )
        export_selected = "export" in selected_stages
        if not export_selected or (stopped and set(quantized_by_shard) != {shard.shard_id for shard in project.shards}):
            project_exports = previous_exports or {}
        elif exports_ok:
            project_exports = previous_exports
        else:
            export_project = self._project_with_quantized_curves(project, quantized_by_shard)
            project_exports = self._build_project_exports(export_project)
        current_stage_shas = {
            shard_id: {
                stage: manifest["shards"][shard_id][stage]["artifact_sha256"]
                for stage in selected_stages
                if stage in manifest["shards"].get(shard_id, {})
            }
            for shard_id in manifest["shards"]
        }
        current_fps = {
            shard.shard_id: stage_fingerprints(shard, self.config)
            for shard in project.shards
        }
        project_fp = project_version_fingerprint(project, current_fps, self.config)
        structure_fp = structure_version_fingerprint(project)
        quantized_bounds = {
            shard.shard_id: {
                stage: manifest["shards"][shard.shard_id][stage]["artifact_summary"]
                for stage in ("quantize",)
                if shard.shard_id in manifest["shards"] and stage in manifest["shards"][shard.shard_id]
            }
            for shard in project.shards
        }
        core = {
            "project_id": project.project_id,
            "config_version": self.config.version,
            "project_version_fingerprint": project_fp,
            "structure_version_fingerprint": structure_fp,
            "stage_artifact_sha256": current_stage_shas,
            "project_exports": project_exports,
            "truncated_frames_total": truncated_total if not stopped else None,
        }
        deterministic_fingerprint = sha256_bytes(canonical_json_bytes(core))
        report = self._base_report(project, mode, stopped=stopped, next_shard=next_shard)
        report.update(
            {
                "stopped": stopped,
                "next_shard": next_shard,
                "shard_order": [shard.shard_id for shard in project.shards],
                "until_stage": until_stage,
                "run_stages": list(selected_stages),
                "complete": (until_stage is None or until_stage == "export") and not stopped,
                "recomputed_stages": recomputed_stages,
                "reused_stages": reused_stages,
                "truncated_frames_total": truncated_total,
                "project_version_fingerprint": project_fp,
                "structure_version_fingerprint": structure_fp,
                "stage_artifact_sha256": current_stage_shas,
                "project_exports": project_exports,
                "deterministic_fingerprint": deterministic_fingerprint,
                "quantized_artifact_summaries": quantized_bounds,
                "checkpoint": {
                    "last_sequence": self.workspace.checkpoints.last_sequence(),
                    "record_count": len(self.workspace.checkpoints.records()),
                },
                "invalidation": shard_plans,
                "stop": self.workspace.stop.status(),
            }
        )
        self.workspace.write_state(
            {
                "last_build": report,
                "invalidation": shard_plans,
            }
        )
        return report

    def _base_report(self, project: Project, mode: str, *, stopped: bool, next_shard: str | None) -> dict[str, Any]:
        return {
            "format": "tonelab.build-report",
            "schema": 1,
            "mode": mode,
            "project_id": project.project_id,
            "config_version": self.config.version,
            "shard_count": len(project.shards),
            "sample_total": project.total_samples,
            "stopped": stopped,
            "next_shard": next_shard,
        }

    def request_stop(self, reason: str = "user requested stop") -> dict[str, Any]:
        payload = self.workspace.stop.request(reason)
        state = self.workspace.read_state()
        state["stop"] = payload
        self.workspace.write_state(state)
        return payload

    def resume(self) -> dict[str, Any]:
        self.workspace.stop.clear()
        return self.build("incremental", clear_stop=True)

    def run_stage(self, stage: str, mode: str = "incremental") -> dict[str, Any]:
        return self.build(mode, until_stage=stage)

    def status(self) -> dict[str, Any]:
        project = self.workspace.load_project()
        manifest = self._load_manifest()
        current_fps = {shard.shard_id: stage_fingerprints(shard, self.config) for shard in project.shards}
        invalidations: dict[str, Any] = {}
        stage_status: dict[str, Any] = {}
        for shard in project.shards:
            old_record = manifest["shards"].get(shard.shard_id, {})
            old_fps = {
                stage: {"fingerprint": old_record[stage]["fingerprint"], "inputs": old_record[stage].get("inputs", {})}
                for stage in STAGES
                if stage in old_record
            }
            plan = plan_invalidation(old_fps, current_fps[shard.shard_id])
            invalidations[shard.shard_id] = plan
            stages: dict[str, Any] = {}
            for stage in STAGES:
                record = old_record.get(stage)
                stages[stage] = {
                    "built_fingerprint": record.get("fingerprint") if record else None,
                    "current_fingerprint": current_fps[shard.shard_id][stage]["fingerprint"],
                    "valid": bool(record and record.get("fingerprint") == current_fps[shard.shard_id][stage]["fingerprint"]),
                    "artifact_file": record.get("artifact_file") if record else None,
                    "artifact_sha256": record.get("artifact_sha256") if record else None,
                }
            stage_status[shard.shard_id] = {
                "shard_fingerprint": shard_version_fingerprint(shard),
                "pcm_sha256": shard.pcm_sha256,
                "sample_count": shard.sample_count,
                "duration_ms": shard.duration_ms,
                "structure_version": shard.structure_version,
                "source_structure_version": shard.source_structure_version,
                "units": len(shard.units),
                "stages": stages,
            }
        records = self.workspace.checkpoints.records()
        sequences = [int(record["sequence"]) for record in records]
        checkpoint_valid = sequences == list(range(1, len(sequences) + 1))
        return {
            "ok": True,
            "project_id": project.project_id,
            "project_version_fingerprint": project_version_fingerprint(project, current_fps, self.config),
            "structure_version_fingerprint": structure_version_fingerprint(project),
            "config_version": self.config.version,
            "shard_count": len(project.shards),
            "sample_total": project.total_samples,
            "shards": stage_status,
            "invalidation": invalidations,
            "checkpoints": {
                "last_sequence": sequences[-1] if sequences else 0,
                "record_count": len(records),
                "continuous": checkpoint_valid,
            },
            "stop": self.workspace.stop.status(),
            "last_build": self.workspace.read_state().get("last_build"),
        }

    def fingerprints(self) -> dict[str, Any]:
        project = self.workspace.load_project()
        manifest = self._load_manifest()
        shards: dict[str, Any] = {}
        current_all: dict[str, dict[str, Any]] = {}
        for shard in project.shards:
            current = stage_fingerprints(shard, self.config)
            current_all[shard.shard_id] = current
            old = manifest["shards"].get(shard.shard_id, {})
            shards[shard.shard_id] = {
                "shard_fingerprint": shard_version_fingerprint(shard),
                "pcm_sha256": shard.pcm_sha256,
                "stages": {
                    stage: {
                        **current[stage],
                        "built_fingerprint": old.get(stage, {}).get("fingerprint"),
                        "built_artifact_sha256": old.get(stage, {}).get("artifact_sha256"),
                    }
                    for stage in STAGES
                },
            }
        return {
            "ok": True,
            "project_version_fingerprint": project_version_fingerprint(project, current_all, self.config),
            "structure_version_fingerprint": structure_version_fingerprint(project),
            "config_version": self.config.version,
            "shards": shards,
        }

    def checkpoints(self) -> dict[str, Any]:
        records = self.workspace.checkpoints.records()
        sequences = [int(record["sequence"]) for record in records]
        return {
            "ok": True,
            "last_sequence": sequences[-1] if sequences else 0,
            "continuous": sequences == list(range(1, len(sequences) + 1)),
            "records": records,
        }

    def export(self, fmt: str, out: str | Path) -> dict[str, Any]:
        project = self.workspace.load_project()
        validate_project(project, self.config)
        target = Path(out)
        if fmt in {"json", "json_v2", "v2"}:
            return export_project_json_v2(project, target, embed_pcm=True)
        if fmt in {"wav", "wav_bundle"}:
            return export_wav_bundle(project, target)
        if fmt in {"units", "annotations"}:
            return export_units_annotations(project, target)
        if fmt in {"json_v1", "v1"}:
            return export_project_json_v1(project, target, self.config)
        raise ToneLabError("E_BAD_REQUEST", "unknown export format", context={"format": fmt})

    def _compare_project_data(self, left: Project, right: Project) -> list[str]:
        differences: list[str] = []
        if len(left.shards) != len(right.shards):
            differences.append(f"shard_count:{len(left.shards)}!={len(right.shards)}")
            return differences
        for a, b in zip(left.shards, right.shards):
            if a.shard_id != b.shard_id:
                differences.append(f"shard_id:{a.shard_id}!={b.shard_id}")
            if not np.array_equal(a.pcm, b.pcm):
                differences.append(f"{a.shard_id}:pcm")
            if a.pitch_curve != b.pitch_curve:
                differences.append(f"{a.shard_id}:pitch_curve")
            if [u.to_dict() for u in a.units] != [u.to_dict() for u in b.units]:
                differences.append(f"{a.shard_id}:units")
            if a.structure_version != b.structure_version or a.source_structure_version != b.source_structure_version:
                differences.append(f"{a.shard_id}:structure_version")
        return differences

    def roundtrip(self, fmt: str) -> dict[str, Any]:
        project = self.workspace.load_project()
        validate_project(project, self.config)
        temp = Path(tempfile.mkdtemp(prefix="roundtrip-", dir=self.workspace.tmp_dir))
        try:
            if fmt in {"json", "json_v2", "v2"}:
                first = temp / "first.json"
                second = temp / "second.json"
                exported = export_project_json_v2(project, first, embed_pcm=True)
                imported = import_project_json(first, self.config)
                diff = self._compare_project_data(project, imported)
                reexported = export_project_json_v2(imported, second, embed_pcm=True)
                stable = sha256_file(first) == sha256_file(second)
                if diff or not stable:
                    raise ToneLabError(
                        "E_VALIDATION",
                        "JSON v2 roundtrip mismatch",
                        context={"differences": diff, "stable_export": stable},
                    )
                return {
                    "ok": True,
                    "format": "json_v2",
                    "first_sha256": exported["sha256"],
                    "second_sha256": reexported["sha256"],
                    "stable_export": stable,
                    "differences": [],
                }
            if fmt in {"wav", "wav_bundle"}:
                first_dir = temp / "wav-first"
                second_dir = temp / "wav-second"
                exported = export_wav_bundle(project, first_dir)
                imported = import_wav_bundle(first_dir, self.config)
                diff = self._compare_project_data(project, imported)
                reexported = export_wav_bundle(imported, second_dir)
                files_a = _file_summaries(first_dir)
                files_b = _file_summaries(second_dir)
                stable = files_a == files_b
                if diff or not stable:
                    raise ToneLabError(
                        "E_VALIDATION",
                        "WAV bundle roundtrip mismatch",
                        context={"differences": diff, "stable_export": stable, "first_files": files_a, "second_files": files_b},
                    )
                return {
                    "ok": True,
                    "format": "wav",
                    "files": files_a,
                    "stable_export": stable,
                    "wav_sha256": {sid: item["wav_sha256"] for sid, item in exported["files"].items()},
                    "differences": [],
                }
            if fmt in {"units", "annotations"}:
                first_dir = temp / "units-first"
                second_dir = temp / "units-second"
                export_units_annotations(project, first_dir)
                imported = project.clone()
                apply_units_annotation(imported, first_dir, self.config)
                diff = self._compare_project_data(project, imported)
                export_units_annotations(imported, second_dir)
                files_a = _file_summaries(first_dir)
                files_b = _file_summaries(second_dir)
                stable = files_a == files_b
                if diff or not stable:
                    raise ToneLabError(
                        "E_VALIDATION",
                        "units annotation roundtrip mismatch",
                        context={"differences": diff, "stable_export": stable},
                    )
                return {
                    "ok": True,
                    "format": "units",
                    "files": files_a,
                    "stable_export": stable,
                    "differences": [],
                }
            if fmt in {"json_v1", "v1"}:
                first_dir = temp / "v1-first"
                second_dir = temp / "v1-second"
                exported = export_project_json_v1(project, first_dir, self.config)
                imported = load_manifest(first_dir / "manifest_v1.json", self.config)
                pcm_equal = all(
                    np.array_equal(a.pcm, b.pcm)
                    for a, b in zip(project.shards, imported.shards)
                )
                reexported = export_project_json_v1(imported, second_dir, self.config)
                stable = sha256_file(first_dir / "manifest_v1.json") == sha256_file(second_dir / "manifest_v1.json")
                if not pcm_equal or not stable:
                    raise ToneLabError(
                        "E_VALIDATION",
                        "version 1 roundtrip mismatch",
                        context={"pcm_equal": pcm_equal, "stable_manifest": stable},
                    )
                percent_error = []
                for original, upgraded in zip(project.shards, imported.shards):
                    for (time_a, value_a), (time_b, value_b) in zip(original.pitch_curve, upgraded.pitch_curve):
                        percent_error.append(abs(value_a - value_b))
                return {
                    "ok": True,
                    "format": "json_v1",
                    "first_sha256": exported["sha256"],
                    "second_sha256": reexported["sha256"],
                    "stable_manifest": stable,
                    "pcm_equal": pcm_equal,
                    "max_normalized_pitch_delta": max(percent_error, default=0.0),
                    "pitch_percent_rounding_error_bound": exported["pitch_percent_rounding_error_bound"],
                    "differences": [],
                }
            raise ToneLabError("E_BAD_REQUEST", "unknown roundtrip format", context={"format": fmt})
        finally:
            shutil.rmtree(temp, ignore_errors=True)

    def replace_sample(self, shard_id: str, wav_path: str | Path) -> dict[str, Any]:
        with file_lock(self.workspace.lock_path):
            project = self.workspace.load_project()
            shard = project.shard(shard_id)
            pcm, info = read_wav(
                wav_path,
                shard_id=shard_id,
                expected_sample_rate=shard.sample_rate,
                expected_channels=shard.channels,
                expected_bit_depth=shard.bit_depth,
            )
            before = shard.sample_count
            shard.pcm = pcm
            project.declared_total_samples += shard.sample_count - before
            validate_project(project, self.config)
            self.workspace.save_project(project)
            mutation = {
                "kind": "replace_sample",
                "shard_id": shard_id,
                "old_sample_count": before,
                "new_sample_count": shard.sample_count,
                "new_pcm_sha256": shard.pcm_sha256,
                "wav_info": info,
            }
            self._record_mutation_locked(mutation)
            return {"ok": True, **mutation}

    def replace_sample_pcm(self, shard_id: str, pcm: np.ndarray) -> dict[str, Any]:
        with file_lock(self.workspace.lock_path):
            project = self.workspace.load_project()
            shard = project.shard(shard_id)
            before = shard.sample_count
            shard.pcm = np.asarray(pcm, dtype=np.int16).copy()
            project.declared_total_samples += shard.sample_count - before
            validate_project(project, self.config)
            self.workspace.save_project(project)
            mutation = {
                "kind": "replace_sample",
                "shard_id": shard_id,
                "old_sample_count": before,
                "new_sample_count": shard.sample_count,
                "new_pcm_sha256": shard.pcm_sha256,
            }
            self._record_mutation_locked(mutation)
            return {"ok": True, **mutation}

    def edit_unit(
        self,
        shard_id: str,
        unit_index: int,
        *,
        start_ms: float | None = None,
        end_ms: float | None = None,
        symbol: str | None = None,
        formants_hz: list[float] | None = None,
        formant_ranges_hz: list[list[float]] | None = None,
    ) -> dict[str, Any]:
        with file_lock(self.workspace.lock_path):
            project = self.workspace.load_project()
            shard = project.shard(shard_id)
            if unit_index < 0 or unit_index >= len(shard.units):
                raise ToneLabError(
                    "E_NOT_FOUND",
                    "unit index is out of range",
                    shard_id=shard_id,
                    unit_index=unit_index,
                    context={"unit_count": len(shard.units)},
                )
            unit = shard.units[unit_index]
            old = unit.to_dict()
            if start_ms is not None:
                unit.start_ms = float(start_ms)
            if end_ms is not None:
                unit.end_ms = float(end_ms)
            if symbol is not None:
                unit.symbol = str(symbol)
            if formants_hz is not None:
                unit.formants_hz = [float(value) for value in formants_hz]
                if formant_ranges_hz is None:
                    unit.formant_ranges_hz = [
                        [value * 0.9, value * 1.1] for value in unit.formants_hz
                    ]
            if formant_ranges_hz is not None:
                unit.formant_ranges_hz = [
                    [float(pair[0]), float(pair[1])] for pair in formant_ranges_hz
                ]
            validate_project(project, self.config)
            self.workspace.save_project(project)
            mutation = {
                "kind": "edit_unit",
                "shard_id": shard_id,
                "unit_index": unit_index,
                "old": old,
                "new": unit.to_dict(),
            }
            self._record_mutation_locked(mutation)
            return {"ok": True, **mutation}

    def edit_pitch_point(
        self,
        shard_id: str,
        point_index: int,
        *,
        time: float | None = None,
        value: float | None = None,
    ) -> dict[str, Any]:
        if time is None and value is None:
            raise ToneLabError("E_BAD_REQUEST", "pitch edit requires time and/or value")
        with file_lock(self.workspace.lock_path):
            project = self.workspace.load_project()
            shard = project.shard(shard_id)
            if point_index < 0 or point_index >= len(shard.pitch_curve):
                raise ToneLabError(
                    "E_NOT_FOUND",
                    "pitch point index is out of range",
                    shard_id=shard_id,
                    position={"kind": "pitch_point", "index": point_index},
                    context={"point_count": len(shard.pitch_curve)},
                )
            old = shard.pitch_curve[point_index]
            shard.pitch_curve[point_index] = (
                old[0] if time is None else float(time),
                old[1] if value is None else float(value),
            )
            validate_project(project, self.config)
            self.workspace.save_project(project)
            mutation = {
                "kind": "edit_pitch",
                "shard_id": shard_id,
                "point_index": point_index,
                "old": old,
                "new": shard.pitch_curve[point_index],
            }
            self._record_mutation_locked(mutation)
            return {"ok": True, **mutation}

    def upgrade_shard(self, shard_id: str, to_version: int = 2) -> dict[str, Any]:
        if int(to_version) != 2:
            raise ToneLabError("E_BAD_REQUEST", "only explicit upgrade to structure version 2 is supported")
        with file_lock(self.workspace.lock_path):
            project = self.workspace.load_project()
            shard = project.shard(shard_id)
            old_source = shard.source_structure_version
            old_structure = shard.structure_version
            shard.structure_version = 2
            shard.source_structure_version = 2
            shard.origin = {**shard.origin, "explicit_upgrade": {"from": old_source, "to": 2}}
            validate_project(project, self.config)
            self.workspace.save_project(project)
            mutation = {
                "kind": "upgrade",
                "shard_id": shard_id,
                "old_structure_version": old_structure,
                "old_source_structure_version": old_source,
                "new_structure_version": 2,
                "new_source_structure_version": 2,
            }
            self._record_mutation_locked(mutation)
            return {"ok": True, **mutation}

    def _record_mutation_locked(self, mutation: dict[str, Any]) -> None:
        state = self.workspace.read_state()
        log = list(state.get("mutations", []))
        log.append(mutation)
        state["mutations"] = log[-200:]
        self.workspace.write_state(state)

    def _mutate_for_consistency(self, project: Project, mutation: dict[str, Any]) -> tuple[Project, dict[str, Any]]:
        mutated = project.clone()
        kind = str(mutation.get("kind"))
        if kind == "sample":
            shard_id = str(mutation["shard_id"])
            shard = mutated.shard(shard_id)
            before = shard.sample_count
            if mutation.get("pcm") is not None:
                pcm = np.asarray(mutation["pcm"], dtype=np.int16)
            elif mutation.get("wav"):
                pcm, _info = read_wav(
                    mutation["wav"],
                    shard_id=shard_id,
                    expected_sample_rate=shard.sample_rate,
                    expected_channels=shard.channels,
                    expected_bit_depth=shard.bit_depth,
                )
            else:
                pcm = shard.pcm.copy()
                pcm = np.roll(pcm, int(mutation.get("shift", 1))).astype(np.int16)
                pcm[0] = (int(pcm[0]) + 1) % 32768
            shard.pcm = pcm
            mutated.declared_total_samples += shard.sample_count - before
        elif kind == "unit":
            shard_id = str(mutation["shard_id"])
            shard = mutated.shard(shard_id)
            unit_index = int(mutation["unit_index"])
            if unit_index < 0 or unit_index >= len(shard.units):
                raise ToneLabError("E_NOT_FOUND", "unit index is out of range", shard_id=shard_id, unit_index=unit_index)
            unit = shard.units[unit_index]
            if "start_ms" in mutation:
                unit.start_ms = float(mutation["start_ms"])
            if "end_ms" in mutation:
                unit.end_ms = float(mutation["end_ms"])
            if "symbol" in mutation:
                unit.symbol = str(mutation["symbol"])
            if "formants_hz" in mutation:
                unit.formants_hz = [float(value) for value in mutation["formants_hz"]]
        elif kind == "upgrade":
            shard_id = str(mutation["shard_id"])
            shard = mutated.shard(shard_id)
            shard.structure_version = 2
            shard.source_structure_version = 2
            shard.origin = {**shard.origin, "explicit_upgrade": {"from": shard.source_structure_version, "to": 2}}
        elif kind == "pitch":
            shard_id = str(mutation["shard_id"])
            shard = mutated.shard(shard_id)
            point_index = int(mutation["point_index"])
            old = shard.pitch_curve[point_index]
            shard.pitch_curve[point_index] = (old[0], float(mutation["value"]))
        else:
            raise ToneLabError("E_BAD_REQUEST", "unsupported consistency mutation", context={"mutation": mutation})
        validate_project(mutated, self.config)
        return mutated, {"kind": kind, "mutation": mutation}

    def consistency_check(self, mutation: dict[str, Any]) -> dict[str, Any]:
        base_project = self.workspace.load_project()
        if not self.manifest_path.exists():
            raise ToneLabError(
                "E_INVALID_STAGE",
                "incremental consistency requires an existing built workspace",
            )
        mutated, mutation_info = self._mutate_for_consistency(base_project, mutation)
        temp_root = Path(tempfile.mkdtemp(prefix="consistency-", dir=self.workspace.tmp_dir))
        inc_root = temp_root / "incremental"
        full_root = temp_root / "full"
        try:
            inc_config = Config(copy.deepcopy(self.config.data), source="consistency")
            full_config = Config(copy.deepcopy(self.config.data), source="consistency")
            inc_workspace = Workspace(inc_root, config=inc_config)
            self.workspace.clone_derived_to(inc_workspace)
            inc_workspace.save_project(mutated)
            inc_report = BuildEngine(inc_workspace).build("incremental")
            full_workspace = Workspace(full_root, config=full_config)
            full_workspace.save_project(mutated.clone())
            full_report = BuildEngine(full_workspace).build("full")
            differences: list[dict[str, Any]] = []
            inc_manifest = read_json(inc_workspace.stages_dir / "fingerprints.json")
            full_manifest = read_json(full_workspace.stages_dir / "fingerprints.json")
            for shard in mutated.shards:
                for stage in STAGES:
                    inc_record = inc_manifest.get("shards", {}).get(shard.shard_id, {}).get(stage)
                    full_record = full_manifest.get("shards", {}).get(shard.shard_id, {}).get(stage)
                    if inc_record is None or full_record is None:
                        differences.append(
                            {"shard_id": shard.shard_id, "stage": stage, "kind": "missing-stage-record"}
                        )
                        continue
                    if inc_record.get("fingerprint") != full_record.get("fingerprint"):
                        differences.append(
                            {
                                "shard_id": shard.shard_id,
                                "stage": stage,
                                "kind": "fingerprint-mismatch",
                                "incremental": inc_record.get("fingerprint"),
                                "full": full_record.get("fingerprint"),
                            }
                        )
                    inc_path = inc_workspace.root / inc_record["artifact_file"]
                    full_path = full_workspace.root / full_record["artifact_file"]
                    inc_bytes = inc_path.read_bytes() if inc_path.exists() else b""
                    full_bytes = full_path.read_bytes() if full_path.exists() else b""
                    if sha256_bytes(inc_bytes) != sha256_bytes(full_bytes) or inc_bytes != full_bytes:
                        differences.append(
                            {
                                "shard_id": shard.shard_id,
                                "stage": stage,
                                "kind": "artifact-bytes-mismatch",
                                "incremental_sha256": sha256_bytes(inc_bytes),
                                "full_sha256": sha256_bytes(full_bytes),
                            }
                        )
            inc_exports = _file_summaries(inc_workspace.exports_dir / "build")
            full_exports = _file_summaries(full_workspace.exports_dir / "build")
            if inc_exports != full_exports:
                for relative in sorted(set(inc_exports) | set(full_exports)):
                    if inc_exports.get(relative) != full_exports.get(relative):
                        differences.append(
                            {
                                "shard_id": "project",
                                "stage": "export",
                                "kind": "export-file-mismatch",
                                "file": relative,
                                "incremental": inc_exports.get(relative),
                                "full": full_exports.get(relative),
                            }
                        )
            if inc_report.get("deterministic_fingerprint") != full_report.get("deterministic_fingerprint"):
                differences.append(
                    {
                        "shard_id": "project",
                        "stage": "export",
                        "kind": "deterministic-fingerprint-mismatch",
                        "incremental": inc_report.get("deterministic_fingerprint"),
                        "full": full_report.get("deterministic_fingerprint"),
                    }
                )
            if differences:
                first = differences[0]
                raise ToneLabError(
                    "E_INCREMENTAL_MISMATCH",
                    "incremental build is not byte-identical to a full rebuild",
                    shard_id=str(first.get("shard_id")),
                    context={
                        "stage": first.get("stage"),
                        "differences": differences,
                        "incremental_fingerprint": inc_report.get("deterministic_fingerprint"),
                        "full_fingerprint": full_report.get("deterministic_fingerprint"),
                    },
                )
            return {
                "ok": True,
                "mutation": mutation_info,
                "incremental_fingerprint": inc_report["deterministic_fingerprint"],
                "full_fingerprint": full_report["deterministic_fingerprint"],
                "incremental_recomputed": inc_report.get("recomputed_stages"),
                "incremental_reused": inc_report.get("reused_stages"),
                "compared": {
                    "shards": len(mutated.shards),
                    "stages": len(mutated.shards) * len(STAGES),
                    "export_files": len(full_exports),
                },
                "differences": [],
            }
        finally:
            shutil.rmtree(temp_root, ignore_errors=True)

