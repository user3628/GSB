"""Pipeline orchestration, status queries, graceful stop, resume, and exports."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .assembly import assemble_samples
from .checkpoints import CheckpointStore
from .config import SCALES, STAGES, STAGE_INDEX, Config
from .decision import budget_ledger, select_algorithm_plan
from .errors import E_INVALID_STAGE, E_NOT_FOUND, E_STALE_CHECKPOINT, PipelineError
from .features import encode_records, normalize_samples
from .sample_store import SampleRepository, ScaleData
from .util import (
    locked_file,
    pretty_json_bytes,
    read_json,
    read_jsonl,
    sha256_file,
    write_json_atomic,
    write_jsonl_atomic,
)
from .versioning import expected_stage_versions, stage_change_reason


class StopController:
    def __init__(self, workdir: Path) -> None:
        self.workdir = Path(workdir).resolve()
        self.path = self.workdir / "STOP_REQUEST"
        self.lock_path = self.workdir / "locks" / "stop.lock"

    def request(self, scope: str) -> dict[str, Any]:
        if scope not in SCALES and scope != "all":
            raise PipelineError(
                E_INVALID_STAGE,
                "stop scope must be a scale or all",
                scope=scope,
            )
        payload = {"scope": scope, "semantics": "finish_current_stage_then_exit"}
        with locked_file(self.lock_path):
            write_json_atomic(self.path, payload)
        return payload

    def peek(self) -> dict[str, Any] | None:
        with locked_file(self.lock_path):
            if not self.path.exists():
                return None
            try:
                value = json.loads(self.path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                return {"scope": "all", "corrupt": True}
            return value if isinstance(value, dict) else {"scope": "all", "corrupt": True}

    def consume(self, scale: str) -> dict[str, Any] | None:
        with locked_file(self.lock_path):
            if not self.path.exists():
                return None
            try:
                value = json.loads(self.path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                value = {"scope": "all", "corrupt": True}
            if not isinstance(value, dict):
                value = {"scope": "all", "corrupt": True}
            scope = value.get("scope", "all")
            if scope not in (scale, "all"):
                return None
            self.path.unlink(missing_ok=True)
            return value


class PipelineManager:
    def __init__(
        self,
        project_root: Path,
        *,
        config_path: Path | None = None,
        config_data: dict[str, Any] | None = None,
        workdir_override: Path | None = None,
        config_label: str = "pipeline-config",
    ) -> None:
        self.project_root = Path(project_root).resolve()
        if config_data is not None:
            self.config = Config.from_data(
                self.project_root,
                config_data,
                workdir_override=workdir_override,
                source_label=config_label,
            )
        else:
            self.config = Config.load(
                self.project_root,
                config_path,
                workdir_override=workdir_override,
            )
        self.workdir = self.config.workdir
        self.repository = SampleRepository(self.config)
        self.checkpoints = CheckpointStore(self.workdir)
        self.stop_controller = StopController(self.workdir)

    def import_samples(self) -> dict[str, Any]:
        return self.repository.import_all()

    def status(self, scale: str) -> dict[str, Any]:
        scale_data = self.repository.load_scale(scale)
        versions = expected_stage_versions(
            self.config,
            scale,
            scale_data.declaration,
            scale_data.sample_input_fingerprint,
        )
        with locked_file(self.checkpoints.checkpoint_lock_path(scale)):
            state = self.checkpoints.scan(scale)
            records = state["records"]
            invalid_files = state["invalid_files"]
            invalid_by_path = {item["path"]: item["reason"] for item in invalid_files}
            stage_statuses: list[dict[str, Any]] = []
            earliest_invalid: str | None = None
            for index, stage in enumerate(STAGES):
                expected = versions[stage]
                record = records[index] if index < len(records) else None
                status: dict[str, Any] = {
                    "index": index,
                    "seq": index + 1,
                    "stage": stage,
                    "state": "missing",
                    "checkpoint_seq": None,
                    "expected_fingerprints": {
                        "input_fingerprint": expected.input_fingerprint,
                        "config_fingerprint": expected.config_fingerprint,
                        "stage_fingerprint": expected.stage_fingerprint,
                    },
                    "stored_fingerprints": None,
                    "artifact_errors": [],
                    "reasons": [],
                    "config_version_differs": False,
                }
                target_path = self.checkpoints.checkpoint_path(scale, index + 1, stage)
                target_display = self.checkpoints._display_path(target_path)
                if earliest_invalid is not None:
                    status["state"] = "blocked_by_upstream"
                    status["reasons"] = ["upstream_stage_invalid"]
                    status["invalidated_by"] = earliest_invalid
                elif record is None:
                    if target_display in invalid_by_path:
                        status["state"] = "corrupt_checkpoint"
                        status["reasons"] = [invalid_by_path[target_display]]
                    else:
                        related_invalid = [
                            item
                            for item in invalid_files
                            if item.get("path", "").startswith(f"{index + 1:04d}_")
                        ]
                        if related_invalid:
                            status["state"] = "corrupt_checkpoint"
                            status["reasons"] = sorted(item["reason"] for item in related_invalid)
                        else:
                            status["state"] = "missing"
                            status["reasons"] = ["stage_not_published"]
                else:
                    status["checkpoint_seq"] = record["seq"]
                    status["stored_fingerprints"] = {
                        "input_fingerprint": record["input_fingerprint"],
                        "config_fingerprint": record["stage_config_fingerprint"],
                        "stage_fingerprint": record["stage_fingerprint"],
                    }
                    artifact_errors = self.checkpoints.verify_artifacts(record)
                    status["artifact_errors"] = artifact_errors
                    input_changed = record["input_fingerprint"] != expected.input_fingerprint
                    config_changed = (
                        record["stage_config_fingerprint"] != expected.config_fingerprint
                    )
                    stage_changed = record["stage_fingerprint"] != expected.stage_fingerprint
                    status["config_version_differs"] = (
                        record["config_version"] != self.config.config_version
                    )
                    if artifact_errors:
                        status["state"] = "artifact_invalid"
                        status["reasons"] = sorted(
                            {item["reason"] for item in artifact_errors}
                        )
                    elif input_changed or config_changed or stage_changed:
                        status["state"] = "stale"
                        status["reasons"] = [
                            stage_change_reason(stage, input_changed, config_changed)
                        ]
                    else:
                        status["state"] = "valid"
                        status["reasons"] = []
                if status["state"] != "valid" and earliest_invalid is None:
                    earliest_invalid = stage
                    status["reasons"].append("earliest_invalid_stage")
                stage_statuses.append(status)

            fully_complete = all(item["state"] == "valid" for item in stage_statuses)
            invalidated_stages = [
                item["stage"] for item in stage_statuses if item["state"] != "valid"
            ]
            result: dict[str, Any] = {
                "scale": scale,
                "config_version": self.config.config_version,
                "config_source": self.config.source_label,
                "workdir": str(self.workdir),
                "declaration": scale_data.declaration,
                "sample_input_fingerprint": scale_data.sample_input_fingerprint,
                "stages": stage_statuses,
                "stage_version_fingerprints": {
                    stage: {
                        "input_fingerprint": item.input_fingerprint,
                        "config_fingerprint": item.config_fingerprint,
                        "stage_fingerprint": item.stage_fingerprint,
                    }
                    for stage, item in versions.items()
                },
                "earliest_invalid_stage": earliest_invalid,
                "invalidated_stages": invalidated_stages,
                "needs_rebuild": earliest_invalid is not None,
                "fully_complete": fully_complete,
                "checkpoint_sequence": state["current_sequence"],
                "checkpoint_digest": self.checkpoints.checkpoint_digest(scale),
                "invalid_checkpoint_files": invalid_files,
                "stop_requested": self.stop_controller.peek(),
            }
            if fully_complete:
                stats_path = self.checkpoints.stage_artifact_dir(scale, "assemble") / "stats.json"
                samples_path = self.checkpoints.stage_artifact_dir(scale, "assemble") / "samples.jsonl"
                if stats_path.exists():
                    result["stats_content_digest"] = read_json(stats_path).get("content_digest")
                if samples_path.exists():
                    result["samples_sha256"] = sha256_file(samples_path)
            return result

    def _stage_files(self, scale: str, stage: str) -> dict[str, Path]:
        directory = self.checkpoints.stage_artifact_dir(scale, stage)
        if stage == "normalize":
            return {
                "normalized": directory / "normalized.jsonl",
                "summary": directory / "normalize_summary.json",
            }
        if stage == "decision":
            return {"decision": directory / "decision.json"}
        if stage == "encode":
            return {
                "encoded": directory / "encoded.jsonl",
                "summary": directory / "encoding_summary.json",
            }
        if stage == "assemble":
            return {
                "samples": directory / "samples.jsonl",
                "discarded": directory / "discarded.jsonl",
                "stats": directory / "stats.json",
                "summary": directory / "assembly_summary.json",
                "budget": directory / "budget_ledger.json",
            }
        raise PipelineError(E_INVALID_STAGE, "unknown stage", stage=stage)

    def _publish_stage(
        self,
        scale: str,
        stage: str,
        declaration: dict[str, Any],
        sample_input_fingerprint: str,
        artifact_paths: list[Path],
    ) -> dict[str, Any]:
        versions = expected_stage_versions(
            self.config, scale, declaration, sample_input_fingerprint
        )
        version = versions[stage]
        return self.checkpoints.publish(
            scale,
            stage,
            expected_seq=STAGE_INDEX[stage] + 1,
            input_fingerprint=version.input_fingerprint,
            config_version=self.config.config_version,
            stage_config_fingerprint=version.config_fingerprint,
            stage_fingerprint=version.stage_fingerprint,
            artifact_paths=artifact_paths,
        )

    def run(
        self,
        scale: str,
        *,
        stop_after_stage: str | None = None,
    ) -> dict[str, Any]:
        if scale not in SCALES:
            raise PipelineError(
                E_INVALID_STAGE,
                "unknown scale",
                scale=scale,
                valid_scales=list(SCALES),
            )
        if stop_after_stage is not None and stop_after_stage not in STAGES:
            raise PipelineError(
                E_INVALID_STAGE,
                "stop_after_stage is not a known stage",
                stage=stop_after_stage,
                valid_stages=list(STAGES),
            )
        self.repository.load_import_manifest()
        with locked_file(self.checkpoints.run_lock_path(scale)):
            before = self.status(scale)
            if before["fully_complete"]:
                return {
                    "state": "already_complete",
                    "scale": scale,
                    "stages_executed": [],
                    "stages_reused": list(STAGES),
                    "status": before,
                }
            earliest = before["earliest_invalid_stage"] or STAGES[0]
            invalidation = self.checkpoints.invalidate_from(scale, earliest)
            scale_data = self.repository.load_scale(scale)
            stage_index = STAGE_INDEX[earliest]
            executed: list[str] = []
            reused = list(STAGES[:stage_index])
            stop_reason: str | None = None
            normalized_records: list[dict[str, Any]] | None = None
            normalized_summary: dict[str, Any] | None = None
            decision_payload: dict[str, Any] | None = None
            plan: dict[str, Any] | None = None
            encoded_records: list[dict[str, Any]] | None = None

            for index, stage in enumerate(STAGES):
                if index < stage_index:
                    continue
                files = self._stage_files(scale, stage)
                if stage == "normalize":
                    normalized_records, normalized_summary = normalize_samples(
                        scale_data.records, self.config, scale
                    )
                    write_jsonl_atomic(files["normalized"], normalized_records)
                    write_json_atomic(files["summary"], normalized_summary, pretty=True)
                    self._publish_stage(
                        scale,
                        stage,
                        scale_data.declaration,
                        scale_data.sample_input_fingerprint,
                        [files["normalized"], files["summary"]],
                    )
                elif stage == "decision":
                    if normalized_records is None:
                        normalized_records = read_jsonl(files_and_upstream(self, scale, "normalize")["normalized"])
                    if normalized_summary is None:
                        normalized_summary = read_json(files_and_upstream(self, scale, "normalize")["summary"])
                    decision_payload = select_algorithm_plan(
                        self.config, scale_data.declaration, normalized_summary
                    )
                    plan = decision_payload["selected"]
                    write_json_atomic(files["decision"], decision_payload, pretty=True)
                    self._publish_stage(
                        scale,
                        stage,
                        scale_data.declaration,
                        scale_data.sample_input_fingerprint,
                        [files["decision"]],
                    )
                elif stage == "encode":
                    if normalized_records is None:
                        normalized_records = read_jsonl(files_and_upstream(self, scale, "normalize")["normalized"])
                    if plan is None:
                        decision_payload = read_json(files_and_upstream(self, scale, "decision")["decision"])
                        plan = decision_payload["selected"]
                    encoded_records, encoding_summary = encode_records(
                        normalized_records, self.config, scale_data.declaration, plan
                    )
                    write_jsonl_atomic(files["encoded"], encoded_records)
                    write_json_atomic(files["summary"], encoding_summary, pretty=True)
                    self._publish_stage(
                        scale,
                        stage,
                        scale_data.declaration,
                        scale_data.sample_input_fingerprint,
                        [files["encoded"], files["summary"]],
                    )
                elif stage == "assemble":
                    if normalized_records is None:
                        normalized_records = read_jsonl(files_and_upstream(self, scale, "normalize")["normalized"])
                    if encoded_records is None:
                        encoded_records = read_jsonl(files_and_upstream(self, scale, "encode")["encoded"])
                    if plan is None:
                        decision_payload = read_json(files_and_upstream(self, scale, "decision")["decision"])
                        plan = decision_payload["selected"]
                    samples, discards, assembly_summary, stats = assemble_samples(
                        normalized_records,
                        encoded_records,
                        self.config,
                        scale_data.declaration,
                        plan,
                    )
                    ledger = budget_ledger(
                        self.config,
                        scale_data.declaration,
                        plan,
                        normalized_count=len(normalized_records),
                        assembled_count=len(samples),
                        discarded_count=len(discards),
                    )
                    write_jsonl_atomic(files["samples"], samples)
                    write_jsonl_atomic(files["discarded"], discards)
                    write_json_atomic(files["stats"], stats, pretty=True)
                    write_json_atomic(files["summary"], assembly_summary, pretty=True)
                    write_json_atomic(files["budget"], ledger, pretty=True)
                    self._publish_stage(
                        scale,
                        stage,
                        scale_data.declaration,
                        scale_data.sample_input_fingerprint,
                        [
                            files["samples"],
                            files["discarded"],
                            files["stats"],
                            files["summary"],
                            files["budget"],
                        ],
                    )
                executed.append(stage)
                if stage == stop_after_stage:
                    self.stop_controller.consume(scale)
                    stop_reason = "stop_after_stage"
                else:
                    request = self.stop_controller.consume(scale)
                    if request is not None:
                        stop_reason = "stop_request"
                if stop_reason is not None:
                    break

            after = self.status(scale)
            return {
                "state": "stopped" if stop_reason else "completed",
                "scale": scale,
                "stop_reason": stop_reason,
                "stages_executed": executed,
                "stages_reused": reused,
                "invalidation": invalidation,
                "status": after,
            }

    def resume(self, scale: str, *, stop_after_stage: str | None = None) -> dict[str, Any]:
        return self.run(scale, stop_after_stage=stop_after_stage)

    def run_all(self, *, stop_after_stage: str | None = None) -> dict[str, Any]:
        self.repository.import_all()
        results: list[dict[str, Any]] = []
        stopped = False
        for scale in SCALES:
            result = self.run(scale, stop_after_stage=stop_after_stage)
            results.append(result)
            if result["state"] == "stopped":
                stopped = True
                break
        return {
            "state": "stopped" if stopped else "completed",
            "results": results,
        }

    def request_stop(self, scope: str) -> dict[str, Any]:
        return self.stop_controller.request(scope)

    def _ensure_stage_complete(self, scale: str, stage: str) -> dict[str, Any]:
        status = self.status(scale)
        stage_status = next(item for item in status["stages"] if item["stage"] == stage)
        if stage_status["state"] != "valid":
            raise PipelineError(
                E_NOT_FOUND,
                "requested artifact is not available because its stage is not valid",
                scale=scale,
                stage=stage,
                stage_state=stage_status["state"],
                earliest_invalid_stage=status["earliest_invalid_stage"],
            )
        return status

    def get_decision(self, scale: str) -> dict[str, Any]:
        self._ensure_stage_complete(scale, "decision")
        return read_json(self._stage_files(scale, "decision")["decision"])

    def get_encoding_summary(self, scale: str) -> dict[str, Any]:
        self._ensure_stage_complete(scale, "encode")
        return read_json(self._stage_files(scale, "encode")["summary"])

    def get_budget(self, scale: str) -> dict[str, Any]:
        self._ensure_stage_complete(scale, "decision")
        decision = self.get_decision(scale)
        ledger_path = self._stage_files(scale, "assemble")["budget"]
        ledger = read_json(ledger_path) if ledger_path.exists() else None
        return {
            "scale": scale,
            "selected_plan_budget": decision["budget"],
            "actual_ledger": ledger,
        }

    def get_fingerprints(self, scale: str) -> dict[str, Any]:
        status = self.status(scale)
        return {
            "scale": scale,
            "config_version": status["config_version"],
            "stage_version_fingerprints": status["stage_version_fingerprints"],
            "stages": [
                {
                    "stage": item["stage"],
                    "state": item["state"],
                    "stored_fingerprints": item["stored_fingerprints"],
                    "expected_fingerprints": item["expected_fingerprints"],
                    "reasons": item["reasons"],
                }
                for item in status["stages"]
            ],
            "earliest_invalid_stage": status["earliest_invalid_stage"],
            "invalidated_stages": status["invalidated_stages"],
            "invalidation_reasons": {
                item["stage"]: item["reasons"]
                for item in status["stages"]
                if item["state"] != "valid"
            },
        }

    def export(self, scale: str, kind: str) -> dict[str, Any]:
        if kind == "all":
            self._ensure_stage_complete(scale, "assemble")
            samples = self._stage_files(scale, "assemble")["samples"]
            stats = self._stage_files(scale, "assemble")["stats"]
            return {
                "scale": scale,
                "exports": {
                    "samples": self._file_info(samples),
                    "stats": self._file_info(stats),
                },
            }
        mapping = {
            "samples": ("assemble", "samples", "application/x-ndjson"),
            "discarded": ("assemble", "discarded", "application/x-ndjson"),
            "stats": ("assemble", "stats", "application/json"),
            "assembly_summary": ("assemble", "summary", "application/json"),
            "budget": ("assemble", "budget", "application/json"),
            "normalized": ("normalize", "normalized", "application/x-ndjson"),
            "encoded": ("encode", "encoded", "application/x-ndjson"),
            "encoding_summary": ("encode", "summary", "application/json"),
            "decision": ("decision", "decision", "application/json"),
        }
        if kind not in mapping:
            raise PipelineError(
                E_NOT_FOUND,
                "unknown export kind",
                kind=kind,
                valid_kinds=sorted(mapping) + ["all"],
            )
        stage, file_key, content_type = mapping[kind]
        self._ensure_stage_complete(scale, stage)
        path = self._stage_files(scale, stage)[file_key]
        info = self._file_info(path)
        info["content_type"] = content_type
        info["scale"] = scale
        info["kind"] = kind
        return info

    def _file_info(self, path: Path) -> dict[str, Any]:
        return {
            "path": str(path),
            "sha256": sha256_file(path),
            "size": path.stat().st_size,
            "content_type": "application/octet-stream",
        }


def files_and_upstream(manager: PipelineManager, scale: str, stage: str) -> dict[str, Path]:
    return manager._stage_files(scale, stage)
