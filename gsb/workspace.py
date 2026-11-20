"""Persistent stage registry, invalidation and publish-time concurrency guards."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from .config import CONFIG_STAGE_FIELDS, Config, DEFAULT_CONFIG
from .data import Dataset
from .errors import WorkbenchError
from .util import (
    FileLock,
    atomic_write_bytes,
    atomic_write_json,
    canonical_json_bytes,
    ensure_within,
    read_json,
    sha256_bytes,
    sha256_json,
    stage_index,
)


STAGES = ("binning", "boosting", "calibration", "diagnostics")
STAGE_ORDER = {stage: index for index, stage in enumerate(STAGES)}


def _default_registry() -> dict[str, Any]:
    return {
        "schema": 1,
        "seq": 0,
        "dataset": None,
        "config": DEFAULT_CONFIG.to_dict(),
        "config_version": DEFAULT_CONFIG.version,
        "stages": {},
        "invalidated": {},
        "last_run": {"status": "empty", "details": {}},
    }


def _record_summary(record: dict[str, Any] | None) -> dict[str, Any] | None:
    if record is None:
        return None
    return {
        "stage": record.get("stage"),
        "seq": record.get("seq"),
        "input_fingerprint": record.get("input_fingerprint"),
        "upstream_fingerprint": record.get("upstream_fingerprint"),
        "config_version": record.get("config_version"),
        "content_fingerprint": record.get("content_fingerprint"),
        "version_fingerprint": record.get("version_fingerprint"),
        "payload_sha256": record.get("payload_sha256"),
        "accumulator": record.get("accumulator", {}),
    }


class Workspace:
    def __init__(self, project_root: Path, *, state_dir: Path | None = None, data_dir: Path | None = None):
        self.project_root = Path(project_root).resolve()
        self.data_dir = ensure_within(
            self.project_root,
            Path(data_dir) if data_dir is not None else self.project_root / "data",
            label="data_dir",
        )
        self.state_dir = ensure_within(
            self.project_root,
            Path(state_dir) if state_dir is not None else self.project_root / "state",
            label="state_dir",
        )
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir = self.state_dir / "artifacts"
        self.checkpoints_dir = self.state_dir / "checkpoints"
        self.progress_dir = self.state_dir / "progress"
        for directory in (self.artifacts_dir, self.checkpoints_dir, self.progress_dir):
            directory.mkdir(parents=True, exist_ok=True)
        self.registry_path = self.state_dir / "registry.json"
        self.lock_path = self.state_dir / ".lock"
        self.build_lock_path = self.state_dir / ".build.lock"
        self.stop_path = self.state_dir / "stop.request"
        self._ensure_registry()

    def _ensure_registry(self) -> None:
        with FileLock(self.lock_path):
            if not self.registry_path.exists():
                atomic_write_json(self.registry_path, _default_registry(), pretty=True)

    @property
    def config(self) -> Config:
        return Config.from_dict(self._read_registry()["config"])

    def _read_registry(self) -> dict[str, Any]:
        if not self.registry_path.exists():
            return _default_registry()
        return read_json(self.registry_path)

    def _write_registry(self, registry: dict[str, Any]) -> None:
        atomic_write_json(self.registry_path, registry, pretty=True)

    def build_lock(self) -> FileLock:
        return FileLock(self.build_lock_path)

    def _relative_project_path(self, path: Path) -> str:
        resolved = ensure_within(self.project_root, path)
        return os.path.relpath(resolved, self.project_root).replace(os.sep, "/")

    def _data_change_reason(self, old_dataset: dict[str, Any] | None, dataset: Dataset) -> list[dict[str, Any]]:
        reasons: list[dict[str, Any]] = []
        if old_dataset is None:
            reasons.append({"stage": "binning", "code": "dataset_imported", "detail": dataset.key})
            return reasons
        if old_dataset.get("data_fingerprint") != dataset.data_fingerprint:
            reasons.append(
                {
                    "stage": "binning",
                    "code": "sample_content_changed",
                    "previous": old_dataset.get("data_fingerprint"),
                    "current": dataset.data_fingerprint,
                }
            )
        if old_dataset.get("key") != dataset.key:
            reasons.append(
                {
                    "stage": "binning",
                    "code": "dataset_key_changed",
                    "previous": old_dataset.get("key"),
                    "current": dataset.key,
                }
            )
        return reasons

    def import_dataset(self, dataset: Dataset) -> dict[str, Any]:
        with FileLock(self.lock_path):
            registry = self._read_registry()
            old_dataset = registry.get("dataset")
            reasons = self._data_change_reason(old_dataset, dataset)
            registry["dataset"] = {
                "key": dataset.key,
                "npz_path": self._relative_project_path(dataset.npz_path),
                "manifest_path": self._relative_project_path(dataset.manifest_path),
                "manifest_sha256": dataset.manifest_sha256,
                "data_fingerprint": dataset.data_fingerprint,
                "rows": dataset.rows,
                "cols": dataset.cols,
                "missing_columns": list(dataset.manifest.get("missing_columns", [])),
                "positive_rate": float(dataset.manifest.get("positive_rate", 0.0)),
            }
            if reasons:
                self._invalidate_from_unlocked(registry, "binning", reasons)
                registry["last_run"] = {"status": "invalidated", "details": {"reasons": reasons}}
            self._write_registry(registry)
            return self.status_from_registry(registry)

    def set_config(self, patch: dict[str, Any]) -> dict[str, Any]:
        with FileLock(self.lock_path):
            registry = self._read_registry()
            old_config = Config.from_dict(registry["config"])
            new_config = old_config.with_patch(patch)
            changes: list[dict[str, Any]] = []
            earliest_index = len(STAGES)
            for field, current in new_config.to_dict().items():
                previous = getattr(old_config, field)
                if previous == current:
                    continue
                affected_stage = CONFIG_STAGE_FIELDS.get(field)
                if affected_stage is None:
                    continue
                earliest_index = min(earliest_index, STAGE_ORDER[affected_stage])
                changes.append(
                    {
                        "stage": affected_stage,
                        "code": "config_changed",
                        "field": field,
                        "previous": previous,
                        "current": current,
                    }
                )
            registry["config"] = new_config.to_dict()
            registry["config_version"] = new_config.version
            if changes:
                earliest_stage = STAGES[earliest_index]
                self._invalidate_from_unlocked(registry, earliest_stage, changes)
                registry["last_run"] = {"status": "invalidated", "details": {"reasons": changes}}
            self._write_registry(registry)
            return {
                "config": new_config.to_dict(),
                "config_version": new_config.version,
                "changes": changes,
                "earliest_invalid_stage": STAGES[earliest_index] if changes else None,
                "status": self.status_from_registry(registry),
            }

    def _invalidate_from_unlocked(
        self,
        registry: dict[str, Any],
        earliest_stage: str,
        reasons: list[dict[str, Any]],
    ) -> None:
        earliest = stage_index(earliest_stage)
        for stage in STAGES[earliest:]:
            prior = registry["stages"].pop(stage, None)
            existing = registry["invalidated"].get(stage, {})
            existing_reasons = list(existing.get("reasons", []))
            existing_reasons.extend(reason for reason in reasons if STAGE_ORDER[reason["stage"]] <= STAGE_ORDER[stage])
            prior_record = existing.get("prior_record") or _record_summary(prior)
            if prior_record is not None or existing_reasons:
                registry["invalidated"][stage] = {
                    "reasons": existing_reasons,
                    "prior_record": prior_record,
                }

    def invalidate_from(self, earliest_stage: str, reason: dict[str, Any]) -> dict[str, Any]:
        with FileLock(self.lock_path):
            registry = self._read_registry()
            self._invalidate_from_unlocked(registry, earliest_stage, [reason])
            registry["last_run"] = {"status": "invalidated", "details": {"reasons": [reason]}}
            self._write_registry(registry)
            return self.status_from_registry(registry)

    def _write_payload(self, stage: str, payload: dict[str, Any]) -> tuple[str, str, str]:
        stage_index(stage)
        data = canonical_json_bytes(payload)
        content_fingerprint = sha256_bytes(data)
        payload_path = self.artifacts_dir / f"{stage}-{content_fingerprint}.json"
        if payload_path.exists() and sha256_bytes(payload_path.read_bytes()) != content_fingerprint:
            raise WorkbenchError(
                "E_STALE_CHECKPOINT",
                "content-addressed stage payload is corrupt",
                {"stage": stage, "path": payload_path.name},
            )
        if not payload_path.exists():
            atomic_write_bytes(payload_path, data)
        relative = f"artifacts/{payload_path.name}"
        return relative, content_fingerprint, sha256_bytes(data)

    def publish_checkpoint(
        self,
        *,
        stage: str,
        input_fingerprint: str,
        upstream_fingerprint: str | None,
        config_version: str,
        payload: dict[str, Any],
        accumulator: dict[str, Any],
        expected_seq: int | None = None,
    ) -> dict[str, Any]:
        stage_index(stage)
        payload_path, content_fingerprint, payload_sha = self._write_payload(stage, payload)
        with FileLock(self.lock_path):
            registry = self._read_registry()
            if registry.get("dataset") is None:
                raise WorkbenchError("E_INVALID_STAGE", "a dataset must be imported before publishing", {"stage": stage})
            next_seq = int(registry.get("seq", 0)) + 1
            if expected_seq is not None and int(expected_seq) != next_seq:
                raise WorkbenchError(
                    "E_STALE_CHECKPOINT",
                    "checkpoint sequence is stale or out of order",
                    {"stage": stage, "expected_seq": int(expected_seq), "required_seq": next_seq},
                )
            if stage in registry["stages"]:
                current = registry["stages"][stage]
                raise WorkbenchError(
                    "E_STALE_CHECKPOINT",
                    "stage already has a valid checkpoint; invalidate before republishing",
                    {
                        "stage": stage,
                        "current_seq": current.get("seq"),
                        "required_seq": next_seq,
                        "current_version_fingerprint": current.get("version_fingerprint"),
                    },
                )
            order = STAGE_ORDER[stage]
            for downstream in STAGES[order + 1 :]:
                if downstream in registry["stages"]:
                    raise WorkbenchError(
                        "E_STALE_CHECKPOINT",
                        "cross-stage publication would leave a valid downstream checkpoint stale",
                        {"stage": stage, "valid_downstream_stage": downstream},
                    )
            if order > 0:
                prerequisite = STAGES[order - 1]
                prerequisite_record = registry["stages"].get(prerequisite)
                if prerequisite_record is None:
                    raise WorkbenchError(
                        "E_INVALID_STAGE",
                        "prerequisite stage checkpoint is missing",
                        {"stage": stage, "missing_prerequisite": prerequisite},
                    )
                if upstream_fingerprint != prerequisite_record.get("version_fingerprint"):
                    raise WorkbenchError(
                        "E_STALE_CHECKPOINT",
                        "cross-stage upstream fingerprint mismatch",
                        {
                            "stage": stage,
                            "prerequisite": prerequisite,
                            "expected_upstream_fingerprint": prerequisite_record.get("version_fingerprint"),
                            "provided_upstream_fingerprint": upstream_fingerprint,
                        },
                    )
            else:
                upstream_fingerprint = None
            record = {
                "schema": 1,
                "stage": stage,
                "seq": next_seq,
                "input_fingerprint": input_fingerprint,
                "upstream_fingerprint": upstream_fingerprint,
                "config_version": config_version,
                "content_fingerprint": content_fingerprint,
                "payload": payload_path,
                "payload_sha256": payload_sha,
                "accumulator": accumulator,
                "version_fingerprint": sha256_json(
                    {
                        "stage": stage,
                        "input_fingerprint": input_fingerprint,
                        "upstream_fingerprint": upstream_fingerprint,
                        "config_version": config_version,
                        "content_fingerprint": content_fingerprint,
                        "schema": 1,
                    }
                ),
            }
            checkpoint_path = self.checkpoints_dir / f"{next_seq:08d}-{stage}.json"
            atomic_write_json(checkpoint_path, record, pretty=True)
            registry["stages"][stage] = record
            registry["invalidated"].pop(stage, None)
            registry["seq"] = next_seq
            self._write_registry(registry)
            return record

    def current_record(self, stage: str) -> dict[str, Any] | None:
        stage_index(stage)
        return self._read_registry()["stages"].get(stage)

    def require_record(self, stage: str, *, required_by: str | None = None) -> dict[str, Any]:
        stage_index(stage)
        registry = self._read_registry()
        record = registry["stages"].get(stage)
        if record is None:
            raise WorkbenchError(
                "E_INVALID_STAGE",
                "required stage checkpoint is missing",
                {"stage": stage, "required_by": required_by or stage},
            )
        return record

    def read_stage_payload(self, stage: str) -> tuple[dict[str, Any], dict[str, Any]]:
        record = self.require_record(stage)
        relative = record["payload"]
        payload_path = ensure_within(self.state_dir, self.state_dir / relative, label="checkpoint payload")
        if not payload_path.exists():
            raise WorkbenchError(
                "E_INVALID_STAGE",
                "stage payload file is missing",
                {"stage": stage, "payload": relative},
            )
        data = payload_path.read_bytes()
        digest = sha256_bytes(data)
        if digest != record.get("payload_sha256") or digest != record.get("content_fingerprint"):
            raise WorkbenchError(
                "E_INVALID_STAGE",
                "stage payload digest does not match checkpoint",
                {"stage": stage, "expected": record.get("payload_sha256"), "actual": digest},
            )
        try:
            payload = read_json(payload_path)
        except Exception as exc:  # pragma: no cover - corrupt local checkpoint
            raise WorkbenchError(
                "E_INVALID_STAGE",
                "stage payload is not valid JSON",
                {"stage": stage, "payload": relative},
            ) from exc
        return record, payload

    def record_matches(self, stage: str, *, input_fingerprint: str, upstream_fingerprint: str | None) -> bool:
        record = self.current_record(stage)
        return bool(
            record
            and record.get("input_fingerprint") == input_fingerprint
            and record.get("upstream_fingerprint") == upstream_fingerprint
        )

    def progress_path(self, stage: str) -> Path:
        stage_index(stage)
        return self.progress_dir / f"{stage}.json"

    def read_progress(self, stage: str) -> dict[str, Any] | None:
        path = self.progress_path(stage)
        if not path.exists():
            return None
        return read_json(path)

    def write_progress(self, stage: str, payload: dict[str, Any]) -> None:
        atomic_write_json(self.progress_path(stage), payload)

    def clear_progress(self, stage: str) -> None:
        try:
            self.progress_path(stage).unlink()
        except FileNotFoundError:
            pass

    def request_stop(self, reason: str = "user_requested") -> None:
        atomic_write_json(self.stop_path, {"requested": True, "reason": reason})
        with FileLock(self.lock_path):
            registry = self._read_registry()
            registry["last_run"] = {
                "status": "stop_requested",
                "details": {"reason": reason, "scope": "finish_current_boosting_round"},
            }
            self._write_registry(registry)

    def clear_stop(self) -> None:
        try:
            self.stop_path.unlink()
        except FileNotFoundError:
            pass

    def stop_requested(self) -> bool:
        return self.stop_path.exists()

    def set_run_status(self, status: str, details: dict[str, Any] | None = None) -> None:
        with FileLock(self.lock_path):
            registry = self._read_registry()
            registry["last_run"] = {"status": status, "details": details or {}}
            self._write_registry(registry)

    def status(self) -> dict[str, Any]:
        return self.status_from_registry(self._read_registry())

    def status_from_registry(self, registry: dict[str, Any]) -> dict[str, Any]:
        stage_status: dict[str, Any] = {}
        earliest_missing: str | None = None
        earliest_invalid: str | None = None
        for stage in STAGES:
            record = registry["stages"].get(stage)
            if record is not None:
                stage_status[stage] = {
                    "status": "valid",
                    **_record_summary(record),
                    "payload": record.get("payload"),
                }
                continue
            invalidated = registry["invalidated"].get(stage)
            if invalidated is not None:
                stage_status[stage] = {
                    "status": "invalid",
                    **(_record_summary(invalidated.get("prior_record")) or {}),
                    "reasons": invalidated.get("reasons", []),
                }
                if earliest_invalid is None:
                    earliest_invalid = stage
            else:
                stage_status[stage] = {"status": "missing"}
                if earliest_missing is None:
                    earliest_missing = stage
            progress = self.read_progress(stage)
            if progress is not None:
                stage_status[stage]["progress"] = {
                    "input_fingerprint": progress.get("input_fingerprint"),
                    "rounds_done": progress.get("rounds_done"),
                    "rounds_total": progress.get("rounds_total"),
                    "fold_index": progress.get("fold_index"),
                    "fold_count": progress.get("fold_count"),
                }
        all_valid = all(stage_status[stage]["status"] == "valid" for stage in STAGES)
        if all_valid:
            overall = "complete"
        elif self.stop_requested():
            overall = "stopped"
        elif earliest_invalid:
            overall = "invalid"
        elif earliest_missing:
            overall = "partial" if any(stage_status[stage]["status"] == "valid" for stage in STAGES) else "empty"
        else:
            overall = "unknown"
        return {
            "schema": 1,
            "overall": overall,
            "dataset": registry.get("dataset"),
            "config": Config.from_dict(registry["config"]).to_dict(),
            "config_version": registry.get("config_version"),
            "seq": int(registry.get("seq", 0)),
            "stages": stage_status,
            "earliest_invalid_stage": earliest_invalid,
            "earliest_missing_stage": earliest_missing,
            "stop_requested": self.stop_requested(),
            "last_run": registry.get("last_run", {}),
            "fingerprints": {
                stage: {
                    "input": stage_status[stage].get("input_fingerprint"),
                    "upstream": stage_status[stage].get("upstream_fingerprint"),
                    "config_version": stage_status[stage].get("config_version"),
                    "content": stage_status[stage].get("content_fingerprint"),
                    "version": stage_status[stage].get("version_fingerprint"),
                }
                for stage in STAGES
            },
        }

    def reset_stage_progress(self, stage: str) -> None:
        self.clear_progress(stage)
