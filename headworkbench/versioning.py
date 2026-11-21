from __future__ import annotations

import fcntl
import os
import re
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .canonical import digest_object, read_json, write_json_file
from .errors import WorkbenchError, stale_version
from .pipeline import Pipeline, compare_runs


RECORD_PATTERN = re.compile(r"^(\d{12})\.json$")
HEAD_SCHEMA = "headworkbench.head/v1"
RECORD_SCHEMA = "headworkbench.version-record/v1"


def width_fingerprint(head_widths: dict[str, int]) -> str:
    normalized = {str(key): int(value) for key, value in sorted(head_widths.items(), key=lambda item: int(item[0]))}
    return digest_object({"schema": "headworkbench.head-widths/v1", "head_widths": normalized})


class VersionStore:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.workbench_dir = self.root / ".workbench"
        self.versions_dir = self.workbench_dir / "versions"
        self.checkpoints_dir = self.workbench_dir / "checkpoints"
        self.lock_path = self.workbench_dir / "version.lock"
        self.head_path = self.workbench_dir / "HEAD.json"

    def ensure_layout(self) -> dict[str, Any]:
        for directory in (self.workbench_dir, self.versions_dir, self.checkpoints_dir):
            directory.mkdir(parents=True, exist_ok=True)
        writable = os.access(self.workbench_dir, os.W_OK) and os.access(self.checkpoints_dir, os.W_OK)
        if not writable:
            raise WorkbenchError(
                "E_WORKSPACE_NOT_WRITABLE",
                "workspace checkpoint and lock directories are not writable",
                status_code=500,
                details={"workspace": str(self.root)},
            )
        descriptor, probe_name = tempfile.mkstemp(prefix=".write-probe-", dir=self.checkpoints_dir)
        try:
            os.write(descriptor, b"ok")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
            Path(probe_name).unlink(missing_ok=True)
        with self._file_lock(exclusive=True):
            pass
        return {
            "workspace_writable": True,
            "file_lock_available": True,
            "checkpoint_directory": str(self.checkpoints_dir.relative_to(self.root)),
            "versions_directory": str(self.versions_dir.relative_to(self.root)),
        }

    @contextmanager
    def _file_lock(self, *, exclusive: bool) -> Iterator[None]:
        self.workbench_dir.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            yield
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)

    def record_path(self, version: int) -> Path:
        return self.versions_dir / f"{version:012d}.json"

    def _record_versions(self) -> list[int]:
        if not self.versions_dir.exists():
            return []
        versions: list[int] = []
        for path in self.versions_dir.iterdir():
            match = RECORD_PATTERN.match(path.name)
            if match:
                versions.append(int(match.group(1)))
        return sorted(versions)

    def _read_record_file(self, version: int) -> dict[str, Any]:
        path = self.record_path(version)
        if not path.is_file():
            raise stale_version(
                f"version record {version} is missing",
                cause="record_tampering",
                version=version,
            )
        try:
            payload = read_json(path)
        except Exception as exc:
            raise stale_version(
                "version record is not valid JSON",
                cause="record_tampering",
                version=version,
                parse_error=str(exc),
            ) from exc
        self._validate_record(payload, expected_version=version)
        return payload

    def _validate_record(self, record: dict[str, Any], *, expected_version: int | None = None) -> None:
        if record.get("schema") != RECORD_SCHEMA:
            raise stale_version("version record schema is not recognized", cause="record_tampering")
        version = record.get("version")
        if not isinstance(version, int) or version < 1:
            raise stale_version("version record has an invalid version number", cause="record_tampering")
        if expected_version is not None and version != expected_version:
            raise stale_version(
                "version record number does not match its filename",
                cause="record_tampering",
                filename_version=expected_version,
                record_version=version,
            )
        recorded_digest = record.get("record_digest")
        content = dict(record)
        content.pop("record_digest", None)
        actual_digest = digest_object(content)
        if recorded_digest != actual_digest:
            raise stale_version(
                "version record digest mismatch",
                cause="record_tampering",
                version=version,
                expected=recorded_digest,
                actual=actual_digest,
            )
        attempted = record.get("attempted_head_widths", record.get("head_widths"))
        effective = record.get("head_widths")
        if not isinstance(attempted, dict) or not isinstance(effective, dict):
            raise stale_version("version record has no width fingerprints", cause="record_tampering", version=version)
        if record.get("width_fingerprint") != width_fingerprint(attempted):
            raise stale_version(
                "attempted per-head width fingerprint changed",
                cause="record_tampering",
                version=version,
            )
        if record.get("effective_width_fingerprint") != width_fingerprint(effective):
            raise stale_version(
                "effective per-head width fingerprint changed",
                cause="record_tampering",
                version=version,
            )
        parent_version = record.get("parent_version")
        if version == 1 and parent_version not in (None, 0):
            raise stale_version("first version has an invalid parent", cause="record_tampering", version=version)
        if version > 1 and parent_version != version - 1:
            raise stale_version(
                "version chain parent is not contiguous",
                cause="record_tampering",
                version=version,
                parent_version=parent_version,
            )

    def audit_locked(self) -> dict[str, Any]:
        versions = self._record_versions()
        if versions and versions != list(range(1, versions[-1] + 1)):
            missing = sorted(set(range(1, versions[-1] + 1)).difference(versions))
            raise stale_version(
                "version history has a gap",
                cause="record_tampering",
                missing_versions=missing,
            )
        records: dict[int, dict[str, Any]] = {}
        previous_digest: str | None = None
        for version in versions:
            record = self._read_record_file(version)
            if version > 1 and record.get("parent_record_digest") != previous_digest:
                raise stale_version(
                    "version parent digest does not match the previous record",
                    cause="record_tampering",
                    version=version,
                )
            previous_digest = record["record_digest"]
            records[version] = record

        if not self.head_path.exists():
            if versions:
                raise stale_version(
                    "HEAD is missing while version records exist",
                    cause="missing_head",
                    highest_version=versions[-1],
                    recoverable=True,
                )
            return {"head": None, "records": [], "highest_version": 0}
        try:
            head = read_json(self.head_path)
        except Exception as exc:
            raise stale_version(
                "HEAD is not valid JSON",
                cause="record_tampering",
                parse_error=str(exc),
            ) from exc
        if head.get("schema") != HEAD_SCHEMA:
            raise stale_version("HEAD schema is not recognized", cause="record_tampering")
        head_digest = head.get("head_digest")
        head_content = dict(head)
        head_content.pop("head_digest", None)
        if head_digest != digest_object(head_content):
            raise stale_version("HEAD digest mismatch", cause="record_tampering")
        head_version = head.get("version")
        if not isinstance(head_version, int) or head_version < 1:
            raise stale_version("HEAD version is invalid", cause="record_tampering")
        if versions and head_version < versions[-1]:
            raise stale_version(
                "HEAD version regressed below the newest immutable record",
                cause="version_rollback",
                head_version=head_version,
                highest_version=versions[-1],
            )
        if head_version > (versions[-1] if versions else 0):
            raise stale_version(
                "HEAD points to a version record that does not exist",
                cause="record_tampering",
                head_version=head_version,
            )
        current = records.get(head_version)
        if current is None:
            raise stale_version("HEAD record is missing", cause="record_tampering", version=head_version)
        if head.get("record_digest") != current["record_digest"]:
            raise stale_version(
                "HEAD record digest does not match the immutable record",
                cause="record_tampering",
                version=head_version,
            )
        if head.get("width_fingerprint") != current.get("effective_width_fingerprint"):
            raise stale_version(
                "HEAD width fingerprint does not match the current record",
                cause="record_tampering",
                version=head_version,
            )
        return {
            "head": head,
            "current_record": current,
            "records": [records[version] for version in versions],
            "highest_version": versions[-1] if versions else 0,
        }

    def current(self) -> dict[str, Any]:
        with self._file_lock(exclusive=False):
            audit = self.audit_locked()
        return audit.get("current_record")

    def history(self) -> dict[str, Any]:
        with self._file_lock(exclusive=False):
            audit = self.audit_locked()
        records = audit["records"]
        return {
            "current_version": audit["current_record"]["version"] if audit.get("current_record") else None,
            "highest_version": audit["highest_version"],
            "versions": [self._record_summary(record) for record in records],
            "rollback_records": [
                self._record_summary(record)
                for record in records
                if record.get("status") in {"rolled_back", "manual_rollback"}
            ],
        }

    def rollback_records(self) -> dict[str, Any]:
        history = self.history()
        return {
            "current_version": history["current_version"],
            "records": history["rollback_records"],
        }

    def get_record(self, version: int) -> dict[str, Any]:
        with self._file_lock(exclusive=False):
            audit = self.audit_locked()
        for record in audit["records"]:
            if record["version"] == version:
                return record
        raise WorkbenchError(
            "E_TARGET_VERSION",
            "version record does not exist",
            status_code=404,
            details={"version": version},
        )

    @staticmethod
    def _record_summary(record: dict[str, Any]) -> dict[str, Any]:
        return {
            "version": record["version"],
            "status": record["status"],
            "head_widths": record["head_widths"],
            "attempted_head_widths": record.get("attempted_head_widths", record["head_widths"]),
            "width_fingerprint": record["width_fingerprint"],
            "effective_width_fingerprint": record["effective_width_fingerprint"],
            "sampling_window_digest": record["sampling_window_digest"],
            "config_version": record["config_version"],
            "adjudication_digest": record["adjudication_digest"],
            "record_digest": record["record_digest"],
            "parent_version": record.get("parent_version"),
            "rollback": record.get("rollback"),
            "validation": record.get("validation"),
            "note": record.get("note"),
        }

    def _read_head_or_zero(self) -> dict[str, Any] | None:
        if not self.head_path.exists():
            return None
        return read_json(self.head_path)

    def evaluate_composition(
        self,
        result: dict[str, Any],
        head_widths: dict[str, int],
        *,
        adjudication_digest: str | None,
    ) -> dict[str, Any]:
        payload = result["payload"]
        heads = payload["adjudication"]["heads_by_id"]
        expected_ids = [str(head["head_id"]) for head in heads]
        supplied_ids = list(head_widths.keys())
        errors: list[dict[str, Any]] = []
        if sorted(supplied_ids, key=int) != sorted(expected_ids, key=int):
            errors.append(
                {
                    "code": "head_set_mismatch",
                    "expected": sorted(expected_ids, key=int),
                    "actual": sorted(supplied_ids, key=int),
                }
            )
        valid_bits = set(payload["allocation"]["candidate_bitwidths"])
        per_head = {str(head["head_id"]): head for head in heads}
        total_error = 0.0
        head_estimates: dict[str, float] = {}
        tolerance_failures: list[str] = []
        for head_id in expected_ids:
            if head_id not in head_widths:
                continue
            bits = int(head_widths[head_id])
            if bits not in valid_bits:
                errors.append(
                    {
                        "code": "invalid_bitwidth",
                        "head_id": int(head_id),
                        "bits": bits,
                        "candidate_bitwidths": sorted(valid_bits),
                    }
                )
                continue
            error = float(per_head[head_id]["error_upper_bounds_by_width"][str(bits)])
            head_estimates[head_id] = error
            total_error += error
            if error > payload["allocation"]["per_head_error_tolerance"]:
                tolerance_failures.append(head_id)
        actual_adjudication_digest = payload["stages"]["adjudication"]["digest"]
        if adjudication_digest is not None and adjudication_digest != actual_adjudication_digest:
            errors.append(
                {
                    "code": "adjudication_digest_mismatch",
                    "expected": actual_adjudication_digest,
                    "actual": adjudication_digest,
                }
            )
        if total_error > payload["allocation"]["global_error_budget"]:
            errors.append(
                {
                    "code": "global_budget_exceeded",
                    "total_error_estimate": total_error,
                    "global_error_budget": payload["allocation"]["global_error_budget"],
                }
            )
        if tolerance_failures:
            errors.append(
                {
                    "code": "per_head_tolerance_exceeded",
                    "head_ids": [int(head_id) for head_id in tolerance_failures],
                    "per_head_error_tolerance": payload["allocation"]["per_head_error_tolerance"],
                }
            )
        return {
            "valid": not errors,
            "errors": errors,
            "total_error_estimate": total_error,
            "per_head_error_estimates": head_estimates,
            "adjudication_digest": actual_adjudication_digest,
            "window_name": result["window_name"],
            "window": payload["window"],
            "config_version": payload["config_version"],
            "config_digest": payload["config_digest"],
            "data_digest": payload["data_digest"],
            "pipeline_fingerprint": payload["pipeline_fingerprint"],
            "result_digest": result["result_digest"],
        }

    @staticmethod
    def composition_analysis(result: dict[str, Any], head_widths: dict[str, int]) -> dict[str, Any]:
        payload = result["payload"]
        heads_by_id = {str(head["head_id"]): head for head in payload["adjudication"]["heads_by_id"]}
        allocation_by_id = {str(head["head_id"]): head for head in payload["allocation"]["heads"]}
        budget = float(payload["allocation"]["global_error_budget"])
        tolerance = float(payload["allocation"]["per_head_error_tolerance"])
        total = 0.0
        heads: list[dict[str, Any]] = []
        for head_id in sorted(head_widths, key=int):
            bits = int(head_widths[head_id])
            head = heads_by_id[head_id]
            error = float(head["error_upper_bounds_by_width"][str(bits)])
            total += error
            allocation = allocation_by_id[head_id]
            heads.append(
                {
                    "head_id": int(head_id),
                    "bits": bits,
                    "error_upper_bound": error,
                    "score": float(head["score"]),
                    "score_rank": next(
                        entry["rank"]
                        for entry in payload["adjudication"]["ranking"]
                        if entry["head_id"] == int(head_id)
                    ),
                    "within_per_head_tolerance": error <= tolerance,
                    "per_head_error_tolerance": tolerance,
                    "decision": allocation["decision"],
                    "reason": allocation["reason"],
                }
            )
        return {
            "head_widths": {key: int(value) for key, value in sorted(head_widths.items(), key=lambda item: int(item[0]))},
            "total_error_estimate": total,
            "global_error_budget": budget,
            "budget_remaining": budget - total,
            "within_global_budget": total <= budget,
            "heads": heads,
        }

    def commit(
        self,
        pipeline: Pipeline,
        head_widths: dict[str, int],
        *,
        expected_base_version: int,
        window_name: str,
        adjudication_digest: str | None = None,
        note: str | None = None,
        verify_full: bool = True,
    ) -> dict[str, Any]:
        self.ensure_layout()
        result = pipeline.run(window_name, mode="incremental")
        if verify_full:
            full_result = pipeline.run(window_name, mode="full")
            compare_runs(result, full_result)
        validation = self.evaluate_composition(
            result,
            head_widths,
            adjudication_digest=adjudication_digest,
        )
        with self._file_lock(exclusive=True):
            audit = self.audit_locked()
            current_record = audit.get("current_record")
            current_version = int(current_record["version"]) if current_record else 0
            if expected_base_version != current_version:
                raise stale_version(
                    "expected base version does not match the current version",
                    cause="concurrent_or_duplicate_commit",
                    expected_base_version=expected_base_version,
                    current_version=current_version,
                )
            next_version = current_version + 1
            attempted = {str(key): int(value) for key, value in sorted(head_widths.items(), key=lambda item: int(item[0]))}
            parent_record_digest = current_record["record_digest"] if current_record else None
            if validation["valid"]:
                status = "active"
                effective = dict(attempted)
                rollback = None
                total_error = validation["total_error_estimate"]
                head_estimates = validation["per_head_error_estimates"]
            else:
                if current_record is None:
                    raise WorkbenchError(
                        "E_NO_ACTIVE_VERSION",
                        "the first committed composition is invalid and there is no version to roll back to",
                        status_code=409,
                        details={"validation": validation},
                    )
                status = "rolled_back"
                effective = dict(current_record["head_widths"])
                total_error = float(current_record.get("total_error_estimate", 0.0))
                head_estimates = dict(current_record.get("per_head_error_estimates", {}))
                rollback = {
                    "to_version": current_record["version"],
                    "to_record_digest": current_record["record_digest"],
                    "reason_codes": [item["code"] for item in validation["errors"]],
                    "attempted_validation": validation,
                }
            effective_analysis = self.composition_analysis(result, effective)
            record_payload: dict[str, Any] = {
                "schema": RECORD_SCHEMA,
                "version": next_version,
                "status": status,
                "parent_version": current_version if current_version else None,
                "parent_record_digest": parent_record_digest,
                "head_widths": {str(key): int(value) for key, value in sorted(effective.items(), key=lambda item: int(item[0]))},
                "attempted_head_widths": attempted,
                "width_fingerprint": width_fingerprint(attempted),
                "effective_width_fingerprint": width_fingerprint(effective),
                "sampling_window_name": result["window_name"],
                "sampling_window_digest": result["payload"]["window"]["window_digest"],
                "sampling_window": result["payload"]["window"],
                "config_version": result["payload"]["config_version"],
                "config_digest": result["payload"]["config_digest"],
                "data_digest": result["payload"]["data_digest"],
                "adjudication_digest": result["payload"]["stages"]["adjudication"]["digest"],
                "allocation_digest": result["payload"]["stages"]["allocation"]["digest"],
                "pipeline_fingerprint": result["payload"]["pipeline_fingerprint"],
                "result_digest": result["result_digest"],
                "full_run_verified": verify_full,
                "global_error_budget": result["payload"]["allocation"]["global_error_budget"],
                "total_error_estimate": total_error,
                "per_head_error_estimates": head_estimates,
                "validation": validation,
                "adjudication_summary": {
                    "ranking": result["payload"]["adjudication"]["ranking"],
                    "heads_by_id": result["payload"]["adjudication"]["heads_by_id"],
                    "sampling_insufficient_head_ids": result["payload"]["adjudication"][
                        "sampling_insufficient_head_ids"
                    ],
                    "threshold": result["payload"]["adjudication"]["threshold"],
                },
                "allocation_summary": {
                    "heads": result["payload"]["allocation"]["heads"],
                    "diagnostics": result["payload"]["allocation"]["diagnostics"],
                    "suggested_head_widths": result["payload"]["allocation"]["suggested_head_widths"],
                    "total_error_estimate": result["payload"]["allocation"]["total_error_estimate"],
                    "budget_limited_head_ids": result["payload"]["allocation"]["budget_limited_head_ids"],
                },
                "effective_composition_analysis": effective_analysis,
                "rollback": rollback,
                "note": note,
            }
            record_payload["record_digest"] = digest_object(record_payload)
            record_path = self.record_path(next_version)
            write_json_file(record_path, record_payload)
            head_payload = {
                "schema": HEAD_SCHEMA,
                "version": next_version,
                "record_file": str(record_path.relative_to(self.root)),
                "record_digest": record_payload["record_digest"],
                "width_fingerprint": record_payload["effective_width_fingerprint"],
                "status": status,
            }
            head_payload["head_digest"] = digest_object(head_payload)
            write_json_file(self.head_path, head_payload)
        return {
            "version": next_version,
            "status": status,
            "effective_head_widths": effective,
            "attempted_head_widths": attempted,
            "record_digest": record_payload["record_digest"],
            "rollback": rollback,
            "validation": validation,
            "head_estimate_total": total_error,
        }

    def rollback_to(
        self,
        *,
        target_version: int,
        expected_base_version: int,
        note: str | None = None,
    ) -> dict[str, Any]:
        self.ensure_layout()
        with self._file_lock(exclusive=True):
            audit = self.audit_locked()
            current_record = audit.get("current_record")
            current_version = int(current_record["version"]) if current_record else 0
            if expected_base_version != current_version:
                raise stale_version(
                    "expected base version does not match the current version",
                    cause="concurrent_or_duplicate_commit",
                    expected_base_version=expected_base_version,
                    current_version=current_version,
                )
            if target_version < 1 or target_version > audit["highest_version"]:
                raise WorkbenchError(
                    "E_TARGET_VERSION",
                    "target version does not exist",
                    details={"target_version": target_version},
                )
            target = self._read_record_file(target_version)
            if target.get("status") not in {"active", "manual_rollback"}:
                raise WorkbenchError(
                    "E_TARGET_VERSION",
                    "target version is not an available active composition",
                    details={"target_version": target_version, "status": target.get("status")},
                )
            next_version = current_version + 1
            effective = dict(target["head_widths"])
            record_payload: dict[str, Any] = {
                "schema": RECORD_SCHEMA,
                "version": next_version,
                "status": "manual_rollback",
                "parent_version": current_version if current_version else None,
                "parent_record_digest": current_record["record_digest"] if current_record else None,
                "head_widths": effective,
                "attempted_head_widths": effective,
                "width_fingerprint": width_fingerprint(effective),
                "effective_width_fingerprint": width_fingerprint(effective),
                "sampling_window_name": target["sampling_window_name"],
                "sampling_window_digest": target["sampling_window_digest"],
                "sampling_window": target["sampling_window"],
                "config_version": target["config_version"],
                "config_digest": target["config_digest"],
                "data_digest": target["data_digest"],
                "adjudication_digest": target["adjudication_digest"],
                "allocation_digest": target["allocation_digest"],
                "pipeline_fingerprint": target["pipeline_fingerprint"],
                "result_digest": target["result_digest"],
                "full_run_verified": target.get("full_run_verified", True),
                "global_error_budget": target["global_error_budget"],
                "total_error_estimate": target["total_error_estimate"],
                "per_head_error_estimates": target["per_head_error_estimates"],
                "validation": {"valid": True, "restored_from_version": target_version},
                "adjudication_summary": target.get("adjudication_summary"),
                "allocation_summary": target.get("allocation_summary"),
                "effective_composition_analysis": target.get("effective_composition_analysis"),
                "rollback": {
                    "to_version": target_version,
                    "to_record_digest": target["record_digest"],
                    "reason_codes": ["explicit_rollback"],
                },
                "note": note,
            }
            record_payload["record_digest"] = digest_object(record_payload)
            record_path = self.record_path(next_version)
            write_json_file(record_path, record_payload)
            head_payload = {
                "schema": HEAD_SCHEMA,
                "version": next_version,
                "record_file": str(record_path.relative_to(self.root)),
                "record_digest": record_payload["record_digest"],
                "width_fingerprint": record_payload["effective_width_fingerprint"],
                "status": "manual_rollback",
            }
            head_payload["head_digest"] = digest_object(head_payload)
            write_json_file(self.head_path, head_payload)
        return {
            "version": next_version,
            "status": "manual_rollback",
            "effective_head_widths": effective,
            "rollback": record_payload["rollback"],
            "record_digest": record_payload["record_digest"],
        }

    def recover(self) -> dict[str, Any]:
        self.ensure_layout()
        cleaned: list[str] = []
        with self._file_lock(exclusive=True):
            for directory in (self.versions_dir, self.workbench_dir):
                if directory.exists():
                    for path in directory.iterdir():
                        if path.is_file() and path.name.startswith(".tmp-"):
                            path.unlink(missing_ok=True)
                            cleaned.append(str(path.relative_to(self.root)))
            versions = self._record_versions()
            if not versions:
                return {"recovered": False, "cleaned_temp_files": cleaned, "current_version": None}
            records: dict[int, dict[str, Any]] = {}
            previous_digest = None
            for version in versions:
                record = self._read_record_file(version)
                if version > 1 and record.get("parent_record_digest") != previous_digest:
                    raise stale_version(
                        "cannot recover a broken version chain",
                        cause="record_tampering",
                        version=version,
                    )
                previous_digest = record["record_digest"]
                records[version] = record
            highest = versions[-1]
            target = records[highest]
            head_payload = {
                "schema": HEAD_SCHEMA,
                "version": highest,
                "record_file": str(self.record_path(highest).relative_to(self.root)),
                "record_digest": target["record_digest"],
                "width_fingerprint": target["effective_width_fingerprint"],
                "status": target["status"],
            }
            head_payload["head_digest"] = digest_object(head_payload)
            try:
                current_head = read_json(self.head_path) if self.head_path.exists() else None
            except Exception:
                current_head = None
            changed = current_head != head_payload
            if changed:
                write_json_file(self.head_path, head_payload)
            audit = self.audit_locked()
        return {
            "recovered": changed,
            "cleaned_temp_files": cleaned,
            "current_version": highest,
            "current_status": target["status"],
            "record_digest": target["record_digest"],
            "head": audit["head"],
        }

