"""Atomic, sequence-checked stage checkpoint ledger."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from .config import STAGES
from .errors import WorkbenchError
from .util import (
    atomic_write_bytes,
    canonical_json_bytes,
    digest_object,
    file_lock,
    read_json,
    safe_relative_path,
    sha256_bytes,
    sha256_file,
)

RECORD_REQUIRED_FIELDS = {
    "schema_version",
    "seq",
    "stage",
    "tier",
    "input_fingerprint",
    "config_version",
    "artifact_digest",
    "artifact_path",
    "artifact_size",
    "record_digest",
}


class CheckpointStore:
    def __init__(self, workspace_root: str | os.PathLike[str]) -> None:
        self.workspace_root = Path(workspace_root)
        self.base = self.workspace_root / "checkpoints"
        self.records_dir = self.base / "records"
        self.artifacts_dir = self.base / "artifacts"
        self.quarantine_dir = self.base / "quarantine"
        self.lock_path = self.workspace_root / "locks" / "checkpoints.lock"
        for directory in (self.records_dir, self.artifacts_dir, self.quarantine_dir):
            directory.mkdir(parents=True, exist_ok=True)

    def _record_digest(self, record: dict[str, Any]) -> str:
        body = {key: value for key, value in record.items() if key != "record_digest"}
        return digest_object(body)

    def _validate_record(
        self, path: Path, record: Any
    ) -> tuple[dict[str, Any] | None, str | None]:
        if not isinstance(record, dict):
            return None, "record is not an object"
        missing = RECORD_REQUIRED_FIELDS - set(record)
        if missing:
            return None, f"missing fields: {sorted(missing)}"
        if record.get("schema_version") != 1:
            return None, "unsupported checkpoint schema"
        if not isinstance(record.get("seq"), int) or record["seq"] < 1:
            return None, "invalid sequence"
        if record.get("stage") not in STAGES:
            return None, "invalid stage"
        if not isinstance(record.get("tier"), str) or not record["tier"]:
            return None, "invalid tier"
        if record.get("record_digest") != self._record_digest(record):
            return None, "record digest mismatch"
        expected_name = f"{record['seq']:08d}.json"
        if path.name != expected_name and not path.name.startswith(f"{record['seq']:08d}."):
            return None, "record filename does not match sequence"
        try:
            artifact_path = safe_relative_path(self.workspace_root, str(record["artifact_path"]))
        except Exception:
            return None, "artifact path escapes workspace"
        if not artifact_path.exists():
            return None, "artifact is missing"
        if int(artifact_path.stat().st_size) != int(record.get("artifact_size", -1)):
            return None, "artifact size mismatch"
        if sha256_file(artifact_path) != record.get("artifact_digest"):
            return None, "artifact digest mismatch"
        return record, None

    def _all_record_files(self) -> list[Path]:
        if not self.records_dir.exists():
            return []
        return sorted(path for path in self.records_dir.glob("*.json") if path.is_file())

    def scan(self) -> dict[str, Any]:
        """Return only the contiguous valid checkpoint prefix."""

        grouped: dict[int, list[tuple[Path, dict[str, Any] | None, str | None]]] = {}
        malformed: list[dict[str, Any]] = []
        for path in self._all_record_files():
            filename_sequence: int | None = None
            prefix = path.name.split(".", 1)[0]
            if prefix.isdigit():
                filename_sequence = int(prefix)
            try:
                record = read_json(path)
            except Exception as exc:
                malformed.append(
                    {
                        "seq": filename_sequence,
                        "file": path.name,
                        "reason": f"unreadable: {exc}",
                    }
                )
                continue
            sequence = record.get("seq") if isinstance(record, dict) else None
            if not isinstance(sequence, int) or sequence < 1:
                malformed.append(
                    {
                        "seq": filename_sequence,
                        "file": path.name,
                        "reason": "invalid sequence",
                    }
                )
                continue
            grouped.setdefault(sequence, []).append((path, record, None))

        valid_by_sequence: dict[int, tuple[Path, dict[str, Any]]] = {}
        ignored: list[dict[str, Any]] = list(malformed)
        for sequence in sorted(grouped):
            candidates = grouped[sequence]
            if len(candidates) != 1:
                ignored.append(
                    {
                        "seq": sequence,
                        "reason": "duplicate sequence records",
                        "files": [path.name for path, _, _ in candidates],
                    }
                )
                continue
            path, record, _ = candidates[0]
            assert isinstance(record, dict)
            validated, reason = self._validate_record(path, record)
            if validated is None:
                ignored.append({"seq": sequence, "file": path.name, "reason": reason})
            else:
                valid_by_sequence[sequence] = (path, validated)

        prefix: list[dict[str, Any]] = []
        expected = 1
        while expected in valid_by_sequence:
            path, record = valid_by_sequence[expected]
            prefix.append({**record, "_file": path.name})
            expected += 1
        tail = [sequence for sequence in sorted(valid_by_sequence) if sequence >= expected]
        if tail:
            ignored.append(
                {
                    "seq": tail[0],
                    "reason": "sequence gap after last consistent checkpoint",
                    "ignored_tail_sequences": tail,
                }
            )
        return {
            "records": prefix,
            "max_seq": len(prefix),
            "ignored": ignored,
            "file_count": len(self._all_record_files()),
        }

    def _quarantine_tail(self, scan_result: dict[str, Any]) -> list[str]:
        valid_files = {record["_file"] for record in scan_result["records"]}
        moved: list[str] = []
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)
        for path in self._all_record_files():
            if path.name in valid_files:
                continue
            destination = self.quarantine_dir / path.name
            counter = 1
            while destination.exists():
                destination = self.quarantine_dir / f"{path.stem}.{counter}{path.suffix}"
                counter += 1
            os.replace(path, destination)
            moved.append(path.name)
        for temporary in self.records_dir.glob("*.tmp"):
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
        return moved

    def artifact_path(self, artifact_digest: str) -> Path:
        return self.artifacts_dir / f"{artifact_digest}.json"

    def _latest_for_stage(
        self, records: list[dict[str, Any]], stage: str, tier: str
    ) -> dict[str, Any] | None:
        for record in reversed(records):
            if record["stage"] == stage and record["tier"] == tier:
                return record
        return None

    def publish(
        self,
        *,
        stage: str,
        tier: str,
        input_fingerprint: str,
        config_version: str,
        artifact_bytes: bytes,
        upstream_fingerprints: dict[str, str] | None = None,
        expected_seq: int | None = None,
    ) -> dict[str, Any]:
        if stage not in STAGES:
            raise WorkbenchError(
                "E_INVALID_STAGE",
                f"unknown stage: {stage}",
                stage=stage,
                details={"stage": stage, "valid_stages": list(STAGES)},
            )
        if not isinstance(tier, str) or not tier:
            raise WorkbenchError("E_INVALID_STAGE", "checkpoint tier is required", stage=stage)
        artifact_digest = sha256_bytes(artifact_bytes)
        with file_lock(self.lock_path):
            scan_result = self.scan()
            next_sequence = int(scan_result["max_seq"]) + 1
            if expected_seq is not None and int(expected_seq) != next_sequence:
                raise WorkbenchError(
                    "E_STALE_CHECKPOINT",
                    "checkpoint sequence is stale or advanced concurrently",
                    stage=stage,
                    details={
                        "expected_by_caller": int(expected_seq),
                        "required_next_sequence": next_sequence,
                        "max_seq": int(scan_result["max_seq"]),
                    },
                )
            existing = self._latest_for_stage(scan_result["records"], stage, tier)
            if existing is not None and existing["input_fingerprint"] == input_fingerprint:
                raise WorkbenchError(
                    "E_STALE_CHECKPOINT",
                    "this stage input is already published",
                    stage=stage,
                    details={
                        "existing_seq": existing["seq"],
                        "input_fingerprint": input_fingerprint,
                    },
                )
            upstream_fingerprints = upstream_fingerprints or {}
            stage_position = STAGES.index(stage)
            if stage_position > 0:
                prerequisite = STAGES[stage_position - 1]
                prerequisite_record = self._latest_for_stage(
                    scan_result["records"], prerequisite, tier
                )
                if prerequisite_record is None:
                    raise WorkbenchError(
                        "E_INVALID_STAGE",
                        f"cannot publish {stage} without a valid {prerequisite} checkpoint",
                        stage=stage,
                        details={"required_stage": prerequisite, "tier": tier},
                    )
                expected_upstream = upstream_fingerprints.get(prerequisite)
                if expected_upstream is None:
                    raise WorkbenchError(
                        "E_INVALID_STAGE",
                        f"missing upstream fingerprint for {prerequisite}",
                        stage=stage,
                        details={"required_stage": prerequisite, "tier": tier},
                    )
                if prerequisite_record["input_fingerprint"] != expected_upstream:
                    raise WorkbenchError(
                        "E_STALE_CHECKPOINT",
                        f"upstream {prerequisite} checkpoint is not the expected version",
                        stage=stage,
                        details={
                            "required_stage": prerequisite,
                            "expected_input_fingerprint": expected_upstream,
                            "published_input_fingerprint": prerequisite_record["input_fingerprint"],
                            "published_seq": prerequisite_record["seq"],
                        },
                    )

            quarantined = self._quarantine_tail(scan_result)
            artifact_path = self.artifact_path(artifact_digest)
            if not artifact_path.exists():
                atomic_write_bytes(artifact_path, artifact_bytes)
            else:
                if sha256_file(artifact_path) != artifact_digest:
                    atomic_write_bytes(artifact_path, artifact_bytes)
            relative_artifact = str(artifact_path.relative_to(self.workspace_root))
            record: dict[str, Any] = {
                "schema_version": 1,
                "seq": next_sequence,
                "stage": stage,
                "tier": tier,
                "input_fingerprint": input_fingerprint,
                "config_version": config_version,
                "artifact_digest": artifact_digest,
                "artifact_path": relative_artifact,
                "artifact_size": len(artifact_bytes),
            }
            record["record_digest"] = self._record_digest(record)
            record_path = self.records_dir / f"{next_sequence:08d}.json"
            atomic_write_bytes(record_path, canonical_json_bytes(record))
            return {**record, "quarantined": quarantined}

    def load_artifact(self, record: dict[str, Any], *, verify: bool = True) -> dict[str, Any]:
        try:
            path = safe_relative_path(self.workspace_root, str(record["artifact_path"]))
            raw = path.read_bytes()
        except Exception as exc:
            raise WorkbenchError(
                "E_INVALID_STAGE",
                "checkpoint artifact cannot be loaded",
                stage=record.get("stage"),
                details={"artifact_path": record.get("artifact_path"), "error": str(exc)},
            ) from exc
        if verify and sha256_bytes(raw) != record["artifact_digest"]:
            raise WorkbenchError(
                "E_INVALID_STAGE",
                "checkpoint artifact digest mismatch",
                stage=record.get("stage"),
                details={"artifact_path": record.get("artifact_path")},
            )
        import json

        try:
            artifact = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise WorkbenchError(
                "E_INVALID_STAGE",
                "checkpoint artifact is not valid JSON",
                stage=record.get("stage"),
                details={"artifact_path": record.get("artifact_path"), "error": str(exc)},
            ) from exc
        artifact["artifact_digest"] = record["artifact_digest"]
        return artifact

    def artifact_bytes(self, record: dict[str, Any], *, verify: bool = True) -> bytes:
        path = safe_relative_path(self.workspace_root, str(record["artifact_path"]))
        raw = path.read_bytes()
        if verify and sha256_bytes(raw) != record["artifact_digest"]:
            raise WorkbenchError(
                "E_INVALID_STAGE",
                "checkpoint artifact digest mismatch",
                stage=record.get("stage"),
                details={"artifact_path": record.get("artifact_path")},
            )
        return raw

    def latest(self, stage: str, tier: str, *, input_fingerprint: str | None = None) -> dict[str, Any] | None:
        scan_result = self.scan()
        for record in reversed(scan_result["records"]):
            if record["stage"] != stage or record["tier"] != tier:
                continue
            if input_fingerprint is not None and record["input_fingerprint"] != input_fingerprint:
                continue
            return record
        return None

    def status(self, *, include_records: bool = True) -> dict[str, Any]:
        scan_result = self.scan()
        result: dict[str, Any] = {
            "max_seq": scan_result["max_seq"],
            "ignored": scan_result["ignored"],
            "file_count": scan_result["file_count"],
            "quarantine_count": len(list(self.quarantine_dir.glob("*.json"))),
        }
        if include_records:
            result["records"] = [
                {key: value for key, value in record.items() if key != "_file"}
                for record in scan_result["records"]
            ]
        return result
