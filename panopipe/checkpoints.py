"""Atomic, monotonically sequenced stage checkpoints."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from .errors import E_CHECKPOINT_CORRUPT, E_STALE_CHECKPOINT, PipelineError
from .fingerprint import canonical_json, digest_json
from .fsutil import atomic_write_json, file_lock


STAGES = ("decode", "merge", "encode", "validate")
STAGE_INDEX = {stage: index for index, stage in enumerate(STAGES)}
_CHECKPOINT_NAME = re.compile(r"^(\d{8})\.json$")


def validate_stage(stage: str) -> str:
    if stage not in STAGE_INDEX:
        raise PipelineError(
            E_CHECKPOINT_CORRUPT,
            "Unknown pipeline stage",
            {"stage": stage, "supported": list(STAGES)},
        )
    return stage


def record_identity(record: dict[str, Any]) -> tuple[Any, ...]:
    return (
        record.get("shard_id"),
        record.get("stage"),
        record.get("stage_fingerprint"),
        record.get("input_fingerprint"),
        record.get("manifest_version_fingerprint"),
        record.get("artifact_digest"),
    )


def _with_self_digest(record: dict[str, Any]) -> dict[str, Any]:
    value = dict(record)
    value.pop("record_digest", None)
    value["record_digest"] = digest_json(value)
    return value


def verify_record(record: dict[str, Any]) -> None:
    stored = record.get("record_digest")
    expected = digest_json({key: value for key, value in record.items() if key != "record_digest"})
    if stored != expected:
        raise PipelineError(
            E_CHECKPOINT_CORRUPT,
            "Checkpoint self-digest does not match its contents",
            {"sequence": record.get("sequence")},
        )
    required = {
        "sequence",
        "shard_id",
        "stage",
        "input_fingerprint",
        "manifest_version_fingerprint",
        "artifact_digest",
        "stage_fingerprint",
        "artifact_files",
        "configuration_axes",
    }
    missing = sorted(required - set(record))
    if missing:
        raise PipelineError(
            E_CHECKPOINT_CORRUPT,
            "Checkpoint is missing required fields",
            {"sequence": record.get("sequence"), "missing": missing},
        )
    if record["stage"] not in STAGE_INDEX:
        raise PipelineError(E_CHECKPOINT_CORRUPT, "Checkpoint contains an unknown stage")


class CheckpointStore:
    def __init__(self, workspace_root: Path):
        self.workspace_root = workspace_root.resolve()
        self.directory = self.workspace_root / "checkpoints"
        self.lock_path = self.workspace_root / "locks" / "checkpoints.lock"

    def list_records(self) -> list[dict[str, Any]]:
        if not self.directory.exists():
            return []
        records: list[dict[str, Any]] = []
        for path in sorted(self.directory.iterdir()):
            match = _CHECKPOINT_NAME.match(path.name)
            if not match:
                continue
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise PipelineError(
                    E_CHECKPOINT_CORRUPT,
                    "Checkpoint file is unreadable",
                    {"path": str(path), "reason": str(exc)},
                ) from exc
            verify_record(record)
            if int(record["sequence"]) != int(match.group(1)):
                raise PipelineError(
                    E_CHECKPOINT_CORRUPT,
                    "Checkpoint filename and sequence disagree",
                    {"path": str(path), "sequence": record["sequence"]},
                )
            records.append(record)
        records.sort(key=lambda item: int(item["sequence"]))
        expected = 1
        for record in records:
            if int(record["sequence"]) != expected:
                raise PipelineError(
                    E_CHECKPOINT_CORRUPT,
                    "Checkpoint sequence is not contiguous",
                    {"expected": expected, "actual": record["sequence"]},
                )
            expected += 1
        return records

    def last_sequence(self) -> int:
        records = self.list_records()
        return int(records[-1]["sequence"]) if records else 0

    def latest_by_stage(self, shard_id: str | None = None) -> dict[tuple[str, str], dict[str, Any]]:
        latest: dict[tuple[str, str], dict[str, Any]] = {}
        for record in self.list_records():
            if shard_id is not None and record["shard_id"] != shard_id:
                continue
            latest[(record["shard_id"], record["stage"])] = record
        return latest

    def publish(
        self,
        record: dict[str, Any],
        *,
        expected_sequence: int | None = None,
    ) -> dict[str, Any]:
        """Publish one checkpoint.

        With ``expected_sequence`` the call is an optimistic publish: it only
        succeeds if that sequence is still the next contiguous value. Otherwise
        it fails with E_STALE_CHECKPOINT. Without it, the next sequence is
        allocated under the lock.
        """

        validate_stage(str(record.get("stage")))
        with file_lock(self.lock_path):
            self.directory.mkdir(parents=True, exist_ok=True)
            existing = self.list_records()
            last = int(existing[-1]["sequence"]) if existing else 0
            candidate_sequence = last + 1
            if expected_sequence is not None and int(expected_sequence) != candidate_sequence:
                raise PipelineError(
                    E_STALE_CHECKPOINT,
                    "Checkpoint sequence is stale",
                    {"expected_sequence": expected_sequence, "required_next": candidate_sequence},
                )
            requested = record.get("sequence")
            if requested is not None and int(requested) != candidate_sequence:
                raise PipelineError(
                    E_STALE_CHECKPOINT,
                    "Checkpoint sequence is stale or out of order",
                    {"requested_sequence": requested, "required_next": candidate_sequence},
                )
            identity = record_identity(record)
            for previous in existing:
                if record_identity(previous) == identity:
                    raise PipelineError(
                        E_STALE_CHECKPOINT,
                        "Duplicate checkpoint publication",
                        {
                            "shard_id": record.get("shard_id"),
                            "stage": record.get("stage"),
                            "existing_sequence": previous["sequence"],
                            "required_next": candidate_sequence,
                        },
                    )
            value = dict(record)
            value["sequence"] = candidate_sequence
            value = _with_self_digest(value)
            path = self.directory / f"{candidate_sequence:08d}.json"
            atomic_write_json(path, value)
            return value

    def records_for(self, shard_id: str, stage: str | None = None) -> list[dict[str, Any]]:
        records = [record for record in self.list_records() if record["shard_id"] == shard_id]
        if stage is not None:
            validate_stage(stage)
            records = [record for record in records if record["stage"] == stage]
        return records

    def latest(self, shard_id: str, stage: str) -> dict[str, Any] | None:
        validate_stage(stage)
        records = self.records_for(shard_id, stage)
        return records[-1] if records else None

    def canonical_record(self, record: dict[str, Any]) -> str:
        return canonical_json(record)
