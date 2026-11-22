from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Iterable

from .errors import WorkbenchError
from .util import FileLock, atomic_write_json, atomic_write_text, digest_object, read_json


STAGE_RANK = {
    "type_inference": 0,
    "encoding": 1,
    "selection": 2,
    "aggregate": 3,
}

REQUIRED_RECORD_FIELDS = {
    "stage",
    "scope",
    "pipeline_version",
    "stage_version",
    "input_fingerprint",
    "config_version",
    "artifact_digest",
}


def _slug(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", value)
    return cleaned[:48] or "record"


class CheckpointStore:
    def __init__(self, state_dir: Path) -> None:
        self.state_dir = Path(state_dir)
        self.checkpoint_dir = self.state_dir / "checkpoints"
        self.records_dir = self.checkpoint_dir / "records"
        self.lock_path = self.checkpoint_dir / ".publish.lock"
        self.stop_path = self.checkpoint_dir / "STOP"
        self.records_dir.mkdir(parents=True, exist_ok=True)

    def list_records(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for path in sorted(self.records_dir.glob("*.json")):
            try:
                record = read_json(path)
            except (OSError, ValueError) as exc:
                raise WorkbenchError(
                    "E_STATE_INVALID",
                    "Checkpoint record is unreadable",
                    {"path": str(path), "reason": str(exc)},
                ) from exc
            if not isinstance(record, dict) or "sequence" not in record:
                raise WorkbenchError(
                    "E_STATE_INVALID",
                    "Checkpoint record has an invalid structure",
                    {"path": str(path)},
                )
            records.append(record)
        records.sort(key=lambda item: int(item["sequence"]))
        for expected, record in enumerate(records, start=1):
            if int(record["sequence"]) != expected:
                raise WorkbenchError(
                    "E_STATE_INVALID",
                    "Checkpoint sequence is not continuous",
                    {
                        "expected": expected,
                        "observed": record["sequence"],
                        "record_id": record.get("record_id"),
                    },
                )
        return records

    def next_sequence(self) -> int:
        records = self.list_records()
        return int(records[-1]["sequence"]) + 1 if records else 1

    @staticmethod
    def _record_key(record: dict[str, Any]) -> tuple[str, str, str]:
        return (
            str(record["stage"]),
            str(record["stage_version"]),
            str(record["scope"]),
        )

    @staticmethod
    def _validate_dependencies(
        record: dict[str, Any],
        records: list[dict[str, Any]],
    ) -> None:
        keys = {CheckpointStore._record_key(existing) for existing in records}
        for dependency in record.get("dependencies", []):
            key = (
                str(dependency.get("stage")),
                str(dependency.get("stage_version")),
                str(dependency.get("scope")),
            )
            if key not in keys:
                raise WorkbenchError(
                    "E_STALE_CHECKPOINT",
                    "Checkpoint dependency has not been published",
                    {
                        "record_id": record.get("record_id"),
                        "dependency": dependency,
                    },
                )

    def publish(
        self,
        record: dict[str, Any],
        *,
        expected_sequence: int | None = None,
    ) -> dict[str, Any]:
        missing = REQUIRED_RECORD_FIELDS.difference(record)
        if missing:
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Checkpoint record is missing required fields",
                {"missing": sorted(missing)},
            )
        stage = str(record["stage"])
        if stage not in STAGE_RANK:
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Checkpoint record has an unknown stage",
                {"stage": stage},
            )
        record_id = str(
            record.get("record_id")
            or f"{stage}:{record['scope']}:{record['stage_version']}"
        )
        record = dict(record)
        record["record_id"] = record_id
        record.setdefault("dependencies", [])
        with FileLock(self.lock_path):
            records = self.list_records()
            next_sequence = int(records[-1]["sequence"]) + 1 if records else 1
            if expected_sequence is not None and int(expected_sequence) != next_sequence:
                raise WorkbenchError(
                    "E_STALE_CHECKPOINT",
                    "Checkpoint publish sequence is stale",
                    {
                        "record_id": record_id,
                        "expected_sequence": int(expected_sequence),
                        "actual_next_sequence": next_sequence,
                    },
                )
            duplicate = next(
                (existing for existing in records if existing.get("record_id") == record_id),
                None,
            )
            if duplicate is not None:
                raise WorkbenchError(
                    "E_STALE_CHECKPOINT",
                    "Checkpoint record was already published",
                    {
                        "record_id": record_id,
                        "existing_sequence": duplicate.get("sequence"),
                    },
                )
            pipeline_version = str(record["pipeline_version"])
            current_cycle = [
                existing
                for existing in records
                if str(existing.get("pipeline_version")) == pipeline_version
            ]
            if current_cycle:
                last_rank = max(STAGE_RANK[str(existing["stage"])] for existing in current_cycle)
                if STAGE_RANK[stage] < last_rank:
                    raise WorkbenchError(
                        "E_STALE_CHECKPOINT",
                        "Checkpoint publication crosses stages out of order",
                        {
                            "record_id": record_id,
                            "stage": stage,
                            "last_published_stage_rank": last_rank,
                            "current_stage_rank": STAGE_RANK[stage],
                            "pipeline_version": pipeline_version,
                        },
                    )
            self._validate_dependencies(record, records)
            record["sequence"] = next_sequence
            record["record_digest"] = digest_object(record)
            filename = f"{next_sequence:012d}-{_slug(stage)}-{_slug(record['scope'])}.json"
            atomic_write_json(self.records_dir / filename, record)
            return record

    def latest_record(
        self,
        *,
        stage: str | None = None,
        stage_version: str | None = None,
        scope: str | None = None,
        pipeline_version: str | None = None,
        records: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        candidates = records if records is not None else self.list_records()
        filtered = []
        for record in candidates:
            if stage is not None and record.get("stage") != stage:
                continue
            if stage_version is not None and record.get("stage_version") != stage_version:
                continue
            if scope is not None and record.get("scope") != scope:
                continue
            if pipeline_version is not None and record.get("pipeline_version") != pipeline_version:
                continue
            filtered.append(record)
        if not filtered:
            return None
        return max(filtered, key=lambda item: int(item["sequence"]))

    def records_for_stage_version(
        self,
        stage: str,
        stage_version: str,
        records: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        candidates = records if records is not None else self.list_records()
        return [
            record
            for record in candidates
            if record.get("stage") == stage and record.get("stage_version") == stage_version
        ]

    def sequence_summary(self) -> dict[str, Any]:
        records = self.list_records()
        return {
            "count": len(records),
            "first_sequence": int(records[0]["sequence"]) if records else None,
            "last_sequence": int(records[-1]["sequence"]) if records else None,
            "continuous": True,
        }

    def request_stop(self, reason: str = "user_requested") -> None:
        atomic_write_text(self.stop_path, reason + "\n")

    def stop_requested(self) -> bool:
        return self.stop_path.exists()

    def stop_reason(self) -> str | None:
        if not self.stop_path.exists():
            return None
        try:
            return self.stop_path.read_text(encoding="utf-8").strip() or "stop_requested"
        except OSError:
            return "stop_requested"

    def clear_stop(self) -> None:
        if self.stop_path.exists():
            self.stop_path.unlink()


def make_record(
    *,
    stage: str,
    scope: str,
    pipeline_version: str,
    stage_version: str,
    input_fingerprint: str,
    config_version: str,
    artifact_digest: str,
    shard_id: int | None = None,
    tier: str | None = None,
    payload: dict[str, Any] | None = None,
    dependencies: Iterable[dict[str, Any]] = (),
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "record_id": f"{stage}:{scope}:{stage_version}",
        "stage": stage,
        "scope": scope,
        "shard_id": shard_id,
        "tier": tier,
        "pipeline_version": pipeline_version,
        "stage_version": stage_version,
        "input_fingerprint": input_fingerprint,
        "config_version": config_version,
        "artifact_digest": artifact_digest,
        "payload": payload or {},
        "dependencies": list(dependencies),
    }

