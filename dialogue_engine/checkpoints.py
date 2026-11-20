"""Append-only, chained, atomically published stage checkpoints."""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from .config import STAGE_INDEX, STAGES
from .errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, as_error
from .locks import FileLock
from .serialization import atomic_write_json, digest_file, digest_json, read_json


GENESIS_HASH = "genesis"
JOURNAL_SCHEMA_VERSION = 1


@dataclass
class LedgerScan:
    records: list[dict[str, Any]] = field(default_factory=list)
    paths: dict[int, Path] = field(default_factory=dict)
    issues: list[dict[str, Any]] = field(default_factory=list)
    invalid_paths: list[Path] = field(default_factory=list)
    first_bad_sequence: int | None = None
    head: dict[str, Any] | None = None
    head_issue: dict[str, Any] | None = None

    @property
    def sequence(self) -> int:
        return int(self.records[-1]["sequence"]) if self.records else 0

    @property
    def head_hash(self) -> str:
        return str(self.records[-1]["record_hash"]) if self.records else GENESIS_HASH


def _safe_component(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._") or "item"
    return f"{cleaned[:48]}-{digest_json(value)[:10]}"


class CheckpointStore:
    """Persistent stage ledger under ``<workspace>/checkpoints``."""

    def __init__(self, workspace: str | Path) -> None:
        self.workspace = Path(workspace).expanduser().resolve()
        self.root = self.workspace / "checkpoints"
        self.journal_dir = self.root / "journal"
        self.artifacts_dir = self.root / "artifacts"
        self.quarantine_dir = self.root / "quarantine"
        self.lock_path = self.root / "checkpoint.lock"
        self.head_path = self.root / "head.json"
        self.recovery_dir = self.root / "recovery"
        for directory in (self.root, self.journal_dir, self.artifacts_dir):
            directory.mkdir(parents=True, exist_ok=True)

    def lock(self, timeout: float = 30.0) -> FileLock:
        return FileLock(self.lock_path, timeout=timeout)

    def artifact_path(
        self,
        shard_id: str,
        turn_index: int,
        stage: str,
        input_fingerprint: str | None = None,
    ) -> Path:
        if stage not in STAGE_INDEX:
            raise as_error(
                E_INVALID_STAGE,
                f"unknown stage {stage!r}",
                shard_id=shard_id,
                turn_index=turn_index,
                stage=stage,
            )
        version = "" if not input_fingerprint else f"-{input_fingerprint[:16]}"
        return (
            self.artifacts_dir
            / stage
            / _safe_component(shard_id)
            / f"turn_{turn_index:06d}{version}.json"
        )

    def write_artifact(
        self,
        shard_id: str,
        turn_index: int,
        stage: str,
        artifact: dict[str, Any],
        *,
        input_fingerprint: str,
    ) -> tuple[str, Path]:
        path = self.artifact_path(shard_id, turn_index, stage, input_fingerprint)
        atomic_write_json(path, artifact)
        return digest_file(path), path

    def read_artifact(self, record: dict[str, Any]) -> dict[str, Any]:
        path = self.artifact_path(
            str(record["shard_id"]),
            int(record["turn_index"]),
            str(record["stage"]),
            str(record.get("input_fingerprint", "")),
        )
        if not path.is_file():
            raise as_error(
                E_STALE_CHECKPOINT,
                "checkpoint artifact is missing",
                shard_id=record.get("shard_id"),
                turn_index=record.get("turn_index"),
                stage=record.get("stage"),
                kind="tamper",
                artifact_path=str(path),
            )
        actual_digest = digest_file(path)
        if actual_digest != record.get("artifact_digest"):
            raise as_error(
                E_STALE_CHECKPOINT,
                "checkpoint artifact digest does not match the record",
                shard_id=record.get("shard_id"),
                turn_index=record.get("turn_index"),
                stage=record.get("stage"),
                kind="tamper",
                expected_digest=record.get("artifact_digest"),
                actual_digest=actual_digest,
            )
        try:
            return read_json(path)
        except Exception as exc:
            raise as_error(
                E_STALE_CHECKPOINT,
                f"checkpoint artifact cannot be parsed: {exc}",
                shard_id=record.get("shard_id"),
                turn_index=record.get("turn_index"),
                stage=record.get("stage"),
                kind="tamper",
            ) from exc

    def _load_head(self) -> dict[str, Any] | None:
        if not self.head_path.is_file():
            return None
        try:
            head = read_json(self.head_path)
        except Exception as exc:
            raise as_error(
                E_STALE_CHECKPOINT,
                f"checkpoint head is corrupt: {exc}",
                stage="journal",
                kind="tamper",
            ) from exc
        if not isinstance(head, dict):
            raise as_error(
                E_STALE_CHECKPOINT,
                "checkpoint head is not an object",
                stage="journal",
                kind="tamper",
            )
        return head

    def _record_hash(self, record: dict[str, Any]) -> str:
        return digest_json(
            {key: value for key, value in record.items() if key != "record_hash"}
        )

    def _validate_record_shape(self, record: Any) -> tuple[bool, str]:
        if not isinstance(record, dict):
            return False, "record is not an object"
        required = {
            "schema_version",
            "sequence",
            "shard_id",
            "turn_index",
            "stage",
            "work_order",
            "input_fingerprint",
            "config_version",
            "artifact_digest",
            "artifact_summary",
            "prev_hash",
            "record_hash",
            "shard_order",
            "shard_complete",
        }
        missing = required - set(record)
        if missing:
            return False, f"record is missing fields: {sorted(missing)}"
        if record.get("schema_version") != JOURNAL_SCHEMA_VERSION:
            return False, "unsupported checkpoint schema_version"
        if isinstance(record.get("sequence"), bool) or not isinstance(
            record.get("sequence"), int
        ):
            return False, "sequence is not an integer"
        if record.get("stage") not in STAGE_INDEX:
            return False, "invalid stage in checkpoint"
        return True, ""

    def scan(self, *, verify_effective_artifacts: bool = False) -> LedgerScan:
        scan = LedgerScan()
        parsed: dict[int, list[tuple[Path, dict[str, Any]]]] = {}
        for path in sorted(self.journal_dir.glob("*.json")):
            if path.name.startswith("."):
                continue
            try:
                record = read_json(path)
            except Exception as exc:
                scan.issues.append(
                    {"kind": "corrupt", "path": str(path), "message": str(exc)}
                )
                scan.invalid_paths.append(path)
                continue
            valid, reason = self._validate_record_shape(record)
            if not valid:
                scan.issues.append(
                    {"kind": "corrupt", "path": str(path), "message": reason}
                )
                scan.invalid_paths.append(path)
                continue
            parsed.setdefault(int(record["sequence"]), []).append((path, record))

        expected = 1
        previous_hash = GENESIS_HASH
        for sequence in sorted(parsed):
            entries = parsed[sequence]
            if len(entries) > 1:
                scan.issues.append(
                    {
                        "kind": "duplicate_sequence",
                        "sequence": sequence,
                        "paths": [str(path) for path, _ in entries],
                    }
                )
                scan.invalid_paths.extend(path for path, _ in entries)
                scan.first_bad_sequence = sequence
                break
            path, record = entries[0]
            if sequence != expected:
                scan.issues.append(
                    {
                        "kind": "sequence_gap",
                        "expected": expected,
                        "actual": sequence,
                        "path": str(path),
                    }
                )
                scan.invalid_paths.append(path)
                scan.first_bad_sequence = expected
                break
            expected_hash = self._record_hash(record)
            if record.get("record_hash") != expected_hash:
                scan.issues.append(
                    {
                        "kind": "record_hash_mismatch",
                        "sequence": sequence,
                        "path": str(path),
                        "expected": expected_hash,
                        "actual": record.get("record_hash"),
                    }
                )
                scan.invalid_paths.append(path)
                scan.first_bad_sequence = sequence
                break
            if record.get("prev_hash") != previous_hash:
                scan.issues.append(
                    {
                        "kind": "chain_mismatch",
                        "sequence": sequence,
                        "path": str(path),
                        "expected": previous_hash,
                        "actual": record.get("prev_hash"),
                    }
                )
                scan.invalid_paths.append(path)
                scan.first_bad_sequence = sequence
                break
            scan.records.append(record)
            scan.paths[sequence] = path
            previous_hash = str(record["record_hash"])
            expected += 1

        try:
            scan.head = self._load_head()
        except Exception as exc:
            scan.head = None
            scan.head_issue = {"kind": "head_corrupt", "message": str(exc)}
        if scan.head is not None:
            head_sequence = scan.head.get("sequence")
            if isinstance(head_sequence, bool) or not isinstance(head_sequence, int):
                scan.head_issue = {"kind": "head_corrupt", "message": "bad head sequence"}
            elif head_sequence > scan.sequence:
                scan.head_issue = {
                    "kind": "rollback",
                    "message": "head points beyond the durable journal prefix",
                    "head_sequence": head_sequence,
                    "durable_sequence": scan.sequence,
                }
            elif head_sequence > 0:
                expected_head_hash = scan.records[head_sequence - 1].get("record_hash")
                if scan.head.get("record_hash") != expected_head_hash:
                    scan.head_issue = {
                        "kind": "tamper",
                        "message": "head record hash does not match the journal",
                        "head_sequence": head_sequence,
                    }
            if scan.head_issue:
                scan.issues.append(scan.head_issue)

        if verify_effective_artifacts:
            for record in self._effective_records(scan.records).values():
                try:
                    self.read_artifact(record)
                except Exception as exc:
                    scan.issues.append(
                        {
                            "kind": "artifact_tamper",
                            "sequence": record.get("sequence"),
                            "message": str(exc),
                        }
                    )
                    scan.head_issue = scan.head_issue or {
                        "kind": "tamper",
                        "message": "an effective checkpoint artifact is invalid",
                    }
        return scan

    @staticmethod
    def _effective_records(
        records: Iterable[dict[str, Any]],
    ) -> dict[tuple[str, int, str], dict[str, Any]]:
        latest: dict[tuple[str, int, str], dict[str, Any]] = {}
        for record in records:
            key = (
                str(record["shard_id"]),
                int(record["turn_index"]),
                str(record["stage"]),
            )
            existing = latest.get(key)
            if existing is None or int(record["sequence"]) > int(existing["sequence"]):
                latest[key] = record
        return latest

    def effective_records(self) -> dict[tuple[str, int, str], dict[str, Any]]:
        return self._effective_records(self.scan().records)

    def latest_checkpoint(
        self, shard_id: str, turn_index: int, stage: str
    ) -> dict[str, Any] | None:
        if stage not in STAGE_INDEX:
            raise as_error(
                E_INVALID_STAGE,
                f"unknown stage {stage!r}",
                shard_id=shard_id,
                turn_index=turn_index,
                stage=stage,
            )
        return self._effective_records(self.scan().records).get(
            (shard_id, turn_index, stage)
        )

    def _ensure_dependencies(self, shard_id: str, turn_index: int, stage: str) -> None:
        stage_index = STAGE_INDEX[stage]
        effective = self.effective_records()
        if stage_index > 0:
            prerequisite = STAGES[stage_index - 1]
            if (shard_id, turn_index, prerequisite) not in effective:
                raise as_error(
                    E_INVALID_STAGE,
                    f"stage {stage!r} requires completed {prerequisite!r}",
                    shard_id=shard_id,
                    turn_index=turn_index,
                    stage=stage,
                    missing_stage=prerequisite,
                )
        if turn_index > 0 and (shard_id, turn_index - 1, "state") not in effective:
            raise as_error(
                E_INVALID_STAGE,
                "turn stages require the previous turn to be fully summarized",
                shard_id=shard_id,
                turn_index=turn_index,
                stage=stage,
                missing_turn=turn_index - 1,
            )

    def publish_checkpoint(
        self,
        *,
        shard_id: str,
        turn_index: int,
        stage: str,
        work_order: int,
        shard_order: int,
        input_fingerprint: str,
        config_version: str,
        artifact_digest: str,
        artifact_summary: dict[str, Any],
        shard_complete: bool,
        expected_sequence: int | None = None,
        replacement: bool = False,
    ) -> dict[str, Any]:
        if stage not in STAGE_INDEX:
            raise as_error(
                E_INVALID_STAGE,
                f"unknown stage {stage!r}",
                shard_id=shard_id,
                turn_index=turn_index,
                stage=stage,
            )
        with self.lock():
            scan = self.scan()
            if scan.first_bad_sequence is not None or scan.head_issue:
                issue = scan.head_issue or scan.issues[-1]
                raise as_error(
                    E_STALE_CHECKPOINT,
                    "checkpoint journal is not in a publishable state",
                    shard_id=shard_id,
                    turn_index=turn_index,
                    stage=stage,
                    kind=issue.get("kind", "stale"),
                    issue=issue,
                )
            head = scan.head
            next_sequence = scan.sequence + 1
            if expected_sequence is not None and expected_sequence != next_sequence:
                kind = "duplicate" if expected_sequence <= scan.sequence else "sequence_gap"
                raise as_error(
                    E_STALE_CHECKPOINT,
                    f"expected sequence {next_sequence}, got {expected_sequence}",
                    shard_id=shard_id,
                    turn_index=turn_index,
                    stage=stage,
                    kind=kind,
                    current_sequence=scan.sequence,
                )
            self._ensure_dependencies(shard_id, turn_index, stage)
            key = (shard_id, turn_index, stage)
            existing = self._effective_records(scan.records).get(key)
            rebuild = (head or {}).get("rebuild")
            if existing is not None and not replacement and not rebuild:
                kind = (
                    "fingerprint_mismatch"
                    if existing.get("input_fingerprint") != input_fingerprint
                    else "duplicate"
                )
                raise as_error(
                    E_STALE_CHECKPOINT,
                    "checkpoint key is already published without a rebuild",
                    shard_id=shard_id,
                    turn_index=turn_index,
                    stage=stage,
                    kind=kind,
                    existing_sequence=existing.get("sequence"),
                )

            active_shard = (head or {}).get("active_shard_id")
            active_complete = bool((head or {}).get("active_shard_complete", False))
            active_order = (head or {}).get("active_shard_order")
            if head is not None and active_shard is not None and not rebuild:
                if not active_complete and shard_id != active_shard:
                    raise as_error(
                        E_STALE_CHECKPOINT,
                        "cross-shard checkpoint before active shard completion",
                        shard_id=shard_id,
                        turn_index=turn_index,
                        stage=stage,
                        kind="cross_shard",
                        active_shard_id=active_shard,
                    )
                if active_complete and shard_id == active_shard:
                    raise as_error(
                        E_STALE_CHECKPOINT,
                        "checkpoint arrived after the active shard was completed",
                        shard_id=shard_id,
                        turn_index=turn_index,
                        stage=stage,
                        kind="cross_shard",
                        active_shard_id=active_shard,
                    )
                if (
                    active_complete
                    and shard_id != active_shard
                    and isinstance(active_order, int)
                    and shard_order != active_order + 1
                ):
                    raise as_error(
                        E_STALE_CHECKPOINT,
                        "cross-shard checkpoint does not follow the shard order",
                        shard_id=shard_id,
                        turn_index=turn_index,
                        stage=stage,
                        kind="cross_shard",
                        active_shard_id=active_shard,
                        active_shard_order=active_order,
                        published_shard_order=shard_order,
                    )

            artifact_path = self.artifact_path(
                shard_id, turn_index, stage, input_fingerprint
            )
            if not artifact_path.is_file() or digest_file(artifact_path) != artifact_digest:
                raise as_error(
                    E_STALE_CHECKPOINT,
                    "artifact is missing or does not match its declared digest",
                    shard_id=shard_id,
                    turn_index=turn_index,
                    stage=stage,
                    kind="tamper",
                )

            record: dict[str, Any] = {
                "schema_version": JOURNAL_SCHEMA_VERSION,
                "sequence": next_sequence,
                "shard_id": shard_id,
                "turn_index": turn_index,
                "stage": stage,
                "work_order": int(work_order),
                "shard_order": int(shard_order),
                "input_fingerprint": input_fingerprint,
                "config_version": config_version,
                "artifact_digest": artifact_digest,
                "artifact_summary": artifact_summary,
                "shard_complete": bool(shard_complete),
                "prev_hash": scan.head_hash,
            }
            record["record_hash"] = self._record_hash(record)
            record_path = self.journal_dir / f"{next_sequence:012d}.json"
            if record_path.exists():
                raise as_error(
                    E_STALE_CHECKPOINT,
                    "checkpoint record path already exists",
                    shard_id=shard_id,
                    turn_index=turn_index,
                    stage=stage,
                    kind="duplicate",
                )
            atomic_write_json(record_path, record)

            if rebuild:
                new_active_shard = (head or {}).get("active_shard_id", shard_id)
                new_active_order = (head or {}).get("active_shard_order", shard_order)
                new_active_complete = bool(
                    (head or {}).get("active_shard_complete", False)
                )
                if shard_id != new_active_shard:
                    new_active_shard = shard_id
                    new_active_order = shard_order
                    new_active_complete = False
                if shard_complete and shard_id == new_active_shard:
                    new_active_complete = True
            else:
                new_active_shard = shard_id
                new_active_order = shard_order
                new_active_complete = bool(shard_complete)
            new_head = {
                "schema_version": JOURNAL_SCHEMA_VERSION,
                "sequence": next_sequence,
                "record_hash": record["record_hash"],
                "active_shard_id": new_active_shard,
                "active_shard_order": new_active_order,
                "active_shard_complete": new_active_complete,
                "rebuild": rebuild,
                "last_invalidation": (head or {}).get("last_invalidation"),
            }
            atomic_write_json(self.head_path, new_head)
            return record

    def begin_rebuild(
        self,
        *,
        earliest_stage: str,
        earliest_shard_id: str,
        earliest_shard_order: int,
        reason: str,
        fingerprints: dict[str, Any],
    ) -> dict[str, Any]:
        if earliest_stage not in STAGE_INDEX:
            raise as_error(
                E_INVALID_STAGE,
                f"unknown earliest stage {earliest_stage!r}",
                shard_id=earliest_shard_id,
                stage=earliest_stage,
            )
        with self.lock():
            scan = self.scan()
            if scan.first_bad_sequence is not None or scan.head_issue:
                issue = scan.head_issue or scan.issues[-1]
                raise as_error(
                    E_STALE_CHECKPOINT,
                    "cannot start rebuild on an inconsistent journal",
                    shard_id=earliest_shard_id,
                    stage=earliest_stage,
                    kind=issue.get("kind", "stale"),
                )
            head = scan.head or {
                "schema_version": JOURNAL_SCHEMA_VERSION,
                "sequence": scan.sequence,
                "record_hash": scan.head_hash,
                "active_shard_id": None,
                "active_shard_order": None,
                "active_shard_complete": True,
                "rebuild": None,
            }
            rebuild = {
                "reason": reason,
                "earliest_stage": earliest_stage,
                "earliest_shard_id": earliest_shard_id,
                "started_sequence": int(head.get("sequence", scan.sequence)),
                "fingerprints": fingerprints,
            }
            head = dict(head)
            head["rebuild"] = rebuild
            head["active_shard_id"] = earliest_shard_id
            head["active_shard_order"] = earliest_shard_order
            head["active_shard_complete"] = False
            atomic_write_json(self.head_path, head)
            return rebuild

    def finish_rebuild(self, *, active_shard_id: str, active_shard_order: int) -> None:
        with self.lock():
            scan = self.scan()
            if scan.first_bad_sequence is not None or scan.head_issue:
                issue = scan.head_issue or scan.issues[-1]
                raise as_error(
                    E_STALE_CHECKPOINT,
                    "cannot finish rebuild on an inconsistent journal",
                    shard_id=active_shard_id,
                    stage="state",
                    kind=issue.get("kind", "stale"),
                )
            head = scan.head or {
                "schema_version": JOURNAL_SCHEMA_VERSION,
                "sequence": scan.sequence,
                "record_hash": scan.head_hash,
            }
            head = dict(head)
            rebuild = head.get("rebuild")
            if rebuild:
                rebuild = dict(rebuild)
                rebuild["completed_sequence"] = scan.sequence
                head["last_invalidation"] = rebuild
            head["rebuild"] = None
            head["active_shard_id"] = active_shard_id
            head["active_shard_order"] = active_shard_order
            head["active_shard_complete"] = True
            atomic_write_json(self.head_path, head)

    def recover(self) -> dict[str, Any]:
        """Quarantine corrupt/duplicate suffix records and restore a clean head."""

        with self.lock():
            scan = self.scan()
            changed = bool(
                scan.first_bad_sequence is not None
                or scan.head_issue
                or scan.invalid_paths
            )
            report: dict[str, Any] = {
                "recovered": changed,
                "valid_sequence": scan.sequence,
                "issues": scan.issues,
                "quarantined": [],
            }
            if not changed:
                return report
            quarantine = self.quarantine_dir / f"recovery-{time.time_ns()}"
            stale_paths: list[Path] = []
            if scan.first_bad_sequence is not None:
                for path in sorted(self.journal_dir.glob("*.json")):
                    try:
                        record = read_json(path)
                    except Exception:
                        stale_paths.append(path)
                        continue
                    sequence = record.get("sequence") if isinstance(record, dict) else None
                    if isinstance(sequence, int) and sequence >= scan.first_bad_sequence:
                        stale_paths.append(path)
            else:
                stale_paths.extend(scan.invalid_paths)
            for path in dict.fromkeys(stale_paths):
                quarantine.mkdir(parents=True, exist_ok=True)
                destination = quarantine / path.name
                counter = 1
                while destination.exists():
                    destination = quarantine / f"{path.stem}.{counter}{path.suffix}"
                    counter += 1
                os.replace(path, destination)
                report["quarantined"].append(str(destination))
            if scan.sequence == 0:
                try:
                    self.head_path.unlink()
                except FileNotFoundError:
                    pass
            else:
                last = scan.records[-1]
                atomic_write_json(
                    self.head_path,
                    {
                        "schema_version": JOURNAL_SCHEMA_VERSION,
                        "sequence": int(last["sequence"]),
                        "record_hash": last["record_hash"],
                        "active_shard_id": None,
                        "active_shard_order": None,
                        "active_shard_complete": True,
                        "rebuild": None,
                        "last_invalidation": (scan.head or {}).get(
                            "last_invalidation"
                        ),
                    },
                )
            self.recovery_dir.mkdir(parents=True, exist_ok=True)
            atomic_write_json(self.recovery_dir / "last_recovery.json", report)
            return report

    def status(self) -> dict[str, Any]:
        scan = self.scan(verify_effective_artifacts=True)
        head = scan.head or {}
        effective = self._effective_records(scan.records)
        return {
            "sequence": scan.sequence,
            "record_count": len(scan.records),
            "effective_checkpoint_count": len(effective),
            "head": head,
            "healthy": not scan.issues,
            "issues": scan.issues,
            "active_shard_id": head.get("active_shard_id"),
            "active_shard_complete": head.get("active_shard_complete"),
            "rebuild": head.get("rebuild"),
            "last_invalidation": head.get("last_invalidation"),
        }




