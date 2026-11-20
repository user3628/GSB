from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .blocking import BlockPlan
from .storage import (
    FileLock,
    atomic_write_json,
    digest_value,
    read_json,
    stale_config,
    stale_publish,
)


SCHEMA_VERSION = 1


@dataclass(frozen=True)
class ConfigRecord:
    seq: int
    record_digest: str
    prev_digest: str | None
    payload: dict[str, Any]
    record_file: str

    @property
    def fingerprint(self) -> str:
        return str(self.payload.get("fingerprint", ""))

    @property
    def plan(self) -> BlockPlan:
        return BlockPlan.from_dict(self.payload["plan"])

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "record_digest": self.record_digest,
            "prev_digest": self.prev_digest,
            "record_file": self.record_file,
            "payload": self.payload,
        }


@dataclass(frozen=True)
class ConfigState:
    status: str
    record: ConfigRecord | None
    anchor_seq: int | None
    head_seq: int | None

    @property
    def seq(self) -> int:
        return self.record.seq if self.record is not None else 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "seq": self.seq,
            "anchor_seq": self.anchor_seq,
            "head_seq": self.head_seq,
            "record": self.record.to_public_dict() if self.record is not None else None,
        }


class VersionedConfigStore:
    """Atomic append-only tuning records with an external high-water anchor."""

    def __init__(
        self,
        root: str | Path,
        *,
        lock_timeout_seconds: float = 10.0,
        lock_path: str | Path | None = None,
    ) -> None:
        self.root = Path(root)
        self.records_dir = self.root / "records"
        self.lock_path = Path(lock_path) if lock_path is not None else self.root / ".lock"
        self.head_path = self.root / "HEAD.json"
        self.anchor_path = self.root.parent / f".{self.root.name}.anchor.json"
        self.lock_timeout_seconds = lock_timeout_seconds
        self.records_dir.mkdir(parents=True, exist_ok=True)

    def _lock(self) -> FileLock:
        return FileLock(self.lock_path, timeout_seconds=self.lock_timeout_seconds)

    def _record_path(self, seq: int) -> Path:
        return self.records_dir / f"{seq:012d}.json"

    def _record_files(self) -> list[Path]:
        return sorted(self.records_dir.glob("*.json"), key=lambda path: path.name)

    def _verify_record(self, record: dict[str, Any], path: Path) -> ConfigRecord:
        try:
            seq = int(record["seq"])
            digest = str(record["record_digest"])
            prev_digest = record.get("prev_digest")
            payload = record["payload"]
            payload_digest = str(record["payload_digest"])
        except (KeyError, TypeError, ValueError) as exc:
            stale_config(
                "tampered_record",
                "configuration record lacks required fields",
                path=str(path),
                error=str(exc),
            )
        if not isinstance(payload, dict):
            stale_config("tampered_record", "configuration payload must be an object", path=str(path))
        expected_filename = f"{seq:012d}.json"
        if path.name != expected_filename:
            stale_config(
                "tampered_record",
                "configuration sequence does not match filename",
                path=str(path),
                expected_filename=expected_filename,
            )
        if digest_value(payload) != payload_digest:
            stale_config(
                "tampered_record",
                "configuration payload digest mismatch",
                path=str(path),
                seq=seq,
            )
        digest_input = {key: value for key, value in record.items() if key != "record_digest"}
        if digest_value(digest_input) != digest:
            stale_config(
                "tampered_record",
                "configuration record digest mismatch",
                path=str(path),
                seq=seq,
            )
        return ConfigRecord(
            seq=seq,
            record_digest=digest,
            prev_digest=str(prev_digest) if prev_digest is not None else None,
            payload=payload,
            record_file=path.name,
        )

    def _verify_unlocked(self, *, expected_min_seq: int | None = None) -> ConfigState:
        record_files = self._record_files()
        anchor_exists = self.anchor_path.exists()
        head_exists = self.head_path.exists()
        if not record_files:
            if anchor_exists or head_exists:
                stale_config(
                    "tampered_store",
                    "configuration index exists without records",
                    anchor_exists=anchor_exists,
                    head_exists=head_exists,
                )
            return ConfigState(status="empty", record=None, anchor_seq=None, head_seq=None)

        anchor = read_json(self.anchor_path) if anchor_exists else None
        head = read_json(self.head_path) if head_exists else None
        if anchor is None:
            stale_config("tampered_store", "configuration high-water anchor is missing")
        if head is None:
            stale_config("tampered_store", "configuration HEAD pointer is missing")

        verified: list[ConfigRecord] = []
        for expected_seq, path in enumerate(record_files, start=1):
            raw = read_json(path)
            record = self._verify_record(raw, path)
            if record.seq != expected_seq:
                stale_config(
                    "sequence_gap_or_reorder",
                    "configuration sequence is not continuous",
                    expected_seq=expected_seq,
                    actual_seq=record.seq,
                    path=str(path),
                )
            previous_digest = verified[-1].record_digest if verified else None
            if record.prev_digest != previous_digest:
                stale_config(
                    "broken_chain",
                    "configuration previous-record digest does not match",
                    seq=record.seq,
                    expected_prev_digest=previous_digest,
                    actual_prev_digest=record.prev_digest,
                )
            verified.append(record)
        latest = verified[-1]
        try:
            anchor_seq = int(anchor["seq"])
            anchor_digest = str(anchor["record_digest"])
            head_seq = int(head["seq"])
            head_digest = str(head["record_digest"])
        except (KeyError, TypeError, ValueError) as exc:
            stale_config("tampered_store", "anchor/HEAD pointer is malformed", error=str(exc))
        if anchor_seq > latest.seq:
            stale_config(
                "rollback",
                "configuration store was replaced by an older snapshot",
                anchor_seq=anchor_seq,
                latest_record_seq=latest.seq,
                known_high_water_seq=anchor_seq,
            )
        if anchor_seq < latest.seq:
            stale_config(
                "tampered_store",
                "record exists beyond the high-water anchor",
                anchor_seq=anchor_seq,
                latest_record_seq=latest.seq,
            )
        if head_seq < latest.seq:
            stale_config(
                "rollback",
                "configuration HEAD pointer moved backwards",
                head_seq=head_seq,
                latest_record_seq=latest.seq,
            )
        if head_seq > latest.seq:
            stale_config(
                "tampered_store",
                "configuration HEAD points beyond available records",
                head_seq=head_seq,
                latest_record_seq=latest.seq,
            )
        if anchor_digest != latest.record_digest or head_digest != latest.record_digest:
            stale_config(
                "tampered_store",
                "configuration HEAD/anchor digest mismatch",
                latest_record_digest=latest.record_digest,
                anchor_digest=anchor_digest,
                head_digest=head_digest,
            )
        if expected_min_seq is not None and latest.seq < expected_min_seq:
            stale_config(
                "rollback",
                "configuration sequence regressed below caller expectation",
                expected_min_seq=expected_min_seq,
                latest_record_seq=latest.seq,
            )
        return ConfigState(
            status="valid",
            record=latest,
            anchor_seq=anchor_seq,
            head_seq=head_seq,
        )

    def verify_state(self, *, expected_min_seq: int | None = None) -> ConfigState:
        with self._lock():
            return self._verify_unlocked(expected_min_seq=expected_min_seq)

    def load_latest(
        self,
        *,
        expected_fingerprint: str | None = None,
        expected_min_seq: int | None = None,
    ) -> ConfigRecord | None:
        state = self.verify_state(expected_min_seq=expected_min_seq)
        record = state.record
        if record is not None and expected_fingerprint is not None:
            if record.fingerprint != expected_fingerprint:
                stale_config(
                    "fingerprint_mismatch",
                    "latest configuration fingerprint differs from requested input",
                    expected_fingerprint=expected_fingerprint,
                    actual_fingerprint=record.fingerprint,
                    seq=record.seq,
                )
        return record

    def publish(
        self,
        *,
        fingerprint: str,
        plan: BlockPlan,
        sum_strategy: str,
        cache_budget: dict[str, int],
        selection_source: str,
        metadata: dict[str, Any] | None = None,
        expected_seq: int | None = None,
    ) -> ConfigRecord:
        with self._lock():
            state = self._verify_unlocked()
            current_seq = state.seq
            if expected_seq is not None and expected_seq != current_seq:
                stale_publish(
                    "sequence_conflict",
                    "configuration publish requires the exact current sequence",
                    expected_seq=expected_seq,
                    current_seq=current_seq,
                )
            next_seq = current_seq + 1
            record_path = self._record_path(next_seq)
            if record_path.exists():
                stale_publish(
                    "duplicate_sequence",
                    "configuration record sequence already exists",
                    seq=next_seq,
                    path=str(record_path),
                )
            payload = {
                "fingerprint": fingerprint,
                "plan": plan.to_dict(),
                "sum_strategy": sum_strategy,
                "cache_budget": {
                    "l1_bytes": int(cache_budget["l1_bytes"]),
                    "l2_bytes": int(cache_budget["l2_bytes"]),
                },
                "selection_source": selection_source,
                "metadata": metadata or {},
            }
            record: dict[str, Any] = {
                "schema_version": SCHEMA_VERSION,
                "seq": next_seq,
                "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                "prev_digest": state.record.record_digest if state.record is not None else None,
                "payload": payload,
                "payload_digest": digest_value(payload),
            }
            record["record_digest"] = digest_value(record)
            head = {
                "schema_version": SCHEMA_VERSION,
                "seq": next_seq,
                "record_digest": record["record_digest"],
                "record_file": record_path.name,
            }
            anchor = {
                "schema_version": SCHEMA_VERSION,
                "seq": next_seq,
                "record_digest": record["record_digest"],
                "record_file": record_path.name,
            }
            atomic_write_json(record_path, record)
            atomic_write_json(self.head_path, head)
            atomic_write_json(self.anchor_path, anchor)
            return ConfigRecord(
                seq=next_seq,
                record_digest=str(record["record_digest"]),
                prev_digest=record["prev_digest"],
                payload=payload,
                record_file=record_path.name,
            )

