from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .canonical import (
    atomic_write_json,
    canonical_json_bytes,
    file_lock,
    fsync_directory,
    read_json,
    sha256_bytes,
)
from .errors import (
    E_CHECKPOINT_CORRUPT,
    E_INVALID_STAGE,
    E_STALE_CHECKPOINT,
    E_STALE_VERSION,
    business_error,
)


@dataclass(frozen=True)
class WorkspaceHistory:
    records: tuple[dict[str, Any], ...]
    ignored_records: tuple[dict[str, Any], ...]
    generation: int
    events: tuple[dict[str, Any], ...]

    @property
    def head(self) -> dict[str, Any] | None:
        return self.records[-1] if self.records else None

    @property
    def next_sequence(self) -> int:
        head = self.head
        return 1 if head is None else int(head["sequence"]) + 1

    @property
    def next_batch_id(self) -> int:
        head = self.head
        return 0 if head is None else int(head["batch_id"]) + 1

    @property
    def state_version(self) -> int:
        head = self.head
        return 0 if head is None else int(head["state_summary"]["state_version"])


class Workspace:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.checkpoints_dir = self.root / "checkpoints"
        self.invalidations_path = self.root / "invalidations.json"
        self.inputs_path = self.root / "inputs.json"
        self.lock_path = self.root / ".workspace.lock"
        self.stop_request_path = self.root / ".stop.request"
        self.stop_ack_path = self.root / ".stop.ack"

    def initialize(self) -> Workspace:
        self.root.mkdir(parents=True, exist_ok=True)
        self.checkpoints_dir.mkdir(parents=True, exist_ok=True)
        if not self.invalidations_path.exists():
            atomic_write_json(
                self.invalidations_path,
                {"schema_version": 1, "generation": 1, "events": []},
            )
        return self

    def lock(self):
        return file_lock(self.lock_path)

    def _default_invalidations(self) -> dict[str, Any]:
        return {"schema_version": 1, "generation": 1, "events": []}

    def load_invalidations(self) -> dict[str, Any]:
        if not self.invalidations_path.exists():
            return self._default_invalidations()
        try:
            value = read_json(self.invalidations_path)
        except Exception as exc:
            raise business_error(
                E_CHECKPOINT_CORRUPT, f"invalidiations metadata is unreadable: {exc}"
            ) from exc
        if (
            not isinstance(value, dict)
            or "generation" not in value
            or "events" not in value
        ):
            raise business_error(
                E_CHECKPOINT_CORRUPT, "invalidation metadata has an invalid schema"
            )
        return value

    def current_generation(self) -> int:
        return int(self.load_invalidations()["generation"])

    def latest_invalidation(self) -> dict[str, Any] | None:
        events = self.load_invalidations().get("events", [])
        return events[-1] if events else None

    def checkpoint_path(self, sequence: int) -> Path:
        return self.checkpoints_dir / f"checkpoint-{sequence:08d}.json"

    def _read_checkpoint(self, path: Path) -> dict[str, Any]:
        try:
            value = read_json(path)
        except Exception as exc:
            raise business_error(
                E_CHECKPOINT_CORRUPT,
                f"checkpoint is unreadable: {path.name}: {exc}",
                file=str(path),
            ) from exc
        required = {
            "schema_version",
            "sequence",
            "batch_id",
            "generation",
            "state_summary",
            "state",
            "config_fingerprint",
            "version_fingerprint",
        }
        if not isinstance(value, dict) or not required.issubset(value):
            raise business_error(
                E_CHECKPOINT_CORRUPT,
                "checkpoint is missing required fields",
                file=str(path),
            )
        return value

    def _is_valid_for_events(
        self, record: dict[str, Any], events: list[dict[str, Any]]
    ) -> tuple[bool, str | None]:
        batch_id = int(record["batch_id"])
        applicable = [
            event for event in events if int(event["affected_batch_start"]) <= batch_id
        ]
        if not applicable:
            return True, None
        latest = max(applicable, key=lambda event: int(event["event_seq"]))
        if int(record["generation"]) != int(latest["generation"]):
            return False, f"superseded by invalidation event {latest['event_seq']}"
        return True, None

    def history(self) -> WorkspaceHistory:
        metadata = self.load_invalidations()
        generation = int(metadata["generation"])
        events = list(metadata.get("events", []))
        physical: list[dict[str, Any]] = []
        for path in sorted(self.checkpoints_dir.glob("checkpoint-*.json")):
            if path.name.startswith("."):
                continue
            physical.append(self._read_checkpoint(path))
        valid_by_sequence: dict[int, dict[str, Any]] = {}
        ignored: list[dict[str, Any]] = []
        for record in physical:
            sequence = int(record["sequence"])
            batch_id = int(record["batch_id"])
            file_name = self.checkpoint_path(sequence).name
            if sequence < 1:
                raise business_error(
                    E_CHECKPOINT_CORRUPT,
                    "checkpoint sequence must be positive",
                    file=file_name,
                )
            if batch_id != sequence - 1:
                raise business_error(
                    E_CHECKPOINT_CORRUPT,
                    "checkpoint batch and sequence are not aligned",
                    file=file_name,
                    sequence=sequence,
                    batch_id=batch_id,
                )
            is_valid, reason = self._is_valid_for_events(record, events)
            if not is_valid:
                ignored.append(
                    {"file": file_name, "batch_id": batch_id, "reason": reason}
                )
                continue
            if sequence in valid_by_sequence:
                raise business_error(
                    E_CHECKPOINT_CORRUPT,
                    "multiple active checkpoints have the same sequence",
                    sequence=sequence,
                )
            valid_by_sequence[sequence] = record
        ordered: list[dict[str, Any]] = []
        expected = 1
        while expected in valid_by_sequence:
            ordered.append(valid_by_sequence[expected])
            expected += 1
        for sequence, record in sorted(valid_by_sequence.items()):
            if sequence >= expected:
                ignored.append(
                    {
                        "file": self.checkpoint_path(sequence).name,
                        "batch_id": int(record["batch_id"]),
                        "reason": "sequence gap before this record",
                    }
                )
        return WorkspaceHistory(
            records=tuple(ordered),
            ignored_records=tuple(ignored),
            generation=generation,
            events=tuple(events),
        )

    def head_record(self) -> dict[str, Any] | None:
        return self.history().head

    def publish_record(self, record: dict[str, Any]) -> dict[str, Any]:
        with self.lock():
            history = self.history()
            expected_sequence = history.next_sequence
            expected_batch_id = history.next_batch_id
            record_sequence = int(record["sequence"])
            record_batch_id = int(record["batch_id"])
            if (
                record_sequence != expected_sequence
                or record_batch_id != expected_batch_id
            ):
                raise business_error(
                    E_STALE_CHECKPOINT,
                    "checkpoint publication is not the next contiguous sequence",
                    expected_sequence=expected_sequence,
                    expected_batch_id=expected_batch_id,
                    provided_sequence=record_sequence,
                    provided_batch_id=record_batch_id,
                )
            if int(record["generation"]) != history.generation:
                raise business_error(
                    E_STALE_CHECKPOINT,
                    "checkpoint was computed under a superseded workspace generation",
                    expected_generation=history.generation,
                    provided_generation=int(record["generation"]),
                    batch_id=record_batch_id,
                )
            if int(record["state_summary"]["state_version"]) != record_batch_id + 1:
                raise business_error(
                    E_STALE_CHECKPOINT,
                    "checkpoint state version does not match its batch",
                    provided_state_version=int(
                        record["state_summary"]["state_version"]
                    ),
                    batch_id=record_batch_id,
                )
            path = self.checkpoint_path(record_sequence)
            if path.exists():
                os.unlink(path)
                fsync_directory(self.checkpoints_dir)
            atomic_write_json(path, record)
            return record

    def apply_invalidation(
        self,
        *,
        reason: str,
        affected_batch_start: int,
        affected_batch_end: int,
        old_version_fingerprint: str | None,
        new_version_fingerprint: str,
        based_on_generation: int,
    ) -> dict[str, Any]:
        with self.lock():
            metadata = self.load_invalidations()
            current_generation = int(metadata["generation"])
            if current_generation != based_on_generation:
                raise business_error(
                    E_STALE_VERSION,
                    "invalidation plan was computed against a stale workspace generation",
                    expected_generation=current_generation,
                    provided_generation=based_on_generation,
                )
            history = self.history()
            event_seq = (
                max(
                    (int(event["event_seq"]) for event in metadata["events"]), default=0
                )
                + 1
            )
            new_generation = current_generation + 1
            event = {
                "event_seq": event_seq,
                "generation": new_generation,
                "reason": reason,
                "affected_batch_start": int(affected_batch_start),
                "affected_batch_end": int(affected_batch_end),
                "old_version_fingerprint": old_version_fingerprint,
                "new_version_fingerprint": new_version_fingerprint,
            }
            metadata["generation"] = new_generation
            metadata["events"].append(event)
            atomic_write_json(self.invalidations_path, metadata)
            for record in history.records:
                if int(record["batch_id"]) >= affected_batch_start:
                    path = self.checkpoint_path(int(record["sequence"]))
                    try:
                        os.unlink(path)
                    except FileNotFoundError:
                        pass
            for path in sorted(self.checkpoints_dir.glob("checkpoint-*.json")):
                try:
                    record = self._read_checkpoint(path)
                except business_error:
                    continue
                if int(record["batch_id"]) >= affected_batch_start:
                    try:
                        os.unlink(path)
                    except FileNotFoundError:
                        pass
            fsync_directory(self.checkpoints_dir)
            return event

    def write_inputs(self, snapshot: dict[str, Any]) -> None:
        with self.lock():
            atomic_write_json(self.inputs_path, snapshot)

    def load_inputs(self) -> dict[str, Any]:
        if not self.inputs_path.exists():
            raise business_error(E_INVALID_STAGE, "workspace has not imported inputs")
        try:
            value = read_json(self.inputs_path)
        except Exception as exc:
            raise business_error(
                E_INVALID_STAGE, f"workspace input snapshot is unreadable: {exc}"
            ) from exc
        return value

    def request_stop(self, details: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = {"schema_version": 1, "reason": "requested", **(details or {})}
        atomic_write_json(self.stop_request_path, payload)
        return payload

    def stop_requested(self) -> bool:
        return self.stop_request_path.exists()

    def consume_stop(self) -> dict[str, Any] | None:
        with self.lock():
            if not self.stop_request_path.exists():
                return None
            try:
                payload = read_json(self.stop_request_path)
            except Exception as exc:
                raise business_error(
                    E_CHECKPOINT_CORRUPT, f"stop request is unreadable: {exc}"
                ) from exc
            os.replace(self.stop_request_path, self.stop_ack_path)
            fsync_directory(self.root)
            return payload

    def clear_stop(self) -> None:
        with self.lock():
            for path in (self.stop_request_path, self.stop_ack_path):
                try:
                    os.unlink(path)
                except FileNotFoundError:
                    pass

    def record_bytes(self, sequence: int) -> bytes:
        return self.checkpoint_path(sequence).read_bytes()

    def record_chain_digest(self) -> str | None:
        records = self.history().records
        if not records:
            return None
        digest_input = b"".join(
            canonical_json_bytes(record) + b"\n" for record in records
        )
        return sha256_bytes(digest_input)
