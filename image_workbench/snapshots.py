from __future__ import annotations

import copy
import json
import os
import time
from collections.abc import Iterable
from contextlib import AbstractContextManager
from pathlib import Path
from types import TracebackType
from typing import Any, BinaryIO, Self

from .atomic import atomic_write_json
from .errors import E_SNAPSHOT_DIGEST, E_STALE_SNAPSHOT, E_VALIDATION, WorkbenchError
from .hashing import digest_json

SNAPSHOT_VERSION = 1


class InterProcessLock(AbstractContextManager["InterProcessLock"]):
    """Cross-platform exclusive lock backed by fcntl or msvcrt."""

    def __init__(self, path: str | Path, *, timeout: float = 30.0) -> None:
        self.path = Path(path)
        self.timeout = timeout
        self._handle: BinaryIO | None = None

    def __enter__(self) -> Self:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = open(self.path, "a+b")
        if os.name == "nt":
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                if os.name == "nt":
                    import msvcrt

                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    handle.close()
                    raise TimeoutError(f"Timed out waiting for snapshot lock: {self.path}")
                time.sleep(0.02)
        self._handle = handle
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._handle is None:
            return
        try:
            if os.name == "nt":
                import msvcrt

                self._handle.seek(0)
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None


def snapshot_digest(snapshot: dict[str, Any]) -> str:
    return digest_json({key: value for key, value in snapshot.items() if key != "digest"})


def finalize_snapshot(
    sequence: int,
    records: Iterable[dict[str, Any]],
    *,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
        raise WorkbenchError(E_VALIDATION, "Snapshot sequence must be a positive integer")
    record_list = [copy.deepcopy(record) for record in records]
    for record in record_list:
        record["snapshot_sequence"] = sequence
        for field in ("fragment_id", "stage", "input_fingerprint", "config_fingerprint", "artifact_digest"):
            if field not in record:
                raise WorkbenchError(E_VALIDATION, "Stage record is missing a required field", details={"field": field})
    snapshot: dict[str, Any] = {
        "snapshot_version": SNAPSHOT_VERSION,
        "sequence": sequence,
        "records": record_list,
        "payload": copy.deepcopy(payload or {}),
    }
    try:
        snapshot["digest"] = snapshot_digest(snapshot)
    except (TypeError, ValueError) as exc:
        raise WorkbenchError(E_VALIDATION, "Snapshot payload must be finite JSON data") from exc
    return snapshot


def validate_snapshot(snapshot: dict[str, Any], *, check_digest: bool = True) -> None:
    if not isinstance(snapshot, dict):
        raise WorkbenchError(E_VALIDATION, "Snapshot must be a JSON object")
    if int(snapshot.get("snapshot_version", -1)) != SNAPSHOT_VERSION:
        raise WorkbenchError(E_SNAPSHOT_DIGEST, "Snapshot version is unsupported", details={"actual": snapshot.get("snapshot_version")})
    sequence = snapshot.get("sequence")
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
        raise WorkbenchError(E_SNAPSHOT_DIGEST, "Snapshot sequence is invalid")
    if not isinstance(snapshot.get("records"), list):
        raise WorkbenchError(E_SNAPSHOT_DIGEST, "Snapshot records must be a list")
    if check_digest:
        declared = snapshot.get("digest")
        try:
            actual = snapshot_digest(snapshot)
        except (TypeError, ValueError) as exc:
            raise WorkbenchError(
                E_SNAPSHOT_DIGEST,
                "Snapshot content cannot be canonicalized for digest verification",
                details={"sequence": sequence},
            ) from exc
        if declared != actual:
            raise WorkbenchError(
                E_SNAPSHOT_DIGEST,
                "Snapshot digest does not match its content",
                details={"declared": declared, "actual": actual, "sequence": sequence},
            )


class SnapshotManager:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.current_path = self.root / "current.json"
        self.high_watermark_path = self.root / "high_watermark.json"
        self.lock_path = self.root / ".publish.lock"
        self.stage_root = self.root / "stages"

    def _read_high_watermark(self) -> dict[str, Any] | None:
        if not self.high_watermark_path.exists():
            return None
        try:
            value = json.loads(self.high_watermark_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise WorkbenchError(E_SNAPSHOT_DIGEST, f"Snapshot high watermark cannot be read: {exc}") from exc
        sequence = value.get("sequence")
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
            raise WorkbenchError(E_SNAPSHOT_DIGEST, "Snapshot high watermark sequence is invalid")
        return value

    def read_current(self, *, allow_missing: bool = True) -> dict[str, Any] | None:
        if not self.current_path.exists():
            if allow_missing:
                return None
            raise WorkbenchError(E_VALIDATION, "No current snapshot exists")
        try:
            value = json.loads(self.current_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise WorkbenchError(E_SNAPSHOT_DIGEST, f"Current snapshot cannot be read: {exc}") from exc
        validate_snapshot(value, check_digest=True)
        return value

    def current_status(self) -> dict[str, Any]:
        try:
            current = self.read_current(allow_missing=True)
            high_watermark = self._read_high_watermark()
        except WorkbenchError as error:
            return {"status": "digest_mismatch", "error": error.to_dict()}
        if current is None:
            if high_watermark is not None:
                return {
                    "status": "rollback",
                    "sequence": 0,
                    "high_watermark_sequence": high_watermark["sequence"],
                }
            return {"status": "empty", "sequence": 0}
        if high_watermark is not None and int(current["sequence"]) < int(high_watermark["sequence"]):
            return {
                "status": "rollback",
                "sequence": current["sequence"],
                "digest": current["digest"],
                "high_watermark_sequence": high_watermark["sequence"],
            }
        return {"status": "normal", "sequence": current["sequence"], "digest": current["digest"]}

    def classify_candidate(self, candidate: dict[str, Any]) -> dict[str, Any]:
        try:
            validate_snapshot(candidate, check_digest=True)
        except WorkbenchError as error:
            return {"status": "digest_mismatch", "error": error.to_dict()}
        try:
            current = self.read_current(allow_missing=True)
            high_watermark = self._read_high_watermark()
        except WorkbenchError as error:
            return {"status": "digest_mismatch", "error": error.to_dict()}
        candidate_sequence = int(candidate["sequence"])
        current_sequence = int(current["sequence"]) if current is not None else 0
        high_sequence = int(high_watermark["sequence"]) if high_watermark is not None else 0
        baseline = max(current_sequence, high_sequence)
        if current is not None and candidate_sequence <= current_sequence:
            return {"status": "rollback", "from": current_sequence, "to": candidate_sequence}
        if high_watermark is not None and candidate_sequence <= high_sequence:
            return {"status": "rollback", "from": high_sequence, "to": candidate_sequence}
        if candidate_sequence == baseline + 1:
            return {"status": "normal_advance", "from": baseline, "to": candidate_sequence}
        return {"status": "gap", "from": baseline, "to": candidate_sequence}

    def inspect_candidate(self, candidate: dict[str, Any]) -> dict[str, Any]:
        return self.classify_candidate(candidate)

    def publish(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        validation = self.classify_candidate(snapshot)
        with InterProcessLock(self.lock_path):
            # Re-read under the lock so two concurrent publishers cannot both pass.
            validation = self.classify_candidate(snapshot)
            if validation["status"] == "digest_mismatch":
                error = validation.get("error", {})
                raise WorkbenchError(
                    E_SNAPSHOT_DIGEST,
                    "Snapshot digest/content mismatch",
                    details=error.get("details", {}),
                )
            if validation["status"] != "normal_advance":
                raise WorkbenchError(
                    E_STALE_SNAPSHOT,
                    "Snapshot publication must advance the current sequence by exactly one",
                    details=validation,
                )
            atomic_write_json(self.current_path, snapshot)
            atomic_write_json(
                self.high_watermark_path,
                {"sequence": snapshot["sequence"], "digest": snapshot["digest"]},
            )
            return {
                "status": "published",
                "advance_status": validation["status"],
                "from": validation.get("from"),
                "to": validation.get("to"),
                "digest": snapshot["digest"],
            }

    def publish_records(
        self,
        sequence: int,
        records: Iterable[dict[str, Any]],
        *,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.publish(finalize_snapshot(sequence, records, payload=payload))

    def write_stage_record(self, sequence: int, record: dict[str, Any]) -> Path:
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
            raise WorkbenchError(E_VALIDATION, "Stage record sequence must be a positive integer")
        fragment_id = str(record.get("fragment_id", ""))
        stage = str(record.get("stage", ""))
        if not fragment_id or stage not in {"source", "repair", "style", "export"}:
            raise WorkbenchError(E_VALIDATION, "Stage record has invalid fragment_id or stage")
        record_value = copy.deepcopy(record)
        record_value["snapshot_sequence"] = sequence
        directory = self.stage_root / str(sequence) / fragment_id
        path = directory / f"{stage}.json"
        atomic_write_json(path, record_value)
        return path

    def read_stage_record(self, sequence: int, fragment_id: str, stage: str) -> dict[str, Any]:
        path = self.stage_root / str(sequence) / fragment_id / f"{stage}.json"
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise WorkbenchError(E_VALIDATION, "Stage record not found", details={"path": str(path)}) from exc

    def next_sequence(self) -> int:
        current = self.read_current(allow_missing=True)
        return 1 if current is None else int(current["sequence"]) + 1

