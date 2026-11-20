"""Shard-level persistent checkpoints with atomic writes and publish guards."""
from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import (E_CORRUPT_CHECKPOINT, E_INVALID_STAGE, E_STALE_CHECKPOINT,
                     StructuredError, WorkbenchError)
from .hashing import canonical_fingerprint
from .locking import file_lock

CHECKPOINT_SUFFIX = ".checkpoint.json"
LOCK_FILENAME = "publish.lock"
SEQ_FILENAME = "sequence.json"


@dataclass
class CheckpointRecord:
    shard_index: int
    sequence: int
    input_fingerprint: str
    model_fingerprint: str
    config_fingerprint: str
    result_summary: dict[str, Any]
    checksum: str

    def body(self) -> dict[str, Any]:
        return {
            "shard_index": self.shard_index,
            "sequence": self.sequence,
            "input_fingerprint": self.input_fingerprint,
            "model_fingerprint": self.model_fingerprint,
            "config_fingerprint": self.config_fingerprint,
            "result_summary": self.result_summary,
        }

    def to_envelope(self) -> dict[str, Any]:
        body = self.body()
        checksum = self.checksum or _checksum_of(body)
        return {"checksum": checksum, "record": body}

    @classmethod
    def create(cls, shard_index: int, sequence: int, input_fingerprint: str,
               model_fingerprint: str, config_fingerprint: str,
               result_summary: dict[str, Any]) -> "CheckpointRecord":
        record = cls(shard_index, sequence, input_fingerprint,
                     model_fingerprint, config_fingerprint, result_summary, "")
        record.checksum = _checksum_of(record.body())
        return record


def _checksum_of(body: dict[str, Any]) -> str:
    encoded = json.dumps(body, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return canonical_fingerprint(encoded)


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp",
                                    dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
        dir_fd = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except BaseException:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
        raise


class CheckpointStore:
    """Owns the checkpoint directory and enforces monotonic publishing."""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.root / LOCK_FILENAME
        self.sequence_path = self.root / SEQ_FILENAME

    def _shard_path(self, shard_index: int) -> Path:
        return self.root / f"shard_{shard_index:04d}{CHECKPOINT_SUFFIX}"

    def _read_sequence_locked(self) -> int:
        if not self.sequence_path.exists():
            return -1
        try:
            data = json.loads(self.sequence_path.read_text(encoding="utf-8"))
            return int(data["last_shard_index"])
        except (json.JSONDecodeError, KeyError, ValueError, OSError):
            return -1

    def _write_sequence_locked(self, last_shard_index: int) -> None:
        atomic_write_json(self.sequence_path,
                          {"last_shard_index": last_shard_index})

    def publish(self, record: CheckpointRecord) -> None:
        """Publish a shard result; rejects out-of-order or duplicate shards."""
        with file_lock(self.lock_path):
            last_index = self._read_sequence_locked()
            expected = last_index + 1
            if record.shard_index != expected:
                raise WorkbenchError(
                    E_STALE_CHECKPOINT,
                    f"shard {record.shard_index} cannot follow last shard {last_index}",
                    {"shard_index": record.shard_index,
                     "expected_shard_index": expected,
                     "last_shard_index": last_index},
                )
            if self._shard_path(record.shard_index).exists():
                raise WorkbenchError(
                    E_STALE_CHECKPOINT,
                    f"shard {record.shard_index} already published",
                    {"shard_index": record.shard_index},
                )
            envelope = record.to_envelope()
            atomic_write_json(self._shard_path(record.shard_index), envelope)
            self._write_sequence_locked(record.shard_index)

    def load(self, shard_index: int) -> CheckpointRecord | None:
        path = self._shard_path(shard_index)
        if not path.exists():
            return None
        return self._read_file(path)

    def _read_file(self, path: Path) -> CheckpointRecord:
        try:
            envelope = json.loads(path.read_text(encoding="utf-8"))
            body = envelope["record"]
            checksum = envelope["checksum"]
        except (json.JSONDecodeError, KeyError, OSError, TypeError) as exc:
            raise WorkbenchError(
                E_CORRUPT_CHECKPOINT, f"unreadable checkpoint {path.name}: {exc}",
                {"path": str(path)},
            ) from exc
        if checksum != _checksum_of(body):
            raise WorkbenchError(
                E_CORRUPT_CHECKPOINT, f"checksum mismatch in {path.name}",
                {"path": str(path)},
            )
        try:
            return CheckpointRecord(
                shard_index=int(body["shard_index"]),
                sequence=int(body["sequence"]),
                input_fingerprint=body["input_fingerprint"],
                model_fingerprint=body["model_fingerprint"],
                config_fingerprint=body["config_fingerprint"],
                result_summary=body["result_summary"],
                checksum=checksum,
            )
        except (KeyError, ValueError, TypeError) as exc:
            raise WorkbenchError(
                E_CORRUPT_CHECKPOINT, f"malformed checkpoint {path.name}: {exc}",
                {"path": str(path)},
            ) from exc

    def load_consistent_prefix(self) -> tuple[list[CheckpointRecord],
                                              list[StructuredError]]:
        """Return the longest contiguous, intact checkpoint prefix.

        Corrupted or non-contiguous records are ignored (reported structurally)
        so resume can restart from the most recent consistent checkpoint.
        """
        records: list[CheckpointRecord] = []
        warnings: list[StructuredError] = []
        index = 0
        while True:
            path = self._shard_path(index)
            if not path.exists():
                break
            try:
                record = self._read_file(path)
            except WorkbenchError as exc:
                warnings.append(StructuredError.from_exception(exc))
                break
            if record.shard_index != index:
                warnings.append(StructuredError(
                    E_CORRUPT_CHECKPOINT,
                    f"non-contiguous checkpoint at shard {index}",
                    {"path": str(path)},
                ))
                break
            records.append(record)
            index += 1
        return records, warnings

    def require_prefix(self, up_to_shard: int) -> list[CheckpointRecord]:
        """Guard used before aggregation: every predecessor must exist."""
        records, warnings = self.load_consistent_prefix()
        available = {r.shard_index for r in records}
        for index in range(0, up_to_shard + 1):
            if index not in available:
                raise WorkbenchError(
                    E_INVALID_STAGE,
                    f"cannot aggregate shard {up_to_shard}: predecessor {index} missing",
                    {"requested_shard": up_to_shard, "missing_shard": index,
                     "checkpoint_warnings": [w.to_dict() for w in warnings]},
                )
        return records[: up_to_shard + 1]

    def reset(self) -> None:
        with file_lock(self.lock_path):
            for path in self.root.glob(f"*{CHECKPOINT_SUFFIX}"):
                path.unlink()
            if self.sequence_path.exists():
                self.sequence_path.unlink()
