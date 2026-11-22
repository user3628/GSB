"""Stage checkpoint persistence with atomic writes and publish protection.

Workspace layout: ``checkpoints/000001-import.json`` style records, a
``HEAD.json`` pointer, an append-only ``journal.log`` audit file, a
``publish.lock`` flock file and an ``artifacts/`` directory.

Every checkpoint record carries: stage name, monotonic sequence number,
input content fingerprint, config version fingerprint, per-file artifact
digests, an artifact digest, a cumulative version fingerprint and the
previous head fingerprint. Writes are atomic (tmp file + fsync + rename +
dir fsync) so an interrupted write never leaves a half-written record.

verify() distinguishes three situations: ``rollback`` (sequence moved
backwards relative to the append-only journal), ``tamper`` (fingerprint or
digest mismatch) and ``normal`` progression. Only ``normal`` is accepted;
anything else raises E_STALE_CHECKPOINT with expected/actual fingerprints.
"""
from __future__ import annotations

import fcntl
import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from .errors import E_STALE_CHECKPOINT, WorkbenchError
from .util import canonical_bytes, canonical_json, digest_obj, sha256_hex

STAGES: tuple[str, ...] = ("import", "dedup", "score", "sample", "export")


def compute_version_fingerprint(
    stage_digests: dict[str, str],
    config_fingerprint: str,
    prev_fingerprint: str = "",
) -> str:
    """Cumulative version fingerprint: stage digests + config + previous head.

    Chaining the previous fingerprint makes any upstream change (content or
    config) propagate to every downstream fingerprint.
    """
    return digest_obj(
        {
            "stages": stage_digests,
            "config": config_fingerprint,
            "prev": prev_fingerprint,
        }
    )


@dataclass
class CheckpointRecord:
    stage: str
    seq: int
    input_fingerprint: str
    config_fingerprint: str
    artifacts: dict[str, str]
    artifact_digest: str
    version_fingerprint: str
    prev_fingerprint: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "seq": self.seq,
            "input_fingerprint": self.input_fingerprint,
            "config_fingerprint": self.config_fingerprint,
            "artifacts": dict(self.artifacts),
            "artifact_digest": self.artifact_digest,
            "version_fingerprint": self.version_fingerprint,
            "prev_fingerprint": self.prev_fingerprint,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "CheckpointRecord":
        return CheckpointRecord(
            stage=data["stage"],
            seq=int(data["seq"]),
            input_fingerprint=data["input_fingerprint"],
            config_fingerprint=data["config_fingerprint"],
            artifacts=dict(data["artifacts"]),
            artifact_digest=data["artifact_digest"],
            version_fingerprint=data["version_fingerprint"],
            prev_fingerprint=data["prev_fingerprint"],
        )


@dataclass
class Verification:
    kind: str  # "normal" | "rollback" | "tamper"
    message: str
    expected_fingerprint: str = ""
    actual_fingerprint: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "message": self.message,
            "expected_fingerprint": self.expected_fingerprint,
            "actual_fingerprint": self.actual_fingerprint,
        }


def atomic_write(path: Path, data: bytes) -> None:
    """Write a file atomically: tmp file, fsync, rename, dir fsync."""
    tmp = path.with_name(f"{path.name}.tmp-{os.getpid()}")
    with open(tmp, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    dir_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


class CheckpointStore:
    def __init__(self, workspace: str | Path):
        self.workspace = Path(workspace)
        self.ckpt_dir = self.workspace / "checkpoints"
        self.artifacts_dir = self.workspace / "artifacts"
        self.head_path = self.workspace / "HEAD.json"
        self.journal_path = self.workspace / "journal.log"
        self.lock_path = self.workspace / "publish.lock"
        for directory in (self.workspace, self.ckpt_dir, self.artifacts_dir):
            directory.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def locked(self) -> Iterator[None]:
        self.lock_path.touch(exist_ok=True)
        with open(self.lock_path, "r+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _read_json(self, path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        with open(path, "rb") as handle:
            return json.loads(handle.read().decode("utf-8"))

    def head(self) -> dict[str, Any] | None:
        return self._read_json(self.head_path)

    def journal_entries(self) -> list[dict[str, Any]]:
        if not self.journal_path.exists():
            return []
        entries = []
        with open(self.journal_path, "rb") as handle:
            for line in handle.read().decode("utf-8").splitlines():
                if line.strip():
                    entries.append(json.loads(line))
        return entries

    def list_records(self) -> list[CheckpointRecord]:
        records = []
        for path in sorted(self.ckpt_dir.glob("*.json")):
            data = self._read_json(path)
            if data is not None:
                records.append(CheckpointRecord.from_dict(data))
        records.sort(key=lambda record: record.seq)
        return records

    def load(self, stage: str) -> CheckpointRecord | None:
        for record in self.list_records():
            if record.stage == stage:
                return record
        return None

    def publish(self, record: CheckpointRecord) -> CheckpointRecord:
        """Publish a stage record under the file lock with sequence checks."""
        with self.locked():
            head = self.head()
            expected_seq = int(head["seq"]) + 1 if head else 1
            if record.seq != expected_seq:
                raise WorkbenchError(
                    E_STALE_CHECKPOINT,
                    "checkpoint sequence is not a strict progression",
                    {
                        "expected_seq": expected_seq,
                        "actual_seq": record.seq,
                        "expected_fingerprint": head["version_fingerprint"] if head else "",
                        "actual_fingerprint": record.prev_fingerprint,
                    },
                )
            if expected_seq > len(STAGES) or record.stage != STAGES[expected_seq - 1]:
                raise WorkbenchError(
                    E_STALE_CHECKPOINT,
                    "checkpoint stage is out of order",
                    {
                        "expected_stage": (
                            STAGES[expected_seq - 1] if expected_seq <= len(STAGES) else None
                        ),
                        "actual_stage": record.stage,
                        "expected_fingerprint": head["version_fingerprint"] if head else "",
                        "actual_fingerprint": record.prev_fingerprint,
                    },
                )
            prev_fingerprint = head["version_fingerprint"] if head else ""
            if record.prev_fingerprint != prev_fingerprint:
                raise WorkbenchError(
                    E_STALE_CHECKPOINT,
                    "checkpoint does not chain to the current head",
                    {
                        "expected_fingerprint": prev_fingerprint,
                        "actual_fingerprint": record.prev_fingerprint,
                    },
                )
            record_path = self.ckpt_dir / f"{record.seq:06d}-{record.stage}.json"
            atomic_write(record_path, canonical_bytes(record.to_dict()) + b"\n")
            head_doc = {
                "seq": record.seq,
                "stage": record.stage,
                "version_fingerprint": record.version_fingerprint,
                "artifact_digest": record.artifact_digest,
            }
            atomic_write(self.head_path, canonical_bytes(head_doc) + b"\n")
            journal_line = canonical_json(
                {
                    "seq": record.seq,
                    "stage": record.stage,
                    "version_fingerprint": record.version_fingerprint,
                }
            )
            with open(self.journal_path, "ab") as handle:
                handle.write(journal_line.encode("utf-8") + b"\n")
                handle.flush()
                os.fsync(handle.fileno())
        return record

    def verify(self) -> Verification:
        records = self.list_records()
        head = self.head()
        journal = self.journal_entries()
        if not records:
            if head is None and not journal:
                return Verification("normal", "empty workspace")
            return Verification(
                "tamper",
                "checkpoint records are missing but head/journal exist",
                expected_fingerprint=journal[-1]["version_fingerprint"] if journal else "",
                actual_fingerprint=head["version_fingerprint"] if head else "",
            )
        for idx, record in enumerate(records):
            expected_seq = idx + 1
            if record.seq != expected_seq or record.stage != STAGES[idx]:
                return Verification(
                    "tamper",
                    f"checkpoint sequence/stage mismatch at position {expected_seq}",
                    expected_fingerprint=f"{expected_seq}:{STAGES[idx]}",
                    actual_fingerprint=f"{record.seq}:{record.stage}",
                )
        stage_digests: dict[str, str] = {}
        prev_fingerprint = ""
        for record in records:
            if record.prev_fingerprint != prev_fingerprint:
                return Verification(
                    "tamper",
                    f"prev fingerprint chain broken at stage '{record.stage}'",
                    expected_fingerprint=prev_fingerprint,
                    actual_fingerprint=record.prev_fingerprint,
                )
            for name, digest in record.artifacts.items():
                artifact_path = self.artifacts_dir / name
                if not artifact_path.exists():
                    return Verification(
                        "tamper",
                        f"artifact '{name}' of stage '{record.stage}' is missing",
                        expected_fingerprint=digest,
                        actual_fingerprint="",
                    )
                actual = sha256_hex(artifact_path.read_bytes())
                if actual != digest:
                    return Verification(
                        "tamper",
                        f"artifact '{name}' of stage '{record.stage}' digest mismatch",
                        expected_fingerprint=digest,
                        actual_fingerprint=actual,
                    )
            if digest_obj(record.artifacts) != record.artifact_digest:
                return Verification(
                    "tamper",
                    f"artifact digest mismatch at stage '{record.stage}'",
                    expected_fingerprint=record.artifact_digest,
                    actual_fingerprint=digest_obj(record.artifacts),
                )
            stage_digests[record.stage] = record.artifact_digest
            expected_vf = compute_version_fingerprint(
                stage_digests, record.config_fingerprint, record.prev_fingerprint
            )
            if expected_vf != record.version_fingerprint:
                return Verification(
                    "tamper",
                    f"version fingerprint mismatch at stage '{record.stage}'",
                    expected_fingerprint=expected_vf,
                    actual_fingerprint=record.version_fingerprint,
                )
            prev_fingerprint = record.version_fingerprint
        last = records[-1]
        if head is None:
            return Verification(
                "rollback",
                "head pointer is missing but checkpoint records exist",
                expected_fingerprint=last.version_fingerprint,
                actual_fingerprint="",
            )
        if int(head["seq"]) != last.seq or head["version_fingerprint"] != last.version_fingerprint:
            kind = "rollback" if int(head["seq"]) < last.seq else "tamper"
            return Verification(
                kind,
                "head pointer does not match the latest checkpoint record",
                expected_fingerprint=last.version_fingerprint,
                actual_fingerprint=head["version_fingerprint"],
            )
        if journal:
            tail = journal[-1]
            if int(tail["seq"]) > last.seq:
                return Verification(
                    "rollback",
                    "persisted state moved backwards relative to the journal",
                    expected_fingerprint=tail["version_fingerprint"],
                    actual_fingerprint=head["version_fingerprint"],
                )
            by_seq = {int(entry["seq"]): entry for entry in journal}
            for record in records:
                entry = by_seq.get(record.seq)
                if entry is None or entry["version_fingerprint"] != record.version_fingerprint:
                    return Verification(
                        "tamper",
                        f"journal does not confirm checkpoint seq {record.seq}",
                        expected_fingerprint=record.version_fingerprint,
                        actual_fingerprint=(entry or {}).get("version_fingerprint", ""),
                    )
        else:
            return Verification(
                "tamper",
                "journal is missing but checkpoint records exist",
                expected_fingerprint=last.version_fingerprint,
                actual_fingerprint="",
            )
        return Verification("normal", "checkpoint chain verified")

    def verify_or_raise(self) -> Verification:
        result = self.verify()
        if result.kind != "normal":
            raise WorkbenchError(
                E_STALE_CHECKPOINT,
                result.message,
                {
                    "kind": result.kind,
                    "expected_fingerprint": result.expected_fingerprint,
                    "actual_fingerprint": result.actual_fingerprint,
                },
            )
        return result

    def invalidate_from(self, stage: str) -> list[str]:
        """Drop checkpoints of `stage` and all downstream stages."""
        if stage not in STAGES:
            raise ValueError(f"unknown stage: {stage}")
        cutoff = STAGES.index(stage)
        removed: list[str] = []
        with self.locked():
            records = self.list_records()
            kept = [r for r in records if STAGES.index(r.stage) < cutoff]
            dropped = [r for r in records if STAGES.index(r.stage) >= cutoff]
            for record in dropped:
                path = self.ckpt_dir / f"{record.seq:06d}-{record.stage}.json"
                if path.exists():
                    path.unlink()
                removed.append(record.stage)
            if kept:
                last = kept[-1]
                head_doc = {
                    "seq": last.seq,
                    "stage": last.stage,
                    "version_fingerprint": last.version_fingerprint,
                    "artifact_digest": last.artifact_digest,
                }
                atomic_write(self.head_path, canonical_bytes(head_doc) + b"\n")
            elif self.head_path.exists():
                self.head_path.unlink()
            journal = self.journal_entries()
            journal_kept = [e for e in journal if int(e["seq"]) <= len(kept)]
            data = "".join(canonical_json(e) + "\n" for e in journal_kept).encode("utf-8")
            atomic_write(self.journal_path, data)
        return removed
