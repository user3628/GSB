"""Persistent stage checkpoints with atomic writes and monotonic sequences.

Layout: ``<workspace>/checkpoints/<tile_id>/NNNNNN_<stage>.json``.

Each record contains: tile id, stage name, monotonic sequence number, the
stage fingerprint (input content fingerprint + config version), the artifact
digest manifest and the engine version. Records are written atomically
(temp file + os.replace), so an interrupted write never leaves a half-written
record. On load, corrupted / incomplete / non-contiguous records are
quarantined (renamed) and ignored; recovery continues from the most recent
consistent checkpoint.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

from . import ENGINE_VERSION, STAGES
from .errors import InvalidStageError, StaleCheckpointError
from .locks import FileLock
from .util import atomic_write_text, pretty_json

_RECORD_NAME = re.compile(r"^(\d{6})_([a-z]+)\.json$")

_RECORD_FIELDS = (
    "tile_id",
    "stage",
    "seq",
    "fingerprint",
    "input_fingerprint",
    "config_version",
    "artifact_digest",
    "artifacts",
    "engine_version",
)


@dataclass
class CheckpointRecord:
    tile_id: str
    stage: str
    seq: int
    fingerprint: str
    input_fingerprint: str
    config_version: str
    artifact_digest: str
    artifacts: dict  # relative file name -> sha256 hex
    engine_version: str = ENGINE_VERSION

    def to_dict(self) -> dict:
        return {name: getattr(self, name) for name in _RECORD_FIELDS}

    @staticmethod
    def from_dict(data: dict) -> "CheckpointRecord":
        if not isinstance(data, dict):
            raise ValueError("record is not an object")
        for name in _RECORD_FIELDS:
            if name not in data:
                raise ValueError(f"missing field {name}")
        if not isinstance(data["seq"], int) or data["seq"] < 1:
            raise ValueError("bad seq")
        if data["stage"] not in STAGES:
            raise ValueError("bad stage")
        if not isinstance(data["artifacts"], dict):
            raise ValueError("bad artifacts")
        for name in ("tile_id", "fingerprint", "input_fingerprint", "config_version",
                     "artifact_digest", "engine_version"):
            if not isinstance(data[name], str):
                raise ValueError(f"bad field {name}")
        return CheckpointRecord(**{name: data[name] for name in _RECORD_FIELDS})


class CheckpointStore:
    def __init__(self, workspace):
        self.workspace = Path(workspace)
        self.root = self.workspace / "checkpoints"
        self.lock_dir = self.workspace / "locks"

    def tile_dir(self, tile_id: str) -> Path:
        return self.root / tile_id

    def lock(self, tile_id: str) -> FileLock:
        return FileLock(self.lock_dir / f"{tile_id}.lock")

    @staticmethod
    def _quarantine(path: Path, suffix: str) -> None:
        try:
            path.rename(path.with_name(path.name + suffix))
        except OSError:
            pass

    def load_records(self, tile_id: str) -> list:
        """Load the consistent checkpoint chain for a tile.

        Corrupted, incomplete, duplicated or non-contiguous records are
        quarantined and ignored. The returned chain always has continuous
        sequence numbers 1..N with no gaps and no duplicates.
        """
        tile_dir = self.tile_dir(tile_id)
        records = []
        if tile_dir.is_dir():
            for path in sorted(tile_dir.glob("*.json")):
                match = _RECORD_NAME.match(path.name)
                if not match:
                    continue
                try:
                    record = CheckpointRecord.from_dict(
                        json.loads(path.read_text(encoding="utf-8"))
                    )
                except (OSError, ValueError):
                    self._quarantine(path, ".corrupt")
                    continue
                if record.seq != int(match.group(1)) or record.stage != match.group(2):
                    self._quarantine(path, ".mismatch")
                    continue
                records.append((record, path))

        chain = []
        expected = 1
        seen_seqs = set()
        for record, path in sorted(records, key=lambda item: (item[0].seq, item[0].stage)):
            if record.seq in seen_seqs:
                self._quarantine(path, ".dup")
                continue
            if record.seq != expected:
                self._quarantine(path, ".gap")
                continue
            chain.append(record)
            seen_seqs.add(record.seq)
            expected += 1
        return chain

    def max_seq(self, tile_id: str) -> int:
        chain = self.load_records(tile_id)
        return chain[-1].seq if chain else 0

    def latest_by_stage(self, tile_id: str) -> dict:
        latest = {}
        for record in self.load_records(tile_id):
            latest[record.stage] = record
        return latest

    def append(self, record: CheckpointRecord) -> CheckpointRecord:
        """Publish a record under the tile lock with sequence/stage validation."""
        with self.lock(record.tile_id):
            return self._append_locked(record)

    def _append_locked(self, record: CheckpointRecord) -> CheckpointRecord:
        chain = self.load_records(record.tile_id)
        max_seq = chain[-1].seq if chain else 0
        if record.seq != max_seq + 1:
            raise StaleCheckpointError(
                f"tile {record.tile_id}: checkpoint seq {record.seq} is not the "
                f"next expected seq {max_seq + 1}",
                {"tile_id": record.tile_id, "seq": record.seq, "expected": max_seq + 1},
            )
        stage_index = STAGES.index(record.stage)
        if stage_index > 0:
            previous = STAGES[stage_index - 1]
            if not any(item.stage == previous for item in chain):
                raise InvalidStageError(
                    f"tile {record.tile_id}: cannot publish {record.stage} before "
                    f"upstream stage {previous} has a checkpoint",
                    {"tile_id": record.tile_id, "stage": record.stage, "missing": previous},
                )
        path = self.tile_dir(record.tile_id) / f"{record.seq:06d}_{record.stage}.json"
        atomic_write_text(path, pretty_json(record.to_dict()))
        return record

    def publish(self, tile_id: str, stage: str, *, fingerprint: str,
                input_fingerprint: str, config_version: str,
                artifact_digest: str, artifacts: dict) -> CheckpointRecord:
        """Publish the next checkpoint for a tile (sequence assigned under lock)."""
        with self.lock(tile_id) as held:
            record = CheckpointRecord(
                tile_id=tile_id,
                stage=stage,
                seq=self.max_seq(tile_id) + 1,
                fingerprint=fingerprint,
                input_fingerprint=input_fingerprint,
                config_version=config_version,
                artifact_digest=artifact_digest,
                artifacts=dict(artifacts),
            )
            return self._append_locked(record)
