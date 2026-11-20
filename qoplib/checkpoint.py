"""Versioned, atomic checkpoint storage with concurrency-safe publishing."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from .errors import InvalidStageError, StaleCheckpointError
from .filelock import FileLock
from .fingerprints import canonical_json_bytes, sha256_hex
from .io_util import atomic_write_json, ensure_dir

PIPELINE_STAGES = ("validate", "quantize", "linear", "verify")
STAGE_ORDER = {stage: idx for idx, stage in enumerate(PIPELINE_STAGES)}
COUNTER_FILE = "counter.json"
COUNTER_LOCK = ".counter.lock"
RESERVATION_FILE = "reservation.json"


def deterministic_sequence(shard_index: int, stage: str,
                           stages_per_shard: int = len(PIPELINE_STAGES)
                           ) -> int:
    """Global deterministic sequence: shard order first, stage order second.

    Because the sequence is a pure function of (shard index, stage), serial
    and parallel executions publish identical numbers even when workers
    finish out of order.
    """

    return shard_index * stages_per_shard + STAGE_ORDER[stage]


@dataclass(frozen=True)
class CheckpointRecord:
    sequence: int
    shard_id: str
    shard_index: int
    stage: str
    input_fingerprint: str
    config_version: int
    stage_fingerprint: str
    product_summary: Dict[str, Any]
    run_fingerprint: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sequence": self.sequence,
            "shard_id": self.shard_id,
            "shard_index": self.shard_index,
            "stage": self.stage,
            "input_fingerprint": self.input_fingerprint,
            "config_version": self.config_version,
            "stage_fingerprint": self.stage_fingerprint,
            "product_summary": self.product_summary,
            "run_fingerprint": self.run_fingerprint,
        }


class CheckpointStore:
    """File-system backed checkpoint store for one run directory."""

    def __init__(self, run_dir: str | Path, enforce_contiguous: bool = False):
        self.run_dir = Path(run_dir)
        self.shards_dir = ensure_dir(self.run_dir / "shards")
        self._counter_path = self.run_dir / COUNTER_FILE
        self._lock_path = self.run_dir / COUNTER_LOCK
        self._reservation_path = self.run_dir / RESERVATION_FILE
        # The parallel runner owns a global deterministic publisher and turns
        # this on; direct single-shard library calls keep deterministic
        # (shard, stage) sequences without a global ordering requirement.
        self.enforce_contiguous = enforce_contiguous

    # ---- shard paths --------------------------------------------------

    def shard_dir(self, shard_id: str) -> Path:
        return ensure_dir(self.shards_dir / shard_id)

    def stage_dir(self, shard_id: str, stage: str) -> Path:
        if stage not in PIPELINE_STAGES:
            raise InvalidStageError(
                f"unknown pipeline stage {stage!r}",
                details={"stage": stage, "valid_stages": list(PIPELINE_STAGES)},
            )
        return ensure_dir(self.shard_dir(shard_id) / stage)

    def stage_record_path(self, shard_id: str, stage: str) -> Path:
        return self.stage_dir(shard_id, stage) / "record.json"

    def stage_artifact_path(self, shard_id: str, stage: str,
                            name: str) -> Path:
        return self.stage_dir(shard_id, stage) / name

    # ---- counter / reservation ---------------------------------------

    def _read_counter(self) -> Dict[str, Any]:
        if not self._counter_path.exists():
            return {"next_sequence": 0, "committed": []}
        return json.loads(self._counter_path.read_text(encoding="utf-8"))

    def _write_counter(self, counter: Dict[str, Any]) -> None:
        counter["next_sequence"] = (
            max((item["sequence"] for item in counter["committed"]),
                default=-1) + 1
        ) if counter["committed"] else 0
        atomic_write_json(self._counter_path, counter)

    def reserve(self, shard_id: str, shard_index: int,
                stage: str) -> Dict[str, Any]:
        """Reserve the deterministic sequence number under a file lock."""

        if stage not in PIPELINE_STAGES:
            raise InvalidStageError(
                f"unknown pipeline stage {stage!r}",
                details={"stage": stage,
                         "valid_stages": list(PIPELINE_STAGES)},
            )
        sequence = deterministic_sequence(shard_index, stage)
        with FileLock(self._lock_path):
            counter = self._read_counter()
            for item in counter["committed"]:
                if item["shard_id"] == shard_id and item["stage"] == stage:
                    raise StaleCheckpointError(
                        f"duplicate reservation for shard {shard_id} stage "
                        f"{stage}",
                        details={"shard_id": shard_id, "stage": stage,
                                 "sequence": item["sequence"]},
                    )
            reservation = {"sequence": sequence, "shard_id": shard_id,
                           "shard_index": shard_index, "stage": stage}
            atomic_write_json(self._reservation_path, reservation)
            return reservation

    def commit(self, record: CheckpointRecord) -> Path:
        """Publish a stage record, rejecting stale / out-of-order writers."""

        if record.stage not in PIPELINE_STAGES:
            raise InvalidStageError(
                f"unknown pipeline stage {record.stage!r}",
                details={"stage": record.stage,
                         "valid_stages": list(PIPELINE_STAGES)},
            )
        expected_sequence = deterministic_sequence(
            record.shard_index, record.stage)
        with FileLock(self._lock_path):
            counter = self._read_counter()
            committed = counter.setdefault("committed", [])

            if record.sequence != expected_sequence:
                raise StaleCheckpointError(
                    "checkpoint sequence does not match deterministic "
                    "(shard index, stage) ordering",
                    details={"shard_id": record.shard_id,
                             "stage": record.stage,
                             "expected_sequence": expected_sequence,
                             "required_sequence": expected_sequence,
                             "attempted_sequence": record.sequence},
                )
            for item in committed:
                if item["shard_id"] == record.shard_id and \
                        item["stage"] == record.stage:
                    raise StaleCheckpointError(
                        f"repeat publish for shard {record.shard_id} stage "
                        f"{record.stage}",
                        details={"shard_id": record.shard_id,
                                 "stage": record.stage,
                                 "existing_sequence": item["sequence"]},
                    )
            max_committed = max((item["sequence"] for item in committed),
                                default=-1)
            # In runner-managed workspaces only contiguous progression is
            # allowed, so a late, duplicate or cross-shard misaligned publish
            # is rejected. Direct library calls still reject duplicates and
            # wrong (shard, stage) sequences via the checks above.
            if self.enforce_contiguous and                     record.sequence != max_committed + 1:
                raise StaleCheckpointError(
                    "only contiguous monotonic checkpoints may publish",
                    details={"shard_id": record.shard_id,
                             "stage": record.stage,
                             "attempted_sequence": record.sequence,
                             "required_sequence": max_committed + 1,
                             "max_committed_sequence": max_committed},
                )

            payload = record.to_dict()
            target = self.stage_record_path(record.shard_id, record.stage)
            atomic_write_json(target, payload)
            committed.append({"sequence": record.sequence,
                              "shard_id": record.shard_id,
                              "shard_index": record.shard_index,
                              "stage": record.stage})
            committed.sort(key=lambda item: item["sequence"])
            self._write_counter(counter)
            return target

    def forget_committed(self, shard_id: str,
                         from_stage: str) -> List[Dict[str, Any]]:
        """Drop committed entries for ``shard_id`` at/after ``from_stage``.

        Used when fingerprint invalidation forces downstream stages to be
        rebuilt; the freed deterministic sequences can then be republished.
        """

        cutoff = STAGE_ORDER[from_stage]
        with FileLock(self._lock_path):
            counter = self._read_counter()
            kept, dropped = [], []
            for item in counter.get("committed", []):
                if item["shard_id"] == shard_id and                         STAGE_ORDER[item["stage"]] >= cutoff:
                    dropped.append(item)
                else:
                    kept.append(item)
            counter["committed"] = kept
            self._write_counter(counter)
            return dropped

    # ---- reads / recovery ---------------------------------------------

    def load_stage_record(self, shard_id: str, stage: str
                          ) -> Optional[Dict[str, Any]]:
        path = self.stage_record_path(shard_id, stage)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def shards_dir_names(self) -> List[str]:
        if not self.shards_dir.exists():
            return []
        return sorted(path.name for path in self.shards_dir.iterdir()
                      if path.is_dir())

    def completed_shards(self) -> Dict[str, Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {}
        if not self.shards_dir.exists():
            return result
        for shard_dir in sorted(self.shards_dir.iterdir()):
            if not shard_dir.is_dir():
                continue
            verify_path = shard_dir / "verify" / "record.json"
            if verify_path.exists():
                record = json.loads(verify_path.read_text(encoding="utf-8"))
                result[shard_dir.name] = record
        return result

    def latest_sequence(self) -> int:
        counter = self._read_counter()
        committed = counter.get("committed", [])
        if not committed:
            return -1
        return max(item["sequence"] for item in committed)

    def next_sequence(self) -> int:
        return self._read_counter().get("next_sequence", 0)

    def all_records(self) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        if not self.shards_dir.exists():
            return records
        for shard_dir in sorted(self.shards_dir.iterdir()):
            if not shard_dir.is_dir():
                continue
            for stage in PIPELINE_STAGES:
                path = shard_dir / stage / "record.json"
                if path.exists():
                    records.append(json.loads(
                        path.read_text(encoding="utf-8")))
        records.sort(key=lambda item: item["sequence"])
        return records


def build_record(sequence: int, shard_id: str, shard_index: int,
                 stage: str, input_fingerprint: str, config_version: int,
                 stage_fingerprint: str, product_summary: Dict[str, Any],
                 run_fingerprint: str) -> CheckpointRecord:
    return CheckpointRecord(
        sequence=sequence,
        shard_id=shard_id,
        shard_index=shard_index,
        stage=stage,
        input_fingerprint=input_fingerprint,
        config_version=config_version,
        stage_fingerprint=stage_fingerprint,
        product_summary=product_summary,
        run_fingerprint=run_fingerprint,
    )


def record_fingerprint(record: Dict[str, Any]) -> str:
    return sha256_hex(canonical_json_bytes(record))
