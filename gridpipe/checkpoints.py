"""Atomic checkpoint journal and strict publication ordering."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .canonical import (
    atomic_write_bytes,
    digest_json,
    dumps,
    dump_bytes,
    read_json,
    sha256_bytes,
)
from .config import STAGE_INDEX, STAGES
from .errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, PipelineError


class FileLock:
    """Cross-process exclusive lock that works on POSIX and Windows."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.handle = None

    def __enter__(self) -> "FileLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = open(self.path, "a+b")
        if os.name == "nt":  # pragma: no cover - Windows branch
            import msvcrt

            self.handle.seek(0, os.SEEK_END)
            if self.handle.tell() == 0:
                self.handle.write(b"\0")
                self.handle.flush()
            self.handle.seek(0)
            msvcrt.locking(self.handle.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl

            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self.handle is None:
            return
        try:
            if os.name == "nt":  # pragma: no cover - Windows branch
                import msvcrt

                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.handle.close()
            self.handle = None


class CheckpointStore:
    def __init__(self, work_dir: Path | str, run_id: str) -> None:
        self.work_dir = Path(work_dir).resolve()
        self.run_id = run_id
        self.root = self.work_dir / "checkpoints" / run_id
        self.records_dir = self.root / "records"
        self.cache_dir = self.work_dir / "cache"
        self.lock_path = self.work_dir / "checkpoints" / ".publish.lock"
        self.records_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _record_path(self, sequence: int) -> Path:
        return self.records_dir / f"{sequence:012d}.json"

    def _artifact_relative(self, stage: str, fingerprint: str) -> str:
        return f"cache/{stage}/{fingerprint}.json"

    def _artifact_path(self, stage: str, fingerprint: str) -> Path:
        return self.work_dir / self._artifact_relative(stage, fingerprint)

    def _load_one(self, path: Path) -> Optional[Dict[str, Any]]:
        try:
            record = read_json(path)
            sequence = int(record["sequence"])
            if path.name != f"{sequence:012d}.json":
                return None
            stage = str(record["stage"])
            if stage not in STAGE_INDEX:
                return None
            if int(record["stage_index"]) != STAGE_INDEX[stage]:
                return None
            artifact_path = self.work_dir / str(record["artifact_path"])
            if not artifact_path.is_file():
                return None
            artifact_bytes = artifact_path.read_bytes()
            if sha256_bytes(artifact_bytes) != str(record["artifact_sha256"]):
                return None
            claimed_digest = str(record.pop("record_sha256", ""))
            actual_digest = digest_json(record)
            if claimed_digest != actual_digest:
                return None
            record["_record_sha256"] = actual_digest
            record["_artifact_bytes"] = artifact_bytes
            return record
        except Exception:
            return None

    def load_valid_records(self) -> List[Dict[str, Any]]:
        files = sorted(self.records_dir.glob("*.json"), key=lambda path: path.name)
        parsed: Dict[int, Dict[str, Any]] = {}
        duplicates = set()
        for path in files:
            record = self._load_one(path)
            if record is None:
                continue
            sequence = int(record["sequence"])
            if sequence in parsed:
                duplicates.add(sequence)
            else:
                parsed[sequence] = record
        accepted: List[Dict[str, Any]] = []
        expected_sequence = 1
        expected_previous = ""
        seen_stages = set()
        for sequence in sorted(parsed):
            if sequence in duplicates or sequence != expected_sequence:
                break
            record = parsed[sequence]
            if record.get("previous_record_sha256", "") != expected_previous:
                break
            stage_key = (record["shard_id"], record["stage"])
            if stage_key in seen_stages:
                break
            accepted.append(record)
            seen_stages.add(stage_key)
            expected_previous = record["_record_sha256"]
            expected_sequence += 1
        return accepted

    def last_sequence(self) -> int:
        records = self.load_valid_records()
        return int(records[-1]["sequence"]) if records else 0

    def next_sequence(self) -> int:
        return self.last_sequence() + 1

    def publish(
        self,
        *,
        shard_id: str,
        shard_index: int,
        stage: str,
        input_fingerprint: str,
        config_version: str,
        artifact_bytes: bytes,
        sequence: Optional[int] = None,
        expected_sequence: Optional[int] = None,
    ) -> Dict[str, Any]:
        if stage not in STAGE_INDEX:
            raise PipelineError(
                E_INVALID_STAGE,
                f"unknown checkpoint stage: {stage}",
                shard_id=shard_id,
                diagnostics={"stage": stage, "allowed": list(STAGES)},
            )
        stage_index = STAGE_INDEX[stage]
        with FileLock(self.lock_path):
            records = self.load_valid_records()
            next_expected = int(records[-1]["sequence"]) + 1 if records else 1
            target_sequence = next_expected if sequence is None else int(sequence)
            if expected_sequence is not None and int(expected_sequence) != next_expected:
                raise PipelineError(
                    E_STALE_CHECKPOINT,
                    "checkpoint publisher is behind the current sequence",
                    shard_id=shard_id,
                    diagnostics={
                        "expected_sequence": int(expected_sequence),
                        "next_sequence": next_expected,
                    },
                )
            if target_sequence != next_expected:
                raise PipelineError(
                    E_STALE_CHECKPOINT,
                    "checkpoint sequence is not contiguous",
                    shard_id=shard_id,
                    diagnostics={
                        "requested_sequence": target_sequence,
                        "next_sequence": next_expected,
                    },
                )

            by_key = {(record["shard_id"], record["stage"]): record for record in records}
            if (shard_id, stage) in by_key:
                raise PipelineError(
                    E_STALE_CHECKPOINT,
                    "checkpoint for shard and stage already exists",
                    shard_id=shard_id,
                    diagnostics={
                        "stage": stage,
                        "existing_sequence": by_key[(shard_id, stage)]["sequence"],
                    },
                )
            present_stages = {
                record["stage"]
                for record in records
                if record["shard_id"] == shard_id
            }
            for required_stage in STAGES[: stage_index - 1]:
                if required_stage not in present_stages:
                    raise PipelineError(
                        E_INVALID_STAGE,
                        "checkpoint publication skips a prerequisite stage",
                        shard_id=shard_id,
                        diagnostics={
                            "requested_stage": stage,
                            "missing_stage": required_stage,
                        },
                    )
            same_stage_records = [record for record in records if record["stage"] == stage]
            if same_stage_records and int(shard_index) < int(same_stage_records[-1]["shard_index"]):
                raise PipelineError(
                    E_STALE_CHECKPOINT,
                    "cross-shard checkpoint publication is out of order",
                    shard_id=shard_id,
                    diagnostics={
                        "stage": stage,
                        "shard_index": int(shard_index),
                        "last_shard_index": int(same_stage_records[-1]["shard_index"]),
                    },
                )

            artifact_path = self._artifact_path(stage, input_fingerprint)
            artifact_digest = sha256_bytes(artifact_bytes)
            if artifact_path.exists():
                existing = artifact_path.read_bytes()
                if sha256_bytes(existing) != artifact_digest:
                    raise PipelineError(
                        E_STALE_CHECKPOINT,
                        "content fingerprint maps to a different artifact",
                        shard_id=shard_id,
                        diagnostics={"stage": stage, "fingerprint": input_fingerprint},
                    )
            else:
                atomic_write_bytes(artifact_path, artifact_bytes)

            previous_digest = records[-1]["_record_sha256"] if records else ""
            record: Dict[str, Any] = {
                "schema_version": 1,
                "run_id": self.run_id,
                "sequence": target_sequence,
                "shard_id": shard_id,
                "shard_index": int(shard_index),
                "stage": stage,
                "stage_index": stage_index,
                "input_fingerprint": input_fingerprint,
                "config_version": config_version,
                "artifact_path": self._artifact_relative(stage, input_fingerprint),
                "artifact_sha256": artifact_digest,
                "previous_record_sha256": previous_digest,
            }
            record_digest = digest_json(record)
            record["record_sha256"] = record_digest
            record_path = self._record_path(target_sequence)
            atomic_write_bytes(record_path, dump_bytes(record))
            record["_record_sha256"] = record_digest
            record["_artifact_bytes"] = artifact_bytes
            return record

    def stages_by_shard(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        result: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for record in self.load_valid_records():
            result.setdefault(record["shard_id"], {})[record["stage"]] = record
        return result

    def read_artifact(self, record: Dict[str, Any]) -> Any:
        path = self.work_dir / str(record["artifact_path"])
        data = path.read_bytes()
        if sha256_bytes(data) != str(record["artifact_sha256"]):
            raise PipelineError(
                E_STALE_CHECKPOINT,
                "checkpoint artifact digest changed after publication",
                shard_id=record.get("shard_id"),
                diagnostics={"artifact_path": str(record["artifact_path"])},
            )
        import json

        return json.loads(data.decode("utf-8"))
