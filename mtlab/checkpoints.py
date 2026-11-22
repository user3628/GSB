from __future__ import annotations

import fcntl
import json
import os
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .config import STAGE_INDEX, STAGE_ORDER, TIERS
from .errors import MtlError
from .util import (
    canonical_bytes,
    fsync_dir,
    read_json,
    sha256_json,
    sha256_tree,
    write_json_atomic,
)

LEDGER_SCHEMA_VERSION = 1


class CheckpointStore:
    def __init__(self, root: Path):
        self.root = root.resolve()
        self.mtlab_dir = self.root / ".mtlab"
        self.lock_path = self.mtlab_dir / "workspace.lock"
        self.checkpoint_root = self.root / "artifacts" / "checkpoints"
        self.stage_root = self.root / "artifacts" / "stages"
        self.staging_root = self.stage_root / ".staging"
        self.ledger_path = self.checkpoint_root / "ledger.json"
        self.invalidation_path = self.root / "artifacts" / "invalidation_log.json"
        self.ensure_layout()

    def ensure_layout(self) -> None:
        for path in (self.mtlab_dir, self.checkpoint_root, self.stage_root, self.staging_root):
            path.mkdir(parents=True, exist_ok=True)
        self.lock_path.touch(exist_ok=True)
        for tier in TIERS:
            (self.checkpoint_root / tier).mkdir(parents=True, exist_ok=True)
            (self.stage_root / tier).mkdir(parents=True, exist_ok=True)
        if not self.ledger_path.exists():
            self._write_ledger(self._empty_ledger())

    @contextmanager
    def lock(self) -> Iterator[None]:
        with self.lock_path.open("a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def healthcheck(self) -> dict[str, Any]:
        with self.lock():
            probe = self.mtlab_dir / "write_probe.txt"
            probe.write_bytes(b"ok\n")
            content = probe.read_bytes()
            probe.unlink()
            return {
                "workspace": str(self.root),
                "read_write": content == b"ok\n",
                "lock_file": str(self.lock_path),
                "checkpoint_dir": str(self.checkpoint_root),
                "stage_dir": str(self.stage_root),
            }

    @staticmethod
    def _empty_ledger() -> dict[str, Any]:
        return {
            "schema_version": LEDGER_SCHEMA_VERSION,
            "tiers": {
                tier: {"records": []}
                for tier in TIERS
            },
        }

    def _write_ledger(self, ledger: dict[str, Any]) -> None:
        write_json_atomic(self.ledger_path, ledger)

    def load_ledger(self) -> dict[str, Any]:
        if not self.ledger_path.exists():
            return self._empty_ledger()
        with self.ledger_path.open("r", encoding="utf-8") as handle:
            ledger = json.load(handle)
        if ledger.get("schema_version") != LEDGER_SCHEMA_VERSION:
            raise MtlError(
                code="E_INVALID_STAGE",
                message="unsupported checkpoint ledger schema",
                details={"path": str(self.ledger_path)},
            )
        return ledger

    def records(self, tier: str) -> list[dict[str, Any]]:
        ledger = self.load_ledger()
        return list(ledger["tiers"].get(tier, {}).get("records", []))

    def record_for_stage(self, tier: str, stage: str) -> dict[str, Any] | None:
        for record in self.records(tier):
            if record["stage"] == stage:
                return record
        return None

    def artifact_path(self, record: dict[str, Any]) -> Path:
        return self.root / record["artifact_path"]

    def record_file_path(self, record: dict[str, Any]) -> Path:
        return (
            self.checkpoint_root
            / record["tier"]
            / f"{int(record['sequence']):04d}-{record['stage']}.json"
        )

    def is_record_file_valid(self, record: dict[str, Any]) -> bool:
        record_path = self.record_file_path(record)
        if not record_path.exists():
            return False
        return record_path.read_bytes() == canonical_bytes(record) + b"\n"

    def is_record_artifact_valid(self, record: dict[str, Any]) -> bool:
        artifact = self.artifact_path(record)
        if not artifact.exists() or not artifact.is_dir():
            return False
        return sha256_tree(artifact) == record["artifact_digest"]

    def make_staging_dir(self, tier: str, stage: str) -> Path:
        return Path(
            tempfile.mkdtemp(
                prefix=f"{tier}-{stage}-",
                dir=self.staging_root,
            )
        )

    @staticmethod
    def discard_staging_dir(path: Path) -> None:
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)

    @staticmethod
    def _stale(
        *,
        tier: str,
        stage: str,
        expected_sequence: int,
        actual_sequence: int,
        reason: str,
        records: list[dict[str, Any]] | None = None,
    ) -> MtlError:
        return MtlError(
            code="E_STALE_CHECKPOINT",
            message=reason,
            details={
                "tier": tier,
                "stage": stage,
                "expected_sequence": expected_sequence,
                "actual_sequence": actual_sequence,
                "existing_stages": [record["stage"] for record in (records or [])],
            },
        )

    def publish_stage(
        self,
        *,
        tier: str,
        stage: str,
        staged_dir: Path,
        input_descriptor: dict[str, Any],
        decision_fingerprint: str,
        expected_sequence: int | None = None,
    ) -> dict[str, Any]:
        if tier not in TIERS or stage not in STAGE_ORDER:
            raise MtlError(
                code="E_INVALID_STAGE",
                message="unknown tier or stage",
                details={"tier": tier, "stage": stage},
            )
        with self.lock():
            ledger = self.load_ledger()
            records = list(ledger["tiers"][tier]["records"])
            current_count = len(records)
            expected_stage = STAGE_ORDER[current_count] if current_count < len(STAGE_ORDER) else None
            expected_seq = current_count + 1
            actual_seq = int(expected_sequence) if expected_sequence is not None else expected_seq

            meta_path = staged_dir / "_meta.json"
            if not meta_path.exists():
                raise self._stale(
                    tier=tier,
                    stage=stage,
                    expected_sequence=expected_seq,
                    actual_sequence=actual_seq,
                    reason="staged artifact lacks _meta.json",
                    records=records,
                )
            meta = read_json(meta_path)
            if meta.get("tier") != tier or meta.get("stage") != stage:
                raise self._stale(
                    tier=tier,
                    stage=stage,
                    expected_sequence=expected_seq,
                    actual_sequence=actual_seq,
                    reason="cross-tier or cross-stage artifact publication",
                    records=records,
                )
            expected_descriptor_hash = sha256_json(input_descriptor)
            if meta.get("input_fingerprint") != expected_descriptor_hash:
                raise self._stale(
                    tier=tier,
                    stage=stage,
                    expected_sequence=expected_seq,
                    actual_sequence=actual_seq,
                    reason="artifact input fingerprint does not match current descriptor",
                    records=records,
                )

            completed_stages = [record["stage"] for record in records]
            if stage in completed_stages:
                raise self._stale(
                    tier=tier,
                    stage=stage,
                    expected_sequence=expected_seq,
                    actual_sequence=actual_seq,
                    reason="duplicate stage publication",
                    records=records,
                )
            if current_count >= len(STAGE_ORDER):
                raise self._stale(
                    tier=tier,
                    stage=stage,
                    expected_sequence=expected_seq,
                    actual_sequence=actual_seq,
                    reason="tier already has all checkpoints",
                    records=records,
                )
            if stage != expected_stage:
                if STAGE_INDEX[stage] > STAGE_INDEX.get(expected_stage or "evaluate", 5):
                    raise MtlError(
                        code="E_INVALID_STAGE",
                        message="illegal stage jump or missing prerequisite",
                        details={
                            "tier": tier,
                            "stage": stage,
                            "expected_stage": expected_stage,
                            "completed_stages": completed_stages,
                        },
                    )
                raise self._stale(
                    tier=tier,
                    stage=stage,
                    expected_sequence=expected_seq,
                    actual_sequence=actual_seq,
                    reason="stage order is behind or inconsistent",
                    records=records,
                )
            if actual_seq != expected_seq:
                raise self._stale(
                    tier=tier,
                    stage=stage,
                    expected_sequence=expected_seq,
                    actual_sequence=actual_seq,
                    reason="checkpoint sequence is not contiguous",
                    records=records,
                )

            final_artifact = self.stage_root / tier / stage
            artifact_digest = sha256_tree(staged_dir)
            if final_artifact.exists():
                shutil.rmtree(final_artifact)
            final_artifact.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staged_dir, final_artifact)
            fsync_dir(final_artifact.parent)

            record = {
                "schema_version": 1,
                "tier": tier,
                "stage": stage,
                "stage_index": STAGE_INDEX[stage],
                "sequence": expected_seq,
                "input_fingerprint": expected_descriptor_hash,
                "input_descriptor": input_descriptor,
                "decision_fingerprint": decision_fingerprint,
                "artifact_digest": artifact_digest,
                "artifact_path": final_artifact.relative_to(self.root).as_posix(),
            }
            record_path = self.checkpoint_root / tier / f"{expected_seq:04d}-{stage}.json"
            write_json_atomic(record_path, record)
            records.append(record)
            ledger["tiers"][tier]["records"] = records
            self._write_ledger(ledger)
            return record

    def invalidate_from(
        self,
        *,
        tier: str,
        stage: str,
        reasons: list[str],
        detail: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if tier not in TIERS or stage not in STAGE_ORDER:
            raise MtlError(
                code="E_INVALID_STAGE",
                message="unknown tier or stage",
                details={"tier": tier, "stage": stage},
            )
        with self.lock():
            ledger = self.load_ledger()
            records = list(ledger["tiers"][tier]["records"])
            start_index = next(
                (
                    index
                    for index, record in enumerate(records)
                    if STAGE_INDEX[record["stage"]] >= STAGE_INDEX[stage]
                ),
                len(records),
            )
            retained = records[:start_index]
            removed = records[start_index:]
            for record in removed:
                record_path = self.record_file_path(record)
                if record_path.exists():
                    record_path.unlink()
                artifact = self.artifact_path(record)
                if artifact.exists():
                    shutil.rmtree(artifact)
            ledger["tiers"][tier]["records"] = retained
            self._write_ledger(ledger)
            event = {
                "tier": tier,
                "earliest_stage": stage if removed else None,
                "reasons": sorted(reasons),
                "detail": detail or {},
                "affected_stages": [record["stage"] for record in removed],
                "retained_stages": [record["stage"] for record in retained],
                "next_sequence": len(retained) + 1,
            }
            if not removed:
                return event
            history: list[dict[str, Any]] = []
            if self.invalidation_path.exists():
                with self.invalidation_path.open("r", encoding="utf-8") as handle:
                    history = json.load(handle)
            if not isinstance(history, list):
                history = []
            history.append(event)
            write_json_atomic(self.invalidation_path, history)
            return event

    def latest_invalidation(self) -> dict[str, Any] | None:
        if not self.invalidation_path.exists():
            return None
        with self.invalidation_path.open("r", encoding="utf-8") as handle:
            history = json.load(handle)
        return history[-1] if history else None

