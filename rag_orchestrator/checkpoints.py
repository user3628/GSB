"""Atomic, file-locked checkpoints with monotonic per-query sequences."""

from __future__ import annotations

import hashlib
import re
import time
from pathlib import Path
from typing import Any

from .errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, EngineError
from .util import (
    FileLock,
    atomic_write_json,
    canonical_json_bytes,
    ensure_workspace,
    read_json,
    sha256_json,
)


STAGES = ("normalize", "dispatch", "rerank", "assemble")
STAGE_INDEX = {stage: index for index, stage in enumerate(STAGES)}
_SAFE_QUERY_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,120}$")


def _safe_query_component(query_id: str) -> str:
    if _SAFE_QUERY_RE.fullmatch(query_id):
        return query_id
    digest = hashlib.sha256(query_id.encode("utf-8")).hexdigest()[:16]
    return f"sha256-{digest}"


def _new_ledger(query_id: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "query_id": query_id,
        "last_seq": 0,
        "last_stage": None,
        "cursor": 0,
        "epoch": 0,
        "epoch_start_stage": None,
        "epoch_reasons": [],
        "latest_by_stage": {},
        "stage_seq": {stage: 0 for stage in STAGES},
        "updated_at_ns": 0,
    }


class CheckpointStore:
    def __init__(self, workspace: str | Path) -> None:
        self.workspace = Path(workspace)
        ensure_workspace(self.workspace)
        self.lock_path = self.workspace / "locks" / "checkpoints.lock"

    def _query_root(self, query_id: str) -> Path:
        return self.workspace / "checkpoints" / _safe_query_component(query_id)

    def _ledger_path(self, query_id: str) -> Path:
        return self._query_root(query_id) / "ledger.json"

    def _stage_dir(self, query_id: str, stage: str) -> Path:
        return self._query_root(query_id) / stage

    def _record_path(self, query_id: str, stage: str, seq: int) -> Path:
        return self._stage_dir(query_id, stage) / f"{seq:08d}.json"

    def _read_ledger_unlocked(self, query_id: str) -> dict[str, Any] | None:
        path = self._ledger_path(query_id)
        if not path.exists():
            return None
        value = read_json(path)
        if not isinstance(value, dict) or value.get("query_id") != query_id:
            raise EngineError(
                E_STALE_CHECKPOINT,
                "checkpoint ledger does not belong to this query",
                query_id=query_id,
            )
        return value

    def _all_records_unlocked(self, query_id: str) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for stage in STAGES:
            directory = self._stage_dir(query_id, stage)
            if not directory.exists():
                continue
            for path in sorted(directory.glob("*.json")):
                if path.name.startswith("."):
                    continue
                try:
                    record = read_json(path)
                except (OSError, ValueError):
                    continue
                if not isinstance(record, dict):
                    continue
                if record.get("query_id") != query_id or record.get("stage") != stage:
                    continue
                if isinstance(record.get("seq"), int):
                    records.append(record)
        records.sort(key=lambda item: item["seq"])
        return records

    def _recover_ledger_unlocked(self, query_id: str, ledger: dict[str, Any] | None) -> dict[str, Any] | None:
        """Recover a single record written just before a process interruption.

        The record file is written before the ledger. Under the exclusive lock,
        a single orphan at ``last_seq + 1`` can be folded back into the ledger.
        Existing partial temp files are ignored because they are never renamed.
        """

        if ledger is None:
            records = self._all_records_unlocked(query_id)
            if not records:
                return None
            ledger = _new_ledger(query_id)
            for record in records:
                if record["seq"] != ledger["last_seq"] + 1:
                    break
                self._apply_record_to_ledger(ledger, record)
            return ledger

        records = self._all_records_unlocked(query_id)
        by_seq = {record["seq"]: record for record in records}
        changed = False
        while True:
            orphan = by_seq.get(ledger["last_seq"] + 1)
            if orphan is None:
                break
            self._apply_record_to_ledger(ledger, orphan)
            changed = True
        if changed:
            ledger["updated_at_ns"] = time.time_ns()
            atomic_write_json(self._ledger_path(query_id), ledger)
        return ledger

    def _apply_record_to_ledger(self, ledger: dict[str, Any], record: dict[str, Any]) -> None:
        stage = record["stage"]
        stage_index = STAGE_INDEX[stage]
        ledger["last_seq"] = int(record["seq"])
        ledger["last_stage"] = stage
        ledger["cursor"] = stage_index + 1
        ledger["epoch"] = int(record["epoch"])
        ledger["latest_by_stage"][stage] = int(record["seq"])
        ledger["stage_seq"][stage] = int(record["stage_seq"])
        if ledger["epoch_start_stage"] is None:
            ledger["epoch_start_stage"] = stage
        ledger["updated_at_ns"] = int(record.get("written_at_ns", 0))

    def _load_ledger_locked(self, query_id: str) -> dict[str, Any] | None:
        return self._recover_ledger_unlocked(query_id, self._read_ledger_unlocked(query_id))

    def read_ledger(self, query_id: str) -> dict[str, Any] | None:
        with FileLock(self.lock_path):
            return self._load_ledger_locked(query_id)

    def _validate_existing_prerequisites(self, query_id: str, ledger: dict[str, Any], stage_index: int) -> None:
        for previous_index in range(stage_index):
            previous_stage = STAGES[previous_index]
            seq = ledger.get("latest_by_stage", {}).get(previous_stage)
            if not isinstance(seq, int):
                raise EngineError(
                    E_INVALID_STAGE,
                    "a prerequisite stage checkpoint is missing",
                    query_id=query_id,
                    details={"required_stage": previous_stage, "target_stage": STAGES[stage_index]},
                )
            path = self._record_path(query_id, previous_stage, seq)
            if not path.exists():
                raise EngineError(
                    E_INVALID_STAGE,
                    "a prerequisite stage checkpoint file is missing",
                    query_id=query_id,
                    details={"required_stage": previous_stage, "seq": seq, "target_stage": STAGES[stage_index]},
                )

    def begin_epoch(
        self,
        query_id: str,
        stage: str,
        reasons: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if stage not in STAGE_INDEX:
            raise EngineError(E_INVALID_STAGE, "unknown stage", query_id=query_id, details={"stage": stage})
        stage_index = STAGE_INDEX[stage]
        with FileLock(self.lock_path):
            ledger = self._load_ledger_locked(query_id)
            if ledger is None:
                raise EngineError(
                    E_INVALID_STAGE,
                    "cannot begin a rebuild epoch without existing checkpoints",
                    query_id=query_id,
                    details={"stage": stage},
                )
            self._validate_existing_prerequisites(query_id, ledger, stage_index)
            ledger["epoch"] = int(ledger["epoch"]) + 1
            ledger["cursor"] = stage_index
            ledger["epoch_start_stage"] = stage
            ledger["epoch_reasons"] = reasons
            ledger["updated_at_ns"] = time.time_ns()
            atomic_write_json(self._ledger_path(query_id), ledger)
            return dict(ledger)

    def append(
        self,
        *,
        query_id: str,
        stage: str,
        input_fingerprint: str,
        component_fingerprints: dict[str, str],
        stage_fingerprint: str,
        budget_config_version: str,
        product_summary: dict[str, Any],
        product: dict[str, Any],
    ) -> dict[str, Any]:
        if not isinstance(query_id, str) or not query_id:
            raise EngineError(E_STALE_CHECKPOINT, "checkpoint query_id is required")
        if stage not in STAGE_INDEX:
            raise EngineError(E_INVALID_STAGE, "unknown stage", query_id=query_id, details={"stage": stage})
        if product.get("query_id") not in (None, query_id):
            raise EngineError(
                E_STALE_CHECKPOINT,
                "checkpoint payload belongs to a different query",
                query_id=query_id,
                details={"payload_query_id": product.get("query_id")},
            )
        stage_index = STAGE_INDEX[stage]
        product_digest = sha256_json(product)
        with FileLock(self.lock_path):
            ledger = self._load_ledger_locked(query_id)
            if ledger is None:
                if stage_index != 0:
                    raise EngineError(
                        E_INVALID_STAGE,
                        "first checkpoint must be the normalize stage",
                        query_id=query_id,
                        details={"stage": stage},
                    )
                ledger = _new_ledger(query_id)
                ledger["epoch"] = 1
                ledger["epoch_start_stage"] = stage
            else:
                cursor = int(ledger["cursor"])
                if stage_index > cursor:
                    raise EngineError(
                        E_INVALID_STAGE,
                        "illegal stage jump: prerequisite stage checkpoint is missing",
                        query_id=query_id,
                        details={"stage": stage, "expected_stage": STAGES[min(cursor, 3)]},
                    )
                if stage_index < cursor:
                    raise EngineError(
                        E_STALE_CHECKPOINT,
                        "stage checkpoint is behind the current cursor; begin a rebuild epoch first",
                        query_id=query_id,
                        details={"stage": stage, "cursor": cursor},
                    )
                self._validate_existing_prerequisites(query_id, ledger, stage_index)

            seq = int(ledger["last_seq"]) + 1
            record_path = self._record_path(query_id, stage, seq)
            if record_path.exists():
                raise EngineError(
                    E_STALE_CHECKPOINT,
                    "checkpoint sequence already exists",
                    query_id=query_id,
                    details={"stage": stage, "seq": seq},
                )
            stage_seq = int(ledger["stage_seq"].get(stage, 0)) + 1
            record = {
                "schema_version": 1,
                "query_id": query_id,
                "stage": stage,
                "stage_index": stage_index,
                "seq": seq,
                "stage_seq": stage_seq,
                "epoch": int(ledger["epoch"]),
                "input_fingerprint": input_fingerprint,
                "component_fingerprints": dict(component_fingerprints),
                "stage_fingerprint": stage_fingerprint,
                "budget_config_version": budget_config_version,
                "product_summary": product_summary,
                "product_sha256": product_digest,
                "product_size_bytes": len(canonical_json_bytes(product)),
                "product": product,
                "written_at_ns": time.time_ns(),
            }
            atomic_write_json(record_path, record)
            ledger["last_seq"] = seq
            ledger["last_stage"] = stage
            ledger["cursor"] = stage_index + 1
            ledger["latest_by_stage"][stage] = seq
            ledger["stage_seq"][stage] = stage_seq
            ledger["updated_at_ns"] = time.time_ns()
            atomic_write_json(self._ledger_path(query_id), ledger)
            return record

    def find_matching(self, query_id: str, stage: str, stage_fingerprint: str) -> dict[str, Any] | None:
        if stage not in STAGE_INDEX:
            raise EngineError(E_INVALID_STAGE, "unknown stage", query_id=query_id, details={"stage": stage})
        directory = self._stage_dir(query_id, stage)
        if not directory.exists():
            return None
        paths = sorted(directory.glob("*.json"), key=lambda item: item.name, reverse=True)
        for path in paths:
            if path.name.startswith("."):
                continue
            try:
                record = read_json(path)
            except (OSError, ValueError):
                continue
            if (
                isinstance(record, dict)
                and record.get("query_id") == query_id
                and record.get("stage") == stage
                and record.get("stage_fingerprint") == stage_fingerprint
                and isinstance(record.get("product"), dict)
            ):
                return record
        return None

    def latest_record(self, query_id: str, stage: str) -> dict[str, Any] | None:
        ledger = self.read_ledger(query_id)
        if ledger is None:
            return None
        seq = ledger.get("latest_by_stage", {}).get(stage)
        if not isinstance(seq, int):
            return None
        path = self._record_path(query_id, stage, seq)
        if not path.exists():
            return None
        return read_json(path)

    def records(self, query_id: str) -> list[dict[str, Any]]:
        with FileLock(self.lock_path):
            return self._all_records_unlocked(query_id)

    def summary(self, query_id: str) -> dict[str, Any]:
        ledger = self.read_ledger(query_id)
        records = self.records(query_id)
        return {
            "query_id": query_id,
            "ledger": ledger,
            "records": [
                {
                    "seq": record["seq"],
                    "stage_seq": record["stage_seq"],
                    "epoch": record["epoch"],
                    "stage": record["stage"],
                    "stage_fingerprint": record["stage_fingerprint"],
                    "product_sha256": record["product_sha256"],
                    "product_summary": record["product_summary"],
                }
                for record in records
            ],
        }

    def write_version_snapshot(self, query_id: str, descriptor: dict[str, Any]) -> Path:
        path = self.workspace / "fingerprints" / f"{_safe_query_component(query_id)}.json"
        atomic_write_json(path, descriptor)
        return path

    def write_output(self, query_id: str, stable: dict[str, Any], diagnostics: dict[str, Any]) -> Path:
        path = self.workspace / "outputs" / f"{_safe_query_component(query_id)}.json"
        atomic_write_json(
            path,
            {
                "query_id": query_id,
                "stable": stable,
                "diagnostics": diagnostics,
            },
        )
        return path
