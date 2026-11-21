"""Atomic checkpoint log, plan enforcement, content-addressed artifacts and audit."""

from __future__ import annotations

import copy
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from .canonical import (
    atomic_write_json,
    digest_obj,
    file_lock,
    read_json,
    try_read_json,
)
from .errors import CMIRError, E_INVALID_STAGE, E_STALE_CHECKPOINT


GENESIS = "0" * 64
STAGES = ("feature", "projection_quant", "index", "recall_validation")
CHECKPOINT_SCHEMA = "cmir.checkpoint-record.v1"
HEAD_SCHEMA = "cmir.checkpoint-head.v1"
PLAN_SCHEMA = "cmir.checkpoint-plan.v1"
REQUIRED_RECORD_FIELDS = (
    "schema",
    "seq",
    "shard_id",
    "stage",
    "shard_seq",
    "cycle",
    "plan_id",
    "plan_index",
    "input_digest",
    "config_version",
    "config_digest",
    "quant_param_digest",
    "artifact_digest",
    "artifact_object",
    "artifact_path",
    "prev_digest",
    "record_digest",
)


def _record_digest(record: dict[str, Any]) -> str:
    payload = {key: value for key, value in record.items() if key != "record_digest"}
    return digest_obj(payload)


@dataclass
class Recovery:
    records: list[dict[str, Any]] = field(default_factory=list)
    last_seq: int = 0
    chain_digest: str = GENESIS
    plan: dict[str, Any] | None = None
    next_unit_index: int = 0
    plan_complete: bool = False
    anomalies: list[dict[str, Any]] = field(default_factory=list)
    ignored_seq: list[int] = field(default_factory=list)
    head: dict[str, Any] | None = None

    def summary(self) -> dict[str, Any]:
        return {
            "last_seq": self.last_seq,
            "chain_digest": self.chain_digest,
            "active_plan_id": self.plan.get("plan_id") if self.plan else None,
            "next_unit_index": self.next_unit_index,
            "plan_complete": self.plan_complete,
            "record_count": len(self.records),
            "anomalies": self.anomalies,
            "ignored_seq": self.ignored_seq,
        }


class CheckpointStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.checkpoint_dir = self.root / "checkpoints"
        self.record_dir = self.checkpoint_dir / "records"
        self.objects_dir = self.root / "objects"
        self.locks_dir = self.root / "locks"
        self.head_path = self.checkpoint_dir / "head.json"
        self.plan_path = self.checkpoint_dir / "plan.json"
        self.lock_path = self.locks_dir / "checkpoints.lock"
        for path in (self.record_dir, self.objects_dir, self.locks_dir):
            path.mkdir(parents=True, exist_ok=True)

    def _record_path(self, seq: int) -> Path:
        return self.record_dir / f"{seq:012d}.json"

    def _object_path(self, digest: str) -> Path:
        return self.objects_dir / digest[:2] / f"{digest}.json"

    def put_object(self, value: Any) -> str:
        digest = digest_obj(value)
        path = self._object_path(digest)
        if not path.is_file():
            atomic_write_json(path, value)
            return digest
        existing, error = try_read_json(path)
        if error is not None or digest_obj(existing) != digest:
            raise CMIRError(
                E_INVALID_STAGE,
                "content-addressed object is corrupted",
                object_digest=digest,
                object_path=str(path),
                reason=error,
            )
        return digest

    def get_object(self, digest: str, *, verify: bool = True) -> Any:
        path = self._object_path(digest)
        value, error = try_read_json(path)
        if error is not None:
            raise CMIRError(
                E_INVALID_STAGE,
                "checkpoint artifact object is missing or invalid",
                object_digest=digest,
                object_path=str(path),
                reason=error,
            )
        actual = digest_obj(value)
        if verify and actual != digest:
            raise CMIRError(
                E_INVALID_STAGE,
                "checkpoint artifact digest mismatch",
                expected_digest=digest,
                actual_digest=actual,
                object_path=str(path),
            )
        return value

    def _load_plan(self) -> dict[str, Any] | None:
        value, error = try_read_json(self.plan_path)
        return value if error is None and isinstance(value, dict) else None

    def _recover_locked(self) -> Recovery:
        recovery = Recovery()
        head, head_error = try_read_json(self.head_path)
        recovery.head = head if head_error is None and isinstance(head, dict) else None
        plan = self._load_plan()
        recovery.plan = plan
        files: dict[int, Path] = {}
        if self.record_dir.is_dir():
            for path in self.record_dir.iterdir():
                if not path.is_file() or not path.name.endswith(".json"):
                    continue
                stem = path.name[:-5]
                if stem.isdigit():
                    files[int(stem)] = path
        previous = GENESIS
        expected_seq = 1
        while True:
            path = files.get(expected_seq)
            if path is None:
                break
            record, error = try_read_json(path)
            if error is not None or not isinstance(record, dict):
                recovery.anomalies.append(
                    {
                        "kind": "corrupt_record",
                        "seq": expected_seq,
                        "reason": error or "record is not an object",
                    }
                )
                recovery.ignored_seq.append(expected_seq)
                break
            missing = [name for name in REQUIRED_RECORD_FIELDS if name not in record]
            if missing:
                recovery.anomalies.append(
                    {"kind": "incomplete_record", "seq": expected_seq, "missing": missing}
                )
                recovery.ignored_seq.append(expected_seq)
                break
            if int(record["seq"]) != expected_seq:
                recovery.anomalies.append(
                    {
                        "kind": "sequence_mismatch",
                        "expected": expected_seq,
                        "actual": record.get("seq"),
                    }
                )
                recovery.ignored_seq.append(expected_seq)
                break
            if record.get("prev_digest") != previous:
                recovery.anomalies.append(
                    {
                        "kind": "chain_break",
                        "seq": expected_seq,
                        "expected_prev": previous,
                        "actual_prev": record.get("prev_digest"),
                    }
                )
                recovery.ignored_seq.append(expected_seq)
                break
            actual_digest = _record_digest(record)
            if actual_digest != record.get("record_digest"):
                recovery.anomalies.append(
                    {
                        "kind": "record_digest_mismatch",
                        "seq": expected_seq,
                        "expected": record.get("record_digest"),
                        "actual": actual_digest,
                    }
                )
                recovery.ignored_seq.append(expected_seq)
                break
            object_path = self._object_path(str(record["artifact_object"]))
            if not object_path.is_file():
                recovery.anomalies.append(
                    {
                        "kind": "missing_artifact_object",
                        "seq": expected_seq,
                        "object_digest": record.get("artifact_object"),
                    }
                )
                recovery.ignored_seq.append(expected_seq)
                break
            recovery.records.append(record)
            recovery.last_seq = expected_seq
            recovery.chain_digest = actual_digest
            previous = actual_digest
            expected_seq += 1
        ignored_later = sorted(seq for seq in files if seq > expected_seq - 1)
        # Files above the first gap are intentionally not appended to ignored_seq unless
        # they claim to be part of the active plan; the head audit will flag them.
        recovery.ignored_seq.extend(seq for seq in ignored_later if seq not in recovery.ignored_seq)
        last = recovery.records[-1] if recovery.records else None
        if plan and isinstance(plan.get("units"), list):
            if last and last.get("plan_id") == plan.get("plan_id"):
                recovery.next_unit_index = int(last.get("plan_index", -1)) + 1
            else:
                recovery.next_unit_index = 0
            recovery.plan_complete = recovery.next_unit_index >= len(plan["units"])
        elif last:
            recovery.next_unit_index = int(last.get("plan_index", -1)) + 1
        if recovery.head is not None:
            head_seq = recovery.head.get("last_seq")
            if isinstance(head_seq, int) and head_seq > recovery.last_seq:
                recovery.anomalies.append(
                    {
                        "kind": "head_ahead_of_valid_chain",
                        "head_seq": head_seq,
                        "valid_seq": recovery.last_seq,
                    }
                )
        return recovery

    def recover(self) -> Recovery:
        with file_lock(self.lock_path):
            return self._recover_locked()

    def audit(self) -> dict[str, Any]:
        with file_lock(self.lock_path):
            recovery = self._recover_locked()
            kinds = {anomaly["kind"] for anomaly in recovery.anomalies}
            rollback = False
            tamper = False
            if recovery.head is not None:
                head_seq = recovery.head.get("last_seq")
                if isinstance(head_seq, int) and head_seq < recovery.last_seq:
                    rollback = True
                if isinstance(head_seq, int) and head_seq > recovery.last_seq:
                    tamper = True
                if recovery.plan and recovery.head.get("active_plan_id") != recovery.plan.get("plan_id"):
                    plan_head_mismatch = True
                else:
                    plan_head_mismatch = False
                if plan_head_mismatch:
                    tamper = True
            if kinds:
                tamper = True
            if recovery.plan and isinstance(recovery.plan.get("units"), list):
                by_index = {int(record["plan_index"]): record for record in recovery.records if record.get("plan_id") == recovery.plan.get("plan_id")}
                for index, record in by_index.items():
                    if index >= len(recovery.plan["units"]):
                        tamper = True
                        break
                    expected = recovery.plan["units"][index]
                    for field_name in (
                        "shard_id",
                        "stage",
                        "shard_seq",
                        "cycle",
                        "input_digest",
                        "config_digest",
                        "quant_param_digest",
                        "artifact_digest",
                        "artifact_object",
                    ):
                        if field_name in expected and expected[field_name] != record.get(field_name):
                            tamper = True
                            recovery.anomalies.append(
                                {
                                    "kind": "plan_unit_mismatch",
                                    "plan_index": index,
                                    "field": field_name,
                                    "expected": expected[field_name],
                                    "actual": record.get(field_name),
                                }
                            )
                for record in recovery.records:
                    try:
                        self.get_object(str(record["artifact_object"]))
                    except CMIRError as exc:
                        tamper = True
                        recovery.anomalies.append(
                            {
                                "kind": "artifact_digest_mismatch",
                                "seq": record["seq"],
                                "details": exc.details,
                            }
                        )
            status = "tamper" if tamper else ("rollback" if rollback else "advance")
            return {
                "ok": True,
                "status": status,
                "recovery": recovery.summary(),
                "head_consistent": recovery.head is not None
                and recovery.head.get("last_seq") == recovery.last_seq
                and recovery.head.get("chain_digest") == recovery.chain_digest,
                "plan_complete": recovery.plan_complete,
            }

    def latest_cycles(self) -> dict[str, int]:
        recovery = self.recover()
        cycles: dict[str, int] = {}
        for record in recovery.records:
            shard_id = str(record["shard_id"])
            cycles[shard_id] = max(cycles.get(shard_id, 0), int(record["cycle"]))
        return cycles

    def latest_records(self) -> dict[tuple[str, str], dict[str, Any]]:
        recovery = self.recover()
        result: dict[tuple[str, str], dict[str, Any]] = {}
        for record in recovery.records:
            key = (str(record["shard_id"]), str(record["stage"]))
            current = result.get(key)
            if current is None or (
                int(record["cycle"]), int(record["shard_seq"])
            ) > (int(current["cycle"]), int(current["shard_seq"])):
                result[key] = record
        return result

    def active_plan(self) -> dict[str, Any] | None:
        with file_lock(self.lock_path):
            recovery = self._recover_locked()
            if recovery.plan is None or recovery.plan_complete:
                return None
            return {
                "plan": recovery.plan,
                "next_unit_index": recovery.next_unit_index,
                "last_seq": recovery.last_seq,
            }

    def create_plan(
        self,
        units: list[dict[str, Any]],
        *,
        plan_id: str,
        context: dict[str, Any],
    ) -> dict[str, Any]:
        if not units:
            raise CMIRError(E_INVALID_STAGE, "checkpoint plan has no units")
        with file_lock(self.lock_path):
            recovery = self._recover_locked()
            if recovery.plan is not None and not recovery.plan_complete:
                raise CMIRError(
                    E_INVALID_STAGE,
                    "an incomplete checkpoint plan must be resumed before creating a new plan",
                    active_plan_id=recovery.plan.get("plan_id"),
                    next_unit_index=recovery.next_unit_index,
                    last_seq=recovery.last_seq,
                )
            normalized_units = []
            for index, unit in enumerate(units):
                row = copy.deepcopy(unit)
                row["index"] = index
                normalized_units.append(row)
            plan = {
                "schema": PLAN_SCHEMA,
                "plan_id": plan_id,
                "created_from_seq": recovery.last_seq,
                "units": normalized_units,
                "context_digest": digest_obj(context),
            }
            curl_plan_digest = digest_obj(plan)
            atomic_write_json(self.plan_path, plan)
            head = {
                "schema": HEAD_SCHEMA,
                "last_seq": recovery.last_seq,
                "chain_digest": recovery.chain_digest,
                "active_plan_id": plan_id,
                "plan_digest": curl_plan_digest,
                "next_unit_index": 0,
                "plan_complete": False,
            }
            atomic_write_json(self.head_path, head)
            return {"plan": plan, "plan_digest": curl_plan_digest, "head": head}

    def _previous_stage_present(
        self,
        records: Iterable[dict[str, Any]],
        unit: dict[str, Any],
    ) -> bool:
        shard_seq = int(unit["shard_seq"])
        if shard_seq == 1:
            return True
        expected_stage = STAGES[shard_seq - 2]
        for record in records:
            if (
                record.get("shard_id") == unit["shard_id"]
                and int(record.get("cycle", -1)) == int(unit["cycle"])
                and record.get("stage") == expected_stage
                and int(record.get("shard_seq", -1)) == shard_seq - 1
            ):
                return True
        return False

    def publish(self, unit_index: int) -> dict[str, Any]:
        with file_lock(self.lock_path):
            recovery = self._recover_locked()
            plan = recovery.plan
            if plan is None:
                raise CMIRError(E_INVALID_STAGE, "no active checkpoint plan")
            head = recovery.head
            if head is not None:
                head_seq = head.get("last_seq")
                if isinstance(head_seq, int) and head_seq > recovery.last_seq:
                    raise CMIRError(
                        E_STALE_CHECKPOINT,
                        "checkpoint head points beyond the valid record chain",
                        classification="tamper",
                        head_seq=head_seq,
                        valid_seq=recovery.last_seq,
                    )
                if isinstance(head_seq, int) and recovery.last_seq - head_seq > 1:
                    raise CMIRError(
                        E_STALE_CHECKPOINT,
                        "checkpoint head rollback spans more than one atomic write",
                        classification="rollback",
                        head_seq=head_seq,
                        valid_seq=recovery.last_seq,
                    )
                if isinstance(head_seq, int):
                    at_head = None
                    for candidate in recovery.records:
                        if int(candidate["seq"]) == head_seq:
                            at_head = candidate
                            break
                    if at_head is not None and head.get("chain_digest") != at_head.get("record_digest"):
                        raise CMIRError(
                            E_STALE_CHECKPOINT,
                            "checkpoint head digest does not match the record chain",
                            classification="tamper",
                            head_seq=head_seq,
                            expected_chain_digest=at_head.get("record_digest"),
                            actual_chain_digest=head.get("chain_digest"),
                        )
                if head.get("active_plan_id") not in (None, plan.get("plan_id")):
                    raise CMIRError(
                        E_STALE_CHECKPOINT,
                        "checkpoint head references a different plan snapshot",
                        classification="tamper",
                        head_plan_id=head.get("active_plan_id"),
                        active_plan_id=plan.get("plan_id"),
                    )
            units = plan["units"]
            if unit_index >= len(units):
                raise CMIRError(
                    E_INVALID_STAGE,
                    "checkpoint unit index is outside the active plan",
                    plan_id=plan.get("plan_id"),
                )
            if unit_index != recovery.next_unit_index:
                expected_unit = (
                    units[recovery.next_unit_index]
                    if recovery.next_unit_index < len(units)
                    else None
                )
                raise CMIRError(
                    E_STALE_CHECKPOINT,
                    "checkpoint unit is stale, duplicate, or cross-shard out of order",
                    expected_unit_index=recovery.next_unit_index,
                    actual_unit_index=unit_index,
                    expected_unit=expected_unit,
                    actual_unit=units[unit_index],
                    last_seq=recovery.last_seq,
                )
            if recovery.plan_complete:
                raise CMIRError(
                    E_INVALID_STAGE,
                    "checkpoint plan is already complete",
                    plan_id=plan.get("plan_id"),
                )
            unit = units[unit_index]
            if not self._previous_stage_present(recovery.records, unit):
                raise CMIRError(
                    E_INVALID_STAGE,
                    "previous stage checkpoint is missing",
                    shard_id=unit.get("shard_id"),
                    stage=unit.get("stage"),
                    shard_seq=unit.get("shard_seq"),
                    cycle=unit.get("cycle"),
                )
            object_digest = self.put_object(self.get_object(str(unit["artifact_object"])))
            if object_digest != unit["artifact_object"]:
                raise CMIRError(
                    E_STALE_CHECKPOINT,
                    "artifact object digest changed before publication",
                    expected_artifact_digest=unit["artifact_object"],
                    actual_artifact_digest=object_digest,
                )
            seq = recovery.last_seq + 1
            record: dict[str, Any] = {
                "schema": CHECKPOINT_SCHEMA,
                "seq": seq,
                "shard_id": unit["shard_id"],
                "stage": unit["stage"],
                "shard_seq": int(unit["shard_seq"]),
                "cycle": int(unit["cycle"]),
                "plan_id": plan["plan_id"],
                "plan_index": unit_index,
                "input_digest": unit["input_digest"],
                "config_version": unit.get("config_version", ""),
                "config_digest": unit["config_digest"],
                "quant_param_digest": unit["quant_param_digest"],
                "artifact_digest": object_digest,
                "artifact_object": object_digest,
                "artifact_path": unit["artifact_path"],
                "reused": bool(unit.get("reused", False)),
                "prev_digest": recovery.chain_digest,
            }
            record["record_digest"] = _record_digest(record)
            atomic_write_json(self._record_path(seq), record)
            next_index = unit_index + 1
            complete = next_index >= len(units)
            head = {
                "schema": HEAD_SCHEMA,
                "last_seq": seq,
                "chain_digest": record["record_digest"],
                "active_plan_id": plan["plan_id"],
                "plan_digest": digest_obj(plan),
                "next_unit_index": next_index,
                "plan_complete": complete,
            }
            atomic_write_json(self.head_path, head)
            return record

    def repair_head(self) -> dict[str, Any]:
        """Rebuild the head pointer from the longest valid immutable record prefix."""
        with file_lock(self.lock_path):
            recovery = self._recover_locked()
            plan = recovery.plan
            next_index = recovery.next_unit_index
            complete = recovery.plan_complete
            head = {
                "schema": HEAD_SCHEMA,
                "last_seq": recovery.last_seq,
                "chain_digest": recovery.chain_digest,
                "active_plan_id": plan.get("plan_id") if plan else None,
                "plan_digest": digest_obj(plan) if plan else None,
                "next_unit_index": next_index,
                "plan_complete": complete,
            }
            atomic_write_json(self.head_path, head)
            return {"head": head, "recovery": recovery.summary()}
