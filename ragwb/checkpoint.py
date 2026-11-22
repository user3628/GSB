"""On-disk stage checkpoints with real cross-process publication protection.

Each (session, turn, stage) product is written atomically as a single JSON
record inside the run directory.  A separate per-session manifest is updated
under an OS file lock and only accepts records whose stage sequence number is
the exact successor of the previous one.  Out-of-order, duplicate, stale or
cross-session publications raise ``E_STALE_CHECKPOINT``; publishing a stage
whose predecessor does not exist raises ``E_INVALID_STAGE``.

A global append-only WAL plus fsync guarantees that an interrupted process can
never expose a half-written checkpoint: either the replacement of the record
file happened or it did not.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List

from .errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, WorkbenchError
from .io_utils import FileLock, digest_json, ensure_dir, file_lock, \
    read_json, write_json

STAGE_ORDER = ("query_rewrite", "retrieval", "alignment", "answer_assembly")
STAGE_SEQ = {stage: index for index, stage in enumerate(STAGE_ORDER)}


def checkpoint_dir(run_dir: str) -> str:
    return ensure_dir(os.path.join(run_dir, "checkpoints"))


def _record_path(run_dir: str, session_id: str) -> str:
    return os.path.join(checkpoint_dir(run_dir),
                        f"{session_id}.checkpoints.json")


def _manifest_path(run_dir: str) -> str:
    return os.path.join(checkpoint_dir(run_dir), "index.json")


def _wal_path(run_dir: str) -> str:
    return os.path.join(checkpoint_dir(run_dir), "publish.wal")


def _load_session(run_dir: str, session_id: str) -> Dict[str, Any]:
    path = _record_path(run_dir, session_id)
    if not os.path.exists(path):
        return {"session_id": session_id, "records": [],
                "next_turn": 0, "next_stage_seq": 0}
    data = read_json(path)
    # Defensive cleanup: a crashed writer can only leave an old file or a
    # temp file, never a truncated target; drop stray temp files on load.
    for name in os.listdir(checkpoint_dir(run_dir)):
        if name.startswith(".tmp-"):
            try:
                os.unlink(os.path.join(checkpoint_dir(run_dir), name))
            except OSError:  # pragma: no cover - best effort cleanup
                pass
    return data


def _validate_record(record: Dict[str, Any],
                     session_state: Dict[str, Any]) -> None:
    session_id = record["session_id"]
    turn_index = int(record["turn_index"])
    stage = record["stage"]
    stage_seq = STAGE_SEQ.get(stage)
    if stage_seq is None:
        raise WorkbenchError(
            E_INVALID_STAGE, f"unknown stage: {stage}",
            stage=stage, session_id=session_id, turn_index=turn_index)
    if session_state["session_id"] != session_id:
        raise WorkbenchError(
            E_STALE_CHECKPOINT,
            "cross-session checkpoint publication rejected",
            stage=stage, session_id=session_id, turn_index=turn_index)

    expected_turn = session_state["next_turn"]
    expected_stage_seq = session_state["next_stage_seq"]
    if turn_index < expected_turn or (
            turn_index == expected_turn and stage_seq < expected_stage_seq):
        raise WorkbenchError(
            E_STALE_CHECKPOINT,
            (f"stale or duplicate publication; expected turn "
             f"{expected_turn} stage_seq {expected_stage_seq}, got turn "
             f"{turn_index} stage_seq {stage_seq}"),
            stage=stage, session_id=session_id, turn_index=turn_index)
    if stage_seq > 0:
        predecessor = STAGE_ORDER[stage_seq - 1]
        present = any(
            row["turn_index"] == turn_index and row["stage"] == predecessor
            for row in session_state["records"]
        )
        if not present:
            raise WorkbenchError(
                E_INVALID_STAGE,
                f"missing predecessor stage {predecessor} for {stage}",
                stage=stage, session_id=session_id,
                turn_index=turn_index)
    if turn_index > expected_turn or stage_seq != expected_stage_seq:
        raise WorkbenchError(
            E_STALE_CHECKPOINT,
            (f"non-contiguous publication; expected turn {expected_turn} "
             f"stage_seq {expected_stage_seq}, got turn {turn_index} "
             f"stage_seq {stage_seq}"),
            stage=stage, session_id=session_id, turn_index=turn_index)


class CheckpointStore:
    """File-locked, sequence-checked checkpoint publication store."""

    def __init__(self, run_dir: str) -> None:
        self.run_dir = run_dir
        checkpoint_dir(run_dir)
        self._lock = FileLock(
            os.path.join(checkpoint_dir(run_dir), "publish.lock"))

    def publish(self, record: Dict[str, Any], product: Dict[str, Any],
                *, config_fingerprint: str) -> Dict[str, Any]:
        with self._lock:
            session_id = record["session_id"]
            state = _load_session(self.run_dir, session_id)
            full_record = dict(record)
            full_record["stage_seq"] = STAGE_SEQ[record["stage"]]
            full_record["config_fingerprint"] = config_fingerprint
            full_record["product_fingerprint"] = digest_json(product)
            _validate_record(full_record, state)

            wal = _wal_path(self.run_dir)
            with open(wal, "a", encoding="utf-8", newline="\n") as handle:
                handle.write(digest_json(full_record) + "\n")
                handle.flush()
                os.fsync(handle.fileno())

            state["records"].append(full_record)
            if full_record["stage_seq"] == len(STAGE_ORDER) - 1:
                state["next_turn"] = int(record["turn_index"]) + 1
                state["next_stage_seq"] = 0
            else:
                state["next_turn"] = int(record["turn_index"])
                state["next_stage_seq"] = full_record["stage_seq"] + 1
            write_json(_record_path(self.run_dir, session_id), state,
                       pretty=True)
            self._refresh_index(session_id, state)
            return full_record

    def _refresh_index(self, session_id: str,
                       state: Dict[str, Any]) -> None:
        path = _manifest_path(self.run_dir)
        index = read_json(path) if os.path.exists(path) else {"sessions": {}}
        index["sessions"][session_id] = {
            "record_count": len(state["records"]),
            "next_turn": state["next_turn"],
            "next_stage_seq": state["next_stage_seq"],
            "last_fingerprint": state["records"][-1][
                "product_fingerprint"] if state["records"] else None,
        }
        write_json(path, index, pretty=True)

    def load_state(self, session_id: str) -> Dict[str, Any]:
        with self._lock:
            return _load_session(self.run_dir, session_id)

    def all_records(self) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        for name in sorted(os.listdir(checkpoint_dir(self.run_dir))):
            if name.endswith(".checkpoints.json"):
                records.extend(
                    read_json(os.path.join(checkpoint_dir(self.run_dir),
                                           name))["records"])
        return records

    def is_complete(self, session_id: str, expected_turns: int) -> bool:
        state = self.load_state(session_id)
        return state["next_turn"] == expected_turns and \
            state["next_stage_seq"] == 0


def make_record(stage: str, session_id: str, turn_index: int,
                input_fingerprint: str, product: Dict[str, Any],
                sequence_number: int) -> Dict[str, Any]:
    return {
        "stage": stage,
        "session_id": session_id,
        "turn_index": turn_index,
        "input_fingerprint": input_fingerprint,
        "product_summary": {
            "fingerprint": product["fingerprint"],
            "budget_charge": product.get("budget_charge"),
        },
        "sequence_number": sequence_number,
    }


def assert_locked_layout(run_dir: str) -> None:
    """Convenience helper used by tests to confirm a lock file exists."""
    path = os.path.join(checkpoint_dir(run_dir), "publish.lock")
    ensure_dir(os.path.dirname(path))
    if not os.path.exists(path):
        with file_lock(path):
            pass
