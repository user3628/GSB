"""Crash recovery: reconcile durable facts and resolve unfinished intents."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List

from .errors import CheckpointInvalidError
from .sqlite_store import SqliteStore
from .wal import WalManager


@dataclass
class RecoveryReport:
    started: bool = False
    durable_next_lsn: int = 0
    resolved_intents: List[Dict[str, Any]] = field(default_factory=list)
    rolled_back_checkpoint: Any = None
    active_checkpoint_seq: Any = None
    corrupt_segments: List[str] = field(default_factory=list)
    actions: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "started": self.started,
            "durable_next_lsn": self.durable_next_lsn,
            "resolved_intents": self.resolved_intents,
            "rolled_back_checkpoint": self.rolled_back_checkpoint,
            "active_checkpoint_seq": self.active_checkpoint_seq,
            "corrupt_segments": self.corrupt_segments,
            "actions": self.actions,
        }


class RecoveryManager:
    def __init__(self, store: SqliteStore, wal: WalManager):
        self.store = store
        self.wal = wal

    def _last_committed_checkpoint(self):
        row = self.store.latest_checkpoint_row()
        if row is None:
            return None
        cp = self.store.row_to_checkpoint(row)
        if not cp.verify():
            raise CheckpointInvalidError(
                "latest committed checkpoint failed checksum during recovery",
                {"seq": cp.seq},
            )
        return cp

    def recover(self) -> RecoveryReport:
        report = RecoveryReport(started=True)

        # 1. WAL reconciliation: durable high-water mark becomes the only truth.
        self.wal.rescan()
        report.durable_next_lsn = self.wal.next_lsn
        report.corrupt_segments = [
            s.name for s in self.wal.segments() if s.corrupt
        ]

        # 2. Resolve every OPEN intent strictly by what is durable.
        for row in self.store.open_intents():
            kind = row["kind"]
            intent_id = row["intent_id"]
            payload = json.loads(row["payload"])
            entry: Dict[str, Any] = {
                "intent_id": intent_id,
                "kind": kind,
                "payload": payload,
            }

            if kind == "WAL_APPEND":
                # A missing durable record means the append never completed.
                wanted = int(payload.get("lsn", -1))
                durable = self.wal.max_durable_lsn()
                if wanted <= durable:
                    entry["decision"] = "already_durable"
                else:
                    entry["decision"] = "aborted"
                    entry["note"] = (
                        f"lsn {wanted} never reached disk; high-water mark {durable}"
                    )
                self.store.update_intent(intent_id, "ABORTED" if wanted > durable else "COMMITTED")
                report.actions.append(f"resolved {kind} intent {intent_id} -> {entry['decision']}")

            elif kind == "CHECKPOINT":
                seq = int(payload.get("seq"))
                existing = self.store.conn.execute(
                    "SELECT status FROM checkpoints WHERE seq=?", (seq,)
                ).fetchone()
                cp = self._last_committed_checkpoint()
                if existing is not None and existing["status"] == "COMMITTED" and cp is not None and cp.seq == seq and cp.verify():
                    entry["decision"] = "committed"
                    self.store.update_intent(intent_id, "COMMITTED")
                else:
                    # Roll pages and checkpoint pointer back to the last good one.
                    if existing is not None:
                        self.store.set_checkpoint_status(seq, "ABORTED")
                    if cp is None:
                        self.store.reset_pages({})
                        report.rolled_back_checkpoint = None
                    else:
                        self.store.reset_pages(cp.pages)
                        report.rolled_back_checkpoint = cp.seq
                    entry["decision"] = "rolled_back"
                    entry["restored_checkpoint_seq"] = None if cp is None else cp.seq
                    self.store.update_intent(intent_id, "ABORTED")
                report.actions.append(f"resolved {kind} intent {intent_id} -> {entry['decision']}")

            elif kind == "TRUNCATE":
                entry["decision"] = "aborted"
                self.store.update_intent(intent_id, "ABORTED")
                report.actions.append(f"resolved {kind} intent {intent_id} -> aborted")

            elif kind == "REPLAY":
                entry["decision"] = "aborted"
                cp = self._last_committed_checkpoint()
                if cp is None:
                    self.store.reset_pages({})
                else:
                    self.store.reset_pages(cp.pages)
                report.rolled_back_checkpoint = None if cp is None else cp.seq
                self.store.update_intent(intent_id, "ABORTED")
                report.actions.append(f"resolved {kind} intent {intent_id} -> aborted, pages restored")

            else:
                entry["decision"] = "aborted_unknown"
                self.store.update_intent(intent_id, "ABORTED")

            report.resolved_intents.append(entry)

        # 3. Any OPEN (non-committed) checkpoint rows left without an intent are aborted.
        for row in self.store.conn.execute(
            "SELECT seq FROM checkpoints WHERE status='OPEN'"
        ).fetchall():
            self.store.set_checkpoint_status(row["seq"], "ABORTED")
            report.actions.append(f"marked dangling checkpoint {row['seq']} ABORTED")

        cp = self._last_committed_checkpoint()
        report.active_checkpoint_seq = None if cp is None else cp.seq
        return report
