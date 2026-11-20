"""Crash recovery: scan pending merge intents on startup / on demand.

The component never promises a clean exit report after SIGKILL. Recovery only
states facts durable at scan time:

* a 'pending' intent exists;
* the session was left in 'merging';
* no object metadata was committed for that intent (the metadata and the
  intent's 'committed' mark share one transaction, so either both exist or
  neither does).

Policy: pending intents are re-opened for retry. Re-running the merge then
recomputes the digest from the same persisted parts, yielding an identical
object digest. Intents older than their session's terminal state (defensive
edge case) are marked aborted instead.
"""
from __future__ import annotations

import json
import uuid

from app.config import Settings
from app.db.connection import connect, transaction
from app.repositories import IntentRepository, ObjectRepository, SessionRepository


class RecoveryService:
    def __init__(self, settings: Settings):
        self.settings = settings

    def recover(self) -> dict:
        with connect(self.settings) as conn, transaction(conn, immediate=True):
            intent_repo = IntentRepository(conn)
            session_repo = SessionRepository(conn)
            object_repo = ObjectRepository(conn)

            pending = intent_repo.list_pending()
            actions = []
            recovered = 0
            aborted = 0

            for intent in pending:
                session = session_repo.get(intent["session_id"])
                current = object_repo.get_current(intent["object_key"])
                entry = {
                    "intent_id": intent["intent_id"],
                    "session_id": intent["session_id"],
                    "object_key": intent["object_key"],
                    "strategy": intent["strategy"],
                    "intent_created_at": intent["created_at"],
                    "session_status_at_scan": session["status"] if session else None,
                    "object_visible_at_scan": current is not None,
                    "facts": [
                        "merge intent persisted in state 'pending'",
                        "no metadata transaction committed for this intent"
                        if current is None
                        else "current object exists from another committed merge",
                    ],
                }

                if session is not None and session["status"] == "merging":
                    session_repo.update_status(intent["session_id"], "open")
                    intent_repo.mark(
                        intent["intent_id"], "aborted",
                        detail={**entry, "resolution": "session reopened for retry; "
                                                        "this intent is superseded"},
                    )
                    entry["resolution"] = "reopened_for_retry"
                    recovered += 1
                else:
                    intent_repo.mark(
                        intent["intent_id"], "aborted",
                        detail={**entry, "resolution": "session not in merging state; "
                                                        "intent marked aborted"},
                    )
                    entry["resolution"] = "aborted"
                    aborted += 1
                actions.append(entry)

            report_id = "recovery-" + uuid.uuid4().hex
            report = {
                "report_id": report_id,
                "scanned_pending_intents": len(pending),
                "reopened_for_retry": recovered,
                "aborted": aborted,
                "actions": actions,
                "note": "report derived solely from persisted facts; a process "
                        "killed mid-merge cannot emit a clean exit report",
            }
            intent_repo.save_recovery_report(
                report_id, len(pending), recovered, aborted, report
            )
            return report

    def list_reports(self) -> list[dict]:
        with connect(self.settings) as conn:
            rows = IntentRepository(conn).list_recovery_reports()
            return [
                {
                    "report_id": r["report_id"],
                    "created_at": r["created_at"],
                    "scanned": r["scanned"],
                    "recovered": r["recovered"],
                    "aborted": r["aborted"],
                    "detail": json.loads(r["detail_json"]),
                }
                for r in rows
            ]
