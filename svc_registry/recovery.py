from __future__ import annotations

import json
from typing import Any

from .db import Database, canonical_json, now_ms


class RecoveryManager:
    def __init__(self, database: Database) -> None:
        self.db = database

    def run(self) -> dict[str, Any]:
        started_at = now_ms()
        with self.db.transaction() as connection:
            intents = connection.execute(
                "SELECT * FROM intent_log WHERE status = 'STARTED' ORDER BY id"
            ).fetchall()
            aborted_intents: list[dict[str, Any]] = []
            aborted_imports: list[dict[str, Any]] = []
            aborted_rounds: list[dict[str, Any]] = []
            rolled_back_attempts: list[dict[str, Any]] = []
            for intent in intents:
                details = json.loads(intent["detail_json"])
                intent_type = intent["intent_type"]
                ref_id = intent["ref_id"]
                entry: dict[str, Any] = {
                    "intent_id": intent["id"],
                    "intent_type": intent_type,
                    "ref_id": ref_id,
                    "status": "ABORTED",
                    "created_at_ms": intent["created_at_ms"],
                    "intent_detail": details,
                }
                if intent_type == "PROBE_ROUND" and ref_id:
                    connection.execute(
                        "UPDATE probe_rounds SET status = 'ABORTED', completed_at_ms = ? WHERE round_id = ? AND status = 'PROBING'",
                        (started_at, ref_id),
                    )
                    attempts = connection.execute(
                        "SELECT * FROM probe_attempts WHERE round_id = ? AND state IN ('PENDING', 'RUNNING')",
                        (ref_id,),
                    ).fetchall()
                    for attempt in attempts:
                        retry_payload = {
                            "state": "PENDING_FOR_RETRY",
                            "next_state": "PENDING_FOR_RETRY",
                            "reason": "process terminated before probe round commit",
                            "service_name": attempt["service_name"],
                            "instance_id": attempt["instance_id"],
                            "probe_method": attempt["probe_method"],
                        }
                        connection.execute(
                            """
                            UPDATE probe_attempts SET state = 'ROLLED_BACK', completed_at_ms = ?,
                                result_json = ? WHERE id = ?
                            """,
                            (started_at, canonical_json(retry_payload), attempt["id"]),
                        )
                        rolled_back_attempts.append(
                            {
                                "attempt_id": attempt["id"],
                                "round_id": ref_id,
                                "service_name": attempt["service_name"],
                                "instance_id": attempt["instance_id"],
                                "probe_method": attempt["probe_method"],
                                "state": "ROLLED_BACK",
                                "next_state": "PENDING_FOR_RETRY",
                            }
                        )
                    aborted_rounds.append(
                        {
                            "round_id": ref_id,
                            "status": "ABORTED",
                            "rolled_back_attempts": len(attempts),
                            "committed_history_rows_added": 0,
                        }
                    )
                    entry["probe_round"] = ref_id
                    entry["rolled_back_attempts"] = len(attempts)
                elif intent_type == "IMPORT":
                    batch_id = details.get("batch_id") or ref_id
                    batch = connection.execute(
                        "SELECT * FROM import_batches WHERE batch_id = ?",
                        (batch_id,),
                    ).fetchone()
                    if batch is not None and batch["status"] == "STAGING":
                        abort_report = {
                            "ok": False,
                            "batch_id": batch_id,
                            "status": "aborted",
                            "error_code": "E_IMPORT_ABORTED",
                            "content_hash": batch["content_hash"],
                            "atomic_rollback": True,
                            "message": "process terminated before import commit; no instances were changed",
                            "completed_at_ms": started_at,
                        }
                        connection.execute(
                            """
                            UPDATE import_batches SET status = 'ABORTED', report_json = ?,
                                completed_at_ms = ? WHERE batch_id = ?
                            """,
                            (canonical_json(abort_report), started_at, batch_id),
                        )
                        aborted_imports.append(
                            {
                                "batch_id": batch_id,
                                "status": "ABORTED",
                                "content_hash": batch["content_hash"],
                                "instance_records_changed": 0,
                            }
                        )
                    entry["import_batch"] = batch_id
                    entry["batch_state_after_recovery"] = "ABORTED"
                elif intent_type in ("REGISTER", "RENEW_LEASE"):
                    entry["record_changes_committed"] = 0
                connection.execute(
                    """
                    UPDATE intent_log SET status = 'ABORTED', detail_json = ?, completed_at_ms = ?
                    WHERE id = ?
                    """,
                    (
                        canonical_json(
                            {
                                **details,
                                "recovery": "uncommitted intent aborted after forced termination",
                                "recovered_at_ms": started_at,
                            }
                        ),
                        started_at,
                        intent["id"],
                    ),
                )
                aborted_intents.append(entry)
            completed_at = now_ms()
            report = {
                "ok": True,
                "facts_only": True,
                "started_at_ms": started_at,
                "completed_at_ms": completed_at,
                "scanned_intents": len(intents),
                "aborted_intents": aborted_intents,
                "probe": {
                    "aborted_rounds": aborted_rounds,
                    "rolled_back_attempts": rolled_back_attempts,
                    "instance_health_changes": 0,
                    "uncommitted_probe_results": "rolled_back_to_pending",
                },
                "imports": {
                    "aborted_batches": aborted_imports,
                    "instance_records_changed": 0,
                },
                "lease": {
                    "uncommitted_registration_or_renewal_records_changed": 0,
                },
                "instance_mutations_during_recovery": 0,
                "committed_facts_preserved": True,
            }
            cursor = connection.execute(
                """
                INSERT INTO recovery_reports (started_at_ms, completed_at_ms, report_json)
                VALUES (?, ?, ?)
                """,
                (started_at, completed_at, canonical_json(report)),
            )
            report["recovery_report_id"] = int(cursor.lastrowid)
        return report

    def latest(self) -> dict[str, Any] | None:
        return self.db.recent_recovery_report()
