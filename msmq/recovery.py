from __future__ import annotations

import json
from collections import Counter
from typing import Any

from .db import Database
from .formats import canonical_json
from .timeutil import now_us


def recover_pending_intents(db: Database) -> dict[str, Any]:
    """Recover only facts that were durably committed before process death.

    A pending intent proves the request had crossed its pre-commit boundary.
    SQLite itself atomically discarded any uncommitted effect transaction.  The
    recovery routine additionally repairs any effect rows linked to a pending
    intent as a defensive consistency check.
    """

    generated_at = now_us()
    details: list[dict[str, Any]] = []
    rolled_back: Counter[str] = Counter()
    corrected_rows = 0
    with db.transaction() as connection:
        pending = connection.execute(
            "SELECT * FROM intents WHERE state = 'pending' ORDER BY created_at_us, id"
        ).fetchall()
        for intent in pending:
            payload = json.loads(intent["payload_json"])
            category = intent["category"]
            effect_rows = 0
            if category == "claim":
                deliveries = connection.execute(
                    "SELECT * FROM deliveries WHERE intent_id = ?", (intent["id"],)
                ).fetchall()
                effect_rows += len(deliveries)
                for delivery in deliveries:
                    prior_attempt = max(0, int(delivery["attempt"]) - 1)
                    connection.execute(
                        """
                        UPDATE messages
                        SET state = 'pending', lease_owner = NULL, lease_token = NULL,
                            lease_expires_at_us = NULL, lease_intent_id = NULL,
                            delivery_count = ?, updated_at_us = ?
                        WHERE id = ? AND lease_intent_id = ?
                        """,
                        (prior_attempt, generated_at, delivery["message_id"], intent["id"]),
                    )
                    connection.execute(
                        "UPDATE deliveries SET status = 'rolled_back' WHERE id = ?",
                        (delivery["id"],),
                    )
                    corrected_rows += 1
            elif category == "ack":
                ack_rows = connection.execute(
                    "SELECT * FROM acks WHERE intent_id = ?", (intent["id"],)
                ).fetchall()
                effect_rows += len(ack_rows)
                pre_state = payload.get("pre_state") or {}
                for ack in ack_rows:
                    connection.execute("DELETE FROM acks WHERE message_id = ? AND consumer_id = ?", (ack["message_id"], ack["consumer_id"]))
                    if pre_state.get("message_id") == ack["message_id"]:
                        connection.execute(
                            """
                            UPDATE messages SET state = ?, lease_owner = ?, lease_token = ?,
                                lease_expires_at_us = ?, delivery_count = ?, last_write_at_us = ?, updated_at_us = ?
                            WHERE id = ?
                            """,
                            (
                                pre_state.get("state", "leased"),
                                pre_state.get("lease_owner"),
                                pre_state.get("lease_token"),
                                pre_state.get("lease_expires_at_us"),
                                int(pre_state.get("delivery_count", ack["delivery_count"])),
                                int(pre_state.get("last_write_at_us", generated_at)),
                                generated_at,
                                ack["message_id"],
                            ),
                        )
                    corrected_rows += 1
            elif category == "enqueue":
                # An enqueue effect and its intent completion share one SQLite
                # transaction.  If the intent is pending, no message row can be
                # committed under this intent.  The intent id is retained in the
                # payload to make the check auditable.
                effect_rows += int(
                    connection.execute(
                        "SELECT COUNT(*) AS n FROM messages WHERE id = ?",
                        (payload.get("message_id"),),
                    ).fetchone()["n"]
                )
            elif category == "import":
                # Batch import state and intent completion are atomic.  A pending
                # import intent therefore has no committed batch effects.  The
                # pre-intent size/digest remains in the durable intent row.
                effect_rows += 0
            rolled_back[category] += 1
            connection.execute(
                """
                UPDATE intents SET state = 'rolled_back', detail_json = ?, updated_at_us = ?
                WHERE id = ? AND state = 'pending'
                """,
                (
                    canonical_json(
                        {
                            "recovery": "rolled_back",
                            "persisted_effect_rows_repaired": effect_rows,
                            "recovered_at_us": generated_at,
                        }
                    ),
                    generated_at,
                    intent["id"],
                ),
            )
            details.append(
                {
                    "intent_id": intent["id"],
                    "category": category,
                    "intent_was_committed_before_crash": False,
                    "disposition": "rolled_back",
                    "persisted_effect_rows_found_or_repaired": effect_rows,
                }
            )
        report = {
            "schema_version": 1,
            "status": "recovered" if pending else "clean",
            "generated_at_us": generated_at,
            "intents_scanned": len(pending),
            "rolled_back_counts": dict(sorted(rolled_back.items())),
            "rolled_back_claims": rolled_back.get("claim", 0),
            "rolled_back_acks": rolled_back.get("ack", 0),
            "rolled_back_enqueues": rolled_back.get("enqueue", 0),
            "rolled_back_imports": rolled_back.get("import", 0),
            "persisted_effect_rows_corrected": corrected_rows,
            "evidence": {
                "source": "sqlite_committed_rows_only",
                "runtime_memory_used": False,
                "uncommitted_transaction_effects_visible": False,
                "crash_exit_report_available": False,
                "statement": (
                    "Recovery decisions use only durable intents and SQLite rows. "
                    "No complete process-exit report is claimed after a forced termination."
                ),
            },
            "details": details,
        }
        connection.execute(
            "INSERT INTO recovery_reports(created_at_us, report_json) VALUES (?, ?)",
            (generated_at, canonical_json(report)),
        )
    return report


def latest_recovery_report(db: Database) -> dict[str, Any] | None:
    return db.latest_recovery_report()
