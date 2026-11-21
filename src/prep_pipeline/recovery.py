from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from .config import Settings
from .storage import Database, INCOMPLETE_BATCH_STATUSES, utc_now


def _tree_probe(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "existed": False, "file_count": 0, "byte_count": 0}
    files = [candidate for candidate in path.rglob("*") if candidate.is_file()]
    return {
        "path": str(path),
        "existed": True,
        "file_count": len(files),
        "byte_count": sum(candidate.stat().st_size for candidate in files),
    }


def _remove_tree(path: Path) -> None:
    if path.is_symlink():
        path.unlink(missing_ok=True)
    elif path.exists():
        shutil.rmtree(path)


class RecoveryManager:
    def __init__(self, settings: Settings, db: Database):
        self.settings = settings
        self.db = db

    def scan(self) -> list[dict[str, Any]]:
        with self.db.read_transaction() as conn:
            rows = conn.execute(
                "SELECT batch_id FROM batches WHERE status IN ('reserved', 'running', 'committing') "
                "ORDER BY created_at"
            ).fetchall()
        reports: list[dict[str, Any]] = []
        for row in rows:
            report = self._recover_one(row["batch_id"])
            if report is not None:
                reports.append(report)
        self._remove_orphan_locks()
        return reports

    def _recover_one(self, batch_id: str) -> dict[str, Any] | None:
        with self.db.read_transaction() as conn:
            batch_row = conn.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if batch_row is None or batch_row["status"] not in INCOMPLETE_BATCH_STATUSES:
                return None
            intent_row = conn.execute(
                "SELECT * FROM intents WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            stage_rows = conn.execute(
                "SELECT * FROM stage_facts WHERE batch_id = ? ORDER BY stage_seq",
                (batch_id,),
            ).fetchall()
        batch = dict(batch_row)
        intent = dict(intent_row) if intent_row is not None else None
        stage_facts = [dict(row) for row in stage_rows]
        completed = [fact for fact in stage_facts if fact["status"] == "completed"]
        incomplete = next((fact for fact in stage_facts if fact["status"] != "completed"), None)
        last_complete = completed[-1] if completed else None

        staging_path = self.settings.staging_dir / batch["attempt_id"]
        published_path = self.settings.published_dir / batch_id
        staging_probe = _tree_probe(staging_path)
        published_probe = _tree_probe(published_path)

        _remove_tree(staging_path)
        _remove_tree(published_path)
        staging_after = _tree_probe(staging_path)
        published_after = _tree_probe(published_path)

        report_id = f"recovery_{batch_id}"
        generated_at = utc_now()
        report: dict[str, Any] = {
            "report_id": report_id,
            "batch_id": batch_id,
            "intent_id": batch["intent_id"],
            "generated_at": generated_at,
            "recovery_trigger": "scan of persisted incomplete intent/batch state",
            "previous_status": batch["status"],
            "recovered_status": "aborted",
            "clean_shutdown_report_available": False,
            "exit_report_available": False,
            "final_commit_observed": False,
            "source_path": batch["input_path"],
            "source_sha256": batch["source_sha256"],
            "policy_version": batch["policy_version"],
            "intent": (
                {
                    "status": intent["status"],
                    "created_at": intent["created_at"],
                    "updated_at": intent["updated_at"],
                    "request_json": json.loads(intent["request_json"]),
                }
                if intent is not None
                else None
            ),
            "completed_stages": [
                {
                    "stage": fact["stage_name"],
                    "sequence": fact["stage_seq"],
                    "input_digest": fact["input_digest"],
                    "output_digest": fact["output_digest"],
                    "stats": json.loads(fact["stats_json"]) if fact["stats_json"] else None,
                }
                for fact in completed
            ],
            "last_complete_stage": (
                {
                    "stage": last_complete["stage_name"],
                    "sequence": last_complete["stage_seq"],
                    "output_digest": last_complete["output_digest"],
                }
                if last_complete is not None
                else None
            ),
            "incomplete_stage": (
                {
                    "stage": incomplete["stage_name"],
                    "sequence": incomplete["stage_seq"],
                    "status": incomplete["status"],
                    "input_digest": incomplete["input_digest"],
                    "error_code": incomplete["error_code"],
                }
                if incomplete is not None
                else None
            ),
            "filesystem_facts": {
                "staging_before_recovery": staging_probe,
                "published_before_recovery": published_probe,
                "staging_after_recovery": staging_after,
                "published_after_recovery": published_after,
                "staging_removed": bool(staging_probe["existed"] and not staging_after["existed"]),
                "published_uncommitted_removed": bool(
                    published_probe["existed"] and not published_after["existed"]
                ),
                "removal_incomplete": bool(
                    staging_after["existed"] or published_after["existed"]
                ),
            },
            "committed_shards": [],
            "uncommitted_artifacts_recoverable": False,
            "recovery_conclusion": (
                "aborted: no final batch commit was observed; only persisted intent and "
                "completed stage facts were used. Unpublished staging artifacts are not "
                "promised to be recoverable."
            ),
            "fact_sources": [
                "sqlite:intents",
                "sqlite:batches",
                "sqlite:stage_facts",
                "filesystem:existence_and_byte_probe",
            ],
        }

        error_payload = {
            "error": {
                "code": "E_RECOVERY_ABORTED",
                "message": "incomplete batch was aborted based on persisted facts",
                "context": {
                    "batch_id": batch_id,
                    "last_complete_stage": (
                        last_complete["stage_name"] if last_complete is not None else None
                    ),
                    "recovery_report_id": report_id,
                    "exit_report_available": False,
                },
            }
        }
        with self.db.write_transaction() as conn:
            current = conn.execute(
                "SELECT status, policy_version, attempt_id FROM batches WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            if current is None or current["status"] not in INCOMPLETE_BATCH_STATUSES:
                return None
            now = utc_now()
            conn.execute(
                """
                UPDATE batches
                SET status = 'aborted', error_code = 'E_RECOVERY_ABORTED',
                    error_json = ?, updated_at = ?
                WHERE batch_id = ?
                """,
                (
                    json.dumps(error_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                    now,
                    batch_id,
                ),
            )
            conn.execute(
                "UPDATE intents SET status = 'aborted', updated_at = ? WHERE batch_id = ?",
                (now, batch_id),
            )
            conn.execute(
                "DELETE FROM policy_locks WHERE policy_version = ? AND attempt_id = ?",
                (current["policy_version"], current["attempt_id"]),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO recovery_reports(
                    report_id, batch_id, intent_id, created_at, report_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    report_id,
                    batch_id,
                    batch["intent_id"],
                    generated_at,
                    json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                ),
            )
        return report

    def _remove_orphan_locks(self) -> None:
        with self.db.write_transaction() as conn:
            conn.execute(
                """
                DELETE FROM policy_locks
                WHERE batch_id NOT IN (
                    SELECT batch_id FROM batches
                    WHERE status IN ('reserved', 'running', 'committing')
                )
                """
            )

    def reports(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        return self.db.get_recovery_reports(batch_id)
