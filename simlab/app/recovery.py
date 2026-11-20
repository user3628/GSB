"""Crash recovery driven by the persisted intent log.

After a hard kill, any intent still 'pending' means its round never reached
a complete commit.  Recovery marks those rounds ABORTED and rolls fault-domain
quota, failover ops and service states back to the last complete version.
The report only describes persisted, observable facts; it never promises that
a killed process produced a complete exit report.
"""
from __future__ import annotations

import json
import time

from .db import Database


def recover(db: Database) -> dict:
    pending = db.pending_intents()
    report = {
        "scanned_pending_intents": len(pending),
        "aborted_rounds": [],
        "quota_rollbacks": [],
        "failover_rollbacks": [],
        "state_diffs": [],
        "note": "report reflects persisted facts only; a force-killed process is "
                "not guaranteed to have produced a complete exit report",
    }
    if not pending:
        return report

    def _do(conn):
        for intent in pending:
            payload = json.loads(intent["payload"])
            if intent["kind"] != "ROUND_RUN":
                conn.execute("UPDATE intents SET status='aborted', resolved_at=?"
                             " WHERE intent_id=?", (time.time(), intent["intent_id"]))
                continue
            round_id = payload["round_id"]
            rnd = conn.execute("SELECT * FROM rounds WHERE round_id=?",
                               (round_id,)).fetchone()
            if not rnd or rnd["status"] != "RUNNING":
                conn.execute("UPDATE intents SET status='aborted', resolved_at=?"
                             " WHERE intent_id=?", (time.time(), intent["intent_id"]))
                continue
            # rollback quota held by the interrupted round
            held = conn.execute(
                "SELECT id, fault_domain, service, injection_id FROM domain_quota_ledger"
                " WHERE round_id=? AND released_at IS NULL", (round_id,)).fetchall()
            for h in held:
                conn.execute("DELETE FROM domain_quota_ledger WHERE id=?", (h["id"],))
                report["quota_rollbacks"].append({
                    "round_id": round_id, "fault_domain": h["fault_domain"],
                    "service": h["service"], "injection_id": h["injection_id"]})
            # rollback failover ops of the interrupted round
            fos = conn.execute(
                "SELECT op_id, service, status FROM failover_ops WHERE round_id=?",
                (round_id,)).fetchall()
            for fo in fos:
                conn.execute("DELETE FROM failover_ops WHERE op_id=?", (fo["op_id"],))
                report["failover_rollbacks"].append({
                    "round_id": round_id, "service": fo["service"], "status": fo["status"]})
            # service states revert to the last complete version
            partial = conn.execute(
                "SELECT service, state FROM service_states WHERE round_id=?",
                (round_id,)).fetchall()
            conn.execute("DELETE FROM service_states WHERE round_id=?", (round_id,))
            last_complete = conn.execute(
                "SELECT ss.service, ss.state FROM service_states ss JOIN rounds r"
                " ON r.round_id=ss.round_id WHERE r.plan_id=? AND r.status='COMPLETED'"
                " ORDER BY r.round_no DESC", (rnd["plan_id"],)).fetchall()
            restored = {}
            for row in last_complete:
                restored.setdefault(row["service"], row["state"])
            for p in partial:
                before = p["state"]
                after = restored.get(p["service"], "HEALTHY")
                if before != after:
                    report["state_diffs"].append({
                        "service": p["service"], "aborted_state": before,
                        "restored_state": after,
                        "basis": "last committed service_states version"})
            conn.execute(
                "UPDATE injections SET status='ABORTED', skip_reason='ROUND_ABORTED'"
                " WHERE round_id=? AND status IN ('ADMITTED','RUNNING')", (round_id,))
            conn.execute("UPDATE rounds SET status='ABORTED', finished_at=?"
                         " WHERE round_id=?", (time.time(), round_id))
            conn.execute("UPDATE intents SET status='aborted', resolved_at=?"
                         " WHERE intent_id=?", (time.time(), intent["intent_id"]))
            report["aborted_rounds"].append({
                "round_id": round_id, "plan_id": rnd["plan_id"],
                "round_no": rnd["round_no"]})

    db.write(_do)
    return report
