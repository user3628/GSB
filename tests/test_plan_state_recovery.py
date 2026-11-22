from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from capacity_lab.errors import InvalidTransition, PlanInvalid, RoundActive
from capacity_lab.repository import Database

from tests.support import load_fixture


class PlanStateRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.db"
        self.db = Database(self.db_path)
        self.db.initialise()

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_invalid_plan_does_not_change_latest_version(self) -> None:
        fixture = load_fixture("sample_plan.json")
        first = self.db.submit_plan("capacity-demo", fixture)
        self.assertEqual(first["version"], 1)
        stored = first["stored_thresholds"]
        self.assertEqual(stored["error_rate_max"]["fixed"], 10_000)
        self.assertEqual(stored["error_rate_max"]["decimals"], 6)

        invalid = load_fixture("sample_plan.json")
        invalid["stages"][1]["concurrency"] = invalid["stages"][0]["concurrency"]
        with self.assertRaises(PlanInvalid) as context:
            self.db.submit_plan("capacity-demo", invalid)
        self.assertEqual(context.exception.code, "E_PLAN_INVALID")
        latest = self.db.get_plan("capacity-demo")
        self.assertEqual(latest["version"], 1)
        self.assertEqual(latest["fingerprint"], first["fingerprint"])

    def test_invalid_transition_leaves_state_and_conclusion_version_untouched(self) -> None:
        fixture = load_fixture("sample_plan.json")
        plan = self.db.submit_plan("capacity-demo", fixture)
        run = self.db.create_run("capacity-demo", plan["version"])
        stage = plan["stages"][0]
        round_data, created = self.db.trigger_round(
            run_id=run["run_id"],
            stage_key="base:0",
            stage=stage,
            kind="BASE",
            attempt_no=0,
            quantile_method=plan["numeric_contract"]["quantile_method"],
            plan_fingerprint_value=plan["fingerprint"],
        )
        self.assertTrue(created)
        self.assertEqual(round_data["status"], "SAMPLING")
        before = self.db.get_round(round_data["round_id"])
        self.assertNotIn("samples", before)
        self.assertNotIn("conclusion", before)

        with self.assertRaises(InvalidTransition) as context:
            self.db.transition_round(round_data["round_id"], "PENDING", "SAMPLING")
        self.assertEqual(context.exception.code, "E_INVALID_TRANSITION")
        after = self.db.get_round(round_data["round_id"])
        self.assertEqual(after["status"], "SAMPLING")
        self.assertNotIn("conclusion_version", after)
        with closing(sqlite3.connect(self.db_path)) as connection:
            transition_scope = connection.execute(
                """
                SELECT plan_id, plan_version FROM round_transitions
                WHERE entity_type = 'ROUND' AND entity_id = ? AND to_state = 'SAMPLING'
                """,
                (round_data["round_id"],),
            ).fetchone()
        self.assertEqual(transition_scope, ("capacity-demo", 1))

        with self.assertRaises(InvalidTransition):
            self.db.transition_round(round_data["round_id"], "SAMPLING", "CONVERGED")
        self.assertEqual(self.db.get_round(round_data["round_id"])["status"], "SAMPLING")

    def test_duplicate_run_start_is_rejected_while_a_run_is_active(self) -> None:
        fixture = load_fixture("sample_plan.json")
        plan = self.db.submit_plan("capacity-demo", fixture)
        self.db.create_run("capacity-demo", plan["version"])
        with self.assertRaises(RoundActive) as context:
            self.db.create_run("capacity-demo", plan["version"])
        self.assertEqual(context.exception.code, "E_ROUND_ACTIVE")

    def test_restart_recovery_aborts_unfinished_intent_using_persisted_facts_only(self) -> None:
        fixture = load_fixture("sample_plan.json")
        plan = self.db.submit_plan("capacity-demo", fixture)
        run = self.db.create_run("capacity-demo", plan["version"])
        round_data, _ = self.db.trigger_round(
            run_id=run["run_id"],
            stage_key="base:0",
            stage=plan["stages"][0],
            kind="BASE",
            attempt_no=0,
            quantile_method=plan["numeric_contract"]["quantile_method"],
            plan_fingerprint_value=plan["fingerprint"],
        )

        restarted = Database(self.db_path)
        restarted.initialise()
        report = restarted.recover()
        self.assertEqual(report["unresolved_intents"], 1)
        self.assertEqual(len(report["rounds_aborted"]), 1)
        self.assertFalse(report["unsaved_samples_recovered"])
        self.assertEqual(report["persisted_samples_available"], 0)
        self.assertEqual(report["resume_boundary"], "ABORTED")

        aborted_round = restarted.get_round(round_data["round_id"])
        self.assertEqual(aborted_round["status"], "ABORTED")
        self.assertEqual(aborted_round["error_code"], "E_PROCESS_INTERRUPTED")
        self.assertNotIn("samples", aborted_round)
        restored_run = restarted.get_run(run["run_id"])
        self.assertEqual(restored_run["status"], "ABORTED")
        self.assertEqual(restored_run["error_code"], "E_PROCESS_INTERRUPTED")
        latest_report = restarted.get_latest_recovery()
        self.assertEqual(latest_report["unresolved_intents"], 1)
        self.assertIn("无法恢复", latest_report["disclosure"])

        with closing(sqlite3.connect(self.db_path)) as connection:
            intents = connection.execute(
                "SELECT status, error_code FROM write_intents"
            ).fetchall()
        self.assertEqual(intents, [("ABORTED", "E_PROCESS_INTERRUPTED")])


if __name__ == "__main__":
    unittest.main()

