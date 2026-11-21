import json
import os
import shutil
import tempfile
import threading
import unittest

from tls_workbench import states
from tls_workbench.engine import HandshakeConfig, HandshakeEngine, SampleStore
from tls_workbench.errors import Codes, WorkbenchError
from tls_workbench.persistence import Store
from tls_workbench.samplegen import generate_all


class EngineFixture(unittest.TestCase):
    def setUp(self):
        self.base_dir = tempfile.mkdtemp(prefix="tls-wb-engine-")
        generate_all(self.base_dir)
        self.db_path = os.path.join(self.base_dir, "data", "test.sqlite3")
        self.store = Store(self.db_path)
        self.samples = SampleStore(self.base_dir)
        self.engine = HandshakeEngine(self.store, self.samples)
        with open(os.path.join(self.base_dir, "samples", "handshake_samples.json"),
                  encoding="utf-8") as handle:
            self.sample_map = {
                item["sample_id"]: item
                for item in json.load(handle)["samples"]
            }

    def tearDown(self):
        self.store.close()
        shutil.rmtree(self.base_dir, ignore_errors=True)

    def start(self, sample_id, session_id="s1", config=None):
        params = dict(self.sample_map[sample_id])
        self.engine.create_session(session_id, params["chain_id"], params)
        return self.engine.get_or_advance(session_id, params, config), params


class StateMachineTests(EngineFixture):
    def test_legal_path(self):
        self.assertTrue(states.can_transition("INIT", states.CLIENT_HELLO))
        self.assertTrue(states.can_transition(states.CERT_VERIFY, states.FINISHED))

    def test_illegal_transition(self):
        with self.assertRaises(WorkbenchError) as ctx:
            states.assert_transition(states.CLIENT_HELLO, states.FINISHED)
        self.assertEqual(ctx.exception.code, Codes.INVALID_TRANSITION)
        with self.assertRaises(WorkbenchError):
            states.assert_transition(states.FINISHED, states.CLIENT_HELLO)


class HandshakeTests(EngineFixture):
    def test_normal_handshake_persists_five_stages(self):
        result, _ = self.start("normal_handshake")
        self.assertEqual(result["session_status"], "COMPLETED")
        self.assertEqual(
            [row["stage"] for row in result["stage_states"]], list(states.STAGES)
        )
        self.assertTrue(all(row["status"] == "OK" for row in result["stage_states"]))

    def test_budget_exceeded_reports_trigger(self):
        params = dict(self.sample_map["over_budget_handshake"])
        self.engine.create_session("budget", params["chain_id"], params)
        with self.assertRaises(WorkbenchError) as ctx:
            self.engine.get_or_advance("budget", params)
        self.assertEqual(ctx.exception.code, Codes.BUDGET_EXCEEDED)
        self.assertEqual(ctx.exception.details["trigger_stage"], "TOTAL")
        self.assertGreater(
            ctx.exception.details["actual_ms_fixed"],
            ctx.exception.details["limit_ms_fixed"],
        )
        # All five stage boundaries are still committed before adjudication.
        self.assertEqual(len(self.store.get_stages("budget")), len(states.STAGES))
        persisted = self.store.get_diagnostic("budget")["payload"]
        self.assertTrue(
            any(item["code"] == Codes.BUDGET_EXCEEDED
                for item in persisted["failures"])
        )

    def test_budget_shares_and_total_are_deterministic(self):
        first, _ = self.start("normal_handshake", session_id="a")
        second, _ = self.start("normal_handshake", session_id="b")
        self.assertEqual(
            first["budget_report"]["total_budget_ms_fixed"],
            second["budget_report"]["total_budget_ms_fixed"],
        )
        self.assertEqual(
            [row["share_percent_fixed"] for row in first["budget_report"]["stages"]],
            [row["share_percent_fixed"] for row in second["budget_report"]["stages"]],
        )

    def test_invalid_transition_does_not_mutate_persisted_state(self):
        params = dict(self.sample_map["normal_handshake"])
        self.engine.create_session("illegal", params["chain_id"], params)
        with self.assertRaises(WorkbenchError) as ctx:
            states.assert_transition("INIT", states.FINISHED)
        self.assertEqual(ctx.exception.code, Codes.INVALID_TRANSITION)
        session = self.store.get_session("illegal")
        self.assertEqual(session["current_stage"], "INIT")
        self.assertEqual(self.store.get_stages("illegal"), [])

    def test_continue_on_failure_records_reason_and_finishes_diagnostics(self):
        result, _ = self.start(
            "clock_skewed_handshake",
            session_id="skew",
            config=HandshakeConfig(continue_on_failure=True),
        )
        self.assertEqual(result["session_status"], "COMPLETED_WITH_FAILURES")
        codes = {item["code"] for item in result["failures"]}
        self.assertIn(Codes.CERT_EXPIRED, codes)

    def test_downgrade_protects_critical_checks(self):
        from tls_workbench.budget import apply_downgrade
        with self.assertRaises(WorkbenchError) as ctx:
            apply_downgrade(["CERT_SIGNATURE_VERIFICATION"])
        self.assertEqual(ctx.exception.code, Codes.DOWNGRADE_FORBIDDEN)

    def test_conflicting_session_params_rejected(self):
        params = dict(self.sample_map["normal_handshake"])
        self.engine.create_session("conflict", params["chain_id"], params)
        changed = dict(params)
        changed["sni"] = "different.example"
        with self.assertRaises(WorkbenchError) as ctx:
            self.engine.get_or_advance("conflict", changed)
        self.assertEqual(ctx.exception.code, Codes.CONFLICTING_SESSION)

    def test_repeated_advance_executes_once_and_reuses_result(self):
        params = dict(self.sample_map["normal_handshake"])
        self.engine.create_session("once", params["chain_id"], params)
        first = self.engine.get_or_advance("once", params)
        second = self.engine.get_or_advance("once", params)
        self.assertIs(first, second)
        self.assertEqual(len(self.store.get_stages("once")), len(states.STAGES))


class RecoveryTests(EngineFixture):
    def test_pending_intent_aborts_at_last_boundary(self):
        params = dict(self.sample_map["normal_handshake"])
        session_id = "crash"
        self.engine.create_session(session_id, params["chain_id"], params)
        chain = self.samples.load(params["chain_id"])
        from tls_workbench import timeutil
        verify_instant = timeutil.build_verification_instant(
            params["verification_wall_ms"], params["clock_offset_ms"]
        )
        for stage in states.STAGES[:2]:
            detail, status = self.engine._execute_stage(
                stage, chain, verify_instant, params,
                self.engine._build_estimates(params, []), [], {},
            )
            self.store.append_stage(session_id, stage, status, detail)
        self.store._write_intent(
            session_id, "ADVANCE_STAGE", states.STAGES[2], {"kill": True}
        )
        self.store.close()
        restarted = Store(self.db_path)
        report = restarted.recover_pending()
        self.assertEqual(len(report["actions"]), 1)
        session = restarted.get_session(session_id)
        self.assertEqual(session["status"], "ABORTED")
        self.assertEqual(session["current_stage"], states.SERVER_HELLO)
        # Recovery is idempotent: a second scan finds nothing pending.
        second = restarted.recover_pending()
        self.assertEqual(second["actions"], [])
        restarted.close()


class ConcurrencyTests(EngineFixture):
    def test_concurrent_advancement_runs_handshake_once(self):
        params = dict(self.sample_map["normal_handshake"])
        session_id = "parallel"
        self.engine.create_session(session_id, params["chain_id"], params)
        results = []
        errors = []

        def worker():
            try:
                results.append(self.engine.get_or_advance(session_id, params))
            except WorkbenchError as exc:  # pragma: no cover - failure path
                errors.append(exc.code)

        threads = [threading.Thread(target=worker) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 6)
        self.assertEqual(len(self.store.get_stages(session_id)), len(states.STAGES))
        versions = {item["version"] for item in results}
        self.assertEqual(versions, {len(states.STAGES)})


if __name__ == "__main__":
    unittest.main()
