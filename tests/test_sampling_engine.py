from __future__ import annotations

import json
import socket
import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch
from urllib.request import urlopen

from capacity_lab.deterministic_service import start_server
from capacity_lab.engine import CapacityEngine
from capacity_lab.errors import InsufficientSamples, RoundActive
from capacity_lab.models import normalize_plan
from capacity_lab.repository import Database
from capacity_lab.sampler import Sampler
from capacity_lab.verdict import evaluate_round

from tests.support import load_fixture


class SamplingEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service, _thread = start_server(port=0)
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "engine.db"
        self.db = Database(self.db_path)
        self.db.initialise()

    def tearDown(self) -> None:
        self.service.shutdown()
        self.service.server_close()
        self.tempdir.cleanup()

    def _fixture(self, name: str = "sample_plan.json") -> dict:
        plan = load_fixture(name)
        plan["target"] = f"http://127.0.0.1:{self.service.server_port}/load"
        return plan

    def test_repeated_sampling_is_sample_for_sample_identical_and_bounded(self) -> None:
        plan = normalize_plan(self._fixture(), "capacity-demo")
        stage = plan["stages"][0]
        stream = "determinism-stream"
        self.service.stats.reset()
        first, first_diagnostics = Sampler(plan=plan, stage=stage, stream_id=stream).sample()
        second, second_diagnostics = Sampler(plan=plan, stage=stage, stream_id=stream).sample()
        self.assertEqual(
            [sample["latency_us"] for sample in first],
            [sample["latency_us"] for sample in second],
        )
        self.assertEqual(first_diagnostics["sampler_peak_in_flight"], 2)
        self.assertLessEqual(second_diagnostics["sampler_peak_in_flight"], stage["concurrency"])
        first_verdict = evaluate_round(plan, stage, first)
        second_verdict = evaluate_round(plan, stage, second)
        self.assertEqual(first_verdict["computation_digest"], second_verdict["computation_digest"])
        self.assertEqual(first_verdict["metrics"], second_verdict["metrics"])
        self.assertEqual(first_verdict["verdict"], second_verdict["verdict"])
        stats = json.loads(
            urlopen(f"http://127.0.0.1:{self.service.server_port}/stats", timeout=5).read()
        )["stats"]
        self.assertLessEqual(stats["by_stream"][stream]["max_active"], stage["concurrency"])
        self.assertEqual(stats["by_stream"][stream]["total_requests"], stage["sample_size"] * 2)

    def test_timeout_records_error_and_closes_the_underlying_connection(self) -> None:
        plan = normalize_plan(self._fixture(), "capacity-demo")
        stage = plan["stages"][0]
        stage = dict(stage)
        stage["request_timeout_ms"] = 25
        closed = {"value": False}

        class FakeConnection:
            def __init__(self, *args, **kwargs):
                pass

            def request(self, *args, **kwargs):
                return None

            def getresponse(self):
                raise socket.timeout("simulated timeout")

            def close(self):
                closed["value"] = True

        with patch("capacity_lab.sampler.http.client.HTTPConnection", FakeConnection):
            sampler = Sampler(plan=plan, stage=stage, stream_id="timeout-test")
            result = sampler._one_request(0)
        self.assertTrue(result["timed_out"])
        self.assertEqual(result["error_kind"], "timeout")
        self.assertEqual(result["latency_us"], 25_000)
        self.assertTrue(closed["value"])

    def test_full_run_finds_retest_inflection_and_explains_interpolation_difference(self) -> None:
        plan = self.db.submit_plan("capacity-demo", self._fixture())
        engine = CapacityEngine(self.db)
        run = engine.create_and_execute_run("capacity-demo")
        self.assertEqual(run["status"], "CONVERGED")
        self.assertEqual(run["inflection"]["kind"], "BETWEEN_PASS_AND_REJECT")
        self.assertEqual(run["inflection"]["concurrency"], 12)
        self.assertEqual(run["rejected_stage"]["concurrency"], 16)
        self.assertEqual(run["decision_basis"]["outcome"], "DOWNSHIFT_RETEST_PASSED")
        self.assertEqual(len(run["rounds"]), 5)
        verdicts = [(round_data["kind"], round_data["concurrency"], round_data["conclusion"]["verdict"]) for round_data in run["rounds"]]
        self.assertEqual(
            verdicts,
            [
                ("BASE", 2, "PASS"),
                ("BASE", 4, "PASS"),
                ("BASE", 8, "PASS"),
                ("BASE", 16, "REJECT"),
                ("DOWNSHIFT_RETEST", 12, "PASS"),
            ],
        )
        rejected = run["rounds"][3]
        conclusion = rejected["conclusion"]
        self.assertEqual(conclusion["conclusion_version"], 4)
        self.assertEqual(conclusion["plan_version"], plan["version"])
        self.assertEqual(conclusion["quantile_method"], "nearest_rank")
        comparison = conclusion["p99_comparison"]
        self.assertGreater(
            comparison["nearest_minus_linear_fixed"],
            0,
        )
        boundary = conclusion["threshold_checks"]["p99_latency_ms_max"][
            "pass_boundary_fixed"
        ]
        self.assertGreater(comparison["nearest_rank"]["fixed"], boundary)
        self.assertLess(comparison["linear"]["fixed"], boundary)
        ledger = conclusion["metrics"]["error_rate"]["ledger"]
        self.assertGreater(len(ledger["steps"]), 0)
        self.assertEqual(ledger["rounding"], plan["numeric_contract"]["rounding"])
        self.assertEqual(ledger["equivalent_reference"]["fixed"], 0)
        self.assertEqual(run["conclusion_version"], 5)

        duplicate = engine.trigger_manual_stage("capacity-demo", 3)
        self.assertEqual(duplicate["round_id"], rejected["round_id"])
        self.assertEqual(duplicate["status"], "CONVERGED")
        self.assertEqual(duplicate["conclusion"]["conclusion_version"], 4)

        with closing(sqlite3.connect(self.db_path)) as connection:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE round_conclusions SET verdict = 'PASS' WHERE round_id = ?",
                    (rejected["round_id"],),
                )

    def test_insufficient_samples_returns_business_code_and_aborts(self) -> None:
        fixture = self._fixture()
        for stage in fixture["stages"]:
            stage["sample_size"] = 50
        self.db.submit_plan("capacity-demo", fixture)
        with self.assertRaises(InsufficientSamples) as context:
            CapacityEngine(self.db).create_and_execute_run("capacity-demo")
        self.assertEqual(context.exception.code, "E_INSUFFICIENT_SAMPLES")
        run = self.db.latest_run_for_plan("capacity-demo")
        self.assertIsNotNone(run)
        self.assertEqual(run["status"], "ABORTED")
        self.assertEqual(run["error_code"], "E_INSUFFICIENT_SAMPLES")
        self.assertEqual(len(run["rounds"]), 1)
        self.assertEqual(run["rounds"][0]["status"], "ABORTED")
        self.assertEqual(run["rounds"][0]["error_code"], "E_INSUFFICIENT_SAMPLES")

    def test_injected_errors_and_real_timeouts_are_counted_in_verdict(self) -> None:
        self.db.submit_plan("timeout-error-drill", self._fixture("timeout_plan.json"))
        run = CapacityEngine(self.db).create_and_execute_run("timeout-error-drill")
        self.assertEqual(run["status"], "CONVERGED")
        first_round = run["rounds"][0]
        self.assertEqual(first_round["conclusion"]["verdict"], "REJECT")
        details = self.db.get_round(first_round["round_id"], include_samples=True)
        self.assertGreater(details["sample_aggregate"]["timeout_count"], 0)
        self.assertGreater(details["sample_aggregate"]["error_count"], details["sample_aggregate"]["timeout_count"])
        timed_out = [sample for sample in details["samples"] if sample["timed_out"]]
        self.assertGreater(len(timed_out), 0)
        self.assertTrue(all(sample["error_kind"] == "timeout" for sample in timed_out))
        self.assertTrue(all(sample["http_status"] is None for sample in timed_out))
        checks = first_round["conclusion"]["threshold_checks"]
        self.assertTrue(checks["error_rate_max"]["triggered"])
        self.assertFalse(checks["error_rate_max"]["passed"])

    def test_retests_are_distinct_by_plan_version_run_and_round_identifier(self) -> None:
        fixture = self._fixture()
        fixture["stages"] = [fixture["stages"][0]]
        fixture["stages"][0]["service_sleep_ms"] = 0
        plan_v1 = self.db.submit_plan("capacity-demo", fixture)
        engine = CapacityEngine(self.db)
        run_v1_a = engine.create_and_execute_run("capacity-demo")
        run_v1_b = engine.create_and_execute_run("capacity-demo")
        plan_v2 = self.db.submit_plan("capacity-demo", fixture)
        run_v2 = engine.create_and_execute_run("capacity-demo")
        self.assertEqual(plan_v1["version"], 1)
        self.assertEqual(plan_v2["version"], 2)
        self.assertEqual(len({run_v1_a["run_id"], run_v1_b["run_id"], run_v2["run_id"]}), 3)
        self.assertEqual(len({run_v1_a["rounds"][0]["round_id"], run_v1_b["rounds"][0]["round_id"], run_v2["rounds"][0]["round_id"]}), 3)
        self.assertEqual(run_v1_a["rounds"][0]["plan_version"], 1)
        self.assertEqual(run_v1_b["rounds"][0]["plan_version"], 1)
        self.assertEqual(run_v2["rounds"][0]["plan_version"], 2)
        self.assertEqual(run_v2["rounds"][0]["conclusion"]["plan_version"], 2)
        self.assertEqual(
            run_v1_a["rounds"][0]["conclusion"]["metrics"]["sample_digest"],
            run_v1_b["rounds"][0]["conclusion"]["metrics"]["sample_digest"],
        )

    def test_duplicate_stage_trigger_runs_once_and_waits_for_same_result(self) -> None:
        fixture = self._fixture()
        fixture["stages"] = [fixture["stages"][0]]
        fixture["stages"][0]["service_sleep_ms"] = 20
        plan = self.db.submit_plan("capacity-demo", fixture)
        run = self.db.create_run("capacity-demo", plan["version"])
        engine = CapacityEngine(self.db)
        stage = plan["stages"][0]
        barrier = threading.Barrier(2)
        results: list[dict] = []
        errors: list[BaseException] = []

        def invoke() -> None:
            try:
                barrier.wait(timeout=5)
                results.append(
                    engine.execute_stage(
                        run_id=run["run_id"],
                        stage=stage,
                        stage_key="base:0",
                        kind="BASE",
                        attempt_no=0,
                        plan=plan,
                    )
                )
            except BaseException as exc:
                errors.append(exc)

        threads = [threading.Thread(target=invoke) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        round_ids = {result["round_id"] for result in results}
        self.assertEqual(len(round_ids), 1)
        rounds = self.db.get_run(run["run_id"])["rounds"]
        self.assertEqual(len(rounds), 1)
        self.assertEqual(rounds[0]["status"], "CONVERGED")

    def test_different_stage_start_while_sampling_returns_round_active(self) -> None:
        fixture = self._fixture()
        fixture["stages"][0]["service_sleep_ms"] = 25
        plan = self.db.submit_plan("capacity-demo", fixture)
        run = self.db.create_run("capacity-demo", plan["version"])
        engine = CapacityEngine(self.db)
        errors: list[BaseException] = []

        def run_first() -> None:
            try:
                engine.execute_stage(
                    run_id=run["run_id"],
                    stage=plan["stages"][0],
                    stage_key="base:0",
                    kind="BASE",
                    attempt_no=0,
                    plan=plan,
                )
            except BaseException as exc:
                errors.append(exc)

        thread = threading.Thread(target=run_first)
        thread.start()
        deadline = time.time() + 5
        while time.time() < deadline:
            current = self.db.get_run(run["run_id"])["rounds"]
            if current and current[0]["status"] == "SAMPLING":
                break
            time.sleep(0.01)
        with self.assertRaises(RoundActive) as context:
            engine.execute_stage(
                run_id=run["run_id"],
                stage=plan["stages"][1],
                stage_key="base:1",
                kind="BASE",
                attempt_no=0,
                plan=plan,
            )
        self.assertEqual(context.exception.code, "E_ROUND_ACTIVE")
        thread.join(timeout=20)
        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()

