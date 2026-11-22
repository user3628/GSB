from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from support import ROOT, mutate_jsonl, predicates, sample_bytes

from joincost.errors import (
    BusinessError,
    E_CONFLICTING_REQUEST,
    E_FORMAT_CORRUPT,
    E_FORMAT_UNSUPPORTED,
    E_REQUEST_ABORTED,
    E_STAT_INVALID,
    E_VERSION_RETIRED,
)
from joincost.service import Service
from joincost.store import Store


class ServiceStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "joincost.db"
        self.store = Store(self.db)
        self.service = Service(self.store, workspace_root=ROOT, recover_on_start=False)
        self.service.import_snapshot_path("samples/stats_v2.bin")

    def tearDown(self):
        self.temp.cleanup()

    def plan(self, request_id: str, version: int = 1001, preds=None, options=None):
        return self.service.submit_plan(
            {
                "request_id": request_id,
                "snapshot_version": version,
                "predicates": predicates() if preds is None else preds,
                "options": options or {"max_relations": 8, "tolerance_ppm": 1000},
            }
        )

    def test_binary_jsonl_interchange_plan_core_is_equal(self):
        binary_plan = self.plan("binary-plan")
        duplicate = self.service.import_snapshot_path("samples/stats_v2.jsonl")
        self.assertTrue(duplicate["replacement"]["duplicate"])
        self.assertEqual(duplicate["import_report"]["decision"], "ACCEPT")
        jsonl_plan = self.plan("jsonl-plan")
        self.assertEqual(binary_plan["ordered_relations"], jsonl_plan["ordered_relations"])
        self.assertEqual(binary_plan["cost"], jsonl_plan["cost"])
        self.assertEqual(binary_plan["plan"]["steps"], jsonl_plan["plan"]["steps"])
        self.assertEqual(
            binary_plan["predicate_selectivity"], jsonl_plan["predicate_selectivity"]
        )

    def test_corrupt_import_writes_no_state(self):
        before_imports = len(self.service.imports()["imports"])
        before_current = self.service.current_snapshot()["current_snapshot"]
        corrupt = bytearray(sample_bytes("stats_v2.bin"))
        corrupt[-1] ^= 0xFF
        with self.assertRaises(BusinessError) as raised:
            self.service.import_snapshot_bytes(bytes(corrupt), source="corrupt.bin")
        self.assertEqual(raised.exception.code, E_FORMAT_CORRUPT)
        self.assertEqual(len(self.service.imports()["imports"]), before_imports)
        self.assertEqual(
            self.service.current_snapshot()["current_snapshot"], before_current
        )

    def test_invalid_stats_keep_old_snapshot(self):
        invalid = mutate_jsonl(
            sample_bytes("stats_v2.jsonl"),
            record_mutator=lambda record: record.__setitem__("row_count", -1),
        )
        with self.assertRaises(BusinessError) as raised:
            self.service.import_snapshot_bytes(invalid, source="invalid.jsonl")
        self.assertEqual(raised.exception.code, E_STAT_INVALID)
        self.assertEqual(
            self.service.current_snapshot()["current_snapshot"]["snapshot_version"], 1001
        )

    def test_overflow_stats_keep_old_snapshot(self):
        invalid = mutate_jsonl(
            sample_bytes("stats_v2.jsonl"),
            record_mutator=lambda record: record.__setitem__("row_count", 2**63),
        )
        with self.assertRaises(BusinessError) as raised:
            self.service.import_snapshot_bytes(invalid, source="overflow.jsonl")
        self.assertEqual(raised.exception.code, E_STAT_INVALID)
        self.assertEqual(
            self.service.current_snapshot()["current_snapshot"]["snapshot_version"], 1001
        )

    def test_unsupported_import_is_recorded_without_replacement(self):
        with self.assertRaises(BusinessError) as raised:
            self.service.import_snapshot_path("samples/stats_v3_unsupported.jsonl")
        self.assertEqual(raised.exception.code, E_FORMAT_UNSUPPORTED)
        self.assertEqual(
            self.service.current_snapshot()["current_snapshot"]["snapshot_version"], 1001
        )
        reports = self.service.imports()["imports"]
        self.assertTrue(any(item["code"] == E_FORMAT_UNSUPPORTED for item in reports))
        rejected = next(item for item in reports if item["code"] == E_FORMAT_UNSUPPORTED)
        self.assertEqual(rejected["report_json"]["decision"], "REJECT")

    def test_older_snapshot_version_is_retired(self):
        with self.assertRaises(BusinessError) as raised:
            self.service.import_snapshot_path("samples/stats_v1_legacy.bin")
        self.assertEqual(raised.exception.code, E_VERSION_RETIRED)
        self.assertEqual(
            self.service.current_snapshot()["current_snapshot"]["snapshot_version"], 1001
        )

    def test_legacy_import_reports_are_persisted_with_degradation_details(self):
        for filename in ("stats_v1_legacy.bin", "stats_v1_legacy.jsonl"):
            with self.subTest(filename=filename):
                legacy_store = Store(Path(self.temp.name) / f"legacy-{filename}.db")
                legacy_service = Service(
                    legacy_store, workspace_root=ROOT, recover_on_start=False
                )
                result = legacy_service.import_snapshot_path(f"samples/{filename}")
                self.assertEqual(result["import_report"]["decision"], "DEGRADE_ACCEPT")
                reports = legacy_service.imports()["imports"]
                self.assertTrue(
                    any(
                        item["report_json"]["decision"] == "DEGRADE_ACCEPT"
                        and item["report_json"]["degradations"]
                        for item in reports
                    )
                )

    def test_snapshot_replacement_is_whole_and_old_bound_plan_survives(self):
        old_plan = self.plan("old-bound")
        replacement = self.service.import_snapshot_path("samples/stats_replacement.bin")
        self.assertEqual(replacement["replacement"]["current_version"], 1002)
        current = self.service.current_snapshot()["current_snapshot"]
        self.assertEqual(current["snapshot_version"], 1002)
        self.assertEqual(current["digest"], replacement["replacement"]["digest"])
        self.assertEqual(self.service.get_plan("old-bound")["snapshot_version"], 1001)
        self.assertEqual(self.service.get_plan("old-bound")["cost"], old_plan["cost"])
        new_plan = self.plan("new-bound", version=1002)
        self.assertEqual(new_plan["snapshot_version"], 1002)
        self.assertNotEqual(new_plan["snapshot_digest"], old_plan["snapshot_digest"])

    def test_request_conflict_and_duplicate_reuse(self):
        first = self.plan("same-id")
        second = self.plan("same-id")
        self.assertEqual(first, second)
        with self.assertRaises(BusinessError) as raised:
            self.plan("same-id", preds=predicates()[:-1])
        self.assertEqual(raised.exception.code, E_CONFLICTING_REQUEST)

    def test_concurrent_duplicate_request_executes_once(self):
        service = Service(self.store, workspace_root=ROOT, build_delay_ms=150)
        payload = {
            "request_id": "concurrent",
            "snapshot_version": 1001,
            "predicates": predicates(),
            "options": {"max_relations": 8, "tolerance_ppm": 1000},
        }
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(lambda _: service.submit_plan(payload), range(5)))
        self.assertTrue(all(result == results[0] for result in results))
        status = service.request_status("concurrent")["request"]
        self.assertEqual(status["status"], "COMPLETED")

    def test_import_during_build_completes_on_bound_version(self):
        slow_service = Service(self.store, workspace_root=ROOT, build_delay_ms=500)
        other_service = Service(self.store, workspace_root=ROOT, recover_on_start=False)
        result_holder = {}

        def build():
            try:
                result_holder["plan"] = slow_service.submit_plan(
                    {
                        "request_id": "retired-during-build",
                        "snapshot_version": 1001,
                        "predicates": predicates(),
                    }
                )
            except Exception as exc:
                result_holder["error"] = exc

        thread = threading.Thread(target=build)
        thread.start()
        time.sleep(0.15)
        other_service.import_snapshot_path("samples/stats_replacement.bin")
        thread.join(5)
        self.assertNotIn("error", result_holder)
        plan = result_holder["plan"]
        self.assertEqual(plan["snapshot_version"], 1001)
        self.assertTrue(plan["version_binding"]["version_retired_during_build"])
        self.assertTrue(plan["version_binding"]["completed_on_bound_snapshot"])
        self.assertEqual(
            self.service.current_snapshot()["current_snapshot"]["snapshot_version"], 1002
        )

    def test_pending_plan_intent_becomes_aborted(self):
        self.store.begin_plan_request(
            request_id="crash-me",
            request_hash="hash",
            snapshot_version=1001,
            snapshot_digest=self.service.current_snapshot()["current_snapshot"]["digest"],
        )
        recovery = Store(self.db).recover()
        self.assertEqual(recovery["current_version_before"], 1001)
        self.assertEqual(recovery["current_version_after"], 1001)
        self.assertIn(
            "crash-me", [item["request_id"] for item in recovery["aborted_plans"]]
        )
        request = self.store.get_plan_request("crash-me")
        self.assertEqual(request["status"], "ABORTED")
        self.assertEqual(request["error"]["error"]["code"], E_REQUEST_ABORTED)

    def test_pending_snapshot_intent_aborts_and_preserves_old_snapshot(self):
        self.store.write_intent(
            intent_id="snapshot-pending",
            kind="snapshot_commit",
            reference_version=1999,
            payload={"snapshot_version": 1999, "digest": "not-committed"},
        )
        recovery = Store(self.db).recover()
        self.assertEqual(recovery["current_version_before"], 1001)
        self.assertEqual(recovery["current_version_after"], 1001)
        self.assertTrue(recovery["differences"])

    def test_recovery_rolls_back_dangling_current_pointer(self):
        connection = sqlite3.connect(self.db)
        connection.execute(
            "UPDATE current_snapshot SET version = 1999, digest = 'dangling' WHERE singleton = 1"
        )
        connection.commit()
        connection.close()
        recovery = Store(self.db).recover()
        self.assertEqual(recovery["current_version_after"], 1001)
        self.assertTrue(
            any(item["kind"] == "current_snapshot_rolled_back" for item in recovery["differences"])
        )

    def test_recovery_restores_missing_current_pointer(self):
        connection = sqlite3.connect(self.db)
        connection.execute("DELETE FROM current_snapshot")
        connection.commit()
        connection.close()
        recovery = Store(self.db).recover()
        self.assertEqual(recovery["current_version_after"], 1001)
        self.assertTrue(
            any(item["kind"] == "current_snapshot_recovered" for item in recovery["differences"])
        )

    def test_snapshot_commit_intent_is_completed(self):
        connection = sqlite3.connect(self.db)
        rows = connection.execute(
            "SELECT kind, status FROM intents ORDER BY created_at"
        ).fetchall()
        connection.close()
        self.assertIn(("snapshot_commit", "COMPLETED"), rows)


if __name__ == "__main__":
    unittest.main()

