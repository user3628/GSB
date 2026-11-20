from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from lease_lock.engine import LeaseEngine
from lease_lock.errors import ApiError


ROOT = Path(__file__).resolve().parents[1]
SAMPLE = json.loads((ROOT / "samples" / "deterministic_leases.json").read_text(encoding="utf-8"))


class InjectedCrash(BaseException):
    pass


class LeaseEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "leases.db"
        self.engine = LeaseEngine(self.db_path)
        loaded = self.engine.replace_snapshot(0, SAMPLE["leases"])
        self.assertEqual(loaded["snapshot_version"], 1)

    def tearDown(self) -> None:
        self.engine.close()
        self.tempdir.cleanup()

    def test_acquire_held_and_strict_renew(self) -> None:
        acquired = self.engine.acquire("exclusive", "holder-a", 5000)
        self.assertEqual(acquired["snapshot_version"], 2)
        initial_version = acquired["lease"]["lease_version"]

        with self.assertRaises(ApiError) as held:
            self.engine.acquire("exclusive", "holder-b", 5000)
        self.assertEqual(held.exception.code, "E_LOCK_HELD")
        self.assertEqual(held.exception.details["holder"], "holder-a")
        self.assertGreater(held.exception.details["remaining_ttl_ms"], 0)
        self.assertEqual(self.engine.snapshot_version, 2)

        with self.assertRaises(ApiError) as stale:
            self.engine.renew("exclusive", "holder-a", initial_version + 100, 5000)
        self.assertEqual(stale.exception.code, "E_STALE_LEASE")
        self.assertEqual(self.engine.snapshot_version, 2)

        renewed = self.engine.renew("exclusive", "holder-a", initial_version, 5000)
        self.assertEqual(renewed["lease"]["lease_version"], initial_version + 1)
        self.assertEqual(renewed["snapshot_version"], 3)

    def test_expired_lease_can_be_reacquired_with_new_version(self) -> None:
        first = self.engine.acquire("short", "holder-a", 1000)
        self.engine.advance_clock(1001)
        second = self.engine.acquire("short", "holder-b", 1000)
        self.assertGreater(
            second["lease"]["lease_version"], first["lease"]["lease_version"]
        )
        self.assertEqual(second["lease"]["holder"], "holder-b")

    def test_incremental_ttl_change_rebuilds_only_downstream_cone(self) -> None:
        result = self.engine.commit_increment(
            1,
            [SAMPLE["incremental_ttl_change"]],
        )
        self.assertEqual(result["snapshot_version"], 2)
        self.assertEqual(set(result["affected_leases"]), {"db", "cache", "reports"})
        self.assertEqual(result["changed_leases"], ["db"])
        self.assertNotIn("root", result["affected_leases"])
        self.assertNotIn("unrelated", result["affected_leases"])
        self.assertEqual(self.engine.get_lease("db")["lease"]["lease_version"], 2)
        self.assertEqual(self.engine.get_lease("root")["derived_index_version"], 1)

    def test_incremental_dependency_rewire_rebuilds_parent_and_child_indexes(self) -> None:
        result = self.engine.commit_increment(1, [{
            "lock_key": "cache",
            "dependencies": [{"lock_key": "unrelated", "mode": "weak"}],
            "expected_lease_version": 1,
        }])
        self.assertEqual(set(result["affected_leases"]), {"cache", "reports"})
        cache = self.engine.get_lease("cache")
        self.assertEqual(cache["dependency_chain"]["parents"], [
            {"lock_key": "unrelated", "mode": "weak"}
        ])
        self.assertEqual(cache["dependency_chain"]["ancestors"], ["unrelated"])
        self.assertEqual(cache["derived_index_version"], 2)
        db = self.engine.get_lease("db")
        self.assertEqual(db["dependency_chain"]["children"], [
            {"lock_key": "reports", "mode": "strong"}
        ])
        unrelated = self.engine.get_lease("unrelated")
        self.assertEqual(unrelated["dependency_chain"]["children"], [
            {"lock_key": "cache", "mode": "weak"}
        ])

    def test_same_base_concurrent_increments_allow_one_winner(self) -> None:
        barrier = threading.Barrier(2)
        results: list[dict] = []
        errors: list[ApiError] = []
        lock = threading.Lock()

        def worker(holder: str) -> None:
            barrier.wait()
            try:
                result = self.engine.commit_increment(
                    1,
                    [{
                        "lock_key": "unrelated",
                        "holder": holder,
                        "ttl_ms": 91000,
                        "expected_lease_version": 1,
                    }],
                )
                with lock:
                    results.append(result)
            except ApiError as exc:
                with lock:
                    errors.append(exc)

        threads = [threading.Thread(target=worker, args=(name,)) for name in ("a", "b")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertEqual(len(results), 1)
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0].code, "E_STALE_BASE")
        self.assertEqual(self.engine.snapshot_version, 2)

    def test_cycle_rejection_does_not_change_snapshot(self) -> None:
        with self.assertRaises(ApiError) as caught:
            self.engine.commit_increment(1, [SAMPLE["cycle_change"]])
        self.assertEqual(caught.exception.code, "E_DEPENDENCY_CYCLE")
        self.assertEqual(self.engine.snapshot_version, 1)
        root = self.engine.get_lease("root")
        self.assertEqual(root["lease"]["dependencies"], [])

    def test_strong_dependency_expiry_releases_and_weak_dependency_degrades(self) -> None:
        self.engine.advance_clock(SAMPLE["clock_advance_ms"])
        swept = self.engine.sweep()
        self.assertEqual(swept["snapshot_version"], 2)
        self.assertEqual(set(swept["affected_leases"]), {"root", "db", "cache", "reports"})

        root = self.engine.get_lease("root")["lease"]
        db = self.engine.get_lease("db")["lease"]
        cache = self.engine.get_lease("cache")["lease"]
        reports = self.engine.get_lease("reports")["lease"]
        self.assertEqual((root["state"], root["release_reason"]), ("released", "LEASE_EXPIRED"))
        self.assertEqual((db["state"], db["release_reason"]), ("released", "DEPENDENCY_EXPIRED"))
        self.assertEqual((cache["state"], cache["dependency_expired"]), ("active", True))
        self.assertEqual((reports["state"], reports["release_reason"]), ("released", "DEPENDENCY_EXPIRED"))

        with self.assertRaises(ApiError) as caught:
            self.engine.renew("db", "svc-db", db["lease_version"], 45000)
        self.assertEqual(caught.exception.code, "E_DEPENDENCY_EXPIRED")

        before = self.engine.snapshot_version
        with self.assertRaises(ApiError) as acquire_error:
            self.engine.acquire(
                "new-strong-child",
                "holder",
                1000,
                [{"lock_key": "db", "mode": "strong"}],
            )
        self.assertEqual(acquire_error.exception.code, "E_DEPENDENCY_EXPIRED")
        self.assertEqual(self.engine.snapshot_version, before)

    def test_full_snapshot_replace_is_atomic_and_history_is_immutable(self) -> None:
        with self.assertRaises(ApiError) as stale:
            self.engine.replace_snapshot(999, SAMPLE["leases"])
        self.assertEqual(stale.exception.code, "E_STALE_BASE")
        self.assertEqual(self.engine.snapshot_version, 1)
        current_keys = {
            lease["lock_key"]
            for lease in self.engine.get_current_snapshot()["snapshot"]["leases"]
        }
        self.assertIn("db", current_keys)

        replacement = [{
            "lock_key": "only",
            "holder": "replacement-holder",
            "ttl_ms": 10000,
            "dependencies": [],
        }]
        replaced = self.engine.replace_snapshot(1, replacement)
        self.assertEqual(replaced["snapshot_version"], 2)
        with self.assertRaises(ApiError) as missing:
            self.engine.get_lease("db")
        self.assertEqual(missing.exception.code, "E_LEASE_NOT_FOUND")

        historical = self.engine.get_snapshot_version(1)["snapshot"]
        historical_keys = {lease["lock_key"] for lease in historical["leases"]}
        self.assertIn("db", historical_keys)
        current = self.engine.get_current_snapshot()["snapshot"]
        self.assertEqual({lease["lock_key"] for lease in current["leases"]}, {"only"})

    def test_lease_version_remains_strict_after_full_replacement(self) -> None:
        acquired = self.engine.acquire("versioned", "a", 5000)
        released = self.engine.release(
            "versioned", "a", acquired["lease"]["lease_version"]
        )
        old_version = released["lease"]["lease_version"]
        snapshot_version = self.engine.snapshot_version
        self.engine.replace_snapshot(snapshot_version, [{
            "lock_key": "versioned",
            "holder": "b",
            "ttl_ms": 5000,
            "lease_version": 1,
            "dependencies": [],
        }])
        current = self.engine.get_lease("versioned")["lease"]
        self.assertGreater(current["lease_version"], old_version)

    def test_recovery_marks_pending_intent_aborted(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO intents(intent_id, kind, base_version, target_version, status, "
                "payload_json, created_at_ms, pid) VALUES (?, ?, ?, ?, 'STARTED', ?, ?, ?)",
                ("pending-test", "increment", 1, None, "{}", 1, 99999),
            )
            conn.commit()

        recovered = LeaseEngine(self.db_path)
        try:
            report = recovered.get_recovery_report()["report"]
            self.assertEqual(report["aborted_intent_count"], 1)
            self.assertEqual(report["recovered_snapshot_version"], 1)
            self.assertFalse(report["complete_exit_report"])
            self.assertFalse(report["snapshot_restored_from_checkpoint"])
            self.assertEqual(recovered.snapshot_version, 1)
        finally:
            recovered.close()

        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT status, error_code FROM intents WHERE intent_id = 'pending-test'"
            ).fetchone()
        self.assertEqual(row[0], "ABORTED")
        self.assertEqual(row[1], "E_RECOVERED_ABORTED_INTENT")

    def test_recovery_repairs_materialized_state_from_checkpoint(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE leases SET holder = 'corrupted' WHERE lock_key = 'db'")
            conn.commit()

        recovered = LeaseEngine(self.db_path)
        try:
            report = recovered.get_recovery_report()["report"]
            self.assertTrue(report["snapshot_restored_from_checkpoint"])
            self.assertEqual(recovered.get_lease("db")["lease"]["holder"], "svc-db")
            self.assertEqual(recovered.snapshot_version, 1)
        finally:
            recovered.close()

    def test_fault_after_intent_leaves_no_committed_snapshot(self) -> None:
        def fault_hook(point: str, _context: dict) -> None:
            if point == "after_intent":
                raise InjectedCrash(point)

        crashing = LeaseEngine(self.db_path, enable_faults=True, fault_hook=fault_hook)
        try:
            with self.assertRaises(InjectedCrash):
                crashing.commit_increment(
                    1,
                    [SAMPLE["recovery_change"]],
                    debug_fault="after_intent",
                )
            self.assertEqual(crashing.snapshot_version, 1)
        finally:
            crashing.close()

        recovered = LeaseEngine(self.db_path)
        try:
            report = recovered.get_recovery_report()["report"]
            self.assertEqual(report["aborted_intent_count"], 1)
            self.assertEqual(recovered.snapshot_version, 1)
            lease = recovered.get_lease("unrelated")["lease"]
            self.assertEqual(lease["ttl_ms"], 90000)
        finally:
            recovered.close()

    def test_two_engine_instances_refresh_to_database_checkpoints(self) -> None:
        second = LeaseEngine(self.db_path)
        try:
            acquired = self.engine.acquire("cross-process", "holder-a", 5000)
            self.assertEqual(acquired["snapshot_version"], 2)
            seen = second.get_lease("cross-process")
            self.assertEqual(seen["snapshot_version"], 2)
            self.assertEqual(seen["lease"]["holder"], "holder-a")

            with self.assertRaises(ApiError) as held:
                second.acquire("cross-process", "holder-b", 5000)
            self.assertEqual(held.exception.code, "E_LOCK_HELD")

            changed = second.commit_increment(2, [{
                "lock_key": "cross-process",
                "ttl_ms": 7000,
                "expected_lease_version": acquired["lease"]["lease_version"],
            }])
            self.assertEqual(changed["snapshot_version"], 3)
            refreshed = self.engine.get_lease("cross-process")
            self.assertEqual(refreshed["snapshot_version"], 3)
            self.assertEqual(refreshed["lease"]["ttl_ms"], 7000)
        finally:
            second.close()


if __name__ == "__main__":
    unittest.main()

