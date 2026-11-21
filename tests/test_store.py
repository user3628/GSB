import json
import os
import tempfile
import threading
import unittest

from exp_tracker.models import MetricPoint, Run
from exp_tracker.negotiation import NegotiationResult
from exp_tracker.store import (
    ExperimentStore, OUTCOME_OVERWRITE, OUTCOME_REJECT, OUTCOME_SKIP,
)
from exp_tracker.errors import TrackerError


def make_run(run_id="r1", experiment="exp", version="2.1.0", last_write=1000,
             values=None, params=None):
    values = [1.0, 2.0, 3.0] if values is None else values
    return Run(
        run_id=run_id, experiment=experiment,
        params={"batch_size": 32} if params is None else params,
        metrics=[MetricPoint(step=i, value=value, time_us=1000 + i)
                 for i, value in enumerate(values)],
        toolchain_version=version, last_write_us=last_write,
    )


def neg(kind="binary", version=2):
    return NegotiationResult(kind=kind, format_version=version, verdict="accept",
                             reason="test", precision="us", params_present=True).to_dict()


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.db = os.path.join(self.tmp, "store.db")
        self.store = ExperimentStore(self.db)

    def tearDown(self):
        self.store.close()

    def test_submit_builds_incremental_summary_and_param_index(self):
        self.store.submit_run(make_run("r1", "exp", "2.1.0"))
        self.store.submit_run(make_run("r2", "exp", "2.1.0", values=[4.0]))
        summary = self.store.get_summary("exp", "2.1.0")
        self.assertEqual(summary["run_count"], 2)
        self.assertEqual(summary["point_count"], 4)
        self.assertEqual(self.store.find_runs_by_param("batch_size"), ["r1", "r2"])

    def test_conflict_higher_version_overwrites(self):
        self.store.submit_run(make_run("r1", version="2.1.0", last_write=1000))
        result = self.store.submit_run(
            make_run("r1", version="2.2.0", last_write=500), request_id="r1b")
        self.assertEqual(result["outcome"], OUTCOME_OVERWRITE)
        self.assertEqual(self.store.get_run("r1").toolchain_version, "2.2.0")

    def test_conflict_equal_signature_skips(self):
        run = make_run("r1")
        self.store.submit_run(run)
        result = self.store.submit_run(make_run("r1", last_write=2000))
        self.assertEqual(result["outcome"], OUTCOME_SKIP)

    def test_conflict_reject_equal_version_different_metrics(self):
        self.store.submit_run(make_run("r1", values=[1.0, 2.0], last_write=1000))
        with self.assertRaises(TrackerError) as ctx:
            self.store.submit_run(make_run("r1", values=[9.0, 9.0], last_write=1000))
        self.assertEqual(ctx.exception.code, "E_CONFLICTING_RUN")

    def test_batch_rolls_back_on_any_reject(self):
        self.store.submit_run(make_run("r1", values=[1.0, 2.0], last_write=1000))
        batch = [
            make_run("r9", values=[5.0]),
            make_run("r1", values=[9.0, 9.0], last_write=1000),  # reject
        ]
        before = self.store.revision()
        with self.assertRaises(TrackerError) as ctx:
            self.store.import_runs(batch, neg(), "rep-reject")
        self.assertEqual(ctx.exception.code, "E_CONFLICTING_RUN")
        self.assertEqual(self.store.revision(), before)
        self.assertIsNone(self.store.get_run("r9"))
        report = self.store.get_import_report("rep-reject")
        self.assertFalse(report["committed"])

    def test_report_matches_committed_state(self):
        batch = [make_run("r1"), make_run("r2")]
        report = self.store.import_runs(batch, neg(), "rep-ok")
        self.assertTrue(report["committed"])
        self.assertEqual(report["counts"]["insert"], 2)
        self.assertIsNotNone(self.store.get_run("r1"))

    def test_cascade_rebuild_is_atomic_and_reindexes(self):
        self.store.submit_run(make_run("r1", "exp", params={"a": 1}))
        result = self.store.cascade_rebuild("exp")
        self.assertEqual(result["reindexed_run_ids"], ["r1"])
        self.assertEqual(self.store.find_runs_by_param("a"), ["r1"])
        self.assertEqual(self.store.get_summary("exp", "2.1.0")["run_count"], 1)

    def test_snapshot_invalid_keeps_old_state(self):
        self.store.submit_run(make_run("r1"))
        bad = os.path.join(self.tmp, "bad.json")
        with open(bad, "w") as handle:
            handle.write("{broken")
        with self.assertRaises(TrackerError) as ctx:
            self.store.load_snapshot(bad)
        self.assertEqual(ctx.exception.code, "E_SNAPSHOT_INVALID")
        self.assertIsNotNone(self.store.get_run("r1"))

    def test_snapshot_switch_and_monotonic_seq(self):
        self.store.submit_run(make_run("r1"))
        snap = os.path.join(self.tmp, "s.json")
        self.store.export_snapshot(snap, generated_at_us=123)
        # A second empty store loads the snapshot (seq 1 > current 0).
        other_db = os.path.join(self.tmp, "other.db")
        other = ExperimentStore(other_db)
        result = other.load_snapshot(snap)
        self.assertEqual(result["run_count"], 1)
        # Same sequence reload is allowed (no regression), derived caches rebuilt.
        again = other.load_snapshot(snap, request_id="reload")
        self.assertEqual(again["run_count"], 1)
        # An older sequence snapshot must be rejected as stale.
        with open(snap, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        payload["snapshot_seq"] = 0
        old_snap = os.path.join(self.tmp, "old.json")
        with open(old_snap, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        with self.assertRaises(TrackerError) as ctx:
            other.load_snapshot(old_snap)
        self.assertEqual(ctx.exception.code, "E_STALE_BASE")
        self.assertIsNotNone(other.get_run("r1"))
        other.close()

    def test_snapshot_rebuilds_derived_state_consistently(self):
        self.store.submit_run(make_run("r1", "exp", params={"k": 1}))
        snap = os.path.join(self.tmp, "s2.json")
        self.store.export_snapshot(snap)
        other = ExperimentStore(os.path.join(self.tmp, "o2.db"))
        other.load_snapshot(snap)
        self.assertEqual(other.get_summary("exp", "2.1.0")["run_count"], 1)
        self.assertEqual(other.find_runs_by_param("k"), ["r1"])
        other.close()

    def test_stale_base_revision(self):
        with self.assertRaises(TrackerError) as ctx:
            self.store.submit_run(make_run("r1"), expected_revision=99)
        self.assertEqual(ctx.exception.code, "E_STALE_BASE")

    def test_idempotent_request_id_executes_once(self):
        run = make_run("r1")
        first = self.store.submit_run(run, request_id="dup")
        second = self.store.submit_run(make_run("r1", values=[7.0]), request_id="dup")
        self.assertEqual(first, second)
        # Original metric values prove the second submission never executed.
        self.assertEqual(self.store.get_run("r1").metrics[0].value, 1.0)

    def test_concurrent_duplicate_requests_share_result(self):
        results = []

        def worker():
            results.append(self.store.submit_run(make_run("r1"), request_id="concurrent"))

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(results), 4)
        self.assertTrue(all(item == results[0] for item in results))
        self.assertEqual(len(self.store.list_runs()), 1)

    def test_recovery_marks_pending_intent_aborted(self):
        self.store.submit_run(make_run("r1"), request_id="good")
        self.store.mark_crash_point("trapped", "import", {"report_id": "x"})
        self.store.close()
        revived = ExperimentStore(self.db)
        self.assertIn("trapped", revived.recovery_report.aborted_intents)
        self.assertIsNotNone(revived.get_run("r1"))
        revived.close()


if __name__ == "__main__":
    unittest.main()
