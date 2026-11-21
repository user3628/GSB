import json
import os
import subprocess
import sys
import unittest

from tests.support import clean, feed, load_sample, new_engine, test_config
from wckpt import Config, Engine

WORKER = os.path.join("tests", "worker", "crash_worker.py")


class CrashRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.small = load_sample("wal_sequence.json")
        self.base_cfg = test_config(fsync_wal=False).to_dict()

    def _launch_crash(self, run_dir, crash_stage, seed_records=0, crash_index=0):
        clean(run_dir)
        spec = {
            "root": run_dir,
            "crash_stage": crash_stage,
            "crash_index": crash_index,
            "seed_records": seed_records,
            "records": self.small["records"],
            "config": self.base_cfg,
        }
        spec_path = os.path.join(run_dir, "spec.json")
        os.makedirs(run_dir, exist_ok=True)
        with open(spec_path, "w", encoding="utf-8") as fh:
            json.dump(spec, fh)
        proc = subprocess.run(
            [sys.executable, WORKER, spec_path],
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            capture_output=True,
            text=True,
        )
        self.assertEqual(proc.returncode, 77, proc.stderr)

    def _reopen(self, run_dir):
        engine = Engine(run_dir, Config.from_dict(self.base_cfg))
        report = engine.open()
        return engine, report

    def test_crash_during_wal_append_reconciles_lsn(self):
        run_dir = os.path.join("artifacts", "test_runs", "crash_wal")
        self._launch_crash(run_dir, "wal_before_write", seed_records=30, crash_index=30)
        engine, report = self._reopen(run_dir)
        self.assertTrue(report["started"])
        kinds = {i["kind"] for i in report["resolved_intents"]}
        self.assertIn("WAL_APPEND", kinds)
        # High-water mark must be contiguous and append can continue safely.
        lsns = [r.lsn for r in engine.wal.iter_records()]
        self.assertEqual(lsns, list(range(1, len(lsns) + 1)))
        n = len(lsns)
        feed(engine, self.small["records"][n:n + 5])
        new_lsns = [r.lsn for r in engine.wal.iter_records()]
        self.assertEqual(len(set(new_lsns)), len(new_lsns))
        engine.close()

    def test_crash_during_checkpoint_rolls_back_to_last_good(self):
        run_dir = os.path.join("artifacts", "test_runs", "crash_cp")
        self._launch_crash(run_dir, "checkpoint", seed_records=40)
        engine, report = self._reopen(run_dir)
        cp_intents = [i for i in report["resolved_intents"] if i["kind"] == "CHECKPOINT"]
        self.assertTrue(cp_intents)
        self.assertEqual(cp_intents[0]["decision"], "rolled_back")
        # Active checkpoint is the last committed one and pages match it.
        active = engine.active_checkpoint()
        self.assertEqual(active.seq, 1)
        snap = engine.snapshot_pages()
        for page_no, page in active.pages.items():
            self.assertEqual(snap[page_no]["digest"], page.digest)
        # The dangling OPEN checkpoint row is marked ABORTED.
        statuses = {
            r["seq"]: r["status"] for r in engine.store.checkpoint_rows()
        }
        aborted = [seq for seq, st in statuses.items() if st == "ABORTED"]
        self.assertTrue(aborted)
        engine.close()

    def test_recovery_report_based_only_on_durable_facts(self):
        run_dir = os.path.join("artifacts", "test_runs", "crash_report")
        self._launch_crash(run_dir, "wal_after_write", seed_records=20, crash_index=20)
        engine, report = self._reopen(run_dir)
        self.assertIn("durable_next_lsn", report)
        self.assertEqual(report["durable_next_lsn"], engine.wal.next_lsn)
        for entry in report["resolved_intents"]:
            self.assertIn("decision", entry)
        engine.close()


if __name__ == "__main__":
    unittest.main()
