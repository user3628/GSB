"""Crash recovery: torn journal writes, graceful stop, hard kill, resume."""

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workbench.config import default_config_store
from workbench.engine import Engine
from workbench.sample import generate_csv
from workbench.store import Store

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class RecoveryBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb-rec-")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.sample = os.path.join(self.tmp, "sample.csv")
        generate_csv(self.sample, seed=20261120, rows=600)
        self.state = os.path.join(self.tmp, "state")
        store = Store(self.state)
        store.ensure()
        store.write_config_store(default_config_store())
        store.write_sample_registry(
            {"path": self.sample, "sha256": "", "rows": 600, "columns": []}
        )

    def cli(self, *args, **kw):
        return subprocess.run(
            [sys.executable, "-m", "workbench", "--state", self.state] + list(args),
            cwd=ROOT, capture_output=True, text=True, **kw
        )


class TestTornJournal(RecoveryBase):
    def test_torn_record_ignored(self):
        Engine(self.state).run()
        store = Store(self.state)
        self.assertEqual(store.head_seq(), 4)
        # simulate a torn write: garbage in the next journal slot + temp junk
        with open(os.path.join(store.journal_dir, "000005_select.json"), "w") as f:
            f.write('{"seq": 5, "stage": "select", "truncat')
        with open(os.path.join(store.journal_dir, ".atomic-xyz.part"), "w") as f:
            f.write("partial")
        self.assertEqual(store.head_seq(), 4)
        # a digest-mismatched record also truncates the chain
        rec_path = os.path.join(store.journal_dir, "000003_select.json")
        rec = json.load(open(rec_path))
        rec["artifact_digest"] = "0" * 64
        with open(rec_path, "w") as f:
            json.dump(rec, f)
        self.assertEqual(store.head_seq(), 2)

    def test_recovery_continues_from_last_consistent(self):
        eng = Engine(self.state)
        r1 = eng.run()
        store = Store(self.state)
        # drop the last two journal records -> resume recomputes only those
        for name in ("000004_summarize.json", "000003_select.json"):
            os.unlink(os.path.join(store.journal_dir, name))
        r2 = eng.run()
        sources = {s["stage"]: s["source"] for s in r2["stages"]}
        self.assertEqual(sources["infer"], "journal")
        self.assertEqual(sources["encode"], "journal")
        # artifacts survive in the cache and are republished, not recomputed
        self.assertEqual(sources["select"], "cache")
        self.assertEqual(sources["summarize"], "cache")
        self.assertEqual(Store(self.state).head_seq(), 4)
        self.assertEqual(r1["summary_digest"], r2["summary_digest"])


class TestGracefulStop(RecoveryBase):
    def test_sigterm_exits_zero_and_preserves_journal(self):
        proc = subprocess.Popen(
            [sys.executable, "-m", "workbench", "--state", self.state,
             "run", "--stage-sleep-ms", "900"],
            cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        time.sleep(1.4)
        proc.send_signal(signal.SIGTERM)
        out, err = proc.communicate(timeout=30)
        self.assertEqual(proc.returncode, 0, err)
        report = json.loads(out)
        self.assertEqual(report["status"], "stopped")
        completed = report["completed_stages"]
        self.assertTrue(1 <= len(completed) < 4)
        self.assertEqual(Store(self.state).head_seq(), len(completed))
        # resume completes and matches an uninterrupted reference run
        out2 = self.cli("resume")
        self.assertEqual(out2.returncode, 0, out2.stderr)
        digest_resumed = json.loads(out2.stdout)["summary_digest"]
        ref_state = os.path.join(self.tmp, "state-ref")
        store = Store(ref_state)
        store.ensure()
        store.write_config_store(default_config_store())
        store.write_sample_registry(
            {"path": self.sample, "sha256": "", "rows": 600, "columns": []}
        )
        digest_ref = Engine(ref_state).run()["summary_digest"]
        self.assertEqual(digest_resumed, digest_ref)

    def test_stop_file_request(self):
        store = Store(self.state)
        store.request_stop()
        proc = subprocess.Popen(
            [sys.executable, "-m", "workbench", "--state", self.state,
             "run", "--stage-sleep-ms", "500"],
            cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        time.sleep(0.8)
        store.request_stop()
        out, err = proc.communicate(timeout=30)
        # engine clears the flag at start; the mid-run request stops it
        self.assertEqual(proc.returncode, 0, err)
        self.assertIn(json.loads(out)["status"], ("stopped", "completed"))


class TestHardKill(RecoveryBase):
    def test_sigkill_resume_from_last_record(self):
        proc = subprocess.Popen(
            [sys.executable, "-m", "workbench", "--state", self.state,
             "run", "--stage-sleep-ms", "900"],
            cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        time.sleep(1.4)
        proc.send_signal(signal.SIGKILL)
        proc.communicate(timeout=30)
        self.assertEqual(proc.returncode, -signal.SIGKILL)
        head = Store(self.state).head_seq()
        self.assertTrue(1 <= head < 4)
        out = self.cli("resume")
        self.assertEqual(out.returncode, 0, out.stderr)
        report = json.loads(out.stdout)
        self.assertEqual(report["status"], "completed")
        self.assertEqual(Store(self.state).head_seq(), 4)
        ref_state = os.path.join(self.tmp, "state-ref2")
        store = Store(ref_state)
        store.ensure()
        store.write_config_store(default_config_store())
        store.write_sample_registry(
            {"path": self.sample, "sha256": "", "rows": 600, "columns": []}
        )
        digest_ref = Engine(ref_state).run()["summary_digest"]
        self.assertEqual(report["summary_digest"], digest_ref)


if __name__ == "__main__":
    unittest.main()
