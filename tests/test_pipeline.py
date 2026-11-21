import hashlib
import json
import os
import shutil
import tempfile
import unittest

from src.checkpoints import read_state
from src.config import BacktestConfig, load_config
from src.pipeline import run_pipeline


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MANIFEST = os.path.join(REPO_ROOT, "samples", "manifest.json")


def state_bytes(workspace):
    state = read_state(workspace)
    state.pop("exports", None)
    return json.dumps(state, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def checkpoint_hashes(workspace):
    directory = os.path.join(workspace, "checkpoints")
    result = {}
    for name in sorted(os.listdir(directory)):
        if not name.startswith("shard_"):
            continue
        with open(os.path.join(directory, name), "rb") as handle:
            result[name] = hashlib.sha256(handle.read()).hexdigest()
    return result


class PipelineIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not os.path.exists(MANIFEST):
            from scripts.generate_samples import generate
            generate(os.path.join(REPO_ROOT, "samples"))

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb-pipe-")
        self.serial_ws = os.path.join(self.tmp, "serial")
        self.parallel_ws = os.path.join(self.tmp, "parallel")
        os.makedirs(self.serial_ws)
        os.makedirs(self.parallel_ws)
        self.cfg = load_config(os.path.join(REPO_ROOT,
                                            "config", "backtest.yaml"))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_serial_parallel_byte_identical(self):
        serial = run_pipeline(self.serial_ws, MANIFEST, self.cfg,
                              parallel=False)
        parallel = run_pipeline(self.parallel_ws, MANIFEST, self.cfg,
                                parallel=True)
        self.assertEqual(serial["run_status"], "completed")
        self.assertEqual(parallel["run_status"], "completed")
        self.assertEqual(state_bytes(self.serial_ws),
                         state_bytes(self.parallel_ws))
        self.assertEqual(checkpoint_hashes(self.serial_ws),
                         checkpoint_hashes(self.parallel_ws))

    def test_stop_and_resume_matches_full_run(self):
        full = os.path.join(self.tmp, "full")
        stopped = os.path.join(self.tmp, "stopped")
        os.makedirs(full)
        os.makedirs(stopped)
        run_pipeline(full, MANIFEST, self.cfg, parallel=True)

        first = run_pipeline(stopped, MANIFEST, self.cfg, parallel=True,
                             stop_after_folds=5)
        self.assertEqual(first["run_status"], "stopped")
        resumed = run_pipeline(stopped, MANIFEST, self.cfg, parallel=True)
        self.assertEqual(resumed["run_status"], "completed")
        self.assertEqual(state_bytes(full), state_bytes(stopped))
        self.assertEqual(checkpoint_hashes(full),
                         checkpoint_hashes(stopped))

    def test_budget_rejection_is_recorded(self):
        cfg = BacktestConfig(
            method="exponential_smoothing", window_length=24, step=6,
            horizon=3, max_workers=3, global_fit_budget=12,
            worker_fit_quota=5)
        state = run_pipeline(self.serial_ws, MANIFEST, cfg, parallel=False)
        self.assertEqual(state["scheduler"]["accepted_folds"], 12)
        self.assertGreater(len(state["rejected_folds"]), 0)
        codes = {item["code"] for item in state["rejected_folds"]}
        self.assertTrue(codes)

    def test_series_business_errors_are_structured(self):
        state = run_pipeline(self.serial_ws, MANIFEST, self.cfg,
                             parallel=False)
        codes = {item["code"] for item in state["series_errors"]}
        self.assertIn("E_MISSING_RATIO", codes)
        self.assertIn("E_SERIES_TOO_SHORT", codes)
        for item in state["series_errors"]:
            self.assertTrue(item["series_id"])

    def test_config_change_invalidates_downstream(self):
        run_pipeline(self.serial_ws, MANIFEST, self.cfg, parallel=False)
        before = checkpoint_hashes(self.serial_ws)
        changed = BacktestConfig(**{**self.cfg.to_dict(),
                                    "method": "linear_ar"})
        state = run_pipeline(self.serial_ws, MANIFEST, changed,
                             parallel=False)
        self.assertTrue(state["invalidation"])
        affected = {item["shard_id"] for item in state["invalidation"]}
        self.assertIn("shard-01", affected)
        # rebuilt checkpoints must differ from the method-specific ones
        after = checkpoint_hashes(self.serial_ws)
        self.assertNotEqual(before, after)

    def test_unchanged_rerun_reuses_checkpoints(self):
        run_pipeline(self.serial_ws, MANIFEST, self.cfg, parallel=False)
        first = checkpoint_hashes(self.serial_ws)
        run_pipeline(self.serial_ws, MANIFEST, self.cfg, parallel=False)
        self.assertEqual(first, checkpoint_hashes(self.serial_ws))


if __name__ == "__main__":
    unittest.main()
