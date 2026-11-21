from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ctr_cvr_workbench.config import Config
from ctr_cvr_workbench.data import load_manifest
from ctr_cvr_workbench.errors import PipelineError
from ctr_cvr_workbench.pipeline import Pipeline
from ctr_cvr_workbench.scheduler import plan_shards

from .helpers import make_workspace


class SchedulerAndErrorTests(unittest.TestCase):
    def test_budget_and_quota_are_not_exceeded(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            make_workspace(root, shards=4, rows_per_shard=80, epochs=3, budget=600, quota=80)
            config = Config.load("configs/test.yaml", root)
            manifest = load_manifest(root / "data" / "shards" / "manifest.json")
            plan = plan_shards(manifest.shards, config, worker_override=4)
            self.assertEqual(plan.consumed, 480)
            self.assertEqual(len(plan.accepted), 2)
            self.assertEqual(len(plan.unprocessed), 2)
            self.assertLessEqual(plan.consumed, plan.budget)
            self.assertEqual(plan.workers_effective, 2)
            self.assertTrue(all(item.cost == 240 for item in plan.accepted))
            self.assertTrue(all("budget would be exceeded" in item.reason for item in plan.unprocessed))

    def test_stable_injection_error_codes_and_locations(self) -> None:
        expected = {
            "zero_feature_dim": "E_INVALID_PARAM",
            "manifest_mismatch": "E_MANIFEST_MISMATCH",
            "invalid_ctr_label": "E_LABEL_INVALID",
            "cvr_without_ctr": "E_FUNNEL_VIOLATION",
            "negative_lr": "E_INVALID_PARAM",
            "negative_epochs": "E_INVALID_PARAM",
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            make_workspace(root, shards=2, rows_per_shard=40, epochs=1)
            for injection, code in expected.items():
                with self.subTest(injection=injection):
                    with self.assertRaises(PipelineError) as caught:
                        Pipeline(
                            "configs/test.yaml",
                            base_dir=root,
                            injection=injection,
                            execution_mode="sequential",
                        ).run()
                    error = caught.exception
                    self.assertEqual(error.code, code)
                    self.assertIsNotNone(error.shard_id)
                    self.assertIsNotNone(error.row_number)
                    payload = error.to_dict()
                    self.assertEqual(payload["code"], code)


if __name__ == "__main__":
    unittest.main()
