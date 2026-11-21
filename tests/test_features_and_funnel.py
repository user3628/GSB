from __future__ import annotations

import math
import stat
import tempfile
import unittest
from pathlib import Path

import numpy as np

from ctr_cvr_workbench.config import Config
from ctr_cvr_workbench.data import load_manifest, load_rows, make_data_bundle, register_manifest_observation
from ctr_cvr_workbench.features import assemble_features, normalize_category
from ctr_cvr_workbench.funnel import evaluate_scores
from ctr_cvr_workbench.model import log_sigmoid
from ctr_cvr_workbench.training import ScoreRecord

from .helpers import make_workspace


class FeatureAndFunnelTests(unittest.TestCase):
    def test_normalization_and_missing_and_feature_artifact_determinism(self) -> None:
        self.assertEqual(normalize_category("  ＳＨＡＮＧＨＡＩ  "), "shanghai")
        self.assertEqual(normalize_category("A\t B\nC"), "a b c")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            make_workspace(root, shards=1, rows_per_shard=40)
            config = Config.load("configs/test.yaml", root)
            manifest = load_manifest(root / "data" / "shards" / "manifest.json")
            observation = register_manifest_observation(manifest, config.state_root)
            if __import__("os").name != "nt":
                self.assertEqual(stat.S_IMODE(observation.stat().st_mode) & 0o777, 0o444)
            shards = load_rows(manifest)
            data = make_data_bundle(manifest, shards)
            first = assemble_features(data, config)
            second = assemble_features(data, config)
            self.assertEqual(first.artifact_bytes(), second.artifact_bytes())
            self.assertEqual(first.aggregate_fingerprint, second.aggregate_fingerprint)
            rows = [row for shard in first.shards for row in shard.rows]
            self.assertTrue(any(row.missing for row in rows))
            self.assertTrue(any(any(value is not None for value in row.bins.values()) for row in rows))
            missing_row = next(row for row in rows if row.missing)
            self.assertTrue(any(token.startswith("__MISSING__|") for token in missing_row.tokens))
            raw_row = shards[0].rows[missing_row.row_number - 1]
            self.assertNotIn(missing_row.missing[0].split(".")[-1] if "." in missing_row.missing[0] else missing_row.missing[0], raw_row.get(missing_row.missing[0].split(".")[0], {}))

    def test_log_domain_is_finite_at_extremes(self) -> None:
        values = log_sigmoid(np.asarray([-1000.0, -50.0, 0.0, 50.0, 1000.0]))
        self.assertTrue(np.isfinite(values).all())
        self.assertTrue((values <= 0.0).all())

    def test_funnel_reports_violation_without_clipping(self) -> None:
        records = [
            ScoreRecord(
                impression_id="imp-a",
                shard_id="shard-0",
                shard_index=0,
                row_number=1,
                log_ctr=math.log(0.4),
                log_cvr=math.log(0.8),
                log_conversion=math.log(0.32),
                ctr_label=1,
                cvr_label=1,
            ),
            ScoreRecord(
                impression_id="imp-b",
                shard_id="shard-0",
                shard_index=0,
                row_number=2,
                log_ctr=math.log(0.5),
                log_cvr=math.log(0.2),
                log_conversion=0.001,
                ctr_label=0,
                cvr_label=None,
            ),
        ]
        diagnostics = evaluate_scores(records, 0.5)
        checks = {item["check"] for item in diagnostics["violations"]}
        self.assertIn("cvr_probability_not_greater_than_ctr", checks)
        self.assertIn("combined_probability_in_unit_interval", checks)
        self.assertEqual(diagnostics["violations"][0]["impression_id"], "imp-a")
        self.assertEqual(diagnostics["score_checks"]["probability_policy"], "no clipping, no truncation")
        self.assertEqual(diagnostics["violations"][0]["violated_value"]["log_cvr"], records[0].log_cvr)


if __name__ == "__main__":
    unittest.main()
