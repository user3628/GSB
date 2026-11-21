from __future__ import annotations

import dataclasses
import math
import tempfile
import unittest
from pathlib import Path

import numpy as np

from kde_drift.canonical import canonical_json_bytes
from kde_drift.config import normalize_config
from kde_drift.data import compare_column_schemas, load_dataset
from kde_drift.errors import WorkbenchError
from kde_drift.workbench import Workbench

from tests.common import fast_config, generate_small_data


class KdeMetricTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp = tempfile.TemporaryDirectory()
        cls.data_dir = generate_small_data(Path(cls.temp.name) / "data")
        cls.dataset = load_dataset(cls.data_dir, "small")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temp.cleanup()

    def test_kde_is_deterministic_and_precision_bound_is_checked(self) -> None:
        config = fast_config()
        first = Workbench(
            self.dataset, config, Path(self.temp.name) / "deterministic-a"
        ).run()
        second = Workbench(
            self.dataset, config, Path(self.temp.name) / "deterministic-b"
        ).run()
        for stage in ("bandwidth", "density", "metrics", "drift"):
            self.assertEqual(
                canonical_json_bytes(first["payloads"][stage], strict=True),
                canonical_json_bytes(second["payloads"][stage], strict=True),
            )
        density = first["payloads"]["density"]
        self.assertTrue(density["diagnostics"]["all_curves_unit_area"])
        for column in density["columns"]:
            for side in ("reference", "current"):
                diagnostics = column[f"{side}_diagnostics"]
                self.assertLessEqual(
                    diagnostics["observed_high_precision_error"],
                    diagnostics["verified_upper_bound"],
                )
                self.assertGreaterEqual(
                    diagnostics["verified_upper_bound"],
                    diagnostics["theoretical_upper_bound"],
                )
                self.assertTrue(math.isfinite(diagnostics["verified_upper_bound"]))
        self.assertTrue(density["diagnostics"]["columns_with_imputation"])

    def test_pairwise_reduction_also_produces_verified_bounds(self) -> None:
        result = Workbench(
            self.dataset,
            fast_config(summation="pairwise"),
            Path(self.temp.name) / "pairwise",
        ).run()
        self.assertEqual(result["payloads"]["density"]["summation"], "pairwise")
        for column in result["payloads"]["density"]["columns"]:
            self.assertGreater(column["reference_diagnostics"]["chunks"], 0)
            self.assertLessEqual(
                column["reference_diagnostics"]["observed_high_precision_error"],
                column["reference_diagnostics"]["verified_upper_bound"],
            )

    def test_alternate_kernels_bandwidth_rules_and_imputation_complete(self) -> None:
        configurations = [
            {
                "kernel": "epanechnikov",
                "bandwidth": {"rule": "fixed", "value": 0.4},
                "imputation": {"method": "median"},
                "summation": "pairwise",
                "drift": {"bootstrap_resamples": 16},
            },
            {
                "kernel": "gaussian",
                "bandwidth": {"rule": "scott"},
                "imputation": {"method": "zero"},
                "summation": "kahan",
                "drift": {"bootstrap_resamples": 16},
            },
        ]
        for index, overrides in enumerate(configurations):
            result = Workbench(
                self.dataset,
                fast_config(**overrides),
                Path(self.temp.name) / f"alternate-{index}",
            ).run()
            self.assertEqual(result["status"], "completed")
            self.assertTrue(result["payloads"]["density"]["diagnostics"]["all_curves_unit_area"])
            self.assertTrue(result["payloads"]["metrics"]["diagnostics"]["all_metrics_finite"])

    def test_zero_bandwidth_reports_numeric_error_with_column_and_stage(self) -> None:
        workbench = Workbench(
            self.dataset,
            {"bandwidth": {"rule": "fixed", "value": 0.0}},
            Path(self.temp.name) / "zero-bandwidth",
        )
        with self.assertRaises(WorkbenchError) as context:
            workbench.run()
        error = context.exception
        self.assertEqual(error.code, "E_NUMERIC")
        self.assertEqual(error.stage, "bandwidth")
        self.assertEqual(error.column, self.dataset.column_names[0])
        self.assertEqual(error.location, "kde.bandwidth.value")

    def test_nan_bandwidth_reports_numeric_error(self) -> None:
        workbench = Workbench(
            self.dataset,
            {"bandwidth": {"rule": "fixed", "value": float("nan")}},
            Path(self.temp.name) / "nan-bandwidth",
        )
        with self.assertRaises(WorkbenchError) as context:
            workbench.run()
        self.assertEqual(context.exception.code, "E_NUMERIC")
        self.assertEqual(context.exception.stage, "bandwidth")

    def test_insufficient_sample_size_reports_numeric_error(self) -> None:
        tiny = dataclasses.replace(
            self.dataset,
            reference_features=self.dataset.reference_features[:1].copy(),
            current_features=self.dataset.current_features[:1].copy(),
            reference_labels=self.dataset.reference_labels[:1].copy(),
            current_labels=self.dataset.current_labels[:1].copy(),
        )
        workbench = Workbench(
            tiny,
            fast_config(bandwidth={"rule": "silverman"}),
            Path(self.temp.name) / "insufficient",
        )
        with self.assertRaises(WorkbenchError) as context:
            workbench.run()
        self.assertEqual(context.exception.code, "E_NUMERIC")
        self.assertEqual(context.exception.stage, "bandwidth")
        self.assertIn("kde.bandwidth.sample_size", context.exception.location)

    def test_constant_column_with_estimated_bandwidth_is_numeric_error(self) -> None:
        features = self.dataset.reference_features.copy()
        features[:, 0] = 2.5
        constant = dataclasses.replace(self.dataset, reference_features=features)
        workbench = Workbench(
            constant,
            fast_config(),
            Path(self.temp.name) / "constant-column",
        )
        with self.assertRaises(WorkbenchError) as context:
            workbench.run()
        self.assertEqual(context.exception.code, "E_NUMERIC")
        self.assertEqual(context.exception.column, self.dataset.column_names[0])

    def test_column_mismatch_lists_every_difference(self) -> None:
        with self.assertRaises(WorkbenchError) as context:
            compare_column_schemas(
                ["a", "b", "c"], ["a", "d"], (10, 3), (10, 2)
            )
        error = context.exception
        self.assertEqual(error.code, "E_COLUMN_MISMATCH")
        kinds = {item["kind"] for item in error.details["differences"]}
        self.assertEqual(kinds, {"column_count", "column_name_count", "column_name"})

    def test_metrics_and_drift_record_sample_sizes_parameters_and_indices(self) -> None:
        result = Workbench(
            self.dataset, fast_config(), Path(self.temp.name) / "evidence"
        ).run()
        for metric in result["payloads"]["metrics"]["columns"]:
            sample_sizes = metric["mmd"]["sample_sizes"]
            self.assertGreaterEqual(sample_sizes["n_reference"], 2)
            self.assertGreaterEqual(sample_sizes["n_current"], 2)
            self.assertIn("comparison_bandwidth", metric["mmd"]["kernel_parameter"])
            self.assertIn("reference_bandwidth", metric["kl"]["kernel_parameters"])
        for column in result["payloads"]["drift"]["columns"]:
            self.assertEqual(column["resampling"]["reference_index_range"][0], 0)
            self.assertEqual(
                column["resampling"]["current_index_range"][1],
                result["summary"]["rows_current"] - 1,
            )
            self.assertIn("p_value", column)
            self.assertIn("threshold", column)
            self.assertIn("alpha", column)
            self.assertTrue(column["significance"]["null_scores_digest"])


if __name__ == "__main__":
    unittest.main()
