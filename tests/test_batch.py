from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import numpy as np

from kde_drift.batch import BatchAccumulator, ingest_batches, split_into_batches
from kde_drift.canonical import canonical_json_bytes
from kde_drift.data import load_dataset
from kde_drift.errors import WorkbenchError
from kde_drift.workbench import Workbench

from tests.common import fast_config, generate_small_data


class BatchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp = tempfile.TemporaryDirectory()
        cls.data_dir = generate_small_data(Path(cls.temp.name) / "data")
        cls.dataset = load_dataset(cls.data_dir, "small")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temp.cleanup()

    def test_arbitrary_batch_size_and_order_are_byte_identical(self) -> None:
        config = fast_config()
        baseline = Workbench(
            self.dataset, config, Path(self.temp.name) / "baseline"
        ).run()
        for batch_size, order, label in (
            (97, "forward", "forward-97"),
            (173, "reverse", "reverse-173"),
            (501, "reverse", "reverse-501"),
        ):
            result = Workbench(
                self.dataset, config, Path(self.temp.name) / label
            ).run(batch_size=batch_size, batch_order=order)
            self.assertEqual(
                canonical_json_bytes(baseline["summary"], strict=True),
                canonical_json_bytes(result["summary"], strict=True),
            )
            for stage in ("density", "metrics", "drift"):
                self.assertEqual(
                    canonical_json_bytes(baseline["payloads"][stage], strict=True),
                    canonical_json_bytes(result["payloads"][stage], strict=True),
                )
            self.assertEqual(result["batch_evaluation"]["accepted_rows"], 1500)

    def test_external_batch_accumulator_preserves_canonical_content(self) -> None:
        batches = split_into_batches(
            self.dataset.current_features, batch_size=113, order="reverse"
        )
        merged, summary = ingest_batches(
            batches,
            expected_columns=self.dataset.columns,
            column_names=self.dataset.column_names,
            expected_rows=self.dataset.rows_current,
        )
        self.assertEqual(summary["accepted_rows"], 1500)
        np.testing.assert_array_equal(
            np.sort(merged, axis=0), np.sort(self.dataset.current_features, axis=0)
        )

    def test_invalid_batches_do_not_pollute_accepted_state(self) -> None:
        accumulator = BatchAccumulator(expected_columns=3, column_names=["a", "b", "c"])
        first = np.arange(12, dtype=np.float64).reshape(4, 3)
        second = np.arange(9, 18, dtype=np.float64).reshape(3, 3)
        accumulator.add_batch(first)
        with self.assertRaises(WorkbenchError) as empty_error:
            accumulator.add_batch(np.empty((0, 3), dtype=np.float64))
        self.assertEqual(empty_error.exception.code, "E_INVALID_BATCH")
        self.assertIn("batch[1]", empty_error.exception.location)
        with self.assertRaises(WorkbenchError) as dimension_error:
            accumulator.add_batch(np.ones((5, 2), dtype=np.float64))
        self.assertEqual(dimension_error.exception.code, "E_INVALID_BATCH")
        self.assertEqual(dimension_error.exception.details["expected_columns"], 3)
        accumulator.add_batch(second)
        merged = accumulator.finalize()
        np.testing.assert_array_equal(merged, np.concatenate([first, second], axis=0))
        self.assertEqual(accumulator.accepted_rows, 7)

    def test_batch_ingest_error_does_not_replace_checkpoint_chain(self) -> None:
        workdir = Path(self.temp.name) / "stable-chain"
        config = fast_config()
        baseline = Workbench(self.dataset, config, workdir).run()
        store_sequence = baseline["outer_sequence"]
        with self.assertRaises(WorkbenchError) as context:
            Workbench(self.dataset, config, workdir).run(
                current_batches=[np.empty((0, self.dataset.columns))]
            )
        self.assertEqual(context.exception.code, "E_INVALID_BATCH")
        status = Workbench(self.dataset, config, workdir).status()
        self.assertTrue(status["valid"])
        self.assertEqual(status["latest_sequence"], store_sequence)


if __name__ == "__main__":
    unittest.main()
