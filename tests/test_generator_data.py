from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

import numpy as np

from kde_drift.data import DatasetRegistry, load_dataset
from kde_drift.errors import WorkbenchError
from kde_drift.generator import generate_dataset
from kde_drift.canonical import atomic_write_json, read_json


class GeneratorTests(unittest.TestCase):
    def test_generator_is_byte_reproducible_and_declarations_match(self) -> None:
        with tempfile.TemporaryDirectory() as first_dir, tempfile.TemporaryDirectory() as second_dir:
            first = Path(first_dir)
            second = Path(second_dir)
            generate_dataset(first)
            generate_dataset(second)
            first_files = {
                str(path.relative_to(first)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted(first.rglob("*"))
                if path.is_file()
            }
            second_files = {
                str(path.relative_to(second)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted(second.rglob("*"))
                if path.is_file()
            }
            self.assertEqual(first_files, second_files)

            registry = DatasetRegistry(first)
            self.assertEqual(registry.names(), ["large", "small"])
            small = load_dataset(first, "small")
            large = load_dataset(first, "large")
            self.assertEqual((small.rows_reference, small.columns), (1500, 8))
            self.assertEqual((large.rows_reference, large.columns), (15000, 24))
            self.assertEqual(small.missing_columns, [2, 6])
            self.assertEqual(large.missing_columns, [2, 6, 11, 18])
            for column in small.missing_columns:
                self.assertTrue(np.isnan(small.reference_features[:, column]).any())
                self.assertTrue(np.isnan(small.current_features[:, column]).any())
            self.assertTrue(np.all(small.reference_labels == "reference"))
            self.assertTrue(np.all(small.current_labels == "current"))

            declaration = small.declaration
            self.assertEqual(declaration["expected_output"]["aggregation"], "per_column")
            self.assertEqual(declaration["expected_output"]["kl"]["epsilon"], 1e-12)
            self.assertEqual(
                declaration["expected_output"]["drift"]["minimum_bootstrap_columns"], 2
            )

    def test_declaration_column_name_mismatch_is_structured(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            generate_dataset(root, scales=("small",))
            declaration_path = root / "small" / "declaration.json"
            declaration = read_json(declaration_path)
            declaration["current"]["column_names"] = declaration["current"]["column_names"].copy()
            declaration["current"]["column_names"][0] = "different_name"
            atomic_write_json(declaration_path, declaration)
            with self.assertRaises(WorkbenchError) as context:
                load_dataset(root, "small")
            self.assertEqual(context.exception.code, "E_COLUMN_MISMATCH")
            self.assertTrue(context.exception.details["differences"])


if __name__ == "__main__":
    unittest.main()
