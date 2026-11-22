from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import yaml

from telemetry_align.config import load_config, parse_config, semantic_equal
from telemetry_align.errors import TelemetryError
from telemetry_align.samples import generate_demo


class ConfigTests(unittest.TestCase):
    def test_v1_upgrade_and_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            generate_demo(root)
            v1 = load_config(root / "data/configs/config_v1.yaml")
            v2 = load_config(root / "data/configs/config_v2.yaml")
            self.assertEqual(v1.schema_version, 1)
            self.assertEqual(v1.effective_schema_version, 2)
            self.assertTrue(v1.degraded)
            self.assertEqual(v1.effective["tolerance_ms"], 200)
            self.assertEqual(
                v1.provenance["conversion"]["tolerance"]["formula"],
                "half_up(20% * 1000ms / 100)",
            )
            self.assertTrue(semantic_equal(v1, v2))
            exported = parse_config(yaml.safe_load(v1.to_v2_yaml()))
            self.assertTrue(semantic_equal(v1, exported))
            self.assertEqual(v1.semantic_fingerprint, exported.semantic_fingerprint)

    def test_v1_percent_rounding_rule(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            generate_demo(root)
            path = root / "data/configs/config_v1.yaml"
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
            data["tolerance_percent"] = 15
            data["compatibility"]["tolerance_basis_ms"] = 333
            data["grid"]["step_ms"] = 50
            upgraded = parse_config(data)
            self.assertEqual(upgraded.effective["tolerance_ms"], 50)
            conversion = upgraded.provenance["conversion"]["tolerance"]
            self.assertEqual(conversion["percent"], 15)
            self.assertEqual(conversion["basis_ms"], 333)
            self.assertEqual(conversion["rounding"], "half_up")
            self.assertEqual(conversion["tolerance_ms"], 50)

    def test_grid_step_must_not_exceed_tolerance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            generate_demo(root)
            data = yaml.safe_load(
                (root / "data/configs/config_v2.yaml").read_text(encoding="utf-8")
            )
            data["grid"]["step_ms"] = data["tolerance_ms"] + 1
            with self.assertRaises(TelemetryError) as raised:
                parse_config(data)
            self.assertEqual(raised.exception.code, "E_GRID_INVALID")


if __name__ == "__main__":
    unittest.main()

