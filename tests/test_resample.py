from __future__ import annotations

import unittest

from telemetry_align.errors import TelemetryError
from telemetry_align.normalize import build_grid
from telemetry_align.resample import resample_channel


GRID = [{"index": i, "timestamp_ms": t} for i, t in enumerate((0, 50, 100))]


class ResampleTests(unittest.TestCase):
    def test_all_three_methods(self) -> None:
        known = [(0, 0.0), (100, 10.0)]
        ffill = resample_channel(
            known,
            GRID,
            method="ffill",
            max_gap_ms=100,
            allow_extrapolation=False,
            valid_range=None,
        )
        linear = resample_channel(
            known,
            GRID,
            method="linear",
            max_gap_ms=100,
            allow_extrapolation=False,
            valid_range=None,
        )
        nearest = resample_channel(
            known,
            GRID,
            method="nearest",
            max_gap_ms=100,
            allow_extrapolation=False,
            valid_range=None,
        )
        self.assertEqual([p["value"] for p in ffill], [0.0, 0.0, 10.0])
        self.assertEqual([p["value"] for p in linear], [0.0, 5.0, 10.0])
        # Equal distance is deterministically resolved toward the earlier sample.
        self.assertEqual([p["value"] for p in nearest], [0.0, 0.0, 10.0])
        self.assertEqual(linear[1]["source_timestamps_ms"], [0, 100])
        self.assertEqual(nearest[1]["source_timestamps_ms"], [0])

    def test_linear_clamps_to_valid_range(self) -> None:
        result = resample_channel(
            [(0, 0.0), (100, 10.0)],
            GRID,
            method="linear",
            max_gap_ms=100,
            allow_extrapolation=False,
            valid_range={"min": 0.0, "max": 4.0},
        )
        self.assertEqual(result[1]["value"], 4.0)
        self.assertTrue(result[1]["clamped"])

    def test_gap_limit_blocks_interpolation(self) -> None:
        result = resample_channel(
            [(0, 1.0), (100, 3.0)],
            GRID,
            method="linear",
            max_gap_ms=40,
            allow_extrapolation=False,
            valid_range=None,
        )
        self.assertIsNone(result[1]["value"])
        self.assertEqual(result[1]["method"], "missing")
        self.assertEqual(result[1]["reason"], "gap_exceeds_max_gap_ms")

    def test_nearest_extrapolation_is_marked(self) -> None:
        result = resample_channel(
            [(0, 1.0), (100, 2.0)],
            [{"index": 0, "timestamp_ms": 150}],
            method="nearest",
            max_gap_ms=100,
            allow_extrapolation=True,
            valid_range=None,
        )
        self.assertEqual(result[0]["method"], "nearest")
        self.assertTrue(result[0]["extrapolated"])
        self.assertEqual(result[0]["source_timestamps_ms"], [100])

    def test_grid_invalid_codes(self) -> None:
        for step in (0, -10, 500):
            with self.subTest(step=step):
                with self.assertRaises(TelemetryError) as raised:
                    build_grid(
                        {"grid": {"step_ms": step, "start_ms": 0, "end_ms": 100}},
                        tolerance_ms=200,
                    )
                self.assertEqual(raised.exception.code, "E_GRID_INVALID")


if __name__ == "__main__":
    unittest.main()

