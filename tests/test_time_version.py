import unittest

from exp_tracker.timeutil import (
    TRUNCATE, ROUND, PRESERVE, ms_to_us, normalize_batch, normalize_metric_time,
    retention_to_expiry, us_to_ms_grid,
)
from exp_tracker.versions import compare_versions, parse_version


class TimeNormalizationTests(unittest.TestCase):
    def test_ms_to_us(self):
        self.assertEqual(ms_to_us(1234), 1_234_000)

    def test_truncate_discards_remainder(self):
        self.assertEqual(us_to_ms_grid(1_234_700, TRUNCATE), 1_234_000)

    def test_round_half_away(self):
        self.assertEqual(us_to_ms_grid(1_234_500, ROUND), 1_235_000)
        self.assertEqual(us_to_ms_grid(1_234_499, ROUND), 1_234_000)

    def test_preserve_keeps_precision(self):
        self.assertEqual(us_to_ms_grid(1_234_700, PRESERVE), 1_234_700)

    def test_record_exposes_rounding_and_cumulative_error(self):
        records = normalize_batch([("a", 1_234_700), ("b", 1_000_300)], TRUNCATE)
        self.assertEqual(records[0].rounded_digits, 3)
        self.assertEqual(records[0].discarded_us, -700)
        self.assertEqual(records[1].cumulative_error_us, -1000)

    def test_preserve_has_zero_rounding(self):
        record = normalize_metric_time("a", 1_234_700, PRESERVE)
        self.assertEqual(record.rounded_digits, 0)
        self.assertEqual(record.discarded_us, 0)

    def test_retention_to_absolute_expiry(self):
        self.assertEqual(retention_to_expiry(1_000_000, 1.5), 2_500_000)


class VersionSemanticsTests(unittest.TestCase):
    def test_tri_state(self):
        self.assertEqual(compare_versions("2.1.0", "2.1.0"), "equal")
        self.assertEqual(compare_versions("2.2.0", "2.1.0"), "higher")
        self.assertEqual(compare_versions("2.1.0", "2.2.0"), "lower")

    def test_prerelease_below_release(self):
        self.assertEqual(compare_versions("2.1.0-rc3", "2.1.0"), "lower")
        self.assertEqual(compare_versions("2.1.0", "2.1.0-rc3"), "higher")

    def test_numeric_core_segments(self):
        self.assertEqual(compare_versions("2.10.0", "2.9.0"), "higher")

    def test_order_independence(self):
        left = parse_version("2.1.0-rc3")
        right = parse_version("2.1.0-rc10")
        self.assertLess(left, right)
        self.assertGreater(right, left)
        self.assertEqual(compare_versions("1.9.4", "2.1.0"),
                         {"higher": "lower", "lower": "higher"}[
                             compare_versions("2.1.0", "1.9.4")])

    def test_unequal_core_length(self):
        self.assertEqual(compare_versions("2.1", "2.1.0"), "equal")


if __name__ == "__main__":
    unittest.main()
