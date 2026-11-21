import unittest

from tls_workbench import timeutil
from tls_workbench.errors import Codes


class TimeRuleTests(unittest.TestCase):
    WALL = 1_000_000_000_500

    def test_offset_injected_before_truncation(self):
        # WALL is mid-second: +250ms stays in the same second, +750 crosses it.
        instant = timeutil.build_verification_instant(self.WALL, 250, 1000)
        self.assertEqual(instant.effective_verify_ms, 1_000_000_000_000)
        instant = timeutil.build_verification_instant(self.WALL, 750, 1000)
        self.assertEqual(instant.effective_verify_ms, 1_000_000_001_000)

    def test_negative_offset_floor_truncation(self):
        instant = timeutil.build_verification_instant(self.WALL, -250, 1000)
        self.assertEqual(instant.effective_verify_ms, 1_000_000_000_000)
        instant = timeutil.build_verification_instant(self.WALL, -750, 1000)
        self.assertEqual(instant.effective_verify_ms, 999_999_999_000)

    def test_zero_margin_boundaries_are_valid(self):
        instant = timeutil.build_verification_instant(10_000, 0)
        lower = timeutil.evaluate_validity(10_000, 20_000, instant)
        upper = timeutil.evaluate_validity(0, 10_000, instant)
        self.assertTrue(lower.is_valid and upper.is_valid)
        self.assertEqual(lower.status, "VALID")

    def test_expired_and_not_yet_valid(self):
        instant = timeutil.build_verification_instant(10_000, 0)
        expired = timeutil.evaluate_validity(0, 9_999, instant)
        early = timeutil.evaluate_validity(10_001, 20_000, instant)
        self.assertEqual(expired.status, Codes.CERT_EXPIRED)
        self.assertEqual(early.status, Codes.CERT_NOT_YET_VALID)
        self.assertEqual(expired.margin_after_ms, -1)
        self.assertEqual(early.margin_before_ms, 1)

    def test_repeated_instant_is_identical(self):
        a = timeutil.build_verification_instant(self.WALL, 123, 1)
        b = timeutil.build_verification_instant(self.WALL, 123, 1)
        self.assertEqual(a.effective_verify_ms, b.effective_verify_ms)


if __name__ == "__main__":
    unittest.main()
