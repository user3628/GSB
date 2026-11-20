"""Numerical stability: Kahan summation, error bounds, determinism."""

import struct
import unittest

from qopt.numeric import (UNIT_ROUNDOFF, clamp01, compare_costs, kahan_sum,
                          naive_sum, round_cost, rounding_contract,
                          summation_error_report)


def bits(x):
    return struct.pack(">d", x)


class KahanTest(unittest.TestCase):
    def test_compensates_mixed_magnitudes(self):
        # Skewed mix: one huge count, many tiny counts, one cancellation.
        values = [1e16] + [1.0] * 1000 + [-1e16]
        kahan = kahan_sum(values)
        naive = naive_sum(values)
        self.assertEqual(kahan, 1000.0)
        self.assertNotEqual(naive, 1000.0)  # naive loses the small terms

    def test_error_bounds_reported(self):
        values = [1e12, 1.0, 1.0, 1.0, -1e12]
        report = summation_error_report(values)
        self.assertEqual(report["n_terms"], 5)
        self.assertAlmostEqual(report["kahan_error_bound"],
                               2.0 * UNIT_ROUNDOFF * report["sum_abs"])
        self.assertAlmostEqual(report["naive_error_bound"],
                               4.0 * UNIT_ROUNDOFF * report["sum_abs"])
        # Kahan stays within its stated bound against the exact result 3.0.
        self.assertLessEqual(abs(report["kahan_result"] - 3.0),
                             report["kahan_error_bound"] + 1e-300)

    def test_bitwise_determinism(self):
        values = [3.14159, 1e-9, 2.71828, 1e12, -7.5, 42.0] * 50
        first = bits(kahan_sum(values))
        for _ in range(100):
            self.assertEqual(bits(kahan_sum(values)), first)

    def test_empty_and_single(self):
        self.assertEqual(kahan_sum([]), 0.0)
        self.assertEqual(kahan_sum([2.5]), 2.5)


class ClampTest(unittest.TestCase):
    def test_bounds(self):
        self.assertEqual(clamp01(-0.5), 0.0)
        self.assertEqual(clamp01(1.5), 1.0)
        self.assertEqual(clamp01(0.25), 0.25)


class RoundingContractTest(unittest.TestCase):
    def test_fixed_decimals(self):
        self.assertEqual(round_cost(1.23456789), 1.234568)
        self.assertEqual(round_cost(0.0), 0.0)
        self.assertEqual(round_cost(-0.0), 0.0)
        self.assertEqual(round_cost(2.5), 2.5)

    def test_idempotent(self):
        self.assertEqual(round_cost(round_cost(7.123456789)), round_cost(7.123456789))

    def test_rounding_note(self):
        note = rounding_contract(3)
        self.assertEqual(note["decimals"], 6)
        self.assertEqual(note["mode"], "ROUND_HALF_EVEN")
        self.assertAlmostEqual(note["max_accumulated_rounding_error"], 1.5e-6)


class CompareTest(unittest.TestCase):
    def test_relative_tolerance(self):
        self.assertEqual(compare_costs(100.0, 100.0 + 1e-8), 0)   # within rel tol
        self.assertEqual(compare_costs(100.0, 100.2), -1)
        self.assertEqual(compare_costs(100.2, 100.0), 1)

    def test_abs_floor(self):
        self.assertEqual(compare_costs(0.0, 1e-13), 0)


if __name__ == "__main__":
    unittest.main()
