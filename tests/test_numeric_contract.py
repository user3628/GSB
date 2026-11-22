from __future__ import annotations

import unittest
from fractions import Fraction

from capacity_lab.errors import InsufficientSamples
from capacity_lab.fixedpoint import (
    compare_at_least,
    compare_at_most,
    fixed_decimal,
    quantize,
)
from capacity_lab.quantiles import (
    linear_fraction,
    nearest_rank_fraction,
    percentile_fraction,
)


class NumericContractTests(unittest.TestCase):
    def test_nearest_rank_and_linear_are_exact(self) -> None:
        values = list(range(1, 101))
        self.assertEqual(nearest_rank_fraction(values, "99%"), 99)
        self.assertEqual(linear_fraction(values, "99%"), Fraction(9901, 100))

    def test_p99_requires_enough_samples(self) -> None:
        with self.assertRaises(InsufficientSamples) as context:
            percentile_fraction(range(1, 100), Fraction(99, 100), "nearest_rank")
        self.assertEqual(context.exception.code, "E_INSUFFICIENT_SAMPLES")
        self.assertEqual(context.exception.details["required_samples"], 100)

    def test_fixed_comparisons_use_relative_allowance(self) -> None:
        passing = compare_at_most(
            100_100,
            100_000,
            1_000,
            actual_decimals=3,
            threshold_decimals=3,
        )
        failing = compare_at_most(
            100_101,
            100_000,
            1_000,
            actual_decimals=3,
            threshold_decimals=3,
        )
        self.assertTrue(passing["passed"])
        self.assertFalse(failing["passed"])
        self.assertEqual(passing["allowance_fixed"], 100)
        self.assertEqual(passing["pass_boundary_fixed"], 100_100)

        throughput = compare_at_least(
            19_981,
            20_000,
            1_000,
            actual_decimals=3,
            threshold_decimals=3,
        )
        self.assertTrue(throughput["passed"])
        self.assertEqual(throughput["pass_boundary_fixed"], 19_980)

    def test_rounding_modes_are_declared_and_deterministic(self) -> None:
        value = Fraction(1, 8)
        self.assertEqual(quantize(value, 1000, "ROUND_HALF_EVEN"), 125)
        self.assertEqual(quantize(Fraction(1, 1000), 100, "ROUND_HALF_UP"), 0)
        self.assertEqual(quantize(Fraction(1, 200), 100, "ROUND_HALF_UP"), 1)
        self.assertEqual(fixed_decimal(-1250, 3), "-1.250")


if __name__ == "__main__":
    unittest.main()

