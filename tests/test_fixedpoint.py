import unittest

from tls_workbench import fixedpoint as fp
from tls_workbench.kahan import kahan_sum_fixed, kahan_sum_float, naive_sum_float


class FixedPointTests(unittest.TestCase):
    def test_exact_integer_scaling(self):
        self.assertEqual(fp.from_int(7), 7 * fp.SCALE)
        self.assertEqual(fp.to_str(fp.from_int(7)), "7.000000000")

    def test_half_even_rounding(self):
        # 1 / 2 at scale 1e9 => exact 5e8
        self.assertEqual(fp.from_fraction(1, 2), 500_000_000)
        # 1 / 3 rounds half-even consistently
        first = fp.from_fraction(1, 3)
        self.assertEqual(first, fp.from_fraction(1, 3))
        self.assertEqual(fp.to_str(first), "0.333333333")

    def test_mul_div_roundtrip_and_determinism(self):
        value = fp.from_fraction(25, 10)
        weight = fp.from_fraction(11, 10)
        once = fp.mul(value, weight)
        twice = fp.mul(value, weight)
        self.assertEqual(once, twice)
        back = fp.div(once, weight)
        self.assertEqual(back, value)

    def test_negative_half_even(self):
        self.assertEqual(fp.from_fraction(-1, 2), -500_000_000)

    def test_relative_tolerance(self):
        left = fp.from_int(10)
        right = left + 1_000  # 1e-6 absolute at scale 1e9, rel diff 1e-10
        tol = fp.from_fraction(1, 1_000_000)
        self.assertTrue(fp.within_relative_tolerance(left, right, tol))
        far = fp.from_int(11)
        self.assertFalse(fp.within_relative_tolerance(left, far, tol))

    def test_fixed_kahan_is_exact(self):
        terms = [fp.from_int(1), 3, fp.from_fraction(1, 7), -fp.from_int(2)]
        result = kahan_sum_fixed(terms)
        self.assertEqual(result.compensation, 0)
        self.assertEqual(result.total, sum(terms))


class FloatKahanTests(unittest.TestCase):
    def test_float_compensation_reduces_error(self):
        terms = [1e16, 1.0, 1.0, -1e16]
        naive = naive_sum_float(terms).total
        kahan = kahan_sum_float(terms).total
        # Naive summation loses small terms against the huge cancellation;
        # compensated summation retains them.
        self.assertNotEqual(naive, 2.0)
        self.assertEqual(kahan, 2.0)
        self.assertLess(abs(kahan - 2.0), abs(naive - 2.0))


if __name__ == "__main__":
    unittest.main()
