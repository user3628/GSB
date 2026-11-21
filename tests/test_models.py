import unittest

import numpy as np

from src.config import BacktestConfig
from src.models import (fit_exponential_smoothing, fit_linear_ar,
                        fit_moving_average, fit_window)


class ModelDeterminismTest(unittest.TestCase):
    def setUp(self):
        rng = np.random.RandomState(7)
        self.window = np.cumsum(rng.normal(0, 1, 32)) + 10.0

    def test_moving_average_is_constant(self):
        cfg = BacktestConfig(horizon=4)
        result = fit_moving_average(self.window, cfg)
        self.assertTrue(all(p == result.predictions[0]
                            for p in result.predictions))
        self.assertAlmostEqual(result.params["mean"],
                               float(np.mean(self.window)))

    def test_exponential_smoothing_damping(self):
        cfg = BacktestConfig(horizon=5, es_alpha=0.4, es_phi=0.8)
        result = fit_exponential_smoothing(self.window, cfg)
        self.assertEqual(len(result.predictions), 5)
        self.assertEqual(result.params["alpha"], 0.4)
        self.assertEqual(result.params["phi"], 0.8)

    def test_linear_ar_recovers_linear_series(self):
        t = np.arange(40, dtype=np.float64)
        series = 2.0 + 0.5 * t
        cfg = BacktestConfig(method="linear_ar", ar_lags=2, horizon=2)
        result = fit_linear_ar(series, cfg)
        np.testing.assert_allclose(result.predictions,
                                   [2.0 + 0.5 * 40, 2.0 + 0.5 * 41],
                                   rtol=1e-8)

    def test_repeated_fits_byte_identical(self):
        for method in ("moving_average", "exponential_smoothing",
                       "linear_ar"):
            cfg = BacktestConfig(method=method, ar_lags=3, horizon=3)
            first = fit_window(self.window, cfg)
            second = fit_window(self.window, cfg)
            self.assertEqual(first.predictions, second.predictions)
            self.assertEqual(first.params, second.params)
            self.assertEqual(first.residuals, second.residuals)


if __name__ == "__main__":
    unittest.main()
