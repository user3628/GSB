import unittest

import numpy as np

from src.config import BacktestConfig
from src.engine import _worker, WorkerPayload
from src.errors import E_WORKER_FAILED
from src.metrics import aggregate, build_fold_result
from src.models import fit_moving_average
from src.scheduler import FoldTask


def make_task():
    return FoldTask(
        shard_id="shard-01", series_id="s1", fold_index=0, worker_id=0,
        train_start=0, train_end=4, test_start=4, test_end=7,
        has_changepoint=False)


class MetricsTest(unittest.TestCase):
    def test_mape_zero_denominator_is_skipped_and_counted(self):
        cfg = BacktestConfig(horizon=3)
        fit = fit_moving_average(np.arange(4, dtype=np.float64) + 1.0, cfg)
        actual = [0.0, 5.0, 0.0]  # two zero denominators
        result = build_fold_result(make_task(), fit, actual,
                                   [10, 11, 12], cfg)
        self.assertEqual(result.zero_denominator_count, 2)
        self.assertEqual(len(result.absolute_percentage_errors), 1)
        metrics = aggregate([result])
        self.assertEqual(metrics["summary"]["mape_skipped_zero_denominator"],
                         2)
        self.assertIsNotNone(metrics["summary"]["mape"])

    def test_all_zero_denominator_yields_null_mape(self):
        cfg = BacktestConfig(horizon=2)
        fit = fit_moving_average(np.arange(4, dtype=np.float64) + 1.0, cfg)
        result = build_fold_result(make_task(), fit, [0.0, 0.0],
                                   [10, 11], cfg)
        metrics = aggregate([result])
        self.assertIsNone(metrics["summary"]["mape"])


class WorkerFailureTest(unittest.TestCase):
    def test_worker_exception_becomes_structured_failure(self):
        cfg = BacktestConfig(method="linear_ar", ar_lags=5, horizon=2)
        # window shorter than ar_lags triggers a deterministic exception
        payload = WorkerPayload(
            task=make_task(), values=[1.0, 2.0, 3.0, 4.0],
            actual=[5.0, 6.0], timestamps=[10, 11], config=cfg,
            deadline_ms=2000)
        status, body = _worker(payload)
        self.assertEqual(status, "error")
        self.assertEqual(body["code"], E_WORKER_FAILED)
        self.assertIn("series_id", body)
        self.assertIn("message", body)


if __name__ == "__main__":
    unittest.main()
