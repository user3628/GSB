import unittest

import numpy as np

from src.config import BacktestConfig
from src.data import SeriesData
from src.errors import E_MISSING_RATIO, E_SERIES_TOO_SHORT
from src.windows import (build_series_plan, candidate_fold_count,
                         detect_changepoints)


def make_series(values, missing_ts=(), missing_target=()):
    n = len(values)
    target = np.asarray(values, dtype=np.float64)
    ts_mask = np.zeros(n, dtype=bool)
    tg_mask = np.zeros(n, dtype=bool)
    for index in missing_ts:
        ts_mask[index] = True
    for index in missing_target:
        tg_mask[index] = True
        target[index] = np.nan
    return SeriesData(
        series_id="s1", timestamps=list(range(n)), target=target,
        missing_timestamp=ts_mask, missing_target=tg_mask,
        exogenous={})


class WindowTest(unittest.TestCase):
    def test_fold_count_overlaps(self):
        cfg = BacktestConfig(window_length=10, step=2, horizon=3)
        # (20 - 13)//2 + 1 = 4
        self.assertEqual(candidate_fold_count(20, cfg), 4)

    def test_too_short_series_raises(self):
        cfg = BacktestConfig(window_length=24)
        series = make_series(list(range(5)))
        plan = build_series_plan(series, cfg)
        self.assertEqual(plan.error.code, E_SERIES_TOO_SHORT)

    def test_missing_ratio_business_error(self):
        cfg = BacktestConfig(window_length=4, max_missing_ratio=0.2)
        values = list(range(20))
        series = make_series(values, missing_target=(2, 4, 6, 8, 10))
        plan = build_series_plan(series, cfg)
        self.assertEqual(plan.error.code, E_MISSING_RATIO)

    def test_missing_slots_exclude_folds_with_reasons(self):
        cfg = BacktestConfig(window_length=4, step=1, horizon=2,
                             max_missing_ratio=0.5)
        values = list(range(20))
        series = make_series(values, missing_target=(3,))
        plan = build_series_plan(series, cfg)
        self.assertIsNone(plan.error)
        self.assertTrue(plan.excluded)
        reasons = {item["reason"] for item in plan.excluded}
        self.assertIn("MISSING_TRAIN_SLOT", reasons)
        # every kept fold has complete train and test spans
        for fold in plan.folds:
            self.assertFalse(
                np.isnan(series.target[fold.train_start:fold.test_end]).any())

    def test_changepoint_detection_threshold(self):
        values = [1.0] * 10 + [5.0] * 10
        series = make_series(values)
        points = detect_changepoints(series, threshold=0.5)
        self.assertEqual(points, [10])


if __name__ == "__main__":
    unittest.main()
