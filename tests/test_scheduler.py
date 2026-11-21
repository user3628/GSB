import unittest

from src.config import BacktestConfig
from src.errors import E_BUDGET_EXCEEDED, E_WORKER_QUOTA
from src.scheduler import schedule_folds
from src.windows import FoldSpec


def make_folds(shard_id, count):
    return [FoldSpec(series_id=f"s{i % 3}", fold_index=i,
                     train_start=i, train_end=i + 4,
                     test_start=i + 4, test_end=i + 6,
                     has_changepoint=False)
            for i in range(count)]


class SchedulerTest(unittest.TestCase):
    def test_global_budget_never_exceeded(self):
        cfg = BacktestConfig(max_workers=3, global_fit_budget=10,
                             worker_fit_quota=100)
        folds = {"shard-01": make_folds("shard-01", 25)}
        result = schedule_folds(folds, cfg)
        self.assertEqual(result.accepted_count, 10)
        self.assertEqual(len(result.rejected), 15)
        self.assertTrue(all(item["code"] == E_BUDGET_EXCEEDED
                            for item in result.rejected))
        self.assertLessEqual(
            sum(result.worker_loads.values()), cfg.global_fit_budget)

    def test_worker_quota_binds(self):
        cfg = BacktestConfig(max_workers=2, global_fit_budget=100,
                             worker_fit_quota=4)
        folds = {"shard-01": make_folds("shard-01", 20)}
        result = schedule_folds(folds, cfg)
        self.assertEqual(result.accepted_count, 8)
        self.assertTrue(all(item["code"] == E_WORKER_QUOTA
                            for item in result.rejected))
        self.assertEqual(max(result.worker_loads.values()), 4)

    def test_deterministic_worker_assignment(self):
        cfg = BacktestConfig(max_workers=2, global_fit_budget=100,
                             worker_fit_quota=100)
        folds = {"shard-01": make_folds("shard-01", 8)}
        first = schedule_folds(folds, cfg)
        second = schedule_folds(folds, cfg)
        self.assertEqual([t.worker_id for t in first.accepted],
                         [t.worker_id for t in second.accepted])

    def test_canonical_cross_shard_order(self):
        cfg = BacktestConfig(max_workers=2, global_fit_budget=100,
                             worker_fit_quota=100)
        folds = {"shard-02": make_folds("shard-02", 3),
                 "shard-01": make_folds("shard-01", 3)}
        result = schedule_folds(folds, cfg)
        shard_sequence = [t.shard_id for t in result.accepted]
        self.assertEqual(shard_sequence,
                         ["shard-01"] * 3 + ["shard-02"] * 3)


if __name__ == "__main__":
    unittest.main()
