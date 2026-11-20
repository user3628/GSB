import unittest

from detpipe.config import PipelineConfig
from detpipe.errors import E_BUDGET_EXHAUSTED, E_INVALID_CONFIG, E_WORKER_QUOTA, PipeError
from detpipe.generate import generate_dataset
from detpipe.scheduler import BudgetScheduler, plan_shards


class TestScheduler(unittest.TestCase):
    def setUp(self):
        self.plans = plan_shards(generate_dataset()["images"], shard_size=2)

    def test_global_budget_never_exceeded(self):
        sched = BudgetScheduler(self.plans, max_evaluations=30, worker_quota=1000, workers=2)
        accepted_cost = 0
        rejections = []
        for plan in self.plans:
            admission = sched.request(plan, worker_id=plan.index % 2)
            if admission.accepted:
                accepted_cost += plan.candidate_count
            else:
                rejections.append(admission.code)
        self.assertLessEqual(accepted_cost, 30)
        self.assertIn(E_BUDGET_EXHAUSTED, rejections)

    def test_worker_quota_enforced(self):
        sched = BudgetScheduler(self.plans, max_evaluations=10**6, worker_quota=5, workers=2)
        for plan in self.plans:
            admission = sched.request(plan, worker_id=0)
            if admission.accepted:
                self.assertLessEqual(
                    sched.state_snapshot()["per_worker_used"][0], 5
                )
        self.assertLessEqual(sched.state_snapshot()["per_worker_used"][0], 5)
        # worker 1 keeps its own independent quota
        self.assertEqual(sched.state_snapshot()["per_worker_used"][1], 0)

    def test_quota_rejection_code(self):
        sched = BudgetScheduler(self.plans, max_evaluations=10**6, worker_quota=1, workers=1)
        admission = sched.request(self.plans[0], worker_id=0)
        self.assertFalse(admission.accepted)
        self.assertEqual(admission.code, E_WORKER_QUOTA)

    def test_invalid_config_raises_with_field(self):
        with self.assertRaises(PipeError) as ctx:
            PipelineConfig(max_evaluations=-3).validate()
        self.assertEqual(ctx.exception.code, E_INVALID_CONFIG)
        self.assertEqual(ctx.exception.field, "max_evaluations")
        with self.assertRaises(PipeError) as ctx:
            PipelineConfig(iou_threshold=2.0).validate()
        self.assertEqual(ctx.exception.field, "iou_threshold")


if __name__ == "__main__":
    unittest.main()
