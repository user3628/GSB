from __future__ import annotations

import unittest

from gspipe.scheduler import TaskEstimate, build_schedule


class SchedulerTests(unittest.TestCase):
    def test_three_budget_admission_is_deterministic(self) -> None:
        estimates = [
            TaskEstimate("task_c", 400),
            TaskEstimate("task_a", 300),
            TaskEstimate("task_b", 700),
            TaskEstimate("task_d", 200),
        ]
        config = {"total_step_budget": 1200, "worker_step_quota": 700, "max_workers": 2}
        first = build_schedule(estimates, config)
        second = build_schedule(list(reversed(estimates)), config)
        self.assertEqual(first.to_dict(), second.to_dict())
        self.assertEqual([item.task_id for item in first.accepted], ["task_a", "task_b", "task_d"])
        self.assertEqual([item.task_id for item in first.rejected], ["task_c"])
        self.assertEqual(first.rejected[0].reason, "TOTAL_STEP_BUDGET")
        self.assertTrue(all(load <= 700 for load in first.lane_loads))
        self.assertLessEqual(first.total_estimated_steps, 1200)
        self.assertEqual(first.planned_parallelism, 2)

    def test_single_task_over_worker_quota(self) -> None:
        plan = build_schedule(
            [TaskEstimate("task_big", 10), TaskEstimate("task_small", 1)],
            {"total_step_budget": 100, "worker_step_quota": 5, "max_workers": 2},
        )
        self.assertEqual([item.task_id for item in plan.accepted], ["task_small"])
        self.assertEqual(plan.rejected[0].task_id, "task_big")
        self.assertEqual(plan.rejected[0].reason, "WORKER_STEP_QUOTA")
        self.assertEqual(plan.rejected[0].details["phase"], "single_task")

    def test_lane_capacity_rejection(self) -> None:
        plan = build_schedule(
            [TaskEstimate("task_a", 6), TaskEstimate("task_b", 6), TaskEstimate("task_c", 5)],
            {"total_step_budget": 100, "worker_step_quota": 10, "max_workers": 2},
        )
        self.assertEqual([item.task_id for item in plan.accepted], ["task_a", "task_b"])
        self.assertEqual(plan.rejected[0].task_id, "task_c")
        self.assertEqual(plan.rejected[0].details["phase"], "no_lane_capacity")


if __name__ == "__main__":
    unittest.main()
