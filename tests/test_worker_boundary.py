from __future__ import annotations

import unittest

from ctr_cvr_workbench.model import execute_tasks


def _sometimes_fail(task: dict) -> dict:
    if task["shard_index"] == 1:
        raise RuntimeError("injected worker failure")
    return {"ok": True, "shard_id": task["shard_id"], "shard_index": task["shard_index"]}


class WorkerBoundaryTests(unittest.TestCase):
    def test_worker_error_is_structured_and_completed_results_are_kept(self) -> None:
        tasks = [
            {"shard_id": "shard-a", "shard_index": 0},
            {"shard_id": "shard-b", "shard_index": 1},
        ]
        successes, errors = execute_tasks(_sometimes_fail, tasks, 2, "sequential")
        self.assertEqual([item["shard_id"] for item in successes], ["shard-a"])
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["code"], "E_WORKER_FAILED")
        self.assertEqual(errors[0]["shard_id"], "shard-b")
        self.assertIn("injected worker failure", errors[0]["message"])


if __name__ == "__main__":
    unittest.main()
