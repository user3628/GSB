from __future__ import annotations

import unittest

from gspipe.parser import parse_instruction
from gspipe.pngio import decode_rgb_png
from gspipe.sample_data import generate_samples


class SampleDataTests(unittest.TestCase):
    def test_samples_are_deterministic_and_complete(self) -> None:
        first_shards, first_tasks, first_config = generate_samples()
        second_shards, second_tasks, second_config = generate_samples()
        self.assertEqual(first_shards, second_shards)
        self.assertEqual(first_tasks, second_tasks)
        self.assertEqual(first_config, second_config)
        self.assertGreaterEqual(len(first_shards), 4)
        self.assertGreaterEqual(len(first_tasks["tasks"]), 6)
        self.assertTrue(any(task["latency_budget_ms"] == 0 for task in first_tasks["tasks"]))
        self.assertTrue(any(any("\u4e00" <= char <= "\u9fff" for char in task["instruction"]) for task in first_tasks["tasks"]))
        self.assertTrue(any(any(char.isascii() and char.isalpha() for char in task["instruction"]) for task in first_tasks["tasks"]))

        for task in first_tasks["tasks"]:
            image = decode_rgb_png(first_shards[task["input"]])
            parsed = parse_instruction(
                task["task_id"],
                task["instruction"],
                task["allowed_ops"],
                (image.shape[1], image.shape[0]),
            )
            self.assertEqual(parsed.chain_summary, task["expected_summary"])
            self.assertEqual(
                list(parsed.output_size),
                [task["expected_output"]["width"], task["expected_output"]["height"]],
            )


if __name__ == "__main__":
    unittest.main()
