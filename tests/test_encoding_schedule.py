from __future__ import annotations

import math
import unittest
from pathlib import Path

import numpy as np

from detdiff.config import EncoderConfig, parse_config
from detdiff.encoding import encode_prompt, schedule_weights
from detdiff.errors import BusinessError, ErrorCode


class EncodingAndScheduleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.encoder = EncoderConfig(domain="unit.test")

    def test_embedding_is_normalized_and_byte_deterministic(self) -> None:
        first, normalized_first = encode_prompt("  Mixed   中文 Prompt  ", self.encoder)
        second, normalized_second = encode_prompt("mixed 中文 prompt", self.encoder)
        self.assertEqual(normalized_first, "mixed 中文 prompt")
        self.assertEqual(normalized_first, normalized_second)
        self.assertEqual(first.shape, (16,))
        self.assertEqual(first.dtype, np.dtype("<f8"))
        self.assertAlmostEqual(float(np.linalg.norm(first)), 1.0, places=14)
        self.assertEqual(first.tobytes(), second.tobytes())

    def test_schedule_length_equals_steps_and_is_deterministic(self) -> None:
        for steps in (1, 2, 7, 64):
            first = schedule_weights(5.25, steps, shard_id="alpha")
            second = schedule_weights(5.25, steps, shard_id="alpha")
            self.assertEqual(first.size, steps)
            self.assertTrue(np.all(np.isfinite(first)))
            self.assertEqual(first.tobytes(), second.tobytes())
        self.assertFalse(np.array_equal(schedule_weights(0.0, 7), schedule_weights(8.0, 7)))

    def test_invalid_guidance_reports_shard_and_field(self) -> None:
        for value in (-1.0, float("nan"), float("inf")):
            with self.subTest(value=value):
                with self.assertRaises(BusinessError) as context:
                    schedule_weights(value, 4, shard_id="alpha")
                self.assertEqual(context.exception.code, ErrorCode.E_INVALID_CONFIG)
                self.assertEqual(context.exception.shard, "alpha")
                self.assertEqual(context.exception.field, "guidance_scale")

    def test_config_validation_rejects_invalid_guidance(self) -> None:
        raw = {
            "schema_version": 1,
            "global": {"image": {"width": 16, "height": 16}},
            "shards": [
                {"id": "alpha", "prompt": "x", "guidance_scale": -0.1, "steps": 3, "seed": 1}
            ],
        }
        with self.assertRaises(BusinessError) as context:
            parse_config(raw, root=Path("."))
        self.assertEqual(context.exception.code, ErrorCode.E_INVALID_CONFIG)
        self.assertEqual(context.exception.shard, "alpha")
        self.assertEqual(context.exception.field, "guidance_scale")


if __name__ == "__main__":
    unittest.main()

