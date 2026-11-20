from __future__ import annotations

import io
import tempfile
import unittest
from pathlib import Path

import numpy as np
from PIL import Image

from detdiff.encoding import encode_prompt, schedule_weights
from detdiff.errors import BusinessError, ErrorCode
from detdiff.quantization import quantize_to_png
from detdiff.sampling import generate_noise, load_reference_tensor, run_sampling

from helpers import create_image


class SamplingQuantizationTests(unittest.TestCase):
    def test_noise_and_sampling_are_repeatable(self) -> None:
        embedding, _ = encode_prompt("deterministic field", encoder=_encoder())
        weights = schedule_weights(3.5, 6)
        first = run_sampling(embedding, weights, 991, (12, 10, 3), shard_id="alpha")
        second = run_sampling(embedding, weights, 991, (12, 10, 3), shard_id="alpha")
        self.assertEqual(first[0].tobytes(), second[0].tobytes())
        self.assertEqual(first[1], second[1])
        self.assertEqual(generate_noise(991, (12, 10, 3)).tobytes(), generate_noise(991, (12, 10, 3)).tobytes())

    def test_trajectory_has_bounded_summary_per_step(self) -> None:
        embedding, _ = encode_prompt("trajectory", encoder=_encoder())
        weights = schedule_weights(2.0, 9)
        _, trajectory = run_sampling(embedding, weights, 4, (9, 7, 3), shard_id="alpha")
        self.assertEqual(len(trajectory), 9)
        for index, point in enumerate(trajectory):
            self.assertEqual(set(point), {"step", "mean", "variance"})
            self.assertEqual(point["step"], index)
            self.assertTrue(np.isfinite(point["mean"]))
            self.assertTrue(np.isfinite(point["variance"]))
            self.assertGreaterEqual(point["variance"], 0.0)

    def test_quantization_clamps_and_reports_counts(self) -> None:
        tensor = np.zeros((4, 5, 3), dtype=np.float64)
        tensor[0, 0, 0] = -4.0
        tensor[0, 1, 0] = 9.0
        tensor[1, 1, 1] = float("nan")
        result = quantize_to_png(tensor)
        self.assertEqual(result.pre_clamp_count, 3)
        self.assertEqual(result.post_clamp_count, 0)
        self.assertEqual(result.array.dtype, np.uint8)
        self.assertEqual(result.array.min(), 0)
        self.assertEqual(result.array.max(), 255)
        with Image.open(io.BytesIO(result.png_bytes)) as image:
            self.assertEqual(image.mode, "RGB")
            self.assertEqual(image.size, (5, 4))
        self.assertEqual(result.png_bytes, quantize_to_png(tensor).png_bytes)

    def test_reference_missing_and_wrong_size_are_business_errors(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            missing = root / "missing.png"
            with self.assertRaises(BusinessError) as context:
                load_reference_tensor(missing, (8, 8, 3), shard_id="beta")
            self.assertEqual(context.exception.code, ErrorCode.E_MISSING_REFERENCE)
            self.assertEqual(context.exception.field, "reference")

            wrong = root / "wrong.png"
            create_image(wrong, 5, width=7, height=9)
            with self.assertRaises(BusinessError) as context:
                load_reference_tensor(wrong, (8, 8, 3), shard_id="beta")
            self.assertEqual(context.exception.code, ErrorCode.E_MISSING_REFERENCE)
            self.assertEqual(context.exception.details["expected"], [8, 8, 3])

    def test_reference_fusion_changes_sample_only_when_strength_nonzero(self) -> None:
        embedding, _ = encode_prompt("reference", encoder=_encoder())
        weights = schedule_weights(3.0, 5)
        reference = np.full((8, 8, 3), 0.5, dtype=np.float64)
        pure, _ = run_sampling(embedding, weights, 9, (8, 8, 3), shard_id="beta")
        zero_blend, _ = run_sampling(
            embedding,
            weights,
            9,
            (8, 8, 3),
            reference_tensor=reference,
            blend_strength=0.0,
            shard_id="beta",
        )
        fused, _ = run_sampling(
            embedding,
            weights,
            9,
            (8, 8, 3),
            reference_tensor=reference,
            blend_strength=0.5,
            shard_id="beta",
        )
        self.assertEqual(pure.tobytes(), zero_blend.tobytes())
        self.assertNotEqual(pure.tobytes(), fused.tobytes())


def _encoder():
    from detdiff.config import EncoderConfig

    return EncoderConfig(domain="sampling.tests")


if __name__ == "__main__":
    unittest.main()

