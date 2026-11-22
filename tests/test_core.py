from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import numpy as np

from image_workbench.chain import parse_chain
from image_workbench.config import Config
from image_workbench.demo import demo_config_mapping, generate_demo
from image_workbench.errors import E_CHAIN_INVALID, WorkbenchError
from image_workbench.imaging import encode_png_rgb, load_rgb, pixel_digest
from image_workbench.models import TaskSpec
from image_workbench.operators import apply_chain, crop, quantize_rgb, resize, rotate
from image_workbench.workbench import Workbench


class OperatorTests(unittest.TestCase):
    def test_quantization_is_round_half_up_and_clamped(self) -> None:
        values = np.array([[[-1.0, 0.49, 0.5], [1.5, 254.6, 300.0]]], dtype=np.float64)
        output = quantize_rgb(values)
        self.assertEqual(output.tolist(), [[[0, 0, 1], [2, 255, 255]]])

    def test_crop_and_nearest_resize_are_exact(self) -> None:
        image = np.arange(4 * 5 * 3, dtype=np.uint8).reshape(4, 5, 3)
        cropped = crop(image, {"x": 1, "y": 1, "width": 3, "height": 2})
        self.assertEqual(cropped.shape, (2, 3, 3))
        np.testing.assert_array_equal(cropped, image[1:3, 1:4, :])
        base = np.array(
            [
                [[0, 0, 0], [10, 10, 10]],
                [[20, 20, 20], [30, 30, 30]],
            ],
            dtype=np.uint8,
        )
        nearest = resize(base, {"width": 4, "height": 4, "mode": "nearest"})
        self.assertEqual(nearest[0, 0].tolist(), [0, 0, 0])
        self.assertEqual(nearest[0, 3].tolist(), [10, 10, 10])
        self.assertEqual(nearest[3, 3].tolist(), [30, 30, 30])

    def test_bilinear_constant_image_is_constant(self) -> None:
        image = np.full((3, 5, 3), 37, dtype=np.uint8)
        output = resize(image, {"width": 9, "height": 7, "mode": "bilinear"})
        self.assertTrue(np.all(output == 37))

    def test_rotate_zero_and_deterministic_chain(self) -> None:
        rng = np.random.default_rng(123)
        image = rng.integers(0, 256, size=(11, 13, 3), dtype=np.uint8)
        np.testing.assert_array_equal(rotate(image, {"angle": 0}), image)
        chain = [
            {"op": "rotate", "params": {"angle": 33.25}},
            {"op": "resize", "params": {"width": 8, "height": 9, "mode": "bilinear"}},
            {"op": "color_shift", "params": {"red": 0.5, "green": -2.25, "blue": 7.0}},
            {"op": "crop", "params": {"x": 1, "y": 2, "width": 5, "height": 4}},
        ]
        first = apply_chain(image, chain)
        second = apply_chain(image, chain)
        np.testing.assert_array_equal(first, second)
        self.assertEqual(first.shape, (4, 5, 3))

    def test_canonical_png_round_trip(self) -> None:
        rng = np.random.default_rng(9001)
        image = rng.integers(0, 256, size=(17, 19, 3), dtype=np.uint8)
        encoded = encode_png_rgb(image)
        self.assertEqual(encoded, encode_png_rgb(image))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "x.png"
            path.write_bytes(encoded)
            loaded = load_rgb(path)
        np.testing.assert_array_equal(loaded.pixels, image)
        self.assertEqual(loaded.content_digest, pixel_digest(image))


class ChainValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = Config.from_mapping(demo_config_mapping())

    def _task(self, operations: list[dict], allowed: list[str] | None = None) -> TaskSpec:
        names = [operation["op"] for operation in operations]
        return TaskSpec.from_mapping(
            {
                "task_id": "chain-test",
                "input": "x.png",
                "instruction": "test",
                "allowed_operators": allowed if allowed is not None else names,
                "operations": operations,
                "expected_output": {"width": 1, "height": 1, "sha256": "0" * 64},
            }
        )

    def assert_chain_error(self, task: TaskSpec, position: int, reason: str) -> None:
        with self.assertRaises(WorkbenchError) as raised:
            parse_chain(task, (64, 64), self.config)
        error = raised.exception
        self.assertEqual(error.code, E_CHAIN_INVALID)
        self.assertEqual(error.task_id, "chain-test")
        self.assertEqual(error.position, position)
        self.assertEqual(error.reason, reason)

    def test_crop_empty_intersection_at_position(self) -> None:
        task = self._task(
            [
                {"op": "crop", "x": 10, "y": 10, "width": 20, "height": 20},
                {"op": "crop", "x": 80, "y": 80, "width": 5, "height": 5},
            ]
        )
        self.assert_chain_error(task, 1, "CROP_EMPTY_INTERSECTION")

    def test_resize_positive_and_angle_range(self) -> None:
        self.assert_chain_error(
            self._task([{"op": "resize", "width": 0, "height": 8}]),
            0,
            "RESIZE_NON_POSITIVE_SIZE",
        )
        self.assert_chain_error(
            self._task([{"op": "rotate", "angle": 181}]),
            0,
            "ROTATE_ANGLE_OUT_OF_RANGE",
        )

    def test_unknown_and_not_allowed(self) -> None:
        self.assert_chain_error(
            self._task([{"op": "blur", "radius": 2}], allowed=["blur"]),
            0,
            "OPERATOR_UNKNOWN",
        )
        task = TaskSpec.from_mapping(
            {
                "task_id": "chain-test",
                "input": "x.png",
                "allowed_operators": ["resize"],
                "operations": [{"op": "rotate", "angle": 5}],
                "expected_output": {"width": 1, "height": 1, "sha256": "0" * 64},
            }
        )
        self.assert_chain_error(task, 0, "OPERATOR_NOT_ALLOWED")

    def test_chain_length_limit(self) -> None:
        operations = [{"op": "rotate", "angle": 1}] * (self.config.max_chain_length + 1)
        task = self._task(operations)
        self.assert_chain_error(task, self.config.max_chain_length, "CHAIN_TOO_LONG")


class SchedulerTests(unittest.TestCase):
    def test_demo_budget_acceptance_and_shared_shard_cost(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / "demo"
            generate_demo(workspace)
            config, prepared, plan = Workbench(workspace).plan()
        self.assertEqual(plan.accepted_task_ids, ["t010", "t020", "t030"])
        self.assertEqual(plan.rejected_task_ids, ["t040", "t050"])
        self.assertEqual(plan.accepted_steps, 30)
        self.assertLessEqual(plan.accepted_steps, config.global_step_budget)
        self.assertTrue(all(slot.used_steps <= config.worker_step_quota for slot in plan.slots))
        t010 = plan.decision_for("t010")
        t030 = plan.decision_for("t030")
        self.assertEqual(t010.estimated_steps, 10)
        self.assertEqual(t030.estimated_steps, 10)
        self.assertIn("E_WORKER_QUOTA_EXCEEDED", plan.decision_for("t040").reason_codes)
        self.assertEqual(plan.decision_for("t050").reason_codes, ["E_CHAIN_INVALID"])
        self.assertEqual(plan.decision_for("t050").error["position"], 0)


if __name__ == "__main__":
    unittest.main()
