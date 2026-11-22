from __future__ import annotations

import unittest

import numpy as np

from gspipe.errors import E_INVALID_PARAM, PipelineError
from gspipe.ops import blur, color_shift, crop, resize, rotate
from gspipe.pngio import decode_rgb_png, encode_rgb_png, pixel_fingerprint


def pattern_image(height: int = 5, width: int = 6) -> np.ndarray:
    image = np.zeros((height, width, 3), dtype=np.uint8)
    for y in range(height):
        for x in range(width):
            image[y, x] = (x * 31 + y * 7, x * 3 + y * 43, (x * 17 + y * 19) % 256)
    return image


class OperatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.image = pattern_image()

    def test_png_encoding_is_byte_deterministic(self) -> None:
        first = encode_rgb_png(self.image)
        second = encode_rgb_png(self.image.copy())
        self.assertEqual(first, second)
        self.assertTrue(np.array_equal(decode_rgb_png(first), self.image))

    def test_crop_exact_pixels_and_bounds(self) -> None:
        output = crop(self.image, {"x": 1, "y": 2, "w": 3, "h": 2})
        self.assertTrue(np.array_equal(output, self.image[2:4, 1:4]))
        with self.assertRaises(PipelineError) as ctx:
            crop(self.image, {"x": 4, "y": 4, "w": 3, "h": 3})
        self.assertEqual(ctx.exception.code, E_INVALID_PARAM)
        self.assertEqual(ctx.exception.operator, "crop")

    def test_resize_nearest_explicit_mapping(self) -> None:
        source = np.array(
            [
                [[0, 0, 0], [10, 0, 0]],
                [[20, 0, 0], [30, 0, 0]],
            ],
            dtype=np.uint8,
        )
        output = resize(source, {"w": 4, "h": 4, "mode": "nearest"})
        expected = np.array(
            [
                [0, 0, 10, 10],
                [0, 0, 10, 10],
                [20, 20, 30, 30],
                [20, 20, 30, 30],
            ],
            dtype=np.uint8,
        )
        self.assertTrue(np.array_equal(output[:, :, 0], expected))

    def test_resize_bilinear_fixedpoint_and_repeatability(self) -> None:
        first = resize(self.image, {"w": 11, "h": 9, "mode": "bilinear"})
        second = resize(self.image.copy(), {"w": 11, "h": 9, "mode": "bilinear"})
        self.assertTrue(np.array_equal(first, second))
        self.assertEqual(first.shape, (9, 11, 3))
        self.assertTrue(np.array_equal(first[0, 0], self.image[0, 0]))
        self.assertTrue(np.array_equal(first[-1, -1], self.image[-1, -1]))
        with self.assertRaises(PipelineError) as ctx:
            resize(self.image, {"w": 4, "h": 4, "mode": "bicubic"})
        self.assertEqual(ctx.exception.code, E_INVALID_PARAM)
        with self.assertRaises(PipelineError) as ctx:
            resize(self.image, {"w": 8193, "h": 1, "mode": "nearest"})
        self.assertEqual(ctx.exception.code, E_INVALID_PARAM)

    def test_rotate_fixed_angle_sampling(self) -> None:
        identity = rotate(self.image, {"angle": 360})
        self.assertTrue(np.array_equal(identity, self.image))
        square = pattern_image(5, 5)
        quarter = rotate(square, {"angle": 90})
        twice = rotate(rotate(square, {"angle": 90}), {"angle": 90})
        half = rotate(square, {"angle": 180})
        self.assertTrue(np.array_equal(twice, half))
        self.assertTrue(np.array_equal(quarter, rotate(square.copy(), {"angle": 90})))
        with self.assertRaises(PipelineError) as ctx:
            rotate(self.image, {"angle": 361})
        self.assertEqual(ctx.exception.code, E_INVALID_PARAM)

    def test_color_shift_fixed_quantization_and_clipping(self) -> None:
        output = color_shift(self.image, {"dr": 10, "dg": -10, "db": 100})
        expected = np.clip(self.image.astype(np.int16) + np.array([10, -10, 100]), 0, 255).astype(np.uint8)
        self.assertTrue(np.array_equal(output, expected))
        with self.assertRaises(PipelineError) as ctx:
            color_shift(self.image, {"dr": 256, "dg": 0, "db": 0})
        self.assertEqual(ctx.exception.code, E_INVALID_PARAM)

    def test_blur_integer_separable_kernel(self) -> None:
        source = np.arange(9, dtype=np.uint8).reshape(3, 3, 1).repeat(3, axis=2)
        output = blur(source, {"radius": 1})
        expected_channel = np.array(
            [
                [1, 2, 3],
                [3, 4, 5],
                [5, 6, 7],
            ],
            dtype=np.uint8,
        )
        # Values are derived from an edge-clamped 3x3 integer box filter.
        self.assertEqual(output.shape, source.shape)
        self.assertEqual(output[:, :, 0].tolist(), expected_channel.tolist())
        self.assertTrue(np.array_equal(blur(source, {"radius": 0}), source))
        with self.assertRaises(PipelineError) as ctx:
            blur(source, {"radius": 33})
        self.assertEqual(ctx.exception.code, E_INVALID_PARAM)

    def test_all_operations_repeat_identically(self) -> None:
        operations = [
            ("crop", {"x": 1, "y": 1, "w": 4, "h": 3}),
            ("resize", {"w": 8, "h": 7, "mode": "nearest"}),
            ("resize", {"w": 8, "h": 7, "mode": "bilinear"}),
            ("rotate", {"angle": 33}),
            ("color_shift", {"dr": -20, "dg": 15, "db": 5}),
            ("blur", {"radius": 2}),
        ]
        funcs = {"crop": crop, "resize": resize, "rotate": rotate, "color_shift": color_shift, "blur": blur}
        for name, params in operations:
            first = funcs[name](self.image, params)
            second = funcs[name](self.image.copy(), params)
            self.assertEqual(pixel_fingerprint(first), pixel_fingerprint(second), msg=name)
            self.assertEqual(encode_rgb_png(first), encode_rgb_png(second), msg=name)


if __name__ == "__main__":
    unittest.main()
