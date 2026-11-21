from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from PIL import Image

from shardseek.config import BASE_FEATURE_CONFIG
from shardseek.errors import BusinessError
from shardseek.features import (
    extract_text_features,
    extract_visual_features,
    normalize_caption,
)
from shardseek.generator import generate_samples
from shardseek.util import canonical_bytes, sha256_file


class FeatureTests(unittest.TestCase):
    def test_generator_is_deterministic_and_emits_required_shapes(self) -> None:
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            summary_a = generate_samples(first, seed=20261122)
            summary_b = generate_samples(second, seed=20261122)
            self.assertEqual(summary_a, summary_b)
            self.assertGreaterEqual(summary_a["image_count"], 4)
            self.assertGreaterEqual(summary_a["enabled_count"], 4)
            self.assertEqual(summary_a["quarantined_count"], 1)
            for row in summary_a["images"]:
                path_a = Path(first) / row["image_file"]
                path_b = Path(second) / row["image_file"]
                self.assertEqual(sha256_file(path_a), sha256_file(path_b))
                with Image.open(path_a) as image:
                    self.assertEqual(image.mode, "RGB")
                    self.assertEqual(image.size, (64, 64))

    def test_visual_descriptor_is_byte_stable_and_fixed_dimension(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            generate_samples(directory)
            image = Path(directory) / "images" / "item-01.png"
            one = extract_visual_features(image, BASE_FEATURE_CONFIG, item_id="item-01")
            two = extract_visual_features(image, BASE_FEATURE_CONFIG, item_id="item-01")
            self.assertEqual(one["vector"].shape, (56,))
            self.assertEqual(canonical_bytes(one["summary"]), canonical_bytes(two["summary"]))
            self.assertEqual(one["summary_sha256"], two["summary_sha256"])
            self.assertEqual(len(one["summary"]["blocks"]), 4)

    def test_text_normalization_bow_and_char_ngrams(self) -> None:
        result = extract_text_features(
            "  ＦＵＬＬ　WIDTH\n红色  RED  ",
            BASE_FEATURE_CONFIG,
            item_id="item-nfkc",
        )
        self.assertEqual(result["summary"]["normalized_caption"], "full width 红色 red")
        self.assertEqual(result["vector"].shape, (128,))
        self.assertIn("full", result["summary"]["tokens"])
        self.assertIn("红", result["summary"]["tokens"])
        self.assertGreater(result["summary"]["char_ngram_count"], 0)

    def test_empty_caption_has_item_stage_and_business_code(self) -> None:
        with self.assertRaises(BusinessError) as caught:
            extract_text_features(" \n\t ", BASE_FEATURE_CONFIG, item_id="item-empty")
        self.assertEqual(caught.exception.code, "E_EMPTY_CAPTION")
        self.assertEqual(caught.exception.stage, "text_feature")
        self.assertEqual(caught.exception.item_id, "item-empty")
        self.assertEqual(normalize_caption(" A  B "), "a b")

    def test_empty_image_and_grid_out_of_bounds_are_structured(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            empty = Path(directory) / "empty.png"
            empty.write_bytes(b"")
            with self.assertRaises(BusinessError) as caught:
                extract_visual_features(empty, BASE_FEATURE_CONFIG, item_id="item-image")
            self.assertEqual(caught.exception.code, "E_EMPTY_IMAGE")
            self.assertEqual(caught.exception.stage, "visual_feature")
            self.assertEqual(caught.exception.item_id, "item-image")

            generate_samples(directory)
            image = Path(directory) / "images" / "item-01.png"
            bad_grid = dict(BASE_FEATURE_CONFIG)
            bad_grid["grid_rows"] = 65
            with self.assertRaises(BusinessError) as caught:
                extract_visual_features(image, bad_grid, item_id="item-01")
            self.assertEqual(caught.exception.code, "E_GRID_OUT_OF_BOUNDS")
            self.assertEqual(caught.exception.item_id, "item-01")
            self.assertEqual(caught.exception.stage, "visual_feature")

            four_bins = dict(BASE_FEATURE_CONFIG)
            four_bins["orientation_bins"] = 4
            result = extract_visual_features(image, four_bins, item_id="item-01")
            self.assertEqual(result["vector"].shape, (40,))


if __name__ == "__main__":
    unittest.main()

