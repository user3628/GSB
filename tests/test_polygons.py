from __future__ import annotations

import unittest

from segval.canonical import canonical_bytes
from segval.config import DEFAULT_CONFIG, validate_config
from segval.decode import decode_shard
from segval.polygons import has_self_intersection, polygon_area, polygon_bbox, polygon_iou


def _shard(predictions):
    return {
        "schema_version": 1,
        "shard_id": "shard-test",
        "shard_index": 1,
        "samples": [
            {
                "image_id": "img-a",
                "width": 100,
                "height": 50,
                "ground_truth": {"person": [[[0.2, 0.2], [0.4, 0.2], [0.4, 0.6], [0.2, 0.6]]]},
                "predictions": predictions,
            }
        ],
    }


class PolygonTests(unittest.TestCase):
    def test_shoelace_area_and_bbox(self):
        polygon = [(0.0, 0.0), (10.0, 0.0), (10.0, 5.0), (0.0, 5.0)]
        self.assertAlmostEqual(polygon_area(polygon), 50.0)
        self.assertEqual(polygon_bbox(polygon), [0.0, 0.0, 10.0, 5.0])

    def test_intersection_and_iou(self):
        a = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]
        b = [(5.0, 0.0), (15.0, 0.0), (15.0, 10.0), (5.0, 10.0)]
        self.assertAlmostEqual(polygon_iou(a, b), 1.0 / 3.0, places=12)

    def test_concave_polygon_intersection_is_not_bbox_approximated(self):
        l_shape = [(0.0, 0.0), (4.0, 0.0), (4.0, 2.0), (2.0, 2.0), (2.0, 4.0), (0.0, 4.0)]
        square = [(1.0, 1.0), (3.0, 1.0), (3.0, 3.0), (1.0, 3.0)]
        self.assertAlmostEqual(polygon_area(l_shape), 12.0)
        self.assertAlmostEqual(polygon_iou(l_shape, square), 3.0 / 13.0, places=12)

    def test_self_intersection_detection(self):
        bowtie = [(0.0, 0.0), (10.0, 10.0), (0.0, 10.0), (10.0, 0.0)]
        self.assertTrue(has_self_intersection(bowtie))
        square = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]
        self.assertFalse(has_self_intersection(square))

    def test_decode_is_byte_deterministic_and_records_invalid_reasons(self):
        shard = _shard(
            [
                {
                    "prediction_id": "p-valid",
                    "class_name": "person",
                    "confidence": 0.9,
                    "source_operator": "baseline",
                    "polygon": [[0.21, 0.21], [0.39, 0.21], [0.39, 0.59], [0.21, 0.59]],
                },
                {
                    "prediction_id": "p-oob",
                    "class_name": "person",
                    "confidence": 0.8,
                    "source_operator": "tile-infer",
                    "polygon": [[-0.1, 0.2], [0.1, 0.2], [0.1, 0.4], [-0.1, 0.4]],
                },
                {
                    "prediction_id": "p-few",
                    "class_name": "person",
                    "confidence": 0.7,
                    "source_operator": "broken",
                    "polygon": [[0.1, 0.1], [0.2, 0.2]],
                },
                {
                    "prediction_id": "p-self",
                    "class_name": "person",
                    "confidence": 0.6,
                    "source_operator": "augment",
                    "polygon": [[0.1, 0.1], [0.3, 0.3], [0.1, 0.3], [0.3, 0.1]],
                },
                {
                    "prediction_id": "p-zero",
                    "class_name": "person",
                    "confidence": 0.5,
                    "source_operator": "broken",
                    "polygon": [[0.1, 0.1], [0.2, 0.1], [0.3, 0.1]],
                },
            ]
        )
        clamp_config = validate_config(DEFAULT_CONFIG)
        reject_config = validate_config(
            {**DEFAULT_CONFIG, "decode": {**DEFAULT_CONFIG["decode"], "out_of_bounds_policy": "reject"}}
        )
        first = decode_shard(shard, clamp_config)
        second = decode_shard(shard, clamp_config)
        self.assertEqual(canonical_bytes(first), canonical_bytes(second))
        by_id = {item["instance_id"]: item for item in first["samples"][0]["predictions"]}
        self.assertTrue(by_id["p-valid"]["valid"])
        self.assertEqual(by_id["p-valid"]["area"], 342.0)
        self.assertEqual(by_id["p-valid"]["bbox"], [21.0, 10.5, 39.0, 29.5])
        self.assertTrue(by_id["p-oob"]["valid"])
        self.assertIn("OUT_OF_BOUNDS", [d["code"] for d in by_id["p-oob"]["diagnostics"]])
        self.assertIn("TOO_FEW_VERTICES", by_id["p-few"]["reason_codes"])
        self.assertIn("SELF_INTERSECTION", by_id["p-self"]["reason_codes"])
        self.assertIn("ZERO_AREA", by_id["p-zero"]["reason_codes"])
        rejected = decode_shard(shard, reject_config)
        rejected_by_id = {item["instance_id"]: item for item in rejected["samples"][0]["predictions"]}
        self.assertFalse(rejected_by_id["p-oob"]["valid"])
        self.assertIn("OUT_OF_BOUNDS", rejected_by_id["p-oob"]["reason_codes"])


if __name__ == "__main__":
    unittest.main()

