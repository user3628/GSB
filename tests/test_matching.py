import unittest

from detpipe.matching import iou_normalized, match_image


def sample():
    return {
        "image_id": "t",
        "width": 100,
        "height": 100,
        "ground_truth": [
            {"gt_id": "g1", "category": "cat", "bbox": [0.0, 0.0, 0.5, 0.5]},
            {"gt_id": "g2", "category": "cat", "bbox": [0.5, 0.5, 0.5, 0.5]},
        ],
        "candidates": [
            # exact overlap g1, high score
            {"candidate_id": "c0", "category": "cat", "score": 0.9,
             "bbox": [0.0, 0.0, 0.5, 0.5], "source": "op_a"},
            # overlaps g1, duplicate (same IoU family but lower score)
            {"candidate_id": "c1", "category": "cat", "score": 0.8,
             "bbox": [0.02, 0.02, 0.46, 0.46], "source": "op_a"},
            # exact overlap g2
            {"candidate_id": "c2", "category": "cat", "score": 0.7,
             "bbox": [0.5, 0.5, 0.5, 0.5], "source": "op_b"},
            # background: high score, no overlap
            {"candidate_id": "c3", "category": "dog", "score": 0.95,
             "bbox": [0.0, 0.0, 0.1, 0.1], "source": "op_b"},
            # low confidence noise
            {"candidate_id": "c4", "category": "dog", "score": 0.1,
             "bbox": [0.8, 0.8, 0.1, 0.1], "source": "op_c"},
        ],
    }


class TestMatching(unittest.TestCase):
    def test_iou_identical_boxes(self):
        self.assertEqual(iou_normalized([0, 0, 1, 1], [0, 0, 1, 1], 640, 480), 1.0)

    def test_iou_disjoint(self):
        self.assertEqual(iou_normalized([0, 0, 0.2, 0.2], [0.8, 0.8, 0.2, 0.2], 100, 100), 0.0)

    def test_greedy_unique_assignment(self):
        result = match_image(sample(), 0.5, 0.5)
        matched_indices = {m["candidate_index"] for m in result["matches"]}
        self.assertEqual(len(matched_indices), 2)
        self.assertEqual(len(result["matches"]), 2)
        # c0 must beat c1 for g1 on score tie-break of IoU ordering
        g1 = next(m for m in result["matches"] if m["gt_index"] == 0)
        self.assertEqual(g1["candidate_index"], 0)
        self.assertIn(1, [d["candidate_index"] for d in result["duplicate_candidates"]])
        self.assertEqual(result["unmatched_ground_truth"], [])

    def test_background_and_low_confidence_split(self):
        result = match_image(sample(), 0.5, 0.5)
        bg = [c["candidate_index"] for c in result["background_candidates"]]
        low = [c["candidate_index"] for c in result["low_confidence_candidates"]]
        self.assertIn(3, bg)
        self.assertIn(4, low)

    def test_threshold_changes_deterministically(self):
        strict = match_image(sample(), 0.99, 0.5)
        loose = match_image(sample(), 0.3, 0.5)
        # Match count is capped by ground truth count; the observable,
        # deterministic change is where the jittered candidate c1 lands.
        self.assertEqual(len(strict["matches"]), 2)
        self.assertIn(1, [c["candidate_index"] for c in strict["background_candidates"]])
        self.assertIn(1, [c["candidate_index"] for c in loose["duplicate_candidates"]])
        # Repeated runs are reproducible.
        self.assertEqual(loose["matches"], match_image(sample(), 0.3, 0.5)["matches"])

    def test_stable_tie_break_uses_candidate_index(self):
        s = sample()
        # Two identical boxes, identical score: lower index must win.
        s["candidates"][0]["score"] = 0.7
        s["candidates"][1] = dict(s["candidates"][0])
        s["candidates"][1]["candidate_id"] = "c1"
        result = match_image(s, 0.5, 0.5)
        g1 = next(m for m in result["matches"] if m["gt_index"] == 0)
        self.assertEqual(g1["candidate_index"], 0)


if __name__ == "__main__":
    unittest.main()
