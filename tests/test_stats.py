import unittest

from detpipe.generate import generate_dataset
from detpipe.matching import match_image
from detpipe.stats import build_image_stats, reduce_global


class TestStats(unittest.TestCase):
    def setUp(self):
        self.dataset = generate_dataset()

    def test_image_stats_has_class_and_source_dims(self):
        img = self.dataset["images"][0]
        summary = match_image(img, 0.5, 0.5)
        stats = build_image_stats(summary)
        self.assertTrue(stats["by_class"])
        self.assertEqual(set(stats["by_source"]), set(self.dataset["sources"]))
        for row in stats["by_source"].values():
            self.assertIn("hits", row)
            self.assertIn("false_positives", row)
            self.assertIn("avg_iou", row)

    def test_global_totals_partition_candidates(self):
        summaries = [match_image(img, 0.5, 0.5) for img in self.dataset["images"]]
        stats = reduce_global(summaries)
        totals = stats["totals"]
        partition = (
            totals["matched"] + totals["duplicate"]
            + totals["background"] + totals["low_confidence"]
        )
        self.assertEqual(partition, totals["candidates_evaluated"])
        self.assertEqual(totals["images"], len(self.dataset["images"]))

    def test_reduction_order_independent(self):
        summaries = [match_image(img, 0.5, 0.5) for img in self.dataset["images"]]
        a = reduce_global(summaries)
        b = reduce_global(list(reversed(summaries)))
        self.assertEqual(a, b)


if __name__ == "__main__":
    unittest.main()
