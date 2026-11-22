import os
import shutil
import unittest

from ragwb import generator
from ragwb.io_utils import digest_bytes
from ragwb.pipeline import Pipeline
from tests.helpers import fresh_run_dir


class DemoDeterminismTest(unittest.TestCase):
    def test_generated_data_is_byte_reproducible(self):
        base_a = os.path.join(self._base(), "data-a")
        base_b = os.path.join(self._base(), "data-b")
        generator.generate(base_a)
        generator.generate(base_b)
        first_root, second_root = base_a, base_b
        relative = [
            "config.yaml", "manifest.json",
            os.path.join("kb", "kb_shard_1.jsonl"),
            os.path.join("kb", "kb_shard_2.jsonl"),
            os.path.join("kb", "kb_shard_3.jsonl"),
            os.path.join("sessions", "sess-s1-aurora.jsonl"),
            os.path.join("sessions", "sess-s4-budget.jsonl"),
        ]
        for rel in relative:
            with open(os.path.join(first_root, rel), "rb") as handle:
                first_digest = digest_bytes(handle.read())
            with open(os.path.join(second_root, rel), "rb") as handle:
                second_digest = digest_bytes(handle.read())
            self.assertEqual(first_digest, second_digest, rel)

    def test_full_run_has_fixed_digest_and_covers_required_scenarios(self):
        data_dir = os.path.join(self._base(), "data-c")
        generator.generate(data_dir)
        run_dir = fresh_run_dir(data_dir, "run-c")
        summary = Pipeline(data_dir, run_dir).run()
        self.assertEqual(
            summary["run_digest"],
            Pipeline(data_dir, fresh_run_dir(data_dir, "run-d")).run()[
                "run_digest"])
        self.assertGreaterEqual(
            len([name for name in os.listdir(os.path.join(
                data_dir, "sessions"))]), 4)
        self.assertEqual(
            len(os.listdir(os.path.join(data_dir, "kb"))), 3)
        self.assertEqual(summary["budget"]["consumed"],
                         summary["budget"]["budget"])
        self.assertEqual(
            summary["sessions"]["sess-s4-budget"]["error"],
            "E_RETRIEVAL_EMPTY")
        self.assertEqual(
            summary["sessions"]["sess-s5-fallback"]["error"], None)

    def _base(self):
        base = os.path.join(os.path.dirname(__file__),
                            ".tmp-demo-determinism")
        os.makedirs(base, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(base, ignore_errors=True))
        return base


if __name__ == "__main__":
    unittest.main()
