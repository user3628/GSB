"""Generator determinism and sample coverage."""

from __future__ import annotations

import hashlib
import json
import os

from _base import WorkspaceTestCase

from rerank_bench.generator import DEFAULT_SEED, generate


def _tree_digest(root):
    h = hashlib.sha256()
    for dirpath, _, files in sorted(os.walk(root)):
        for name in sorted(files):
            path = os.path.join(dirpath, name)
            h.update(name.encode())
            with open(path, "rb") as fh:
                h.update(fh.read())
    return h.hexdigest()


class TestGenerator(WorkspaceTestCase):
    def test_reproducible_byte_identical(self):
        other = os.path.join(self.tmp, "data2")
        generate(other, seed=DEFAULT_SEED, num_shards=4)
        self.assertEqual(_tree_digest(self.data_dir), _tree_digest(other))

    def test_sample_coverage(self):
        categories = set()
        languages = set()
        shards = sorted(f for f in os.listdir(self.data_dir)
                        if f.startswith("shard-"))
        self.assertGreaterEqual(len(shards), 4)
        for name in shards:
            with open(os.path.join(self.data_dir, name),
                      encoding="utf-8") as fh:
                for line in fh:
                    rec = json.loads(line)
                    if rec["type"] == "candidate":
                        categories.add(rec["category"])
                        languages.add(rec["language"])
        self.assertGreaterEqual(len(categories), 3)
        self.assertEqual(languages, {"zh", "en"})

    def test_config_file_present(self):
        self.assertTrue(os.path.exists(
            os.path.join(self.data_dir, "schedule.yaml")))
