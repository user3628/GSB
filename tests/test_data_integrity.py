import json
import os
import shutil
import tempfile
import unittest

from src.data import load_shards
from src.errors import E_MANIFEST_MISMATCH, ManifestError


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MANIFEST = os.path.join(REPO_ROOT, "samples", "manifest.json")


class DataIntegrityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not os.path.exists(MANIFEST):
            from scripts.generate_samples import generate
            generate(os.path.join(REPO_ROOT, "samples"))

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb-data-")
        self.samples = os.path.join(self.tmp, "samples")
        shutil.copytree(os.path.join(REPO_ROOT, "samples"), self.samples)
        with open(os.path.join(self.samples, "manifest.json"),
                  encoding="utf-8") as handle:
            self.manifest = json.load(handle)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_declared_fingerprint_matches_file(self):
        entry = self.manifest["shards"][0]
        _, shards = load_shards(os.path.join(self.samples,
                                             "manifest.json"))
        self.assertEqual(shards[0].content_fingerprint,
                         entry["content_fingerprint"])

    def test_tampered_row_count_is_rejected(self):
        self.manifest["shards"][0]["rows"] = 999999
        path = os.path.join(self.samples, "manifest.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(self.manifest, handle)
        with self.assertRaises(ManifestError) as ctx:
            load_shards(path)
        self.assertEqual(ctx.exception.code, E_MANIFEST_MISMATCH)

    def test_tampered_fingerprint_is_rejected(self):
        self.manifest["shards"][0]["content_fingerprint"] = "deadbeef"
        path = os.path.join(self.samples, "manifest.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(self.manifest, handle)
        with self.assertRaises(ManifestError) as ctx:
            load_shards(path)
        self.assertEqual(ctx.exception.code, E_MANIFEST_MISMATCH)


if __name__ == "__main__":
    unittest.main()
