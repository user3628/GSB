import unittest

from qrwb.config import load_config
from qrwb.errors import E_EMPTY_QUERY
from qrwb.normalize import normalize_query, tokenize, build_dispatch_plan
from qrwb.util import canonical_dumps


class NormalizeDispatchTest(unittest.TestCase):
    def setUp(self):
        self.cfg = load_config("config/demo.yaml") \
            if __import__("os").path.exists("config/demo.yaml") \
            else load_config(self._write_default())

    @staticmethod
    def _write_default():
        from qrwb.config import write_default_config
        import tempfile
        path = tempfile.mktemp(suffix=".yaml")
        write_default_config(path)
        return path

    def test_nfkc_fullwidth_lowercase_whitespace(self):
        self.assertEqual(normalize_query("Ｂｕｙ　Phone"), "buy phone")
        self.assertEqual(normalize_query("  手机  耳机 "), "手机 耳机")

    def test_tokenize_offsets(self):
        tokens = tokenize("buy 手机 x1")
        kinds = [(t.text, t.kind, t.start, t.end) for t in tokens]
        self.assertIn(("buy", "latin", 0, 3), kinds)
        self.assertIn(("手", "cjk", 4, 5), kinds)
        self.assertIn(("x1", "latin", 7, 9), kinds)

    def test_empty_query_error(self):
        record = {"query_id": "q1", "raw_query": "!!!", "lang": "empty",
                  "channel": "app"}
        plan = build_dispatch_plan(record, self.cfg)
        self.assertEqual(plan["status"], "error")
        self.assertEqual(plan["error"]["code"], E_EMPTY_QUERY)
        self.assertEqual(plan["error"]["query_id"], "q1")
        self.assertIn("normalized_query", plan["error"]["context"])

    def test_deterministic_plan_bytes(self):
        record = {"query_id": "q2", "raw_query": "phone 手机",
                  "lang": "mixed", "channel": "web"}
        a = canonical_dumps(build_dispatch_plan(record, self.cfg))
        b = canonical_dumps(build_dispatch_plan(record, self.cfg))
        self.assertEqual(a, b)
        plan = build_dispatch_plan(record, self.cfg)
        self.assertEqual(set(plan["routes"].keys()),
                         {"synonym", "glyph", "bridge"})


if __name__ == "__main__":
    unittest.main()
