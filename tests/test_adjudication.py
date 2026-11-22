import tempfile
import unittest

from qrwb.config import load_config, write_default_config
from qrwb.normalize import build_dispatch_plan
from qrwb.candidates import generate_candidates
from qrwb.sampling import (build_bridge_dict, build_glyph_index,
                           build_synonym_table)
from qrwb.adjudication import adjudicate, overlap_ratio


class AdjudicationTest(unittest.TestCase):
    def setUp(self):
        path = tempfile.mktemp(suffix=".yaml")
        write_default_config(path)
        self.cfg = load_config(path)
        self.resources = {
            "synonym": build_synonym_table(self.cfg),
            "glyph": build_glyph_index(self.cfg),
            "bridge": build_bridge_dict(self.cfg),
        }

    def test_log_domain_scoring_and_deterministic_tie_break(self):
        record = {"query_id": "q", "raw_query": "phone 手机",
                  "lang": "mixed", "channel": "app"}
        plan = build_dispatch_plan(record, self.cfg)
        bundle = generate_candidates(plan, self.cfg, self.resources)
        result = adjudicate(bundle, plan, self.cfg)
        self.assertLessEqual(len(result["adoptions"]),
                             self.cfg.adjudication["max_adoptions_per_query"])
        scores = [c["score"] for c in
                  result["adoptions"] + result["rejected"]]
        self.assertEqual(scores, sorted(scores, reverse=True))
        # 重复运行完全一致
        again = adjudicate(bundle, plan, self.cfg)
        from qrwb.util import canonical_dumps
        self.assertEqual(canonical_dumps(result), canonical_dumps(again))

    def test_duplicate_and_empty_rewrite_rejected(self):
        record = {"query_id": "q", "raw_query": "phone 手机",
                  "lang": "mixed", "channel": "app"}
        plan = build_dispatch_plan(record, self.cfg)
        bundle = generate_candidates(plan, self.cfg, self.resources)
        result = adjudicate(bundle, plan, self.cfg)
        reasons = {c["reject_reason"] for c in result["rejected"]}
        self.assertTrue(reasons.issubset(
            {"ADOPTION_LIMIT", "EMPTY_REWRITE",
             "DUPLICATE_OF_ORIGINAL", "DUPLICATE_REWRITE"}))
        adopted_texts = result["adopted_queries"]
        self.assertNotIn(plan["normalized_query"], adopted_texts)
        self.assertEqual(len(adopted_texts), len(set(adopted_texts)))

    def test_overlap_ratio_bounds(self):
        value = overlap_ratio("buy phone", "buy watch")
        self.assertGreaterEqual(value, 0.0)
        self.assertLessEqual(value, 1.0)

    def test_rejected_kept_with_basis_in_diagnostics(self):
        record = {"query_id": "q", "raw_query": "phone laptop 手机 咖啡",
                  "lang": "mixed", "channel": "app"}
        plan = build_dispatch_plan(record, self.cfg)
        bundle = generate_candidates(plan, self.cfg, self.resources)
        result = adjudicate(bundle, plan, self.cfg)
        for rejected in result["rejected"]:
            self.assertIn("basis", rejected)
            self.assertIn("reject_reason", rejected)


if __name__ == "__main__":
    unittest.main()
