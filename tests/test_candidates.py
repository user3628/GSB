import unittest

from qrwb.config import load_config, write_default_config
from qrwb.normalize import build_dispatch_plan
from qrwb.candidates import generate_candidates
from qrwb.sampling import (build_bridge_dict, build_glyph_index,
                           build_synonym_table)
from qrwb.errors import E_UNKNOWN_TOKEN


class CandidatesTest(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.cfg_path = tempfile.mktemp(suffix=".yaml")
        write_default_config(self.cfg_path)
        self.cfg = load_config(self.cfg_path)
        self.resources = {
            "synonym": build_synonym_table(self.cfg),
            "glyph": build_glyph_index(self.cfg),
            "bridge": build_bridge_dict(self.cfg),
        }

    def _bundle(self, raw_query):
        record = {"query_id": "q", "raw_query": raw_query, "lang": "mixed",
                  "channel": "app"}
        plan = build_dispatch_plan(record, self.cfg)
        return plan, generate_candidates(plan, self.cfg, self.resources)

    def test_three_routes_have_required_fields(self):
        plan, bundle = self._bundle("phone 手机")
        self.assertGreater(len(bundle["candidates"]), 0)
        for cand in bundle["candidates"]:
            self.assertIn("route", cand)
            self.assertIn("rewritten_query", cand)
            self.assertIn("confidence", cand)
            self.assertIn("basis", cand)
            self.assertIn("candidate_id", cand)

    def test_synonym_records_entry_version_and_offset(self):
        plan, bundle = self._bundle("phone 手机")
        syn = [c for c in bundle["candidates"] if c["route"] == "synonym"]
        if syn:
            basis = syn[0]["basis"]
            self.assertEqual(basis["type"], "synonym_entry")
            self.assertTrue(basis["entry_version"].startswith("syn-v1"))
            self.assertEqual(len(basis["replaced_offset"]), 2)

    def test_bridge_unknown_marker_fallback(self):
        # 强制构造一个桥接词典必然缺失的词项
        record = {"query_id": "q", "raw_query": "zzzzphone",
                  "lang": "en", "channel": "app"}
        plan = build_dispatch_plan(record, self.cfg)
        bundle = generate_candidates(plan, self.cfg, self.resources)
        bridge = [c for c in bundle["candidates"]
                  if c["route"] == "bridge"]
        self.assertTrue(bridge)
        unknown = [c for c in bridge
                   if c["basis"]["type"] == "unknown_marker_fallback"]
        self.assertTrue(unknown)
        self.assertIn("[UNK:zzzzphone]", unknown[0]["rewritten_query"])
        events = unknown[0]["basis"]["unknown_events"]
        self.assertEqual(events[0]["code"], E_UNKNOWN_TOKEN)

    def test_route_candidate_caps_respected(self):
        record = {"query_id": "q", "raw_query": "phone laptop coffee 手机",
                  "lang": "mixed", "channel": "app"}
        plan = build_dispatch_plan(record, self.cfg)
        bundle = generate_candidates(plan, self.cfg, self.resources)
        for route, count in bundle["route_counts"].items():
            self.assertLessEqual(count, self.cfg.route_max(route))

    def test_deterministic_candidates(self):
        plan, b1 = self._bundle("phone 手机")
        _, b2 = self._bundle("phone 手机")
        from qrwb.util import canonical_dumps
        self.assertEqual(canonical_dumps(b1), canonical_dumps(b2))


if __name__ == "__main__":
    unittest.main()
