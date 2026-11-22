import unittest

from ragwb import alignment as alignment_mod
from ragwb import answer as answer_mod
from ragwb.errors import E_ANSWER_INVALID, WorkbenchError
from tests.helpers import load_config, make_workspace


def _candidate(doc_id, text, version, channel, lang, score=0.1):
    return {
        "doc_id": doc_id, "score": score,
        "components": {},
        "paragraph": {"doc_id": doc_id, "text": text, "version": version,
                      "channel": channel, "lang": lang},
    }


class AlignmentTest(unittest.TestCase):
    def setUp(self):
        self.data_dir = make_workspace()
        self.config = load_config(self.data_dir)

    def test_conflict_adjudication_prefers_newer_official(self):
        retrieval_product = {
            "session_id": "s", "turn_index": 0, "query_lang": "zh",
            "candidates": [
                _candidate("kb-0001", "极光电池容量为 75 千瓦时。", 3,
                           "official", "zh"),
                _candidate("kb-0002", "极光电池容量为 70 千瓦时。", 1,
                           "forum", "zh"),
            ],
        }
        product = alignment_mod.align(retrieval_product, self.config)
        self.assertEqual(len(product["clusters"]), 1)
        cluster = product["clusters"][0]
        self.assertTrue(cluster["has_conflict"])
        self.assertEqual(cluster["adopted_doc_id"], "kb-0001")
        demoted = {entry["doc_id"]: entry for entry in product["demoted"]}
        self.assertIn("kb-0002", demoted)
        self.assertEqual(
            demoted["kb-0002"]["reason"],
            "conflicting_number_or_date_overridden")
        # Conflicting evidence is retained, never dropped.
        self.assertEqual(
            cluster["evidence_retained"], ["kb-0001", "kb-0002"])

    def test_fully_conflicted_cluster_retains_evidence(self):
        retrieval_product = {
            "session_id": "s", "turn_index": 0, "query_lang": "zh",
            "candidates": [
                _candidate("kb-a", "极光电池容量为 75 千瓦时。", 2,
                           "report", "zh"),
                _candidate("kb-b", "极光电池容量为 90 千瓦时。", 2,
                           "report", "zh"),
            ],
        }
        product = alignment_mod.align(retrieval_product, self.config)
        cluster = product["clusters"][0]
        self.assertTrue(cluster["fully_conflicted"])
        # A deterministic doc_id arbitration still adopts one paragraph.
        self.assertEqual(cluster["adopted_doc_id"], "kb-a")
        self.assertEqual(
            product["adopted"][0]["adoption_reason"],
            "conflict_tie_arbitration")
        self.assertEqual(
            [entry["doc_id"] for entry in product["demoted"]], ["kb-b"])

    def test_date_conflict_detected(self):
        retrieval_product = {
            "session_id": "s", "turn_index": 0, "query_lang": "zh",
            "candidates": [
                _candidate("kb-a", "云岭手机上市日期 2025-02-20。", 2,
                           "official", "zh"),
                _candidate("kb-b", "云岭手机上市日期 2025-01-10。", 1,
                           "report", "zh"),
            ],
        }
        product = alignment_mod.align(retrieval_product, self.config)
        self.assertTrue(product["clusters"][0]["has_conflict"])


class AnswerTest(unittest.TestCase):
    def setUp(self):
        self.data_dir = make_workspace()
        self.config = load_config(self.data_dir)

    def _alignment_product(self):
        return alignment_mod.align(
            {"session_id": "s", "turn_index": 0, "query_lang": "zh",
             "candidates": [
                 _candidate("kb-0001", "极光电池容量为 75 千瓦时。", 3,
                            "official", "zh"),
                 _candidate("kb-0004", "极光电池容量高达 90 千瓦时。", 1,
                            "forum", "zh")]},
            self.config)

    def test_citation_mapping_and_rebuild(self):
        alignment_product = self._alignment_product()
        retrieval_product = {"session_id": "s", "turn_index": 0,
                             "rewritten_query": "极光电池容量"}
        product = answer_mod.assemble(
            alignment_product, retrieval_product, self.config)
        self.assertEqual(product["citation_map"], {"1": "kb-0001"})
        chain = product["evidence_chain"]
        self.assertEqual(chain[0]["version"], 3)
        self.assertEqual(chain[0]["channel"], "official")
        self.assertEqual(chain[0]["text_offset"], {"start": 0, "end": 15})
        rebuilt = answer_mod.rebuild_from_chain(
            product, self.config, "极光电池容量")
        self.assertEqual(rebuilt["answer_text"], product["answer_text"])
        self.assertEqual(rebuilt["citation_map"], product["citation_map"])

    def test_missing_required_slot_raises_invalid(self):
        config = load_config(self.data_dir)
        config["answer"]["template"] = "答案：{answer_body}"
        alignment_product = self._alignment_product()
        with self.assertRaises(WorkbenchError) as caught:
            answer_mod.assemble(
                alignment_product,
                {"session_id": "s", "turn_index": 0,
                 "rewritten_query": "q"}, config)
        self.assertEqual(caught.exception.code, E_ANSWER_INVALID)

    def test_discontinuous_citations_raise_invalid(self):
        chain = [{
            "citation_number": 2, "doc_id": "kb-x",
            "text": "text", "version": 1, "channel": "official",
            "lang": "zh", "adoption_reason": "r",
        }]
        with self.assertRaises(WorkbenchError) as caught:
            answer_mod.render_answer(
                chain, self.config, session_id="s", turn_index=0,
                query="q")
        self.assertEqual(caught.exception.code, E_ANSWER_INVALID)
        self.assertIn("chain", caught.exception.context)


if __name__ == "__main__":
    unittest.main()
