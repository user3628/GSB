from __future__ import annotations

import unittest

from sumengine.candidates import generate_candidates, generate_compressed
from sumengine.config import config_from_file, normalize_config
from sumengine.errors import EngineError
from sumengine.fusion import fuse_candidates
from sumengine.samples import load_documents
from sumengine.scheduler import schedule_documents, validate_documents
from sumengine.text import prepare_sentences, split_sentences
from sumengine.utils import canonical_bytes

from .common import SAMPLES


class TextAndCandidateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config = config_from_file(SAMPLES / "config.yaml")
        cls.docs = validate_documents(load_documents(SAMPLES / "documents.jsonl"), cls.config)
        cls.by_id = {doc["doc_id"]: doc for doc in cls.docs}

    def test_sentence_offsets_are_exact(self) -> None:
        body = "第一句！Second sentence? 第三句。\n\nEnglish final."
        sentences = split_sentences(body)
        self.assertEqual([body[item["start"] : item["end"]] for item in sentences], [item["text"] for item in sentences])
        self.assertEqual([item["index"] for item in sentences], list(range(len(sentences))))
        self.assertGreaterEqual(len(sentences), 4)

    def test_input_too_short_carries_doc_id(self) -> None:
        doc = {"doc_id": "tiny-1", "body": "太短。", "title": "短", "language": "zh"}
        with self.assertRaises(EngineError) as context:
            prepare_sentences(doc, self.config)
        self.assertEqual(context.exception.code, "E_INPUT_TOO_SHORT")
        self.assertEqual(context.exception.doc_id, "tiny-1")

    def test_extractive_scores_and_candidate_are_deterministic(self) -> None:
        doc = self.by_id["news-zh-001"]
        first = generate_candidates(doc, self.config)
        second = generate_candidates(doc, self.config)
        self.assertEqual(canonical_bytes(first), canonical_bytes(second))
        candidate = first["candidates"][0]
        self.assertEqual(candidate["route"], "extractive")
        self.assertTrue(candidate["selected_sentence_indices"])
        self.assertEqual(len(candidate["sentences"]), first["sentence_count"])
        for sentence in candidate["sentences"]:
            self.assertEqual(doc["body"][sentence["start"] : sentence["end"]], sentence["text"])
            self.assertEqual(
                set(sentence["components"]),
                {"term_frequency", "position", "title_overlap", "length_fit"},
            )

    def test_all_three_routes_keep_coverage_and_fallbacks(self) -> None:
        doc = self.by_id["news-zh-001"]
        bundle = generate_candidates(doc, self.config)
        extractive, compressed, template = bundle["candidates"]
        self.assertEqual(extractive["route"], "extractive")
        self.assertEqual(compressed["route"], "compressed")
        self.assertEqual(template["route"], "template")
        self.assertGreaterEqual(compressed["coverage"] + 1e-12, compressed["coverage_minimum"])
        self.assertTrue(compressed["coverage_ok"])
        self.assertGreaterEqual(template["coverage"] + 1e-12, template["coverage_minimum"])
        self.assertTrue(template["coverage_ok"])
        self.assertEqual(compressed["fallback_count"], 1)
        self.assertEqual(compressed["fallbacks"][0]["reason"], "NO_SYNONYM_ENTRY")
        self.assertEqual(compressed["sentence_count_out"], len(compressed["kept_sentences"]))
        self.assertIn("表示", {item["from"] for item in compressed["replacements"]})
        self.assertTrue(template["slots"]["who"])
        self.assertTrue(template["slots"]["what"])

    def test_compression_reports_removed_fragments_when_guard_allows(self) -> None:
        doc = self.by_id["news-en-001"]
        bundle = generate_candidates(doc, self.config)
        compressed = bundle["candidates"][1]
        self.assertGreaterEqual(len(compressed["removed_fragments"]), 1)
        for fragment in compressed["removed_fragments"]:
            self.assertTrue(fragment["text"])
            self.assertTrue(fragment["rule"])
        self.assertFalse(compressed["silent_sentence_drop"])

    def test_missing_unrelated_config_is_never_silently_ignored(self) -> None:
        config = normalize_config({})
        config["runtime"]["min_body_chars"] = 20
        config["compression"]["required_replacement_terms"] = ["记者"]
        doc = {
            "doc_id": "fallback-doc",
            "title": "测试记者稿件",
            "body": "记者在现场记录了一项新的公共服务试点。工作人员介绍了试点范围与后续评估安排。",
            "language": "zh",
            "source": "wire",
            "target_length": "short",
        }
        sentences = prepare_sentences(doc, config)
        from sumengine.candidates import generate_extractive
        from sumengine.text import key_points

        points = key_points(doc, sentences)
        extractive = generate_extractive(doc, sentences, points, config)
        compressed = generate_compressed(doc, sentences, points, config, extractive)
        self.assertGreaterEqual(compressed["fallback_count"], 1)
        self.assertEqual(compressed["fallbacks"][0]["action"], "KEEP_ORIGINAL")


class FusionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config = config_from_file(SAMPLES / "config.yaml")
        cls.docs = validate_documents(load_documents(SAMPLES / "documents.jsonl"), cls.config)
        cls.by_id = {doc["doc_id"]: doc for doc in cls.docs}

    @staticmethod
    def _candidate(route: str, text: str, *, coverage: float = 0.8, language: str = "zh", reasons=None):
        language_score = 1.0 if language == "zh" else 0.0
        return {
            "candidate_id": f"tie:{route}",
            "route": route,
            "text": text,
            "length_chars": len(text),
            "target_band": {"min_chars": 5, "max_chars": 100},
            "coverage": coverage,
            "language": {"consistency_score": language_score, "consistent": language_score > 0},
            "structural_unavailable_reasons": reasons or [],
            "coverage_minimum": 0.5,
            "coverage_ok": True,
        }

    def test_tie_is_decided_by_route_priority(self) -> None:
        doc = {"doc_id": "tie-doc", "source": "wire", "language": "zh", "target_length": "short"}
        text = "相同文本"
        bundle = {
            "candidates": [
                self._candidate("extractive", text),
                self._candidate("compressed", text),
                self._candidate("template", text),
            ]
        }
        result = fuse_candidates(doc, bundle, self.config)
        self.assertEqual(result["winner"]["route"], "extractive")
        self.assertIn("TIE_BROKEN", result["candidates"][1]["eliminated_reason"])

    def test_unavailable_candidates_are_marked_not_dropped(self) -> None:
        doc = self.by_id["news-zh-001"]
        bundle = generate_candidates(doc, self.config)
        fusion = fuse_candidates(doc, bundle, self.config)
        self.assertEqual(len(fusion["candidates"]), 3)
        template = next(item for item in fusion["candidates"] if item["route"] == "template")
        self.assertFalse(template["available"])
        self.assertIn("LENGTH_OUT_OF_BAND", template["unavailable_reasons"])
        self.assertIsNotNone(template["eliminated_reason"])

    def test_language_mismatch_is_unavailable(self) -> None:
        doc = {"doc_id": "lang-doc", "source": "wire", "language": "zh", "target_length": "short"}
        bad = self._candidate("template", "This summary is English only.", language="en", reasons=["LANGUAGE_MISMATCH"])
        bad["language"] = {"consistency_score": 0.0, "consistent": False}
        good = self._candidate("extractive", "中文摘要内容足够长。", language="zh")
        result = fuse_candidates(doc, {"candidates": [good, bad]}, self.config)
        self.assertEqual(result["winner"]["route"], "extractive")
        result_bad = next(item for item in result["candidates"] if item["route"] == "template")
        self.assertFalse(result_bad["available"])
        self.assertIn("LANGUAGE_MISMATCH", result_bad["unavailable_reasons"])


class SchedulerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config = config_from_file(SAMPLES / "config.yaml")
        cls.docs = validate_documents(load_documents(SAMPLES / "documents.jsonl"), cls.config)

    def test_sample_schedule_has_one_over_budget_document(self) -> None:
        result = schedule_documents(self.docs, self.config)
        self.assertEqual(len(result["accepted_doc_ids"]), 4)
        self.assertEqual(result["accepted_doc_ids"], sorted(result["accepted_doc_ids"]))
        self.assertEqual(result["unprocessed"][0]["doc_id"], "news-zh-oversize")
        self.assertEqual(result["unprocessed"][0]["code"], "E_GLOBAL_BUDGET")
        self.assertLessEqual(result["accepted_evaluations"], result["global_budget"])

    def test_worker_quota_and_latency_rejections(self) -> None:
        quota_config = normalize_config(self.config)
        quota_config["budget"]["per_worker_sentence_evaluations"] = 20
        quota_config["budget"]["global_sentence_evaluations"] = 200
        quota = schedule_documents(self.docs[:2], quota_config)
        self.assertFalse(quota["accepted_doc_ids"])
        self.assertTrue(all(item["code"] == "E_WORKER_QUOTA" for item in quota["unprocessed"]))

        latency_config = normalize_config(self.config)
        latency_config["budget"]["latency_budget_ms"] = 1
        latency = schedule_documents(self.docs[:2], latency_config)
        self.assertFalse(latency["accepted_doc_ids"])
        self.assertTrue(all(item["code"] == "E_LATENCY_BUDGET" for item in latency["unprocessed"]))


if __name__ == "__main__":
    unittest.main()
