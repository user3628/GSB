"""segtag 组件测试：确定性、数值稳定、错误码、批量一致性、缓存版本。"""
from __future__ import annotations

import math
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from segtag import (
    E_EMPTY,
    E_INVALID_TAG,
    E_NUMERIC,
    E_SMOOTHING,
    Corpus,
    Segmenter,
    SegTagError,
    estimate,
    safe_log,
    validate_and_segment,
)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRAIN = os.path.join(ROOT, "data", "train.txt")
INPUTS = [os.path.join(ROOT, "data", f"input{i}.txt") for i in (1, 2, 3)]


class TestTables(unittest.TestCase):
    def test_estimate_deterministic_byte_identical(self):
        corpus = Corpus.from_file(TRAIN)
        t1 = estimate(corpus, 0.1)
        t2 = estimate(corpus, 0.1)
        self.assertEqual(t1.to_canonical_json(), t2.to_canonical_json())
        self.assertEqual(t1.fingerprint, t2.fingerprint)

    def test_fingerprint_changes_with_smoothing(self):
        corpus = Corpus.from_file(TRAIN)
        self.assertNotEqual(estimate(corpus, 0.1).fingerprint, estimate(corpus, 0.5).fingerprint)

    def test_invalid_smoothing(self):
        corpus = Corpus.from_file(TRAIN)
        for bad in (0, -0.1, float("nan"), float("inf"), float("-inf"), "x", None, True):
            with self.assertRaises(SegTagError) as ctx:
                estimate(corpus, bad)
            self.assertEqual(ctx.exception.code, E_SMOOTHING, bad)

    def test_safe_log_rejects_non_positive(self):
        self.assertAlmostEqual(safe_log(math.e), 1.0)
        for bad in (0.0, -1.0, float("nan"), float("inf"), float("-inf")):
            with self.assertRaises(SegTagError) as ctx:
                safe_log(bad)
            self.assertEqual(ctx.exception.code, E_NUMERIC, bad)

    def test_unknown_token_fallback(self):
        corpus = Corpus.from_file(TRAIN)
        tables = estimate(corpus, 0.1, unknown_token="<OOV>")
        self.assertEqual(tables.unknown_token, "<OOV>")
        value = tables.emit_logprob("S-n", "龘")
        self.assertTrue(math.isfinite(value) and value < 0)
        trans = tables.trans_logprob("S-n", "B-zzz")
        self.assertTrue(math.isfinite(trans) and trans < 0)


class TestDecode(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.seg = Segmenter.train(TRAIN, smoothing=0.1)

    def test_decode_known_sentence(self):
        r = self.seg.tag_sentence("我爱北京。")
        self.assertTrue(r.ok, r.error)
        self.assertEqual([w.surface for w in r.words], ["我", "爱", "北京", "。"])
        self.assertEqual([(w.start, w.end) for w in r.words], [(0, 1), (1, 2), (2, 4), (4, 5)])
        self.assertEqual([w.pos for w in r.words], ["r", "v", "ns", "w"])
        self.assertTrue(math.isfinite(r.logprob) and r.logprob < 0)

    def test_decode_deterministic(self):
        a = self.seg.tag_sentence("他用Python写代码。")
        b = self.seg.tag_sentence("他用Python写代码。")
        self.assertEqual(a.logprob, b.logprob)
        self.assertEqual(a.tags, b.tags)
        self.assertEqual([w.surface for w in a.words], [w.surface for w in b.words])

    def test_empty_sentence(self):
        r = self.seg.tag_sentence("")
        self.assertFalse(r.ok)
        self.assertEqual(r.error.code, E_EMPTY)
        self.assertIsNone(r.words)
        self.assertIsNone(r.logprob)

    def test_oov_full_coverage(self):
        text = "这个表情很囧！"
        r = self.seg.tag_sentence(text)
        self.assertTrue(r.ok, r.error)
        pos = 0
        for w in r.words:
            self.assertEqual(w.start, pos)
            pos = w.end
        self.assertEqual(pos, len(text))

    def test_e_numeric_injection_reports_position(self):
        seg = Segmenter.train(TRAIN, smoothing=0.1)
        seg.inject_emission_for_test("爱", "S-v", float("-inf"))
        r = seg.tag_sentence("我爱北京。", sentence_index=3)
        self.assertFalse(r.ok)
        self.assertEqual(r.error.code, E_NUMERIC)
        self.assertEqual(r.error.sentence_index, 3)
        self.assertEqual(r.error.char_index, 1)
        self.assertIsNone(r.words)

    def test_e_numeric_nan_injection(self):
        seg = Segmenter.train(TRAIN, smoothing=0.1)
        seg.inject_emission_for_test("北", "B-ns", float("nan"))
        r = seg.tag_sentence("我爱北京。")
        self.assertFalse(r.ok)
        self.assertEqual(r.error.code, E_NUMERIC)
        self.assertEqual(r.error.char_index, 2)


class TestValidate(unittest.TestCase):
    POS = ("n", "v", "r", "w")

    def test_valid_sequence(self):
        words = validate_and_segment("我爱北京", ("S-r", "S-v", "B-n", "E-n"), self.POS)
        self.assertEqual([(w.start, w.end, w.surface, w.pos) for w in words],
                         [(0, 1, "我", "r"), (1, 2, "爱", "v"), (2, 4, "北京", "n")])

    def test_unknown_pos(self):
        with self.assertRaises(SegTagError) as ctx:
            validate_and_segment("好", ("S-zzz",), self.POS)
        self.assertEqual(ctx.exception.code, E_INVALID_TAG)

    def test_unclosed_word(self):
        with self.assertRaises(SegTagError) as ctx:
            validate_and_segment("北京", ("B-n", "M-n"), self.POS)
        self.assertEqual(ctx.exception.code, E_INVALID_TAG)

    def test_pos_switch_inside_word(self):
        with self.assertRaises(SegTagError) as ctx:
            validate_and_segment("北京", ("B-n", "E-v"), self.POS)
        self.assertEqual(ctx.exception.code, E_INVALID_TAG)

    def test_stray_e_and_s_inside_word(self):
        for tags in (("E-n",), ("B-n", "S-n"), ("M-n",), ("B-n", "B-n", "E-n")):
            with self.assertRaises(SegTagError) as ctx:
                validate_and_segment("字" * len(tags), tags, self.POS)
            self.assertEqual(ctx.exception.code, E_INVALID_TAG, tags)

    def test_length_mismatch_and_bad_format(self):
        with self.assertRaises(SegTagError):
            validate_and_segment("北京", ("S-n",), self.POS)
        with self.assertRaises(SegTagError):
            validate_and_segment("北", ("X-n",), self.POS)
        with self.assertRaises(SegTagError):
            validate_and_segment("北", ("n",), self.POS)


class TestBatch(unittest.TestCase):
    def test_batch_consistency(self):
        seg = Segmenter.train(TRAIN, smoothing=0.1)
        report = seg.tag_files(INPUTS)
        self.assertEqual(report.error_count, 0)
        self.assertEqual(report.ok_count, 10)
        self.assertLessEqual(report.crosscheck_diff, 1e-9)
        self.assertTrue(report.consistent)
        self.assertEqual(report.total_words, sum(len(r.words) for r in report.records))
        sequential = 0.0
        for rec in report.records:
            sequential += rec.logprob
        self.assertEqual(sequential, report.total_logprob)

    def test_batch_error_isolation(self):
        seg = Segmenter.train(TRAIN, smoothing=0.1)
        with tempfile.NamedTemporaryFile("w", suffix=".txt", encoding="utf-8", delete=False) as fh:
            fh.write("我爱北京。\n\n天气真好！\n")
            path = fh.name
        try:
            report = seg.tag_files([path])
        finally:
            os.unlink(path)
        self.assertEqual(report.ok_count, 2)
        self.assertEqual(report.error_count, 1)
        self.assertEqual(report.records[1].error.code, E_EMPTY)
        self.assertIsNone(report.records[1].words)


class TestCacheAndSmoothing(unittest.TestCase):
    def test_cache_hit_and_invalidation(self):
        seg = Segmenter.train(TRAIN, smoothing=0.1)
        first = seg.tag_sentence("我爱北京。")
        seg.tag_sentence("我爱北京。")
        info = seg.cache_info()
        self.assertEqual(info["hits"], 1)
        self.assertEqual(info["misses"], 1)
        self.assertEqual(info["size"], 1)
        old_fp = seg.fingerprint
        seg.set_smoothing(0.5)
        self.assertNotEqual(old_fp, seg.fingerprint)
        info = seg.cache_info()
        self.assertEqual(info["invalidations"], 1)
        self.assertEqual(info["size"], 0)
        rebuilt = seg.tag_sentence("我爱北京。")
        self.assertTrue(rebuilt.ok)
        self.assertNotEqual(rebuilt.logprob, first.logprob)
        self.assertEqual(seg.cache_info()["fingerprint"], seg.fingerprint)

    def test_invalid_smoothing_keeps_old_version(self):
        seg = Segmenter.train(TRAIN, smoothing=0.1)
        fp = seg.fingerprint
        with self.assertRaises(SegTagError) as ctx:
            seg.set_smoothing(-1)
        self.assertEqual(ctx.exception.code, E_SMOOTHING)
        self.assertEqual(seg.fingerprint, fp)
        self.assertEqual(seg.smoothing, 0.1)


if __name__ == "__main__":
    unittest.main()

