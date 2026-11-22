from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import numpy as np

from mtlab.alignment_stage import estimate_log_transition
from mtlab.checkpoints import CheckpointStore
from mtlab.config import ExperimentConfig
from mtlab.errors import MtlError
from mtlab.policy_stage import make_policy_decision
from mtlab.split_stage import run_split
from mtlab.tokenizer import has_content_tokens, normalize_text, pretokenize
from mtlab.translation import decode_record
from mtlab.util import sha256_json, write_json_atomic, write_jsonl_atomic


class TokenizerTests(unittest.TestCase):
    def test_normalization_and_offsets(self) -> None:
        text = "  ＡＢＣ　红色！！  "
        normalized = normalize_text(text)
        self.assertEqual(normalized, "abc 红色!!")
        tokens = pretokenize(normalized)
        self.assertEqual([token["text"] for token in tokens], ["abc", "红", "色", "!", "!"])
        self.assertEqual([token["lang"] for token in tokens], ["lat", "zh", "zh", "punct", "punct"])
        for token in tokens:
            self.assertEqual(
                normalized[int(token["start"]) : int(token["end"])], token["text"]
            )

    def test_punctuation_only_is_not_content(self) -> None:
        self.assertFalse(has_content_tokens(pretokenize(normalize_text("。。。!!"))))
        self.assertTrue(has_content_tokens(pretokenize(normalize_text("猫 cat"))))


class SplitStageTests(unittest.TestCase):
    def test_empty_diagnostic_and_bpe_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            corpus = root / "data" / "corpora" / "small.jsonl"
            rows = [
                {
                    "pair_id": "small-000001",
                    "source_lang": "en",
                    "target_lang": "zh",
                    "lang_pair": "en-zh",
                    "source": "Red red cat.",
                    "target": "红红猫。",
                    "domain": "daily",
                    "noise_flags": [],
                },
                {
                    "pair_id": "small-000002",
                    "source_lang": "en",
                    "target_lang": "zh",
                    "lang_pair": "en-zh",
                    "source": "...",
                    "target": "",
                    "domain": "daily",
                    "noise_flags": ["punctuation_only", "zero_length_ratio"],
                },
            ]
            write_jsonl_atomic(corpus, rows)
            manifest = {"pair_count": 2, "corpus_file": str(corpus)}
            config = ExperimentConfig(bpe_merges=4, bpe_min_count=1)
            out = root / "out"
            summary = run_split(root, "small", config, manifest, out)
            self.assertEqual(summary["valid_pair_count"], 1)
            self.assertEqual(summary["diagnostic_count"], 1)
            diagnostics = [
                json.loads(line)
                for line in (out / "diagnostics.jsonl").read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(diagnostics[0]["code"], "E_EMPTY_SENTENCE")
            self.assertEqual(diagnostics[0]["pair_id"], "small-000002")
            records = [
                json.loads(line)
                for line in (out / "tokens.jsonl").read_text(encoding="utf-8").splitlines()
            ]
            target_tokens = records[0]["target_tokens"]
            merged = [token for token in target_tokens if len(token["merge_source"]) > 1]
            self.assertTrue(merged)
            self.assertTrue(merged[0]["merge_rules"])
            for token in records[0]["source_tokens"] + target_tokens:
                self.assertIn("start", token)
                self.assertIn("end", token)
                self.assertIn("lang", token)


class AlignmentTests(unittest.TestCase):
    def _records(self):
        return [
            {
                "pair_id": "p1",
                "source_tokens": [{"text": "a"}],
                "target_tokens": [{"text": "一"}],
            }
        ]

    def test_zero_smoothing_is_numeric_error(self) -> None:
        counts = np.ones((2, 2), dtype=np.float64)
        sequences = [(np.array([0, 1]), np.array([0, 1]))]
        with self.assertRaises(MtlError) as context:
            estimate_log_transition(
                counts,
                smoothing=0.0,
                iterations=1,
                sequences=sequences,
                lang_pair="en-zh",
                records=self._records(),
                source_vocab=["a", "b"],
                target_vocab=["一", "二"],
            )
        self.assertEqual(context.exception.code, "E_NUMERIC")
        self.assertEqual(context.exception.details["lang_pair"], "en-zh")
        self.assertTrue(context.exception.details["positions"])

    def test_negative_count_is_numeric_error(self) -> None:
        counts = np.ones((2, 2), dtype=np.float64)
        counts[0, 0] = -1.0
        sequences = [(np.array([0, 1]), np.array([0, 1]))]
        with self.assertRaises(MtlError) as context:
            estimate_log_transition(
                counts,
                smoothing=0.5,
                iterations=1,
                sequences=sequences,
                lang_pair="en-zh",
                records=self._records(),
                source_vocab=["a", "b"],
                target_vocab=["一", "二"],
            )
        self.assertEqual(context.exception.code, "E_NUMERIC")


class PolicyTests(unittest.TestCase):
    def test_scale_choices_and_fingerprint(self) -> None:
        manifests = {
            "small": {
                "pair_count": 200,
                "avg_token_length": 6.1,
                "avg_source_tokens": 6.0,
                "avg_target_tokens": 6.2,
                "source_vocab_size": 50,
                "target_vocab_size": 50,
                "vocab_size_union": 70,
                "total_token_upper_bound": 2469,
            },
            "medium": {
                "pair_count": 2000,
                "avg_token_length": 6.1,
                "avg_source_tokens": 6.0,
                "avg_target_tokens": 6.2,
                "source_vocab_size": 80,
                "target_vocab_size": 80,
                "vocab_size_union": 120,
                "total_token_upper_bound": 24324,
            },
            "large": {
                "pair_count": 12000,
                "avg_token_length": 6.1,
                "avg_source_tokens": 6.0,
                "avg_target_tokens": 6.2,
                "source_vocab_size": 110,
                "target_vocab_size": 110,
                "vocab_size_union": 170,
                "total_token_upper_bound": 145956,
            },
        }
        config = ExperimentConfig()
        decisions = {
            tier: make_policy_decision(tier, manifest, config)
            for tier, manifest in manifests.items()
        }
        self.assertEqual(decisions["small"]["selected"]["alignment_mode"], "subword_approx")
        self.assertLessEqual(decisions["small"]["selected"]["phrase_limit"], 256)
        self.assertLessEqual(decisions["small"]["selected"]["beam_width"], 4)
        self.assertEqual(decisions["medium"]["selected"]["alignment_mode"], "word_exact")
        self.assertEqual(decisions["large"]["selected"]["alignment_mode"], "word_exact")
        self.assertEqual(
            decisions["large"]["decision_fingerprint"],
            make_policy_decision("large", manifests["large"], config)["decision_fingerprint"],
        )


class DecodeFailureTests(unittest.TestCase):
    def test_zero_beam_width(self) -> None:
        record = {
            "pair_id": "p1",
            "source_lang": "en",
            "target_lang": "zh",
            "lang_pair": "en-zh",
            "domain": "daily",
            "source_tokens": [{"text": "cat"}],
            "target_tokens": [{"text": "猫"}],
        }
        with self.assertRaises(MtlError) as context:
            decode_record(record, {}, beam_width=0)
        self.assertEqual(context.exception.code, "E_DECODE_FAILED")
        self.assertEqual(context.exception.details["pair_id"], "p1")
        self.assertEqual(context.exception.details["step_index"], 0)


class CheckpointTests(unittest.TestCase):
    @staticmethod
    def _stage(store: CheckpointStore, tier: str, stage: str, descriptor: dict):
        staged = store.make_staging_dir(tier, stage)
        write_json_atomic(
            staged / "_meta.json",
            {
                "tier": tier,
                "stage": stage,
                "input_fingerprint": sha256_json(descriptor),
            },
        )
        write_json_atomic(staged / "payload.json", {"ok": True})
        return staged

    def test_stage_jump_and_stale_sequence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = CheckpointStore(Path(tmp))
            descriptor = {"stage": "align"}
            staged = self._stage(store, "small", "align", descriptor)
            with self.assertRaises(MtlError) as jump:
                store.publish_stage(
                    tier="small",
                    stage="align",
                    staged_dir=staged,
                    input_descriptor=descriptor,
                    decision_fingerprint="d",
                )
            self.assertEqual(jump.exception.code, "E_INVALID_STAGE")
            store.discard_staging_dir(staged)

            split_descriptor = {"stage": "split"}
            staged = self._stage(store, "small", "split", split_descriptor)
            record = store.publish_stage(
                tier="small",
                stage="split",
                staged_dir=staged,
                input_descriptor=split_descriptor,
                decision_fingerprint="d",
            )
            self.assertEqual(record["sequence"], 1)

            staged = self._stage(store, "small", "split", split_descriptor)
            with self.assertRaises(MtlError) as duplicate:
                store.publish_stage(
                    tier="small",
                    stage="split",
                    staged_dir=staged,
                    input_descriptor=split_descriptor,
                    decision_fingerprint="d",
                )
            self.assertEqual(duplicate.exception.code, "E_STALE_CHECKPOINT")
            store.discard_staging_dir(staged)

            align_descriptor = {"stage": "align", "v": 1}
            staged = self._stage(store, "small", "align", align_descriptor)
            with self.assertRaises(MtlError) as stale:
                store.publish_stage(
                    tier="small",
                    stage="align",
                    staged_dir=staged,
                    input_descriptor=align_descriptor,
                    decision_fingerprint="d",
                    expected_sequence=9,
                )
            self.assertEqual(stale.exception.code, "E_STALE_CHECKPOINT")
            store.discard_staging_dir(staged)


if __name__ == "__main__":
    unittest.main()

