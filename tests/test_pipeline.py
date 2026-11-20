from __future__ import annotations

import json
import os
import tempfile
import unittest

from crwbench.checkpoints import CheckpointStore, STAGES
from crwbench.config import load_config
from crwbench.errors import (
    E_INCREMENTAL_MISMATCH,
    E_INVALID_STAGE,
    E_STALE_CHECKPOINT,
    E_STALE_SNAPSHOT,
    WorkbenchError,
)
from crwbench.features import UNKNOWN_KEYWORD_TABLE
from crwbench.generator import generate_dataset
from crwbench.pipeline import Pipeline, load_shards, verify_workdir_outputs
from crwbench.strategy import (
    STRATEGY_EXACT,
    STRATEGY_KEYWORD,
    STRATEGY_TIERED,
)
from crwbench.util import canonical_dumps


CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "default_config.yaml")


class WorkbenchTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = self.tmp.name
        self.data_dir = os.path.join(self.root, "data")
        self.workdir = os.path.join(self.root, "work")
        generate_dataset(
            seed=20261121,
            sizes={"small": 20, "medium": 90, "large": 200},
            out_dir=self.data_dir,
            shard_count=3,
        )
        self.shards = sorted(
            os.path.join(self.data_dir, name)
            for name in os.listdir(self.data_dir)
            if name.endswith(".jsonl")
        )
        self.config = load_config(CONFIG_PATH)

    def tearDown(self):
        self.tmp.cleanup()

    def run_pipeline(self, workdir=None, force=False, config=None):
        pipe = Pipeline(config or self.config, workdir or self.workdir)
        return pipe, pipe.run(self.shards, force=force)


class TestNormalize(WorkbenchTestBase):
    def test_nfkc_width_and_offsets(self):
        from crwbench.normalize import normalize_text, split_sentences

        text = "　Ｆｕｌｌ－ｗｉｄｔｈ　文本。Second sentence!"
        normalized = normalize_text(text)
        self.assertNotIn("　", normalized)
        self.assertIn("Full-width", normalized)
        segments = split_sentences(normalized)
        self.assertGreaterEqual(len(segments), 2)
        for seg in segments:
            self.assertEqual(normalized[seg["start"]:seg["end"]], seg["text"])
            self.assertIn(seg["lang_norm"], {"zh", "en", "mixed", "neutral"})

    def test_empty_text_diagnostic_retained(self):
        pipe, report = self.run_pipeline()
        normalized = pipe.store.load_artifact("normalize")
        empty = [n for n in normalized if n["status"] == "empty"]
        self.assertTrue(empty)
        for record in empty:
            self.assertEqual(record["diagnostics"][0]["code"], "E_EMPTY_TEXT")
            self.assertEqual(record["diagnostics"][0]["clause_id"], record["clause_id"])

    def test_byte_identical_normalization(self):
        from crwbench.normalize import run

        clauses, _, _ = load_shards(self.shards)
        first = canonical_dumps(run(clauses))
        second = canonical_dumps(run(clauses))
        self.assertEqual(first, second)


class TestFeatures(WorkbenchTestBase):
    def test_hits_carry_offsets_and_sources(self):
        pipe, _ = self.run_pipeline()
        features = pipe.store.load_artifact("features")
        with_hits = [f for f in features if f["hit_count"] > 0]
        self.assertTrue(with_hits)
        normalized = {n["clause_id"]: n for n in pipe.store.load_artifact("normalize")}
        for feature in with_hits[:5]:
            text = normalized[feature["clause_id"]]["normalized_text"]
            for hit in feature["hits"]:
                self.assertEqual(text[hit["start"]:hit["end"]], hit["keyword"])
                self.assertIn(hit["polarity"], {"positive", "negative"})

    def test_negation_flips_polarity(self):
        from crwbench.features import extract_clause
        from crwbench.normalize import normalize_clause

        clause = {
            "clause_id": "X1",
            "contract_id": "K",
            "template_id": "t",
            "lang": "zh",
            "text": "甲方不承担责任，且无须支付违约金。",
        }
        feature = extract_clause(normalize_clause(clause), self.config)
        negated = [h for h in feature["hits"] if h["negated"]]
        self.assertTrue(negated)
        self.assertTrue(all(h["polarity"] == "negative" for h in negated))

    def test_unknown_keyword_table_marker(self):
        from crwbench.features import extract_clause
        from crwbench.normalize import normalize_clause

        raw = json.loads(canonical_dumps(self.config.raw).decode("utf-8"))
        for cat in raw["categories"]:
            if cat["key"] == "termination":
                cat["keywords"] = {}
        from crwbench.config import Config

        cfg = Config(raw)
        clause = {
            "clause_id": "X2",
            "contract_id": "K",
            "template_id": "t",
            "lang": "zh",
            "text": "任何一方违约应支付违约金。",
        }
        feature = extract_clause(normalize_clause(clause), cfg)
        markers = {u["category"]: u["marker"] for u in feature["unknown_categories"]}
        self.assertEqual(markers["termination"], UNKNOWN_KEYWORD_TABLE)


class TestScoring(WorkbenchTestBase):
    def test_levels_in_range_and_monotone(self):
        pipe, _ = self.run_pipeline()
        scored = pipe.store.load_artifact("scoring")
        for record in scored:
            self.assertIn(record["level"], [0, 1, 2, 3])
            self.assertTrue(record["level_basis"])
        # Direction invariant across clauses.
        ordered = sorted(scored, key=lambda r: r["clause_score"])
        levels = [r["level"] for r in ordered]
        self.assertEqual(levels, sorted(levels))

    def test_default_low_risk_records_basis(self):
        pipe, _ = self.run_pipeline()
        scored = pipe.store.load_artifact("scoring")
        low = [r for r in scored if r["level"] == 0 and r["status"] == "ok"]
        self.assertTrue(low)
        self.assertTrue(
            all(r["level_basis"] == "all_category_scores_below_threshold" for r in low)
        )

    def test_five_categories_always_present(self):
        pipe, _ = self.run_pipeline()
        scored = pipe.store.load_artifact("scoring")
        for record in scored:
            self.assertEqual(len(record["category_scores"]), 5)


class TestStrategy(WorkbenchTestBase):
    def _strategy_for_size(self, sizes, workdir):
        data_dir = os.path.join(self.root, workdir + "_data")
        result = generate_dataset(99, sizes, data_dir, shard_count=3)
        pipe = Pipeline(self.config, os.path.join(self.root, workdir))
        report = pipe.run(result["paths"], force=True)
        final = pipe.store.load_artifact("summary")
        return final, report

    def test_three_strategies_across_scales(self):
        small, _ = self._strategy_for_size({"small": 24}, "wd_small")
        medium, _ = self._strategy_for_size({"medium": 120}, "wd_medium")
        large, _ = self._strategy_for_size({"large": 320}, "wd_large")
        self.assertEqual(small["summary"]["strategy"], STRATEGY_EXACT)
        self.assertEqual(medium["summary"]["strategy"], STRATEGY_KEYWORD)
        self.assertEqual(large["summary"]["strategy"], STRATEGY_TIERED)

    def test_budget_and_quota_invariants(self):
        final, _ = self._strategy_for_size({"large": 320}, "wd_budget")
        decision = final["strategy_decision"]
        self.assertLessEqual(
            decision["plan_judgment_counts"][decision["chosen_strategy"]],
            decision["global_budget"],
        )
        # Rejected strategies carry explicit reasons.
        for name, reason in decision["rejected_reasons"].items():
            self.assertIn(name, {STRATEGY_EXACT, STRATEGY_KEYWORD, STRATEGY_TIERED})
            self.assertTrue(reason)
        # Committed clauses keep exact levels; uncommitted default to 0.
        scored = {s["clause_id"]: s for s in Pipeline(self.config, os.path.join(self.root, "wd_budget")).store.load_artifact("scoring")}
        for row in final["level_list"]:
            if row["committed"]:
                self.assertEqual(row["level"], scored[row["clause_id"]]["level"])
            else:
                self.assertEqual(row["level"], 0)


class TestIncremental(WorkbenchTestBase):
    def test_mismatch_raises_with_diff_summary(self):
        dir_a = os.path.join(self.root, "a")
        dir_b = os.path.join(self.root, "b")
        Pipeline(self.config, dir_a).run(self.shards, force=True)
        other = generate_dataset(7, {"small": 12}, os.path.join(self.root, "other"), 3)
        Pipeline(self.config, dir_b).run(other["paths"], force=True)
        with self.assertRaises(WorkbenchError) as ctx:
            verify_workdir_outputs(dir_a, dir_b)
        self.assertEqual(ctx.exception.code, E_INCREMENTAL_MISMATCH)
        self.assertTrue(ctx.exception.details["differences"])

    def test_config_change_invalidates_downstream_and_matches_clean_run(self):
        pipe, _ = self.run_pipeline(force=True)
        raw = json.loads(canonical_dumps(self.config.raw).decode("utf-8"))
        raw["config_version"] = "vX"
        raw["degree"]["terms"]["zh"]["严重"] = 2.5
        raw["scoring"]["level_thresholds"] = [0.4, 1.2, 1.8]
        from crwbench.config import Config

        changed = Config(raw)
        report = Pipeline(changed, self.workdir).run(self.shards)
        self.assertEqual(report["reused_stages"], ["normalize"])
        self.assertEqual(
            report["recomputed_stages"], ["features", "scoring", "summary"]
        )
        clean_dir = os.path.join(self.root, "clean")
        Pipeline(changed, clean_dir).run(self.shards, force=True)
        verify_workdir_outputs(clean_dir, self.workdir)

    def test_threshold_only_change_reuses_features(self):
        self.run_pipeline(force=True)
        raw = json.loads(canonical_dumps(self.config.raw).decode("utf-8"))
        raw["config_version"] = "vY"
        raw["scoring"]["level_thresholds"] = [0.6, 1.4, 2.0]
        from crwbench.config import Config

        report = Pipeline(Config(raw), self.workdir).run(self.shards)
        self.assertEqual(report["reused_stages"], ["normalize", "features"])
        self.assertEqual(report["recomputed_stages"], ["scoring", "summary"])


class TestCheckpointsAndErrors(WorkbenchTestBase):
    def test_checkpoint_sequence_and_fields(self):
        pipe, _ = self.run_pipeline(force=True)
        records = pipe.store.load_records()
        self.assertEqual([r["stage"] for r in records], STAGES)
        self.assertEqual([r["seq"] for r in records], [1, 2, 3, 4])
        for record in records:
            for field in (
                "input_digest",
                "config_version",
                "config_fingerprint",
                "artifact_digest",
                "record_digest",
            ):
                self.assertTrue(record[field])

    def test_corrupt_record_is_ignored(self):
        pipe, _ = self.run_pipeline(force=True)
        path = os.path.join(self.workdir, "checkpoints.jsonl")
        with open(path, "ab") as fh:
            fh.write(b"{corrupt-half-written-record\n")
        records = pipe.store.load_records()
        self.assertEqual(len(records), 4)
        report = pipe.run(self.shards)
        self.assertEqual(report["reused_stages"], STAGES)

    def test_stale_duplicate_and_invalid_stage(self):
        pipe, _ = self.run_pipeline(force=True)
        with self.assertRaises(WorkbenchError) as ctx:
            pipe.store.publish(
                stage="normalize",
                input_digest="x",
                config_fingerprint="y",
                config_version="v",
                artifact_path="dup.json",
                artifact={},
                expected_seq=4,
            )
        self.assertEqual(ctx.exception.code, E_STALE_CHECKPOINT)
        # Roll back to stage 1, then attempt to publish stage 3 without stage 2.
        pipe.store.rollback_to(0)
        with self.assertRaises(WorkbenchError) as ctx2:
            pipe.store.publish(
                stage="scoring",
                input_digest="x",
                config_fingerprint="y",
                config_version="v",
                artifact_path="jump.json",
                artifact={},
                expected_seq=1,
            )
        self.assertEqual(ctx2.exception.code, E_INVALID_STAGE)

    def test_snapshot_replacement_detected(self):
        pipe, input_digest = self.run_pipeline(force=True)
        # Tamper an artifact after the checkpoint digest was recorded.
        artifact = os.path.join(self.workdir, "stage3_scoring.json")
        with open(artifact, "ab") as fh:
            fh.write(b' {"tampered": true}\n')
        with self.assertRaises(WorkbenchError) as ctx:
            pipe.store.verify_chain(
                self.config.fingerprint,
                pipe.store.load_records()[0]["input_digest"],
            )
        self.assertEqual(ctx.exception.code, E_STALE_SNAPSHOT)
        # Original other artifacts remain in place.
        self.assertTrue(os.path.exists(os.path.join(self.workdir, "stage1_normalized.json")))


class TestStopResume(WorkbenchTestBase):
    def test_stop_at_unit_boundary_and_resume_identical(self):
        baseline_dir = os.path.join(self.root, "baseline")
        Pipeline(self.config, baseline_dir).run(self.shards, force=True)

        pipe = Pipeline(self.config, self.workdir)
        pipe.run(self.shards, force=True)
        pipe.store.rollback_to(0)
        pipe.request_stop()
        stopped = pipe.run(self.shards)
        self.assertTrue(stopped["stopped"])
        self.assertEqual(stopped["completed_stages"], ["normalize"])
        resumed = pipe.resume(self.shards)
        self.assertFalse(resumed["stopped"])
        verify_workdir_outputs(baseline_dir, self.workdir)


if __name__ == "__main__":
    unittest.main(verbosity=2)
