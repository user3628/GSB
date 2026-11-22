from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from halu_pipeline.aggregate import aggregate_consistency_from_samples  # noqa: E402
from halu_pipeline.canonical import (  # noqa: E402
    GracefulStop,
    PipelineError,
    canonical_bytes,
    canonical_json,
)
from halu_pipeline.checkpoints import CheckpointStore  # noqa: E402
from halu_pipeline.config import Config  # noqa: E402
from halu_pipeline.http_server import PipelineHTTPServer  # noqa: E402
from halu_pipeline.normalize import (  # noqa: E402
    normalize_text,
    split_sentences_with_offsets,
    tokenize_with_offsets,
    validate_and_normalize_record,
)
from halu_pipeline.pipeline import Pipeline  # noqa: E402
from halu_pipeline.query import QueryService  # noqa: E402
from halu_pipeline.scoring import judge_record, judge_records  # noqa: E402


DEMO_DIR = ROOT / "data" / "demo"


def demo_manifest() -> dict:
    return json.loads((DEMO_DIR / "manifest.json").read_text(encoding="utf-8"))


def demo_shards() -> list[Path]:
    return sorted(DEMO_DIR.glob("shard-*.jsonl"))


def demo_config(*, workers: int = 1, threshold: float = 0.82) -> dict:
    return {
        "declared_strata": demo_manifest()["declared_strata"],
        "consistency_min": 0.80,
        "latency_budget_ms": 100.0,
        "parallel_workers": workers,
        "sentence_support_threshold": threshold,
    }


def make_pipeline(root: Path, *, workers: int = 1, threshold: float = 0.82) -> Pipeline:
    pipeline = Pipeline(root)
    pipeline.initialize(demo_shards(), demo_config(workers=workers, threshold=threshold))
    return pipeline


class NormalizationTests(unittest.TestCase):
    def test_nfkc_whitespace_and_offsets(self) -> None:
        self.assertEqual(normalize_text("ＴＨＥ   ＳＵＮ\n 是  恒星。"), "the sun 是 恒星。")
        text = normalize_text("ＡＢＣ   中文")
        tokens = tokenize_with_offsets(text)
        self.assertEqual([item["text"] for item in tokens], ["abc", "中", "文"])
        for token in tokens:
            self.assertEqual(text[token["start"] : token["end"]], token["text"])
        sentences = split_sentences_with_offsets(text)
        for sentence in sentences:
            for token in sentence["tokens"]:
                self.assertEqual(text[token["start"] : token["end"]], token["text"])

    def test_empty_evidence_error_and_determinism(self) -> None:
        record = {
            "sample_id": "empty-1",
            "prompt": "p",
            "model_output": "answer",
            "reference_answer": "answer",
            "retrieved_evidence": [],
            "lang": "en",
            "source": "demo",
        }
        first = validate_and_normalize_record(record)
        second = validate_and_normalize_record(record)
        self.assertEqual(canonical_bytes(first), canonical_bytes(second))
        self.assertEqual(first["status"], "error")
        self.assertEqual(first["errors"][0]["code"], "E_EMPTY_FIELD")
        self.assertEqual(first["errors"][0]["sample_id"], "empty-1")

    def test_empty_output_error(self) -> None:
        record = {
            "sample_id": "empty-2",
            "prompt": "p",
            "model_output": "   ",
            "reference_answer": "x",
            "retrieved_evidence": ["evidence"],
            "lang": "en",
            "source": "demo",
        }
        normalized = validate_and_normalize_record(record)
        self.assertEqual(normalized["status"], "error")
        self.assertEqual(normalized["errors"][0]["field"], "model_output")

    def test_tie_break_chooses_first_evidence_sentence(self) -> None:
        record = {
            "sample_id": "tie-1",
            "prompt": "p",
            "model_output": "the same fact.",
            "reference_answer": "same",
            "retrieved_evidence": ["The same fact.", "the same fact."],
            "lang": "en",
            "source": "demo",
        }
        stage1 = validate_and_normalize_record(record)
        config = Config.with_overrides({})
        judged = judge_record(stage1, config)
        best = judged["sentence_support"][0]["best_evidence"]
        self.assertEqual(best["evidence_index"], 0)
        self.assertEqual(best["sentence_index"], 0)


class FullPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tempdir = tempfile.TemporaryDirectory(prefix="halu-full-")
        cls.root = Path(cls.tempdir.name) / "run"
        cls.pipeline = make_pipeline(cls.root)
        cls.run_result = cls.pipeline.run()
        cls.stage1 = cls.pipeline.load_stage_payload(1)
        cls.stage2 = cls.pipeline.load_stage_payload(2)
        cls.stage3 = cls.pipeline.load_stage_payload(3)
        cls.stage4 = cls.pipeline.load_stage_payload(4)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.tempdir.cleanup()

    def test_full_run_and_stage_records_sorted(self) -> None:
        self.assertEqual(self.run_result["verification"], "PASS")
        self.assertEqual(self.run_result["recomputed_stages"], [1, 2, 3, 4])
        ids1 = [item["sample_id"] for item in self.stage1["records"]]
        ids2 = [item["sample_id"] for item in self.stage2["records"]]
        self.assertEqual(ids1, sorted(ids1))
        self.assertEqual(ids2, sorted(ids2))
        self.assertEqual(len(ids1), 20)

    def test_empty_evidence_and_output_are_explicit_errors(self) -> None:
        errors = {
            item["sample_id"]: item["errors"]
            for item in self.stage1["records"]
            if item["status"] == "error"
        }
        self.assertEqual(set(errors), {"zh-003", "zh-005", "zh-008"})
        for error_list in errors.values():
            self.assertEqual(error_list[0]["code"], "E_EMPTY_FIELD")

    def test_hallucination_examples_and_rule_features(self) -> None:
        records = {item["sample_id"]: item for item in self.stage2["records"]}
        self.assertFalse(records["en-001"]["hallucination"])
        self.assertTrue(records["en-002"]["hallucination"])
        self.assertEqual(
            records["en-002"]["sentence_support"][0]["features"]["rule_mismatches"],
            ["NUMERIC_MISMATCH"],
        )
        self.assertTrue(records["mix-002"]["hallucination"])
        self.assertTrue(records["zh-007"]["hallucination"])
        self.assertEqual(
            records["zh-007"]["sentence_support"][0]["features"]["rule_mismatches"],
            ["NEGATION_POLARITY_MISMATCH"],
        )
        self.assertIsNone(records["zh-005"]["hallucination"])
        self.assertEqual(records["zh-005"]["status"], "error")

    def test_stage2_repeat_is_byte_identical(self) -> None:
        config = self.pipeline.load_config()
        first = judge_records(self.stage1, config)
        second = judge_records(self.stage1, config)
        self.assertEqual(canonical_bytes(first), canonical_bytes(second))

    def test_strata_fixed_point_and_empty_layers(self) -> None:
        self.assertEqual(len(self.stage3["strata"]), 8)
        empty = [
            item for item in self.stage3["strata"] if item["sample_count"] == 0
        ]
        self.assertEqual(
            {(item["source"], item["lang"]) for item in empty},
            {("kb_zh", "ja"), ("shadow_source", "en")},
        )
        self.assertEqual(self.stage3["totals"]["sample_count"], 20)
        for item in self.stage3["strata"]:
            self.assertRegex(item["avg_consistency"], r"^\d+\.\d{6}$")
            self.assertRegex(item["hallucination_rate"], r"^\d+\.\d{6}$")

    def test_summary_matches_sorted_sample_accumulation(self) -> None:
        records = self.stage2["records"]
        for stratum in self.stage3["strata"]:
            subset = [
                item
                for item in records
                if item["source"] == stratum["source"] and item["lang"] == stratum["lang"]
            ]
            expected = aggregate_consistency_from_samples(subset)
            for key in (
                "sample_count",
                "judged_count",
                "hallucination_count",
                "hallucination_rate_fp",
                "avg_consistency_fp",
                "pending_review_count",
            ):
                self.assertEqual(stratum[key], expected[key], (stratum["source"], stratum["lang"], key))

    def test_annotation_validation_lists_both_error_types(self) -> None:
        overall = self.stage4["overall_confusion"]
        self.assertGreaterEqual(overall["false_positive"], 1)
        self.assertGreaterEqual(overall["false_negative"], 1)
        self.assertTrue(overall["false_positive_samples"])
        self.assertTrue(overall["false_negative_samples"])
        for key in ("false_positive_samples", "false_negative_samples"):
            for item in overall[key]:
                self.assertIn("consistency_score", item["basis"])
                self.assertIn("sentence_support", item["basis"])

    def test_review_flags_missing_annotations_and_low_sources(self) -> None:
        reviews = {item["source"]: item for item in self.stage4["source_reviews"]}
        faq = reviews["faq_zh"]
        self.assertTrue(faq["review_required"])
        self.assertIn("LOW_ANNOTATION_COVERAGE", faq["review_reasons"])
        self.assertIn("zh-003", faq["missing_annotation_sample_ids"])
        self.assertIn("zh-005", faq["missing_annotation_sample_ids"])
        self.assertTrue(reviews["kb_zh"]["review_required"])
        self.assertIn("LOW_ANNOTATION_CONSISTENCY", reviews["kb_zh"]["review_reasons"])
        review_ids = {item["sample_id"] for item in self.stage4["samples_requiring_review"]}
        self.assertIn("zh-003", review_ids)
        self.assertIn("mix-005", review_ids)

    def test_query_cache_byte_equality_and_latency_error(self) -> None:
        service = QueryService(self.root)
        service.clear_cache()
        first = service.query_sample("zh-002", budget_ms=1000)
        cached = service.query_sample("zh-002", budget_ms=1000)
        service.clear_cache()
        recomputed = service.query_sample("zh-002", budget_ms=1000)
        self.assertEqual(first["code"], "OK")
        self.assertEqual(canonical_bytes(first), canonical_bytes(cached))
        self.assertEqual(canonical_bytes(first), canonical_bytes(recomputed))
        self.assertIn("stage_fingerprint", first)
        self.assertIn("content_fingerprint", first)
        timeout = service.query_sample("zh-002", budget_ms=0, query_id="q-latency")
        self.assertEqual(timeout["code"], "E_LATENCY_BUDGET")
        self.assertEqual(timeout["query_id"], "q-latency")
        self.assertIn("elapsed_ms", timeout)
        self.assertIn("elapsed_us", timeout)

    def test_checkpoint_sequence_and_stale_rejection(self) -> None:
        checkpoints = self.pipeline.checkpoints.list()
        sequences = [item["sequence"] for item in checkpoints]
        self.assertEqual(sequences, [1, 2, 3, 4])
        with self.assertRaises(PipelineError) as caught:
            self.pipeline.checkpoints.publish(
                stage="judge",
                input_fingerprint="in",
                config_version="cfg",
                artifact_digest="digest",
                sequence=1,
            )
        self.assertEqual(caught.exception.code, "E_STALE_CHECKPOINT")


class IncrementalAndParallelTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory(prefix="halu-inc-")
        self.base = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    @staticmethod
    def _stage_bytes(root: Path) -> dict[int, bytes]:
        return {
            stage: (root / "stages" / f"stage{stage}.json").read_bytes()
            for stage in range(1, 5)
        }

    def test_incremental_threshold_edit_matches_fresh_run(self) -> None:
        incremental_root = self.base / "incremental"
        pipeline = make_pipeline(incremental_root)
        pipeline.run()
        original_stage1 = (incremental_root / "stages" / "stage1.json").read_bytes()
        update = pipeline.update_config(assignments=["sentence_support_threshold=0.84"])
        self.assertEqual(update["earliest_affected_stage"], 2)
        result = pipeline.run()
        self.assertEqual(result["reused_stages"], [1])
        self.assertEqual(result["recomputed_stages"], [2, 3, 4])
        self.assertEqual(
            original_stage1, (incremental_root / "stages" / "stage1.json").read_bytes()
        )

        fresh_root = self.base / "fresh"
        fresh = make_pipeline(fresh_root, threshold=0.84)
        fresh.run()
        self.assertEqual(self._stage_bytes(incremental_root), self._stage_bytes(fresh_root))

    def test_consistency_min_edit_reuses_first_three_stages(self) -> None:
        incremental_root = self.base / "incremental"
        pipeline = make_pipeline(incremental_root)
        pipeline.run()
        before = self._stage_bytes(incremental_root)
        update = pipeline.update_config(assignments=["consistency_min=0.9"])
        self.assertEqual(update["earliest_affected_stage"], 4)
        result = pipeline.run()
        self.assertEqual(result["reused_stages"], [1, 2, 3])
        self.assertEqual(result["recomputed_stages"], [4])
        after = self._stage_bytes(incremental_root)
        self.assertEqual(before[1], after[1])
        self.assertEqual(before[2], after[2])
        self.assertEqual(before[3], after[3])

        config = pipeline.load_config().to_dict()
        fresh_root = self.base / "fresh"
        fresh = Pipeline(fresh_root)
        fresh.initialize(demo_shards(), config)
        fresh.run()
        self.assertEqual(after[4], (fresh_root / "stages" / "stage4.json").read_bytes())

    def test_parallel_execution_matches_sequential_bytes(self) -> None:
        sequential_root = self.base / "sequential"
        parallel_root = self.base / "parallel"
        sequential = make_pipeline(sequential_root, workers=1)
        parallel = make_pipeline(parallel_root, workers=4)
        self.assertEqual(sequential.run()["verification"], "PASS")
        self.assertEqual(parallel.run()["verification"], "PASS")
        self.assertEqual(
            self._stage_bytes(sequential_root), self._stage_bytes(parallel_root)
        )

    def test_incremental_mismatch_reports_difference_samples(self) -> None:
        root = self.base / "mismatch"
        pipeline = make_pipeline(root)
        pipeline.run()
        pipeline.update_config(assignments=["sentence_support_threshold=0.84"])
        original_compute_all = pipeline._compute_all

        def corrupted_compute_all(*args, **kwargs):
            payloads = original_compute_all(*args, **kwargs)
            record = next(
                item for item in payloads[2]["records"] if item["sample_id"] == "zh-001"
            )
            record["hallucination"] = not record["hallucination"]
            return payloads

        pipeline._compute_all = corrupted_compute_all  # type: ignore[method-assign]
        with self.assertRaises(PipelineError) as caught:
            pipeline.run()
        self.assertEqual(caught.exception.code, "E_INCREMENTAL_MISMATCH")
        self.assertEqual(caught.exception.details["different_stages"], [2])
        self.assertIn("zh-001", caught.exception.details["different_sample_ids"])

    def test_status_and_query_expose_invalidation_reason(self) -> None:
        root = self.base / "invalidation"
        pipeline = make_pipeline(root)
        pipeline.run()
        pipeline.update_config(assignments=["sentence_support_threshold=0.84"])
        status = pipeline.status()
        self.assertEqual(status["earliest_invalidated_stage"], 2)
        self.assertEqual(status["invalidation_reason"], "STAGE_2_CONFIG_CHANGED")
        result = QueryService(root).query_sample("zh-002", budget_ms=1000)
        self.assertTrue(result["invalidation"]["stale"])
        self.assertEqual(result["invalidation"]["earliest_affected_stage"], 2)
        self.assertEqual(result["invalidation"]["reason"], "STAGE_2_CONFIG_CHANGED")


class StopResumeTests(unittest.TestCase):
    def test_stop_between_stages_and_resume_equals_fresh(self) -> None:
        with tempfile.TemporaryDirectory(prefix="halu-stop-") as tmp:
            base = Path(tmp)
            stopped_root = base / "stopped"
            fresh_root = base / "fresh"
            pipeline = make_pipeline(stopped_root)
            pipeline.run(until_stage=1)
            pipeline.request_stop()
            with self.assertRaises(GracefulStop) as caught:
                pipeline.run()
            self.assertEqual(caught.exception.code, "E_STOPPED")
            pipeline.resume()
            resumed = pipeline.run()
            self.assertEqual(resumed["reused_stages"], [1])
            self.assertEqual(resumed["verification"], "PASS")

            fresh = make_pipeline(fresh_root)
            fresh.run()
            for stage in range(1, 5):
                self.assertEqual(
                    (stopped_root / "stages" / f"stage{stage}.json").read_bytes(),
                    (fresh_root / "stages" / f"stage{stage}.json").read_bytes(),
                )


class CheckpointConcurrencyTests(unittest.TestCase):
    def test_two_processes_publish_same_sequence(self) -> None:
        with tempfile.TemporaryDirectory(prefix="halu-lock-") as tmp:
            store_dir = Path(tmp) / "run"
            code = (
                "import json,sys\n"
                "from pathlib import Path\n"
                "from halu_pipeline.checkpoints import CheckpointStore\n"
                "from halu_pipeline.canonical import PipelineError\n"
                "store=CheckpointStore(Path(sys.argv[1]))\n"
                "try:\n"
                " r=store.publish(stage='s',input_fingerprint='i',config_version='c',artifact_digest='a',sequence=1)\n"
                " print(json.dumps({'code':'OK','sequence':r['sequence']}))\n"
                "except PipelineError as e:\n"
                " print(json.dumps(e.to_dict()))\n"
            )
            env = dict(os.environ)
            env["PYTHONPATH"] = str(ROOT / "src")
            processes = [
                subprocess.Popen(
                    [sys.executable, "-c", code, str(store_dir)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=env,
                )
                for _ in range(2)
            ]
            outputs = []
            for process in processes:
                stdout, stderr = process.communicate(timeout=20)
                self.assertEqual(process.returncode, 0, stderr)
                outputs.append(json.loads(stdout))
            codes = sorted(item["code"] for item in outputs)
            self.assertEqual(codes, ["E_STALE_CHECKPOINT", "OK"])
            records = CheckpointStore(store_dir).list()
            self.assertEqual(len(records), 1)
            leftovers = list((store_dir / "checkpoints").glob("*.tmp"))
            self.assertEqual(leftovers, [])


class HTTPTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tempdir = tempfile.TemporaryDirectory(prefix="halu-http-")
        cls.root = Path(cls.tempdir.name) / "run"
        pipeline = make_pipeline(cls.root)
        pipeline.run()
        cls.server = PipelineHTTPServer(("127.0.0.1", 0), cls.root)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)
        cls.tempdir.cleanup()

    def _request(self, method: str, path: str, payload: dict | None = None):
        data = None if payload is None else canonical_bytes(payload)
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}",
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            try:
                return exc.code, json.loads(exc.read().decode("utf-8"))
            finally:
                exc.close()

    def test_status_sample_summary_and_invalid_threshold(self) -> None:
        status, body = self._request("GET", "/status")
        self.assertEqual(status, 200)
        self.assertEqual(body["code"], "OK")
        self.assertEqual(body["checkpoint_last_sequence"], 4)

        status, body = self._request(
            "POST", "/query/sample", {"sample_id": "zh-002", "budget_ms": 1000}
        )
        self.assertEqual(status, 200)
        self.assertTrue(body["detection"]["hallucination"])

        status, body = self._request("GET", "/summary")
        self.assertEqual(status, 200)
        self.assertEqual(len(body["strata"]), 8)

        status, body = self._request(
            "POST", "/config", {"assignments": ["sentence_support_threshold=2.0"]}
        )
        self.assertEqual(status, 400)
        self.assertEqual(body["code"], "E_INVALID_CONFIG")

    def test_http_latency_budget_error(self) -> None:
        status, body = self._request(
            "POST",
            "/query/sample",
            {"sample_id": "zh-002", "budget_ms": 0, "query_id": "http-budget"},
        )
        self.assertEqual(status, 408)
        self.assertEqual(body["code"], "E_LATENCY_BUDGET")
        self.assertEqual(body["query_id"], "http-budget")
        self.assertIn("elapsed_ms", body)


if __name__ == "__main__":
    unittest.main(verbosity=2)
