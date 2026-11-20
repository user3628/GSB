from __future__ import annotations

import json
import random
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest import mock

from sparse_index.config import IndexConfig
from sparse_index.errors import PipelineError
from sparse_index.http_api import LocalIndexService, make_handler
from sparse_index.pipeline import Pipeline
from sparse_index.query import QueryEngine
from sparse_index.reference import reference_query
from sparse_index.stages import verify_compressed_postings
from sparse_index.utils import canonical_json_bytes

from tests.helpers import BASE_CONFIG, DATA_DIR, artifact_bytes, base_query_list, read_json, variant_config


class PipelineAcceptanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._temp = tempfile.TemporaryDirectory(prefix="sparse-index-tests-")
        cls.workspace = Path(cls._temp.name)
        cls.config = IndexConfig.load(BASE_CONFIG)
        cls.pipeline = Pipeline(cls.config, cls.workspace)
        cls.build_result = cls.pipeline.build()
        cls.stage4 = read_json(cls.workspace / "artifacts" / "stage4" / "query_results.json")

    @classmethod
    def tearDownClass(cls) -> None:
        cls._temp.cleanup()

    def _fresh_pipeline(self, prefix: str) -> tuple[tempfile.TemporaryDirectory, Path]:
        holder = tempfile.TemporaryDirectory(prefix=prefix)
        return holder, Path(holder.name)

    def test_complete_build_and_contract_files(self) -> None:
        self.assertEqual(self.build_result["status"], "complete")
        stage1 = read_json(self.workspace / "artifacts" / "stage1" / "terms.json")
        self.assertTrue(any(document["empty_body"] for document in stage1["documents"]))
        self.assertEqual(
            [term["term_id"] for term in stage1["terms"]],
            list(range(len(stage1["terms"]))),
        )
        terms = [term["term"] for term in stage1["terms"]]
        self.assertEqual(terms, sorted(terms))
        stage2 = read_json(self.workspace / "artifacts" / "stage2" / "statistics.json")
        self.assertTrue(all(term["quant_scale"] > 0 for term in stage2["terms"]))
        self.assertTrue(all(term["document_frequency"] > 0 for term in stage2["terms"]))
        manifest = read_json(self.workspace / "artifacts" / "stage3" / "postings.json")
        self.assertEqual(manifest["encoding"]["skip_step"], 4)
        self.assertEqual(manifest["encoding"]["block_size"], 6)
        self.assertTrue(all(term["skips"] for term in manifest["terms"]))

    def test_byte_determinism_across_clean_builds(self) -> None:
        holder, other_workspace = self._fresh_pipeline("sparse-index-determinism-")
        try:
            Pipeline(self.config, other_workspace).build()
            self.assertEqual(artifact_bytes(self.workspace), artifact_bytes(other_workspace))
        finally:
            holder.cleanup()

    def test_block_round_trip_and_all_benchmarks_match_reference(self) -> None:
        report = verify_compressed_postings(
            self.config,
            self.workspace / "artifacts" / "stage2",
            self.workspace / "artifacts" / "stage3",
        )
        self.assertTrue(report["ok"])
        engine = QueryEngine(self.config, self.workspace / "artifacts")
        for query in base_query_list():
            compressed = engine.execute(
                query_id=query["query_id"],
                terms=query["terms"],
                mode=query["mode"],
                limit=query["limit"],
                use_cache=False,
            )["result"]
            sequential = reference_query(
                self.config,
                self.workspace / "artifacts",
                terms=query["terms"],
                mode=query["mode"],
                limit=query["limit"],
            )
            self.assertEqual(compressed, sequential, query["query_id"])

    def test_randomized_and_or_queries_match_reference(self) -> None:
        engine = QueryEngine(self.config, self.workspace / "artifacts")
        stage1 = read_json(self.workspace / "artifacts" / "stage1" / "terms.json")
        vocabulary = [term["term"] for term in stage1["terms"]]
        rng = random.Random(424242)
        for index in range(80):
            count = rng.randint(1, 4)
            terms = rng.sample(vocabulary, count)
            if index % 11 == 0:
                terms.append("__missing_term__")
            mode = "and" if index % 2 == 0 else "or"
            limit = rng.choice([1, 5, 20, 100])
            compressed = engine.execute(
                query_id=f"random-{index}",
                terms=terms,
                mode=mode,
                limit=limit,
                use_cache=False,
            )["result"]
            sequential = reference_query(
                self.config,
                self.workspace / "artifacts",
                terms=terms,
                mode=mode,
                limit=limit,
            )
            self.assertEqual(compressed, sequential, f"random query {index}: {terms} {mode}")

    def test_cache_hit_and_eviction_preserve_result_fields(self) -> None:
        cache_path = self.workspace / "artifacts" / "cache" / "query_cache.json"
        if cache_path.exists():
            cache_path.unlink()
        engine = QueryEngine(self.config, self.workspace / "artifacts")
        first = engine.execute(query_id="cache-1", terms=["检索", "索引"], mode="and", limit=20)
        second = engine.execute(query_id="cache-1", terms=["索引", "检索"], mode="and", limit=20)
        self.assertFalse(first["meta"]["cache_hit"])
        self.assertTrue(second["meta"]["cache_hit"])
        self.assertEqual(first["result"], second["result"])
        for index, term in enumerate(["压缩", "缓存", "跳表", "分块"]):
            engine.execute(query_id=f"fill-{index}", terms=[term], mode="or", limit=20)
        after_eviction = engine.execute(query_id="cache-1", terms=["检索", "索引"], mode="and", limit=20)
        self.assertEqual(first["result"], after_eviction["result"])
        self.assertGreaterEqual(engine.cache.evictions, 1)

    def test_latency_budget_error_has_query_id_and_elapsed(self) -> None:
        holder, temp_dir = self._fresh_pipeline("sparse-index-latency-")
        try:
            config = variant_config(
                temp_dir,
                lambda raw: raw["query"].__setitem__("latency_budget_ms", 0),
            )
            engine = QueryEngine(config, self.workspace / "artifacts")
            with self.assertRaises(PipelineError) as context:
                engine.execute(query_id="latency-q", terms=["检索"], mode="or", limit=20, use_cache=False)
            self.assertEqual(context.exception.code, "E_LATENCY_BUDGET")
            self.assertEqual(context.exception.details["query_id"], "latency-q")
            self.assertIn("elapsed_ms", context.exception.details)
        finally:
            holder.cleanup()

    def test_out_of_bounds_query_error(self) -> None:
        engine = QueryEngine(self.config, self.workspace / "artifacts")
        with self.assertRaises(PipelineError) as context:
            engine.execute(query_id="oob", terms=["检索"], mode="and", limit=0)
        self.assertEqual(context.exception.code, "E_QUERY_OUT_OF_BOUNDS")
        self.assertEqual(context.exception.details["limit"], 0)

    def test_invalid_skip_step_stops_stage_three(self) -> None:
        holder, temp_dir = self._fresh_pipeline("sparse-index-skip-")
        try:
            config = variant_config(temp_dir, lambda raw: raw["postings"].__setitem__("skip_step", 0))
            pipeline = Pipeline(config, temp_dir)
            with self.assertRaises(PipelineError) as context:
                pipeline.build()
            self.assertEqual(context.exception.code, "E_INVALID_SKIP_STEP")
            active, _ = pipeline.checkpoints.active()
            self.assertEqual([record["stage"] for record in active], ["tokenize", "statistics"])
        finally:
            holder.cleanup()

    def test_nan_weight_raises_business_error_with_context(self) -> None:
        holder, temp_dir = self._fresh_pipeline("sparse-index-weight-")
        try:
            config = variant_config(temp_dir, lambda raw: raw["bm25"].__setitem__("k1", float("nan")))
            pipeline = Pipeline(config, temp_dir)
            with self.assertRaises(PipelineError) as context:
                pipeline.build()
            self.assertEqual(context.exception.code, "E_WEIGHT_NUMERIC")
            self.assertIn("doc_id", context.exception.details)
            self.assertIn("term_id", context.exception.details)
        finally:
            holder.cleanup()

    def test_corrupt_block_is_reported_at_query_time(self) -> None:
        holder, temp_dir = self._fresh_pipeline("sparse-index-corruption-")
        try:
            pipeline = Pipeline(self.config, temp_dir)
            pipeline.build()
            manifest = read_json(temp_dir / "artifacts" / "stage3" / "postings.json")
            term = next(item for item in manifest["terms"] if item["term"] == "检索")
            first_block = term["blocks"][0]
            binary_path = temp_dir / "artifacts" / "stage3" / "postings.bin"
            data = bytearray(binary_path.read_bytes())
            payload_start = int(first_block["offset"]) + 13
            data[payload_start] ^= 0x01
            binary_path.write_bytes(bytes(data))
            engine = QueryEngine(self.config, temp_dir / "artifacts")
            with self.assertRaises(PipelineError) as context:
                engine.execute(query_id="corrupt", terms=["检索"], mode="or", limit=20, use_cache=False)
            self.assertEqual(context.exception.code, "E_BLOCK_CORRUPTION")
            self.assertEqual(context.exception.details["term_id"], term["term_id"])
        finally:
            holder.cleanup()

    def test_query_mismatch_reports_term_ids(self) -> None:
        holder, temp_dir = self._fresh_pipeline("sparse-index-mismatch-")
        real_reference = reference_query

        def changed_reference(*args, **kwargs):
            result = real_reference(*args, **kwargs)
            result["hits"] = []
            result["total_hits"] = 0
            return result

        try:
            pipeline = Pipeline(self.config, temp_dir)
            with mock.patch("sparse_index.reference.reference_query", side_effect=changed_reference):
                with self.assertRaises(PipelineError) as context:
                    pipeline.build()
            self.assertEqual(context.exception.code, "E_QUERY_MISMATCH")
            self.assertIn("term_ids", context.exception.details)
        finally:
            holder.cleanup()

    def test_skip_parameter_invalidates_postings_and_downstream_only(self) -> None:
        holder, temp_dir = self._fresh_pipeline("sparse-index-invalidate-skip-")
        try:
            base_pipeline = Pipeline(self.config, temp_dir)
            base_pipeline.build()
            before = artifact_bytes(temp_dir)
            engine = QueryEngine(self.config, temp_dir / "artifacts")
            engine.execute(query_id="prime-cache", terms=["检索", "索引"], mode="and", limit=20)
            cache_path = temp_dir / "artifacts" / "cache" / "query_cache.json"
            self.assertTrue(cache_path.exists())

            config_dir = temp_dir / "variants"
            config_dir.mkdir()
            changed = variant_config(
                config_dir,
                lambda raw: raw["postings"].__setitem__("skip_step", 5),
                filename="skip-five.yaml",
            )
            pending = Pipeline(changed, temp_dir).status()
            self.assertFalse(pending["up_to_date"])
            self.assertEqual(pending["invalidation_plan"]["earliest_affected_stage"], 3)
            self.assertEqual(pending["invalidation_plan"]["affected_stages"], ["postings", "query_summary"])
            self.assertNotEqual(
                pending["old_stage_fingerprints"]["postings"],
                pending["new_stage_fingerprints"]["postings"],
            )
            self.assertTrue(any("skip_step" in reason for reason in pending["invalidation_plan"]["reasons"]))
            result = Pipeline(changed, temp_dir).build()
            self.assertEqual(result["invalidation"]["earliest_affected_stage"], 3)
            self.assertEqual(result["invalidation"]["affected_stages"], ["postings", "query_summary"])
            after = artifact_bytes(temp_dir)
            self.assertEqual(before["artifacts/stage1/terms.json"], after["artifacts/stage1/terms.json"])
            self.assertEqual(before["artifacts/stage2/statistics.json"], after["artifacts/stage2/statistics.json"])
            self.assertNotEqual(before["artifacts/stage3/postings.json"], after["artifacts/stage3/postings.json"])
            self.assertFalse(cache_path.exists())
            status = Pipeline(changed, temp_dir).status()
            self.assertTrue(status["up_to_date"])
            self.assertTrue(any("skip_step" in reason for reason in status["invalidation_history"][-1]["reasons"]))
        finally:
            holder.cleanup()

    def test_cache_config_change_invalidates_stage_four_and_cache(self) -> None:
        holder, temp_dir = self._fresh_pipeline("sparse-index-invalidate-cache-")
        try:
            base_pipeline = Pipeline(self.config, temp_dir)
            base_pipeline.build()
            before = artifact_bytes(temp_dir)
            engine = QueryEngine(self.config, temp_dir / "artifacts")
            engine.execute(query_id="prime-cache", terms=["检索"], mode="or", limit=20)
            change_dir = temp_dir / "variants"
            change_dir.mkdir()
            changed = variant_config(
                change_dir,
                lambda raw: raw["query"].__setitem__("cache_capacity", 2),
                filename="cache-two.yaml",
            )
            result = Pipeline(changed, temp_dir).build()
            self.assertEqual(result["invalidation"]["earliest_affected_stage"], 4)
            after = artifact_bytes(temp_dir)
            for relative in [
                "artifacts/stage1/terms.json",
                "artifacts/stage2/statistics.json",
                "artifacts/stage3/postings.bin",
                "artifacts/stage3/postings.json",
            ]:
                self.assertEqual(before[relative], after[relative], relative)
            self.assertFalse((temp_dir / "artifacts" / "cache" / "query_cache.json").exists())
        finally:
            holder.cleanup()

    def test_stop_then_resume_is_byte_identical(self) -> None:
        holder_a, stopped_workspace = self._fresh_pipeline("sparse-index-stop-")
        holder_b, complete_workspace = self._fresh_pipeline("sparse-index-complete-")
        try:
            pipeline = Pipeline(self.config, stopped_workspace)
            counter = {"value": 0}

            def stop_after_three() -> bool:
                counter["value"] += 1
                return counter["value"] > 3

            with self.assertRaises(Exception) as context:
                pipeline.build(stop_check=stop_after_three)
            self.assertEqual(context.exception.__class__.__name__, "GracefulStop")
            self.assertEqual(pipeline.checkpoints.active()[0], [])
            resumed = pipeline.build()
            self.assertEqual(resumed["status"], "complete")
            Pipeline(self.config, complete_workspace).build()
            self.assertEqual(artifact_bytes(stopped_workspace), artifact_bytes(complete_workspace))
        finally:
            holder_a.cleanup()
            holder_b.cleanup()

    def test_artifact_tampering_is_detected_as_earliest_stage(self) -> None:
        holder, temp_dir = self._fresh_pipeline("sparse-index-artifact-tamper-")
        try:
            pipeline = Pipeline(self.config, temp_dir)
            pipeline.build()
            statistics = temp_dir / "artifacts" / "stage2" / "statistics.json"
            statistics.write_bytes(statistics.read_bytes() + b" ")
            inspection = pipeline.inspect()
            self.assertEqual(inspection["earliest_affected_stage"], 2)
            self.assertIn("artifact_missing_or_digest_mismatch", inspection["reasons"])
        finally:
            holder.cleanup()

    def test_http_query_and_structured_error(self) -> None:
        service = LocalIndexService(self.config, self.workspace)
        server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(service))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = server.server_address[1]
            request = urllib.request.Request(
                f"http://127.0.0.1:{port}/query",
                data=canonical_json_bytes(
                    {"query_id": "http-test", "terms": ["检索", "索引"], "mode": "and", "limit": 20}
                ),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=5) as response:
                payload = json.loads(response.read().decode("utf-8"))
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["result"]["total_hits"], 4)

            bad_request = urllib.request.Request(
                f"http://127.0.0.1:{port}/query",
                data=canonical_json_bytes(
                    {"query_id": "http-oob", "terms": ["检索"], "mode": "and", "limit": 0}
                ),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with self.assertRaises(urllib.error.HTTPError) as context:
                urllib.request.urlopen(bad_request, timeout=5)
            self.assertEqual(context.exception.code, 400)
            error_payload = json.loads(context.exception.read().decode("utf-8"))
            context.exception.close()
            self.assertEqual(error_payload["error"]["code"], "E_QUERY_OUT_OF_BOUNDS")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
