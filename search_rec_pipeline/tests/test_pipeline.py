from __future__ import annotations

import copy
import json
import shutil
import tempfile
import threading
import unittest
from pathlib import Path

import yaml

from recsys_pipeline.assembly import assemble_samples
from recsys_pipeline.checkpoints import CheckpointStore
from recsys_pipeline.config import Config, STAGES
from recsys_pipeline.decision import select_algorithm_plan
from recsys_pipeline.errors import (
    E_CROSS_LIMIT,
    E_HASH_DIM_ZERO,
    E_INVALID_FEATURE,
    E_INVALID_STAGE,
    E_STALE_CHECKPOINT,
    E_VECTOR_LENGTH_MISMATCH,
    PipelineError,
)
from recsys_pipeline.features import encode_records, normalize_samples
from recsys_pipeline.generator import SCALE_SIZES
from recsys_pipeline.pipeline import PipelineManager
from recsys_pipeline.sample_store import SampleRepository
from recsys_pipeline.server import create_server
from recsys_pipeline.util import (
    canonical_bytes,
    hash_json,
    read_json,
    read_jsonl,
    sha256_file,
    write_json_atomic,
    write_jsonl_atomic,
)
from recsys_pipeline.versioning import expected_stage_versions


ROOT = Path(__file__).resolve().parents[1]


class PipelineTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tempdir = tempfile.TemporaryDirectory()
        self.temp = Path(self._tempdir.name)
        self.manager = PipelineManager(ROOT, workdir_override=self.temp / "work")
        self.small = self.manager.repository.load_scale("small")
        self.plan = select_algorithm_plan(
            self.manager.config, self.small.declaration
        )["selected"]

    def tearDown(self) -> None:
        self._tempdir.cleanup()

    def test_three_scale_declarations_and_decisions(self) -> None:
        for scale in ("small", "medium", "large"):
            data = self.manager.repository.load_scale(scale)
            self.assertEqual(len(data.records), SCALE_SIZES[scale])
            decision = select_algorithm_plan(self.manager.config, data.declaration)
            selected = decision["selected"]
            self.assertLessEqual(selected["estimated_cost_units"], data.declaration["budget_units"])
            self.assertTrue(decision["budget"]["within_budget"])
            self.assertLessEqual(selected["cross_count"], data.declaration["cross_limit"])
            self.assertLessEqual(
                selected["cross_count"], self.manager.config.features["crossing_limit_hard"]
            )
            if scale == "small":
                self.assertEqual(selected["encoding_strategy"], "full_exact")
                self.assertEqual(selected["assembly_strategy"], "exact_materialize")
                guarded = [
                    item
                    for item in decision["candidate_evaluations"]
                    if item["encoding_strategy"] != "full_exact"
                ]
                self.assertTrue(guarded)
                self.assertTrue(
                    all(item["eliminated_reason"] == "small_scale_guard" for item in guarded)
                )
            elif scale == "medium":
                self.assertEqual(selected["encoding_strategy"], "batched_approx")
            else:
                self.assertEqual(selected["encoding_strategy"], "sampled_features")
                self.assertLess(
                    len(selected["active_categorical_fields"]),
                    len(self.manager.config.categorical_fields),
                )

    def test_normalization_is_deterministic_and_normalizes_unicode(self) -> None:
        records = copy.deepcopy(self.small.records[:80])
        records[0]["user"]["city"] = "  ＳＨＡＮＧＨＡＩ\t  "
        records[1]["user"]["age"] = None
        normalized_a, summary = normalize_samples(records, self.manager.config, "small")
        normalized_b, _ = normalize_samples(records, self.manager.config, "small")
        self.assertEqual(canonical_bytes(normalized_a), canonical_bytes(normalized_b))
        self.assertEqual(normalized_a[0]["features"]["user"]["city"]["value"], "shanghai")
        age = normalized_a[1]["features"]["user"]["age"]
        self.assertTrue(age["missing"])
        self.assertEqual(age["bucket_index"], -1)
        self.assertEqual(age["bucket_label"], "__MISSING__")
        self.assertEqual(summary["sample_count"], 80)

    def test_invalid_feature_errors_include_sample_id_and_field(self) -> None:
        empty_region = copy.deepcopy(self.small.records[:2])
        empty_region[0]["user"] = {}
        with self.assertRaises(PipelineError) as captured:
            normalize_samples(empty_region, self.manager.config, "small")
        self.assertEqual(captured.exception.code, E_INVALID_FEATURE)
        self.assertEqual(captured.exception.details["sample_id"], empty_region[0]["sample_id"])
        self.assertEqual(captured.exception.details["field"], "user")

        data = copy.deepcopy(self.manager.config.data)
        data["features"]["numeric"]["user.age"]["boundaries"] = [35, 18, 50]
        bad_config = Config.from_data(ROOT, data)
        with self.assertRaises(PipelineError) as captured:
            normalize_samples(copy.deepcopy(self.small.records[:3]), bad_config, "small")
        self.assertEqual(captured.exception.code, E_INVALID_FEATURE)
        self.assertEqual(captured.exception.details["sample_id"], self.small.records[0]["sample_id"])
        self.assertEqual(captured.exception.details["field"], "user.age")

    def test_encoding_errors_and_collision_observability(self) -> None:
        normalized, _ = normalize_samples(self.small.records, self.manager.config, "small")
        encoded_a, summary_a = encode_records(
            normalized, self.manager.config, self.small.declaration, self.plan
        )
        encoded_b, summary_b = encode_records(
            normalized, self.manager.config, self.small.declaration, self.plan
        )
        self.assertEqual(canonical_bytes(encoded_a), canonical_bytes(encoded_b))
        self.assertEqual(canonical_bytes(summary_a), canonical_bytes(summary_b))
        self.assertEqual(len(encoded_a[0]["vector"]), self.manager.config.features["vector_length"])
        self.assertIn("category_collisions", summary_a)
        self.assertIn("cross_collisions", summary_a)
        self.assertIn("category_vector_collision_count", summary_a)
        self.assertIn("cross_vector_collision_count", summary_a)
        self.assertIn("collision_count", next(iter(summary_a["cross_collisions"].values())))

        zero_data = copy.deepcopy(self.manager.config.data)
        zero_data["features"]["hash_dim"] = 0
        zero_config = Config.from_data(ROOT, zero_data)
        with self.assertRaises(PipelineError) as captured:
            encode_records(normalized, zero_config, self.small.declaration, self.plan)
        self.assertEqual(captured.exception.code, E_HASH_DIM_ZERO)

        mismatch_data = copy.deepcopy(self.manager.config.data)
        mismatch_data["features"]["vector_length"] = int(
            mismatch_data["features"]["vector_length"]
        ) + 1
        mismatch_config = Config.from_data(ROOT, mismatch_data)
        with self.assertRaises(PipelineError) as captured:
            encode_records(normalized, mismatch_config, self.small.declaration, self.plan)
        self.assertEqual(captured.exception.code, E_VECTOR_LENGTH_MISMATCH)

        over_plan = copy.deepcopy(self.plan)
        over_plan["cross_definitions"] = self.manager.config.cross_candidates[:6]
        over_plan["cross_count"] = 6
        with self.assertRaises(PipelineError) as captured:
            encode_records(normalized, self.manager.config, self.small.declaration, over_plan)
        self.assertEqual(captured.exception.code, E_CROSS_LIMIT)

    def test_cross_limit_and_budget_edges(self) -> None:
        declaration = copy.deepcopy(self.small.declaration)
        declaration["cross_limit"] = int(self.manager.config.features["crossing_limit_hard"]) + 1
        with self.assertRaises(PipelineError) as captured:
            select_algorithm_plan(self.manager.config, declaration)
        self.assertEqual(captured.exception.code, E_CROSS_LIMIT)

        declaration = copy.deepcopy(self.small.declaration)
        declaration["budget_units"] = 1
        with self.assertRaises(PipelineError) as captured:
            select_algorithm_plan(self.manager.config, declaration)
        self.assertEqual(captured.exception.code, "E_BUDGET_EXCEEDED")

    def test_assembly_statistics_are_derived_from_assembled_records(self) -> None:
        result = self.manager.run("small")
        self.assertEqual(result["state"], "completed")
        stats_path = self.manager._stage_files("small", "assemble")["stats"]
        samples_path = self.manager._stage_files("small", "assemble")["samples"]
        discarded_path = self.manager._stage_files("small", "assemble")["discarded"]
        stats = read_json(stats_path)
        samples = read_jsonl(samples_path)
        discards = read_jsonl(discarded_path)
        self.assertEqual(stats["assembled_count"], len(samples))
        self.assertEqual(stats["discarded_count"], len(discards))
        self.assertGreater(stats["discarded_reasons"].get("missing_label", 0), 0)
        body = dict(stats)
        digest = body.pop("content_digest")
        self.assertEqual(digest, hash_json(body))
        self.assertEqual(stats["by_source_lang"], sorted(
            stats["by_source_lang"], key=lambda item: (item["source"], item["lang"])
        ))

    def test_stop_request_resume_and_corrupt_checkpoint_match_full_run(self) -> None:
        full_manager = PipelineManager(ROOT, workdir_override=self.temp / "full")
        full_result = full_manager.run("small")
        self.assertEqual(full_result["state"], "completed")
        expected_samples = full_manager._stage_files("small", "assemble")["samples"].read_bytes()
        expected_stats = full_manager._stage_files("small", "assemble")["stats"].read_bytes()

        resume_manager = PipelineManager(ROOT, workdir_override=self.temp / "resume")
        resume_manager.request_stop("small")
        stopped = resume_manager.run("small")
        self.assertEqual(stopped["state"], "stopped")
        self.assertEqual(stopped["stop_reason"], "stop_request")
        self.assertEqual(stopped["stages_executed"], ["normalize"])
        resumed = resume_manager.resume("small")
        self.assertEqual(resumed["state"], "completed")
        self.assertEqual(resumed["stages_reused"], ["normalize"])
        self.assertEqual(
            resume_manager._stage_files("small", "assemble")["samples"].read_bytes(),
            expected_samples,
        )
        self.assertEqual(
            resume_manager._stage_files("small", "assemble")["stats"].read_bytes(),
            expected_stats,
        )

        corrupt_manager = PipelineManager(ROOT, workdir_override=self.temp / "corrupt")
        corrupt_manager.run("small", stop_after_stage="normalize")
        checkpoint = corrupt_manager.checkpoints.checkpoint_path("small", 1, "normalize")
        checkpoint.write_bytes(b'{"broken":')
        status = corrupt_manager.status("small")
        self.assertEqual(status["earliest_invalid_stage"], "normalize")
        self.assertEqual(status["stages"][0]["state"], "corrupt_checkpoint")
        recovered = corrupt_manager.run("small")
        self.assertEqual(recovered["state"], "completed")
        self.assertEqual(
            corrupt_manager._stage_files("small", "assemble")["stats"].read_bytes(),
            expected_stats,
        )

    def test_decision_parameter_change_reuses_normalize_and_invalidates_downstream(self) -> None:
        manager = PipelineManager(ROOT, workdir_override=self.temp / "version")
        manager.run("small")
        normalized_before = manager._stage_files("small", "normalize")["normalized"].read_bytes()
        changed = copy.deepcopy(manager.config.data)
        changed["decision"]["score_weights"]["cost"] = 0.81
        changed_manager = PipelineManager(
            ROOT,
            config_data=changed,
            workdir_override=self.temp / "version",
            config_label="changed-decision-weights",
        )
        status = changed_manager.status("small")
        self.assertEqual(status["earliest_invalid_stage"], "decision")
        self.assertEqual(status["stages"][0]["state"], "valid")
        result = changed_manager.run("small")
        self.assertEqual(result["state"], "completed")
        self.assertEqual(result["stages_reused"], ["normalize"])
        self.assertEqual(result["stages_executed"], ["decision", "encode", "assemble"])
        self.assertEqual(
            changed_manager._stage_files("small", "normalize")["normalized"].read_bytes(),
            normalized_before,
        )

    def test_version_fingerprint_earliest_stage_mapping(self) -> None:
        declaration = self.small.declaration
        baseline = expected_stage_versions(
            self.manager.config,
            "small",
            declaration,
            self.small.sample_input_fingerprint,
        )

        budget_changed = copy.deepcopy(declaration)
        budget_changed["budget_units"] += 1000
        versions = expected_stage_versions(
            self.manager.config,
            "small",
            budget_changed,
            self.small.sample_input_fingerprint,
        )
        self.assertEqual(
            baseline["normalize"].stage_fingerprint,
            versions["normalize"].stage_fingerprint,
        )
        self.assertNotEqual(
            baseline["decision"].stage_fingerprint,
            versions["decision"].stage_fingerprint,
        )

        bucket_data = copy.deepcopy(self.manager.config.data)
        bucket_data["features"]["numeric"]["user.age"]["boundaries"] = [18, 26, 35, 50, 65]
        bucket_config = Config.from_data(ROOT, bucket_data)
        versions = expected_stage_versions(
            bucket_config,
            "small",
            declaration,
            self.small.sample_input_fingerprint,
        )
        self.assertNotEqual(
            baseline["normalize"].stage_fingerprint,
            versions["normalize"].stage_fingerprint,
        )

        hash_data = copy.deepcopy(self.manager.config.data)
        hash_data["features"]["hash_dim"] = 32
        hash_data["features"]["vector_length"] = 42 + 32 + int(
            hash_data["features"]["cross_hash_dim"]
        )
        hash_config = Config.from_data(ROOT, hash_data)
        versions = expected_stage_versions(
            hash_config,
            "small",
            declaration,
            self.small.sample_input_fingerprint,
        )
        self.assertEqual(
            baseline["decision"].stage_fingerprint,
            versions["decision"].stage_fingerprint,
        )
        self.assertNotEqual(
            baseline["encode"].stage_fingerprint,
            versions["encode"].stage_fingerprint,
        )

    def test_sample_content_change_invalidates_normalize(self) -> None:
        project = self.temp / "content_project"
        (project / "config").mkdir(parents=True)
        (project / "samples").mkdir(parents=True)
        shutil.copyfile(ROOT / "config" / "pipeline.yaml", project / "config" / "pipeline.yaml")
        shutil.copyfile(ROOT / "samples" / "small.jsonl", project / "samples" / "small.jsonl")
        shutil.copyfile(
            ROOT / "samples" / "small.declaration.json",
            project / "samples" / "small.declaration.json",
        )
        config = Config.load(project)
        write_json_atomic(
            config.workdir / "import_manifest.json",
            {"schema_version": 1, "config_version": config.config_version, "scales": {}},
        )
        manager = PipelineManager(project)
        manager.run("small")
        sample_path = project / "samples" / "small.jsonl"
        records = read_jsonl(sample_path)
        records[0]["user"]["city"] = "上海·新"
        write_jsonl_atomic(sample_path, records)
        status = manager.status("small")
        self.assertEqual(status["earliest_invalid_stage"], "normalize")
        self.assertIn("sample_content_changed", status["stages"][0]["reasons"])
        self.assertEqual(manager.run("small")["state"], "completed")

    def test_declaration_count_mismatch_and_illegal_stage(self) -> None:
        project = self.temp / "mismatch_project"
        (project / "config").mkdir(parents=True)
        (project / "samples").mkdir(parents=True)
        shutil.copyfile(ROOT / "config" / "pipeline.yaml", project / "config" / "pipeline.yaml")
        shutil.copyfile(ROOT / "samples" / "small.jsonl", project / "samples" / "small.jsonl")
        declaration = read_json(ROOT / "samples" / "small.declaration.json")
        declaration["sample_count"] += 1
        write_json_atomic(project / "samples" / "small.declaration.json", declaration)
        config = Config.load(project)
        repository = SampleRepository(config)
        with self.assertRaises(PipelineError) as captured:
            repository.load_scale("small")
        self.assertEqual(captured.exception.code, E_INVALID_STAGE)
        self.assertEqual(captured.exception.details["scale"], "small")
        self.assertEqual(captured.exception.details["actual_sample_count"], 600)

    def test_checkpoint_file_lock_sequence_and_cross_scale_rejection(self) -> None:
        store = CheckpointStore(self.temp / "checkpoints")
        artifact = store.stage_artifact_dir("small", "normalize") / "artifact.json"
        write_json_atomic(artifact, {"ok": True})
        barrier = threading.Barrier(2)
        outcomes: list[tuple[str, str | None]] = []

        def publisher() -> None:
            barrier.wait()
            try:
                store.publish(
                    "small",
                    "normalize",
                    expected_seq=1,
                    input_fingerprint="input",
                    config_version="config",
                    stage_config_fingerprint="stage-config",
                    stage_fingerprint="stage",
                    artifact_paths=[artifact],
                )
                outcomes.append(("ok", None))
            except PipelineError as exc:
                outcomes.append(("error", exc.code))

        threads = [threading.Thread(target=publisher), threading.Thread(target=publisher)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sorted(item[0] for item in outcomes), ["error", "ok"])
        self.assertIn(E_STALE_CHECKPOINT, [item[1] for item in outcomes])

        original_artifact = artifact.read_bytes()
        decision_artifact = store.stage_artifact_dir("small", "decision") / "artifact.json"
        write_json_atomic(decision_artifact, {"decision": True})
        artifact.unlink()
        with self.assertRaises(PipelineError) as captured:
            store.publish(
                "small",
                "decision",
                expected_seq=2,
                input_fingerprint="input2",
                config_version="config",
                stage_config_fingerprint="stage-config2",
                stage_fingerprint="stage2",
                artifact_paths=[decision_artifact],
            )
        self.assertEqual(captured.exception.code, E_INVALID_STAGE)
        self.assertEqual(captured.exception.details["prerequisite_stage"], "normalize")
        artifact.write_bytes(original_artifact)

        with self.assertRaises(PipelineError) as captured:
            store.publish(
                "small",
                "decision",
                expected_seq=2,
                input_fingerprint="input2",
                config_version="config",
                stage_config_fingerprint="stage-config2",
                stage_fingerprint="stage2",
                artifact_paths=[artifact],
                record_scale="medium",
            )
        self.assertEqual(captured.exception.code, E_STALE_CHECKPOINT)

        with self.assertRaises(PipelineError) as captured:
            store.publish(
                "small",
                "encode",
                expected_seq=2,
                input_fingerprint="input3",
                config_version="config",
                stage_config_fingerprint="stage-config3",
                stage_fingerprint="stage3",
                artifact_paths=[artifact],
            )
        self.assertEqual(captured.exception.code, E_INVALID_STAGE)

    def test_http_service_smoke(self) -> None:
        import urllib.error
        import urllib.request

        server = create_server(
            ROOT,
            port=0,
            workdir_override=self.temp / "http_work",
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{server.server_address[1]}"

        def request(method: str, path: str, body: dict | None = None):
            data = json.dumps(body).encode("utf-8") if body is not None else None
            req = urllib.request.Request(
                base + path,
                data=data,
                method=method,
                headers={"Content-Type": "application/json"},
            )
            try:
                with urllib.request.urlopen(req, timeout=30) as response:
                    return response.status, response.headers, response.read()
            except urllib.error.HTTPError as exc:
                try:
                    return exc.code, exc.headers, exc.read()
                finally:
                    exc.close()

        try:
            status_code, _headers, body = request("GET", "/health")
            self.assertEqual(status_code, 200)
            self.assertTrue(json.loads(body)["ok"])
            status_code, _headers, body = request("POST", "/import", {})
            self.assertEqual(status_code, 200)
            self.assertTrue(json.loads(body)["ok"])
            status_code, _headers, body = request("POST", "/run", {"scale": "small"})
            self.assertEqual(status_code, 200)
            self.assertEqual(json.loads(body)["data"]["state"], "completed")
            status_code, _headers, body = request("GET", "/status?scale=small")
            self.assertEqual(status_code, 200)
            self.assertTrue(json.loads(body)["data"]["fully_complete"])
            status_code, headers, body = request("GET", "/export?scale=small&kind=stats")
            self.assertEqual(status_code, 200)
            self.assertIn("application/json", headers.get("Content-Type", ""))
            self.assertIn("content_digest", json.loads(body))
            status_code, _headers, body = request("GET", "/status?scale=bogus")
            self.assertEqual(status_code, 409)
            self.assertEqual(json.loads(body)["error"]["code"], E_INVALID_STAGE)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
