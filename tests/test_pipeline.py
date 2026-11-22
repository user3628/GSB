from __future__ import annotations

import json
import multiprocessing as mp
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from fractions import Fraction
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest import mock

from llm_pipeline import engine as engine_module
from llm_pipeline.allocation import apportion_with_capacity
from llm_pipeline.checkpoints import CheckpointStore, record_without_private
from llm_pipeline.config import make_layer_id
from llm_pipeline.dedup import band_keys, deduplicate, normalize_text, simhash64
from llm_pipeline.engine import PipelineEngine
from llm_pipeline.errors import PipelineError
from llm_pipeline.generator import demo_config
from llm_pipeline.server import make_handler
from llm_pipeline.util import canonical_json_line, sha256_bytes, sha256_obj
from llm_pipeline.workspace import (
    Workspace,
    allocation_product_bytes,
    record_keep_product_bytes,
)


ROOT = Path(__file__).resolve().parents[1]
DEMO_DIR = ROOT / "demo"


def _new_workspace(temp_root: Path, name: str = "workspace") -> Workspace:
    workspace = Workspace(temp_root / name)
    workspace.initialize_demo(DEMO_DIR)
    return workspace


def _record_overrides(engine: PipelineEngine) -> dict[str, dict]:
    state = engine.store.load()
    return {
        stage: engine.store.artifact_payload(record)
        for stage, record in state.latest_by_stage.items()
    }


def _race_publish(root: str, barrier, queue, worker_id: int, expected_seq: int) -> None:
    from llm_pipeline.checkpoints import CheckpointStore
    from llm_pipeline.errors import PipelineError
    from llm_pipeline.workspace import Workspace

    workspace = Workspace(root)
    store = CheckpointStore(workspace)
    payload = {"worker": worker_id, "items": [worker_id, worker_id + 1]}
    barrier.wait(timeout=10)
    try:
        record = store.publish(
            stage="normalize",
            payload=payload,
            stage_fingerprint=sha256_obj({"worker": worker_id}),
            fingerprint_components={"worker": worker_id},
            input_shards=[],
            input_shard_fingerprint=sha256_obj([]),
            config_version=sha256_obj({"config": worker_id}),
            config_revision=0,
            expected_seq=expected_seq,
        )
        queue.put(("ok", worker_id, record["seq"], record["artifact_digest"]))
    except PipelineError as exc:
        queue.put(("error", worker_id, exc.code, exc.details))


class PipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp = tempfile.TemporaryDirectory(prefix="llm-pipeline-test-")
        self.temp = Path(self._temp.name)

    def tearDown(self) -> None:
        self._temp.cleanup()

    def test_normalization_simhash_and_deduplication(self) -> None:
        self.assertEqual(normalize_text("Ａ\u3000  Ｂ\n"), "a b")
        value = simhash64(normalize_text("Deterministic pipelines are reproducible."))
        self.assertGreaterEqual(value, 0)
        self.assertLess(value, 1 << 64)
        self.assertEqual(len(band_keys(value)), 4)
        self.assertTrue(all(0 <= part < 65536 for _, part in band_keys(value)))

        workspace = _new_workspace(self.temp)
        engine = PipelineEngine(workspace)
        run = engine.run(force=True)
        self.assertEqual(run["status"], "rebuilt")
        payload = _record_overrides(engine)["normalize"]
        self.assertEqual(payload["stats"]["input_count"], 35)
        self.assertEqual(payload["stats"]["kept_count"], 27)
        self.assertEqual(payload["stats"]["duplicate_count"], 5)
        self.assertEqual(payload["stats"]["empty_error_count"], 3)
        duplicates = {item["id"]: item for item in payload["duplicates"]}
        self.assertEqual(duplicates["demo-0033"]["kept_id"], "demo-0015")
        self.assertEqual(duplicates["demo-0033"]["distance"], 7)
        self.assertEqual(duplicates["demo-0032"]["kept_id"], "demo-0014")
        self.assertTrue(duplicates["demo-0032"]["normalized_equal"])
        self.assertEqual(
            [item["id"] for item in payload["empty_errors"]],
            ["demo-0091", "demo-0092", "demo-0093"],
        )
        self.assertTrue(
            all(item["code"] == "E_EMPTY_TEXT" for item in payload["empty_errors"])
        )
        # Recomputing from the same input bytes must be identical.
        again = deduplicate(engine.snapshot().records, demo_config())
        self.assertEqual(
            json.dumps(payload, ensure_ascii=False, sort_keys=True),
            json.dumps(again, ensure_ascii=False, sort_keys=True),
        )

    def test_largest_remainder_tie_and_empty_layers(self) -> None:
        # Equal remainders; source (then lang) is the deterministic tie-break.
        quotas = apportion_with_capacity(
            1,
            {("b", "x"): Fraction(1), ("a", "y"): Fraction(1)},
            {("b", "x"): 10, ("a", "y"): 10},
        )
        self.assertEqual(quotas[("a", "y")], 1)
        self.assertEqual(quotas[("b", "x")], 0)

        workspace = _new_workspace(self.temp)
        engine = PipelineEngine(workspace)
        engine.run(force=True)
        allocation = _record_overrides(engine)["allocate"]
        self.assertEqual(allocation["total_allocated"], 24)
        layers = {row["layer_id"]: row for row in allocation["layers"]}
        self.assertEqual(layers[make_layer_id("archive", "zh")]["status"], "empty")
        self.assertEqual(layers[make_layer_id("forum", "en")]["status"], "empty")
        self.assertEqual(layers[make_layer_id("archive", "zh")]["allocated"], 0)
        self.assertEqual(layers[make_layer_id("forum", "en")]["allocated"], 0)
        self.assertTrue(all("token_count" in row for row in allocation["layers"]))
        self.assertEqual(
            sum(row["allocated"] for row in allocation["layers"]),
            allocation["total_allocated"],
        )
        report = engine.report()
        self.assertIn(make_layer_id("archive", "zh"), report["empty_layers"])
        self.assertIn(make_layer_id("forum", "en"), report["empty_layers"])

    def test_full_build_is_byte_deterministic_across_workspaces(self) -> None:
        first = PipelineEngine(_new_workspace(self.temp, "a"))
        second = PipelineEngine(_new_workspace(self.temp, "b"))
        first_result = first.run(force=True)
        second_result = second.run(force=True)
        self.assertEqual(first_result["digests"], second_result["digests"])
        self.assertEqual(first.mixed_bytes(), second.mixed_bytes())
        self.assertEqual(first.report(), second.report())
        first_digests = {row["stage"]: row["artifact_digest"] for row in first_result["rebuilt_stages"]}
        second_digests = {row["stage"]: row["artifact_digest"] for row in second_result["rebuilt_stages"]}
        self.assertEqual(first_digests, second_digests)
        # A no-op rerun must not advance the sequence.
        noop = first.run()
        self.assertEqual(noop["status"], "up_to_date")
        self.assertEqual(noop["checkpoint_seq_after"], 4)

    def test_checkpoint_fields_and_sequence(self) -> None:
        workspace = _new_workspace(self.temp)
        engine = PipelineEngine(workspace)
        engine.run(force=True)
        state = CheckpointStore(workspace).load()
        self.assertEqual([record["seq"] for record in state.records], [1, 2, 3, 4])
        self.assertEqual(
            [record["stage"] for record in state.records],
            ["normalize", "allocate", "assemble", "validate"],
        )
        for record in state.records:
            self.assertEqual(record["stage_seq"], 1)
            self.assertEqual(len(record["input_shards"]), 3)
            self.assertEqual(len(record["input_shard_fingerprint"]), 64)
            self.assertEqual(len(record["config_version"]), 64)
            self.assertEqual(len(record["artifact_digest"]), 64)
            self.assertIn(record["stage"], record["artifact_path"])
            self.assertTrue((workspace.state_dir / record["artifact_path"]).is_file())
        self.assertFalse(list(workspace.checkpoint_dir.glob("*.tmp-*")))
        status = engine.status()
        self.assertEqual(status["checkpoint_sequence"]["seqs"], [1, 2, 3, 4])
        self.assertTrue(status["checkpoint_sequence"]["continuous"])
        self.assertIsNone(status["invalidation"])

    def test_corrupt_latest_checkpoint_is_ignored_and_rebuilt(self) -> None:
        workspace = _new_workspace(self.temp)
        engine = PipelineEngine(workspace)
        engine.run(force=True)
        latest = workspace.checkpoint_files()[-1]
        latest.write_text("{broken", encoding="utf-8")
        status = engine.status()
        self.assertEqual(status["checkpoint_sequence"]["current_seq"], 3)
        self.assertEqual(status["invalidation"]["earliest_stage"], "validate")
        self.assertTrue(status["checkpoint_sequence"]["issues"])
        result = engine.run()
        self.assertEqual(result["invalidated_from"], "validate")
        self.assertEqual(result["checkpoint_seq_after"], 4)
        self.assertEqual(engine.verify_incremental(run_if_needed=False)["status"], "consistent")

    def test_corrupt_artifact_is_quarantined_and_repaired(self) -> None:
        workspace = _new_workspace(self.temp)
        engine = PipelineEngine(workspace)
        engine.run(force=True)
        state = engine.store.load()
        normalize = state.item("normalize")
        artifact_path = workspace.state_dir / normalize["artifact_path"]
        artifact_path.write_text("{not-json", encoding="utf-8")
        status = engine.status()
        self.assertEqual(status["checkpoint_sequence"]["current_seq"], 0)
        self.assertEqual(status["invalidation"]["earliest_stage"], "normalize")
        result = engine.run()
        self.assertEqual(result["invalidated_from"], "normalize")
        self.assertEqual(result["checkpoint_seq_after"], 4)
        self.assertEqual(engine.verify_incremental(run_if_needed=False)["status"], "consistent")
        quarantined = list((workspace.quarantine_dir / "artifacts" / "normalize").glob("*.json"))
        self.assertEqual(len(quarantined), 1)

    def test_duplicate_checkpoint_sequence_is_quarantined_and_rebuilt(self) -> None:
        workspace = _new_workspace(self.temp)
        engine = PipelineEngine(workspace)
        engine.run(force=True)
        files = workspace.checkpoint_files()
        shutil.copyfile(files[1], workspace.checkpoint_dir / "duplicate-seq-2.json")
        status = engine.status()
        self.assertEqual(status["checkpoint_sequence"]["current_seq"], 1)
        self.assertTrue(
            any(issue.get("reason") == "duplicate_seq" for issue in status["checkpoint_sequence"]["issues"])
        )
        result = engine.run()
        self.assertEqual(result["invalidated_from"], "allocate")
        self.assertEqual(
            [item["stage"] for item in result["rebuilt_stages"]],
            ["allocate", "assemble", "validate"],
        )
        repaired = engine.status()
        self.assertEqual(repaired["checkpoint_sequence"]["seqs"], [1, 2, 3, 4])
        self.assertEqual(repaired["checkpoint_sequence"]["issues"], [])
        self.assertEqual(engine.verify_incremental(run_if_needed=False)["status"], "consistent")

    def test_incomplete_checkpoint_is_ignored_without_breaking_current_state(self) -> None:
        workspace = _new_workspace(self.temp)
        engine = PipelineEngine(workspace)
        engine.run(force=True)
        (workspace.checkpoint_dir / "incomplete.json").write_text("{}", encoding="utf-8")
        status = engine.status()
        self.assertEqual(status["checkpoint_sequence"]["current_seq"], 4)
        self.assertIsNone(status["invalidation"])
        self.assertTrue(
            any(issue.get("reason") == "incomplete_record" for issue in status["checkpoint_sequence"]["issues"])
        )
        self.assertEqual(engine.run()["status"], "up_to_date")

    def test_cross_stage_misaligned_checkpoint_is_rejected(self) -> None:
        workspace = _new_workspace(self.temp)
        engine = PipelineEngine(workspace)
        engine.run(force=True)
        state = engine.store.load()
        for path in workspace.checkpoint_files()[1:]:
            path.unlink()
        record = record_without_private(state.item("validate"))
        record["seq"] = 2
        record["stage_seq"] = 1
        record["previous_record_digest"] = state.item("normalize")["record_digest"]
        record.pop("record_checksum", None)
        record["record_checksum"] = sha256_obj(record)
        (workspace.checkpoint_dir / "checkpoint-000000000002.json").write_bytes(
            canonical_json_line(record)
        )
        status = engine.status()
        self.assertEqual(status["checkpoint_sequence"]["current_seq"], 1)
        self.assertTrue(
            any(
                issue.get("reason") == "stage_dependency_mismatch"
                for issue in status["checkpoint_sequence"]["issues"]
            )
        )
        result = engine.run()
        self.assertEqual(
            [item["stage"] for item in result["rebuilt_stages"]],
            ["allocate", "assemble", "validate"],
        )
        self.assertEqual(engine.verify_incremental(run_if_needed=False)["status"], "consistent")

    def test_stop_resume_is_byte_identical_to_full_build(self) -> None:
        baseline_engine = PipelineEngine(_new_workspace(self.temp, "baseline"))
        baseline_engine.run(force=True)

        workspace = _new_workspace(self.temp, "resumed")
        engine = PipelineEngine(workspace)
        engine.run_stage("normalize")
        with self.assertRaises(PipelineError) as caught:
            engine.run(stop_after_samples=1)
        self.assertEqual(caught.exception.code, "E_STOPPED")
        self.assertEqual(caught.exception.details["stage_interrupted"], "allocate")
        self.assertEqual(caught.exception.details["checkpoint_seq"], 1)
        self.assertFalse((workspace.artifact_dir / "allocate").exists())
        resumed = engine.resume()
        self.assertEqual(resumed["checkpoint_seq_after"], 4)
        self.assertEqual([item["seq"] for item in resumed["rebuilt_stages"]], [2, 3, 4])
        self.assertEqual(
            resumed["digests"]["keep_list"], baseline_engine.report()["digests"]["keep_list"]
        )
        self.assertEqual(
            resumed["digests"]["allocation_table"],
            baseline_engine.report()["digests"]["allocation_table"],
        )
        self.assertEqual(resumed["digests"]["mixed_set"], baseline_engine.report()["digests"]["mixed_set"])
        self.assertEqual(engine.verify_incremental(run_if_needed=False)["status"], "consistent")
        self.assertEqual(engine.status()["checkpoint_sequence"]["seqs"], [1, 2, 3, 4])

    def test_pending_stop_request_requires_resume(self) -> None:
        workspace = _new_workspace(self.temp)
        engine = PipelineEngine(workspace)
        engine.run(force=True)
        workspace.request_stop("unit-test")
        with self.assertRaises(PipelineError) as caught:
            engine.run()
        self.assertEqual(caught.exception.code, "E_STOP_REQUESTED")
        self.assertEqual(engine.resume()["status"], "up_to_date")
        self.assertFalse(workspace.stop_requested())

    def test_incremental_weight_hamming_and_estimator_invalidation(self) -> None:
        workspace = _new_workspace(self.temp)
        engine = PipelineEngine(workspace)
        engine.run(force=True)

        config = workspace.load_config()
        weights = dict(config["allocation"]["weights"])
        weights[make_layer_id("web", "zh")] = 6.0
        engine.change_config(set_values={"allocation.weights": weights})
        status = engine.status()
        self.assertEqual(status["invalidation"]["earliest_stage"], "allocate")
        self.assertTrue(
            any(reason.get("component") == "weights" for reason in status["invalidation"]["reasons"])
        )
        result = engine.run()
        self.assertEqual(result["invalidated_from"], "allocate")
        self.assertEqual(result["reused_stages"], ["normalize"])
        self.assertEqual(
            [item["stage"] for item in result["rebuilt_stages"]],
            ["allocate", "assemble", "validate"],
        )
        self.assertEqual(result["checkpoint_seq_after"], 7)
        self.assertEqual(engine.status()["stages"]["normalize"]["seq"], 1)
        self.assertEqual(engine.verify_incremental(run_if_needed=False)["status"], "consistent")

        engine.change_config(set_values={"dedup.hamming_threshold": 6})
        status = engine.status()
        self.assertEqual(status["invalidation"]["earliest_stage"], "normalize")
        self.assertTrue(
            any(
                reason.get("component") == "hamming_threshold"
                for reason in status["invalidation"]["reasons"]
            )
        )
        result = engine.run()
        self.assertEqual(result["reused_stages"], [])
        self.assertEqual(result["checkpoint_seq_after"], 11)
        self.assertEqual(engine.status()["stages"]["normalize"]["stage_seq"], 2)
        self.assertEqual(engine.verify_incremental(run_if_needed=False)["status"], "consistent")

        engine.change_config(set_values={"allocation.token_estimator": "whitespace_plus_cjk"})
        self.assertEqual(engine.status()["invalidation"]["earliest_stage"], "allocate")
        result = engine.run()
        self.assertEqual(result["invalidated_from"], "allocate")
        self.assertEqual(result["reused_stages"], ["normalize"])
        self.assertEqual(result["checkpoint_seq_after"], 14)
        self.assertEqual(engine.verify_incremental(run_if_needed=False)["status"], "consistent")

    def test_single_shard_replacement_rebuilds_from_normalize(self) -> None:
        workspace = _new_workspace(self.temp)
        engine = PipelineEngine(workspace)
        engine.run(force=True)
        old_normalize = engine.status()["stages"]["normalize"]["artifact_digest"]

        shard = workspace.shards_dir / "shard-002.jsonl"
        replacement = self.temp / "replacement-shard-002.jsonl"
        replacement.write_bytes(
            shard.read_bytes()
            + canonical_json_line(
                {
                    "id": "demo-9001",
                    "source": "web",
                    "lang": "zh",
                    "text": "新增样本用于验证单分片替换后的确定性增量重建路径。",
                }
            )
        )
        change = engine.replace_shard("shard-002.jsonl", replacement)
        self.assertTrue(change["changed"])
        status = engine.status()
        self.assertEqual(status["invalidation"]["earliest_stage"], "normalize")
        result = engine.run()
        self.assertEqual(result["invalidated_from"], "normalize")
        self.assertEqual(result["reused_stages"], [])
        self.assertNotEqual(engine.status()["stages"]["normalize"]["artifact_digest"], old_normalize)
        self.assertEqual(engine.verify_incremental(run_if_needed=False)["status"], "consistent")

    def test_upstream_rebuild_invalidates_downstream_even_with_same_product_digest(self) -> None:
        workspace = _new_workspace(self.temp)
        engine = PipelineEngine(workspace)
        engine.run(force=True)
        old_normalize = engine.status()["stages"]["normalize"]["artifact_digest"]

        shard = workspace.shards_dir / "shard-003.jsonl"
        replacement = self.temp / "byte-only-change.jsonl"
        replacement.write_bytes(shard.read_bytes() + b"\n\n")
        engine.replace_shard("shard-003.jsonl", replacement)
        staged = engine.run_stage("normalize")
        self.assertEqual(staged["status"], "rebuilt")
        self.assertEqual(staged["artifact_digest"], old_normalize)
        status = engine.status()
        self.assertEqual(status["stages"]["normalize"]["status"], "ready")
        self.assertEqual(status["invalidation"]["earliest_stage"], "allocate")
        result = engine.run()
        self.assertEqual(result["reused_stages"], ["normalize"])
        self.assertEqual(
            [item["stage"] for item in result["rebuilt_stages"]],
            ["allocate", "assemble", "validate"],
        )
        self.assertEqual(result["checkpoint_seq_after"], 8)
        self.assertEqual(engine.verify_incremental(run_if_needed=False)["status"], "consistent")

    def test_replacing_missing_shard_returns_not_found(self) -> None:
        workspace = _new_workspace(self.temp)
        engine = PipelineEngine(workspace)
        replacement = self.temp / "missing.jsonl"
        replacement.write_text("", encoding="utf-8")
        with self.assertRaises(PipelineError) as caught:
            engine.replace_shard("does-not-exist.jsonl", replacement)
        self.assertEqual(caught.exception.code, "E_NOT_FOUND")

    def test_invalid_stage_jump_and_missing_prerequisites(self) -> None:
        workspace = _new_workspace(self.temp)
        engine = PipelineEngine(workspace)
        with self.assertRaises(PipelineError) as caught:
            engine.run_stage("allocate")
        self.assertEqual(caught.exception.code, "E_INVALID_STAGE")
        with self.assertRaises(PipelineError) as caught:
            engine.run_stage("validate")
        self.assertEqual(caught.exception.code, "E_INVALID_STAGE")
        with self.assertRaises(PipelineError) as caught:
            engine.run_stage("not-a-stage")
        self.assertEqual(caught.exception.code, "E_INVALID_STAGE")

        store = CheckpointStore(workspace)
        with self.assertRaises(PipelineError) as caught:
            store.publish(
                stage="assemble",
                payload={"x": 1},
                stage_fingerprint="a" * 64,
                fingerprint_components={},
                input_shards=[],
                input_shard_fingerprint=sha256_obj([]),
                config_version=sha256_obj({}),
                config_revision=0,
                expected_seq=1,
            )
        self.assertEqual(caught.exception.code, "E_INVALID_STAGE")

    def test_concurrent_publishers_one_wins_and_loser_cannot_overwrite(self) -> None:
        if "fork" not in mp.get_all_start_methods():
            self.skipTest("requires fork start method")
        workspace = Workspace(self.temp / "race")
        workspace.initialize()
        ctx = mp.get_context("fork")
        barrier = ctx.Barrier(2)
        queue = ctx.Queue()
        processes = [
            ctx.Process(
                target=_race_publish,
                args=(str(workspace.root), barrier, queue, worker_id, 1),
            )
            for worker_id in (10, 20)
        ]
        for process in processes:
            process.start()
        results = [queue.get(timeout=10), queue.get(timeout=10)]
        for process in processes:
            process.join(timeout=10)
            self.assertEqual(process.exitcode, 0)
        successes = [item for item in results if item[0] == "ok"]
        failures = [item for item in results if item[0] == "error"]
        self.assertEqual(len(successes), 1)
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0][2], "E_STALE_CHECKPOINT")
        winner = successes[0]
        artifact_files = list((workspace.artifact_dir / "normalize").glob("*.json"))
        self.assertEqual(len(artifact_files), 1)
        self.assertEqual(artifact_files[0].stem, winner[3])
        self.assertEqual(CheckpointStore(workspace).load().current_seq, 1)

    def test_cross_stage_publish_mismatch_is_stale(self) -> None:
        workspace = Workspace(self.temp / "cross")
        workspace.initialize()
        store = CheckpointStore(workspace)
        comps = {"algorithm": "test"}
        record = store.publish(
            stage="normalize",
            payload={"kept": []},
            stage_fingerprint=sha256_obj(comps),
            fingerprint_components=comps,
            input_shards=[],
            input_shard_fingerprint=sha256_obj([]),
            config_version=sha256_obj({}),
            config_revision=0,
            expected_seq=1,
        )
        with self.assertRaises(PipelineError) as caught:
            store.publish(
                stage="allocate",
                payload={"layers": []},
                stage_fingerprint="b" * 64,
                fingerprint_components={"normalized_artifact_digest": "0" * 64},
                input_shards=[],
                input_shard_fingerprint=sha256_obj([]),
                config_version=sha256_obj({}),
                config_revision=0,
                expected_seq=2,
            )
        self.assertEqual(caught.exception.code, "E_STALE_CHECKPOINT")
        self.assertEqual(CheckpointStore(workspace).load().current_seq, 1)
        self.assertFalse((workspace.artifact_dir / "allocate").exists())
        self.assertEqual(record["seq"], 1)

    def test_stage_by_stage_execution_matches_full_build(self) -> None:
        baseline = PipelineEngine(_new_workspace(self.temp, "baseline"))
        baseline.run(force=True)
        engine = PipelineEngine(_new_workspace(self.temp, "staged"))
        published = []
        for stage in ("normalize", "allocate", "assemble", "validate"):
            result = engine.run_stage(stage)
            self.assertEqual(result["status"], "rebuilt")
            published.append(result["checkpoint_seq"])
        self.assertEqual(published, [1, 2, 3, 4])
        self.assertEqual(engine.report()["digests"], baseline.report()["digests"])
        self.assertEqual(engine.mixed_bytes(), baseline.mixed_bytes())
        self.assertEqual(engine.verify_incremental(run_if_needed=False)["status"], "consistent")

    def test_incremental_mismatch_error_has_first_layer_id(self) -> None:
        workspace = _new_workspace(self.temp)
        engine = PipelineEngine(workspace)
        engine.run(force=True)
        original = engine_module.allocate

        def altered(normalized_payload, config, **kwargs):
            payload = original(normalized_payload, config, **kwargs)
            payload["layers"][0]["weight"] += 1.0
            payload["allocation_digest"] = sha256_bytes(allocation_product_bytes(payload))
            return payload

        with mock.patch.object(engine_module, "allocate", side_effect=altered):
            with self.assertRaises(PipelineError) as caught:
                engine.verify_incremental(run_if_needed=False)
        self.assertEqual(caught.exception.code, "E_INCREMENTAL_MISMATCH")
        difference = caught.exception.details["first_difference"]
        self.assertEqual(difference["product"], "allocation_table")
        self.assertEqual(difference["kind"], "layer")
        self.assertEqual(difference["id"], "archive::zh")

    def test_incremental_mismatch_error_has_first_sample_id(self) -> None:
        workspace = _new_workspace(self.temp)
        engine = PipelineEngine(workspace)
        engine.run(force=True)
        original = engine_module.deduplicate

        def altered(records, config, **kwargs):
            payload = original(records, config, **kwargs)
            payload["kept"][0]["text"] += " 不兼容修改"
            payload["keep_list_digest"] = sha256_bytes(record_keep_product_bytes(payload))
            return payload

        with mock.patch.object(engine_module, "deduplicate", side_effect=altered):
            with self.assertRaises(PipelineError) as caught:
                engine.verify_incremental(run_if_needed=False)
        self.assertEqual(caught.exception.code, "E_INCREMENTAL_MISMATCH")
        difference = caught.exception.details["first_difference"]
        self.assertEqual(difference["product"], "keep_list")
        self.assertEqual(difference["id"], "demo-0001")

    def test_http_api_end_to_end(self) -> None:
        workspace = _new_workspace(self.temp)
        handler_class = make_handler(workspace)

        class QuietHandler(handler_class):
            def log_message(self, fmt: str, *args) -> None:
                return

        server = ThreadingHTTPServer(("127.0.0.1", 0), QuietHandler)
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{server.server_address[1]}"

        def request(path: str, body: dict | None = None, raw: bool = False):
            data = None if body is None else json.dumps(body).encode("utf-8")
            req = urllib.request.Request(
                base + path,
                data=data,
                headers={"Content-Type": "application/json"} if data is not None else {},
            )
            try:
                with urllib.request.urlopen(req, timeout=20) as response:
                    payload = response.read()
                    return response.status, payload if raw else json.loads(payload)
            except urllib.error.HTTPError as exc:
                try:
                    return exc.code, json.loads(exc.read())
                finally:
                    exc.close()

        try:
            self.assertEqual(request("/health")[0], 200)
            status, payload = request("/api/status")
            self.assertEqual(status, 200)
            self.assertEqual(payload["invalidation"]["earliest_stage"], "normalize")
            status, run = request("/api/run", {})
            self.assertEqual(status, 200)
            self.assertEqual(run["checkpoint_seq_after"], 4)
            status, exported = request("/api/export/mixed", raw=True)
            self.assertEqual(status, 200)
            self.assertEqual(len(exported), 3255)
            status, verification = request("/api/verify-incremental", {})
            self.assertEqual(status, 200)
            self.assertEqual(verification["status"], "consistent")
            status, changed = request(
                "/api/config/change",
                {"set": {"assembly.seed": "http-seed-v2"}, "run": True},
            )
            self.assertEqual(status, 200)
            self.assertEqual(changed["run"]["invalidated_from"], "assemble")
            self.assertEqual(changed["run"]["checkpoint_seq_after"], 6)
            status, report = request("/api/report")
            self.assertEqual(status, 200)
            self.assertEqual(report["status"], "ok")
            status, report_bytes = request("/api/export/report", raw=True)
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(report_bytes)["status"], "ok")
            status, error = request("/api/stages/allocate/run", {})
            self.assertEqual(status, 200)
            self.assertEqual(error["status"], "up_to_date")
            status, invalid = request("/api/stages/not-a-stage")
            self.assertEqual(status, 409)
            self.assertEqual(invalid["code"], "E_INVALID_STAGE")

            status, prechange = request(
                "/api/config/change",
                {"set": {"assembly.seed": "stop-test-v3"}, "run": False},
            )
            self.assertEqual(status, 200)
            self.assertEqual(
                request("/api/status")[1]["invalidation"]["earliest_stage"],
                "assemble",
            )
            run_holder: dict = {}

            def run_with_delay() -> None:
                run_holder["result"] = request(
                    "/api/run", {"delay_ms": 30}
                )

            worker = threading.Thread(target=run_with_delay)
            worker.start()
            threading.Event().wait(0.2)
            status, stop = request("/api/stop", {"reason": "http-test"})
            self.assertEqual(status, 200)
            self.assertTrue(stop["stop"]["requested"])
            worker.join(timeout=10)
            stopped_status, stopped = run_holder["result"]
            self.assertEqual(stopped_status, 202)
            self.assertEqual(stopped["code"], "E_STOPPED")
            status, resumed = request("/api/resume", {})
            self.assertEqual(status, 200)
            self.assertEqual(resumed["status"], "rebuilt")
            self.assertEqual(resumed["checkpoint_seq_after"], 8)
            self.assertEqual(
                [item["stage"] for item in resumed["rebuilt_stages"]],
                ["assemble", "validate"],
            )
            self.assertFalse(request("/api/status")[1]["stop"]["requested"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_bundled_demo_matches_expected_digest(self) -> None:
        expected = json.loads((DEMO_DIR / "expected.json").read_text(encoding="utf-8"))
        engine = PipelineEngine(_new_workspace(self.temp))
        run = engine.run(force=True)
        verification = engine.verify_incremental(run_if_needed=False)
        stage_digests = {
            item["stage"]: item["artifact_digest"] for item in run["rebuilt_stages"]
        }
        self.assertEqual(stage_digests, expected["stage_digests"])
        self.assertEqual(
            {name: details["incremental"] for name, details in verification["products"].items()},
            expected["product_digests"],
        )
        self.assertEqual(
            {name: details["incremental_bytes"] for name, details in verification["products"].items()},
            expected["product_bytes"],
        )
        self.assertEqual(engine.report()["counts"], expected["counts"])
        self.assertEqual(engine.report()["empty_layers"], expected["empty_layers"])

    def test_cli_demo_and_config_change(self) -> None:
        workspace_path = self.temp / "cli-workspace"
        command = [
            sys.executable,
            str(ROOT / "pipeline.py"),
            "demo",
            "--workspace",
            str(workspace_path),
            "--force",
        ]
        completed = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        payload = json.loads(completed.stdout)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["verification"]["status"], "consistent")
        self.assertEqual(payload["run"]["checkpoint_seq_after"], 4)

        command = [
            sys.executable,
            str(ROOT / "pipeline.py"),
            "config-change",
            "--workspace",
            str(workspace_path),
            "--set",
            "allocation.target_samples=20",
        ]
        completed = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        payload = json.loads(completed.stdout)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["run"]["invalidated_from"], "allocate")
        self.assertEqual(payload["run"]["reused_stages"], ["normalize"])

        report_path = self.temp / "cli-report.json"
        command = [
            sys.executable,
            str(ROOT / "pipeline.py"),
            "report",
            "--workspace",
            str(workspace_path),
            "--output",
            str(report_path),
        ]
        completed = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr + completed.stdout)
        exported = json.loads(completed.stdout)
        self.assertTrue(exported["ok"])
        self.assertEqual(json.loads(report_path.read_text(encoding="utf-8"))["status"], "ok")


if __name__ == "__main__":
    unittest.main(verbosity=2)
