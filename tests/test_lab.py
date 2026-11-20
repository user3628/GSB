from __future__ import annotations

import json
import math
import multiprocessing
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

import yaml

from recall_lab.budget import decide_recall
from recall_lab.checkpoint import CheckpointStore
from recall_lab.config import load_config
from recall_lab.data import InputBundle, Item, ManifestEntry, Query, load_input_bundle, load_manifest
from recall_lab.engine import Lab
from recall_lab.errors import BusinessError, ErrorCode
from recall_lab.generate import generate_dataset
from recall_lab.http_api import LabHTTPServer, LabService
from recall_lab.recall import RecallEngine
from recall_lab.util import atomic_write_bytes, canonical_json_bytes, digest_bytes


ROOT = Path(__file__).resolve().parents[1]


def _publish_worker(
    root: str,
    workspace_id: str,
    artifact_digest: str,
    basis_sequence: int,
    started,
    queue,
) -> None:
    store = CheckpointStore(Path(root), workspace_id)
    started.wait()
    try:
        record = store.publish(
            "small",
            "recall",
            input_fingerprint="1" * 64,
            config_version="2" * 64,
            artifact_digest=artifact_digest,
            basis_sequence=basis_sequence,
        )
        queue.put(("ok", record["sequence"]))
    except BusinessError as exc:
        queue.put((exc.code.value, exc.details.get("reason")))


class LabTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.TemporaryDirectory(prefix="recall-lab-tests-")
        cls.base = Path(cls.tmp.name)
        cls.data_dir = cls.base / "data"
        cls.manifest_path = generate_dataset(cls.data_dir, seed=20261120)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.tmp.cleanup()

    def workspace(self, name: str, config_name: str = "fusion.yaml") -> Lab:
        root = self.base / "workspaces" / name
        return Lab.initialize(root, self.manifest_path, ROOT / "configs" / config_name, force=True)

    def alternative_config(self, name: str, weights: dict[str, float], strategy: str = "weighted_sum") -> Path:
        raw = yaml.safe_load((ROOT / "configs" / "fusion.yaml").read_text(encoding="utf-8"))
        raw["fusion"]["weights"] = weights
        raw["fusion"]["strategy"] = strategy
        path = self.base / "configs" / f"{name}.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(raw, sort_keys=False, allow_unicode=True), encoding="utf-8")
        return path


class GeneratorAndDecisionTests(LabTestBase):
    def test_generated_manifest_and_mixed_queries(self) -> None:
        rows = [json.loads(line) for line in self.manifest_path.read_text(encoding="utf-8").splitlines()]
        self.assertEqual([row["scale"] for row in rows], ["small", "medium", "large"])
        self.assertEqual([row["item_count"] for row in rows], [200, 2000, 12000])
        self.assertEqual([row["query_count"] for row in rows], [12, 24, 48])
        small_queries = [
            json.loads(line)
            for line in (self.data_dir / "small" / "queries.jsonl").read_text(encoding="utf-8").splitlines()
        ]
        texts = [row["query_text"] for row in small_queries]
        self.assertTrue(any(any(ord(char) > 127 for char in text) for text in texts))
        self.assertTrue(any(any("a" <= char.lower() <= "z" for char in text) for text in texts))
        self.assertTrue(all(len(row["reference_item_ids"]) == 10 for row in small_queries))

    def test_budget_decisions_respect_all_caps_and_disable_large_vector(self) -> None:
        lab = self.workspace("budget-default")
        status = lab.status()
        default = {item["scale"]: item["decision"] for item in status["scales"]}
        self.assertEqual(set(default["small"]["enabled_sources"]), {"lexical", "category", "vector"})
        self.assertEqual(set(default["medium"]["enabled_sources"]), {"lexical", "category", "vector"})
        self.assertEqual(default["large"]["enabled_sources"], ["lexical", "category"])
        self.assertEqual(default["large"]["routes"]["vector"]["reason"], "DISABLED_GLOBAL_BUDGET")
        for scale, decision in default.items():
            self.assertLessEqual(decision["used_evaluations"], decision["global_budget"])
            lab_config = lab.config
            for source, route in decision["routes"].items():
                self.assertLessEqual(route["planned_evaluations"], lab_config.sources[source].quota)
                if route["enabled"]:
                    self.assertGreater(route["effective_depth"], 0)

        tight = Lab(lab.root, config_path=ROOT / "configs" / "budget-tight.yaml")
        tight_large = tight.decision("large")
        self.assertEqual(tight_large["enabled_sources"], ["lexical"])
        self.assertLessEqual(tight_large["used_evaluations"], tight_large["global_budget"])
        self.assertIn("DISABLED_GLOBAL_BUDGET", {tight_large["routes"]["category"]["reason"], tight_large["routes"]["vector"]["reason"]})


class RecallAndFusionTests(LabTestBase):
    def test_three_routes_sorted_and_cross_route_dedup(self) -> None:
        lab = self.workspace("routes")
        result = lab.run(scales=["small"])
        self.assertEqual(result["status"], "completed")
        recall = lab.artifact("small", "recall")
        found_cross_route_duplicate = False
        for query in recall["queries"]:
            route_sets: list[set[str]] = []
            for source in ("lexical", "category", "vector"):
                route = query["routes"][source]
                self.assertTrue(route["enabled"])
                scores = [candidate["score"] for candidate in route["candidates"]]
                self.assertEqual(scores, sorted(scores, reverse=True))
                self.assertTrue(all(math.isfinite(score) for score in scores))
                route_sets.append({candidate["item_id"] for candidate in route["candidates"]})
            union = set().union(*route_sets)
            if sum(len(route_set) for route_set in route_sets) > len(union):
                found_cross_route_duplicate = True
        self.assertTrue(found_cross_route_duplicate)

        fusion = lab.artifact("small", "fusion")
        self.assertGreater(sum(query["merged_duplicate_count"] for query in fusion["queries"]), 0)
        for query in fusion["queries"]:
            item_ids = [item["item_id"] for item in query["fused"]]
            self.assertEqual(len(item_ids), len(set(item_ids)))
            for item in query["fused"]:
                expected = math.fsum(source["contribution"] for source in item["sources"])
                self.assertAlmostEqual(item["fused_score"], expected, places=12)
                source_order = ["lexical", "category", "vector"]
                self.assertEqual(
                    [source["source"] for source in item["sources"]],
                    sorted((source["source"] for source in item["sources"]), key=source_order.index),
                )

    def test_empty_input_and_numeric_errors(self) -> None:
        config = load_config(ROOT / "configs" / "fusion.yaml")
        entry = ManifestEntry("x", "items.jsonl", "queries.jsonl", 1, 1, 10, 1)
        decision = decide_recall(config, scale="x", item_count=1, query_count=1)
        item = Item("i1", "books", "novel", (1.0, 0.0))
        query = Query("q1", "", "zh-CN", "search", ("books",), (1.0, 0.0), ("i1",))
        bundle = InputBundle(entry, (item,), (query,), "a" * 64, "b" * 64, Path("i"), Path("q"))
        with self.assertRaises(BusinessError) as ctx:
            RecallEngine(bundle).recall_all(decision)
        self.assertEqual(ctx.exception.code, ErrorCode.EMPTY_INPUT)
        self.assertEqual(ctx.exception.details["query_id"], "q1")

        empty_bundle = InputBundle(entry, (), (query,), "a" * 64, "b" * 64, Path("i"), Path("q"))
        empty_decision = decide_recall(config, scale="x", item_count=0, query_count=1)
        with self.assertRaises(BusinessError) as ctx:
            RecallEngine(empty_bundle).recall_all(empty_decision)
        self.assertEqual(ctx.exception.code, ErrorCode.EMPTY_INPUT)

        nonfinite_item = Item("i1", "books", "novel", (math.nan, 0.0))
        bad_bundle = InputBundle(entry, (nonfinite_item,), (query,), "a" * 64, "b" * 64, Path("i"), Path("q"))
        with self.assertRaises(BusinessError) as ctx:
            RecallEngine(bad_bundle)
        self.assertEqual(ctx.exception.code, ErrorCode.NUMERIC)
        self.assertEqual(ctx.exception.details["item_id"], "i1")

        empty_dir = self.base / "empty-load"
        empty_dir.mkdir()
        atomic_write_bytes(empty_dir / "items.jsonl", b"")
        atomic_write_bytes(
            empty_dir / "queries.jsonl",
            canonical_json_bytes(
                {
                    "query_id": "q-empty-library",
                    "query_text": "novel",
                    "locale": "en-US",
                    "channel": "search",
                    "history_categories": ["books"],
                    "query_features": [1.0, 0.0],
                }
            )
            + b"\n",
        )
        atomic_write_bytes(
            empty_dir / "manifest.jsonl",
            canonical_json_bytes(
                {
                    "scale": "empty",
                    "items_file": "items.jsonl",
                    "queries_file": "queries.jsonl",
                    "item_count": 0,
                    "query_count": 1,
                    "expected_candidate_depth": 10,
                    "seed": 1,
                }
            )
            + b"\n",
        )
        parsed_manifest = load_manifest(empty_dir / "manifest.jsonl")
        loaded_empty = load_input_bundle(parsed_manifest, parsed_manifest.entries[0])
        loaded_empty_decision = decide_recall(config, scale="empty", item_count=0, query_count=1)
        with self.assertRaises(BusinessError) as ctx:
            RecallEngine(loaded_empty).recall_all(loaded_empty_decision)
        self.assertEqual(ctx.exception.code, ErrorCode.EMPTY_INPUT)
        self.assertEqual(ctx.exception.details["query_id"], "q-empty-library")

    def test_weight_change_invalidates_only_fusion_downstream(self) -> None:
        lab = self.workspace("weights")
        lab.run(scales=["small"])
        before = lab.status(scales=["small"])["scales"][0]
        baseline_fusion = lab.artifact("small", "fusion")
        original_recall_sequence = before["stages"]["recall"]["checkpoint"]["sequence"]
        alt_path = self.alternative_config(
            "alt-weights",
            {"lexical": 2.5, "category": 0.1, "vector": 0.2},
        )
        changed = Lab(lab.root, config_path=alt_path)
        invalid = changed.status(scales=["small"])["scales"][0]
        self.assertEqual(invalid["earliest_invalid_stage"], "fusion")
        self.assertEqual(invalid["stages"]["recall"]["status"], "valid")
        self.assertEqual(invalid["stages"]["fusion"]["status"], "stale_input")
        changed.run(scales=["small"])
        after = changed.status(scales=["small"])["scales"][0]
        self.assertEqual(after["stages"]["recall"]["checkpoint"]["sequence"], original_recall_sequence)
        self.assertGreater(after["stages"]["fusion"]["checkpoint"]["sequence"], before["stages"]["fusion"]["checkpoint"]["sequence"])
        alt_fusion = changed.artifact("small", "fusion")
        changed_queries = sum(
            [item["item_id"] for item in left["fused"]] != [item["item_id"] for item in right["fused"]]
            for left, right in zip(baseline_fusion["queries"], alt_fusion["queries"])
        )
        self.assertGreater(changed_queries, 0)
        self.assertNotEqual(baseline_fusion["config_version"], alt_fusion["config_version"])

        rrf_path = self.alternative_config(
            "rrf",
            {"lexical": 1.0, "category": 1.0, "vector": 1.0},
            strategy="rrf",
        )
        rrf = Lab(lab.root, config_path=rrf_path)
        rrf.run_stage("small", "fusion")
        rrf_artifact = rrf.artifact("small", "fusion")
        self.assertEqual(rrf_artifact["strategy"], "rrf")
        first = rrf_artifact["queries"][0]["fused"][0]
        for source in first["sources"]:
            self.assertAlmostEqual(
                source["contribution"],
                source["weight"] / (rrf_artifact["fusion_config"]["rrf_k"] + source["rank"]),
                places=15,
            )


class PersistenceAndApiTests(LabTestBase):
    def test_budget_and_item_changes_invalidate_from_recall(self) -> None:
        lab = self.workspace("recall-invalidation")
        lab.run(scales=["small"])
        baseline = lab.status(scales=["small"])["scales"][0]
        tight = Lab(lab.root, config_path=ROOT / "configs" / "budget-tight.yaml")
        budget_changed = tight.status(scales=["small"])["scales"][0]
        self.assertEqual(budget_changed["earliest_invalid_stage"], "recall")
        self.assertEqual(budget_changed["stages"]["recall"]["status"], "stale_input")
        tight.run(scales=["small"])
        after_budget = tight.status(scales=["small"])["scales"][0]
        self.assertGreater(
            after_budget["stages"]["recall"]["checkpoint"]["sequence"],
            baseline["stages"]["recall"]["checkpoint"]["sequence"],
        )

        item_path = lab.root / "input" / "small" / "items.jsonl"
        rows = [json.loads(line) for line in item_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        rows[0]["title"] = rows[0]["title"] + " deterministic-edit"
        atomic_write_bytes(item_path, b"\n".join(canonical_json_bytes(row) for row in rows) + b"\n")
        edited = Lab(lab.root, config_path=ROOT / "configs" / "fusion.yaml")
        item_changed = edited.status(scales=["small"])["scales"][0]
        self.assertEqual(item_changed["earliest_invalid_stage"], "recall")
        self.assertNotEqual(
            budget_changed["version"]["content_digest"],
            item_changed["version"]["content_digest"],
        )

    def test_summary_is_byte_stable_and_scales_are_isolated(self) -> None:
        first = self.workspace("summary-a")
        second = self.workspace("summary-b")
        first.run(scales=["small"])
        first_small_digest = first.status(scales=["small"])["scales"][0]["stages"]["summary"]["checkpoint"]["artifact_digest"]
        first_bytes = (first.root / "exports" / "experiment-summary.json").read_bytes()
        second.run(scales=["small", "medium"])
        second_small_digest = second.status(scales=["small"])["scales"][0]["stages"]["summary"]["checkpoint"]["artifact_digest"]
        second_bytes = (second.root / "exports" / "experiment-summary.json").read_bytes()
        self.assertNotEqual(first_bytes, second_bytes)  # aggregate contains different scales
        self.assertEqual(first_small_digest, second_small_digest)
        self.assertEqual(first.export_summary(scales=["small"])["digest"], digest_bytes(first_bytes))
        self.assertEqual(first.export_summary(scales=["small"])["digest"], digest_bytes(first_bytes))

    def test_stop_resume_matches_uninterrupted_summary(self) -> None:
        full = self.workspace("resume-full")
        stopped = self.workspace("resume-stopped")
        full.run(scales=["small", "medium"])
        partial = stopped.run(scales=["small", "medium"], stop_after="medium:fusion")
        self.assertEqual(partial["status"], "stopped")
        self.assertEqual(partial["last"]["stage"], "fusion")
        resumed = stopped.resume(scales=["small", "medium"])
        self.assertEqual(resumed["status"], "completed")
        full_bytes = (full.root / "exports" / "experiment-summary.json").read_bytes()
        stopped_bytes = (stopped.root / "exports" / "experiment-summary.json").read_bytes()
        self.assertEqual(full_bytes, stopped_bytes)

    def test_invalid_stage_jump(self) -> None:
        lab = self.workspace("invalid-stage")
        with self.assertRaises(BusinessError) as ctx:
            lab.run_stage("small", "fusion")
        self.assertEqual(ctx.exception.code, ErrorCode.INVALID_STAGE)
        with self.assertRaises(BusinessError) as ctx:
            lab.run_stage("small", "summary")
        self.assertEqual(ctx.exception.code, ErrorCode.INVALID_STAGE)

    def test_checkpoint_tamper_rollback_and_scale_replacement(self) -> None:
        lab = self.workspace("tamper")
        lab.run(scales=["small"])
        state_path = lab.store.state_path("small")
        original_state = state_path.read_bytes()
        history_one = (lab.store.history_dir("small") / "00000001.json").read_bytes()

        tampered = json.loads(original_state)
        tampered["records"]["recall"]["artifact_digest"] = "f" * 64
        atomic_write_bytes(state_path, canonical_json_bytes(tampered))
        with self.assertRaises(BusinessError) as ctx:
            lab.status(scales=["small"])
        self.assertEqual(ctx.exception.code, ErrorCode.STALE_CHECKPOINT)
        self.assertEqual(ctx.exception.details["reason"], "STATE_DIGEST_MISMATCH")

        atomic_write_bytes(state_path, original_state)
        atomic_write_bytes(state_path, history_one)
        with self.assertRaises(BusinessError) as ctx:
            lab.status(scales=["small"])
        self.assertEqual(ctx.exception.code, ErrorCode.STALE_CHECKPOINT)
        self.assertEqual(ctx.exception.details["reason"], "SEQUENCE_ROLLBACK")

        atomic_write_bytes(state_path, original_state)
        with self.assertRaises(BusinessError) as ctx:
            lab.store.validate_checkpoint_file(state_path, expected_scale="medium")
        self.assertEqual(ctx.exception.code, ErrorCode.STALE_CHECKPOINT)
        self.assertEqual(ctx.exception.details["reason"], "SCALE_MISMATCH")

        other = self.workspace("tamper-other")
        with self.assertRaises(BusinessError) as ctx:
            other.store.validate_checkpoint_file(state_path, expected_scale="small")
        self.assertEqual(ctx.exception.code, ErrorCode.STALE_CHECKPOINT)
        self.assertIn(
            ctx.exception.details["reason"],
            {"WORKSPACE_REPLACEMENT", "STATE_DIGEST_MISMATCH", "RECORD_DIGEST_MISMATCH"},
        )

    def test_concurrent_publication_allows_only_one_owner(self) -> None:
        lab = self.workspace("concurrent")
        artifact = {"format": "test-artifact-v1", "scale": "small", "input_fingerprint": "1" * 64}
        data = canonical_json_bytes(artifact)
        digest = digest_bytes(data)
        path = lab.store.artifact_path("small", "recall", digest)
        atomic_write_bytes(path, data)

        context = multiprocessing.get_context("fork")
        started = context.Event()
        queue = context.Queue()
        processes = [
            context.Process(
                target=_publish_worker,
                args=(str(lab.root), lab.workspace_id, digest, 0, started, queue),
            )
            for _ in range(2)
        ]
        for process in processes:
            process.start()
        started.set()
        results = [queue.get(timeout=10) for _ in processes]
        for process in processes:
            process.join(timeout=10)
        successes = [result for result in results if result[0] == "ok"]
        failures = [result for result in results if result[0] == "E_STALE_CHECKPOINT"]
        self.assertEqual(len(successes), 1)
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0][1], "DUPLICATE_PUBLICATION")
        self.assertEqual(lab.store.load_state("small").sequence, 1)

        second_artifact = {"format": "test-artifact-v1", "scale": "small", "input_fingerprint": "3" * 64}
        second_data = canonical_json_bytes(second_artifact)
        second_digest = digest_bytes(second_data)
        atomic_write_bytes(lab.store.artifact_path("small", "recall", second_digest), second_data)
        with self.assertRaises(BusinessError) as ctx:
            lab.store.publish(
                "small",
                "recall",
                input_fingerprint="3" * 64,
                config_version="4" * 64,
                artifact_digest=second_digest,
                basis_sequence=0,
            )
        self.assertEqual(ctx.exception.code, ErrorCode.STALE_CHECKPOINT)
        self.assertEqual(ctx.exception.details["reason"], "PUBLICATION_BASIS_STALE")

    def test_http_status_stage_decision_and_stop(self) -> None:
        lab = self.workspace("http", config_name="fusion.yaml")
        lab.run(scales=["small"])
        service = LabService(root=lab.root, config_path=lab.config_path)
        server = LabHTTPServer(("127.0.0.1", 0), service)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        try:
            with urllib.request.urlopen(f"http://{host}:{port}/health", timeout=5) as response:
                self.assertEqual(response.status, 200)
            import_request = urllib.request.Request(
                f"http://{host}:{port}/import",
                data=json.dumps(
                    {
                        "manifest_path": str(self.manifest_path),
                        "config_path": str(ROOT / "configs" / "fusion.yaml"),
                        "force": True,
                    }
                ).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(import_request, timeout=10) as response:
                imported = json.loads(response.read())
                self.assertEqual(imported["data"]["scales"], ["small", "medium", "large"])
            with urllib.request.urlopen(f"http://{host}:{port}/scales/small/decision", timeout=5) as response:
                payload = json.loads(response.read())
                self.assertEqual(payload["data"]["enabled_sources"], ["lexical", "category", "vector"])
            request = urllib.request.Request(
                f"http://{host}:{port}/stop",
                data=json.dumps({"reason": "test"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=5) as response:
                self.assertEqual(response.status, 200)
            self.assertTrue(lab.stop_requested())
            lab.clear_stop()
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                urllib.request.urlopen(f"http://{host}:{port}/scales/small/artifact/bogus", timeout=5)
            self.assertEqual(ctx.exception.code, 409)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
