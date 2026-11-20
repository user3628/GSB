import json
import os
import tempfile
import unittest

import numpy as np
from PIL import Image

from ocrwb.checkpoint import CheckpointStore
from ocrwb.config import STAGES, WorkbenchConfig
from ocrwb.detection import detect_shard
from ocrwb.errors import (
    E_INCREMENTAL_MISMATCH,
    E_INVALID_STAGE,
    E_RECOGNIZE_INVALID,
    E_STALE_CHECKPOINT,
    InvalidStage,
    RecognizeInvalid,
    StaleCheckpoint,
)
from ocrwb.glyphs import glyph_grid, render_shard
from ocrwb.hashing import digest_json
from ocrwb.pipeline import Pipeline
from ocrwb.recognition import build_template_library, recognize_candidate
from ocrwb.samplegen.generator import generate_dataset


class DeterminismTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.mkdtemp()
        self.sample_a = os.path.join(self.temp, "a")
        self.sample_b = os.path.join(self.temp, "b")
        self.manifest_a = generate_dataset(self.sample_a)
        self.manifest_b = generate_dataset(self.sample_b)

    def test_dataset_is_byte_stable(self):
        self.assertEqual(
            json.dumps(self.manifest_a, sort_keys=True),
            json.dumps(self.manifest_b, sort_keys=True),
        )
        image_id = self.manifest_a["shards"][0]["image_id"]
        with open(os.path.join(self.sample_a, "images", image_id + ".png"), "rb") as handle:
            bytes_a = handle.read()
        with open(os.path.join(self.sample_b, "images", image_id + ".png"), "rb") as handle:
            bytes_b = handle.read()
        self.assertEqual(bytes_a, bytes_b)

    def test_detection_is_deterministic(self):
        config = WorkbenchConfig()
        shard = self.manifest_a["shards"][0]
        image = Image.open(os.path.join(self.sample_a, shard["path"]))
        first = detect_shard(image, config, shard["image_id"], "detect_balanced")
        second = detect_shard(image, config, shard["image_id"], "detect_balanced")
        self.assertEqual(digest_json(first), digest_json(second))

    def test_full_run_digest_is_stable(self):
        workspace_a = os.path.join(self.temp, "ws_a")
        workspace_b = os.path.join(self.temp, "ws_b")
        report_a = self._run_report(workspace_a, self.sample_a, self.manifest_a)
        report_b = self._run_report(workspace_b, self.sample_b, self.manifest_b)
        self.assertEqual(report_a["summary_digest"], report_b["summary_digest"])

    @staticmethod
    def _run_report(workspace, sample_dir, manifest):
        pipeline = Pipeline(workspace, sample_dir, WorkbenchConfig())
        pipeline.import_sample(manifest)
        pipeline.run()
        return pipeline.load_report()


class ErrorCodeTests(unittest.TestCase):
    def test_recognize_empty_library(self):
        config = WorkbenchConfig()
        image, _ = render_shard(40, 30, [])
        candidate = {"candidate_id": "x:cand:000", "pixel_box": {"x0": 1, "y0": 1, "x1": 20, "y1": 20}}
        with self.assertRaises(RecognizeInvalid) as context:
            recognize_candidate(image, candidate, {"size": 0, "characters": [], "grids": {}}, config)
        self.assertEqual(context.exception.code, E_RECOGNIZE_INVALID)
        self.assertEqual(context.exception.details["reason"], "empty_library")

    def test_recognize_size_mismatch(self):
        config = WorkbenchConfig()
        image, _ = render_shard(40, 30, [])
        candidate = {"candidate_id": "x:cand:000", "pixel_box": {"x0": 1, "y0": 1, "x1": 2, "y1": 2}}
        library = build_template_library(["A"], config)
        with self.assertRaises(RecognizeInvalid) as context:
            recognize_candidate(image, candidate, library, config)
        self.assertEqual(context.exception.details["reason"], "size_mismatch")

    def test_detect_invalid_box_is_retained_not_raised(self):
        config = WorkbenchConfig()
        image, _ = render_shard(48, 32, [{"text": "AB", "scale": 1, "tilt": 0,
                                         "band_color": (230, 230, 255), "x": 4, "y": 4}])
        result = detect_shard(image, config, "img", "detect_balanced")
        self.assertNotEqual(len(result["candidates"]), 0)
        for candidate in result["candidates"]:
            box = candidate["pixel_box"]
            self.assertTrue(box["x1"] > box["x0"] and box["y1"] > box["y0"])


class StrategyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.mkdtemp()
        self.sample = os.path.join(self.temp, "samples")
        self.manifest = generate_dataset(self.sample)
        self.workspace = os.path.join(self.temp, "ws")
        self.pipeline = Pipeline(self.workspace, self.sample, WorkbenchConfig())
        self.pipeline.import_sample(self.manifest)
        self.pipeline.run()

    def test_small_group_never_uses_heavy_tiers(self):
        for shard in self.manifest["shards"]:
            if shard["group"] != "small":
                continue
            decision = self.pipeline.store.load_artifact(shard["image_id"], "decision")["decision"]
            self.assertNotEqual(decision["detect_strategy"], "detect_deep")
            self.assertNotEqual(decision["recognize_strategy"], "recog_deep")

    def test_budgets_are_respected(self):
        for shard in self.manifest["shards"]:
            decision = self.pipeline.store.load_artifact(shard["image_id"], "decision")["decision"]
            self.assertLessEqual(decision["detect_cost"], decision["detect_budget"])
            self.assertLessEqual(decision["recognize_cost"], decision["recognize_budget"])

    def test_rejected_strategies_have_reasons(self):
        shard = next(item for item in self.manifest["shards"] if item["group"] == "small")
        options = self.pipeline.store.load_artifact(shard["image_id"], "decision")["decision"]
        for option in options["detect_options"] + options["recognize_options"]:
            if not option["eligible"]:
                self.assertTrue(option["rejected_reasons"])


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.mkdtemp()
        self.store = CheckpointStore(os.path.join(self.temp, "ws"))
        self.image_id = "shard_0000"
        self.payload = {"value": 1}

    def _publish(self, stage, sequence, upstream=None, payload=None):
        payload = payload or self.payload
        return self.store._publish_locked(
            self.image_id,
            stage,
            sequence,
            "input",
            "config",
            upstream,
            f"fp-{stage}",
            payload,
        )

    def test_stale_sequence_rejected(self):
        with self.store.shard_lock(self.image_id):
            self._publish("detect", 1)
            with self.assertRaises(StaleCheckpoint) as context:
                self._publish("decision", 5, upstream="fp-detect")
            self.assertEqual(context.exception.code, E_STALE_CHECKPOINT)

    def test_duplicate_stage_rejected(self):
        with self.store.shard_lock(self.image_id):
            self._publish("detect", 1)
            self._publish("decision", 2, upstream="fp-detect")
            with self.assertRaises(StaleCheckpoint):
                self._publish("decision", 3, upstream="fp-detect", payload={"value": 2})

    def test_invalid_stage_skip(self):
        with self.store.shard_lock(self.image_id):
            with self.assertRaises(InvalidStage) as context:
                self._publish("recognize", 1)
            self.assertEqual(context.exception.code, E_INVALID_STAGE)

    def test_unknown_stage(self):
        with self.assertRaises(InvalidStage):
            self.store.artifact_path(self.image_id, "nonexistent")

    def test_concurrent_publish_uses_lock(self):
        import fcntl

        path = os.path.join(self.store.locks_dir, f"{self.image_id}.lock")
        with self.store.shard_lock(self.image_id):
            # A second, independent file handle cannot take the lock while held.
            other = open(path, "w")
            try:
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(other.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                other.close()
        # After release the same non-blocking acquire succeeds.
        other = open(path, "w")
        try:
            fcntl.flock(other.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(other.fileno(), fcntl.LOCK_UN)
        finally:
            other.close()


class IncrementalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.mkdtemp()
        self.sample = os.path.join(self.temp, "samples")
        self.manifest = generate_dataset(self.sample)
        self.workspace = os.path.join(self.temp, "ws")
        self.pipeline = Pipeline(self.workspace, self.sample, WorkbenchConfig())
        self.pipeline.import_sample(self.manifest)
        self.pipeline.run()
        self.baseline = self.pipeline.load_report()

    def test_change_threshold_invalidates_detection_downstream(self):
        target = self.manifest["shards"][0]["image_id"]
        before = self.pipeline.store.read_journal(target)
        changed = WorkbenchConfig()
        object.__setattr__(changed, "binarize", type(changed.binarize)(threshold=120))
        pipeline = Pipeline(self.workspace, self.sample, changed)
        pipeline.store.atomic_write_json(pipeline.store.config_path, changed.to_dict())
        stats = pipeline.run()
        after = pipeline.store.read_journal(target)
        self.assertTrue(any(item["image_id"] == target for item in stats.invalidations))
        self.assertEqual([record["stage"] for record in after], list(STAGES[:-1]))
        # A binarization change legitimately changes the diagnostic output.
        self.assertNotEqual(pipeline.load_report()["summary_digest"], self.baseline["summary_digest"])
        changed_reasons = {
            reason
            for item in stats.invalidations
            if item["image_id"] == target
            for reason in item["reasons"]
        }
        self.assertIn("config_version_changed", changed_reasons)

    def test_pixel_change_only_affects_one_shard(self):
        target = self.manifest["shards"][0]
        image_path = os.path.join(self.sample, target["path"])
        image = Image.open(image_path)
        array = np.asarray(image).copy()
        array[0, 0] = (255 - array[0, 0][0], 0, 0)
        Image.fromarray(array).save(image_path, format="PNG")
        stats = self.pipeline.run()
        affected = {item["image_id"] for item in stats.invalidations}
        self.assertEqual(affected, {target["image_id"]})
        self.assertTrue(all(item["from_stage"] == "detect" for item in stats.invalidations))

    def test_manifest_truth_change_invalidates_detection_stage(self):
        target = self.manifest["shards"][0]
        target["truth"][0]["text"] = target["truth"][0]["text"] + "X"
        target["char_count"] = sum(len(item["text"]) for item in target["truth"])
        self.pipeline.store.atomic_write_json(self.pipeline.store.manifest_path, self.manifest)
        stats = self.pipeline.run()
        affected = [item for item in stats.invalidations if item["image_id"] == target["image_id"]]
        self.assertEqual(len(affected), 1)
        self.assertEqual(affected[0]["from_stage"], "detect")
        self.assertIn("input_pixel_or_truth_changed", affected[0]["reasons"])

    def test_budget_change_invalidates_from_detection(self):
        changed = WorkbenchConfig()
        object.__setattr__(
            changed,
            "budget",
            type(changed.budget)(detect_small=changed.budget.detect_small + 50),
        )
        pipeline = Pipeline(self.workspace, self.sample, changed)
        pipeline.store.atomic_write_json(pipeline.store.config_path, changed.to_dict())
        stats = pipeline.run()
        self.assertTrue(stats.invalidations)
        self.assertTrue(
            all(item["from_stage"] == "detect" for item in stats.invalidations)
        )
        reasons = {reason for item in stats.invalidations for reason in item["reasons"]}
        self.assertIn("config_version_changed", reasons)

    def test_incremental_matches_full_rebuild(self):
        target = self.manifest["shards"][0]["image_id"]
        changed = WorkbenchConfig()
        object.__setattr__(changed, "binarize", type(changed.binarize)(threshold=100))
        pipeline = Pipeline(self.workspace, self.sample, changed)
        pipeline.store.atomic_write_json(pipeline.store.config_path, changed.to_dict())
        pipeline.run()

        clean_workspace = os.path.join(self.temp, "clean")
        clean = Pipeline(clean_workspace, self.sample, changed)
        clean.import_sample(self.manifest)
        clean.run()
        incremental = pipeline.load_report()
        full = clean.load_report()
        self.assertEqual(incremental["summary_digest"], full["summary_digest"])
        # The verification helper must stay silent on matching reports.
        pipeline.verify_incremental_equals_full(full)

    def test_mismatch_helper_raises(self):
        bogus = json.loads(json.dumps(self.baseline))
        bogus["global"]["detection_recall"] = 0.12345
        with self.assertRaises(Exception) as context:
            self.pipeline.verify_incremental_equals_full(bogus)
        self.assertEqual(context.exception.code, E_INCREMENTAL_MISMATCH)


class StopResumeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.mkdtemp()
        self.sample = os.path.join(self.temp, "samples")
        self.manifest = generate_dataset(self.sample)
        self.workspace = os.path.join(self.temp, "ws")
        self.pipeline = Pipeline(self.workspace, self.sample, WorkbenchConfig())
        self.pipeline.import_sample(self.manifest)

    def test_stop_resume_matches_clean_run(self):
        self.pipeline.request_stop()
        stopped_stats = self.pipeline.run()
        self.assertTrue(stopped_stats.stopped)
        partial = self.pipeline.store.status()
        completed = sum(1 for shard in partial["shards"] if shard["published_stages"])
        self.assertGreaterEqual(completed, 1)
        self.assertLess(completed, len(self.manifest["shards"]))

        self.pipeline.clear_stop()
        self.pipeline.run()
        resumed_report = self.pipeline.load_report()

        clean_workspace = os.path.join(self.temp, "clean")
        clean = Pipeline(clean_workspace, self.sample, WorkbenchConfig())
        clean.import_sample(self.manifest)
        clean.run()
        self.assertEqual(resumed_report["summary_digest"], clean.load_report()["summary_digest"])


if __name__ == "__main__":
    unittest.main()
