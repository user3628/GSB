import os
import tempfile
import unittest

import numpy as np

from diarwb.audio import (SAMPLE_RATE, istft, read_wav, stft,
                          write_wav)
from diarwb.config import default_config
from diarwb.datagen import build_demo_dataset, generate_shard
from diarwb.embedding import embed_channel, embedding_bytes
from diarwb.errors import (EImportValidation, EInvalidConfig, EInvalidStage,
                           ELatencyBudget, EStaleSnapshot, EStaleVersion,
                           EStopped, WorkbenchError)
from diarwb.pipeline import STAGE_RECOGNITION, STAGE_SEPARATION
from diarwb.separation import separate_mixture
from diarwb.timeline import Segment, Timeline
from diarwb.workbench import Workbench, atomic_write_json, digest_json, read_json
from diarwb.workbench import Workbench as WB


class WorkspaceTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="diarwb-test-")
        build_demo_dataset(self.root)

    def bench(self):
        from diarwb.workbench import Workbench
        wb = Workbench(self.root, default_config())
        wb.import_workspace()
        return wb


class AudioTests(unittest.TestCase):
    def test_stft_roundtrip(self):
        rng = np.random.default_rng(7)
        x = rng.standard_normal(12345)
        y = istft(stft(x, 512, 256), 512, 256, len(x))
        self.assertEqual(x.shape, y.shape)
        self.assertLess(np.max(np.abs(x - y)), 1e-12)

    def test_wav_layout_and_determinism(self):
        samples, _ = generate_shard(123, 2)
        p1 = os.path.join(tempfile.mkdtemp(), "a.wav")
        p2 = os.path.join(tempfile.mkdtemp(), "b.wav")
        write_wav(p1, samples)
        write_wav(p2, samples)
        with open(p1, "rb") as f1, open(p2, "rb") as f2:
            self.assertEqual(f1.read(), f2.read())
        back = read_wav(p1)
        self.assertEqual(back.shape[0], SAMPLE_RATE * 8)


class SeparationTests(unittest.TestCase):
    def test_deterministic_identical_outputs(self):
        samples, _ = generate_shard(99, 3)
        cfg = default_config()
        a, energy_a, masks_a = separate_mixture(
            samples, 3, cfg.frame_size, cfg.hop_size,
            cfg.mask_iterations, cfg.mask_floor, "wav-test")
        b, energy_b, masks_b = separate_mixture(
            samples, 3, cfg.frame_size, cfg.hop_size,
            cfg.mask_iterations, cfg.mask_floor, "wav-test")
        np.testing.assert_array_equal(a, b)
        np.testing.assert_allclose(energy_a, energy_b)
        np.testing.assert_allclose(masks_a, masks_b)
        self.assertEqual(a.dtype, np.int16)
        self.assertEqual(a.shape, (3, samples.shape[0]))

    def test_embedding_byte_stability(self):
        rng = np.random.default_rng(5)
        channel = (rng.standard_normal(16000) * 1e4).astype(np.int16)
        cfg = default_config()
        v = embed_channel(channel, cfg.sample_rate, cfg.frame_size,
                          cfg.hop_size, cfg.n_mels, cfg.embedding_dim, 11)
        self.assertEqual(embedding_bytes(v), embedding_bytes(v))
        self.assertEqual(len(embedding_bytes(v)), cfg.embedding_dim * 2)


class TimelineTests(unittest.TestCase):
    def test_edits_and_invariants(self):
        tl = Timeline("s", 1000, [
            Segment("a", 0, 100, "S1"),
            Segment("b", 100, 200, "S1"),
            Segment("c", 200, 300, "S2"),
        ])
        split = tl.split("c", 250, "test")
        self.assertEqual(split.version, 1)
        self.assertEqual(len(split.segments), 4)
        merged = tl.merge_adjacent(["a", "b"], "test")
        self.assertEqual(merged.segments[0].end_ms, 200)
        renamed = tl.rename_speaker("S2", "X", "test")
        self.assertEqual(renamed.speakers(), ["S1", "X"])
        combined = tl.merge_speakers("S2", "S1", "test")
        self.assertEqual(combined.speakers(), ["S1"])
        with self.assertRaises(WorkbenchError):
            tl.split("a", 100, "test")  # boundary split illegal
        with self.assertRaises(WorkbenchError):
            tl.upsert_segment(Segment("z", 900, 1001, "S1"), "test")

    def test_monotonic_history(self):
        tl = Timeline("s", 1000, [Segment("a", 0, 100, "S1")])
        tl = tl.rename_speaker("S1", "B", "t1")
        tl = tl.rename_speaker("B", "C", "t2")
        self.assertEqual([h.version for h in tl.history], [1, 2])


class WorkbenchFlowTests(WorkspaceTest):
    def test_full_flow_incremental_and_queries(self):
        wb = self.bench()
        wb.run_all()
        self.assertEqual(wb.verify_consistency()["status"], "byte-identical")

        # Query results are cached then invalidated after an edit.
        first = wb.query("shard-000", "overlaps")
        second = wb.query("shard-000", "overlaps")
        self.assertFalse(first["cached"])
        self.assertTrue(second["cached"])

        seg = wb.get_timeline("shard-000")["segments"][0]
        sep_before = wb.stage_status()[0]["stages"]["separation"]["stage_version"]
        wb.publish_edit("shard-000", "split",
                        {"seg_id": seg["seg_id"],
                         "at_ms": (seg["start_ms"] + seg["end_ms"]) // 2},
                        expected_version=0)
        sep_after = wb.stage_status()[0]["stages"]["separation"]["stage_version"]
        self.assertEqual(sep_before, sep_after)  # upstream reused
        self.assertEqual(wb.verify_consistency()["status"], "byte-identical")
        third = wb.query("shard-000", "overlaps")
        self.assertFalse(third["cached"])  # old-version cache not served

    def test_latency_budget_error(self):
        wb = self.bench()
        wb.run_all()
        with self.assertRaises(ELatencyBudget) as ctx:
            wb.query("shard-000", "overlaps", budget_ms=0)
        details = ctx.exception.details
        self.assertIn("query_id", details)
        self.assertIn("elapsed_ms", details)

    def test_stale_version_and_draft_rollback(self):
        wb = self.bench()
        wb.run_all()
        wb.publish_edit("shard-001", "rename_speaker",
                        {"old": "S1", "new": "A"}, expected_version=0)
        with self.assertRaises(EStaleVersion):
            wb.publish_edit("shard-001", "rename_speaker",
                            {"old": "S2", "new": "B"}, expected_version=0)
        wb.save_draft("shard-002", "rename_speaker",
                      {"old": "S1", "new": "DRAFT"})
        self.assertEqual(wb.get_timeline("shard-002")["version"], 0)
        wb.discard_draft("shard-002")
        self.assertEqual(wb.get_timeline("shard-002")["version"], 0)

    def test_invalid_stage_and_config(self):
        wb = self.bench()
        with self.assertRaises(EInvalidStage):
            wb.run_stage("shard-000", STAGE_RECOGNITION)
        with self.assertRaises(EInvalidConfig):
            wb.update_config(recognition_margin=2.0)
        with self.assertRaises(EInvalidConfig):
            wb.update_config(mask_iterations=0)

    def test_stop_and_recovery_equivalence(self):
        wb = self.bench()
        wb.request_stop()
        with self.assertRaises(EStopped):
            wb.run_all()
        wb.recover()
        self.assertEqual(wb.verify_consistency()["status"], "byte-identical")

    def test_snapshot_rollback_and_tamper(self):
        wb = self.bench()
        wb.run_all()
        path = wb.ws.snapshot_path("shard-000", STAGE_SEPARATION)
        original = read_json(path)
        rolled = dict(original)
        rolled["stage_version"] = "fp-0000000000000000"
        rolled["snapshot_digest"] = digest_json(
            {k: v for k, v in rolled.items() if k != "snapshot_digest"})
        atomic_write_json(path, rolled)
        with self.assertRaises(EStaleSnapshot) as ctx:
            wb.run_stage("shard-000", STAGE_SEPARATION)
        self.assertEqual(ctx.exception.details["kind"], "rollback")
        atomic_write_json(path, original)
        # Restored snapshot equals current artifact version -> accepted.
        wb.run_stage("shard-000", STAGE_SEPARATION)


class ImportValidationTests(WorkspaceTest):
    def test_sample_count_mismatch_rejected(self):
        manifest_path = os.path.join(self.root, "manifest.json")
        manifest = read_json(manifest_path)
        manifest["shards"][0]["n_samples"] += 1
        bad = os.path.join(self.root, "manifest-bad.json")
        atomic_write_json(bad, manifest)
        wb = Workbench(self.root, default_config())
        with self.assertRaises(EImportValidation):
            wb.import_workspace(bad)


if __name__ == "__main__":
    unittest.main()
