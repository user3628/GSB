"""Acceptance tests: deterministic 4-stage pipeline, checkpoints, incremental
rebuilds, stop/resume consistency and business error codes."""

from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from incseg import STAGES
from incseg.checkpoints import CheckpointRecord, CheckpointStore
from incseg.config import TileConfig, WorkspaceConfig
from incseg.engine import Engine, QUANTIZE_OUTPUTS
from incseg.errors import (IncrementalMismatchError, InvalidConfigError,
                           InvalidStageError, StaleCheckpointError)
from incseg.generator import generate_workspace
from incseg.util import atomic_write_text, canonical_json, sha256_file, sha256_text


class WorkspaceCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="incseg-test-"))
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def make_workspace(self, name="ws", tiles=4, seed=42):
        ws = self.tmp / name
        generate_workspace(ws, tiles=tiles, seed=seed)
        return ws

    @staticmethod
    def quantize_digests(engine, tile_ids):
        out = {}
        for tid in tile_ids:
            for name in QUANTIZE_OUTPUTS:
                out[f"{tid}/{name}"] = sha256_file(engine.artifacts_dir(tid, "quantize") / name)
        return out

    @staticmethod
    def edit_tile_config(ws, tile_id, **changes):
        config = WorkspaceConfig.load(ws / "config.json")
        data = config.tile(tile_id).to_dict()
        data.update(changes)
        config.replace_tile(TileConfig.from_dict(data))
        config.save(ws / "config.json")


class TestDeterminism(WorkspaceCase):
    def test_same_seed_byte_identical_outputs(self):
        ws_a = self.make_workspace("a")
        ws_b = self.make_workspace("b")
        for name in ("tile_000.png", "tile_003.png"):
            self.assertEqual(sha256_file(ws_a / "tiles" / name), sha256_file(ws_b / "tiles" / name))
        eng_a, eng_b = Engine(ws_a), Engine(ws_b)
        eng_a.run()
        eng_b.run()
        tile_ids = [f"tile_{i:03d}" for i in range(4)]
        self.assertEqual(self.quantize_digests(eng_a, tile_ids),
                         self.quantize_digests(eng_b, tile_ids))

    def test_rerun_reuses_everything_and_outputs_stable(self):
        ws = self.make_workspace()
        engine = Engine(ws)
        engine.run()
        tile_ids = [f"tile_{i:03d}" for i in range(4)]
        before = self.quantize_digests(engine, tile_ids)
        report = engine.run()
        self.assertEqual(report["units_completed"], 0)
        for tile in report["tiles"].values():
            for stage in STAGES:
                self.assertEqual(tile["stages"][stage]["status"], "reused")
        self.assertEqual(before, self.quantize_digests(engine, tile_ids))


class TestPipelineOutputs(WorkspaceCase):
    def test_summary_instances_and_boundary(self):
        ws = self.make_workspace()
        Engine(ws).run()
        summary = json.loads(
            (ws / "artifacts" / "tile_000" / "quantize" / "summary.json").read_text())
        self.assertEqual(summary["width"], 96)
        self.assertEqual(summary["height"], 96)
        # every configured class reported (none silently dropped)
        class_ids = [c["class_id"] for c in summary["classes"]]
        self.assertEqual(class_ids, [0, 1, 2, 3])
        ratios = [c["ratio"] for c in summary["classes"]]
        self.assertAlmostEqual(sum(ratios), 1.0, places=5)
        # at least two disconnected instances of the same class exist
        items = summary["instances"]["items"]
        red = [i for i in items if i["class_id"] == 1]
        self.assertGreaterEqual(len(red), 2)
        self.assertEqual(summary["instances"]["count"], len(items))
        for item in items:
            x0, y0, x1, y1 = item["bbox"]
            self.assertGreaterEqual(item["area"], (x1 - x0 + 1))  # sane bbox
        self.assertGreater(summary["boundary_pixels"], 0)

    def test_instance_mask_matches_instance_list(self):
        import numpy as np
        from incseg.stages.common import read_instance_mask
        ws = self.make_workspace()
        Engine(ws).run()
        mask = read_instance_mask(ws / "artifacts" / "tile_000" / "quantize" / "instance.png")
        items = json.loads(
            (ws / "artifacts" / "tile_000" / "instances" / "instances.json").read_text())["instances"]
        ids = sorted(int(v) for v in np.unique(mask) if v != 0)
        self.assertEqual(ids, [item["instance_id"] for item in items])
        for item in items:
            self.assertEqual(int((mask == item["instance_id"]).sum()), item["area"])


class TestCheckpoints(WorkspaceCase):
    def test_corrupt_and_truncated_records_ignored(self):
        ws = self.make_workspace(tiles=1)
        engine = Engine(ws)
        engine.run()
        store = CheckpointStore(ws)
        self.assertEqual([r.seq for r in store.load_records("tile_000")], [1, 2, 3, 4])
        # corrupt the seq=3 record and truncate the seq=4 record
        rec3 = ws / "checkpoints" / "tile_000" / "000003_postprocess.json"
        rec3.write_text("{ not json !")
        rec4 = ws / "checkpoints" / "tile_000" / "000004_quantize.json"
        rec4.write_text('{"tile_id": "tile_000", "stage": "quan')
        chain = store.load_records("tile_000")
        self.assertEqual([r.seq for r in chain], [1, 2])
        self.assertTrue(rec3.with_name(rec3.name + ".corrupt").exists())
        # recovery: rebuild continues from the last consistent checkpoint
        report = engine.run()
        stages = report["tiles"]["tile_000"]["stages"]
        self.assertEqual(stages["classify"]["status"], "reused")
        self.assertEqual(stages["instances"]["status"], "reused")
        self.assertEqual(stages["postprocess"]["status"], "rebuilt")
        self.assertEqual(stages["quantize"]["status"], "rebuilt")
        self.assertEqual([r.seq for r in store.load_records("tile_000")], [1, 2, 3, 4])
        self.assertEqual(engine.verify()["status"], "ok")

    def test_stale_checkpoint_rejected(self):
        ws = self.make_workspace(tiles=1)
        store = CheckpointStore(ws)
        rec = CheckpointRecord(tile_id="tile_000", stage="classify", seq=1,
                               fingerprint="f", input_fingerprint="i",
                               config_version="c", artifact_digest="d", artifacts={})
        store.append(rec)
        with self.assertRaises(StaleCheckpointError):  # duplicate seq
            store.append(rec)
        with self.assertRaises(StaleCheckpointError):  # gap in sequence
            store.append(CheckpointRecord(tile_id="tile_000", stage="instances", seq=3,
                                          fingerprint="f", input_fingerprint="i",
                                          config_version="c", artifact_digest="d", artifacts={}))

    def test_invalid_stage_rejected(self):
        ws = self.make_workspace(tiles=1)
        store = CheckpointStore(ws)
        with self.assertRaises(InvalidStageError):  # quantize before upstream stages
            store.append(CheckpointRecord(tile_id="tile_000", stage="quantize", seq=1,
                                          fingerprint="f", input_fingerprint="i",
                                          config_version="c", artifact_digest="d", artifacts={}))
        store.append(CheckpointRecord(tile_id="tile_000", stage="classify", seq=1,
                                      fingerprint="f", input_fingerprint="i",
                                      config_version="c", artifact_digest="d", artifacts={}))
        with self.assertRaises(InvalidStageError):  # postprocess before instances
            store.append(CheckpointRecord(tile_id="tile_000", stage="postprocess", seq=2,
                                          fingerprint="f", input_fingerprint="i",
                                          config_version="c", artifact_digest="d", artifacts={}))

    def test_artifact_tamper_detected_and_rebuilt(self):
        ws = self.make_workspace(tiles=1)
        engine = Engine(ws)
        engine.run()
        labels = ws / "artifacts" / "tile_000" / "classify" / "labels.png"
        labels.write_bytes(labels.read_bytes() + b"tamper")
        plan = engine.inspect_tile(WorkspaceConfig.load(ws / "config.json").tile("tile_000"))
        self.assertEqual(plan["stages"]["classify"]["status"], "invalid")
        self.assertEqual(plan["stages"]["classify"]["reason"], "artifact_mismatch")
        engine.run()
        self.assertEqual(engine.verify()["status"], "ok")


class TestInvalidationPropagation(WorkspaceCase):
    def _earliest_invalid_stage(self, ws, tile_id, **changes):
        self.edit_tile_config(ws, tile_id, **changes)
        engine = Engine(ws)
        plan = engine.inspect_tile(WorkspaceConfig.load(ws / "config.json").tile(tile_id))
        for stage in STAGES:
            if plan["stages"][stage]["status"] == "invalid":
                return stage, plan["stages"][stage]["reason"]
        return None, None

    def test_config_change_matrix(self):
        cases = [
            ({"color_map": {"#1c1c20": 0, "#dc2828": 1, "#28c83c": 2, "#3250dc": 3,
                            "#e6c832": 4},
              "class_names": {"0": "background", "1": "red", "2": "green", "3": "blue",
                              "4": "yellow"},
              "class_priority": [1, 2, 3, 4, 0]}, "classify"),
            ({"smooth_weight": 0.9}, "classify"),
            ({"connectivity": 4}, "instances"),
            ({"min_area": 100}, "instances"),
            ({"refine_iterations": 3}, "postprocess"),
            ({"fusion_weight": 0.7}, "postprocess"),
        ]
        for changes, expected_stage in cases:
            with self.subTest(changes=changes):
                ws = self.make_workspace(tiles=1)
                Engine(ws).run()
                stage, reason = self._earliest_invalid_stage(ws, "tile_000", **changes)
                self.assertEqual(stage, expected_stage)
                self.assertEqual(reason, "config_changed")
                report = Engine(ws).run()
                stages = report["tiles"]["tile_000"]["stages"]
                idx = STAGES.index(expected_stage)
                for earlier in STAGES[:idx]:
                    self.assertEqual(stages[earlier]["status"], "reused")
                for later in STAGES[idx:]:
                    self.assertEqual(stages[later]["status"], "rebuilt")
                self.assertEqual(Engine(ws).verify()["status"], "ok")

    def test_image_change_invalidates_all_stages(self):
        ws = self.make_workspace(tiles=1)
        engine = Engine(ws)
        engine.run()
        image = ws / "tiles" / "tile_000.png"
        data = bytearray(image.read_bytes())
        data[-1] ^= 0xFF
        image.write_bytes(bytes(data))
        plan = engine.inspect_tile(WorkspaceConfig.load(ws / "config.json").tile("tile_000"))
        self.assertEqual(plan["stages"]["classify"]["reason"], "input_changed")
        for stage in STAGES[1:]:
            self.assertEqual(plan["stages"][stage]["reason"], "upstream_invalidated")
        report = engine.run()
        for stage in STAGES:
            self.assertEqual(report["tiles"]["tile_000"]["stages"][stage]["status"], "rebuilt")
        self.assertEqual(engine.verify()["status"], "ok")

    def test_checkpoint_seqs_monotonic_after_rebuild(self):
        ws = self.make_workspace(tiles=1)
        engine = Engine(ws)
        engine.run()
        self.edit_tile_config(ws, "tile_000", min_area=100)
        engine.run()
        seqs = [r.seq for r in engine.store.load_records("tile_000")]
        self.assertEqual(seqs, [1, 2, 3, 4, 5, 6, 7])  # continuous, no gaps/dups


class TestStopResume(WorkspaceCase):
    def test_resume_byte_identical_to_clean_run(self):
        ws_clean = self.make_workspace("clean")
        Engine(ws_clean).run()
        ws_stop = self.make_workspace("stop")
        engine = Engine(ws_stop)
        partial = engine.run(stop_after_units=5)
        self.assertTrue(partial["stopped"])
        self.assertEqual(partial["units_completed"], 5)
        resumed = engine.run()
        self.assertFalse(resumed["stopped"])
        self.assertEqual(resumed["units_completed"], 11)
        tile_ids = [f"tile_{i:03d}" for i in range(4)]
        self.assertEqual(self.quantize_digests(Engine(ws_clean), tile_ids),
                         self.quantize_digests(engine, tile_ids))
        for tid in tile_ids:
            seqs = [r.seq for r in engine.store.load_records(tid)]
            self.assertEqual(seqs, [1, 2, 3, 4])

    def test_external_stop_flag_honored_between_units(self):
        import threading
        import time

        ws = self.make_workspace("ws", tiles=8, seed=7)
        engine = Engine(ws)
        result = {}

        def target():
            result["report"] = engine.run()

        thread = threading.Thread(target=target)
        thread.start()
        deadline = time.time() + 30
        while time.time() < deadline and engine.store.max_seq("tile_000") < 4:
            time.sleep(0.01)
        engine.request_stop()  # external stop request mid-run
        thread.join(timeout=30)
        report = result["report"]
        self.assertTrue(report["stopped"])
        self.assertLess(report["units_completed"], 32)
        final = engine.run()  # resume to completion
        self.assertFalse(final["stopped"])
        self.assertEqual(engine.verify()["status"], "ok")


class TestIncrementalMismatch(WorkspaceCase):
    def test_mismatch_reports_diff_files(self):
        ws = self.make_workspace(tiles=1)
        engine = Engine(ws)
        engine.run()
        # tamper the incremental output AND its checkpoint record, simulating a
        # corrupted-but-consistent incremental state
        semantic = ws / "artifacts" / "tile_000" / "quantize" / "semantic.png"
        semantic.write_bytes(semantic.read_bytes() + b"corrupted")
        rec_path = ws / "checkpoints" / "tile_000" / "000004_quantize.json"
        record = json.loads(rec_path.read_text())
        record["artifacts"]["semantic.png"] = sha256_file(semantic)
        record["artifact_digest"] = sha256_text(canonical_json({"files": record["artifacts"]}))
        atomic_write_text(rec_path, json.dumps(record))
        with self.assertRaises(IncrementalMismatchError) as ctx:
            engine.verify()
        self.assertEqual(ctx.exception.code, "E_INCREMENTAL_MISMATCH")
        files = ctx.exception.details["files"]
        self.assertTrue(any("tile_000/quantize/semantic.png" in f for f in files))


class TestInvalidConfig(WorkspaceCase):
    def _tile_dict(self, **overrides):
        ws = self.make_workspace(tiles=1)
        data = WorkspaceConfig.load(ws / "config.json").tile("tile_000").to_dict()
        data.update(overrides)
        return data

    def test_invalid_configs_raise(self):
        bad_cases = [
            {"connectivity": 5},
            {"connectivity": 7},
            {"min_area": -1},
            {"smooth_weight": 1.5},
            {"fusion_weight": -0.1},
            {"refine_iterations": -1},
            {"color_map": {}},
            {"color_map": {"red": 1}},
            {"color_map": {"#ff0000": 1, "#00ff00": 1}},  # duplicate class id
            {"color_map": {"#ff0000": 300}},  # class id out of range
            {"class_priority": [1, 2]},
        ]
        for overrides in bad_cases:
            with self.subTest(overrides=overrides):
                with self.assertRaises(InvalidConfigError) as ctx:
                    TileConfig.from_dict(self._tile_dict(**overrides))
                self.assertEqual(ctx.exception.code, "E_INVALID_CONFIG")

    def test_missing_color_map(self):
        data = self._tile_dict()
        del data["color_map"]
        with self.assertRaises(InvalidConfigError):
            TileConfig.from_dict(data)


if __name__ == "__main__":
    unittest.main()
