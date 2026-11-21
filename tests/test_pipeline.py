"""recon3d 端到端与单元测试。

覆盖：样本确定性、特征提取与 E_EMPTY_FEATURE、三角化精度与 E_INVALID_POSE、
几何裁剪/降采样与 E_GEOMETRY_DEGENERATE、检查点原子性与恢复、停止恢复一致、
增量与全量逐字节一致、查询缓存与延迟预算、并发发布保护。
"""

from __future__ import annotations

import copy
import json
import os
import shutil
import tempfile
import unittest

import numpy as np

from recon3d.canonical import dumps, fingerprint_bytes, read_json, read_png, write_json, write_png
from recon3d.checkpoint import CheckpointStore
from recon3d.errors import (
    EmptyFeatureError,
    GeometryDegenerateError,
    IncrementalMismatchError,
    InvalidPoseError,
    InvalidStageError,
    LatencyBudgetError,
    StaleCheckpointError,
)
from recon3d.features import extract_features
from recon3d.geometry import cull_and_downsample
from recon3d.generator import generate_samples
from recon3d.packaging import validate_and_package
from recon3d.pipeline import Pipeline
from recon3d.query import QueryService
from recon3d.triangulation import triangulate_tracks


class ReconTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="recon3d_test_")
        self.sample_dir = os.path.join(self.tmp, "samples")
        self.run_dir = os.path.join(self.tmp, "run")
        self.manifest = generate_samples(self.sample_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def wide_tiles(self):
        return self.manifest["tiles"][:3]

    def feature_sets(self, tiles=None):
        tiles = tiles or self.wide_tiles()
        return {
            t["tile_id"]: extract_features(read_png(os.path.join(self.sample_dir, t["image"])), t["tile_id"])
            for t in tiles
        }

    # ------------------------------------------------------------------ #
    def test_01_sample_deterministic(self):
        again = os.path.join(self.tmp, "samples2")
        manifest2 = generate_samples(again)
        self.assertEqual(dumps(self.manifest), dumps(manifest2))
        for tile in self.manifest["tiles"]:
            with open(os.path.join(self.sample_dir, tile["image"]), "rb") as handle:
                a = handle.read()
            with open(os.path.join(again, tile["image"]), "rb") as handle:
                b = handle.read()
            self.assertEqual(a, b)
        self.assertGreaterEqual(len(self.manifest["tiles"]), 3)
        self.assertGreaterEqual(len(self.manifest["truth_points"]), 6)

    def test_02_feature_recall_and_empty(self):
        for tile in self.manifest["tiles"]:
            image = read_png(os.path.join(self.sample_dir, tile["image"]))
            result = extract_features(image, tile["tile_id"])
            self.assertEqual(len(result["features"]), len(tile["visible_points"]))
            # 逐字节一致
            self.assertEqual(dumps(result), dumps(extract_features(image, tile["tile_id"])))
        with self.assertRaises(EmptyFeatureError) as ctx:
            extract_features(np.zeros((16, 16), np.uint8), "tile_blank")
        self.assertEqual(ctx.exception.code, "E_EMPTY_FEATURE")
        self.assertEqual(ctx.exception.payload["tile_id"], "tile_blank")
        with self.assertRaises(EmptyFeatureError):
            extract_features(np.full((16, 16), 120, np.uint8), "tile_const")

    def test_03_triangulation_accuracy(self):
        result = triangulate_tracks(self.wide_tiles(), self.feature_sets())
        self.assertEqual(result["track_count"], len(self.manifest["truth_points"]))
        truth = {p["point_id"]: np.array(p["world"]) for p in self.manifest["truth_points"]}
        tiles = self.wide_tiles()
        errors = []
        for track in result["tracks"]:
            obs = track["observations"][0]
            tile = next(t for t in tiles if t["tile_id"] == obs["tile_id"])
            pid = min(
                tile["visible_points"],
                key=lambda v: (v["pixel"][0] - obs["x"]) ** 2 + (v["pixel"][1] - obs["y"]) ** 2,
            )["point_id"]
            errors.append(float(np.linalg.norm(np.array(track["point"]) - truth[pid])))
            self.assertGreaterEqual(track["view_count"], 2)
        rmse = float(np.sqrt(np.mean(np.square(errors))))
        self.assertLess(rmse, 0.25)

    def test_04_invalid_pose_codes(self):
        fs = self.feature_sets()
        bad = copy.deepcopy(self.wide_tiles())
        bad[0]["rotation"][0][0] += 0.5
        with self.assertRaises(InvalidPoseError) as ctx:
            triangulate_tracks(bad, fs)
        self.assertEqual(ctx.exception.payload["tile_id"], "tile_00")

        bad = copy.deepcopy(self.wide_tiles())
        bad[1]["intrinsics"][0][0] = 0.0
        with self.assertRaises(InvalidPoseError) as ctx:
            triangulate_tracks(bad, fs)
        self.assertEqual(ctx.exception.code, "E_INVALID_POSE")

        with self.assertRaises(InvalidPoseError) as ctx:
            triangulate_tracks(self.wide_tiles()[:1], fs)
        self.assertEqual(ctx.exception.code, "E_INVALID_POSE")

    def test_05_geometry_cull_downsample(self):
        tri = triangulate_tracks(self.wide_tiles(), self.feature_sets())
        geo = cull_and_downsample(
            tri, self.manifest["reprojection_threshold"],
            self.manifest["min_points"], self.manifest["sample_density"],
        )
        self.assertEqual(geo["counts"]["before_cull"], tri["track_count"])
        # 确定性
        self.assertEqual(
            dumps(geo),
            dumps(cull_and_downsample(tri, self.manifest["reprojection_threshold"],
                                      self.manifest["min_points"], self.manifest["sample_density"])),
        )
        # 大体积触发合并
        merged = cull_and_downsample(tri, self.manifest["reprojection_threshold"], 2, 1.5)
        self.assertGreaterEqual(merged["counts"]["merged_voxels"], 1)
        with self.assertRaises(GeometryDegenerateError):
            cull_and_downsample(tri, self.manifest["reprojection_threshold"], 2, 1.5,
                                strict_duplicate_voxel=True)
        with self.assertRaises(GeometryDegenerateError):
            cull_and_downsample(tri, 0.001, self.manifest["min_points"],
                                self.manifest["sample_density"])

    def test_06_full_pipeline_and_validation(self):
        pipeline = Pipeline(self.sample_dir, self.run_dir)
        pipeline.run(force=True)
        summary = read_json(os.path.join(self.run_dir, "exports", "validation_summary.json"))
        self.assertEqual(summary["coverage"], 1.0)
        self.assertLess(summary["rmse"], 0.25)
        self.assertTrue(os.path.exists(os.path.join(self.run_dir, "exports", "point_cloud.ply")))
        # journal 序号连续
        seqs = [r["seq"] for r in pipeline.store.load_journal()]
        self.assertEqual(seqs, list(range(1, len(seqs) + 1)))

    def test_07_stop_and_resume_identical(self):
        stopper = Pipeline(self.sample_dir, os.path.join(self.tmp, "stop"))
        from recon3d.errors import StoppedError

        with self.assertRaises(StoppedError):
            stopper.run(force=True, stop_after="triangulation:global")
        self.assertFalse(stopper.store.has_artifact("packaging", "global"))
        Pipeline(self.sample_dir, os.path.join(self.tmp, "stop")).run(force=False)
        full = Pipeline(self.sample_dir, os.path.join(self.tmp, "full"))
        full.run(force=True)
        stopped = Pipeline(self.sample_dir, os.path.join(self.tmp, "stop"))
        scopes = {
            "features": [t["tile_id"] for t in self.manifest["tiles"]],
            "triangulation": ["global"],
            "geometry": ["global"],
            "packaging": ["global"],
        }
        for stage, ids in scopes.items():
            for scope in ids:
                self.assertEqual(
                    dumps(stopped.store.read_artifact(stage, scope)),
                    dumps(full.store.read_artifact(stage, scope)),
                )

    def test_08_corrupt_journal_recovery(self):
        pipeline = Pipeline(self.sample_dir, self.run_dir)
        pipeline.run(force=True)
        good = len(pipeline.store.load_journal())
        with open(pipeline.store.journal_path, "ab") as handle:
            handle.write(b'{"seq": 99, "stage": "broken"')  # 半写记录
        store = CheckpointStore(self.run_dir)
        self.assertEqual(len(store.load_journal()), good)

    def test_09_incremental_matches_full(self):
        baseline = Pipeline(self.sample_dir, os.path.join(self.tmp, "inc"))
        baseline.run(force=True)
        changed_dir = os.path.join(self.tmp, "changed")
        shutil.copytree(self.sample_dir, changed_dir)
        image_path = os.path.join(changed_dir, "images", "tile_02.png")
        image = read_png(image_path)
        image[2:6, 2:6] = 200
        write_png(image_path, image)
        changed_manifest = read_json(os.path.join(changed_dir, "manifest.json"))
        changed_manifest["reprojection_threshold"] = 1.1
        write_json(os.path.join(changed_dir, "manifest.json"), changed_manifest)

        incremental = Pipeline(changed_dir, os.path.join(self.tmp, "inc"))
        result = incremental.incremental_rebuild(
            changed_manifest, changed_dir, os.path.join(self.tmp, "inc")
        )
        self.assertEqual(result["mismatches"], 0)

    def test_10_query_cache_and_budget(self):
        pipeline = Pipeline(self.sample_dir, self.run_dir)
        pipeline.run(force=True)
        service = QueryService(self.run_dir, pipeline.sample_fp,
                               self.manifest["config_version"], default_budget=1.0)
        service.stage_digests = {r["stage"]: r["artifact_digest"]
                                 for r in pipeline.store.stage_status()}
        cold = service.query("cloud_summary", query_id="c1")
        warm = service.query("cloud_summary", query_id="c2")
        self.assertFalse(cold["cache"]["hit"])
        self.assertTrue(warm["cache"]["hit"])
        self.assertEqual(cold["cache"]["key"], warm["cache"]["key"])
        with self.assertRaises(LatencyBudgetError) as ctx:
            service.query("error_stats", budget=0.0, query_id="slow")
        self.assertIn("elapsed_ms", ctx.exception.payload)
        service.stage_digests["geometry"] = "changed"
        after = service.query("cloud_summary", query_id="c3")
        self.assertNotEqual(after["cache"]["key"], cold["cache"]["key"])

    def test_11_concurrent_publish_guard(self):
        store = CheckpointStore(os.path.join(self.tmp, "cp"))
        store.publish("features", "tile_a", "in", "cfg-1", "art")
        with self.assertRaises(StaleCheckpointError):
            store.publish("features", "tile_a", "in", "cfg-1", "art")
        with self.assertRaises(InvalidStageError):
            store.publish("nope", "global", "in", "cfg-1", "art")

    def test_12_packaging_requires_truth(self):
        tri = triangulate_tracks(self.wide_tiles(), self.feature_sets())
        geo = cull_and_downsample(tri, self.manifest["reprojection_threshold"],
                                  self.manifest["min_points"], self.manifest["sample_density"])
        bad_manifest = copy.deepcopy(self.manifest)
        bad_manifest["truth_points"] = []
        from recon3d.errors import ManifestError

        with self.assertRaises(ManifestError):
            validate_and_package(geo, bad_manifest, os.path.join(self.tmp, "exports"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
