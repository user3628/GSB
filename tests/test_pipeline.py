from __future__ import annotations

import hashlib
import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from pose_pipeline.config import PipelineConfig
from pose_pipeline.constants import (
    ERROR_INVALID_PARAM,
    ERROR_INVALID_STAGE,
    ERROR_POSE_INVALID,
    ERROR_STALE_CHECKPOINT,
)
from pose_pipeline.engine import PipelineEngine
from pose_pipeline.errors import PipelineError
from pose_pipeline.generator import generate_dataset
from pose_pipeline.http_api import create_server
from pose_pipeline.util import (
    atomic_write_pretty_json,
    finalize_artifact,
    read_json_file,
)


def sha256_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.class_tmp = tempfile.TemporaryDirectory()
        cls.source = Path(cls.class_tmp.name) / "source"
        generate_dataset(cls.source)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.class_tmp.cleanup()

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def make_engine(
        self, name: str = "workspace", config: PipelineConfig | None = None
    ) -> PipelineEngine:
        engine = PipelineEngine(self.root / name, config=config)
        engine.import_samples(self.source / "manifest.json")
        return engine

    def test_generator_is_byte_deterministic(self) -> None:
        second = self.root / "second"
        generate_dataset(second)
        self.assertEqual(
            (self.source / "manifest.json").read_bytes(),
            (second / "manifest.json").read_bytes(),
        )
        self.assertEqual(
            (self.source / "ground_truth.json").read_bytes(),
            (second / "ground_truth.json").read_bytes(),
        )
        for image in sorted((self.source / "images").glob("*.png")):
            counterpart = second / "images" / image.name
            self.assertEqual(image.read_bytes(), counterpart.read_bytes())

    def test_full_build_and_deterministic_exports(self) -> None:
        engine = self.make_engine("one")
        result = engine.build()
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["planned_unit_count"], 20)
        self.assertTrue(result["pipeline_complete"])
        report = engine.report()
        self.assertEqual(report["totals"]["shards"], 5)
        self.assertEqual(report["totals"]["truth_instances"], 7)
        self.assertEqual(report["totals"]["visible_truth_keypoints"], 103)
        self.assertEqual(report["totals"]["hits"], 103)
        self.assertEqual(report["totals"]["hit_rate"], 1.0)
        self.assertIn("中文源-Alpha / studio-alpha", report["groups"]["by_source"])
        self.assertIn("head", report["groups"]["by_keypoint"])
        records = list(engine.store.scan().records)
        self.assertEqual([record["sequence"] for record in records], list(range(1, 21)))
        self.assertEqual(
            [record["shard_sequence"] for record in records],
            [1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4],
        )
        preprocess_2 = engine.store.read_artifact_by_digest(
            "shard_02",
            "preprocess",
            engine.store.latest_by_stage()[("shard_02", "preprocess")]["artifact_digest"],
        )
        self.assertEqual(preprocess_2["invisible_keypoints"], ["left_wrist"])
        decode_2 = engine.store.read_artifact_by_digest(
            "shard_02",
            "decode",
            engine.store.latest_by_stage()[("shard_02", "decode")]["artifact_digest"],
        )
        left_wrist = next(
            item for item in decode_2["keypoints"] if item["keypoint"] == "left_wrist"
        )
        self.assertTrue(left_wrist["empty"])
        self.assertEqual(left_wrist["reason"], "invisible_zero_heatmap")
        assembly_3 = engine.store.read_artifact_by_digest(
            "shard_03",
            "assembly",
            engine.store.latest_by_stage()[("shard_03", "assembly")]["artifact_digest"],
        )
        self.assertEqual(assembly_3["instance_count"], 2)
        errors = engine.keypoint_errors()
        self.assertEqual(errors["count"], 103)

        second = self.make_engine("two")
        second.build()
        self.assertEqual(
            (engine.store.exports_dir / "report.json").read_bytes(),
            (second.store.exports_dir / "report.json").read_bytes(),
        )
        self.assertEqual(
            (engine.store.exports_dir / "poses.json").read_bytes(),
            (second.store.exports_dir / "poses.json").read_bytes(),
        )

    def test_incremental_stage_invalidation(self) -> None:
        engine = self.make_engine()
        engine.build()
        before = {
            (shard_id, stage): engine.store.latest_by_stage()[(shard_id, stage)]["sequence"]
            for shard_id in ["shard_01", "shard_02", "shard_03", "shard_04", "shard_05"]
            for stage in ["preprocess", "decode", "assembly", "validation"]
        }
        manifest_path = engine.store.inputs_dir / "manifest.json"
        manifest = read_json_file(manifest_path)
        manifest["skeleton"].append(["left_shoulder", "right_shoulder"])
        for shard in manifest["shards"]:
            shard.pop("skeleton", None)
        atomic_write_pretty_json(manifest_path, manifest)

        with self.assertRaises(PipelineError):
            engine.report()
        self.assertFalse(engine.status()["exports"]["valid"])

        result = engine.build()
        self.assertEqual(result["planned_unit_count"], 10)
        invalidation = result["invalidation"]
        self.assertEqual(invalidation["earliest_stage"], "assembly")
        self.assertEqual(sorted(invalidation["affected_shard_ids"]), [f"shard_0{i}" for i in range(1, 6)])
        self.assertTrue(
            all(item["earliest_stage"] == "assembly" for item in invalidation["affected_shards"])
        )
        for shard_id in ["shard_01", "shard_02", "shard_03", "shard_04", "shard_05"]:
            self.assertEqual(
                before[(shard_id, "preprocess")],
                engine.store.latest_by_stage()[(shard_id, "preprocess")]["sequence"],
            )
            self.assertEqual(
                before[(shard_id, "decode")],
                engine.store.latest_by_stage()[(shard_id, "decode")]["sequence"],
            )
            self.assertGreater(
                engine.store.latest_by_stage()[(shard_id, "assembly")]["sequence"],
                before[(shard_id, "assembly")],
            )

        manifest = read_json_file(manifest_path)
        truth = manifest["shards"][0]["instances"][0]["keypoints"]["head"]
        truth["x"] = round(truth["x"] + 0.01, 8)
        atomic_write_pretty_json(manifest_path, manifest)
        result = engine.build()
        self.assertEqual(result["planned_unit_count"], 4)
        self.assertEqual(result["invalidation"]["earliest_stage"], "preprocess")
        self.assertEqual(result["invalidation"]["affected_shard_ids"], ["shard_01"])

        manifest = read_json_file(manifest_path)
        for shard in manifest["shards"]:
            shard["expected_pose_confidence"] = {"min": 0.995, "max": 1.0}
            for instance in shard["instances"]:
                instance["expected_pose_confidence"] = {"min": 0.995, "max": 1.0}
        atomic_write_pretty_json(manifest_path, manifest)
        before_validation = {
            shard_id: engine.store.latest_by_stage()[(shard_id, "assembly")]["sequence"]
            for shard_id in ["shard_01", "shard_02"]
        }
        result = engine.build()
        self.assertEqual(result["planned_unit_count"], 5)
        self.assertEqual(result["invalidation"]["earliest_stage"], "validation")
        for shard_id, sequence in before_validation.items():
            self.assertEqual(
                sequence,
                engine.store.latest_by_stage()[(shard_id, "assembly")]["sequence"],
            )
        self.assertGreaterEqual(
            engine.report()["totals"]["confidence_out_of_interval_instances"], 1
        )
        saved = engine.invalidations()
        self.assertEqual(saved["status"], "completed")
        self.assertEqual(saved["earliest_stage"], "validation")
        self.assertEqual(engine.invalidations(refresh=True)["earliest_stage"], None)

    def test_stop_resume_and_external_stop_are_byte_identical(self) -> None:
        clean = self.make_engine("clean")
        clean.build()
        clean_report = (clean.store.exports_dir / "report.json").read_bytes()
        clean_poses = (clean.store.exports_dir / "poses.json").read_bytes()
        clean_records = [
            (record["sequence"], record["record_digest"])
            for record in clean.store.scan().records
        ]

        resumed = self.make_engine("resumed")
        partial = resumed.build(stop_after_units=3)
        self.assertEqual(partial["status"], "stopped_after_unit_limit")
        self.assertEqual(len(partial["completed_units"]), 3)
        self.assertFalse(partial["pipeline_complete"])
        self.assertFalse(resumed.status()["exports"]["valid"])
        final = resumed.resume()
        self.assertEqual(final["status"], "completed")
        self.assertEqual(final["planned_unit_count"], 17)
        self.assertEqual(clean_report, (resumed.store.exports_dir / "report.json").read_bytes())
        self.assertEqual(clean_poses, (resumed.store.exports_dir / "poses.json").read_bytes())
        self.assertEqual(
            clean_records,
            [
                (record["sequence"], record["record_digest"])
                for record in resumed.store.scan().records
            ],
        )

        externally_stopped = self.make_engine("external")
        externally_stopped.request_stop()
        stopped = externally_stopped.build()
        self.assertEqual(stopped["status"], "stopped_before_unit")
        self.assertEqual(stopped["completed_units"], [])
        completed = externally_stopped.resume()
        self.assertTrue(completed["pipeline_complete"])
        self.assertEqual(clean_report, (externally_stopped.store.exports_dir / "report.json").read_bytes())

    def test_corrupt_and_duplicate_checkpoints_are_ignored(self) -> None:
        clean = self.make_engine("clean")
        clean.build()
        clean_report = (clean.store.exports_dir / "report.json").read_bytes()

        recovered = self.make_engine("recovered")
        recovered.build(stop_after_units=3)
        checkpoint_dir = recovered.store.checkpoint_dir
        accepted = list(recovered.store.scan().records)
        duplicate = checkpoint_dir / "000000000003-ffffffffffffffff.json"
        duplicate.write_bytes(accepted[2].__class__ and Path(
            next(path for path in checkpoint_dir.glob("000000000003-*.json"))
        ).read_bytes())
        (checkpoint_dir / "000000000004-0000000000000000.json").write_text("{not-json", encoding="utf-8")
        (checkpoint_dir / "000000000005-1111111111111111.json").write_text(
            json.dumps({"sequence": 5}), encoding="utf-8"
        )
        result = recovered.resume()
        self.assertTrue(result["pipeline_complete"])
        self.assertEqual(clean_report, (recovered.store.exports_dir / "report.json").read_bytes())
        scan = recovered.store.scan()
        self.assertEqual([record["sequence"] for record in scan.records], list(range(1, 21)))
        self.assertGreaterEqual(scan.diagnostics["malformed_or_incomplete_records"], 2)
        self.assertGreaterEqual(scan.diagnostics["duplicate_sequence_records"], 1)

    def test_staged_build_and_invalid_stage_jump(self) -> None:
        engine = self.make_engine()
        result = engine.build(to_stage="preprocess")
        self.assertEqual(result["planned_unit_count"], 5)
        self.assertFalse(result["pipeline_complete"])
        self.assertEqual(result["status"], "completed_to_stage")
        with self.assertRaises(PipelineError) as context:
            engine.build(strict_shard_id="shard_01", strict_stage="assembly")
        self.assertEqual(context.exception.code, ERROR_INVALID_STAGE)
        self.assertEqual(context.exception.details["current_stage"], "preprocess")
        strict_decode = engine.build(strict_shard_id="shard_01", strict_stage="decode")
        self.assertEqual(strict_decode["status"], "committed")
        with self.assertRaises(PipelineError) as context:
            engine.build(strict_shard_id="shard_01", strict_stage="validation")
        self.assertEqual(context.exception.code, ERROR_INVALID_STAGE)
        self.assertEqual(context.exception.details["current_stage"], "decode")

    def test_config_changes_hit_exact_stage_and_match_fresh_build(self) -> None:
        engine = self.make_engine()
        engine.build()

        engine = PipelineEngine(engine.workspace, config=PipelineConfig(gaussian_radius=2.5))
        result = engine.build()
        self.assertEqual(result["planned_unit_count"], 20)
        self.assertEqual(result["invalidation"]["earliest_stage"], "preprocess")

        engine = PipelineEngine(
            engine.workspace,
            config=PipelineConfig(gaussian_radius=2.5, max_candidates=3),
        )
        pre_sequence = {
            shard_id: engine.store.latest_by_stage()[(shard_id, "preprocess")]["sequence"]
            for shard_id in ["shard_01", "shard_02"]
        }
        result = engine.build()
        self.assertEqual(result["planned_unit_count"], 15)
        self.assertEqual(result["invalidation"]["earliest_stage"], "decode")
        for shard_id, sequence in pre_sequence.items():
            self.assertEqual(
                sequence,
                engine.store.latest_by_stage()[(shard_id, "preprocess")]["sequence"],
            )

        engine = PipelineEngine(
            engine.workspace,
            config=PipelineConfig(
                gaussian_radius=2.5,
                max_candidates=3,
                association_max_edge_fraction=0.22,
            ),
        )
        result = engine.build()
        self.assertEqual(result["planned_unit_count"], 10)
        self.assertEqual(result["invalidation"]["earliest_stage"], "assembly")

        final_config = PipelineConfig(
            gaussian_radius=2.5,
            max_candidates=3,
            association_max_edge_fraction=0.22,
            round_digits=3,
        )
        engine = PipelineEngine(engine.workspace, config=final_config)
        result = engine.build()
        self.assertEqual(result["planned_unit_count"], 5)
        self.assertEqual(result["invalidation"]["earliest_stage"], "validation")
        self.assertTrue(result["pipeline_complete"])

        fresh = self.make_engine("fresh-rounding", final_config)
        fresh.build()
        self.assertEqual(
            (engine.store.exports_dir / "report.json").read_bytes(),
            (fresh.store.exports_dir / "report.json").read_bytes(),
        )
        self.assertEqual(
            (engine.store.exports_dir / "poses.json").read_bytes(),
            (fresh.store.exports_dir / "poses.json").read_bytes(),
        )

    def test_business_error_codes(self) -> None:
        negative = self.make_engine("negative", PipelineConfig(gaussian_radius=-1))
        with self.assertRaises(PipelineError) as context:
            negative.build()
        self.assertEqual(context.exception.code, ERROR_INVALID_PARAM)
        self.assertEqual(context.exception.stage, "preprocess")
        self.assertEqual(context.exception.shard_id, "shard_01")

        mismatch = self.make_engine(
            "heatmap", PipelineConfig(gaussian_radius=2.2, heatmap_width=64)
        )
        with self.assertRaises(PipelineError) as context:
            mismatch.build()
        self.assertEqual(context.exception.code, ERROR_INVALID_PARAM)
        self.assertEqual(context.exception.details["heatmap_size"], [64, 128])

        unknown = self.make_engine("unknown")
        unknown.build()
        manifest_path = unknown.store.inputs_dir / "manifest.json"
        manifest = read_json_file(manifest_path)
        manifest["skeleton"].append(["head", "unknown_joint"])
        for shard in manifest["shards"]:
            shard["skeleton"] = list(manifest["skeleton"])
        atomic_write_pretty_json(manifest_path, manifest)
        with self.assertRaises(PipelineError) as context:
            unknown.build()
        self.assertEqual(context.exception.code, ERROR_POSE_INVALID)
        self.assertEqual(context.exception.shard_id, "shard_01")
        self.assertEqual(
            context.exception.details["unknown_skeleton_connections"][0]["unknown_keypoints"],
            ["unknown_joint"],
        )

        low = self.make_engine("low", PipelineConfig(min_valid_keypoints=99))
        with self.assertRaises(PipelineError) as context:
            low.build()
        self.assertEqual(context.exception.code, ERROR_POSE_INVALID)
        self.assertEqual(context.exception.details["reason"], "low_valid_keypoints")

        conflict = self.make_engine(
            "conflict", PipelineConfig(association_max_edge_fraction=0.75)
        )
        with self.assertRaises(PipelineError) as context:
            conflict.build()
        self.assertEqual(context.exception.code, ERROR_POSE_INVALID)
        self.assertEqual(context.exception.details["reason"], "multi_instance_conflict")

    def test_stale_checkpoint_and_uncommitted_artifact(self) -> None:
        engine = self.make_engine()
        before = engine.store.scan().head
        self.assertIsNone(before)
        with self.assertRaises(PipelineError) as context:
            engine.store.publish(
                shard_id="shard_01",
                stage="decode",
                input_fingerprint="0" * 64,
                config_version="0" * 64,
                artifact_digest="1" * 64,
                upstream_digest=None,
                expected_sequence=1,
                expected_shard_id="shard_01",
                expected_stage="decode",
            )
        self.assertEqual(context.exception.code, ERROR_INVALID_STAGE)
        self.assertIsNone(context.exception.details["current_stage"])

        engine.build()
        head = engine.store.scan().head
        assert head is not None
        next_sequence = engine.store.next_sequence()
        with self.assertRaises(PipelineError) as context:
            engine.store.publish(
                shard_id="shard_01",
                stage="preprocess",
                input_fingerprint="0" * 64,
                config_version="0" * 64,
                artifact_digest="2" * 64,
                upstream_digest=None,
                expected_sequence=next_sequence - 1,
            )
        self.assertEqual(context.exception.code, ERROR_STALE_CHECKPOINT)
        with self.assertRaises(PipelineError) as context:
            engine.store.publish(
                shard_id="shard_01",
                stage="preprocess",
                input_fingerprint="0" * 64,
                config_version="0" * 64,
                artifact_digest="2" * 64,
                upstream_digest=None,
                expected_sequence=next_sequence,
                expected_shard_id="shard_02",
            )
        self.assertEqual(context.exception.code, ERROR_STALE_CHECKPOINT)
        pre = engine.store.latest_by_stage()[("shard_01", "preprocess")]
        with self.assertRaises(PipelineError) as context:
            engine.store.publish(
                shard_id="shard_01",
                stage="preprocess",
                input_fingerprint=pre["input_fingerprint"],
                config_version=pre["config_version"],
                artifact_digest=pre["artifact_digest"],
                upstream_digest=None,
                expected_sequence=next_sequence,
            )
        self.assertEqual(context.exception.code, ERROR_STALE_CHECKPOINT)
        self.assertEqual(head, engine.store.scan().head)

        uncommitted = finalize_artifact(
            {
                "schema_version": 1,
                "stage": "preprocess",
                "shard_id": "shard_01",
                "input_fingerprint": "3" * 64,
                "config_version": "4" * 64,
                "payload": "rejected-but-readable",
            }
        )
        engine.store.write_artifact(uncommitted)
        listed = engine.uncommitted_artifacts()["artifacts"]
        self.assertTrue(
            any(item["artifact_digest"] == uncommitted["artifact_digest"] for item in listed)
        )
        report = engine.report()
        self.assertNotIn(
            uncommitted["artifact_digest"],
            {shard["summary_artifact_digest"] for shard in report["shards"]},
        )

    def test_http_api(self) -> None:
        workspace = self.root / "http_workspace"
        server = create_server(workspace, host="127.0.0.1", port=0, quiet=True)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{server.server_address[1]}"
        try:
            health = self.http_json("GET", base + "/health")
            self.assertTrue(health["ok"])
            imported = self.http_json(
                "POST",
                base + "/api/import",
                {"manifest_path": str(self.source / "manifest.json")},
            )
            self.assertTrue(imported["ok"])
            built = self.http_json("POST", base + "/api/build", {})
            self.assertTrue(built["pipeline_complete"])
            status = self.http_json("GET", base + "/api/status")
            self.assertTrue(status["pipeline_complete"])
            fingerprints = self.http_json("GET", base + "/api/fingerprints")
            self.assertIn("version_fingerprint", fingerprints)
            report = self.http_json("GET", base + "/api/report")
            self.assertEqual(report["totals"]["shards"], 5)
            keypoints = self.http_json("GET", base + "/api/keypoint-errors")
            self.assertEqual(keypoints["count"], 103)
            invalidations = self.http_json("GET", base + "/api/invalidations")
            self.assertEqual(invalidations["status"], "completed")
            stop = self.http_json("POST", base + "/api/stop", {})
            self.assertTrue(stop["stop_requested"])
            resumed = self.http_json("POST", base + "/api/resume", {})
            self.assertTrue(resumed["pipeline_complete"])

            with self.assertRaises(urllib.error.HTTPError) as context:
                self.http_json(
                    "POST",
                    base + "/api/build",
                    {"config": {"gaussian_radius": -2}},
                )
            self.assertEqual(context.exception.code, 400)
            error_body = json.loads(context.exception.read().decode("utf-8"))
            context.exception.close()
            self.assertEqual(error_body["code"], ERROR_INVALID_PARAM)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    @staticmethod
    def http_json(
        method: str, url: str, payload: dict | None = None
    ) -> dict:
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))


if __name__ == "__main__":
    unittest.main()
