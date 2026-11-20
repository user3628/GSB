from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

import yaml

from detdiff.checkpoints import CheckpointStore
from detdiff.config import load_config
from detdiff.errors import BusinessError, ErrorCode
from detdiff.pipeline import (
    BuildHooks,
    BuildStopped,
    ComputedShard,
    build_workspace,
    compute_shard_from_scratch,
    inspect_workspace,
    verify_against_full,
)
from detdiff.util import sha256_file, tree_manifest
from detdiff.workspace import update_shard, write_config

from helpers import create_image, make_workspace


class PipelineIncrementalTests(unittest.TestCase):
    def test_full_build_creates_all_stages_and_verifies_full_build(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = make_workspace(Path(temp) / "workspace")
            report = build_workspace(root)
            self.assertTrue(report.ok)
            self.assertTrue(report.consistency_verified)
            self.assertEqual(started_stages(report), {
                "alpha": ["encode", "schedule", "sample", "quantize"],
                "beta": ["encode", "schedule", "sample", "quantize"],
            })
            diagnostics = inspect_workspace(root=root)
            self.assertTrue(all(not stage.invalid for diagnostic in diagnostics for stage in diagnostic.stages))
            self.assertEqual(len(diagnostics[0].stages[2].trajectory), 4)
            self.assertEqual(len(diagnostics[1].stages[2].trajectory), 5)
            self.assertTrue(all((root / "outputs" / f"{item.shard.id}.png").is_file() for item in diagnostics))
            verified = verify_against_full(root)
            self.assertTrue(verified["ok"])
            self.assertEqual(verified["files_compared"], 10)

            before = tree_manifest(root)
            inspect_workspace(root=root)
            after = tree_manifest(root)
            self.assertEqual(before, after)

    def test_stop_after_stage_then_resume(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = make_workspace(Path(temp) / "workspace")
            with self.assertRaises(BuildStopped) as context:
                build_workspace(root, hooks=BuildHooks(stop_after_stage="encode"))
            stopped = context.exception.report
            self.assertEqual(stopped.published[-1]["stage"], "encode")
            self.assertEqual(stopped.published[-1]["shard"], "alpha")
            self.assertFalse((root / "outputs" / "alpha.png").exists())

            diagnostics = inspect_workspace(root=root)
            alpha = next(item for item in diagnostics if item.shard.id == "alpha")
            self.assertEqual(alpha.earliest_invalid, "schedule")
            beta = next(item for item in diagnostics if item.shard.id == "beta")
            self.assertEqual(beta.earliest_invalid, "encode")

            resumed = build_workspace(root)
            self.assertTrue(resumed.ok)
            self.assertIn("encode", resumed.reused["alpha"])
            self.assertNotIn("encode", resumed.rebuilt["alpha"])
            self.assertTrue(verify_against_full(root)["ok"])

    def test_invalidation_starts_at_earliest_affected_stage(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = make_workspace(Path(temp) / "workspace")
            build_workspace(root)

            update_shard(root, "alpha", {"prompt": "Changed prompt text"}, build=False)
            diagnostics = inspect_workspace(root=root)
            alpha = _diag(diagnostics, "alpha")
            beta = _diag(diagnostics, "beta")
            self.assertEqual(alpha.earliest_invalid, "encode")
            self.assertIn("normalized_prompt", alpha.stages[0].changed_fields)
            self.assertIsNone(beta.earliest_invalid)
            report = build_workspace(root)
            self.assertEqual(report.rebuilt["alpha"], ["encode", "schedule", "sample", "quantize"])
            self.assertEqual(report.reused["beta"], ["encode", "schedule", "sample", "quantize"])

            update_shard(root, "alpha", {"guidance_scale": 8.0}, build=False)
            alpha = _diag(inspect_workspace(root=root), "alpha")
            self.assertEqual(alpha.earliest_invalid, "schedule")
            self.assertFalse(alpha.stages[0].invalid)
            self.assertIn("guidance_scale", alpha.stages[1].changed_fields)
            report = build_workspace(root)
            self.assertEqual(report.reused["alpha"], ["encode"])
            self.assertEqual(report.rebuilt["alpha"], ["schedule", "sample", "quantize"])

            update_shard(root, "alpha", {"seed": 9999}, build=False)
            alpha = _diag(inspect_workspace(root=root), "alpha")
            self.assertEqual(alpha.earliest_invalid, "sample")
            report = build_workspace(root)
            self.assertEqual(report.reused["alpha"], ["encode", "schedule"])
            self.assertEqual(report.rebuilt["alpha"], ["sample", "quantize"])

            update_shard(root, "beta", {"blend_strength": 0.8}, build=False)
            beta = _diag(inspect_workspace(root=root), "beta")
            self.assertEqual(beta.earliest_invalid, "sample")
            report = build_workspace(root)
            self.assertEqual(report.reused["beta"], ["encode", "schedule"])
            self.assertEqual(report.rebuilt["beta"], ["sample", "quantize"])

            raw = yaml.safe_load((root / "config.yaml").read_text("utf-8"))
            raw["global"]["quantize"]["png_compression"] = 6
            write_config(root / "config.yaml", raw)
            report = build_workspace(root)
            self.assertEqual(report.rebuilt["alpha"], ["quantize"])
            self.assertEqual(report.rebuilt["beta"], ["quantize"])

            raw = yaml.safe_load((root / "config.yaml").read_text("utf-8"))
            raw["shards"][0]["steps"] = 7
            write_config(root / "config.yaml", raw)
            report = build_workspace(root)
            self.assertEqual(report.reused["alpha"], ["encode"])
            self.assertEqual(report.rebuilt["alpha"], ["schedule", "sample", "quantize"])

            raw = yaml.safe_load((root / "config.yaml").read_text("utf-8"))
            raw["global"]["encoder"]["domain"] = "detdiff.tests.v2"
            write_config(root / "config.yaml", raw)
            report = build_workspace(root)
            self.assertEqual(report.rebuilt["alpha"], ["encode", "schedule", "sample", "quantize"])
            self.assertEqual(report.rebuilt["beta"], ["encode", "schedule", "sample", "quantize"])
            self.assertTrue(verify_against_full(root)["ok"])

    def test_reference_content_change_only_rebuilds_sample_and_quantize(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = make_workspace(Path(temp) / "workspace")
            build_workspace(root)
            replacement = Path(temp) / "replacement.png"
            create_image(replacement, 77)
            update_shard(root, "beta", reference_file=replacement)
            diagnostics = inspect_workspace(root=root)
            beta = _diag(diagnostics, "beta")
            self.assertIsNone(beta.earliest_invalid)
            self.assertTrue(verify_against_full(root)["ok"])
            config = load_config(root / "config.yaml")
            self.assertRegex(config.shard("beta").reference or "", r"^references/beta-[0-9a-f]{16}\.png$")

            create_image(replacement, 78)
            result = update_shard(root, "beta", reference_file=replacement)
            self.assertEqual(result["build"]["rebuilt"]["beta"], ["sample", "quantize"])
            self.assertEqual(result["build"]["reused"]["beta"], ["encode", "schedule"])
            self.assertTrue(verify_against_full(root)["ok"])

    def test_missing_or_wrong_size_reference_creates_no_output(self) -> None:
        for kwargs in ({"missing_reference": True}, {"reference_size": (12, 19)}):
            with self.subTest(kwargs=kwargs), tempfile.TemporaryDirectory() as temp:
                root = make_workspace(Path(temp) / "workspace", **kwargs)
                with self.assertRaises(BusinessError) as context:
                    build_workspace(root)
                self.assertEqual(context.exception.code, ErrorCode.E_MISSING_REFERENCE)
                self.assertEqual(context.exception.shard, "beta")
                self.assertFalse((root / "outputs" / "alpha.png").exists())
                self.assertFalse((root / "outputs" / "beta.png").exists())
                checkpoint_dir = root / ".state" / "shards"
                checkpoints = list(checkpoint_dir.glob("*/checkpoints/*.json")) if checkpoint_dir.exists() else []
                self.assertEqual(checkpoints, [])

    def test_invalid_updates_do_not_mutate_config(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = make_workspace(Path(temp) / "workspace")
            build_workspace(root)
            before = (root / "config.yaml").read_bytes()
            with self.assertRaises(BusinessError) as context:
                update_shard(root, "alpha", {"blend_strength": 1.01})
            self.assertEqual(context.exception.code, ErrorCode.E_INVALID_CONFIG)
            self.assertEqual(context.exception.field, "blend_strength")
            self.assertEqual((root / "config.yaml").read_bytes(), before)

            raw = yaml.safe_load(before)
            raw["shards"][1]["reference"] = "references/absent.png"
            candidate = Path(temp) / "candidate.yaml"
            write_config(candidate, raw)
            (root / "config.yaml").write_bytes(candidate.read_bytes())
            before_missing = (root / "config.yaml").read_bytes()
            with self.assertRaises(BusinessError) as context:
                update_shard(root, "alpha", {"guidance_scale": 3.0})
            self.assertEqual(context.exception.code, ErrorCode.E_MISSING_REFERENCE)
            self.assertEqual((root / "config.yaml").read_bytes(), before_missing)

    def test_inconsistency_guard_does_not_publish_or_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = make_workspace(Path(temp) / "workspace")
            build_workspace(root)
            update_shard(root, "alpha", {"prompt": "will be guarded"}, build=False)
            before_output = sha256_file(root / "outputs" / "alpha.png")
            before_records = {
                path.relative_to(root).as_posix(): sha256_file(path)
                for path in root.glob(".state/shards/*/checkpoints/*.json")
            }
            original = compute_shard_from_scratch

            def altered(shard, config, reference):
                computed = original(shard, config, reference)
                if shard.id != "alpha":
                    return computed
                stages = dict(computed.stages)
                stages["sample"] = type(stages["sample"])(
                    artifact=stages["sample"].artifact + b"tampered",
                    input_fingerprint=stages["sample"].input_fingerprint,
                    input_meta=stages["sample"].input_meta,
                    output_sha256=stages["sample"].output_sha256,
                )
                return ComputedShard(stages=stages, png_bytes=computed.png_bytes)

            with mock.patch("detdiff.pipeline.compute_shard_from_scratch", side_effect=altered):
                with self.assertRaises(BusinessError) as context:
                    build_workspace(root)
            self.assertEqual(context.exception.code, ErrorCode.E_INCONSISTENT_STATE)
            self.assertEqual(sha256_file(root / "outputs" / "alpha.png"), before_output)
            after_records = {
                path.relative_to(root).as_posix(): sha256_file(path)
                for path in root.glob(".state/shards/*/checkpoints/*.json")
            }
            self.assertEqual(before_records, after_records)

    def test_incomplete_temp_files_are_ignored_and_missing_stage_recovers(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = make_workspace(Path(temp) / "workspace")
            build_workspace(root)
            store = CheckpointStore(root)
            temp_file = store.checkpoint_path("alpha", "schedule").parent / ".tmp-interrupted-write"
            temp_file.write_bytes(b'{"truncated":')
            report = build_workspace(root)
            self.assertEqual(report.rebuilt, {})
            self.assertTrue(verify_against_full(root)["ok"])

            store.checkpoint_path("alpha", "quantize").unlink()
            report = build_workspace(root)
            self.assertEqual(report.rebuilt["alpha"], ["quantize"])
            self.assertEqual(report.reused["alpha"], ["encode", "schedule", "sample"])
            self.assertTrue(verify_against_full(root)["ok"])


def started_stages(report) -> dict[str, list[str]]:
    return {
        shard: report.rebuilt.get(shard, []) + report.reused.get(shard, [])
        for shard in ("alpha", "beta")
    }


def _diag(diagnostics, shard_id):
    return next(item for item in diagnostics if item.shard.id == shard_id)


if __name__ == "__main__":
    unittest.main()

