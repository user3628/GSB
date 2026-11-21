from __future__ import annotations

import tempfile
import types
import unittest
from pathlib import Path

from shardseek.errors import BusinessError
from shardseek.pipeline import Pipeline
from shardseek.search import run_search
from shardseek.util import write_atomic
from shardseek.verification import verify_stop_resume

from tests.helpers import MANIFEST, PROJECT_ROOT, build_samples, workspace_for
from shardseek.config import load_config


class SnapshotAndResumeTests(unittest.TestCase):
    def test_old_representation_is_rejected_as_snapshot_regression(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace, _ = build_samples(Path(directory) / "workspace", "baseline_8bit")
            old_representation = workspace.representation_path.read_bytes()
            workspace.set_config("compact_4bit")
            Pipeline(workspace).build(mode="incremental")
            current_representation = workspace.representation_path.read_bytes()
            write_atomic(workspace.representation_path, old_representation)
            with self.assertRaises(BusinessError) as caught:
                run_search(workspace, direction="text_to_image", query_text="red", top_k=2)
            self.assertEqual(caught.exception.code, "E_SNAPSHOT_REGRESSION")
            write_atomic(workspace.representation_path, current_representation)

            summary = __import__("json").loads(workspace.representation_summary_path.read_text())
            summary["items"][0]["item_digest"] = "changed"
            write_atomic(
                workspace.representation_summary_path,
                __import__("json").dumps(summary, sort_keys=True).encode() + b"\n",
            )
            with self.assertRaises(BusinessError) as caught:
                run_search(workspace, direction="text_to_image", query_text="red", top_k=2)
            self.assertEqual(caught.exception.code, "E_SNAPSHOT_MISMATCH")

    def test_index_from_another_representation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace, _ = build_samples(Path(directory) / "workspace", "baseline_8bit")
            old_index = workspace.index_path.read_bytes()
            workspace.set_config("compact_4bit")
            Pipeline(workspace).build(mode="incremental")
            current_index = workspace.index_path.read_bytes()
            write_atomic(workspace.index_path, old_index)
            with self.assertRaises(BusinessError) as caught:
                run_search(workspace, direction="text_to_image", query_text="red", top_k=2)
            self.assertEqual(caught.exception.code, "E_SNAPSHOT_REGRESSION")
            write_atomic(workspace.index_path, current_index)

            index_payload = __import__("json").loads(workspace.index_path.read_text())
            index_payload["dense"][0]["visual_q"][0] += 1
            write_atomic(
                workspace.index_path,
                __import__("json").dumps(index_payload, sort_keys=True).encode() + b"\n",
            )
            with self.assertRaises(BusinessError) as caught:
                run_search(workspace, direction="text_to_image", query_text="red", top_k=2)
            self.assertEqual(caught.exception.code, "E_SNAPSHOT_MISMATCH")

    def test_stop_resume_matches_full_and_sequence_is_continuous(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = verify_stop_resume(
                parent_root=Path(directory) / "verify",
                manifest_path=MANIFEST,
                project_root=PROJECT_ROOT,
                stop_after_shards=2,
            )
            self.assertTrue(result["all_equal"])
            self.assertTrue(result["checks"]["checkpoint_sequence_continuous"])
            self.assertTrue(result["checks"]["checkpoint_count_expected"])

    def test_request_stop_is_honored_at_shard_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = workspace_for(Path(directory) / "workspace")
            workspace.import_manifest(MANIFEST)
            calls = {"count": 0}

            def stop_after_first_shard(self) -> bool:
                calls["count"] += 1
                return calls["count"] >= 1

            workspace.stop_requested = types.MethodType(stop_after_first_shard, workspace)
            stopped = Pipeline(workspace).build(mode="full")
            self.assertTrue(stopped["stopped"])
            self.assertEqual(stopped["completed_shards"], 1)
            del workspace.stop_requested
            resumed = Pipeline(workspace).build(mode="resume")
            self.assertFalse(resumed["stopped"])
            self.assertEqual(resumed["checkpoints"]["record_count"], 28)

            full = workspace_for(Path(directory) / "full")
            full.import_manifest(MANIFEST)
            Pipeline(full).build(mode="full")
            self.assertEqual(
                workspace.representation_summary_path.read_bytes(),
                full.representation_summary_path.read_bytes(),
            )


if __name__ == "__main__":
    unittest.main()

