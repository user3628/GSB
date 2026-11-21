from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

from shardseek.config import config_fingerprint
from shardseek.errors import BusinessError
from shardseek.pipeline import Pipeline
from shardseek.search import run_search
from shardseek.util import canonical_bytes, read_json, write_json_atomic
from shardseek.verification import _compare_or_raise, verify_incremental

from tests.helpers import MANIFEST, PROJECT_ROOT, baseline_config, build_samples, workspace_for


class IncrementalTests(unittest.TestCase):
    def test_quantization_change_reuses_features_and_matches_full_build(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = verify_incremental(
                parent_root=Path(directory) / "verify",
                manifest_path=MANIFEST,
                project_root=PROJECT_ROOT,
            )
            self.assertTrue(result["all_equal"])
            self.assertEqual(result["incremental_build"]["earliest_stage"], "project_quantize")
            self.assertEqual(result["incremental_build"]["reused_stages"], ["feature_extract"])
            self.assertTrue(result["comparisons"]["representation_summary_bytes_equal"])
            self.assertTrue(result["comparisons"]["index_summary_bytes_equal"])
            self.assertTrue(result["comparisons"]["search_reports_bytes_equal"])

    def test_ranking_change_starts_at_index_and_matches_full(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace, baseline_build = build_samples(root / "incremental")
            changed = copy.deepcopy(baseline_config())
            changed["name"] = "baseline-ranking-changed"
            changed["ranking"]["dense_weight"] = 0.5
            changed["ranking"]["inverted_weight"] = 0.5
            workspace.set_config(changed)
            incremental = Pipeline(workspace).build(mode="incremental")
            self.assertEqual(incremental["earliest_stage"], "index_build")
            self.assertEqual(incremental["reused_stages"], ["feature_extract", "project_quantize"])
            self.assertEqual(
                incremental["representation_fingerprint"], baseline_build["representation_fingerprint"]
            )

            full_workspace = workspace_for(root / "full")
            full_workspace.import_manifest(MANIFEST)
            full_workspace.set_config(changed)
            full = Pipeline(full_workspace).build(mode="full")
            self.assertEqual(incremental["representation_fingerprint"], full["representation_fingerprint"])
            self.assertEqual(incremental["index_fingerprint"], full["index_fingerprint"])
            self.assertEqual(
                workspace.representation_summary_path.read_bytes(),
                full_workspace.representation_summary_path.read_bytes(),
            )
            self.assertEqual(workspace.index_summary_path.read_bytes(), full_workspace.index_summary_path.read_bytes())
            for direction, kwargs in (
                ("text_to_image", {"query_text": "红色方块 blue gradient"}),
                ("image_to_text", {"image_item_id": "item-01"}),
            ):
                left = canonical_bytes(run_search(workspace, direction=direction, top_k=4, **kwargs))
                right = canonical_bytes(run_search(full_workspace, direction=direction, top_k=4, **kwargs))
                self.assertEqual(left, right)

    def test_caption_change_starts_at_feature_stage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace, _ = build_samples(root / "incremental")
            rows = [json.loads(line) for line in MANIFEST.read_text(encoding="utf-8").splitlines() if line]
            rows[0]["caption"] = "changed caption 红色方块"
            for row in rows:
                row["image_file"] = str((MANIFEST.parent / row["image_file"]).resolve())
            changed_manifest = root / "changed.jsonl"
            changed_manifest.write_text(
                "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows),
                encoding="utf-8",
            )
            workspace.import_manifest(changed_manifest)
            incremental = Pipeline(workspace).build(mode="incremental")
            self.assertEqual(incremental["earliest_stage"], "feature_extract")
            self.assertEqual(incremental["reused_stages"], [])

    def test_projection_change_reextracts_nothing_but_reprojects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace, _ = build_samples(Path(directory) / "workspace")
            workspace.set_config("projection_alt_8bit")
            result = Pipeline(workspace).build(mode="incremental")
            self.assertEqual(result["earliest_stage"], "project_quantize")
            self.assertEqual(result["reused_stages"], ["feature_extract"])

    def test_incremental_mismatch_error_carries_stage_and_first_item(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            left, _ = build_samples(root / "left")
            right, _ = build_samples(root / "right")
            summary = read_json(right.representation_summary_path)
            summary["items"][0]["item_digest"] = "tampered"
            write_json_atomic(right.representation_summary_path, summary)
            with self.assertRaises(BusinessError) as caught:
                _compare_or_raise(
                    inc_workspace=left,
                    full_workspace=right,
                    inc_build=Pipeline(left).build(mode="incremental"),
                    full_build={"item_count": 7},
                )
            self.assertEqual(caught.exception.code, "E_INCREMENTAL_MISMATCH")
            self.assertEqual(caught.exception.stage, "representation")
            self.assertIsNotNone(caught.exception.item_id)


if __name__ == "__main__":
    unittest.main()

