from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from shardseek.config import BASE_FEATURE_CONFIG, build_config, config_fingerprint
from shardseek.errors import BusinessError
from shardseek.features import extract_visual_features
from shardseek.generator import generate_samples
from shardseek.pipeline import Pipeline
from shardseek.search import corpus_diagnostics, run_search
from shardseek.util import canonical_bytes, write_json_atomic

from tests.helpers import MANIFEST, PROJECT_ROOT, build_samples, workspace_for


class PipelineSearchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.TemporaryDirectory()
        cls.workspace, cls.build = build_samples(Path(cls.tmp.name) / "baseline")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.tmp.cleanup()

    def test_bidirectional_search_is_repeatable_and_decomposed(self) -> None:
        first = run_search(
            self.workspace,
            direction="text_to_image",
            query_text="红色方块 blue gradient",
            top_k=5,
        )
        second = run_search(
            self.workspace,
            direction="text_to_image",
            query_text="红色方块 blue gradient",
            top_k=5,
        )
        self.assertEqual(canonical_bytes(first), canonical_bytes(second))
        self.assertEqual(first["results"][0]["item_id"], "item-06")
        self.assertIn(first["results"][1]["item_id"], {"item-01", "item-06"})
        for result in first["results"]:
            self.assertTrue(result["within_declared_bound"])
            self.assertIn("visual", result["log_contributions"])
            self.assertIn("text", result["log_contributions"])
            self.assertIn("visual", result["score_components"])
            self.assertIn("text", result["score_components"])

        image_report = run_search(
            self.workspace,
            direction="image_to_text",
            image_item_id="item-01",
            top_k=3,
        )
        self.assertEqual(image_report["results"][0]["item_id"], "item-01")
        self.assertNotEqual(first["report_sha256"], image_report["report_sha256"])

    def test_ties_break_by_item_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            generate_samples(root)
            image = str((root / "images" / "item-01.png").resolve())
            manifest = root / "ties.jsonl"
            rows = [
                {
                    "item_id": item_id,
                    "image_file": image,
                    "caption": "red block blue gradient",
                    "lang": "en",
                    "source": f"shard-{index}",
                }
                for index, item_id in enumerate(("item-b", "item-a"))
            ]
            manifest.write_text(
                "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
                encoding="utf-8",
            )
            workspace = workspace_for(root / "runtime")
            workspace.import_manifest(manifest)
            Pipeline(workspace).build(mode="full")
            report = run_search(
                workspace,
                direction="text_to_image",
                query_text="red block blue gradient",
                top_k=2,
            )
            self.assertEqual([result["item_id"] for result in report["results"]], ["item-a", "item-b"])
            self.assertEqual(report["results"][0]["combined_score"], report["results"][1]["combined_score"])

    def test_retrieval_business_errors_are_stable(self) -> None:
        with self.assertRaises(BusinessError) as caught:
            run_search(
                self.workspace,
                direction="text_to_image",
                query_text="red",
                top_k=999,
            )
        self.assertEqual(caught.exception.code, "E_INVALID_K")
        self.assertEqual(caught.exception.details["index_size"], 7)

        with self.assertRaises(BusinessError) as caught:
            run_search(self.workspace, direction="text_to_image", query_text="  ", top_k=2)
        self.assertEqual(caught.exception.code, "E_QUERY_EMPTY")

        with tempfile.TemporaryDirectory() as directory:
            empty = workspace_for(Path(directory) / "empty")
            empty.import_manifest(MANIFEST)
            with self.assertRaises(BusinessError) as caught:
                run_search(empty, direction="text_to_image", query_text="red", top_k=2)
            self.assertEqual(caught.exception.code, "E_INDEX_NOT_READY")

    def test_four_bit_bound_exceeds_eight_bit_and_series_tracks_shards(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            compact, _ = build_samples(Path(directory) / "compact", "compact_4bit")
            baseline_diag = corpus_diagnostics(self.workspace)
            compact_diag = corpus_diagnostics(compact)
            self.assertEqual([row["shard_count"] for row in baseline_diag["series"]], list(range(1, 8)))
            self.assertGreater(
                compact_diag["series"][-1]["max_similarity_error_bound"],
                baseline_diag["series"][-1]["max_similarity_error_bound"],
            )
            self.assertGreaterEqual(compact_diag["series"][-1]["pair_count"], 49)

            with self.assertRaises(BusinessError) as caught:
                run_search(
                    self.workspace,
                    direction="text_to_image",
                    query_text="red",
                    top_k=2,
                    weights={"dense_weight": 0.5, "inverted_weight": 0.5},
                )
            self.assertEqual(caught.exception.code, "E_SNAPSHOT_MISMATCH")

    def test_per_vector_quantization_builds_and_searches(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            import copy

            from tests.helpers import baseline_config

            workspace = workspace_for(Path(directory) / "per-vector")
            workspace.import_manifest(MANIFEST)
            config = copy.deepcopy(baseline_config())
            config["name"] = "per-vector-8bit"
            config["quantization"]["granularity"] = "per_vector"
            workspace.set_config(config)
            built = Pipeline(workspace).build(mode="full")
            self.assertFalse(built["stopped"])
            report = run_search(
                workspace,
                direction="text_to_image",
                query_text="红色方块 blue gradient",
                top_k=3,
            )
            self.assertTrue(all(result["within_declared_bound"] for result in report["results"]))
            representation = json.loads(workspace.representation_path.read_text(encoding="utf-8"))
            self.assertEqual(representation["quantization"]["granularity"], "per_vector")
            self.assertEqual(len(representation["items"][0]["visual_quantization"]["dims"]), 64)


if __name__ == "__main__":
    unittest.main()

