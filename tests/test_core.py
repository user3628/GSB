from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import numpy as np

from cmir_alignment.canonical import digest_obj
from cmir_alignment.configs import default_config
from cmir_alignment.datagen import generate_dataset
from cmir_alignment.dataset import Dataset
from cmir_alignment.descriptors import extract_text, extract_visual
from cmir_alignment.engine import RetrievalEngine
from cmir_alignment.errors import (
    CMIRError,
    E_INCREMENTAL_MISMATCH,
    E_INVALID_BITWIDTH,
    E_INVALID_QUERY,
    E_MISSING_INPUT,
    E_NUMERIC,
    E_PROJECTION_INVALID,
    E_QUANTIZATION_ZERO_SCALE,
)


class CoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="cmir-test-")
        self.root = Path(self.temp.name)
        self.data = self.root / "data"
        self.generated = generate_dataset(
            self.data, shards=3, items_per_shard=4, seed=20261122
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def engine(self, *, bits: int = 8, granularity: str = "per_dim", name: str = "ws"):
        engine = RetrievalEngine(
            self.root / name,
            dataset_dir=self.data,
            config=default_config(bits, granularity),
        )
        engine.import_dataset(self.data)
        return engine

    def test_generator_and_feature_determinism(self) -> None:
        second = self.root / "data2"
        generated2 = generate_dataset(
            second, shards=3, items_per_shard=4, seed=20261122
        )
        self.assertEqual(self.generated["digest"], generated2["digest"])
        for relative in (
            "dataset.json",
            "manifest.jsonl",
            "shards/shard-00/data.jsonl",
            "shards/shard-00/images/shard-00-item-000.png",
        ):
            self.assertEqual(
                (self.data / relative).read_bytes(), (second / relative).read_bytes()
            )
        dataset = Dataset.load(self.data)
        dataset.assert_valid()
        item = dataset.items[0]
        config = default_config(8).feature
        visual_a = extract_visual(item.image_path, config, item_id=item.item_id)
        visual_b = extract_visual(item.image_path, config, item_id=item.item_id)
        self.assertEqual(visual_a["summary_digest"], visual_b["summary_digest"])
        self.assertEqual(visual_a["vector"].shape, (224,))
        self.assertEqual(len(visual_a["blocks"]), 16)
        text_a = extract_text(item.caption, config, item_id=item.item_id)
        text_b = extract_text(item.caption, config, item_id=item.item_id)
        self.assertEqual(text_a["summary_digest"], text_b["summary_digest"])
        self.assertEqual(text_a["vector"].shape, (512,))
        self.assertTrue(any(row["kind"] == "word" for row in text_a["features"]))
        self.assertTrue(any(row["kind"].startswith("char_") for row in text_a["features"]))
        for row in text_a["features"]:
            self.assertLessEqual(0, row["offset_start"])
            self.assertLess(row["offset_start"], row["offset_end"])
            self.assertGreater(row["weight"], 0)
            self.assertGreaterEqual(row["hash_bin"], 0)
            self.assertLess(row["hash_bin"], 512)
        self.assertIn("red", {row["value"] for row in text_a["features"]})

    def test_missing_image_and_empty_caption_are_diagnostic_errors(self) -> None:
        dataset = Dataset.load(self.data)
        first = sorted(dataset.items, key=lambda item: item.item_id)[0]
        manifest = self.data / "shards" / "shard-00" / "data.jsonl"
        rows = [json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines() if line]
        rows[0]["caption"] = "  "
        manifest.write_text(
            "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows),
            encoding="utf-8",
        )
        engine = RetrievalEngine(self.root / "ws-empty", dataset_dir=self.data)
        with self.assertRaises(CMIRError) as caught:
            engine.import_dataset(self.data)
        self.assertEqual(caught.exception.code, E_MISSING_INPUT)
        self.assertEqual(caught.exception.details["field"], "caption")
        self.assertTrue(caught.exception.details["diagnostics"])
        diagnostic_path = self.root / "ws-empty" / "diagnostics" / "last_error.json"
        self.assertTrue(diagnostic_path.is_file())
        stored = json.loads(diagnostic_path.read_text(encoding="utf-8"))
        self.assertEqual(stored["error"]["code"], E_MISSING_INPUT)

        first.image_path.unlink()
        engine2 = RetrievalEngine(self.root / "ws-missing", dataset_dir=self.data)
        with self.assertRaises(CMIRError) as caught2:
            engine2.import_dataset(self.data)
        self.assertEqual(caught2.exception.code, E_MISSING_INPUT)
        self.assertTrue(
            any(
                row.get("field") in ("caption", "image_file")
                for row in caught2.exception.details["diagnostics"]
            )
        )

    def test_projection_bitwidth_and_zero_scale_errors(self) -> None:
        engine = self.engine(name="config-ws")
        wrong_visual = [[0.0] * 10 for _ in range(64)]
        with self.assertRaises(CMIRError) as caught:
            engine.set_config(patch={"projection": {"visual_matrix": wrong_visual}})
        self.assertEqual(caught.exception.code, E_PROJECTION_INVALID)
        self.assertIn("dimensions", caught.exception.details)
        self.assertEqual(caught.exception.details["item_id"], "__global__")
        nan_matrix = [[float("nan")] * 224 for _ in range(64)]
        with self.assertRaises(CMIRError) as caught_nan:
            engine.set_config(patch={"projection": {"visual_matrix": nan_matrix}})
        self.assertEqual(caught_nan.exception.code, E_PROJECTION_INVALID)
        self.assertGreater(caught_nan.exception.details["nonfinite_values"], 0)
        with self.assertRaises(CMIRError) as caught_bits:
            engine.set_config(patch={"bits": 6})
        self.assertEqual(caught_bits.exception.code, E_INVALID_BITWIDTH)
        self.assertEqual(caught_bits.exception.details["bitwidth"], 6)

        ones_visual = [[1.0] * 224 for _ in range(64)]
        ones_text = [[1.0] * 512 for _ in range(64)]
        engine.set_config(
            patch={
                "projection": {
                    "visual_matrix": ones_visual,
                    "text_matrix": ones_text,
                },
                "bits": 8,
                "granularity": "per_dim",
            }
        )
        with self.assertRaises(CMIRError) as caught_zero:
            engine.build()
        self.assertEqual(caught_zero.exception.code, E_QUANTIZATION_ZERO_SCALE)
        self.assertIn("dim", caught_zero.exception.details)

    def test_numeric_error_when_bound_limit_is_exceeded(self) -> None:
        engine = self.engine(name="numeric-ws")
        engine.set_config(patch={"recall": {"max_error_bound": 1e-12}})
        with self.assertRaises(CMIRError) as caught:
            engine.build()
        self.assertEqual(caught.exception.code, E_NUMERIC)
        self.assertEqual(caught.exception.details["stage"], "similarity")
        self.assertIn("error_bound", caught.exception.details)

    def test_bidirectional_retrieval_determinism_and_query_errors(self) -> None:
        engine = self.engine(name="query-ws")
        engine.build()
        dataset = Dataset.load(self.data)
        first = sorted(dataset.items, key=lambda item: item.item_id)[0]
        first_copy = engine.search(mode="text2image", query=first.caption, k=5)
        second_copy = engine.search(mode="text2image", query=first.caption, k=5)
        self.assertEqual(digest_obj(first_copy), digest_obj(second_copy))
        self.assertEqual(first_copy["top_k"][0]["item_id"], first.item_id)
        scores = [row["score"] for row in first_copy["top_k"]]
        self.assertEqual(scores, sorted(scores, reverse=True))
        for row in first_copy["top_k"]:
            self.assertIn("visual_route_bound", row["error_propagation"])
            self.assertIn("text_route_bound", row["error_propagation"])
            self.assertIn("effective_digits", row["error_propagation"])
            self.assertIn("visual", row["log_contributions"])
            self.assertIn("text", row["log_contributions"])
            self.assertEqual(row["source_shard"], row["item_id"][:8])
        self.assertIn("by_shard_prefix", first_copy["diagnostics"]["corpus_error"])
        reverse = engine.search(mode="image2text", query_item_id=first.item_id, k=5)
        self.assertEqual(reverse["top_k"][0]["item_id"], first.item_id)
        for body, expected in (
            ({"mode": "text2image", "query": "   ", "k": 2}, "empty"),
            ({"mode": "text2image", "query": "red", "k": 99}, "k"),
            ({"mode": "text2image", "query_vector": [0.0] * 63, "k": 2}, "dimension"),
            ({"mode": "bogus", "query": "red", "k": 2}, "mode"),
        ):
            with self.assertRaises(CMIRError) as caught:
                engine.search(**body)
            self.assertEqual(caught.exception.code, E_INVALID_QUERY)

    def test_bitwidth_incremental_invalidation_and_full_consistency(self) -> None:
        engine = self.engine(name="inc-ws", bits=8)
        initial = engine.build()
        self.assertEqual(initial["status"], "complete")
        initial_state = json.loads(
            (self.root / "inc-ws" / "state.json").read_text(encoding="utf-8")
        )
        original_feature_digests = {
            shard_id: row["feature"]["digest"]
            for shard_id, row in initial_state["shards"].items()
        }
        engine.set_config(patch={"bits": 4})
        rebuilt = engine.build()
        self.assertEqual(rebuilt["status"], "complete")
        invalidation = rebuilt["invalidations"][-1]
        self.assertEqual(invalidation["earliest_stage"], "projection_quant")
        self.assertEqual(invalidation["reason"], "quantization_changed")
        self.assertEqual(
            invalidation["reused_stages"]["feature"],
            ["shard-00", "shard-01", "shard-02"],
        )
        state = json.loads((self.root / "inc-ws" / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(
            original_feature_digests,
            {shard_id: row["feature"]["digest"] for shard_id, row in state["shards"].items()},
        )
        verification = engine.verify_full_consistency()
        self.assertTrue(verification["consistent"])
        self.assertEqual(
            verification["full_run_digest"], verification["incremental_run_digest"]
        )

    def test_single_shard_update_reuses_unaffected_feature_artifacts(self) -> None:
        engine = self.engine(name="shard-ws", bits=8, granularity="per_vector")
        initial = engine.build()
        initial_state = json.loads(
            (self.root / "shard-ws" / "state.json").read_text(encoding="utf-8")
        )
        before = {
            shard_id: row["feature"]["digest"]
            for shard_id, row in initial_state["shards"].items()
        }
        manifest = self.data / "shards" / "shard-01" / "data.jsonl"
        rows = [json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines() if line]
        rows[0]["caption"] = "深色 dark 蓝色 blue 圆形 circle 视角"
        manifest.write_text(
            "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows),
            encoding="utf-8",
        )
        updated = engine.update_shard("shard-01")
        invalidation = updated["invalidations"][-1]
        self.assertEqual(invalidation["earliest_stage"], "feature")
        self.assertEqual(invalidation["feature_rebuild"], ["shard-01"])
        self.assertEqual(invalidation["reused_stages"]["feature"], ["shard-00", "shard-02"])
        updated_state = json.loads(
            (self.root / "shard-ws" / "state.json").read_text(encoding="utf-8")
        )
        after = {
            shard_id: row["feature"]["digest"]
            for shard_id, row in updated_state["shards"].items()
        }
        self.assertEqual(before["shard-00"], after["shard-00"])
        self.assertEqual(before["shard-02"], after["shard-02"])
        self.assertNotEqual(before["shard-01"], after["shard-01"])
        self.assertTrue(engine.verify_full_consistency()["consistent"])

    def test_ranking_change_starts_at_index_and_reuses_upstream(self) -> None:
        engine = self.engine(name="ranking-ws", bits=8)
        initial = engine.build()
        initial_state = json.loads(
            (self.root / "ranking-ws" / "state.json").read_text(encoding="utf-8")
        )
        engine.set_config(patch={"ranking": {"inverted_hit_weight": 0.21}})
        rebuilt = engine.build()
        invalidation = rebuilt["invalidations"][-1]
        self.assertEqual(invalidation["earliest_stage"], "index")
        self.assertEqual(invalidation["reason"], "ranking_changed")
        self.assertEqual(invalidation["index_rebuild"], ["shard-00", "shard-01", "shard-02"])
        self.assertEqual(
            invalidation["reused_stages"]["feature"],
            ["shard-00", "shard-01", "shard-02"],
        )
        self.assertEqual(
            invalidation["reused_stages"]["projection_quant"],
            ["shard-00", "shard-01", "shard-02"],
        )
        final_state = json.loads(
            (self.root / "ranking-ws" / "state.json").read_text(encoding="utf-8")
        )
        for shard_id in initial_state["shards"]:
            self.assertEqual(
                initial_state["shards"][shard_id]["feature"]["digest"],
                final_state["shards"][shard_id]["feature"]["digest"],
            )
            self.assertEqual(
                initial_state["shards"][shard_id]["projection_quant"]["digest"],
                final_state["shards"][shard_id]["projection_quant"]["digest"],
            )
            self.assertNotEqual(
                initial_state["shards"][shard_id]["index"]["digest"],
                final_state["shards"][shard_id]["index"]["digest"],
            )
        self.assertTrue(engine.verify_full_consistency()["consistent"])

    def test_stop_resume_matches_from_scratch_run(self) -> None:
        interrupted = self.engine(name="resume-ws")
        stopped = interrupted.build(stop_after_units=4)
        self.assertEqual(stopped["status"], "stopped")
        self.assertEqual(stopped["published_units"], 4)
        self.assertEqual(len(interrupted.checkpoints.recover().records), 4)
        resumed = interrupted.resume()
        self.assertEqual(resumed["status"], "complete")
        records = interrupted.checkpoints.recover().records
        self.assertEqual([row["seq"] for row in records], list(range(1, len(records) + 1)))
        full = self.engine(name="fresh-ws")
        full_result = full.build()
        self.assertEqual(resumed["run_digest"], full_result["run_digest"])
        self.assertTrue(interrupted.verify_full_consistency()["consistent"])

    def test_incremental_mismatch_error_contains_difference_path(self) -> None:
        from cmir_alignment.canonical import atomic_write_json

        engine = self.engine(name="mismatch-ws")
        engine.build()
        state_path = self.root / "mismatch-ws" / "state.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state["run_digest"] = "0" * 64
        atomic_write_json(state_path, state)
        with self.assertRaises(CMIRError) as caught:
            engine.verify_full_consistency()
        self.assertEqual(caught.exception.code, E_INCREMENTAL_MISMATCH)
        self.assertIn("path", caught.exception.details["difference"])
        self.assertIn("run_digest", caught.exception.details["difference"]["path"])


if __name__ == "__main__":
    unittest.main()
