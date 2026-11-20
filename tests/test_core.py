"""核心确定性、错误码与原子存储测试。"""

import json
import os
import tempfile
import unittest

import numpy as np

from mmpipe import STAGES, text_encoding, visual_alignment, fusion_scoring
from mmpipe.codec import canonical_dumps
from mmpipe.errors import (
    E_MISSING_INPUT,
    E_NUMERIC,
    E_SHAPE_MISMATCH,
    E_STALE_CHECKPOINT,
    E_INVALID_STAGE,
    PipeError,
    StageFailure,
)
from mmpipe.samplegen import generate
from mmpipe.storage import CP_DIR, Workspace
from mmpipe import pipeline
from mmpipe.pipeline import StopController, expected_inputs, run_pipeline


class BaseCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="mmpipe-test-")
        self.sample_dir = os.path.join(self.tmp, "samples")
        generate(self.sample_dir)
        self.ws_dir = os.path.join(self.tmp, "ws")
        self.workspace = Workspace(self.ws_dir)
        with open(os.path.join(self.sample_dir, "pipeline_config.json"), "rb") as f:
            self.config = json.loads(f.read())
        self.texts = pipeline.load_texts(os.path.join(self.sample_dir, "texts.jsonl"))
        self.images = pipeline.load_images(os.path.join(self.sample_dir, "images.json"))

    def run_full(self, **kwargs):
        return run_pipeline(self.workspace, self.config, self.texts,
                            self.images, StopController(self.workspace), **kwargs)


class TestDeterminism(BaseCase):
    def test_text_embedding_byte_stable(self):
        first = text_encoding.hash_embedding("  Multi-Modal  多模态 ", 64)
        second = text_encoding.hash_embedding("  Multi-Modal  多模态 ", 64)
        np.testing.assert_array_equal(first[0], second[0])
        bytes_a = canonical_dumps([float(x) for x in first[0]])
        bytes_b = canonical_dumps([float(x) for x in second[0]])
        self.assertEqual(bytes_a, bytes_b)
        self.assertAlmostEqual(float(np.linalg.norm(first[0])), 1.0)

    def test_normalization_nfkc_whitespace_lower(self):
        vec_a, norm_a, _ = text_encoding.hash_embedding("Ｍｕｌｔi-Modal\u3000x")
        vec_b, norm_b, _ = text_encoding.hash_embedding("multi-modal x")
        self.assertEqual(norm_a, norm_b)
        np.testing.assert_array_equal(vec_a, vec_b)

    def test_full_run_byte_identical_across_workspaces(self):
        report_a = self.run_full()
        other_ws = Workspace(os.path.join(self.tmp, "ws2"))
        run_pipeline(other_ws, self.config, self.texts,
                     self.images, StopController(other_ws))
        self.assertEqual(report_a["head_seq"], 4)
        for stage in STAGES:
            product_a = self.workspace.load_current_product(stage)
            product_b = other_ws.load_current_product(stage)
            self.assertEqual(canonical_dumps(product_a),
                             canonical_dumps(product_b), stage)

    def test_rerun_reuses_all_stages(self):
        self.run_full()
        report = self.run_full()
        self.assertEqual(report["reused"], list(STAGES))
        self.assertEqual(report["executed"], [])
        self.assertEqual(self.workspace.head_seq(), 4)


class TestErrorCodes(BaseCase):
    def test_missing_text(self):
        bad = [{"id": "x1", "text": "", "lang": "en"}]
        with self.assertRaises(StageFailure) as ctx:
            text_encoding.run(bad, self.config)
        self.assertEqual(ctx.exception.errors[0]["code"], E_MISSING_INPUT)
        self.assertEqual(ctx.exception.errors[0]["sample_id"], "x1")
        self.assertEqual(ctx.exception.errors[0]["stage"], "text_encoding")

    def test_shape_mismatch(self):
        text_product = text_encoding.run(self.texts, self.config)
        bad_image = dict(self.images[0])
        bad_image["feature"] = [0.1] * 10
        with self.assertRaises(StageFailure) as ctx:
            visual_alignment.run([bad_image], text_product, self.config)
        self.assertEqual(ctx.exception.errors[0]["code"], E_SHAPE_MISMATCH)
        self.assertEqual(ctx.exception.errors[0]["sample_id"], bad_image["id"])

    def test_missing_text_reference(self):
        text_product = text_encoding.run(self.texts, self.config)
        bad_image = dict(self.images[0])
        bad_image["text_id"] = "does-not-exist"
        with self.assertRaises(StageFailure) as ctx:
            visual_alignment.run([bad_image], text_product, self.config)
        self.assertEqual(ctx.exception.errors[0]["code"], E_MISSING_INPUT)

    def test_numeric_nan(self):
        text_product = text_encoding.run(self.texts, self.config)
        bad_image = dict(self.images[0])
        bad_image["feature"] = [float("nan")] * len(bad_image["feature"])
        with self.assertRaises(StageFailure) as ctx:
            visual_alignment.run([bad_image], text_product, self.config)
        self.assertEqual(ctx.exception.errors[0]["code"], E_NUMERIC)
        self.assertEqual(ctx.exception.errors[0]["stage"], "visual_alignment")

    def test_numeric_inf_in_projection(self):
        text_product = text_encoding.run(self.texts, self.config)
        bad_config = json.loads(json.dumps(self.config))
        bad_config["projection_matrix"][0][0] = float("inf")
        with self.assertRaises(PipeError) as ctx:
            visual_alignment.run(self.images, text_product, bad_config)
        self.assertEqual(ctx.exception.code, E_NUMERIC)

    def test_unknown_operator_confidence(self):
        text_product = text_encoding.run(self.texts, self.config)
        visual_product = visual_alignment.run(self.images, text_product,
                                               self.config)
        bad_item = dict(visual_product["items"][0])
        bad_item["operator"] = "op_unknown"
        with self.assertRaises(StageFailure) as ctx:
            fusion_scoring.run({"items": [bad_item]}, self.config)
        self.assertEqual(ctx.exception.errors[0]["code"], E_MISSING_INPUT)
        self.assertEqual(ctx.exception.errors[0]["stage"], "fusion_scoring")


class TestCheckpointStorage(BaseCase):
    def test_publish_requires_predecessor(self):
        exp = expected_inputs(self.config, self.texts, self.images)
        with self.assertRaises(PipeError) as ctx:
            self.workspace.publish(
                "visual_alignment", {"stage": "visual_alignment"},
                exp["visual_alignment"]["input"],
                exp["visual_alignment"]["config"])
        self.assertEqual(ctx.exception.code, E_INVALID_STAGE)

    def test_duplicate_publish_rejected(self):
        self.run_full()
        cur = self.workspace.stage_state("text_encoding")
        product = self.workspace.load_current_product("text_encoding")
        with self.assertRaises(PipeError) as ctx:
            self.workspace.publish(
                "text_encoding", product, cur["input_fingerprint"],
                cur["config_version"])
        self.assertEqual(ctx.exception.code, E_STALE_CHECKPOINT)

    def test_stale_base_seq_rejected(self):
        text_product = text_encoding.run(self.texts, self.config)
        exp = expected_inputs(self.config, self.texts, self.images)
        self.workspace.publish("text_encoding", text_product,
                               exp["text_encoding"]["input"],
                               exp["text_encoding"]["config"])
        with self.assertRaises(PipeError) as ctx:
            self.workspace.publish(
                "text_encoding", text_product,
                exp["text_encoding"]["input"],
                exp["text_encoding"]["config"], expected_base_seq=99)
        self.assertEqual(ctx.exception.code, E_STALE_CHECKPOINT)

    def test_unknown_stage_rejected(self):
        with self.assertRaises(PipeError) as ctx:
            self.workspace.publish("nope", {}, "a", "b")
        self.assertEqual(ctx.exception.code, E_INVALID_STAGE)

    def test_corrupt_checkpoint_ignored(self):
        self.run_full()
        cp_dir = self.workspace.path(CP_DIR)
        with open(os.path.join(cp_dir, "cp_000002.json"), "wb") as handle:
            handle.write(b"{not-json")
        open(os.path.join(cp_dir, "cp_000005.json"), "w").close()
        ws2 = Workspace(self.ws_dir)
        seqs = [cp["seq"] for cp in ws2.load_valid_checkpoints()]
        self.assertEqual(seqs, [1])
        self.assertEqual(ws2.head_seq(), 1)

    def test_atomic_write_no_half_files(self):
        target = os.path.join(self.tmp, "atomic.json")
        from mmpipe.platform_io import atomic_write
        atomic_write(target, b"abc")
        leftovers = [n for n in os.listdir(self.tmp) if n.endswith(".tmp")]
        self.assertEqual(leftovers, [])
        with open(target, "rb") as handle:
            self.assertEqual(handle.read(), b"abc")


if __name__ == "__main__":
    unittest.main(verbosity=2)
