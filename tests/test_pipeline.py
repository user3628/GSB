"""管线单元与集成测试（python -m unittest discover -s tests）。"""

import multiprocessing
import os
import shutil
import tempfile
import unittest

from pipeline import STAGES
from pipeline.atomicio import read_json
from pipeline.config import load_config
from pipeline.data_ops import edit_training_sample, tamper_model_version_in_request
from pipeline.data_ops import refresh_inference_model_versions
from pipeline.errors import (
    E_EMPTY_SAMPLE,
    E_INVALID_STAGE,
    E_INVALID_TRAIN,
    E_MODEL_VERSION_MISMATCH,
    E_STALE_CHECKPOINT,
    E_NUMERIC,
    PipelineError,
)
from pipeline.featurizer import featurize_text
from pipeline.generator import generate_workspace
from pipeline.model import dequantize_weights
from pipeline.pipeline import Pipeline
from pipeline.shards import read_jsonl, write_jsonl
from pipeline.tokenizer import normalize_text, tokenize


def _publisher_worker(workspace, queue):
    try:
        Pipeline(workspace).publish_raw(
            STAGES[1], "fp", "cfg", {"artifact_name": "x", "digest": "d"}
        )
        queue.put("accepted")
    except PipelineError as exc:
        queue.put(exc.code)


class TokenizerTest(unittest.TestCase):
    def test_normalize_and_tokenize_mixed_text(self):
        config = {
            "normalize": {"nfkc": True, "fold_whitespace": True, "lowercase": True,
                          "halfwidth_fullwidth_unify": True},
            "features": {"han_bigrams": True, "hash_dimension": 256, "l2_norm": True},
        }
        normalized = normalize_text("  Ｈｅｌｌｏ　世界 AB  ", config["normalize"])
        self.assertEqual(normalized, "hello 世界 ab")
        tokens = tokenize(normalized, True)
        self.assertIn("w:hello", tokens)
        self.assertIn("h:世", tokens)
        self.assertIn("hb:世界", tokens)
        self.assertIn("w:ab", tokens)

    def test_deterministic_token_order(self):
        config = {
            "normalize": {"nfkc": True, "fold_whitespace": True, "lowercase": True,
                          "halfwidth_fullwidth_unify": True},
            "features": {"han_bigrams": True, "hash_dimension": 256, "l2_norm": True},
        }
        _, features_a, _ = featurize_text("商品 优惠 Good 商品", config)
        _, features_b, _ = featurize_text("商品 优惠 Good 商品", config)
        self.assertEqual(features_a, features_b)


class PipelineIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dpl-test-")
        self.workspace = os.path.join(self.tmp, "ws")
        generate_workspace(self.workspace)
        self.pipe = Pipeline(self.workspace)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_full_run_byte_identical_across_independent_builds(self):
        self.pipe.run()
        other = os.path.join(self.tmp, "ws2")
        generate_workspace(other)
        Pipeline(other).run()
        for stage in STAGES:
            with open(self.pipe.ws.artifact_path(stage), "rb") as a, \
                 open(Pipeline(other).ws.artifact_path(stage), "rb") as b:
                self.assertEqual(a.read(), b.read(), stage)

    def test_empty_sample_diagnostic_retained(self):
        self.pipe.run(through_stage=STAGES[0])
        artifact, _ = read_json(self.pipe.ws.artifact_path(STAGES[0]))
        self.assertTrue(artifact["diagnostics"])
        self.assertTrue(all(d["error"] == E_EMPTY_SAMPLE for d in artifact["diagnostics"]))
        self.assertTrue(any(d["sample_id"].startswith("train-empty-") for d in artifact["diagnostics"]))

    def test_rerun_reuses_everything(self):
        self.pipe.run()
        report = self.pipe.run()
        self.assertEqual(report["rebuilt"], [])
        self.assertEqual(set(report["reused"]), set(STAGES))

    def test_incremental_edit_invalidates_downstream_and_matches_full_build(self):
        self.pipe.run()
        edit_training_sample(self.pipe.ws, "train-0002", new_text="糟糕透了我要退货投诉", new_label=0)
        first_half = self.pipe.run(through_stage=STAGES[1])
        refresh_inference_model_versions(self.pipe.ws)
        report = self.pipe.run()
        rebuilt = set(first_half["rebuilt"]) | set(report["rebuilt"])
        self.assertEqual(rebuilt, set(STAGES))
        self.assertTrue(first_half["invalidated"] or report["invalidated"])
        result = self.pipe.validate_incremental()
        self.assertTrue(result["byte_identical"])
        events = self.pipe.status()["invalidation_events"]
        self.assertTrue(events)

    def test_threshold_change_only_rebuilds_stage3_stage4(self):
        self.pipe.run()
        import yaml

        config = load_config(self.pipe.ws.config_path)
        config["inference"]["confidence_threshold"] = 0.63
        with open(self.pipe.ws.config_path, "w", encoding="utf-8") as handle:
            yaml.safe_dump(config, handle, allow_unicode=True)
        report = self.pipe.run()
        self.assertEqual(report["reused"], [STAGES[0], STAGES[1]])
        self.assertEqual(set(report["rebuilt"]), {STAGES[2], STAGES[3]})
        self.assertTrue(self.pipe.validate_incremental()["byte_identical"])

    def test_learning_rate_change_keeps_stage1(self):
        self.pipe.run()
        import yaml

        config = load_config(self.pipe.ws.config_path)
        config["model"]["learning_rate"] = 0.25
        with open(self.pipe.ws.config_path, "w", encoding="utf-8") as handle:
            yaml.safe_dump(config, handle, allow_unicode=True)
        self.pipe.run(through_stage=STAGES[1])
        refresh_inference_model_versions(self.pipe.ws)
        report = self.pipe.run()
        self.assertIn(STAGES[0], report["reused"])
        self.assertNotIn(STAGES[0], report["rebuilt"])
        self.assertTrue(self.pipe.validate_incremental()["byte_identical"])

    def test_shard_replace_incremental_mismatch_check(self):
        self.pipe.run()
        shard = os.path.join(self.pipe.ws.train_dir, "train-shard-03.jsonl")
        records = read_jsonl(shard)
        records[1]["text"] = "excellent good nice happy"
        write_jsonl(shard, records)
        self.pipe.run(through_stage=STAGES[1])
        refresh_inference_model_versions(self.pipe.ws)
        self.pipe.run()
        self.assertTrue(self.pipe.validate_incremental(scope=STAGES[0])["byte_identical"])
        self.assertTrue(self.pipe.validate_incremental()["byte_identical"])

    def test_corrupt_and_half_written_checkpoint_recovery(self):
        self.pipe.run()
        os.remove(os.path.join(self.pipe.ws.checkpoint_dir, "record-000003.json"))
        with open(os.path.join(self.pipe.ws.checkpoint_dir, "record-000003.json"), "wb") as handle:
            handle.write(b"{broken")
        report = self.pipe.run()
        status = self.pipe.status()
        self.assertEqual([c["sequence"] for c in status["checkpoints"]], [1, 2, 3, 4])
        quarantine_files = os.listdir(self.pipe.ws.quarantine_dir)
        self.assertTrue(quarantine_files)
        self.assertTrue(self.pipe.validate_incremental()["byte_identical"])
        self.assertIn(STAGES[2], report["rebuilt"])

    def test_duplicate_sequence_record_is_ignored(self):
        self.pipe.run()
        store = self.pipe.store
        records, _ = store.load_valid_records()
        duplicate = dict(records[1])
        path = os.path.join(self.pipe.ws.checkpoint_dir, "record-000003.json")
        import json

        with open(path, "wb") as handle:
            handle.write(json.dumps(duplicate).encode("utf-8"))
        valid, quarantined = store.load_valid_records()
        self.assertEqual(len(valid), 2)
        self.assertTrue(quarantined)

    def test_invalid_train_zero_epochs(self):
        bad = os.path.join(self.tmp, "bad-epochs")
        generate_workspace(bad)
        pipe = Pipeline(bad)
        config = load_config(pipe.ws.config_path)
        config["model"]["epochs"] = 0
        import yaml

        with open(pipe.ws.config_path, "w", encoding="utf-8") as handle:
            yaml.safe_dump(config, handle, allow_unicode=True)
        with self.assertRaises(PipelineError) as ctx:
            pipe.run()
        self.assertEqual(ctx.exception.code, E_INVALID_TRAIN)
        self.assertEqual(ctx.exception.details["epoch"], 0)

    def test_negative_learning_rate_rejected(self):
        bad = os.path.join(self.tmp, "bad-lr")
        generate_workspace(bad)
        pipe = Pipeline(bad)
        config = load_config(pipe.ws.config_path)
        config["model"]["learning_rate"] = -1.0
        import yaml

        with open(pipe.ws.config_path, "w", encoding="utf-8") as handle:
            yaml.safe_dump(config, handle, allow_unicode=True)
        with self.assertRaises(PipelineError) as ctx:
            pipe.run()
        self.assertEqual(ctx.exception.code, E_INVALID_TRAIN)

    def test_model_version_mismatch_blocks_inference(self):
        self.pipe.run(through_stage=STAGES[1])
        tamper_model_version_in_request(self.pipe.ws, "req-0005", "mv_0000000000000000")
        with self.assertRaises(PipelineError) as ctx:
            self.pipe.run(through_stage=STAGES[2])
        self.assertEqual(ctx.exception.code, E_MODEL_VERSION_MISMATCH)
        self.assertEqual(ctx.exception.details["mismatches"][0]["request_id"], "req-0005")
        self.assertFalse(os.path.exists(self.pipe.ws.artifact_path(STAGES[2])))

    def test_missing_prerequisite_stage(self):
        no_data = os.path.join(self.tmp, "no-data")
        os.makedirs(no_data, exist_ok=True)
        with self.assertRaises(PipelineError) as ctx:
            Pipeline(no_data).run(through_stage=STAGES[1])
        self.assertEqual(ctx.exception.code, E_INVALID_STAGE)
        with self.assertRaises(PipelineError) as ctx2:
            self.pipe.run(through_stage="not-a-stage")
        self.assertEqual(ctx2.exception.code, E_INVALID_STAGE)

    def test_quantized_model_roundtrip_and_history(self):
        self.pipe.run(through_stage=STAGES[1])
        artifact, _ = read_json(self.pipe.ws.artifact_path(STAGES[1]))
        self.assertEqual(len(artifact["scale_exponents"]), artifact["dimension"] + 1)
        weights = dequantize_weights(artifact)
        self.assertEqual(len(weights), artifact["dimension"] + 1)
        self.assertEqual(len(artifact["loss_history"]), artifact["epochs"])
        self.assertEqual(artifact["sample_order"], artifact["loss_history"][0]["sample_order"])

    def test_stale_checkpoint_publish_rejected(self):
        self.pipe.run(through_stage=STAGES[0])
        with self.assertRaises(PipelineError) as ctx:
            self.pipe.publish_raw(
                STAGES[3], "fp", "cfg", {"artifact_name": "x", "digest": "d"}
            )
        self.assertEqual(ctx.exception.code, E_STALE_CHECKPOINT)

    def test_concurrent_publish_only_one_accepted(self):
        self.pipe.run(through_stage=STAGES[0])
        queue = multiprocessing.Queue()
        procs = [
            multiprocessing.Process(
                target=_publisher_worker, args=(self.workspace, queue)
            )
            for _ in range(4)
        ]
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join()
        outcomes = [queue.get() for _ in procs]
        self.assertEqual(outcomes.count("accepted"), 1)
        self.assertTrue(all(code == E_STALE_CHECKPOINT for code in outcomes if code != "accepted"))

    def test_graceful_stop_then_resume_is_consistent(self):
        import threading

        event = threading.Event()
        event.set()  # 停止请求先于执行：首个单元完成前不写检查点
        report = self.pipe.run(stop_event=event)
        self.assertTrue(report["stopped"])
        status = self.pipe.status()
        self.assertEqual(status["checkpoints"], [])
        full = self.pipe.run()
        self.assertEqual(set(full["rebuilt"]), set(STAGES))
        self.assertTrue(self.pipe.validate_incremental()["byte_identical"])

    def test_nan_loss_raises_enumeric(self):
        import numpy as np

        from pipeline import model as model_module

        original = model_module._sigmoid
        model_module._sigmoid = lambda values: np.full_like(np.asarray(values, dtype=float), np.nan)
        try:
            with self.assertRaises(PipelineError) as ctx:
                self.pipe.run()
        finally:
            model_module._sigmoid = original
        self.assertEqual(ctx.exception.code, E_NUMERIC)
        self.assertIn("epoch", ctx.exception.details)

    def test_graceful_stop_between_stages_resumes_without_recompute(self):
        import threading

        self.pipe.run(through_stage=STAGES[1])
        event = threading.Event()

        original_execute = self.pipe._execute_stage

        def wrapped(stage, config, stop_event):
            if stage == STAGES[2]:
                event.set()
            return original_execute(stage, config, stop_event)

        self.pipe._execute_stage = wrapped
        report = self.pipe.run(through_stage=STAGES[3], stop_event=event)
        self.assertTrue(report["stopped"])
        sequences = [c["sequence"] for c in self.pipe.status()["checkpoints"]]
        self.assertEqual(sequences, [1, 2, 3])
        self.pipe._execute_stage = original_execute
        resume = self.pipe.run(through_stage=STAGES[3])
        self.assertEqual(resume["rebuilt"], [STAGES[3]])
        self.assertEqual(resume["reused"], [STAGES[0], STAGES[1], STAGES[2]])
        self.assertTrue(self.pipe.validate_incremental()["byte_identical"])


if __name__ == "__main__":
    multiprocessing.freeze_support()
    unittest.main()
