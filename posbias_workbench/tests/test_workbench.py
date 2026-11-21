"""End-to-end and unit tests for the position-bias workbench."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import unittest

import numpy as np
import yaml

from posbias.core.binning import bin_statistics
from posbias.core.checkpoints import CheckpointStore, build_record, atomic_write
from posbias.core.config import (AppConfig, canonical_json,
                                 convert_legacy_v1, export_legacy_v1,
                                 stable_scheme_id)
from posbias.core.encodings import pad_head_dim
from posbias.core.injection import run_injection
from posbias.core.numeric import (QuantizedBias, log_domain_product,
                                  log_domain_ratio)
from posbias.core.pipeline import Pipeline, StopFlag
from posbias.core.reference import compare
from posbias.core.shards import write_shard
from posbias.errors import (InvalidConfigError, NumericError,
                            InvalidStageError, StaleCheckpointError)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(REPO_ROOT, "configs", "config_v2.yaml")
LEGACY_PATH = os.path.join(REPO_ROOT, "configs", "legacy_manifest_v1.yaml")
STAGES = ("config", "bias", "binning", "tolerance")


class WorkbenchCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="posbias-test-")
        self.work = os.path.join(self.tmp, "data")
        self.shard_dir = os.path.join(self.work, "shards")
        os.makedirs(self.shard_dir, exist_ok=True)
        self.cfg = AppConfig.load(CONFIG_PATH)
        for idx in range(3):
            write_shard(self.shard_dir, seed=self.cfg.seed, shard_index=idx,
                        batch=2, heads=4, seq_len=32, head_dim=16)
        self.reference_path = os.path.join(
            self.work, "reference", "reference_metrics.yaml")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def pipeline(self, config=None, stop_flag=None):
        return Pipeline(config or self.cfg, self.work,
                        shard_dir=self.shard_dir,
                        reference_path=self.reference_path,
                        stop_flag=stop_flag or StopFlag())

    def golden(self):
        pipe = self.pipeline()
        pipe.ensure_reference()
        return pipe.run()

    def _reference_for(self, work_dir, shard_dir):
        path = os.path.join(work_dir, "reference", "reference_metrics.yaml")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if not os.path.exists(path):
            shutil.copy(self.reference_path, path)
        return path

    # ------------------------------------------------ requirement 1
    def test_bias_matrix_and_fixed_point_metadata(self):
        report = self.golden()
        self.assertEqual(len(report["schemes"]), 4)
        kinds = {s["type"] for s in report["schemes"]}
        self.assertEqual(
            kinds, {"rope", "ntk_rope", "linear_rope", "alibi"})
        bias_record = self.pipeline().store.load("bias")
        for scheme in bias_record["artifact_summary"]["schemes"]:
            self.assertGreater(scheme["overall_bias_peak"], 0.0)
            for shard in scheme["shards"]:
                self.assertGreater(shard["quant_scale"], 0.0)
                q = np.load(os.path.join(
                    self.work, "artifacts", shard["q_path"]))
                self.assertEqual(q.dtype, np.int32)
                self.assertEqual(q.shape, (32, 32))

    def test_byte_identical_rerun(self):
        report_a = self.golden()
        hashes_a = self._hash_tree()
        report_b = self.pipeline().run()
        hashes_b = self._hash_tree()
        self.assertEqual(hashes_a, hashes_b)
        self.assertEqual(canonical_json(report_a),
                         canonical_json(report_b))

    def test_odd_head_dim_padding_registered(self):
        dim, info = pad_head_dim(15, rule="zero_pad")
        self.assertEqual(dim, 16)
        self.assertTrue(info["padded"])
        self.assertEqual(info["pad_width"], 1)
        dim2, info2 = pad_head_dim(16, rule="zero_pad")
        self.assertEqual(dim2, 16)
        self.assertFalse(info2["padded"])

    # ------------------------------------------------ requirement 2
    def test_three_bins_and_empty_bin_preserved(self):
        self.golden()
        record = self.pipeline().store.load("binning")
        for scheme in record["artifact_summary"]["schemes"]:
            self.assertEqual(len(scheme["bins"]), 3)
            declared = [(b["id"], b["ratio_low"], b["ratio_high"])
                        for b in self.cfg.bins]
            actual = [(b["bin_id"], b["ratio_low"], b["ratio_high"])
                      for b in scheme["bins"]]
            self.assertEqual(declared, actual)
        bias = np.zeros((8, 8))
        records = bin_statistics(
            bias, train_length=8,
            bins=[{"id": "far", "ratio_low": 2.0, "ratio_high": 4.0}])
        self.assertTrue(records[0]["empty"])
        self.assertIsNone(records[0]["mean_bias"])
        self.assertEqual(records[0]["cell_count"], 0)

    def test_alibi_monotonic_and_break_flag(self):
        report = self.golden()
        by_type = {s["type"]: s for s in report["schemes"]}
        self.assertFalse(by_type["alibi"]["monotonic_scheme_break"])
        for bin_record in by_type["alibi"]["bins"]:
            self.assertEqual(bin_record["monotonic_violations"], 0)

    # ------------------------------------------------ requirement 3
    def test_log_domain_and_numeric_errors(self):
        value = log_domain_product(base_freq=10000.0, scale=4.0,
                                  stage="bias", scheme="s")
        self.assertAlmostEqual(value, 40000.0)
        ratio = log_domain_ratio(numerator=8.0, denominator=2.0,
                                 stage="bias", scheme="s")
        self.assertAlmostEqual(ratio, 4.0)
        with self.assertRaises(NumericError) as ctx:
            log_domain_product(base_freq=0.0, scale=1.0, stage="bias",
                               scheme="rope-x", bin_id="far")
        self.assertEqual(ctx.exception.code, "E_NUMERIC")
        self.assertEqual(ctx.exception.scheme, "rope-x")
        self.assertEqual(ctx.exception.bin_id, "far")
        with self.assertRaises(NumericError):
            QuantizedBias(np.zeros((2, 2), dtype=np.int32), 0.0,
                          scheme="rope-x")
        with self.assertRaises(NumericError):
            QuantizedBias.from_float(
                np.array([[np.nan, 1.0]]), scheme="rope-x")

    # ------------------------------------------------ requirement 4
    def test_tolerance_pass_and_failure(self):
        report = self.golden()
        with open(self.reference_path, encoding="utf-8") as handle:
            reference = yaml.safe_load(handle)
        result = compare(report, reference, self.cfg.tolerance)
        self.assertTrue(result["passed"])
        tampered = json.loads(json.dumps(reference))
        sid = report["schemes"][0]["scheme"]
        first_bin = report["schemes"][0]["bins"][0]["bin_id"]
        tampered["schemes"][sid]["bins"][first_bin]["mean_bias"] += 10.0
        failed = compare(report, tampered, self.cfg.tolerance)
        self.assertFalse(failed["passed"])
        violation = failed["scheme_results"][0]["violations"][0]
        self.assertEqual(violation["metric"], "mean_bias")
        self.assertIn("reference", violation)
        self.assertIn("estimate", violation)

    def test_missing_reference_rejected(self):
        self.golden()
        report = self.pipeline()._assemble_report(
            self.pipeline().store.load("binning"))
        reference = {"reference_version": 1, "schemes": {}}
        with self.assertRaises(InvalidConfigError):
            compare(report, reference, self.cfg.tolerance)

    # ------------------------------------------------ requirement 5
    def test_legacy_v1_roundtrip_byte_identical(self):
        with open(LEGACY_PATH, encoding="utf-8") as handle:
            legacy = yaml.safe_load(handle)
        cfg_v2, provenance = convert_legacy_v1(legacy)
        self.assertEqual(len(cfg_v2.schemes), 4)
        rope = next(s for s in cfg_v2.schemes if s["type"] == "rope")
        self.assertAlmostEqual(rope["scale"], 1.0)
        ntk = next(s for s in cfg_v2.schemes if s["type"] == "ntk_rope")
        self.assertAlmostEqual(ntk["scale"], 4.0)
        alibi = next(s for s in cfg_v2.schemes if s["type"] == "alibi")
        self.assertEqual(len(alibi["slopes"]), 4)
        for scheme in cfg_v2.schemes:
            params = {k: v for k, v in scheme.items()
                      if k not in ("id", "type")}
            self.assertEqual(scheme["id"],
                             stable_scheme_id(scheme["type"], params))
        legacy_back = export_legacy_v1(cfg_v2)
        cfg_again, _ = convert_legacy_v1(legacy_back)
        self.assertEqual(canonical_json(cfg_v2.data),
                         canonical_json(cfg_again.data))
        work_a = os.path.join(self.tmp, "a")
        work_b = os.path.join(self.tmp, "b")
        pipe_a = Pipeline(cfg_v2, work_a, shard_dir=self.shard_dir,
                          reference_path=os.path.join(
                              work_a, "reference", "reference_metrics.yaml"))
        pipe_a.ensure_reference()
        ref_b = os.path.join(work_b, "reference", "reference_metrics.yaml")
        os.makedirs(os.path.dirname(ref_b), exist_ok=True)
        import shutil as _sh
        _sh.copy(pipe_a.reference_path, ref_b)
        pipe_b = Pipeline(cfg_again, work_b, shard_dir=self.shard_dir,
                          reference_path=ref_b)
        ra = pipe_a.run(write_report=False)
        rb = pipe_b.run(write_report=False)
        self.assertEqual(canonical_json(ra["schemes"]),
                         canonical_json(rb["schemes"]))

    # ------------------------------------------------ requirement 6
    def test_stop_resume_byte_identical_and_sequences(self):
        self.pipeline().ensure_reference()
        pipe = self.pipeline()
        flag = StopFlag()
        pipe.stop_flag = flag
        pipe.run_config_stage()
        bias_record, _ = pipe.run_bias_stage(pipe.store.load("config"))
        flag.request()
        with self.assertRaises(Exception):
            pipe.run_binning_stage(bias_record)
        resumed = self.pipeline(stop_flag=StopFlag())
        report = resumed.run()
        fresh_work = os.path.join(self.tmp, "fresh")
        fresh_shards = os.path.join(fresh_work, "shards")
        shutil.copytree(self.shard_dir, fresh_shards)
        fresh = Pipeline(self.cfg, fresh_work, shard_dir=fresh_shards,
                         reference_path=os.path.join(
                             fresh_work, "reference",
                             "reference_metrics.yaml"))
        os.makedirs(os.path.dirname(fresh.reference_path), exist_ok=True)
        shutil.copy(self.reference_path, fresh.reference_path)
        fresh_report = fresh.run(write_report=False)
        self.assertEqual(canonical_json(report["schemes"]),
                         canonical_json(fresh_report["schemes"]))
        self.assertEqual(len(set(report["checkpoint_sequences"].values())), 4)

    # ------------------------------------------------ requirement 7
    def test_version_invalidation_and_reuse(self):
        self.golden()
        pipe = self.pipeline()
        before = {stage: pipe.store.load(stage)["fingerprint"]
                  for stage in STAGES}
        import copy
        changed = AppConfig(copy.deepcopy(self.cfg.data))
        changed.data["schemes"][0]["base_freq"] = 20000.0
        changed.validate()
        pipe2 = Pipeline(changed, self.work, shard_dir=self.shard_dir,
                         reference_path=self.reference_path)
        record, reused = pipe2.run_config_stage()
        self.assertFalse(reused)
        self.assertIsNone(pipe2.store.load("bias"))
        pipe2.run_bias_stage(pipe2.store.load("config"))
        self.assertIsNotNone(pipe2.store.load("bias"))
        self.assertNotEqual(before["bias"],
                            pipe2.store.load("bias")["fingerprint"])

    def test_stale_and_invalid_stage_publish(self):
        store = CheckpointStore(os.path.join(self.tmp, "ckpt"))
        record_v1 = build_record(
            "config", sequence=1, input_fingerprint="a",
            config_version=2, artifact_summary={"x": 1})
        store.publish("config", record_v1)
        with self.assertRaises(StaleCheckpointError):
            store.publish("config", build_record(
                "config", sequence=1, input_fingerprint="a",
                config_version=2, artifact_summary={"x": 1}))
        with self.assertRaises(StaleCheckpointError):
            store.publish("config", build_record(
                "config", sequence=0, input_fingerprint="b",
                config_version=2, artifact_summary={"x": 2}))
        fresh_store = CheckpointStore(os.path.join(self.tmp, "ckpt2"))
        with self.assertRaises(InvalidStageError):
            fresh_store.publish("bias", build_record(
                "bias", sequence=1, input_fingerprint="a",
                config_version=2, artifact_summary={}))

    def test_atomic_write_never_half_written(self):
        target = os.path.join(self.tmp, "atomic.json")
        payload = {"stage": "config", "sequence": 1}
        atomic_write(target, payload)
        with open(target, encoding="utf-8") as handle:
            self.assertEqual(json.load(handle), payload)

    # ------------------------------------------------ requirement 8
    def test_injection_error_codes(self):
        expected = {
            "seq_too_long": "E_SEQ_TOO_LONG",
            "invalid_scale": "E_INVALID_PARAM",
            "zero_bins": "E_INVALID_CONFIG",
            "nan_shard": "E_NUMERIC",
            "zero_base_freq": "E_NUMERIC",
            "zero_quant_scale": "E_NUMERIC",
        }
        for kind, code in expected.items():
            outcome = run_injection(kind=kind, config=self.cfg,
                                    work_dir=os.path.join(self.tmp, "inj"),
                                    shard_shape={"batch": 2, "heads": 4,
                                                 "seq_len": 16,
                                                 "head_dim": 16})
            self.assertTrue(outcome["triggered"], kind)
            self.assertEqual(outcome["error"]["code"], code, kind)

    def _hash_tree(self):
        result = {}
        for root, _, files in os.walk(self.work):
            for name in sorted(files):
                if name.endswith(".lock"):
                    continue
                path = os.path.join(root, name)
                with open(path, "rb") as handle:
                    result[os.path.relpath(path, self.work)] = \
                        hashlib.sha256(handle.read()).hexdigest()
        return result


if __name__ == "__main__":
    unittest.main(verbosity=2)
