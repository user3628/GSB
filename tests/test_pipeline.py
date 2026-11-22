import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from asr_pipeline.audio import read_wav, validate_audio
from asr_pipeline.config import FrameConfig, PipelineConfig, estimate_frame_count
from asr_pipeline.decoder import decode_stream, decode_whole, verify_consistency
from asr_pipeline.errors import (
    ClippedAudioError,
    InputLengthError,
    InvalidFrameError,
    SampleRateError,
    SilentAudioError,
    StreamMismatchError,
)
from asr_pipeline.frontend import build_frontend_artifact, featureize
from asr_pipeline.hashing import canonical_json
from asr_pipeline.manifest import load_manifest
from asr_pipeline.pipeline import Pipeline
from asr_pipeline.samplegen import generate_healthy, generate_injections
from asr_pipeline.vocabulary import VOCABULARY, is_mixed_script

FRAME = FrameConfig()


class PipelineTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.mkdtemp(prefix="asr_test_")
        cls.data = os.path.join(cls.temp, "data")
        cls.workspace = os.path.join(cls.temp, "workspace")
        cls.manifest_path, cls.manifest = generate_healthy(cls.data, FRAME)
        cls.injection_dir = os.path.join(cls.data, "injections")
        generate_injections(cls.injection_dir, FRAME)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.temp, ignore_errors=True)

    def _config(self):
        return PipelineConfig()

    def test_frame_conversion_uses_samples_not_ms(self):
        self.assertEqual(FRAME.frame_length, 400)
        self.assertEqual(FRAME.hop_length, 160)
        self.assertNotEqual(FRAME.hop_length, FRAME.hop_ms)

    def test_invalid_frame_params(self):
        with self.assertRaises(InvalidFrameError):
            FrameConfig(hop_ms=0).validate()
        with self.assertRaises(InvalidFrameError):
            FrameConfig(frame_ms=10, hop_ms=25).validate()
        with self.assertRaises(InvalidFrameError):
            FrameConfig(frame_ms=25.7).validate()

    def test_zero_padding_is_deterministic(self):
        rate, samples = read_wav(os.path.join(self.data, "shard-001.wav"))
        features, offsets, padded = featureize(samples, FRAME)
        self.assertIn(True, padded)
        artifact_a = build_frontend_artifact(samples, FRAME)
        artifact_b = build_frontend_artifact(samples, FRAME)
        self.assertEqual(
            artifact_a["feature_digest"], artifact_b["feature_digest"]
        )
        self.assertEqual(canonical_json(artifact_a), canonical_json(artifact_b))

    def test_streaming_matches_whole_shard_for_all_blocks(self):
        config = self._config()
        for entry in self.manifest["shards"]:
            rate, samples = read_wav(os.path.join(self.data, entry["wav_file"]))
            features, _, _ = featureize(samples, FRAME)
            result = verify_consistency(
                features, FRAME, config.decoder, block_sizes=(1, 2, 3, 5, 1000)
            )
            whole = decode_whole(features, FRAME, config.decoder)
            self.assertEqual(result["words"], whole["words"])

    def test_block_size_independence(self):
        config = self._config()
        rate, samples = read_wav(os.path.join(self.data, "shard-001.wav"))
        features, _, _ = featureize(samples, FRAME)
        outputs = {
            block: decode_stream(features, FRAME, config.decoder, block_size=block)
            for block in (1, 3, 7, len(features))
        }
        baseline = outputs[1]
        for block, output in outputs.items():
            self.assertEqual(output["words"], baseline["words"])

    def test_endpoint_alignment_is_exact_for_healthy_shards(self):
        pipeline = Pipeline(self._config(), self.manifest, self.data, self.workspace)
        summary = pipeline.run()
        self.assertEqual(
            summary["alignment"]["total_hits"],
            summary["alignment"]["total_reference_words"],
        )
        self.assertEqual(summary["alignment"]["max_deviation_ms"], 0)
        self.assertEqual(summary["alignment"]["endpoint_failures"], [])
        self.assertIn("alpha确认", summary["alignment"]["mixed_script_words"])

    def test_missing_reference_is_unverified(self):
        from asr_pipeline.align import align_shard, UNVERIFIED

        report = align_shard(
            {"words": []}, {"shard_id": "x", "reference_words": []},
            self._config().align,
        )
        self.assertEqual(report["status"], UNVERIFIED)

    def test_injection_error_codes(self):
        mapping = {
            "inject-length_mismatch": InputLengthError,
            "inject-rate_conflict": SampleRateError,
            "inject-silent": SilentAudioError,
            "inject-clipped": ClippedAudioError,
        }
        manifest = load_manifest(
            os.path.join(self.injection_dir, "manifest.json")
        )
        for entry in manifest["shards"]:
            rate, samples = read_wav(
                os.path.join(self.injection_dir, entry["wav_file"])
            )
            with self.assertRaises(mapping[entry["shard_id"]]):
                validate_audio(rate, samples, dict(entry, sample_rate=16000))

    def test_budget_never_over_admits(self):
        config = PipelineConfig()
        object.__setattr__(
            config.budget, "global_frame_budget", 150
        )
        object.__setattr__(config.budget, "worker_frame_quota", 100)
        object.__setattr__(config.budget, "shard_frame_latency", 100)
        pipeline = Pipeline(config, self.manifest, self.data,
                            os.path.join(self.temp, "ws_budget"))
        plan, used = pipeline.plan_admission()
        self.assertLessEqual(used, 150)
        self.assertTrue(any(not item["admitted"] for item in plan))

    def test_recovery_byte_identical_to_fresh_run(self):
        ws_a = os.path.join(self.temp, "fresh")
        ws_b = os.path.join(self.temp, "recovered")
        summary_a = Pipeline(self._config(), self.manifest, self.data, ws_a).run()
        pipeline_b = Pipeline(self._config(), self.manifest, self.data, ws_b)
        pipeline_b.run()
        summary_b = pipeline_b.run()
        self.assertEqual(
            summary_a["result_fingerprint"], summary_b["result_fingerprint"]
        )
        self.assertEqual(canonical_json(summary_a), canonical_json(summary_b))

    def test_mixed_script_detection(self):
        self.assertTrue(is_mixed_script("alpha确认"))
        self.assertFalse(is_mixed_script("hello"))
        self.assertFalse(is_mixed_script("确认"))

    def test_estimate_frame_count(self):
        self.assertEqual(estimate_frame_count(400, FRAME), 1)
        self.assertEqual(estimate_frame_count(560, FRAME), 2)
        self.assertEqual(estimate_frame_count(19120, FRAME), 118)


if __name__ == "__main__":
    unittest.main(verbosity=2)
