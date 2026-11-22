from __future__ import annotations

import json
import multiprocessing
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from audiofp import (
    E_BUCKET_COLLISION,
    E_INVALID_STAGE,
    E_NO_PEAK,
    E_STALE_CHECKPOINT,
    CheckpointStore,
    FingerprintError,
    detect_peaks,
    generate_fingerprint_pairs,
    generate_samples,
    get_report,
    import_dataset,
    load_manifest,
    query_status,
    quantize_pairs,
    read_wav,
    resume_pipeline,
    run_pipeline,
)
from audiofp.samples import write_pcm16_wav


def _concurrent_publish_worker(
    workspace: str,
    shard_id: str,
    output_path: str,
    barrier: multiprocessing.Barrier,
) -> None:
    store = CheckpointStore(workspace)
    barrier.wait()
    record = {
        "schema": "audiofp.checkpoint.v1",
        "shard_id": shard_id,
        "stage": "peaks",
        "input_digest": "input",
        "config_version": "config",
        "artifact_digest": "artifact",
        "stage_version": "version",
        "dependencies": {},
        "params": {},
    }
    try:
        store.publish(shard_id, record, expected_seq=1)
        payload = {"ok": True}
    except FingerprintError as error:
        payload = {"ok": False, "code": error.code}
    Path(output_path).write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


class AudioFingerprintTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._temp = tempfile.TemporaryDirectory(prefix="audiofp-tests-")
        cls.root = Path(cls._temp.name)
        cls.samples_dir = cls.root / "samples"
        generate_samples(cls.samples_dir, 20261123)
        cls.manifest_path = cls.samples_dir / "fingerprint_config.json"
        cls.manifest = load_manifest(cls.manifest_path)
        cls.configs = cls.manifest.by_id()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._temp.cleanup()

    def _new_workspace(self, name: str) -> Path:
        workspace = self.root / name
        import_dataset(workspace, self.manifest_path)
        return workspace

    def test_peaks_are_deterministic_and_silence_has_business_error(self) -> None:
        config = self.configs["music_a"]
        wav = read_wav(config.wav_path, config.shard_id)
        first = detect_peaks(wav, config)
        second = detect_peaks(wav, config)
        self.assertEqual(first.peaks, second.peaks)
        self.assertGreater(len(first.peaks), 0)
        self.assertEqual(first.peaks, tuple(sorted(first.peaks, key=lambda p: (p.frame, p.freq))))

        silence = self.configs["silence"]
        with self.assertRaises(FingerprintError) as caught:
            detect_peaks(read_wav(silence.wav_path, silence.shard_id), silence)
        self.assertEqual(caught.exception.code, E_NO_PEAK)
        self.assertEqual(caught.exception.shard_id, "silence")

    def test_pair_generation_respects_bounds_and_no_wrap_policy(self) -> None:
        config = self.configs["music_c"]
        peaks = detect_peaks(read_wav(config.wav_path, config.shard_id), config)
        first = generate_fingerprint_pairs(peaks.peaks, config)
        second = generate_fingerprint_pairs(peaks.peaks, config)
        self.assertEqual(first, second)
        self.assertEqual(first.policy, "bounded_no_wrap")
        self.assertEqual(first.terminal_anchor_edges, 0)
        self.assertLessEqual(len(first.pairs), config.max_pairs)
        for pair in first.pairs:
            self.assertGreaterEqual(pair.dt, config.time_delta_min)
            self.assertLessEqual(pair.dt, config.time_delta_max)
            frequency_delta = abs(pair.target_freq - pair.anchor_freq)
            self.assertGreaterEqual(frequency_delta, config.freq_delta_min)
            self.assertLessEqual(frequency_delta, config.freq_delta_max)
            self.assertGreater(pair.target_frame, pair.anchor_frame)

    def test_quantization_bounds_and_collision_error(self) -> None:
        config = self.configs["music_a"]
        peaks = detect_peaks(read_wav(config.wav_path, config.shard_id), config)
        pairs = generate_fingerprint_pairs(peaks.peaks, config)
        quantized = quantize_pairs(pairs.pairs, config)
        for field in ("anchor_freq", "target_freq", "time_delta"):
            payload = quantized.fields[field]
            self.assertAlmostEqual(payload["upper_bound"], payload["quantization_step"] / 2.0, places=12)
            self.assertLessEqual(payload["actual_max_abs_error"], payload["upper_bound"] + 1e-12)
            self.assertLessEqual(payload["effective_bits"], config.quantization_bits)

        collision_config = replace(config, bucket_width=1, max_collision_rate=0.2)
        with self.assertRaises(FingerprintError) as caught:
            quantize_pairs(pairs.pairs, collision_config)
        error = caught.exception
        self.assertEqual(error.code, E_BUCKET_COLLISION)
        self.assertEqual(error.shard_id, "music_a")
        self.assertEqual(error.details["strongest_bucket"], 0)
        self.assertIn("bucket", error.details)

    def test_full_demo_report_has_expected_errors_and_suspicious_shards(self) -> None:
        workspace = self._new_workspace("full")
        result = run_pipeline(workspace)
        self.assertFalse(result.stopped)
        report = get_report(workspace)["summary"]
        errors = {item["shard_id"]: item["code"] for item in report["pipeline_errors"]}
        self.assertEqual(errors["silence"], E_NO_PEAK)
        self.assertEqual(errors["bucket_collision"], E_BUCKET_COLLISION)
        statuses = {item["shard_id"]: item["status"] for item in report["shards"]}
        self.assertEqual(statuses["music_a"], "passed")
        self.assertEqual(statuses["music_b"], "passed")
        self.assertEqual(statuses["music_c"], "passed")
        self.assertTrue(report["suspicious_shards"])
        for shard_id in ("music_a", "music_b", "music_c"):
            self.assertEqual(statuses[shard_id], "passed")
        first_bytes = (workspace / "reports" / "summary.json").read_bytes()
        second = run_pipeline(workspace)
        self.assertEqual(first_bytes, (workspace / "reports" / "summary.json").read_bytes())
        self.assertEqual(result.summary_digest, second.summary_digest)

    def test_stop_resume_is_byte_identical_to_full_run(self) -> None:
        full = self._new_workspace("resume-full")
        resumed = self._new_workspace("resume-partial")
        full_result = run_pipeline(full)
        self.assertFalse(full_result.stopped)
        partial = run_pipeline(resumed, stop_after_stage="quantize")
        self.assertTrue(partial.stopped)
        self.assertFalse((resumed / "reports" / "summary.json").exists())
        resumed_result = resume_pipeline(resumed)
        self.assertFalse(resumed_result.stopped)

        relative_files = [
            "reports/summary.json",
            "reports/summary.sha256",
        ]
        for directory in (
            "artifacts/peaks",
            "artifacts/pairs",
            "artifacts/quantized",
            "reports/shards",
            "errors",
            "checkpoints",
        ):
            relative_files.extend(
                str(path.relative_to(full)) for path in sorted((full / directory).glob("*.json"))
            )
        for relative in relative_files:
            self.assertEqual(
                (full / relative).read_bytes(),
                (resumed / relative).read_bytes(),
                msg=relative,
            )
        self.assertEqual(full_result.summary_digest, resumed_result.summary_digest)

        for shard_id in ("music_a", "music_b", "music_c"):
            state = CheckpointStore(resumed).read_state(shard_id)
            self.assertEqual([state["stages"][stage]["seq"] for stage in ("peaks", "pairs", "quantize", "audit")], [1, 2, 3, 4])

    def test_incremental_invalidation_reuses_upstream_and_matches_fresh_build(self) -> None:
        workspace = self._new_workspace("incremental")
        run_pipeline(workspace)
        old_state = CheckpointStore(workspace).read_state("music_c")
        old_peak_seq = old_state["stages"]["peaks"]["seq"]
        old_pair_seq = old_state["stages"]["pairs"]["seq"]

        manifest_path = workspace / "manifest.json"
        modified = json.loads(manifest_path.read_text(encoding="utf-8"))
        for shard in modified["shards"]:
            if shard["shard_id"] == "music_c":
                shard["bucket_width"] = 8192
        manifest_path.write_text(
            json.dumps(modified, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        run_pipeline(workspace)
        state = CheckpointStore(workspace).read_state("music_c")
        self.assertEqual(state["stages"]["peaks"]["seq"], old_peak_seq)
        self.assertEqual(state["stages"]["pairs"]["seq"], old_pair_seq)
        self.assertEqual(state["stages"]["quantize"]["seq"], 5)
        self.assertEqual(state["stages"]["audit"]["seq"], 6)
        self.assertEqual(state["last_invalidation"]["from_stage"], "quantize")
        self.assertIn("params.bucket_width", state["last_invalidation"]["changed_fields"])
        status = query_status(workspace, shard_id="music_c")
        self.assertEqual(status["shards"][0]["last_invalidation"]["from_stage"], "quantize")

        fresh = self.root / "incremental-fresh"
        import_dataset(fresh, manifest_path)
        run_pipeline(fresh)
        self.assertEqual(
            (workspace / "reports" / "summary.json").read_bytes(),
            (fresh / "reports" / "summary.json").read_bytes(),
        )
        for path in sorted((workspace / "reports" / "shards").glob("*.json")):
            self.assertEqual(path.read_bytes(), (fresh / "reports" / "shards" / path.name).read_bytes())

    def test_each_version_input_hits_the_expected_earliest_stage(self) -> None:
        workspace = self._new_workspace("invalidation-fields")
        run_pipeline(workspace)
        manifest_path = workspace / "manifest.json"
        cases = [
            ("neighborhood", 3, "peaks", "params.neighborhood"),
            ("max_peaks_per_frame", 3, "peaks", "params.max_peaks_per_frame"),
            ("time_delta_max", 11, "pairs", "params.time_delta_max"),
            ("freq_delta_max", 79, "pairs", "params.freq_delta_max"),
            ("quantization_bits", 9, "quantize", "params.quantization_bits"),
            ("bucket_width", 2048, "quantize", "params.bucket_width"),
        ]
        for field, value, expected_stage, expected_field in cases:
            modified = json.loads(manifest_path.read_text(encoding="utf-8"))
            for shard in modified["shards"]:
                if shard["shard_id"] == "music_a":
                    shard[field] = value
            manifest_path.write_text(
                json.dumps(modified, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
                encoding="utf-8",
            )
            run_pipeline(workspace)
            invalidation = CheckpointStore(workspace).read_state("music_a")["last_invalidation"]
            self.assertEqual(invalidation["from_stage"], expected_stage, msg=field)
            self.assertIn(expected_field, invalidation["changed_fields"], msg=field)

        modified = json.loads(manifest_path.read_text(encoding="utf-8"))
        music_a = next(shard for shard in modified["shards"] if shard["shard_id"] == "music_a")
        wav_path = workspace / music_a["wav"]
        wav = read_wav(str(wav_path), "music_a")
        write_pcm16_wav(wav_path, wav.samples * 0.97)
        run_pipeline(workspace)
        invalidation = CheckpointStore(workspace).read_state("music_a")["last_invalidation"]
        self.assertEqual(invalidation["from_stage"], "peaks")
        self.assertIn("sample_digest", invalidation["changed_fields"])

    def test_audit_threshold_failure_is_reported_separately(self) -> None:
        workspace = self._new_workspace("audit-threshold")
        manifest_path = workspace / "manifest.json"
        modified = json.loads(manifest_path.read_text(encoding="utf-8"))
        for shard in modified["shards"]:
            if shard["shard_id"] == "music_a":
                shard["expected_false_match_rate"] = 0.0
        manifest_path.write_text(
            json.dumps(modified, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        run_pipeline(workspace)
        summary = get_report(workspace)["summary"]
        row = next(item for item in summary["shards"] if item["shard_id"] == "music_a")
        self.assertEqual(row["status"], "failed")
        failed = next(item for item in summary["failed_shards"] if item["shard_id"] == "music_a")
        self.assertEqual(failed["kind"], "audit_threshold")
        self.assertEqual(failed["violations"][0]["metric"], "false_match_rate")
        self.assertGreater(
            failed["violations"][0]["estimate_ppm"], failed["violations"][0]["limit_ppm"]
        )

    def test_checkpoint_sequence_stage_and_cross_shard_guards(self) -> None:
        workspace = self.root / "checkpoint-guards"
        store = CheckpointStore(workspace)

        def record(shard_id: str, stage: str, seq: int | None = None) -> dict:
            payload = {
                "schema": "audiofp.checkpoint.v1",
                "shard_id": shard_id,
                "stage": stage,
                "input_digest": "input",
                "config_version": "config",
                "artifact_digest": "artifact",
                "stage_version": "version",
                "dependencies": {},
                "params": {},
            }
            if seq is not None:
                payload["seq"] = seq
            return payload

        with self.assertRaises(FingerprintError) as caught:
            store.publish("fresh", record("fresh", "quantize", 1), expected_seq=1)
        self.assertEqual(caught.exception.code, E_INVALID_STAGE)

        store.publish("guarded", record("guarded", "peaks", 1), expected_seq=1)
        with self.assertRaises(FingerprintError) as caught:
            store.publish("guarded", record("guarded", "peaks", 2), expected_seq=2)
        self.assertEqual(caught.exception.code, E_STALE_CHECKPOINT)
        with self.assertRaises(FingerprintError) as caught:
            store.publish("guarded", record("guarded", "audit", 2), expected_seq=2)
        self.assertEqual(caught.exception.code, E_INVALID_STAGE)
        with self.assertRaises(FingerprintError) as caught:
            store.publish("guarded", record("guarded", "pairs", 9), expected_seq=9)
        self.assertEqual(caught.exception.code, E_STALE_CHECKPOINT)
        with self.assertRaises(FingerprintError) as caught:
            store.publish("guarded", record("other", "pairs", 2), expected_seq=2)
        self.assertEqual(caught.exception.code, E_STALE_CHECKPOINT)

    def test_real_file_lock_serializes_concurrent_publishers(self) -> None:
        workspace = self.root / "concurrent"
        context = multiprocessing.get_context("spawn")
        barrier = context.Barrier(2)
        outputs = [str(self.root / f"publisher-{index}.json") for index in range(2)]
        processes = [
            context.Process(
                target=_concurrent_publish_worker,
                args=(str(workspace), "same-shard", output, barrier),
            )
            for output in outputs
        ]
        for process in processes:
            process.start()
        for process in processes:
            process.join(timeout=10)
            self.assertEqual(process.exitcode, 0)
        results = [json.loads(Path(output).read_text(encoding="utf-8")) for output in outputs]
        self.assertEqual(sum(1 for result in results if result["ok"]), 1)
        self.assertEqual(
            [result.get("code") for result in results if not result["ok"]],
            [E_STALE_CHECKPOINT],
        )


if __name__ == "__main__":
    unittest.main()

