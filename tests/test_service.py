from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from mpc.errors import BusinessError
from mpc.samples import generate_sample_workspace
from mpc.service import CheckpointService

from .helpers import ServiceCase, digest_map, write_snapshot


ROOT = Path(__file__).resolve().parents[1]


class DeterministicSampleTests(unittest.TestCase):
    def test_sample_files_and_manifest_are_byte_deterministic(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as first, TemporaryDirectory() as second:
            manifest_a = generate_sample_workspace(first, overwrite=True)
            manifest_b = generate_sample_workspace(second, overwrite=True)
            self.assertEqual(manifest_a["manifest_digest"], manifest_b["manifest_digest"])
            for shard_id in ("s0", "s1", "s2", "s3", "s_partial"):
                path_a = Path(first) / manifest_a["shards"][shard_id]["path"]
                path_b = Path(second) / manifest_b["shards"][shard_id]["path"]
                self.assertEqual(
                    hashlib.sha256(path_a.read_bytes()).hexdigest(),
                    hashlib.sha256(path_b.read_bytes()).hexdigest(),
                    shard_id,
                )
            self.assertNotIn("s_partial", manifest_a["formal_shard_ids"])

    def test_partial_shard_never_enters_manifest(self):
        case = ServiceCase()
        try:
            case.full_policy()
            with self.assertRaises(BusinessError) as partial:
                case.checkpoint(shards=["s0", "s_partial"])
            self.assertEqual(partial.exception.code, "E_SHARD_NOT_FULL")
            manifest = case.checkpoint(shards=["s0", "s1"])
            self.assertEqual(
                [shard["shard_id"] for shard in manifest["shards"]], ["s0", "s1"]
            )
            self.assertTrue(manifest["formal_manifest_only"])
            self.assertNotIn("s_partial", [shard["shard_id"] for shard in manifest["shards"]])
        finally:
            case.close()


class PolicyAndCascadeTests(unittest.TestCase):
    def setUp(self):
        self.case = ServiceCase()

    def tearDown(self):
        self.case.close()

    def test_single_shard_precision_increment_cascades_and_rebuilds_atomically(self):
        full_policy = self.case.full_policy()
        self.case.checkpoint()
        before = self.case.service.get_checkpoint_derived(1)

        increment = self.case.service.submit_policy_increment(
            {
                "commit_id": "policy-inc-2",
                "base_version": 1,
                "version": 2,
                "shards": {"s1": "FP16"},
            }
        )
        self.assertEqual(increment["policy"]["version"], 2)
        self.assertEqual(increment["changed_precisions"], ["s1"])
        self.assertEqual(increment["affected_shards"], ["s1"])
        self.assertEqual(increment["changed_shards"], ["s1"])
        self.assertFalse(increment["loss_scale_changed"])

        after = self.case.service.get_checkpoint_derived(1)
        before_map = digest_map(before)
        after_map = digest_map(after)
        self.assertNotEqual(before_map["s1"], after_map["s1"])
        for shard_id in ("s0", "s2", "s3"):
            self.assertEqual(before_map[shard_id], after_map[shard_id])
        self.assertNotEqual(
            before["aggregate"]["derived_summary_digest"],
            after["aggregate"]["derived_summary_digest"],
        )
        self.assertEqual(after["policy"]["assignments"]["s1"], "FP16")
        self.assertEqual(after["shards"][1]["precision"], "FP16")
        self.assertEqual(
            before["aggregate"]["memory_bytes"] - after["aggregate"]["memory_bytes"], 48
        )
        self.assertEqual(after, self.case.service.get_checkpoint_derived(1))

        historical = self.case.service.get_checkpoint_derived(1, policy_version=1)
        self.assertTrue(historical["aggregate"]["invalidated"])
        self.assertFalse(historical["is_current_policy"])
        self.assertTrue(all(shard["invalidated"] for shard in historical["shards"]))

    def test_loss_scale_change_recomputes_every_dependent_shard(self):
        self.case.full_policy()
        self.case.checkpoint()
        before = self.case.service.get_checkpoint_derived(1)
        result = self.case.service.submit_policy_increment(
            {
                "commit_id": "policy-loss-scale-2",
                "base_version": 1,
                "version": 2,
                "loss_scale": 32.0,
            }
        )
        self.assertTrue(result["loss_scale_changed"])
        self.assertEqual(result["affected_shards"], [])
        self.assertEqual(result["changed_shards"], ["s0", "s1", "s2", "s3"])
        after = self.case.service.get_checkpoint_derived(1)
        before_map = digest_map(before)
        after_map = digest_map(after)
        for shard_id in before_map:
            self.assertNotEqual(before_map[shard_id], after_map[shard_id])
        self.assertEqual(after["shards"][0]["loss_scale"], 32.0)
        self.assertTrue(
            any(shard["deviations"]["max_absolute"] > 0 for shard in after["shards"])
        )
        self.assertEqual(after, self.case.service.get_checkpoint_derived(1))

    def test_loss_policy_initial_and_growth_fields_are_dependencies(self):
        self.case.full_policy()
        self.case.checkpoint()
        before = self.case.service.get_checkpoint_derived(1)
        result = self.case.service.submit_policy_increment(
            {
                "commit_id": "policy-growth-2",
                "base_version": 1,
                "version": 2,
                "growth_factor": 4.0,
            }
        )
        self.assertTrue(result["loss_scale_changed"])
        self.assertEqual(result["changed_shards"], ["s0", "s1", "s2", "s3"])
        after = self.case.service.get_checkpoint_derived(1)
        self.assertEqual(after["policy"]["loss_scale"], 64.0)
        self.assertEqual(after["policy"]["growth_factor"], 4.0)
        self.assertEqual(
            {shard["shard_id"]: shard["restore_digest"] for shard in before["shards"]},
            {shard["shard_id"]: shard["restore_digest"] for shard in after["shards"]},
        )
        self.assertNotEqual(
            digest_map(before), digest_map(after), "policy dependencies must change summaries"
        )

    def test_stale_versions_do_not_change_state(self):
        self.case.full_policy()
        self.case.checkpoint()
        with self.assertRaises(BusinessError) as stale_base:
            self.case.service.submit_policy_increment(
                {
                    "commit_id": "stale-base",
                    "base_version": 0,
                    "version": 2,
                    "shards": {"s1": "FP16"},
                }
            )
        self.assertEqual(stale_base.exception.code, "E_STALE_BASE")
        with self.assertRaises(BusinessError) as stale_version:
            self.case.service.submit_policy_increment(
                {
                    "commit_id": "stale-version",
                    "base_version": 1,
                    "version": 1,
                    "shards": {"s1": "FP16"},
                }
            )
        self.assertEqual(stale_version.exception.code, "E_STALE_SNAPSHOT")
        with self.assertRaises(BusinessError) as stale_full:
            self.case.full_policy(version=1, commit_id="stale-full")
        self.assertEqual(stale_full.exception.code, "E_STALE_SNAPSHOT")
        self.assertEqual(self.case.service.get_current_policy()["version"], 1)
        self.assertEqual(
            self.case.service.get_checkpoint_derived(1)["policy"]["version"], 1
        )

    def test_checksum_mismatch_does_not_commit_checkpoint(self):
        self.case.full_policy()
        with self.assertRaises(BusinessError) as mismatch:
            self.case.service.submit_checkpoint(
                {
                    "commit_id": "bad-checksum",
                    "round": 1,
                    "scheduler_position": {"step": 1},
                    "data_cursor": {"batch": 1},
                    "shards": [{"shard_id": "s0", "fp16_digest": "0" * 64}],
                }
            )
        self.assertEqual(mismatch.exception.code, "E_CHECKSUM_MISMATCH")
        health = self.case.service.health()
        self.assertEqual(health["latest_checkpoint_version"], 0)

    def test_invalid_precision_is_rejected_before_state_change(self):
        with self.assertRaises(BusinessError) as invalid:
            self.case.full_policy(assignments={"s0": "BF16", "s1": "FP32", "s2": "FP16", "s3": "FP32"})
        self.assertEqual(invalid.exception.code, "E_INVALID_PRECISION")
        with self.assertRaises(BusinessError) as no_policy:
            self.case.service.get_current_policy()
        self.assertEqual(no_policy.exception.code, "E_POLICY_REQUIRED")


class IdempotencyAndConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.case = ServiceCase()

    def tearDown(self):
        self.case.close()

    def test_same_commit_id_reuses_result_and_conflicting_content_fails(self):
        first = self.case.full_policy(commit_id="same-id")
        second = self.case.full_policy(commit_id="same-id")
        self.assertEqual(first, second)
        with self.assertRaises(BusinessError) as conflict:
            self.case.service.submit_policy_snapshot(
                {
                    "commit_id": "same-id",
                    "version": 1,
                    "format_version": 1,
                    "loss_scale": 64.0,
                    "growth_factor": 4.0,
                    "shards": {"s0": "FP16", "s1": "FP32", "s2": "FP16", "s3": "FP32"},
                }
            )
        self.assertEqual(conflict.exception.code, "E_CONFLICTING_COMMIT")
        self.case.checkpoint(commit_id="checkpoint-same")
        with self.assertRaises(BusinessError) as checkpoint_conflict:
            self.case.checkpoint(commit_id="checkpoint-same", round_number=2)
        self.assertEqual(checkpoint_conflict.exception.code, "E_CONFLICTING_COMMIT")
        self.assertEqual(self.case.service.health()["latest_checkpoint_version"], 1)

    def test_concurrent_duplicate_checkpoint_commits_once(self):
        self.case.full_policy()
        body = {
            "commit_id": "concurrent-checkpoint",
            "round": 1,
            "scheduler_position": {"step": 10},
            "data_cursor": {"batch": 1},
            "shard_ids": ["s0", "s1", "s2", "s3"],
        }

        def submit():
            return self.case.service.submit_checkpoint(dict(body))

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: submit(), range(8)))
        self.assertTrue(all(result == results[0] for result in results))
        self.assertEqual(self.case.service.health()["latest_checkpoint_version"], 1)
        self.assertEqual(len(self.case.service.list_commit_operations()), 2)


class SnapshotFileTests(unittest.TestCase):
    def setUp(self):
        self.case = ServiceCase()

    def tearDown(self):
        self.case.close()

    def test_snapshot_file_switch_is_atomic_and_monotonic(self):
        self.case.full_policy()
        self.case.checkpoint()
        before = self.case.service.get_checkpoint_derived(1)
        snapshot = write_snapshot(
            self.case.root / "snapshot-1.json",
            snapshot_seq=1,
            version=2,
            assignments={"s0": "FP32", "s1": "FP16", "s2": "FP16", "s3": "FP32"},
        )
        loaded = self.case.service.load_policy_snapshot_file(snapshot)
        self.assertEqual(loaded["policy"]["version"], 2)
        after = self.case.service.get_checkpoint_derived(1)
        self.assertEqual(after["policy"]["assignments"]["s0"], "FP32")
        self.assertNotEqual(
            before["aggregate"]["derived_summary_digest"],
            after["aggregate"]["derived_summary_digest"],
        )

        stale = write_snapshot(
            self.case.root / "snapshot-stale.json", snapshot_seq=1, version=3
        )
        with self.assertRaises(BusinessError) as stale_error:
            self.case.service.load_policy_snapshot_file(stale, commit_id="stale-snapshot")
        self.assertEqual(stale_error.exception.code, "E_STALE_SNAPSHOT")
        self.assertEqual(self.case.service.get_current_policy()["version"], 2)

        bad_format = write_snapshot(
            self.case.root / "snapshot-bad-format.json",
            snapshot_seq=2,
            version=3,
            format_version=999,
        )
        with self.assertRaises(BusinessError) as format_error:
            self.case.service.load_policy_snapshot_file(bad_format, commit_id="bad-format")
        self.assertEqual(format_error.exception.code, "E_SNAPSHOT_VERSION")
        self.assertEqual(self.case.service.get_current_policy()["version"], 2)

        bad_precision = write_snapshot(
            self.case.root / "snapshot-bad-precision.json",
            snapshot_seq=3,
            version=3,
            assignments={"s0": "INT8", "s1": "FP32", "s2": "FP16", "s3": "FP32"},
        )
        with self.assertRaises(BusinessError) as precision_error:
            self.case.service.load_policy_snapshot_file(
                bad_precision, commit_id="bad-precision"
            )
        self.assertEqual(precision_error.exception.code, "E_INVALID_PRECISION")
        self.assertEqual(self.case.service.get_current_policy()["version"], 2)

        newer = write_snapshot(
            self.case.root / "snapshot-2.json",
            snapshot_seq=2,
            version=3,
            assignments={"s0": "FP32", "s1": "FP16", "s2": "FP16", "s3": "FP16"},
        )
        loaded_newer = self.case.service.load_policy_snapshot_file(newer)
        self.assertEqual(loaded_newer["policy"]["version"], 3)
        self.assertEqual(loaded_newer["released_snapshot_sequences"], [1])
        self.assertEqual(self.case.service.get_current_policy()["version"], 3)


class ForcedTerminationRecoveryTests(unittest.TestCase):
    def test_kill_mid_rebuild_rolls_back_and_reports_only_durable_facts(self):
        case = ServiceCase()
        try:
            case.full_policy()
            case.checkpoint()
            before = case.service.get_checkpoint_derived(1)
            marker = case.root / "crash-ready.marker"
            payload = case.root / "increment.json"
            payload.write_text(
                json.dumps(
                    {
                        "commit_id": "crash-increment",
                        "base_version": 1,
                        "version": 2,
                        "loss_scale": 32.0,
                        "shards": {"s2": "FP32"},
                    }
                ),
                encoding="utf-8",
            )
            env = os.environ.copy()
            env.update(
                {
                    "MPC_CRASH_POINT": "mid_rebuild",
                    "MPC_CRASH_MARKER": str(marker),
                    "MPC_CRASH_HOLD_SECONDS": "60",
                }
            )
            worker = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "mpc.crash_worker",
                    "--db",
                    str(case.db),
                    "--samples",
                    str(case.samples),
                    "--operation",
                    "policy-increment",
                    "--payload",
                    str(payload),
                ],
                cwd=str(ROOT),
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            deadline = time.monotonic() + 20
            while not marker.exists() and time.monotonic() < deadline:
                self.assertIsNone(worker.poll(), "crash worker exited before marker")
                time.sleep(0.05)
            self.assertTrue(marker.exists(), "crash worker did not reach mid_rebuild")

            # A concurrent reader must see the old fully-committed view while
            # the writer transaction is still open.
            during_crash = case.service.get_checkpoint_derived(1)
            self.assertEqual(during_crash, before)
            self.assertEqual(case.service.get_current_policy()["version"], 1)

            worker.kill()
            worker.wait(timeout=10)
            self.assertNotEqual(worker.returncode, 0)

            pending = case.service.list_intents()
            self.assertEqual(pending[-1]["status"], "pending")
            self.assertEqual(case.service.get_current_policy()["version"], 1)

            recovered_service = CheckpointService(case.db, case.samples)
            report = recovered_service.get_recovery_report()
            self.assertEqual(report["scanned_pending"], 1)
            self.assertEqual(report["aborted_count"], 1)
            self.assertEqual(report["completed_count"], 0)
            self.assertEqual(report["current_policy_version"], 1)
            self.assertEqual(report["last_complete_checkpoint_version"], 1)
            self.assertTrue(report["derived_consistency"]["consistent"])
            self.assertFalse(report["guarantees_complete_exit_report"])
            self.assertEqual(
                recovered_service.get_checkpoint_derived(1)["aggregate"][
                    "derived_summary_digest"
                ],
                before["aggregate"]["derived_summary_digest"],
            )
            statuses = {item["intent_id"]: item["status"] for item in recovered_service.list_intents()}
            self.assertEqual(statuses[pending[-1]["intent_id"]], "aborted")
        finally:
            case.close()

    def test_kill_during_checkpoint_commit_never_exposes_new_checkpoint(self):
        case = ServiceCase()
        try:
            case.full_policy()
            marker = case.root / "crash-checkpoint.marker"
            payload = case.root / "checkpoint.json"
            payload.write_text(
                json.dumps(
                    {
                        "commit_id": "crash-checkpoint",
                        "round": 1,
                        "scheduler_position": {"step": 10},
                        "data_cursor": {"batch": 1},
                        "shard_ids": ["s0", "s1", "s2", "s3"],
                    }
                ),
                encoding="utf-8",
            )
            env = os.environ.copy()
            env.update(
                {
                    "MPC_CRASH_POINT": "mid_checkpoint_rebuild",
                    "MPC_CRASH_MARKER": str(marker),
                    "MPC_CRASH_HOLD_SECONDS": "60",
                }
            )
            worker = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "mpc.crash_worker",
                    "--db",
                    str(case.db),
                    "--samples",
                    str(case.samples),
                    "--operation",
                    "checkpoint",
                    "--payload",
                    str(payload),
                ],
                cwd=str(ROOT),
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            deadline = time.monotonic() + 20
            while not marker.exists() and time.monotonic() < deadline:
                self.assertIsNone(worker.poll(), "crash worker exited before marker")
                time.sleep(0.05)
            self.assertTrue(marker.exists())
            worker.kill()
            worker.wait(timeout=10)

            recovered = CheckpointService(case.db, case.samples)
            report = recovered.get_recovery_report()
            self.assertEqual(report["scanned_pending"], 1)
            self.assertEqual(report["aborted_count"], 1)
            self.assertEqual(report["last_complete_checkpoint_version"], 0)
            self.assertEqual(recovered.health()["latest_checkpoint_version"], 0)
            with self.assertRaises(BusinessError) as missing:
                recovered.get_checkpoint_manifest(1)
            self.assertEqual(missing.exception.code, "E_NOT_FOUND")
        finally:
            case.close()


if __name__ == "__main__":
    unittest.main()
