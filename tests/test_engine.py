from __future__ import annotations

import math
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

import numpy as np


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from join_shuffle_engine import service as service_module  # noqa: E402
from join_shuffle_engine.engine import execute_join, execute_shuffle  # noqa: E402
from join_shuffle_engine.errors import EngineError  # noqa: E402
from join_shuffle_engine.hashing import (  # noqa: E402
    deterministic_partition,
    deterministic_partition_ids,
)
from join_shuffle_engine.service import EngineService  # noqa: E402
from join_shuffle_engine.snapshot import load_snapshot, save_snapshot  # noqa: E402
from scripts.generate_samples import generate_samples  # noqa: E402


DATA_ROOT = PROJECT_ROOT / "data" / "snapshots"


class EngineTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        generate_samples(DATA_ROOT, force=True)
        cls.temp_root = tempfile.TemporaryDirectory(prefix="join-engine-tests-")
        cls.root = Path(cls.temp_root.name)
        cls.left = load_snapshot(DATA_ROOT / "demo-left-v1")
        cls.right = load_snapshot(DATA_ROOT / "demo-right-v1")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temp_root.cleanup()

    def new_service(self, name: str = "engine.db") -> EngineService:
        service = EngineService(self.root / name)
        self.addCleanup(service.close)
        service.load_snapshots(
            dataset_id="demo",
            left_path=DATA_ROOT / "demo-left-v1",
            right_path=DATA_ROOT / "demo-right-v1",
        )
        return service

    def test_snapshot_digest_and_monotonic_versions(self) -> None:
        snapshot = load_snapshot(DATA_ROOT / "demo-left-v1")
        self.assertEqual(snapshot.row_count, 9000)
        tampered_root = self.root / "tampered"
        shutil.copytree(DATA_ROOT / "demo-left-v1", tampered_root)
        measure_path = tampered_root / "measures.bin"
        raw = bytearray(measure_path.read_bytes())
        raw[0] ^= 0x01
        measure_path.write_bytes(raw)
        with self.assertRaises(EngineError) as caught:
            load_snapshot(tampered_root)
        self.assertEqual(caught.exception.code, "E_SNAPSHOT_INVALID")
        self.assertIn(
            caught.exception.details["reason"],
            {"array_digest_mismatch", "content_digest_mismatch"},
        )

        service = self.new_service("monotonic.db")
        same_version_error = None
        try:
            service.load_snapshots(
                dataset_id="demo",
                left_path=DATA_ROOT / "demo-left-v1",
                right_path=DATA_ROOT / "demo-right-v1",
            )
        except EngineError as error:
            same_version_error = error
        self.assertIsNotNone(same_version_error)
        self.assertEqual(same_version_error.code, "E_SNAPSHOT_INVALID")
        service.load_snapshots(
            dataset_id="demo",
            left_path=DATA_ROOT / "demo-left-v2",
            right_path=DATA_ROOT / "demo-right-v2",
        )
        self.assertEqual(service.repo.get_active_dataset("demo").pair_version, "L2:R2")

        invalid_dimension = self.root / "bad-dimension"
        with self.assertRaises(EngineError) as caught:
            save_snapshot(
                invalid_dimension,
                snapshot_id="bad",
                side="left",
                version=1,
                generated_at="2026-01-01T00:00:00Z",
                keys=np.array([1], dtype=np.int64),
                measures=np.array([1.0], dtype=np.float64),
                nulls=np.array([0], dtype=np.uint8),
                measure_dimension="length",
            )
        self.assertEqual(caught.exception.code, "E_SNAPSHOT_INVALID")
        self.assertEqual(caught.exception.details["reason"], "invalid_measure_dimension")

        invalid_null = self.root / "bad-null"
        with self.assertRaises(EngineError) as caught:
            save_snapshot(
                invalid_null,
                snapshot_id="bad-null",
                side="left",
                version=1,
                generated_at="2026-01-01T00:00:00Z",
                keys=np.array([1, 2], dtype=np.int64),
                measures=np.array([1.0, 0.0], dtype=np.float64),
                nulls=np.array([0, 2], dtype=np.uint8),
            )
        self.assertEqual(caught.exception.details["reason"], "null_bitmap_value_invalid")

    def test_hash_is_deterministic_and_co_partitions_every_key(self) -> None:
        keys = np.array([-9, -9, -1, 0, 1, 99, 99, 123456789], dtype=np.int64)
        first = deterministic_partition_ids(keys, 37)
        second = deterministic_partition_ids(keys.copy(), 37)
        np.testing.assert_array_equal(first, second)
        for key in np.unique(keys):
            self.assertEqual(len(np.unique(first[keys == key])), 1)
        self.assertEqual(deterministic_partition(-9, 37), int(first[0]))
        self.assertTrue(all(0 <= int(value) < 37 for value in first))

    def test_join_null_semantics_and_stable_order(self) -> None:
        snapshots = self.root / "custom"
        snapshots.mkdir(exist_ok=True)
        left = save_snapshot(
            snapshots / "left",
            snapshot_id="custom-left",
            side="left",
            version=1,
            generated_at="2026-03-01T00:00:00Z",
            keys=np.array([1, 1, 2, 3], dtype=np.int64),
            measures=np.array([10.0, 0.0, 4.0, 6.0], dtype=np.float64),
            nulls=np.array([0, 1, 0, 0], dtype=np.uint8),
        )
        right = save_snapshot(
            snapshots / "right",
            snapshot_id="custom-right",
            side="right",
            version=1,
            generated_at="2026-03-01T00:00:00Z",
            keys=np.array([1, 1, 2], dtype=np.int64),
            measures=np.array([1.0, 0.0, 0.0], dtype=np.float64),
            nulls=np.array([0, 0, 1], dtype=np.uint8),
        )
        inner = execute_join(
            left, right, partitions=3, join_type="inner", measure="left", precision_mode="high"
        )
        self.assertEqual([item.group_key for item in inner.aggregates], [1, 2])
        group_one = inner.aggregates[0]
        self.assertEqual(group_one.count, 4)
        self.assertEqual(group_one.non_null_count, 2)
        self.assertEqual(group_one.sum_value, 20.0)
        self.assertEqual(group_one.mean_value, 10.0)

        outer = execute_join(
            left, right, partitions=3, join_type="left", measure="right", precision_mode="high"
        )
        self.assertEqual([item.group_key for item in outer.aggregates], [1, 2, 3])
        group_one = outer.aggregates[0]
        self.assertEqual((group_one.count, group_one.non_null_count), (4, 4))
        self.assertEqual(group_one.sum_value, 2.0)
        self.assertEqual(group_one.mean_value, 0.5)
        group_two = outer.aggregates[1]
        self.assertEqual((group_two.count, group_two.non_null_count), (1, 0))
        self.assertEqual(group_two.sum_value, 0.0)
        self.assertIsNone(group_two.mean_value)
        group_three = outer.aggregates[2]
        self.assertEqual((group_three.count, group_three.non_null_count), (1, 0))
        self.assertEqual(group_three.sum_value, 0.0)
        self.assertIsNone(group_three.mean_value)

    def test_high_precision_is_bitwise_stable_across_partitions(self) -> None:
        left = load_snapshot(DATA_ROOT / "precision-left-v1")
        right = load_snapshot(DATA_ROOT / "precision-right-v1")
        signatures = []
        sums = []
        for partitions in (1, 2, 5, 13, 32):
            result = execute_join(
                left,
                right,
                partitions=partitions,
                join_type="left",
                measure="left",
                precision_mode="high",
            )
            signatures.append(
                tuple(
                    (
                        item.group_key,
                        item.sum_value.hex(),
                        None if item.mean_value is None else item.mean_value.hex(),
                        item.count,
                        item.non_null_count,
                    )
                    for item in result.aggregates
                )
            )
            sums.append(result.aggregates[0].sum_value)
        self.assertTrue(all(signature == signatures[0] for signature in signatures))
        exact = math.fsum([1.0e16] + [1.0] * 1000)
        self.assertEqual(sums[0], exact)

        fast = execute_join(
            left,
            right,
            partitions=13,
            join_type="left",
            measure="left",
            precision_mode="fast",
        )
        fast_error = abs(fast.aggregates[0].sum_value - exact)
        self.assertGreater(fast_error, 0.0)
        self.assertLessEqual(fast_error, fast.aggregates[0].error_bound)
        fast_repeat = execute_join(
            left,
            right,
            partitions=13,
            join_type="left",
            measure="left",
            precision_mode="fast",
        )
        self.assertEqual(
            tuple((item.group_key, item.sum_value.hex(), item.count) for item in fast.aggregates),
            tuple(
                (item.group_key, item.sum_value.hex(), item.count)
                for item in fast_repeat.aggregates
            ),
        )
        high = execute_join(
            left,
            right,
            partitions=13,
            join_type="left",
            measure="left",
            precision_mode="high",
            target_abs_error=10.0,
        )
        self.assertLessEqual(abs(high.aggregates[0].sum_value - exact), high.aggregates[0].error_bound)
        with self.assertRaises(EngineError) as caught:
            execute_join(
                left,
                right,
                partitions=13,
                join_type="left",
                measure="left",
                precision_mode="fast",
                target_abs_error=10.0,
            )
        self.assertEqual(caught.exception.code, "E_PRECISION_BUDGET")
        with self.assertRaises(EngineError) as caught:
            execute_join(
                left,
                right,
                partitions=13,
                join_type="left",
                measure="left",
                precision_mode="auto",
                target_abs_error=10.0,
                max_operations=2000,
            )
        self.assertEqual(caught.exception.code, "E_PRECISION_BUDGET")

    def test_skew_report_and_reject(self) -> None:
        left = load_snapshot(DATA_ROOT / "skew-left-v1")
        right = load_snapshot(DATA_ROOT / "skew-right-v1")
        report = execute_shuffle(
            left, right, partitions=8, skew_limit=0.35, skew_action="report"
        )
        self.assertTrue(report.any_skew)
        skew = [item for item in report.partitions if item.skew]
        self.assertGreaterEqual(len(skew), 1)
        self.assertEqual(skew[0].hotspots[0].key, 7)
        self.assertEqual(
            sum(item.total_count for item in report.partitions),
            left.row_count + right.row_count,
        )
        self.assertTrue(all(item.estimated_bytes > 0 for item in report.partitions))
        with self.assertRaises(EngineError) as caught:
            execute_shuffle(
                left, right, partitions=8, skew_limit=0.35, skew_action="reject"
            )
        self.assertEqual(caught.exception.code, "E_SKEW_LIMIT")

    def test_duplicate_concurrency_queue_and_committed_reads(self) -> None:
        service = self.new_service("concurrency.db")
        baseline = service.submit_join(
            "baseline", "demo", partitions=3, join_type="inner", measure="left"
        )
        self.assertEqual(baseline["request"]["status"], "committed")

        entered = threading.Event()
        release = threading.Event()
        original = service_module.execute_join

        def blocking_execute(*args, **kwargs):
            entered.set()
            release.wait(timeout=10.0)
            return original(*args, **kwargs)

        with mock.patch.object(service_module, "execute_join", side_effect=blocking_execute):
            first = service.submit_join(
                "queued-first",
                "demo",
                partitions=5,
                join_type="inner",
                measure="left",
                wait=False,
            )
            self.assertTrue(entered.wait(timeout=10.0))
            running = service.get_request_status("queued-first")
            self.assertEqual(running["status"], "running")
            self.assertEqual(
                service.get_aggregates("baseline")["request_id"], "baseline"
            )
            with self.assertRaises(EngineError) as caught:
                service.get_aggregates("queued-first")
            self.assertEqual(caught.exception.code, "E_REQUEST_NOT_COMMITTED")
            second = service.submit_join(
                "queued-second",
                "demo",
                partitions=5,
                join_type="inner",
                measure="left",
                wait=False,
            )
            self.assertEqual(second["request"]["status"], "queued")
            self.assertGreaterEqual(second["queue_position_at_submit"], 2)
            release.set()
            service.wait_for_request("queued-first", timeout=20.0)
            service.wait_for_request("queued-second", timeout=20.0)

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(
                pool.map(
                    lambda _: service.submit_join(
                        "duplicate-request",
                        "demo",
                        partitions=7,
                        join_type="inner",
                        measure="left",
                        precision_mode="high",
                    ),
                    range(4),
                )
            )
        self.assertTrue(all(item["request"]["status"] == "committed" for item in results))
        self.assertEqual(service.repo.count_executions("duplicate-request"), 1)
        signatures = {
            tuple(
                (row["group_key"], row["sum_value"].hex(), row["count"])
                for row in item["result"]["results"]
            )
            for item in results
        }
        self.assertEqual(len(signatures), 1)
        with self.assertRaises(EngineError) as caught:
            service.submit_join(
                "duplicate-request",
                "demo",
                partitions=8,
                join_type="inner",
                measure="left",
                precision_mode="high",
            )
        self.assertEqual(caught.exception.code, "E_REQUEST_CONFLICT")
        with self.assertRaises(EngineError) as caught:
            service.get_request_status("does-not-exist")
        self.assertEqual(caught.exception.code, "E_REQUEST_NOT_FOUND")

    def test_forced_termination_recovery_reports(self) -> None:
        process = subprocess.run(
            [
                sys.executable,
                str(PROJECT_ROOT / "scripts" / "crash_drill.py"),
                "--phase",
                "all",
                "--partitions",
                "8",
                "--crash-at",
                "3",
            ],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=120,
        )
        self.assertEqual(process.returncode, 0, process.stderr)
        import json

        report = json.loads(
            (PROJECT_ROOT / "artifacts" / "crash_recovery_report.json").read_text(
                encoding="utf-8"
            )
        )
        by_phase = {item["phase"]: item for item in report["drills"]}
        partition = by_phase["partition"]
        self.assertEqual(partition["child_exit_code"], 96)
        self.assertEqual(partition["request_status"], "aborted")
        self.assertEqual(partition["result_query_error"], "E_EXECUTION_ABORTED")
        self.assertEqual(partition["recovery_report"]["last_complete_partition"], 2)
        self.assertEqual(partition["recovery_report"]["persisted_partition_count"], 3)
        self.assertEqual(
            partition["recovery_report"]["pending_intents"][0]["payload"]["partition_id"],
            3,
        )
        self.assertFalse(
            partition["recovery_report"]["facts"]["exit_report_complete"]
        )
        self.assertEqual(partition["database_facts"]["row_counts"]["aggregates"], 0)
        merge = by_phase["merge"]
        self.assertEqual(merge["child_exit_code"], 98)
        self.assertEqual(merge["recovery_report"]["last_complete_partition"], 7)
        self.assertEqual(merge["recovery_report"]["persisted_partition_count"], 8)
        self.assertEqual(merge["database_facts"]["row_counts"]["aggregates"], 0)


if __name__ == "__main__":
    unittest.main()
