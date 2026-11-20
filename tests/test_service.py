from __future__ import annotations

import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path
from typing import Any

from rateguard.config import GuardConfig
from rateguard.engine import Engine
from rateguard.errors import BusinessError


def event(
    timestamp_ms: int,
    *,
    success: bool = True,
    latency_us: int = 30_000,
    route: str = "/api/test",
) -> dict[str, Any]:
    return {
        "timestamp_ms": timestamp_ms,
        "route": route,
        "success": success,
        "latency_us": latency_us,
    }


def batch(batch_id: str, events: list[dict[str, Any]], sequence: int = 1) -> dict[str, Any]:
    return {"batch_id": batch_id, "sequence": sequence, "events": events}


class ServiceIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "rateguard.sqlite3"

    def tearDown(self) -> None:
        self.temp.cleanup()

    def make_engine(self, **overrides: Any) -> Engine:
        values: dict[str, Any] = dict(
            window_length_ms=1_000,
            step_ms=1_000,
            min_samples_p50=1,
            min_samples_p95=2,
            min_samples_p99=2,
            max_quota=100,
            min_breaker_samples=2,
            consecutive_failure_threshold=2,
            open_duration_ms=1_000,
            half_open_success_threshold=2,
        )
        values.update(overrides)
        config = GuardConfig(**values)
        return Engine(self.db, config)

    def test_idempotency_conflict_and_first_report_retention(self) -> None:
        engine = self.make_engine()
        payload = batch("b-1", [event(100), event(200), event(1_100, latency_us=31_000)])
        first = engine.ingest_batch(payload)
        self.assertFalse(first["duplicate"])
        composite_version = first["routes"]["/api/test"]["composite_version"]
        window_count = engine.list_windows("/api/test")["windows"][0]["sample_count"]
        self.assertEqual(window_count, 2)

        duplicate = engine.ingest_batch(payload)
        self.assertTrue(duplicate["duplicate"])
        self.assertEqual(duplicate["routes"]["/api/test"]["composite_version"], composite_version)

        conflicting = batch("b-1", [event(100), event(200, latency_us=99), event(1_100)])
        with self.assertRaises(BusinessError) as context:
            engine.ingest_batch(conflicting)
        self.assertEqual(context.exception.code, "E_CONFLICTING_REPORT")
        self.assertEqual(engine.list_windows("/api/test")["windows"][0]["sample_count"], 2)
        persisted = engine.get_batch("b-1", 1)["batch"]
        self.assertEqual(persisted["status"], "COMMITTED")
        self.assertEqual(persisted["event_count"], 3)

    def test_concurrent_identical_submissions_commit_once(self) -> None:
        engine = self.make_engine()
        payload = batch("concurrent-1", [event(100), event(200), event(1_100)])
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: engine.ingest_batch(payload), range(16)))
        self.assertEqual(sum(1 for result in results if not result["duplicate"]), 1)
        self.assertEqual(engine.list_windows("/api/test")["windows"][0]["sample_count"], 2)
        self.assertEqual(engine.get_batch("concurrent-1", 1)["batch"]["event_count"], 3)

    def test_window_boundary_is_lower_inclusive_upper_exclusive(self) -> None:
        engine = self.make_engine()
        engine.ingest_batch(batch("boundary-1", [event(1_000), event(1_999), event(2_000)]))
        windows = engine.list_windows("/api/test")["windows"]
        by_start = {window["start_ms"]: window for window in windows}
        self.assertEqual(by_start[1_000]["sample_count"], 2)
        self.assertNotIn(2_000, by_start)

        engine.ingest_batch(batch("boundary-2", [event(3_000)]))
        windows = engine.list_windows("/api/test")["windows"]
        by_start = {window["start_ms"]: window for window in windows}
        self.assertEqual(by_start[2_000]["sample_count"], 1)
        stats = engine.get_window_stats("/api/test", window_start_ms=2_000)["stats"]
        self.assertEqual(stats["window"]["boundary"], "[start_ms,end_ms)")
        self.assertEqual(stats["window"]["start_ms"], 2_000)
        self.assertEqual(stats["window"]["end_ms"], 3_000)

    def test_decision_reads_same_composite_window_for_both_paths(self) -> None:
        engine = self.make_engine()
        engine.ingest_batch(batch("consistent-1", [event(100), event(200), event(1_100)]))
        decision = engine.decide(
            {
                "route": "/api/test",
                "current_concurrency": 0,
                "budget_watermark": "0.8",
                "as_of_ms": 1_100,
            }
        )
        consistency = decision["consistency"]
        self.assertTrue(consistency["same_window_version_in_limiter_and_breaker"])
        self.assertEqual(consistency["limiter_window_version"], consistency["breaker_window_version"])
        state = engine.get_circuit_state("/api/test")
        self.assertEqual(state["composite_version"], decision["composite_version"])
        self.assertEqual(state["window_version"], decision["window"]["version"])
        self.assertEqual(state["consistency"]["breaker_window_version_id"], decision["window"]["version_id"])

    def test_insufficient_samples_stale_window_and_missing_batch(self) -> None:
        engine = self.make_engine(min_samples_p95=20, min_samples_p99=100)
        engine.ingest_batch(batch("small-1", [event(100), event(200), event(1_100)]))
        with self.assertRaises(BusinessError) as strict:
            engine.get_window_stats("/api/test", strict="P99")
        self.assertEqual(strict.exception.code, "E_INSUFFICIENT_SAMPLES")
        with self.assertRaises(BusinessError) as decision:
            engine.decide({"route": "/api/test", "current_concurrency": 0, "as_of_ms": 1_100})
        self.assertEqual(decision.exception.code, "E_INSUFFICIENT_SAMPLES")

        healthy = self.make_engine()
        healthy.ingest_batch(batch("stale-1", [event(100), event(200), event(1_100)]))
        with self.assertRaises(BusinessError) as stale:
            healthy.decide(
                {"route": "/api/test", "current_concurrency": 0, "window_version": 999}
            )
        self.assertEqual(stale.exception.code, "E_STALE_WINDOW")
        with self.assertRaises(BusinessError) as missing:
            healthy.get_batch("does-not-exist", 1)
        self.assertEqual(missing.exception.code, "E_BATCH_NOT_FOUND")

    def test_manual_legal_and_illegal_transitions(self) -> None:
        engine = self.make_engine()
        engine.ingest_batch(batch("state-1", [event(100), event(200), event(1_100)]))
        before = engine.get_circuit_state("/api/test")
        self.assertEqual(before["state"], "CLOSED")

        with self.assertRaises(BusinessError) as illegal:
            engine.manual_breaker_transition(
                {
                    "route": "/api/test",
                    "to_state": "HALF_OPEN",
                    "occurred_at_ms": 1_000,
                    "reason": "SHOULD_FAIL",
                }
            )
        self.assertEqual(illegal.exception.code, "E_INVALID_TRANSITION")
        after_illegal = engine.get_circuit_state("/api/test")
        self.assertEqual(after_illegal["state"], "CLOSED")
        self.assertEqual(after_illegal["breaker_version"], before["breaker_version"])

        opened = engine.manual_breaker_transition(
            {
                "route": "/api/test",
                "to_state": "OPEN",
                "occurred_at_ms": 1_000,
                "reason": "MANUAL_OPEN",
            }
        )
        self.assertEqual(opened["state"], "OPEN")
        open_state = engine.get_circuit_state("/api/test")

        with self.assertRaises(BusinessError) as illegal_close:
            engine.manual_breaker_transition(
                {
                    "route": "/api/test",
                    "to_state": "CLOSED",
                    "occurred_at_ms": 1_100,
                    "reason": "SHOULD_FAIL",
                }
            )
        self.assertEqual(illegal_close.exception.code, "E_INVALID_TRANSITION")
        self.assertEqual(engine.get_circuit_state("/api/test")["state"], "OPEN")

        half = engine.manual_breaker_transition(
            {
                "route": "/api/test",
                "to_state": "HALF_OPEN",
                "occurred_at_ms": open_state["next_probe_at_ms"],
                "reason": "COOLDOWN_ELAPSED",
            }
        )
        self.assertEqual(half["state"], "HALF_OPEN")
        closed = engine.manual_breaker_transition(
            {
                "route": "/api/test",
                "to_state": "CLOSED",
                "occurred_at_ms": open_state["next_probe_at_ms"] + 100,
                "reason": "MANUAL_RECOVERY",
            }
        )
        self.assertEqual(closed["state"], "CLOSED")

    def test_automatic_open_cooldown_and_recovery(self) -> None:
        engine = self.make_engine()
        engine.ingest_batch(
            batch(
                "auto-1",
                [
                    event(100, success=False, latency_us=100_000),
                    event(200, success=False, latency_us=110_000),
                    event(1_000, success=True),
                ],
            )
        )
        self.assertEqual(engine.get_circuit_state("/api/test")["state"], "OPEN")

        engine.ingest_batch(batch("auto-2", [event(1_100), event(1_200), event(2_000)]))
        self.assertEqual(engine.get_circuit_state("/api/test")["state"], "HALF_OPEN")

        engine.ingest_batch(batch("auto-3", [event(2_100), event(2_200), event(3_000)]))
        self.assertEqual(engine.get_circuit_state("/api/test")["state"], "CLOSED")
        transitions = engine.list_transitions("/api/test")["transitions"]
        pairs = [(item["from_state"], item["to_state"]) for item in transitions]
        self.assertIn(("CLOSED", "OPEN"), pairs)
        self.assertIn(("OPEN", "HALF_OPEN"), pairs)
        self.assertIn(("HALF_OPEN", "CLOSED"), pairs)

    def test_quota_propagates_over_multiple_closed_windows(self) -> None:
        engine = self.make_engine()
        engine.ingest_batch(batch("prop-1", [event(100), event(200), event(1_000)]))
        engine.ingest_batch(batch("prop-2", [event(1_100), event(1_200), event(2_000)]))
        engine.ingest_batch(batch("prop-3", [event(2_100), event(2_200), event(3_000)]))
        derivation = engine.get_quota_derivation("/api/test")["derivation"]
        propagation = derivation["propagation"]
        self.assertGreaterEqual(len(propagation["windows"]), 2)
        self.assertEqual(len(propagation["weights_normalized_newest_first"]), len(propagation["windows"]))
        self.assertTrue(all(Decimal(item["weight"]) > 0 for item in propagation["windows"]))
        self.assertIn("weighted_success_compensation", propagation)

    def test_one_batch_evaluates_multiple_windows_in_chronological_order(self) -> None:
        engine = self.make_engine(step_ms=500)
        engine.ingest_batch(
            batch(
                "multi-window",
                [
                    event(100, success=False, latency_us=100_000),
                    event(600, success=False, latency_us=100_000),
                    event(1_100, success=False, latency_us=100_000),
                    event(1_600, success=False, latency_us=100_000),
                    event(2_100, success=False, latency_us=100_000),
                    event(2_600, success=False, latency_us=100_000),
                    event(3_100, success=False, latency_us=100_000),
                ],
            )
        )
        transitions = engine.list_transitions("/api/test")["transitions"]
        self.assertGreaterEqual(len(transitions), 3)
        occurred = [item["occurred_at_ms"] for item in transitions]
        self.assertEqual(occurred, sorted(occurred))
        pairs = [(item["from_state"], item["to_state"]) for item in transitions]
        self.assertIn(("CLOSED", "OPEN"), pairs)
        self.assertIn(("OPEN", "HALF_OPEN"), pairs)
        self.assertEqual(engine.get_circuit_state("/api/test")["state"], "OPEN")

    def test_late_event_versions_old_window_without_regressing_current_composite(self) -> None:
        engine = self.make_engine()
        engine.ingest_batch(batch("late-1", [event(100), event(200), event(1_100)]))
        engine.ingest_batch(batch("late-2", [event(1_200), event(2_100)]))
        before = engine.get_circuit_state("/api/test")
        self.assertEqual(before["window_start_ms"], 1_000)

        engine.ingest_batch(batch("late-3", [event(300)]))
        after = engine.get_circuit_state("/api/test")
        self.assertEqual(after["window_start_ms"], 1_000)
        self.assertGreater(after["composite_version"], before["composite_version"])
        old_window = engine.get_window_stats("/api/test", window_start_ms=0)["window_version"]
        self.assertEqual(old_window, 2)


if __name__ == "__main__":
    unittest.main()

