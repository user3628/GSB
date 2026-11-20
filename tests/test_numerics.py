from __future__ import annotations

import json
import unittest
from decimal import Decimal

from rateguard.config import GuardConfig
from rateguard.numerics import (
    canonical_json,
    compute_window_stats,
    derive_quota,
    neumaier_sum,
    propagate_metrics,
    quantile_value,
)


def samples_for(count: int, failures: int = 0) -> list[dict[str, object]]:
    rows = []
    for index in range(count):
        rows.append(
            {
                "timestamp_ms": 1_700_000_000_000 + index,
                "batch_id": "batch",
                "sequence": 1,
                "event_index": index,
                "success": index >= failures,
                "latency_us": index + 1,
            }
        )
    return rows


class QuantileContractTests(unittest.TestCase):
    def test_known_nearest_rank_and_linear_values(self) -> None:
        latencies = list(range(1, 101))
        nr_50, _ = quantile_value(latencies, Decimal("0.50"), "nearest_rank")
        lin_50, _ = quantile_value(latencies, Decimal("0.50"), "linear")
        nr_95, _ = quantile_value(latencies, Decimal("0.95"), "nearest_rank")
        lin_95, _ = quantile_value(latencies, Decimal("0.95"), "linear")
        nr_99, _ = quantile_value(latencies, Decimal("0.99"), "nearest_rank")
        lin_99, _ = quantile_value(latencies, Decimal("0.99"), "linear")
        self.assertEqual(nr_50, Decimal(50))
        self.assertEqual(lin_50, Decimal("50.5"))
        self.assertEqual(nr_95, Decimal(95))
        self.assertEqual(lin_95, Decimal("95.05"))
        self.assertEqual(nr_99, Decimal(99))
        self.assertEqual(lin_99, Decimal("99.01"))

    def test_fixed_decimal_output_and_bitwise_recompute(self) -> None:
        config = GuardConfig()
        first = compute_window_stats(
            route="/r",
            start_ms=1_700_000_000_000,
            end_ms=1_700_000_008_000,
            samples=samples_for(120, failures=3),
            config=config,
        )
        shuffled = list(reversed(samples_for(120, failures=3)))
        second = compute_window_stats(
            route="/r",
            start_ms=1_700_000_000_000,
            end_ms=1_700_000_008_000,
            samples=shuffled,
            config=config,
        )
        self.assertEqual(canonical_json(first), canonical_json(second))
        self.assertEqual(first["quantiles"]["linear"]["P95"]["value_us"], "114.050000")
        self.assertEqual(first["success_rate"]["value"], "0.975000000000")
        self.assertEqual(first["success_rate"]["exact_fraction"], "117/120")
        self.assertEqual(first["numeric_contract"]["rounding"], "ROUND_HALF_EVEN")
        self.assertTrue(first["quantiles"]["nearest_rank"]["P95"]["value_us"].endswith(".000000"))

    def test_compensated_sum_is_exact_for_indicators(self) -> None:
        total, compensation = neumaier_sum([Decimal(1), Decimal(0), Decimal(1), Decimal(1)])
        self.assertEqual(total, Decimal(3))
        self.assertEqual(compensation, Decimal(0))

    def test_error_bounds_and_method_spread_are_quantified(self) -> None:
        stats = compute_window_stats(
            route="/r",
            start_ms=0,
            end_ms=8_000,
            samples=samples_for(120),
            config=GuardConfig(),
        )
        comparison = stats["method_comparison"]
        self.assertEqual(comparison["max_abs_deviation_us"], "0.500000")
        for level in ("P50", "P95", "P99"):
            bound = stats["error_propagation"]["quantile_bounds"][level]
            self.assertGreaterEqual(
                Decimal(bound["combined_upper_bound_us"]),
                Decimal(bound["statistical_width_us"]),
            )
            self.assertEqual(bound["confidence"], "0.950000")
        self.assertEqual(
            stats["success_rate"]["compensated_sum"]["accumulation_error_upper_bound"],
            "0.000000000000",
        )
        self.assertIn("window_length_ms", stats["error_propagation"])

    def test_p99_requires_declared_sample_floor(self) -> None:
        stats = compute_window_stats(
            route="/r",
            start_ms=0,
            end_ms=8_000,
            samples=samples_for(40),
            config=GuardConfig(),
        )
        self.assertIsNone(stats["quantiles"]["linear"]["P99"]["value_us"])
        self.assertEqual(
            stats["quantiles"]["linear"]["P99"]["business_code_if_strict"],
            "E_INSUFFICIENT_SAMPLES",
        )


class QuotaContractTests(unittest.TestCase):
    def _propagation(self, count: int = 120) -> dict[str, object]:
        config = GuardConfig(max_quota=100)
        stats = compute_window_stats(
            route="/r",
            start_ms=0,
            end_ms=8_000,
            samples=samples_for(count, failures=3),
            config=config,
        )
        views = []
        for version in (3, 2, 1):
            item = dict(stats)
            item["window_version"] = version
            item["window_version_id"] = version
            item["error_contract"] = {
                "success_rate_ci_half_width": "0.020000000000",
                "method_spread_us": "0.500000",
            }
            views.append(item)
        return propagate_metrics(views, config.propagation_weights)

    def test_quota_is_bounded_fixed_point_and_damped(self) -> None:
        config = GuardConfig(max_quota=100)
        propagation = self._propagation()
        first = derive_quota(
            propagation=propagation,
            breaker_state="CLOSED",
            current_concurrency=8,
            previous_quota=None,
            budget_watermark=Decimal("0.80"),
            target_latency_us=50_000,
            config=config,
        )
        second = derive_quota(
            propagation=propagation,
            breaker_state="CLOSED",
            current_concurrency=60,
            previous_quota=int(first["final_quota"]),
            budget_watermark=Decimal("0.80"),
            target_latency_us=50_000,
            config=config,
        )
        self.assertGreaterEqual(first["final_quota"], config.min_quota)
        self.assertLessEqual(first["final_quota"], config.max_quota)
        self.assertGreaterEqual(second["final_quota"], config.min_quota)
        self.assertLessEqual(second["final_quota"], config.max_quota)
        self.assertEqual(second["steps"][-1]["name"], "damped_quota")
        fixed_step = next(step for step in second["steps"] if step["name"] == "scale_factor_fixed_point")
        self.assertEqual(fixed_step["fixed_point_scale"], 1_000_000)
        self.assertGreaterEqual(fixed_step["fixed_point"], 0)
        self.assertLessEqual(fixed_step["fixed_point"], 1_000_000)

        repeated = derive_quota(
            propagation=propagation,
            breaker_state="CLOSED",
            current_concurrency=60,
            previous_quota=int(first["final_quota"]),
            budget_watermark=Decimal("0.80"),
            target_latency_us=50_000,
            config=config,
        )
        self.assertEqual(json.dumps(second, sort_keys=True), json.dumps(repeated, sort_keys=True))

    def test_open_and_half_open_breaker_weights(self) -> None:
        config = GuardConfig(max_quota=100)
        propagation = self._propagation()
        opened = derive_quota(
            propagation=propagation,
            breaker_state="OPEN",
            current_concurrency=0,
            previous_quota=None,
            budget_watermark=Decimal("1"),
            target_latency_us=50_000,
            config=config,
        )
        half_open = derive_quota(
            propagation=propagation,
            breaker_state="HALF_OPEN",
            current_concurrency=0,
            previous_quota=0,
            budget_watermark=Decimal("1"),
            target_latency_us=50_000,
            config=config,
        )
        self.assertEqual(opened["target_before_damping"], 0)
        self.assertGreaterEqual(half_open["target_before_damping"], 1)
        low_propagation = {
            "weighted_success_rate": "0",
            "propagated_p95_us": "5000000",
            "windows": [],
        }
        probe_floor = derive_quota(
            propagation=low_propagation,
            breaker_state="HALF_OPEN",
            current_concurrency=0,
            previous_quota=0,
            budget_watermark=Decimal("0"),
            target_latency_us=1,
            config=config,
        )
        breaker_step = next(
            step for step in probe_floor["steps"] if step["name"] == "breaker_adjusted_quota"
        )
        self.assertEqual(breaker_step["half_open_probe_floor"], 1)
        self.assertEqual(probe_floor["target_before_damping"], 1)


if __name__ == "__main__":
    unittest.main()

