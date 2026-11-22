from __future__ import annotations

import json
import random
import re
import tempfile
import unittest
from pathlib import Path

from slo_budget import codec
from slo_budget.calculator import calculate
from slo_budget.events import aggregate_events_to_windows, load_request_events
from slo_budget.errors import SloError
from slo_budget.models import WindowRecord, canonical_json

from tests.support import AS_OF_MS, CURRENT_BIN, EVENTS, load_samples_summary, metric_map


class CalculatorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.records = codec.decode(CURRENT_BIN).records
        cls.summary = load_samples_summary()

    def test_sample_expected_sli_burn_and_grades(self) -> None:
        expected = self.summary["expected_query"]
        for service, series_map in expected.items():
            for sli_type, series_expected in series_map.items():
                result = calculate(
                    self.records,
                    service=service,
                    sli_type=sli_type,
                    as_of_ms=AS_OF_MS,
                )
                self.assertEqual(result["alert"]["grade"], series_expected["alert_grade"])
                windows = metric_map(result)
                for window_ms, expected_window in series_expected["windows"].items():
                    self.assertEqual(windows[window_ms]["sli"], expected_window["sli"])
                    self.assertEqual(
                        windows[window_ms]["error_budget"]["burn_rate"],
                        expected_window["burn_rate"],
                    )
                    self.assertEqual(
                        windows[window_ms]["counts"]["good_events"],
                        expected_window["good_events"],
                    )
                    self.assertEqual(
                        windows[window_ms]["counts"]["total_events"],
                        expected_window["total_events"],
                    )
                    self.assertRegex(windows[window_ms]["sli"], r"^\d+\.\d{6}$")
                    self.assertRegex(
                        windows[window_ms]["error_budget"]["burn_rate"],
                        r"^-?\d+\.\d{6}$",
                    )
                    quantiles = windows[window_ms]["bucket_sli_quantiles"]
                    self.assertEqual(quantiles["rounding_mode"], "ROUND_HALF_UP")
                    for name in ("p50", "p95", "p99"):
                        self.assertRegex(quantiles[name], r"^\d+\.\d{6}$")

    def test_zero_total_is_deterministic_and_insufficient(self) -> None:
        empty = calculate(
            (),
            service="no-data",
            sli_type="availability",
            as_of_ms=AS_OF_MS,
        )
        self.assertEqual(empty["business_code"], "E_INSUFFICIENT_SAMPLES")
        self.assertEqual(empty["alert"]["grade"], "UNKNOWN")
        for window in empty["windows"]:
            self.assertEqual(window["counts"]["total_events"], 0)
            self.assertEqual(window["sli"], "1.000000")
            self.assertEqual(window["error_budget"]["burn_rate"], "0.000000")
            self.assertEqual(window["sample_gate"]["status"], "NO_DATA")
            self.assertEqual(window["sample_gate"]["business_code"], "E_INSUFFICIENT_SAMPLES")

    def test_nonzero_but_insufficient_samples(self) -> None:
        record = WindowRecord(
            service="small",
            sli_type="availability",
            window_start_ms=AS_OF_MS - 300_000,
            window_length_ms=300_000,
            good_events=10,
            total_events=10,
            late_count=0,
            last_write_ms=AS_OF_MS,
            format_version=2,
            snapshot_sequence=1,
        )
        result = calculate(
            (record,),
            service="small",
            sli_type="availability",
            as_of_ms=AS_OF_MS,
        )
        self.assertEqual(result["business_code"], "E_INSUFFICIENT_SAMPLES")
        self.assertEqual(result["alert"]["grade"], "UNKNOWN")
        self.assertEqual(result["windows"][0]["sli"], "1.000000")
        self.assertEqual(result["windows"][0]["sample_gate"]["status"], "INSUFFICIENT")

    def test_query_order_does_not_change_any_decimal(self) -> None:
        baseline = calculate(
            self.records,
            service="checkout-api",
            sli_type="availability",
            as_of_ms=AS_OF_MS,
        )
        shuffled = list(self.records)
        random.Random(20261123).shuffle(shuffled)
        for candidate in (tuple(reversed(self.records)), tuple(shuffled)):
            result = calculate(
                candidate,
                service="checkout-api",
                sli_type="availability",
                as_of_ms=AS_OF_MS,
            )
            self.assertEqual(canonical_json(result), canonical_json(baseline))

    def test_short_only_divergence_is_reported_not_silently_dropped(self) -> None:
        records = []
        for index in range(12):
            bad = 5 if index == 11 else 0
            records.append(
                WindowRecord(
                    service="spiky",
                    sli_type="availability",
                    window_start_ms=AS_OF_MS - (12 - index) * 300_000,
                    window_length_ms=300_000,
                    good_events=300 - bad,
                    total_events=300,
                    late_count=0,
                    last_write_ms=AS_OF_MS,
                    format_version=2,
                    snapshot_sequence=1,
                )
            )
        result = calculate(
            records,
            service="spiky",
            sli_type="availability",
            as_of_ms=AS_OF_MS,
        )
        windows = metric_map(result)
        self.assertEqual(windows["300000"]["error_budget"]["burn_rate"], "1.666667")
        self.assertEqual(windows["3600000"]["error_budget"]["burn_rate"], "0.138889")
        self.assertEqual(result["alert"]["grade"], "OK")
        self.assertEqual(result["alert"]["divergence"]["class"], "SHORT_ONLY")
        self.assertEqual(
            result["alert"]["divergence"]["attribution"],
            "recent_regression_inside_short_window",
        )

    def test_unattributable_event_snapshot_difference_fails(self) -> None:
        events, _checksum = load_request_events(EVENTS)
        extra = tuple(
            type(events[0])(
                timestamp_ms=AS_OF_MS - 500,
                service="checkout-api",
                sli_type="availability",
                good=True,
                late=False,
                source_line=100_000 + index,
            )
            for index in range(10)
        )
        with self.assertRaises(SloError) as caught:
            calculate(
                self.records,
                service="checkout-api",
                sli_type="availability",
                as_of_ms=AS_OF_MS,
                events=(*events, *extra),
            )
        self.assertEqual(caught.exception.code, "E_INCONSISTENT_READINGS")
        details = caught.exception.details
        self.assertEqual(details["window_ms"], 300_000)
        self.assertGreater(details["delta"]["total_events"], details["max_attributable_late"])

    def test_event_source_matches_snapshot_exactly(self) -> None:
        events, _checksum = load_request_events(EVENTS)
        result = calculate(
            self.records,
            service="checkout-api",
            sli_type="availability",
            as_of_ms=AS_OF_MS,
            events=events,
        )
        checks = result["consistency"]["cross_source"]
        self.assertEqual(len(checks), 3)
        self.assertTrue(all(item["attribution"] == "exact_match" for item in checks))

    def test_event_aggregation_reproduces_window_counts(self) -> None:
        events, _checksum = load_request_events(EVENTS)
        aggregated = aggregate_events_to_windows(
            events,
            bucket_ms=300_000,
            generated_at_ms=AS_OF_MS,
        )
        original = {
            record.key: (
                record.good_events,
                record.total_events,
                record.late_count,
            )
            for record in self.records
        }
        reproduced = {
            record.key: (
                record.good_events,
                record.total_events,
                record.late_count,
            )
            for record in aggregated
        }
        self.assertEqual(reproduced, original)


if __name__ == "__main__":
    unittest.main()
