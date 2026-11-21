from __future__ import annotations

import tempfile
import unittest
from decimal import Decimal

from tests.helpers import (
    group_payload,
    load_json,
    make_service,
    report_payload,
    round_payload,
)
from replica_repair.errors import BusinessError
from replica_repair.models import parse_group, parse_report_batch
from replica_repair.numerics import (
    compensated_sum_diagnostics,
    convergence_decision,
    kahan_sum,
    normalize_weights,
)


class ValidationAndNumericsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.service = make_service(self.tempdir)
        self.group_payload = group_payload()
        self.service.create_group(self.group_payload)
        self.group = parse_group(self.group_payload)

    def test_group_validation_rejects_bad_weight_math(self) -> None:
        bad_count = dict(self.group_payload)
        bad_count["replica_count"] = 5
        with self.assertRaises(BusinessError) as raised:
            parse_group(bad_count)
        self.assertEqual(raised.exception.code, "E_GROUP_INVALID")

        negative = dict(self.group_payload)
        negative["weights"] = dict(self.group_payload["weights"])
        negative["weights"]["replica-0"] = -0.1
        with self.assertRaises(BusinessError) as raised:
            parse_group(negative)
        self.assertEqual(raised.exception.code, "E_GROUP_INVALID")

        zero_sum = dict(self.group_payload)
        zero_sum["weights"] = {
            "replica-0": 0,
            "replica-1": 0,
            "replica-2": 0,
            "replica-3": 0,
        }
        with self.assertRaises(BusinessError) as raised:
            parse_group(zero_sum)
        self.assertEqual(raised.exception.code, "E_GROUP_INVALID")

    def test_report_validation_rejects_negative_float_and_overflow(self) -> None:
        cases = [
            ("negative", {"block_count": -1}),
            ("float", {"block_count": 1.5}),
            ("overflow", {"block_count": 2**63}),
            ("negative-synced", {"synced_bytes": -1}),
            ("synced-overflow", {"synced_bytes": 2**63}),
            ("unknown-replica", {"replica_id": "replica-9"}),
        ]
        for label, overrides in cases:
            with self.subTest(label):
                payload = report_payload(
                    "r0_ontime.json", round_id="validation", **overrides
                )
                with self.assertRaises(BusinessError) as raised:
                    parse_report_batch({"reports": [payload]}, group=self.group)
                self.assertEqual(raised.exception.code, "E_REPORT_INVALID")

    def test_invalid_report_batch_is_atomic(self) -> None:
        self.service.create_round(round_payload("round-invalid"))
        valid = report_payload("r0_ontime.json", round_id="round-invalid")
        invalid = report_payload(
            "r2_ontime.json", round_id="round-invalid", block_count=-1
        )
        with self.assertRaises(BusinessError) as raised:
            self.service.submit_reports(
                "round-invalid",
                {
                    "group_id": "grp-fixed",
                    "group_version": 1,
                    "reports": [valid, invalid],
                },
            )
        self.assertEqual(raised.exception.code, "E_REPORT_INVALID")
        status = self.service.get_round_status("round-invalid")["round"]
        self.assertEqual(status["eligible_report_count"], 0)
        self.assertEqual(status["pending_late_report_count"], 0)

    def test_three_normalization_modes_are_explicit_and_deterministic(self) -> None:
        weights = self.group.weights
        present = ["replica-2", "replica-0"]
        equal = normalize_weights(weights, present, "ALL_EQUAL")
        reported = normalize_weights(weights, present, "REPORTED_RATIO")
        missing = normalize_weights(weights, present, "MISSING_ZERO")
        self.assertEqual(equal["normalized_weights"]["replica-0"], "0.250000000000000000")
        self.assertEqual(equal["normalized_weights"]["replica-1"], "0.000000000000000000")
        self.assertEqual(
            reported["normalized_weights"]["replica-0"], "0.666666666666666722"
        )
        self.assertEqual(
            missing["normalized_weights"]["replica-0"], "0.400000000000000060"
        )
        self.assertEqual(equal["mode"], "ALL_EQUAL")
        self.assertEqual(reported["mode"], "REPORTED_RATIO")
        self.assertEqual(missing["mode"], "MISSING_ZERO")
        self.assertEqual(
            reported["normalized_weights"],
            normalize_weights(weights, reversed(present), "REPORTED_RATIO")[
                "normalized_weights"
            ],
        )

    def test_kahan_bound_on_fixed_mixed_magnitude_case(self) -> None:
        case = load_json("samples/numerics_cases.json")
        terms = [(item["id"], Decimal(item["value"])) for item in case["terms"]]
        exact, diagnostics = compensated_sum_diagnostics(terms, label="fixed-case")
        self.assertEqual(exact, Decimal("2"))
        self.assertTrue(diagnostics["kahan_within_bound"])
        self.assertTrue(diagnostics["naive_within_bound"])
        self.assertGreater(float(diagnostics["naive_error_bound"]), 0)
        self.assertGreater(float(diagnostics["kahan_error_bound"]), 0)
        canonical_float_terms = [
            float(value) for _, value in sorted(terms, key=lambda item: item[0])
        ]
        self.assertEqual(
            kahan_sum(canonical_float_terms),
            float(diagnostics["kahan_float_sum"]),
        )
        reversed_exact, reversed_diagnostics = compensated_sum_diagnostics(
            list(reversed(terms)), label="fixed-case"
        )
        self.assertEqual(exact, reversed_exact)
        self.assertEqual(
            diagnostics["decimal_sum"], reversed_diagnostics["decimal_sum"]
        )
        self.assertEqual(
            diagnostics["canonical_order"], reversed_diagnostics["canonical_order"]
        )

    def test_relative_tolerance_boundary_is_stable(self) -> None:
        decision = convergence_decision(
            Decimal("0.5000000000"),
            Decimal("0.5000000001"),
            Decimal("0.0000000002"),
            canonical_order=["replica-0:r0"],
        )
        self.assertTrue(decision["converged"])
        self.assertEqual(decision["comparison"]["actual_progress"], "0.500000000000")
        self.assertEqual(decision["comparison"]["target_progress"], "0.500000000100")
        strict = convergence_decision(
            Decimal("0.5000000000"),
            Decimal("0.5000000001"),
            Decimal("0"),
            canonical_order=["replica-0:r0"],
        )
        self.assertFalse(strict["converged"])


if __name__ == "__main__":
    unittest.main()

