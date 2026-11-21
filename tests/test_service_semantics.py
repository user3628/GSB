from __future__ import annotations

import tempfile
import threading
import unittest
from datetime import timedelta
from decimal import Decimal

from tests.helpers import (
    FIXED_DEADLINE,
    FakeClock,
    group_payload,
    make_service,
    report_payload,
    round_payload,
)
from replica_repair.errors import BusinessError
from replica_repair.numerics import canonical_json


class ServiceSemanticsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.service = make_service(self.tempdir)
        self.service.create_group(group_payload())

    def _round(self, round_id: str, **kwargs: object) -> None:
        payload = round_payload(round_id, **kwargs)
        self.service.create_round(payload)

    def _submit(self, round_id: str, reports: list[dict[str, object]]) -> dict:
        return self.service.submit_reports(
            round_id,
            {
                "group_id": "grp-fixed",
                "group_version": 1,
                "reports": reports,
            },
        )

    def test_missing_normalization_is_rejected_and_three_modes_included(self) -> None:
        self._round("round-a")
        self._submit(
            "round-a",
            [
                report_payload("r0_ontime.json", round_id="round-a"),
                report_payload("r2_ontime.json", round_id="round-a"),
            ],
        )
        with self.assertRaises(BusinessError) as raised:
            self.service.aggregate("round-a", {"strict_wait": False})
        self.assertEqual(raised.exception.code, "E_NORMALIZATION_REQUIRED")

        results = {}
        for index, mode in enumerate(
            ("ALL_EQUAL", "REPORTED_RATIO", "MISSING_ZERO"), start=1
        ):
            round_id = f"round-mode-{index}"
            self._round(round_id)
            self._submit(
                round_id,
                [
                    report_payload("r0_ontime.json", round_id=round_id),
                    report_payload("r2_ontime.json", round_id=round_id),
                ],
            )
            result = self.service.aggregate(
                round_id, {"normalization": mode, "strict_wait": False}
            )
            results[mode] = result["conclusion"]
            self.assertEqual(result["conclusion"]["normalization"]["mode"], mode)
            self.assertEqual(set(result["conclusion"]["normalization"]["normalized_weights"]), {
                "replica-0",
                "replica-1",
                "replica-2",
                "replica-3",
            })
        totals = {
            mode: conclusion["estimates"]["estimated_total_bytes"]
            for mode, conclusion in results.items()
        }
        self.assertEqual(len(set(totals.values())), 3)
        self.assertLess(totals["ALL_EQUAL"], totals["MISSING_ZERO"])
        self.assertLess(totals["MISSING_ZERO"], totals["REPORTED_RATIO"])

    def test_report_order_does_not_change_estimates_or_decision(self) -> None:
        reports = [
            report_payload("r0_ontime.json", round_id="order-a"),
            report_payload("r2_ontime.json", round_id="order-a"),
        ]
        reversed_reports = [
            report_payload("r0_ontime.json", round_id="order-b"),
            report_payload("r2_ontime.json", round_id="order-b"),
        ]
        self._round("order-a")
        self._round("order-b")
        self._submit("order-a", reports)
        self._submit("order-b", list(reversed(reversed_reports)))
        first = self.service.aggregate(
            "order-a", {"normalization": "REPORTED_RATIO", "strict_wait": False}
        )["conclusion"]
        second = self.service.aggregate(
            "order-b", {"normalization": "REPORTED_RATIO", "strict_wait": False}
        )["conclusion"]
        self.assertEqual(first["estimates"], second["estimates"])
        self.assertEqual(
            first["convergence"]["converged"],
            second["convergence"]["converged"],
        )
        self.assertEqual(
            first["convergence"]["comparison"],
            second["convergence"]["comparison"],
        )
        self.assertEqual(
            first["convergence"]["tie_break"]["rule"],
            second["convergence"]["tie_break"]["rule"],
        )
        self.assertEqual(
            first["sum_diagnostics"]["weighted_block_count"]["canonical_order"],
            second["sum_diagnostics"]["weighted_block_count"]["canonical_order"],
        )

    def test_tolerance_changes_conclusion_without_changing_estimate(self) -> None:
        loose_round = "round-loose"
        tight_round = "round-tight"
        self._round(loose_round, tolerance=1e-8)
        self._round(tight_round, tolerance=1e-12)
        for round_id in (loose_round, tight_round):
            self._submit(
                round_id,
                [
                    report_payload("r0_ontime.json", round_id=round_id),
                    report_payload("r2_ontime.json", round_id=round_id),
                ],
            )
        loose = self.service.aggregate(
            loose_round, {"normalization": "ALL_EQUAL", "strict_wait": False}
        )["conclusion"]
        tight = self.service.aggregate(
            tight_round, {"normalization": "ALL_EQUAL", "strict_wait": False}
        )["conclusion"]
        self.assertEqual(
            loose["estimates"]["progress"], tight["estimates"]["progress"]
        )
        self.assertTrue(loose["convergence"]["converged"])
        self.assertFalse(tight["convergence"]["converged"])
        comparison = tight["convergence"]["comparison"]
        self.assertIn("actual_progress", comparison)
        self.assertIn("target_progress", comparison)
        self.assertIn("relative_tolerance", comparison)
        self.assertIn("allowed_delta", comparison)

    def test_progress_out_of_range_returns_business_code_and_no_conclusion(self) -> None:
        group = group_payload()
        group.update(
            {
                "group_id": "grp-one",
                "replica_count": 1,
                "block_size_bytes": 1,
                "weights": {"replica-0": 1.0},
            }
        )
        self.service.create_group(group)
        self.service.create_round(
            {
                "round_id": "round-range",
                "group_id": "grp-one",
                "group_version": 1,
                "deadline": FIXED_DEADLINE,
                "target_progress": 0.5,
                "relative_tolerance": 0.1,
            }
        )
        self.service.submit_reports(
            "round-range",
            {
                "group_id": "grp-one",
                "group_version": 1,
                "reports": [
                    {
                        "report_id": "range-report",
                        "replica_id": "replica-0",
                        "reported_at": "2000-01-01T00:00:01Z",
                        "block_count": 10,
                        "synced_bytes": 11,
                    }
                ],
            },
        )
        with self.assertRaises(BusinessError) as raised:
            self.service.aggregate(
                "round-range",
                {"normalization": "ALL_EQUAL", "strict_wait": False},
            )
        self.assertEqual(raised.exception.code, "E_PROGRESS_OUT_OF_RANGE")
        status = self.service.get_round_status("round-range")["round"]
        self.assertEqual(status["status"], "ABORTED")
        with self.assertRaises(BusinessError) as query_error:
            self.service.get_conclusion("round-range")
        self.assertEqual(query_error.exception.code, "E_ROUND_ABORTED")

    def test_late_report_enters_existing_next_round_only(self) -> None:
        self._round("late-source")
        self._round("late-target")
        late = report_payload("r1_late.json", round_id="late-source")
        response = self._submit("late-source", [late])
        self.assertEqual(response["late_report_ids"], ["late-source:r1-late"])
        self.assertEqual(
            response["late_assignments"]["late-source:r1-late"], "late-target"
        )
        self._submit(
            "late-source",
            [
                report_payload("r0_ontime.json", round_id="late-source"),
                report_payload("r2_ontime.json", round_id="late-source"),
            ],
        )
        self._submit(
            "late-target",
            [
                report_payload("r0_next_round.json", round_id="late-target"),
                report_payload("r2_next_round.json", round_id="late-target"),
            ],
        )
        source = self.service.aggregate(
            "late-source", {"normalization": "REPORTED_RATIO", "strict_wait": False}
        )["conclusion"]
        target = self.service.aggregate(
            "late-target", {"normalization": "REPORTED_RATIO", "strict_wait": False}
        )["conclusion"]
        self.assertEqual(source["reports"]["late_report_count_at_seal"], 1)
        self.assertNotIn(
            "late-source:r1-late",
            [
                item["report_id"]
                for item in source["reports"]["eligible_reports"]
            ],
        )
        self.assertEqual(
            target["reports"]["participating_late_report_ids"],
            ["late-source:r1-late"],
        )
        self.assertNotEqual(
            source["estimates"]["pending_repair_bytes"],
            target["estimates"]["pending_repair_bytes"],
        )

    def test_strict_wait_marks_deadline_and_missing_grace_sources(self) -> None:
        clock = FakeClock()
        self.tempdir.cleanup()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        service = make_service(
            self.tempdir,
            now_fn=clock.now,
            sleep_fn=clock.sleep,
        )
        service.create_group(group_payload())
        deadline = (clock.now() + timedelta(seconds=2)).isoformat().replace(
            "+00:00", "Z"
        )
        service.create_round(round_payload("round-wait", deadline=deadline))
        reported_at = clock.now().isoformat().replace("+00:00", "Z")
        service.submit_reports(
            "round-wait",
            {
                "group_id": "grp-fixed",
                "group_version": 1,
                "reports": [
                    report_payload(
                        "r0_ontime.json",
                        round_id="round-wait",
                        reported_at=reported_at,
                    )
                ],
            },
        )
        result = service.aggregate(
            "round-wait",
            {
                "normalization": "MISSING_ZERO",
                "strict_wait": True,
                "grace_ms": 500,
                "max_wait_ms": 5000,
            },
        )
        timing = result["conclusion"]["timing"]
        self.assertEqual(
            timing["delay_sources"],
            ["WAIT_FOR_DEADLINE", "WAIT_FOR_MISSING_REPLICAS"],
        )
        self.assertEqual(timing["delay_ms"], 2500)
        self.assertFalse(timing["closed_early"])
        self.assertEqual(clock.sleeps, [2.0, 0.5])

    def test_duplicate_concurrent_aggregation_executes_once_and_reuses(self) -> None:
        self.tempdir.cleanup()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        service = make_service(self.tempdir, aggregation_delay_ms=250)
        service.create_group(group_payload())
        service.create_round(round_payload("round-concurrent"))
        service.submit_reports(
            "round-concurrent",
            {
                "group_id": "grp-fixed",
                "group_version": 1,
                "reports": [
                    report_payload("r0_ontime.json", round_id="round-concurrent"),
                    report_payload("r2_ontime.json", round_id="round-concurrent"),
                ],
            },
        )
        results: list[dict] = []
        errors: list[BaseException] = []

        def worker() -> None:
            try:
                results.append(
                    service.aggregate(
                        "round-concurrent",
                        {"normalization": "REPORTED_RATIO", "strict_wait": False},
                    )
                )
            except BaseException as exc:  # pragma: no cover - assertion below
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        self.assertEqual(sorted(result["reused"] for result in results), [False, True])
        self.assertEqual(
            len({result["conclusion_digest"] for result in results}), 1
        )
        self.assertEqual(
            canonical_json(results[0]["conclusion"]),
            canonical_json(results[1]["conclusion"]),
        )
        self.assertEqual(
            len(
                {
                    result["conclusion"]["aggregation"]["input_fingerprint"]
                    for result in results
                }
            ),
            1,
        )

    def test_conflicting_concurrent_normalization_returns_conflict(self) -> None:
        self.tempdir.cleanup()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        service = make_service(self.tempdir, aggregation_delay_ms=150)
        service.create_group(group_payload())
        service.create_round(round_payload("round-conflict"))
        service.submit_reports(
            "round-conflict",
            {
                "group_id": "grp-fixed",
                "group_version": 1,
                "reports": [
                    report_payload("r0_ontime.json", round_id="round-conflict"),
                    report_payload("r2_ontime.json", round_id="round-conflict"),
                ],
            },
        )
        outcomes: list[str] = []
        lock = threading.Lock()

        def worker(mode: str) -> None:
            try:
                service.aggregate(
                    "round-conflict",
                    {"normalization": mode, "strict_wait": False},
                )
                with lock:
                    outcomes.append("OK")
            except BusinessError as exc:
                with lock:
                    outcomes.append(exc.code)

        threads = [
            threading.Thread(target=worker, args=("ALL_EQUAL",)),
            threading.Thread(target=worker, args=("REPORTED_RATIO",)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
        self.assertEqual(sorted(outcomes), ["E_CONFLICTING_ROUND", "OK"])


if __name__ == "__main__":
    unittest.main()

