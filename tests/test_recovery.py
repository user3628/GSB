from __future__ import annotations

import tempfile
import unittest
from tests.helpers import (
    group_payload,
    make_service,
    report_payload,
    round_payload,
)


class RecoveryTests(unittest.TestCase):
    def test_startup_recovery_aborts_prepared_intent_and_retry_commits(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            first = make_service(tempdir)
            first.create_group(group_payload())
            first.create_round(round_payload("round-recover"))
            first.submit_reports(
                "round-recover",
                {
                    "group_id": "grp-fixed",
                    "group_version": 1,
                    "reports": [
                        report_payload("r0_ontime.json", round_id="round-recover"),
                        report_payload("r2_ontime.json", round_id="round-recover"),
                    ],
                },
            )
            round_row = first.db.get_round("round-recover")
            group = first.db.get_group("grp-fixed")
            eligible = first.db.eligible_reports("round-recover")
            late = first.db.late_reports_for_submission("round-recover")
            fingerprint = first._fingerprint(
                round_row,
                group,
                "REPORTED_RATIO",
                eligible,
                late,
            )
            first.db.begin_intent(
                round_row,
                fingerprint=fingerprint,
                now=first.now_fn(),
            )
            self.assertEqual(first.db.get_round("round-recover")["status"], "AGGREGATING")
            self.assertEqual(first.db.get_intent("round-recover")["state"], "PREPARED")

            # A new service instance models process restart after SIGKILL.
            second = make_service(tempdir)
            self.assertIsNotNone(second.startup_recovery_report)
            report = second.startup_recovery_report
            self.assertEqual(report["incomplete_intent_count"], 1)
            self.assertEqual(report["aborted_count"], 1)
            self.assertEqual(report["intents"][0]["action"], "MARKED_ABORTED")
            self.assertEqual(report["intents"][0]["round_status_after"], "ABORTED")
            self.assertEqual(second.db.get_round("round-recover")["status"], "ABORTED")
            self.assertEqual(second.db.get_intent("round-recover")["state"], "ABORTED")
            persisted = second.latest_recovery_report()["report"]
            self.assertEqual(persisted["report_digest"], report["report_digest"])

            retry = second.aggregate(
                "round-recover",
                {"normalization": "REPORTED_RATIO", "strict_wait": False},
            )
            self.assertFalse(retry["reused"])
            self.assertEqual(
                retry["conclusion"]["aggregation"]["execution_attempt"], 2
            )
            query = second.get_conclusion("round-recover")
            self.assertEqual(
                query["conclusion_digest"], retry["conclusion_digest"]
            )
            repeat = second.aggregate(
                "round-recover",
                {"normalization": "REPORTED_RATIO", "strict_wait": False},
            )
            self.assertTrue(repeat["reused"])
            self.assertEqual(
                repeat["conclusion_digest"], retry["conclusion_digest"]
            )


if __name__ == "__main__":
    unittest.main()

