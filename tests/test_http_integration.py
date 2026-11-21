from __future__ import annotations

import unittest
from concurrent.futures import ThreadPoolExecutor

from tests.helpers import (
    RunningServer,
    expect_http_success,
    group_payload,
    report_payload,
    round_payload,
)


class HttpIntegrationTests(unittest.TestCase):
    def test_fixed_loopback_http_workflow(self) -> None:
        with RunningServer(port=18765) as running:
            client = running.client
            health = expect_http_success(client.get("/health"), context="health")
            self.assertEqual(health["service"], "replica-repair-domain")

            group = group_payload()
            expect_http_success(client.post("/v1/groups", group), context="group")
            expect_http_success(
                client.post("/v1/rounds", round_payload("http-round")),
                context="round",
            )

            status, payload = client.post(
                "/v1/rounds/http-round/aggregate", {"strict_wait": False}
            )
            self.assertEqual(status, 400)
            self.assertEqual(payload["code"], "E_NORMALIZATION_REQUIRED")

            valid = report_payload("r0_ontime.json", round_id="http-round")
            invalid = report_payload(
                "r2_ontime.json", round_id="http-round", block_count=-5
            )
            status, payload = client.post(
                "/v1/rounds/http-round/reports",
                {
                    "group_id": "grp-fixed",
                    "group_version": 1,
                    "reports": [valid, invalid],
                },
            )
            self.assertEqual(status, 422)
            self.assertEqual(payload["code"], "E_REPORT_INVALID")
            round_status = expect_http_success(
                client.get("/v1/rounds/http-round"), context="round status"
            )
            self.assertEqual(round_status["round"]["eligible_report_count"], 0)

            submit_payload = {
                "group_id": "grp-fixed",
                "group_version": 1,
                "reports": [
                    report_payload("r0_ontime.json", round_id="http-round"),
                    report_payload("r2_ontime.json", round_id="http-round"),
                    report_payload("r1_late.json", round_id="http-round"),
                ],
            }
            status, submitted = client.post(
                "/v1/rounds/http-round/reports", submit_payload
            )
            self.assertEqual(status, 202)
            self.assertEqual(len(submitted["late_report_ids"]), 1)

            first = expect_http_success(
                client.post(
                    "/v1/rounds/http-round/aggregate",
                    {"normalization": "REPORTED_RATIO", "strict_wait": False},
                ),
                context="aggregate",
            )
            self.assertFalse(first["reused"])
            second = expect_http_success(
                client.post(
                    "/v1/rounds/http-round/aggregate",
                    {"normalization": "REPORTED_RATIO", "strict_wait": False},
                ),
                context="repeat aggregate",
            )
            self.assertTrue(second["reused"])
            self.assertEqual(first["conclusion_digest"], second["conclusion_digest"])

            status, conflict = client.post(
                "/v1/rounds/http-round/aggregate",
                {"normalization": "ALL_EQUAL", "strict_wait": False},
            )
            self.assertEqual(status, 409)
            self.assertEqual(conflict["code"], "E_CONFLICTING_ROUND")

            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = [
                    pool.submit(client.get, "/v1/rounds/http-round/conclusion")
                    for _ in range(8)
                ]
            digests = set()
            for future in futures:
                status, query = future.result(timeout=5)
                self.assertEqual(status, 200)
                self.assertTrue(query["ok"])
                digests.add(query["conclusion_digest"])
            self.assertEqual(digests, {first["conclusion_digest"]})


if __name__ == "__main__":
    unittest.main()

