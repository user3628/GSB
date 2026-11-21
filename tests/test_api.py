from __future__ import annotations

import unittest

from mpc.api import create_app

from .helpers import ServiceCase


class HttpApiTests(unittest.TestCase):
    def setUp(self):
        self.case = ServiceCase()
        self.app = create_app(
            self.case.db, self.case.samples, recover_on_start=False
        )
        self.client = self.app.test_client()

    def tearDown(self):
        self.case.close()

    def test_error_envelope_and_main_read_routes(self):
        health = self.client.get("/api/health")
        self.assertEqual(health.status_code, 200)
        self.assertTrue(health.get_json()["ok"])

        bad = self.client.post(
            "/api/policies/snapshots",
            json={
                "commit_id": "api-bad",
                "version": 1,
                "loss_scale": 64.0,
                "shards": {"s0": "FP8", "s1": "FP32", "s2": "FP16", "s3": "FP32"},
            },
        )
        self.assertEqual(bad.status_code, 422)
        payload = bad.get_json()
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["code"], "E_INVALID_PRECISION")
        self.assertEqual(payload["error"]["code"], "E_INVALID_PRECISION")

        policy = self.client.post(
            "/api/policies/snapshots",
            json={
                "commit_id": "api-policy-1",
                "version": 1,
                "loss_scale": 64.0,
                "loss_scale_initial": 64.0,
                "growth_factor": 2.0,
                "shards": {"s0": "FP16", "s1": "FP32", "s2": "FP16", "s3": "FP32"},
            },
        )
        self.assertEqual(policy.status_code, 200)
        checkpoint = self.client.post(
            "/api/checkpoints",
            json={
                "commit_id": "api-checkpoint-1",
                "round": 1,
                "scheduler_position": {"step": 10},
                "data_cursor": {"batch": 1},
            },
        )
        self.assertEqual(checkpoint.status_code, 200)
        derived = self.client.get("/api/versions/1/derived")
        self.assertEqual(derived.status_code, 200)
        self.assertEqual(derived.get_json()["data"]["aggregate"]["shard_count"], 4)
        manifest = self.client.get("/api/checkpoints/latest")
        self.assertEqual(manifest.status_code, 200)
        self.assertEqual(len(manifest.get_json()["data"]["shards"]), 4)
        shard = self.client.get("/api/versions/1/derived?policy_version=1")
        self.assertEqual(shard.status_code, 200)


if __name__ == "__main__":
    unittest.main()
