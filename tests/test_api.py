from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from schedsim.api import create_app
from schedsim.samples import SampleRepository
from tests.support import make_node, make_payload, make_pod

REPO_ROOT = Path(__file__).resolve().parents[1]


def _client(tmp_path, *, auto_recover: bool = True) -> TestClient:
    app = create_app(
        db_path=str(tmp_path / "api.db"),
        sample_dir=str(REPO_ROOT / "samples"),
        auto_recover=auto_recover,
    )
    return TestClient(app)


def test_api_full_sample_queries_results_rejections_and_node_usage(tmp_path) -> None:
    with _client(tmp_path) as client:
        submitted = client.post(
            "/api/v1/batches/sample",
            json={"sample": "default", "batch_id": "api-sample"},
        )
        assert submitted.status_code == 201, submitted.text
        assert submitted.json()["waiting_queue"] == 7
        for _ in range(20):
            status = client.get("/api/v1/batches/api-sample").json()["status"]
            if status == "COMPLETED":
                break
            round_response = client.post("/api/v1/batches/api-sample/rounds", json={})
            assert round_response.status_code == 200, round_response.text
            usage = round_response.json()["budget_usage"]
            assert usage["concurrency_used"] <= usage["concurrency_limit"]
            assert usage["spent_ms"] <= usage["time_budget_ms"]
        results = client.get("/api/v1/batches/api-sample/results")
        assert results.status_code == 200
        body = results.json()
        assert body["status"] == "COMPLETED"
        placement_names = {item["pod_name"] for item in body["placements"]}
        assert {"web-api", "web-worker", "gpu-trainer", "cache", "affinity-client"}.issubset(
            placement_names
        )
        rejections = client.get("/api/v1/batches/api-sample/rejections").json()
        types = {item["constraint_type"] for item in rejections["rejections"]}
        assert "pod_anti_affinity_required" in types
        assert "insufficient_cpu" in types
        assert "taint_no_schedule" in types
        no_fit = client.get("/api/v1/batches/api-sample/pods/no-fit/rejections").json()
        assert no_fit["rejections"]
        node_usage = client.get(
            "/api/v1/nodes/node-a", params={"batch_id": "api-sample"}
        )
        assert node_usage.status_code == 200
        node_body = node_usage.json()
        assert node_body["remaining"]["cpu_millicores"] >= 0
        assert node_body["placements"]
        completed_again = client.post("/api/v1/batches/api-sample/rounds", json={})
        assert completed_again.status_code == 409
        assert completed_again.json()["code"] == "E_INVALID_TRANSITION"
        recovery = client.get("/api/v1/batches/api-sample/recovery")
        assert recovery.status_code == 200
        assert recovery.json()["exit_report_available"] is False
        assert recovery.json()["exit_report"] is None


def test_api_unified_business_errors(tmp_path) -> None:
    with _client(tmp_path) as client:
        invalid = SampleRepository(REPO_ROOT / "samples").load("boundary_invalid")
        response = client.post("/api/v1/batches", json=invalid)
        assert response.status_code == 422
        assert response.json()["code"] == "E_SPEC_INVALID"
        assert response.json()["context"]["failures"]

        no_batch = client.get("/api/v1/batches/not-there/results")
        assert no_batch.status_code == 404
        assert no_batch.json()["code"] == "E_BATCH_NOT_FOUND"

        topology_payload = make_payload(
            [make_node("n1")],
            [
                make_pod(
                    "p1",
                    affinity={
                        "podAffinity": {
                            "requiredDuringSchedulingIgnoredDuringExecution": [
                                {
                                    "labelSelector": {"matchLabels": {"app": "x"}},
                                    "topologyKey": "missing/topology",
                                }
                            ]
                        }
                    },
                )
            ],
            batch_id="topology-bad",
        )
        topology = client.post("/api/v1/batches", json=topology_payload)
        assert topology.status_code == 422
        assert topology.json()["code"] == "E_TOPOLOGY_KEY_MISSING"

        queue_payload = make_payload(
            [make_node("n1")],
            [make_pod("p1"), make_pod("p2")],
            batch_id="queue-bad",
            budget={"concurrency_limit": 1, "queue_capacity": 0},
        )
        queue = client.post("/api/v1/batches", json=queue_payload)
        assert queue.status_code == 429
        assert queue.json()["code"] == "E_SCHED_QUEUE_FULL"

        assert client.get("/api/v1/batches/queue-bad/results").status_code == 404

