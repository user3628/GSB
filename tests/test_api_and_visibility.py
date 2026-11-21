from __future__ import annotations

import sqlite3

import pytest
from fastapi.testclient import TestClient

from prep_pipeline.api import create_app
from prep_pipeline.errors import PipelineError
from prep_pipeline.service import PipelineService

from tests.helpers import batch_request


def test_http_api_success_and_uniform_errors(
    bootstrapped_service: PipelineService,
) -> None:
    app = create_app(bootstrapped_service.settings, bootstrapped_service)
    with TestClient(app) as client:
        health = client.get("/health")
        assert health.status_code == 200

        payload = {
            "batch_id": "api-batch",
            "policy_version": 1,
            "input_path": "records_valid.tsv",
        }
        created = client.post("/v1/batches", json=payload)
        assert created.status_code == 200, created.text
        assert created.json()["status"] == "committed"

        stages = client.get("/v1/batches/api-batch/stages")
        assert stages.status_code == 200
        assert len(stages.json()["stages"]) == 5

        shards = client.get("/v1/batches/api-batch/shards")
        assert shards.status_code == 200
        assert sum(shard["sample_count"] for shard in shards.json()["shards"]) == 12

        verified = client.post("/v1/verify/api-batch", json={})
        assert verified.status_code == 200
        assert verified.json()["status"] == "match"

        failed = client.post(
            "/v1/batches",
            json={
                "batch_id": "api-ratio-error",
                "policy_version": 1,
                "input_path": "records_valid.tsv",
                "injections": {"sample_ratio": 1.5},
            },
        )
        assert failed.status_code == 422
        assert failed.json()["error"]["code"] == "E_SAMPLE_RATIO_INVALID"

        invalid = client.post(
            "/v1/batches",
            json={"batch_id": "bad/id", "policy_version": 1, "input_path": "records_valid.tsv"},
        )
        assert invalid.status_code == 422
        assert invalid.json()["error"]["code"] == "E_REQUEST_INVALID"

        missing = client.get("/v1/batches/does-not-exist")
        assert missing.status_code == 404
        assert missing.json()["error"]["code"] == "E_BATCH_NOT_FOUND"

        route = client.get("/v1/not-a-route")
        assert route.status_code == 404
        assert route.json()["error"]["code"] == "E_ROUTE_NOT_FOUND"


def test_complete_stage_rows_are_hidden_until_committed_status(
    bootstrapped_service: PipelineService,
) -> None:
    batch = bootstrapped_service.submit_batch(batch_request("visibility-boundary"))
    assert len(batch["stages"]) == 5
    assert batch["shards"]
    with sqlite3.connect(bootstrapped_service.settings.db_path) as conn:
        conn.execute(
            "UPDATE batches SET status = 'running' WHERE batch_id = 'visibility-boundary'"
        )
        conn.commit()

    with pytest.raises(PipelineError) as exc:
        bootstrapped_service.get_batch("visibility-boundary")
    assert exc.value.code == "E_BATCH_NOT_COMMITTED"
    assert exc.value.context["visibility_boundary"].startswith("status=committed")
