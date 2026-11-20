from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from slo_budget.api import create_app
from slo_budget.config import Settings

from .conftest import event, window_events

BASE = 1_767_002_400_000


def make_client(tmp_path: Path) -> TestClient:
    settings = Settings(
        base_dir=tmp_path,
        data_dir=tmp_path / "data",
        db_path=tmp_path / "data" / "api.db",
        windows=(300, 1800, 3600),
        minimum_samples=20,
        exact_event_limit=100,
        late_tolerance_seconds=600,
    )
    return TestClient(create_app(settings))


def test_http_end_to_end_and_unified_errors(tmp_path: Path) -> None:
    with make_client(tmp_path) as client:
        payload = {
            "batch_id": "api-1",
            "source": "test",
            "events": window_events(200, BASE, bad_every=100),
        }
        response = client.post("/v1/events", json=payload)
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["accepted_events"] == 200
        assert body["alerts"][0]["status"] == "CRITICAL"

        metric = client.get(
            "/v1/services/checkout/windows",
            params={"windows": "5m,30m,1h", "sli_type": "availability"},
        )
        assert metric.status_code == 200
        assert metric.json()["windows"][0]["sampling"]["used"] is True

        summary = client.get(
            "/v1/services/checkout/budget-summary",
            params={"sli_type": "availability"},
        )
        assert summary.status_code == 200
        assert summary.json()["consistency_checks"][
            "all_tumbling_windows_equal_service_total"
        ]

        alerts = client.get("/v1/alerts", params={"service": "checkout"})
        assert alerts.status_code == 200
        assert alerts.json()["alerts"][0]["status"] == "CRITICAL"

        recovery = client.get("/v1/recovery")
        assert recovery.status_code == 200
        assert "last_complete_version" in recovery.json()

        late = client.post(
            "/v1/events",
            json={"events": [event(BASE - 10 * 60_000, good=False)]},
        )
        assert late.status_code == 409
        assert late.json()["error"]["code"] == "E_LATE_EVENT"
        rejected = client.get("/v1/rejections", params={"service": "checkout"})
        assert rejected.status_code == 200
        assert rejected.json()["count"] == 1
        assert rejected.json()["rejections"][0]["reason"] in {
            "OUT_OF_ORDER_BEYOND_TOLERANCE",
            "WINDOW_CLOSED",
        }

        short = client.post(
            "/v1/events",
            json={"events": [event(BASE + 20 * 60_000, service="small")]},
        )
        assert short.status_code == 200
        insufficient = client.get(
            "/v1/services/small/budget", params={"window": "5m"}
        )
        assert insufficient.status_code == 422
        assert insufficient.json()["error"]["code"] == "E_INSUFFICIENT_DATA"
        assert insufficient.json()["error"]["details"]["sample_count"] == 1


def test_http_validation_error_has_business_code(tmp_path: Path) -> None:
    with make_client(tmp_path) as client:
        response = client.post("/v1/events", json={"events": [{"service": "x"}]})
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "E_VALIDATION"


def test_repeated_sample_query_is_byte_equivalent(tmp_path: Path) -> None:
    with make_client(tmp_path) as client:
        client.post(
            "/v1/events",
            json={"events": window_events(500, BASE, bad_every=50)},
        )
        url = "/v1/services/checkout/budget"
        params = {"window": "5m", "sli_type": "availability"}
        first = client.get(url, params=params)
        second = client.get(url, params=params)
        assert first.status_code == 200
        assert first.content == second.content
