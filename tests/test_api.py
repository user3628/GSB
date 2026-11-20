from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from ree.api import create_app


def test_http_api_flow_and_errors(workspace: Path) -> None:
    client = TestClient(create_app(workspace))
    assert client.get("/health").status_code == 200

    run_response = client.post("/run", json={})
    assert run_response.status_code == 200
    assert run_response.json()["status"] == "complete"

    status = client.get("/status").json()
    assert status["all_fresh"]
    assert [status["stages"][stage]["checkpoint_seq"] for stage in ("parse", "estimate", "extract", "validate")] == [1, 2, 3, 4]

    results = client.get("/results", params={"doc_id": "L001"})
    assert results.status_code == 200
    assert results.json()["document"]["doc_id"] == "L001"

    convert = client.post(
        "/convert",
        json={"doc_id": "d002", "from_format": "BIO", "to_format": "JSON"} ,
    )
    assert convert.status_code == 200
    assert convert.json()["to_format"] == "JSON_INTERVALS"

    diagnostics = client.get("/diagnostics")
    assert diagnostics.status_code == 200
    assert diagnostics.json()["parse_summary"]["diagnostic_codes"]["E_ALIGN_FAIL"] == 1

    unsupported = client.post(
        "/compat/check",
        json={
            "payload": {
                "format": "JSON_INTERVALS",
                "schema_version": 99,
                "doc_id": "high_version",
                "tokens": ["x"],
                "text": "x",
                "source": "http",
            }
        },
    )
    assert unsupported.status_code == 409
    assert unsupported.json()["error"]["code"] == "E_UNSUPPORTED_VERSION"

    invalid_stage = client.post("/run-stage/unknown")
    assert invalid_stage.status_code == 409
    assert invalid_stage.json()["error"]["code"] == "E_INVALID_STAGE"

    assert client.post("/stop", json={}).json()["status"] == "stop_requested"
    assert client.post("/resume", json={}).json()["status"] == "complete"
