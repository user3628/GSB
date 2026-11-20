"""Requirement 1: session model, conflict policy, E_PART_INVALID isolation."""
from tests.conftest import create_session, upload_part  # noqa: E402


def test_conflict_reject_then_allow(client):
    first = create_session(client, object_key="k/conflict")
    r = client.post("/api/v1/sessions", json={
        "object_key": "k/conflict", "min_part_size": 1, "max_part_size": 1024,
    })
    assert r.status_code == 409
    body = r.json()
    assert body["error"]["code"] == "E_SESSION_CONFLICT"
    assert first["session_id"] in body["error"]["details"]["existing_session_ids"]

    r = client.post("/api/v1/sessions", json={
        "object_key": "k/conflict", "min_part_size": 1, "max_part_size": 1024,
        "conflict_policy": "allow",
    })
    assert r.status_code == 200
    data = r.json()["data"]
    assert data["coexistence"] is True
    assert first["session_id"] in data["coexisting_session_ids"]


def test_invalid_part_number_out_of_range_does_not_touch_session(client):
    sess = create_session(client)
    before = client.get(f"/api/v1/sessions/{sess['session_id']}").json()["data"]
    r = upload_part(client, sess["session_id"], 0, b"x")
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "E_PART_INVALID"
    r = upload_part(client, sess["session_id"], 10001, b"x")
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "E_PART_INVALID"
    after = client.get(f"/api/v1/sessions/{sess['session_id']}").json()["data"]
    assert after["part_count"] == before["part_count"] == 0
    assert after["total_size"] == before["total_size"] == 0
    assert after["status"] == "open"


def test_part_size_bounds_enforced(client):
    sess = create_session(client, min_part_size=64, max_part_size=128)
    r = upload_part(client, sess["session_id"], 1, b"small")
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "E_PART_INVALID"
    r = upload_part(client, sess["session_id"], 1, b"x" * 129)
    assert r.status_code == 400
    r = upload_part(client, sess["session_id"], 1, b"x" * 100)
    assert r.status_code == 200


def test_last_part_exemption_uses_expected_total(client):
    sess = create_session(client, min_part_size=64, max_part_size=128,
                          expected_total_size=180)
    assert upload_part(client, sess["session_id"], 1, b"x" * 128).status_code == 200
    # 52 bytes would normally violate the 64-byte lower bound.
    r = upload_part(client, sess["session_id"], 2, b"y" * 52)
    assert r.status_code == 200


def test_bad_base64_returns_business_envelope(client):
    sess = create_session(client)
    r = client.post(f"/api/v1/sessions/{sess['session_id']}/parts/1",
                    json={"content_base64": "!!!not-base64!!!"})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "E_PART_INVALID"


def test_validation_error_uses_unified_envelope(client):
    r = client.post("/api/v1/sessions", json={"object_key": ""})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "E_REQUEST_INVALID"
