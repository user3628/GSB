"""Requirement 3: gap, size and manifest consistency checks before metadata."""
from tests.conftest import create_session, sample_part, upload_part


def test_missing_part_returns_e_part_missing_and_no_object(client, manifest):
    obj = manifest["objects"][1]  # bravo: 3 parts
    sess = create_session(client, object_key=obj["object_key"], min_part_size=1)
    upload_part(client, sess["session_id"], 1, sample_part(manifest, 1, 1))
    upload_part(client, sess["session_id"], 3, sample_part(manifest, 1, 3))

    r = client.post(f"/api/v1/sessions/{sess['session_id']}/merge", json={})
    assert r.status_code == 409
    body = r.json()
    assert body["error"]["code"] == "E_PART_MISSING"
    assert body["error"]["details"]["missing_part_numbers"] == [2]
    # No partially written object metadata may become visible.
    obj_resp = client.get(f"/api/v1/objects/{obj['object_key']}")
    assert obj_resp.status_code == 404
    assert obj_resp.json()["error"]["code"] == "E_OBJECT_NOT_FOUND"
    state = client.get(f"/api/v1/sessions/{sess['session_id']}").json()["data"]
    assert state["status"] == "open"


def test_expected_total_size_mismatch(client, manifest):
    obj = manifest["objects"][0]
    sess = create_session(
        client, object_key=obj["object_key"], min_part_size=1,
        expected_total_size=obj["expected_total_size"] + 10,
    )
    for part_number in range(1, obj["part_count"] + 1):
        upload_part(client, sess["session_id"], part_number,
                    sample_part(manifest, 0, part_number))
    r = client.post(f"/api/v1/sessions/{sess['session_id']}/merge", json={})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "E_SIZE_MISMATCH"
    assert client.get(f"/api/v1/objects/{obj['object_key']}").status_code == 404


def test_empty_session_cannot_merge(client):
    sess = create_session(client)
    r = client.post(f"/api/v1/sessions/{sess['session_id']}/merge", json={})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "E_PART_MISSING"


def test_cancelled_session_rejects_merge_and_parts(client):
    sess = create_session(client)
    upload_part(client, sess["session_id"], 1, b"x" * 40)
    r = client.post(f"/api/v1/sessions/{sess['session_id']}/cancel", json={})
    assert r.status_code == 200
    assert r.json()["data"]["status"] == "cancelled"
    r = client.post(f"/api/v1/sessions/{sess['session_id']}/merge", json={})
    assert r.json()["error"]["code"] == "E_SESSION_CANCELLED"
    r = upload_part(client, sess["session_id"], 2, b"y" * 40)
    assert r.status_code == 409


def test_cancel_is_idempotent(client):
    sess = create_session(client)
    client.post(f"/api/v1/sessions/{sess['session_id']}/cancel", json={})
    r = client.post(f"/api/v1/sessions/{sess['session_id']}/cancel", json={})
    assert r.status_code == 200
    assert r.json()["data"]["status"] == "cancelled"
