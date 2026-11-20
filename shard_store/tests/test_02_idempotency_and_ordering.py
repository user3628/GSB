"""Requirement 2: idempotent re-submission, E_PART_MISMATCH, out-of-order."""
import hashlib

from tests.conftest import create_session, sample_part, upload_part


def test_duplicate_same_content_is_idempotent(client, manifest):
    sess = create_session(client, object_key=manifest["objects"][0]["object_key"])
    content = sample_part(manifest, 0, 1)
    first = upload_part(client, sess["session_id"], 1, content)
    assert first.json()["data"]["status"] == "stored"
    first_at = first.json()["data"].get("uploaded_at")

    second = upload_part(client, sess["session_id"], 1, content)
    assert second.status_code == 200
    data = second.json()["data"]
    assert data["status"] == "idempotent"
    assert data["part_count"] == 1
    if first_at:
        assert data["uploaded_at"] == first_at

    listing = client.get(f"/api/v1/sessions/{sess['session_id']}/parts").json()["data"]
    assert len(listing["parts"]) == 1


def test_duplicate_different_content_mismatch_keeps_first(client):
    sess = create_session(client)
    upload_part(client, sess["session_id"], 3, b"first-content")
    r = upload_part(client, sess["session_id"], 3, b"different-content")
    assert r.status_code == 409
    body = r.json()
    assert body["error"]["code"] == "E_PART_MISMATCH"
    assert body["error"]["details"]["stored_sha256"] == hashlib.sha256(b"first-content").hexdigest()

    listing = client.get(f"/api/v1/sessions/{sess['session_id']}/parts").json()["data"]
    assert len(listing["parts"]) == 1
    assert listing["parts"][0]["sha256"] == hashlib.sha256(b"first-content").hexdigest()


def test_client_digest_mismatch_rejected(client):
    sess = create_session(client)
    r = upload_part(client, sess["session_id"], 1, b"abc",
                    sha256="a" * 64)
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "E_DIGEST_MISMATCH"


def test_out_of_order_submission_merges_in_part_number_order(client, manifest):
    obj = manifest["objects"][0]
    sess = create_session(
        client,
        object_key=obj["object_key"],
        min_part_size=1,
        expected_total_size=obj["expected_total_size"],
    )
    # Deliberately shuffled arrival order: 3, 1, 4, 2.
    for part_number in (3, 1, 4, 2):
        r = upload_part(client, sess["session_id"], part_number,
                        sample_part(manifest, 0, part_number))
        assert r.status_code == 200

    listing = client.get(f"/api/v1/sessions/{sess['session_id']}/parts").json()["data"]
    assert [p["part_number"] for p in listing["parts"]] == [1, 2, 3, 4]

    merged = client.post(f"/api/v1/sessions/{sess['session_id']}/merge", json={})
    assert merged.status_code == 200
    assert merged.json()["data"]["object_sha256"] == obj["expected_object_sha256"]
