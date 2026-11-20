"""Requirement 5: fast vs full path produce identical digest and metadata."""
from tests.conftest import create_session, sample_part, upload_part


def _build_session(client, manifest, object_index, key):
    obj = manifest["objects"][object_index]
    sess = create_session(
        client, object_key=key, min_part_size=1,
        expected_total_size=obj["expected_total_size"],
    )
    for part_number in range(1, obj["part_count"] + 1):
        upload_part(client, sess["session_id"], part_number,
                    sample_part(manifest, object_index, part_number))
    return sess, obj


def test_fast_path_reports_persisted_evidence(client, manifest):
    sess, obj = _build_session(client, manifest, 0, "k/fast")
    r = client.post(f"/api/v1/sessions/{sess['session_id']}/merge",
                    json={"strategy": "fast"})
    assert r.status_code == 200
    data = r.json()["data"]
    report = data["strategy_report"]
    assert report["strategy"] == "fast"
    assert report["source_table"] == "parts"
    assert len(report["parts"]) == obj["part_count"]
    assert all("sha256" in p and "uploaded_at" in p for p in report["parts"])
    assert data["object_sha256"] == obj["expected_object_sha256"]


def test_full_path_reports_per_part_timings_and_same_digest(client, manifest):
    sess, obj = _build_session(client, manifest, 0, "k/full")
    r = client.post(f"/api/v1/sessions/{sess['session_id']}/merge",
                    json={"strategy": "full"})
    assert r.status_code == 200
    data = r.json()["data"]
    report = data["strategy_report"]
    assert report["strategy"] == "full"
    assert len(report["per_part_verification"]) == obj["part_count"]
    assert all(item["match"] for item in report["per_part_verification"])
    assert all("verify_elapsed_ms" in item for item in report["per_part_verification"])
    assert "total_verify_elapsed_ms" in report
    assert data["object_sha256"] == obj["expected_object_sha256"]


def test_strategies_equivalent_across_two_sessions_same_key(client, manifest):
    key = "k/equiv"
    fast_sess, obj = _build_session(client, manifest, 1, key)
    fast = client.post(f"/api/v1/sessions/{fast_sess['session_id']}/merge",
                       json={"strategy": "fast"}).json()["data"]

    # Second, coexisting session uses the full path and becomes version 2.
    full_sess, _ = _build_session(client, manifest, 1, key)
    full = client.post(f"/api/v1/sessions/{full_sess['session_id']}/merge",
                       json={"strategy": "full"}).json()["data"]

    assert fast["object_sha256"] == full["object_sha256"] == obj["expected_object_sha256"]
    assert fast["total_size"] == full["total_size"] == obj["expected_total_size"]
    assert fast["part_count"] == full["part_count"] == obj["part_count"]
    assert fast["etag"] != full["etag"] or fast["version"] != full["version"]
    assert full["version"] == fast["version"] + 1


def test_full_path_detects_corrupted_part_body(client, manifest, settings):
    import sqlite3
    sess, obj = _build_session(client, manifest, 1, "k/corrupt")
    # Tamper directly with stored bytes while leaving the manifest digest intact.
    conn = sqlite3.connect(str(settings.db_path))
    conn.execute("UPDATE part_bodies SET content = ? WHERE session_id = ? AND part_number = 1",
                 (b"corrupted-bytes", sess["session_id"]))
    conn.commit()
    conn.close()

    r = client.post(f"/api/v1/sessions/{sess['session_id']}/merge",
                    json={"strategy": "full"})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "E_DIGEST_MISMATCH"
    # Fast path still succeeds because it trusts persisted manifest evidence.
    r = client.post(f"/api/v1/sessions/{sess['session_id']}/merge",
                    json={"strategy": "fast"})
    assert r.status_code == 200
