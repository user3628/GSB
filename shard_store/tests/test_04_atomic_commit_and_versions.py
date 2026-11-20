"""Requirement 4: one successful merge, monotonic versions, old version hidden."""
from tests.conftest import create_session, sample_part, upload_part


def _merge_object(client, manifest, object_index, object_key, strategy="fast"):
    obj = manifest["objects"][object_index]
    sess = create_session(
        client, object_key=object_key, min_part_size=1,
        expected_total_size=obj["expected_total_size"], merge_strategy=strategy,
        conflict_policy="allow",
    )
    for part_number in range(1, obj["part_count"] + 1):
        upload_part(client, sess["session_id"], part_number,
                    sample_part(manifest, object_index, part_number))
    r = client.post(f"/api/v1/sessions/{sess['session_id']}/merge",
                    json={"strategy": strategy})
    assert r.status_code == 200, r.text
    return r.json()["data"], sess["session_id"]


def test_repeat_merge_rejected_with_existing_result(client, manifest):
    data, session_id = _merge_object(client, manifest, 1, "k/repeat")
    r = client.post(f"/api/v1/sessions/{session_id}/merge", json={})
    assert r.status_code == 409
    body = r.json()
    assert body["error"]["code"] == "E_SESSION_COMPLETED"
    assert body["error"]["details"]["existing_result"]["object_sha256"] == data["object_sha256"]


def test_overwrite_increments_version_and_old_version_not_current(client, manifest):
    key = "k/versioned"
    first, _ = _merge_object(client, manifest, 1, key)
    assert first["version"] == 1
    second, _ = _merge_object(client, manifest, 0, key)
    assert second["version"] == 2
    assert first["object_sha256"] != second["object_sha256"]

    current = client.get(f"/api/v1/objects/{key}").json()["data"]
    assert current["version"] == 2
    assert current["object_sha256"] == second["object_sha256"]

    versions = client.get(f"/api/v1/objects/versions/{key}").json()["data"]
    assert versions["current_version"] == 2
    flags = {v["version"]: v["readable_as_current"] for v in versions["versions"]}
    assert flags == {1: False, 2: True}

    old = client.get(f"/api/v1/objects/versions/{key}?version=1").json()["data"]
    assert old["readable_as_current"] is False
    assert old["object_sha256"] == first["object_sha256"]
