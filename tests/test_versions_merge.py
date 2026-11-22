from __future__ import annotations

import pytest

from upload_index.errors import BusinessError

from conftest import part


def create(service, *, key: str = "objects/merge.bin", session_id: str, policy: str = "reject"):
    return service.create_session(
        {"object_key": key, "session_id": session_id, "conflict_policy": policy}
    )


def test_merge_validation_and_single_success(service):
    session = create(service, session_id="merge-1")
    sid = session["session_id"]
    service.submit_part(sid, part(1, "alpha", size=10))
    service.submit_part(sid, part(3, "gamma", size=30))
    with pytest.raises(BusinessError) as exc:
        service.merge_session(sid, {"generation": 1, "total_size": 40})
    assert exc.value.code == "E_PART_MISSING"
    assert exc.value.details["missing"] == [2]

    service.submit_part(sid, part(2, "beta", size=20))
    with pytest.raises(BusinessError) as exc:
        service.merge_session(sid, {"generation": 1, "total_size": 59})
    assert exc.value.code == "E_SIZE_MISMATCH"

    manifest = [part(1, "alpha", size=10), part(2, "wrong", size=20), part(3, "gamma", size=30)]
    with pytest.raises(BusinessError) as exc:
        service.merge_session(
            sid, {"generation": 1, "total_size": 60, "parts": manifest}
        )
    assert exc.value.code == "E_PART_MISMATCH"

    merged = service.merge_session(
        sid,
        {
            "generation": 1,
            "total_size": 60,
            "parts": [part(1, "alpha", size=10), part(2, "beta", size=20), part(3, "gamma", size=30)],
        },
    )
    assert merged["part_count"] == 3
    assert merged["object_sha256"] == service.get_object_view("objects/merge.bin")["object"]["object_sha256"]
    assert service.get_session_view(sid)["session"]["status"] == "completed"
    with pytest.raises(BusinessError) as exc:
        service.merge_session(sid, {"generation": 1, "total_size": 60})
    assert exc.value.code == "E_OBJECT_EXISTS"


def test_same_key_generations_and_stale_merge(service):
    first = create(service, key="objects/race.bin", session_id="race-old", policy="allow_generation")
    second = create(service, key="objects/race.bin", session_id="race-new", policy="allow_generation")
    assert first["generation"] == 1
    assert second["generation"] == 2
    service.submit_part("race-old", part(1, "old", size=10))
    service.submit_part("race-new", part(1, "new", size=20))

    with pytest.raises(BusinessError) as exc:
        service.merge_session("race-old", {"generation": 1, "total_size": 10})
    assert exc.value.code == "E_SESSION_STALE"
    assert exc.value.details["current_generation"] == 2

    winner = service.merge_session("race-new", {"generation": 2, "total_size": 20})
    assert winner["generation"] == 2
    with pytest.raises(BusinessError) as exc:
        service.merge_session("race-new", {"generation": 2, "total_size": 20})
    assert exc.value.code == "E_OBJECT_EXISTS"
    service.cancel_session("race-old")
    generation = service.get_object_view("objects/race.bin")["generation_index"]
    assert generation["current_generation"] == 2
    assert {item["session_id"]: item["status"] for item in generation["sessions"]} == {
        "race-old": "cancelled",
        "race-new": "completed",
    }


def test_session_conflict_policy(service):
    create(service, key="objects/conflict.bin", session_id="conflict-1")
    with pytest.raises(BusinessError) as exc:
        create(service, key="objects/conflict.bin", session_id="conflict-2")
    assert exc.value.code == "E_SESSION_CONFLICT"
    assert exc.value.details["current_generation"] == 1
    allowed = create(
        service, key="objects/conflict.bin", session_id="conflict-3", policy="allow_generation"
    )
    assert allowed["generation"] == 2


def test_old_version_compaction_and_retirement(service):
    session = create(service, session_id="version-1")
    sid = session["session_id"]
    first = service.submit_batch(
        sid, {"batch_id": "v-first", "parts": [part(2, "beta"), part(1, "alpha")]}
    )
    old_version = first["current_version"]
    old_view = service.get_session_view(sid, old_version)
    second = service.submit_batch(
        sid, {"batch_id": "v-second", "parts": [part(3, "gamma")]}
    )
    new_version = second["current_version"]
    assert service.get_session_view(sid, old_version)["parts"] == old_view["parts"]
    assert service.get_session_view(sid, new_version)["parts"] != old_view["parts"]

    segments_before = service.get_session_segments(sid)
    compacted = service.compact_session(sid)
    assert compacted["compacted"] is True
    assert service.get_session_segments(sid)["segment_count"] < segments_before["segment_count"]
    assert service.get_session_view(sid, old_version)["parts"] == old_view["parts"]
    assert service.get_session_view(sid, new_version)["index"]["contiguous_ranges"] == [[1, 3]]

    lease = service.acquire_query_lease(old_version, ttl_ms=60_000)
    retirement = service.retire_versions(min_active_version=new_version)
    assert retirement["retention_floor"] == old_version
    assert service.get_session_view(sid, old_version)["parts"] == old_view["parts"]
    with pytest.raises(BusinessError) as exc:
        service.get_session_view(sid, session["created_version"])
    assert exc.value.code == "E_VERSION_RETIRED"

    service.release_query_lease(lease["lease_id"])
    final_retirement = service.retire_versions(min_active_version=new_version)
    assert final_retirement["retention_floor"] == new_version
    with pytest.raises(BusinessError) as exc:
        service.get_session_view(sid, old_version)
    assert exc.value.code == "E_VERSION_RETIRED"
    assert service.get_session_view(sid, new_version)["parts"][-1]["seq"] == 3


def test_versions_are_strictly_monotonic(service):
    session = create(service, session_id="monotonic")
    sid = session["session_id"]
    versions = [service.current_version]
    for seq in range(1, 6):
        result = service.submit_part(sid, part(seq, f"content-{seq}"))
        versions.append(result["current_version"])
    assert versions == sorted(versions)
    assert len(set(versions)) == len(versions)


def test_merge_can_derive_total_and_accept_minimal_manifest(service):
    create(service, key="objects/minimal.bin", session_id="minimal-merge")
    service.submit_part("minimal-merge", part(1, "alpha", size=10))
    service.submit_part("minimal-merge", part(2, "beta", size=20))
    manifest = [
        {"seq": 1, "size": 10, "sha256": part(1, "alpha")["sha256"]},
        {"seq": 2, "size": 20, "sha256": part(2, "beta")["sha256"]},
    ]
    merged = service.merge_session(
        "minimal-merge", {"generation": 1, "parts": manifest}
    )
    assert merged["total_size"] == 30
    assert merged["part_count"] == 2

