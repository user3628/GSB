from __future__ import annotations

import pytest

from upload_index.errors import BusinessError

from conftest import part, sha256


def make_session(service, key: str = "objects/a.bin", session_id: str | None = None):
    payload = {"object_key": key, "conflict_policy": "reject"}
    if session_id is not None:
        payload["session_id"] = session_id
    return service.create_session(payload)


def test_invalid_batch_is_atomic(service):
    session = make_session(service)
    before = service.get_current_version()["current_version"]
    invalid = part(1, "alpha")
    invalid["sha256"] = "not-a-digest"
    with pytest.raises(BusinessError) as exc:
        service.submit_batch(
            session["session_id"], {"batch_id": "bad", "parts": [part(2, "beta"), invalid]}
        )
    assert exc.value.code == "E_PART_INVALID"
    assert service.get_current_version()["current_version"] == before
    view = service.get_session_view(session["session_id"])
    assert view["parts"] == []


def test_size_overflow_is_rejected(service):
    session = make_session(service)
    payload = part(1, "alpha")
    payload["size"] = 9_223_372_036_854_775_808
    with pytest.raises(BusinessError) as exc:
        service.submit_part(session["session_id"], payload)
    assert exc.value.code == "E_PART_INVALID"
    assert service.get_session_view(session["session_id"])["parts"] == []


def test_incremental_intervals_duplicate_and_mismatch(service):
    session = make_session(service)
    sid = session["session_id"]
    first = service.submit_batch(
        sid,
        {
            "batch_id": "first",
            "parts": [part(3, "gamma"), part(1, "alpha"), part(3, "gamma"), part(5, "epsilon")],
        },
    )
    assert first["committed"] == 3
    assert first["duplicates"] == 1
    view = service.get_session_view(sid, first["current_version"])
    assert view["index"]["contiguous_ranges"] == [[1, 1], [3, 3], [5, 5]]
    assert view["index"]["missing_ranges"] == [[2, 2], [4, 4]]
    assert [p for p in view["parts"] if p["seq"] == 3][0]["duplicate_count"] == 1
    assert view["session"]["duplicate_commits"] == 1

    version_before_mismatch = service.current_version
    bad = part(3, "different")
    with pytest.raises(BusinessError) as exc:
        service.submit_part(sid, bad)
    assert exc.value.code == "E_PART_MISMATCH"
    assert service.current_version == version_before_mismatch
    preserved = service.get_session_view(sid)
    assert [p["sha256"] for p in preserved["parts"] if p["seq"] == 3] == [part(3, "gamma")["sha256"]]

    second = service.submit_batch(
        sid, {"batch_id": "second", "parts": [part(4, "delta"), part(2, "beta")]}
    )
    final = service.get_session_view(sid, second["current_version"])
    assert final["index"]["contiguous_ranges"] == [[1, 5]]
    assert final["index"]["missing_ranges"] == []


def test_dedup_refcount_pending_and_physical_gc(service):
    left = make_session(service, "objects/left.bin", "sess-left")
    right = make_session(service, "objects/right.bin", "sess-right")
    shared = part(1, "shared-content", size=90)
    service.submit_part(left["session_id"], shared)
    service.submit_part(right["session_id"], shared)
    content = service.get_content_view(shared["sha256"])
    assert content["content"]["ref_count"] == 2
    assert content["content"]["state"] == "active"

    service.cancel_session(left["session_id"])
    content = service.get_content_view(shared["sha256"])
    assert content["content"]["ref_count"] == 1
    assert content["content"]["state"] == "active"

    service.cancel_session(right["session_id"])
    pending = service.get_content_view(shared["sha256"])
    pending_version = pending["version"]
    assert pending["content"]["ref_count"] == 0
    assert pending["content"]["state"] == "pending_gc"
    assert pending["physical_record_exists_now"] is True

    purge = service.purge_pending_content()
    assert shared["sha256"] in purge["purged"]
    removed = service.get_content_view(shared["sha256"])
    assert removed["content"]["state"] == "removed"
    assert removed["physical_record_exists_now"] is False

    historical = service.get_content_view(shared["sha256"], pending_version)
    assert historical["content"]["state"] == "pending_gc"


def test_dedup_revives_pending_content(service):
    first = make_session(service, "objects/a.bin", "sess-a")
    shared = part(1, "same", size=44)
    service.submit_part(first["session_id"], shared)
    service.cancel_session(first["session_id"])
    assert service.get_content_view(shared["sha256"])["content"]["state"] == "pending_gc"

    second = make_session(service, "objects/b.bin", "sess-b")
    service.submit_part(second["session_id"], shared)
    revived = service.get_content_view(shared["sha256"])
    assert revived["content"]["state"] == "active"
    assert revived["content"]["ref_count"] == 1


def test_single_part_delete_updates_interval_and_refcount(service):
    session = make_session(service, "objects/delete.bin", "sess-delete")
    shared = part(1, "delete-me", size=33)
    service.submit_part(session["session_id"], shared)
    service.submit_part(session["session_id"], {**shared, "seq": 2, "arrival_ms": 1002})
    assert service.get_content_view(shared["sha256"])["content"]["ref_count"] == 2

    first_delete = service.delete_part(session["session_id"], 1)
    assert first_delete["content_ref_count"] == 1
    mid = service.get_session_view(session["session_id"])
    assert mid["index"]["contiguous_ranges"] == [[2, 2]]
    assert [item["seq"] for item in mid["parts"]] == [2]

    second_delete = service.delete_part(session["session_id"], 2)
    assert second_delete["pending_gc"] == [shared["sha256"]]
    final = service.get_session_view(session["session_id"])
    assert final["parts"] == []
    assert final["index"]["contiguous_ranges"] == []
    assert service.get_content_view(shared["sha256"])["content"]["state"] == "pending_gc"

    replacement = {"seq": 1, "size": 22, "sha256": sha256("replacement"), "arrival_ms": 1003}
    service.submit_part(session["session_id"], replacement)
    replayed = service.get_session_view(session["session_id"])
    assert replayed["index"]["contiguous_ranges"] == [[1, 1]]

