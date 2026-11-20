from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.errors import BusinessError

from .conftest import full_snapshot, incremental_snapshot, write


def build_three_snapshot_chain(service):
    full_snapshot(service, "s0", "c0")
    write(service, 0, b"v2-block0", "w0")
    write(service, 2, b"v2-block2", "w2")
    incremental_snapshot(service, "s1", "s0", "c1")
    write(service, 1, b"v2-block1", "w1")
    incremental_snapshot(service, "s2", "s1", "c2")


def test_adjacent_merge_rewrites_target_and_marks_source_merged(service):
    build_three_snapshot_chain(service)
    expected = service.get_snapshot_blocks("vol0", "s1")["blocks"]
    result = service.merge_snapshots("vol0", "s1", "s0", "m1")

    assert result["source_state_after"] == "MERGED"
    assert result["refs_rewritten"] == 4
    assert result["children_reparented"] == ["s2"]
    assert result["chain_depth_before"] == 3
    assert result["chain_depth_after"] == 2
    assert result["active_snapshot_count_after"] == 2
    assert result["target_ref_count_after"] == 4

    source = service.get_chain("vol0")["snapshots"]
    s1 = next(item for item in source if item["snapshot_id"] == "s1")
    s2 = next(item for item in source if item["snapshot_id"] == "s2")
    assert s1["state"] == "MERGED"
    assert s1["merged_into"] == "s0"
    assert s2["parent_id"] == "s0"

    merged_target = service.get_snapshot_blocks("vol0", "s0")["blocks"]
    assert [(b["block_no"], b["version"], b["digest"]) for b in merged_target] == [
        (b["block_no"], b["version"], b["digest"]) for b in expected
    ]


def test_merge_rejects_non_adjacent_and_repeated_merge(service):
    build_three_snapshot_chain(service)
    with pytest.raises(BusinessError) as exc:
        service.merge_snapshots("vol0", "s2", "s0", "m-bad")
    assert exc.value.code == "E_SNAPSHOT_NOT_ADJACENT"

    service.merge_snapshots("vol0", "s1", "s0", "m1")
    with pytest.raises(BusinessError) as exc:
        service.merge_snapshots("vol0", "s1", "s0", "m2")
    assert exc.value.code == "E_INVALID_TRANSITION"


def test_concurrent_merge_returns_operation_active(service, monkeypatch):
    build_three_snapshot_chain(service)
    entered = threading.Event()
    release = threading.Event()
    original_transition = service._transition

    def blocked_transition(conn, volume_id, snapshot_id, from_state, to_state, **kwargs):
        if from_state == "COMMITTED" and to_state == "MERGING":
            entered.set()
            release.wait(timeout=5)
        return original_transition(conn, volume_id, snapshot_id, from_state, to_state, **kwargs)

    monkeypatch.setattr(service, "_transition", blocked_transition)
    with ThreadPoolExecutor(max_workers=1) as pool:
        first = pool.submit(service.merge_snapshots, "vol0", "s1", "s0", "m-first")
        assert entered.wait(timeout=5)
        with pytest.raises(BusinessError) as exc:
            service.merge_snapshots("vol0", "s1", "s0", "m-second")
        assert exc.value.code == "E_OPERATION_ACTIVE"
        release.set()
        result = first.result(timeout=5)
    assert result["source_state_after"] == "MERGED"


def test_rollback_diff_and_execution_restore_complete_target_view(service):
    build_three_snapshot_chain(service)
    service.merge_snapshots("vol0", "s1", "s0", "m1")
    preview = service.get_rollback_diff("vol0", "s0")
    assert preview["write_back_blocks"] == 1
    assert preview["unchanged_blocks"] == 3
    assert preview["overwritten_current_versions"][0]["block_no"] == 1

    before = service.get_current_view("vol0")
    rolled = service.rollback_snapshot("vol0", "s0", "rollback-1")
    assert rolled["write_back_blocks"] == 1
    assert rolled["current_consistency_seq_before"] == before["consistency_seq"]
    assert rolled["current_consistency_seq_after"] == before["consistency_seq"] + 1

    target = service.get_snapshot_blocks("vol0", "s0")["blocks"]
    current = service.get_current_view("vol0")["blocks"]
    assert [(b["block_no"], b["version"], b["digest"]) for b in current] == [
        (b["block_no"], b["version"], b["digest"]) for b in target
    ]

    versions = service.get_block_versions("vol0", 1)["versions"]
    next_write = write(service, 1, b"after-rollback", "w-after")
    assert next_write["version"] == max(item["version"] for item in versions) + 1


def test_rollback_to_merged_snapshot_returns_invalid_transition(service):
    build_three_snapshot_chain(service)
    service.merge_snapshots("vol0", "s1", "s0", "m1")
    with pytest.raises(BusinessError) as exc:
        service.rollback_snapshot("vol0", "s1", "rollback-merged")
    assert exc.value.code == "E_INVALID_TRANSITION"


def test_corrupt_snapshot_rollback_keeps_current_view_unchanged(service):
    build_three_snapshot_chain(service)
    before = service.get_current_view("vol0")
    target_version = service.get_snapshot_blocks("vol0", "s0")["blocks"][2]["version"]
    conn = service.db.connect()
    try:
        conn.execute(
            "UPDATE block_versions SET content = ? WHERE volume_id = 'vol0' AND block_no = 2 AND version = ?",
            (b"tampered-content", target_version),
        )
    finally:
        conn.close()

    with pytest.raises(BusinessError) as exc:
        service.rollback_snapshot("vol0", "s0", "rollback-corrupt")
    assert exc.value.code == "E_SNAPSHOT_CORRUPT"
    assert service.get_current_view("vol0") == before


def test_readers_only_observe_before_or_after_complete_rollback(service):
    build_three_snapshot_chain(service)
    service.merge_snapshots("vol0", "s1", "s0", "m1")
    before = service.get_current_view("vol0")
    target = service.get_snapshot_blocks("vol0", "s0")["blocks"]
    target_versions = tuple(item["version"] for item in target)
    before_versions = tuple(item["version"] for item in before["blocks"])
    observed = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            view = service.get_current_view("vol0")
            values = tuple(item["version"] for item in view["blocks"])
            assert values in {before_versions, target_versions}
            observed.append(values)

    thread = threading.Thread(target=reader)
    thread.start()
    try:
        service.rollback_snapshot("vol0", "s0", "rollback-concurrent")
    finally:
        stop.set()
        thread.join(timeout=5)
    assert observed
    assert all(values in {before_versions, target_versions} for values in observed)
    final_view = service.get_current_view("vol0")
    assert tuple(item["version"] for item in final_view["blocks"]) == target_versions

