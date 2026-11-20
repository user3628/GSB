from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.errors import BusinessError

from .conftest import full_snapshot, incremental_snapshot, write


def test_full_snapshot_freezes_refs_and_cow_preserves_old_version(service):
    write(service, 0, b"v2", "w1")
    created = full_snapshot(service, "snap-full", "c1")

    assert created["snapshot"]["state"] == "COMMITTED"
    assert created["snapshot"]["ref_count"] == 4
    assert created["chain_depth"] == 1
    assert created["snapshot"]["resolved_block_count"] == 4

    old = service.get_snapshot_blocks("vol0", "snap-full")["blocks"][0]
    changed = write(service, 0, b"v3", "w2")
    assert changed["cow_performed"] is True
    assert changed["previous_version_ref_count"] == 1

    frozen = service.get_snapshot_blocks("vol0", "snap-full")["blocks"][0]
    current = service.get_current_view("vol0")["blocks"][0]
    assert frozen["version"] == old["version"] == 2
    assert current["version"] == 3
    assert frozen["version_snapshot_ref_count"] == 1


def test_incremental_snapshot_stores_only_changed_blocks_and_resolves_parent_chain(service):
    full_snapshot(service, "snap-0", "c0")
    write(service, 0, b"a", "w0")
    write(service, 2, b"b", "w2")
    inc1 = incremental_snapshot(service, "snap-1", "snap-0", "c1")
    assert inc1["snapshot"]["ref_count"] == 2
    assert inc1["snapshot"]["resolved_block_count"] == 4

    write(service, 1, b"c", "w1")
    inc2 = incremental_snapshot(service, "snap-2", "snap-1", "c2")
    assert inc2["snapshot"]["ref_count"] == 1
    resolved = service.get_snapshot_blocks("vol0", "snap-2")
    assert resolved["resolved_block_count"] == 4
    assert [item["version"] for item in resolved["blocks"]] == [2, 2, 2, 1]

    later_write = write(service, 0, b"not-in-snapshot", "w3")
    frozen_again = service.get_snapshot_blocks("vol0", "snap-2")["blocks"][0]
    assert frozen_again["version"] == 2
    assert later_write["version"] == 3


def test_parent_delete_with_active_child_is_rejected_without_ref_changes(service):
    full_snapshot(service, "parent", "c0")
    write(service, 0, b"x", "w0")
    incremental_snapshot(service, "child", "parent", "c1")
    before = service.get_chain("vol0")

    with pytest.raises(BusinessError) as exc:
        service.delete_snapshot("vol0", "parent", "delete-parent")
    assert exc.value.code == "E_SNAPSHOT_REFERENCED"
    assert service.get_chain("vol0") == before


def test_write_during_snapshot_creation_does_not_enter_frozen_refs(service, monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    original_transition = service._transition

    def blocked_transition(conn, volume_id, snapshot_id, from_state, to_state, **kwargs):
        if from_state == "CREATING" and to_state == "COMMITTED":
            entered.set()
            release.wait(timeout=5)
        return original_transition(conn, volume_id, snapshot_id, from_state, to_state, **kwargs)

    monkeypatch.setattr(service, "_transition", blocked_transition)
    with ThreadPoolExecutor(max_workers=2) as pool:
        snapshot_future = pool.submit(full_snapshot, service, "frozen", "c0")
        assert entered.wait(timeout=5)
        write_future = pool.submit(write, service, 0, b"during-creation", "w-during")
        release.set()
        snapshot_future.result(timeout=5)
        write_future.result(timeout=5)

    frozen = service.get_snapshot_blocks("vol0", "frozen")["blocks"][0]
    current = service.get_current_view("vol0")["blocks"][0]
    assert frozen["version"] == 1
    assert current["version"] == 2


def test_leaf_delete_retains_history_and_illegal_transition_is_rejected(service):
    full_snapshot(service, "parent", "c0")
    write(service, 0, b"x", "w0")
    incremental_snapshot(service, "child", "parent", "c1")
    deleted = service.delete_snapshot("vol0", "child", "d1")
    assert deleted["state"] == "DELETED"
    assert deleted["refs_retained_for_history"] == 1

    chain = service.get_chain("vol0")
    child = next(item for item in chain["snapshots"] if item["snapshot_id"] == "child")
    assert child["state"] == "DELETED"
    assert child["ref_count"] == 1
    assert chain["active_snapshot_count"] == 1

    with pytest.raises(BusinessError) as exc:
        service.delete_snapshot("vol0", "child", "d2")
    assert exc.value.code == "E_INVALID_TRANSITION"

    parent_deleted = service.delete_snapshot("vol0", "parent", "d3")
    assert parent_deleted["state"] == "DELETED"
    assert service.get_chain("vol0")["active_snapshot_count"] == 0

