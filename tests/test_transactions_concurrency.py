from __future__ import annotations

import json
import multiprocessing as mp
from pathlib import Path

import pytest

from cabin_voice.errors import CabinError
from cabin_voice.service import CabinService


def _child_priority_edit(workspace_root: str, expected_version: int, queue) -> None:
    try:
        service = CabinService(workspace_root)
        result = service.edit(
            "set_priority",
            parameters={"zone_id": "passenger", "priority": 3},
            expected_version=expected_version,
            shard_id="shard-001",
            edit_id="child-edit",
        )
        queue.put({"ok": True, "result": result})
    except CabinError as exc:
        queue.put({"ok": False, "error": exc.to_dict()})
    except BaseException as exc:
        queue.put({"ok": False, "error": {"type": type(exc).__name__, "message": str(exc)}})


def _run_child(workspace: Path, expected_version: int) -> dict:
    context = mp.get_context("spawn")
    queue = context.Queue()
    process = context.Process(
        target=_child_priority_edit,
        args=(str(workspace), expected_version, queue),
    )
    process.start()
    payload = queue.get(timeout=30)
    process.join(timeout=30)
    assert process.exitcode == 0
    return payload


def test_concurrent_publish_stale_discard_then_retry(ready_service) -> None:
    service = ready_service
    pending = service.prepare_edit(
        "set_priority",
        parameters={"zone_id": "rear_left", "priority": 1},
        expected_version=0,
        shard_id="shard-001",
        edit_id="pending-1",
    )
    assert pending["status"] == "prepared"
    child = _run_child(service.workspace.root, 0)
    assert child["ok"] is True
    assert child["result"]["edit_version"] == 1
    with pytest.raises(CabinError) as caught:
        service.publish_edit("pending-1")
    assert caught.value.code == "E_STALE_VERSION"
    assert caught.value.details["edit_id"] == "pending-1"
    assert caught.value.details["current_version"] == 1
    pending_list = service.pending_edits()["pending"]
    assert next(item for item in pending_list if item["edit_id"] == "pending-1")["status"] == "stale"
    discarded = service.discard_edit("pending-1")
    assert discarded["status"] == "discarded"

    pending2 = service.prepare_edit(
        "set_priority",
        parameters={"zone_id": "rear_right", "priority": 2},
        expected_version=1,
        shard_id="shard-001",
        edit_id="pending-2",
    )
    assert pending2["candidate_version"] == 2
    child2 = _run_child(service.workspace.root, 1)
    assert child2["ok"] is True
    assert child2["result"]["edit_version"] == 2
    with pytest.raises(CabinError) as caught:
        service.publish_edit("pending-2")
    assert caught.value.code == "E_STALE_VERSION"
    retried = service.retry_edit("pending-2")
    assert retried["status"] == "published"
    assert retried["edit_version"] == 3
    state = service.workspace.load_state()
    assert state["edit"]["version"] == 3
    assert state["edit"]["priorities"]["shard-001"]["rear_right"] == 2
    assert [entry["edit_version"] for entry in state["edit"]["audit"]] == [1, 2, 3]


def test_duplicate_publish_and_stale_file_state(ready_service) -> None:
    service = ready_service
    pending = service.prepare_edit(
        "set_priority",
        parameters={"zone_id": "driver", "priority": 4},
        expected_version=0,
        shard_id="shard-001",
        edit_id="pending-dup",
    )
    assert pending["candidate_version"] == 1
    published = service.publish_edit("pending-dup")
    assert published["edit_version"] == 1
    with pytest.raises(CabinError) as caught:
        service.publish_edit("pending-dup")
    assert caught.value.code == "E_STALE_VERSION"
    assert caught.value.details["reason"] == "duplicate_publish"
    with pytest.raises(CabinError) as caught:
        service.discard_edit("pending-dup")
    assert caught.value.code == "E_STALE_VERSION"
    path = service.workspace.pending_dir / "pending-dup.json"
    assert json.loads(path.read_text(encoding="utf-8"))["status"] == "published"
