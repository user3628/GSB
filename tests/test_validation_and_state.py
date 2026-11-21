from __future__ import annotations

import pytest

from delay_queue.errors import (
    E_IDEMPOTENCY_CONFLICT,
    E_INVALID_TRANSITION,
    E_TASK_INVALID,
    BusinessError,
)

from .conftest import task


def test_invalid_batch_is_atomic_and_reports_all_errors(service):
    batch = [
        task("valid"),
        task("bad-due", due_at_ms=-1),
        task("bad-priority", priority=10),
        task("bad-retries", retries=-1),
        task("bad-resource", resource=0),
    ]
    with pytest.raises(BusinessError) as exc:
        service.submit_tasks("q", batch)
    assert exc.value.code == E_TASK_INVALID
    assert exc.value.context["atomic"] is True
    assert len(exc.value.context["errors"]) == 4
    assert service.get_queue_status("q")["total_tasks"] == 0


def test_duplicate_submission_is_idempotent_and_conflicting_content_is_rejected(service):
    first = service.submit_tasks("q", [task("same", priority=3), task("same", priority=3)])
    assert first["inserted"] == 1
    assert first["batch_duplicates"][0]["reason"] == "DUPLICATE_IN_BATCH"

    again = service.submit_tasks("q", [task("same", priority=3)])
    assert again["inserted"] == 0
    assert again["duplicates"] == ["same"]
    assert service.get_queue_status("q")["total_tasks"] == 1

    with pytest.raises(BusinessError) as exc:
        service.submit_tasks("q", [task("same", priority=4)])
    assert exc.value.code == E_IDEMPOTENCY_CONFLICT


def test_manual_state_machine_rejects_illegal_transition_without_mutation(service):
    service.submit_tasks("q", [task("t1")])
    with pytest.raises(BusinessError) as exc:
        service.storage.manual_transition(
            "q", "t1", "RETRY_WAITING", "not allowed from PENDING"
        )
    assert exc.value.code == E_INVALID_TRANSITION
    assert service.storage.get_task("q", "t1")["state"] == "PENDING"

    updated = service.storage.manual_transition("q", "t1", "READY", "manual test release")
    assert updated["state"] == "READY"
    with pytest.raises(BusinessError) as exc2:
        service.storage.manual_transition("q", "t1", "COMPLETED", "illegal shortcut")
    assert exc2.value.code == E_INVALID_TRANSITION
    assert service.storage.get_task("q", "t1")["state"] == "READY"

    transitions = service.get_task_trajectory("q", "t1")["transitions"]
    assert transitions[-1]["to_state"] == "READY"
    assert transitions[-1]["reason"] == "manual test release"
    assert transitions[-1]["occurred_at_ms"] > 0


def test_non_local_endpoint_rejected(service):
    item = task("remote")
    item["endpoint"] = "http://example.com/dispatch"
    with pytest.raises(BusinessError) as exc:
        service.submit_tasks("q", [item])
    assert exc.value.code == E_TASK_INVALID

