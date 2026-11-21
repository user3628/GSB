from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from typing import Any
from urllib.parse import urlparse

from .errors import E_INVALID_TRANSITION, E_TASK_INVALID, BusinessError


class TaskState(str, Enum):
    PENDING = "PENDING"
    READY = "READY"
    DISPATCHED = "DISPATCHED"
    RETRY_WAITING = "RETRY_WAITING"
    COMPLETED = "COMPLETED"
    DEAD_LETTERED = "DEAD_LETTERED"


TERMINAL_STATES = {TaskState.COMPLETED.value, TaskState.DEAD_LETTERED.value}

ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    TaskState.PENDING.value: frozenset({TaskState.READY.value}),
    TaskState.READY.value: frozenset({TaskState.DISPATCHED.value}),
    TaskState.DISPATCHED.value: frozenset(
        {
            TaskState.COMPLETED.value,
            TaskState.RETRY_WAITING.value,
            TaskState.DEAD_LETTERED.value,
            TaskState.READY.value,
        }
    ),
    TaskState.RETRY_WAITING.value: frozenset({TaskState.READY.value}),
    TaskState.COMPLETED.value: frozenset(),
    TaskState.DEAD_LETTERED.value: frozenset(),
}


def can_transition(from_state: str, to_state: str) -> bool:
    return to_state in ALLOWED_TRANSITIONS.get(from_state, frozenset())


def assert_transition(from_state: str, to_state: str, context: dict[str, Any]) -> None:
    if not can_transition(from_state, to_state):
        raise BusinessError(
            E_INVALID_TRANSITION,
            f"transition {from_state} -> {to_state} is not allowed",
            context,
            409,
        )


def retry_backoff_ms(base_ms: int, attempt_no: int) -> int:
    """Deterministic exponential backoff for the failed attempt number."""
    return base_ms * (2 ** max(0, attempt_no - 1))


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class TaskSpec:
    queue_name: str
    task_id: str
    due_at_ms: int
    priority: int
    remaining_retries: int
    resource_requirement: int
    payload: dict[str, Any]
    endpoint: str
    input_fingerprint: str


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _read_int(raw: dict[str, Any], names: tuple[str, ...]) -> tuple[Any, str | None]:
    for name in names:
        if name in raw:
            return raw[name], name
    return None, names[0]


def _local_endpoint(endpoint: str, allow_non_local: bool) -> bool:
    parsed = urlparse(endpoint)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return False
    if allow_non_local:
        return True
    host = parsed.hostname.lower()
    return host in {"127.0.0.1", "localhost", "::1"}


def validate_task_batch(
    queue_name: str,
    raw_tasks: Any,
    default_endpoint: str,
    allow_non_local_endpoints: bool = False,
) -> tuple[list[TaskSpec], list[dict[str, Any]]]:
    if not isinstance(raw_tasks, list) or not raw_tasks:
        raise BusinessError(
            E_TASK_INVALID,
            "tasks must be a non-empty array",
            {"field": "tasks"},
            422,
        )

    errors: list[dict[str, Any]] = []
    specs: list[TaskSpec] = []
    seen: dict[str, TaskSpec] = {}
    duplicates: list[dict[str, Any]] = []

    for index, raw in enumerate(raw_tasks):
        item_errors: list[dict[str, Any]] = []
        if not isinstance(raw, dict):
            errors.append({"index": index, "reason": "task must be an object"})
            continue

        task_id = raw.get("task_id", raw.get("id"))
        if not isinstance(task_id, str) or not task_id.strip():
            item_errors.append({"field": "task_id", "reason": "must be a non-empty string"})
        elif len(task_id) > 200:
            item_errors.append({"field": "task_id", "reason": "must be <= 200 characters"})

        due_at_ms, due_name = _read_int(raw, ("due_at_ms", "due_at", "due"))
        if not _is_int(due_at_ms) or due_at_ms < 0:
            item_errors.append(
                {"field": due_name or "due_at_ms", "reason": "must be an integer >= 0"}
            )

        priority = raw.get("priority")
        if not _is_int(priority) or not 0 <= priority <= 9:
            item_errors.append(
                {"field": "priority", "reason": "must be an integer in [0, 9]"}
            )

        remaining_retries, retry_name = _read_int(
            raw, ("remaining_retries", "retries", "max_retries", "retry_count")
        )
        if not _is_int(remaining_retries) or remaining_retries < 0:
            item_errors.append(
                {
                    "field": retry_name or "remaining_retries",
                    "reason": "must be an integer >= 0",
                }
            )

        resource_requirement, resource_name = _read_int(
            raw, ("resource_requirement", "resource", "resource_weight")
        )
        if not _is_int(resource_requirement) or resource_requirement < 1:
            item_errors.append(
                {
                    "field": resource_name or "resource_requirement",
                    "reason": "must be an integer >= 1",
                }
            )

        payload = raw.get("payload", {})
        if payload is None:
            payload = {}
        if not isinstance(payload, dict):
            item_errors.append({"field": "payload", "reason": "must be an object"})

        endpoint = raw.get("endpoint", default_endpoint)
        if not isinstance(endpoint, str) or not _local_endpoint(
            endpoint, allow_non_local_endpoints
        ):
            item_errors.append(
                {
                    "field": "endpoint",
                    "reason": (
                        "must be an http(s) URL to a local host "
                        "(127.0.0.1, localhost, or ::1)"
                    ),
                }
            )

        if item_errors:
            errors.append({"index": index, "task_id": task_id, "errors": item_errors})
            continue

        try:
            payload_json = canonical_json(payload)
        except (TypeError, ValueError) as exc:
            errors.append(
                {
                    "index": index,
                    "task_id": task_id,
                    "errors": [{"field": "payload", "reason": f"not JSON serializable: {exc}"}],
                }
            )
            continue

        fingerprint_obj = {
            "queue_name": queue_name,
            "task_id": task_id,
            "due_at_ms": due_at_ms,
            "priority": priority,
            "remaining_retries": remaining_retries,
            "resource_requirement": resource_requirement,
            "payload": json.loads(payload_json),
            "endpoint": endpoint,
        }
        spec = TaskSpec(
            queue_name=queue_name,
            task_id=task_id,
            due_at_ms=due_at_ms,
            priority=priority,
            remaining_retries=remaining_retries,
            resource_requirement=resource_requirement,
            payload=json.loads(payload_json),
            endpoint=endpoint,
            input_fingerprint=canonical_json(fingerprint_obj),
        )
        previous = seen.get(task_id)
        if previous is not None:
            if previous.input_fingerprint != spec.input_fingerprint:
                errors.append(
                    {
                        "index": index,
                        "task_id": task_id,
                        "errors": [
                            {
                                "field": "task_id",
                                "reason": "duplicate id in batch has different content",
                            }
                        ],
                    }
                )
            else:
                duplicates.append(
                    {"index": index, "task_id": task_id, "reason": "DUPLICATE_IN_BATCH"}
                )
            continue
        seen[task_id] = spec
        specs.append(spec)

    if errors:
        raise BusinessError(
            E_TASK_INVALID,
            "one or more tasks failed validation; the entire batch was rejected",
            {"queue": queue_name, "errors": errors, "atomic": True},
            422,
        )
    return specs, duplicates
