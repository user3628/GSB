"""Task manifest validation and immutable task records."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from .errors import E_INVALID_TASKS, PipelineError


_TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")
_FILENAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\.png$")


@dataclass(frozen=True)
class TaskSpec:
    task_id: str
    input_filename: str
    instruction: str
    allowed_ops: tuple[str, ...]
    expected_width: int
    expected_height: int
    expected_summary: str
    latency_budget_ms: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "input": self.input_filename,
            "instruction": self.instruction,
            "allowed_ops": list(self.allowed_ops),
            "expected_output": {
                "width": self.expected_width,
                "height": self.expected_height,
            },
            "expected_summary": self.expected_summary,
            "latency_budget_ms": self.latency_budget_ms,
        }


def validate_task_id(task_id: Any) -> str:
    if not isinstance(task_id, str) or not _TASK_ID_RE.fullmatch(task_id):
        raise PipelineError(
            E_INVALID_TASKS,
            "task_id must be 1-80 characters using letters, digits, dot, underscore, or dash",
            details={"task_id": task_id},
        )
    return task_id


def validate_input_filename(name: Any) -> str:
    if not isinstance(name, str):
        raise PipelineError(E_INVALID_TASKS, "Task input must be a filename string", details={"input": name})
    basename = Path(name).name
    if basename != name or not _FILENAME_RE.fullmatch(name):
        raise PipelineError(
            E_INVALID_TASKS,
            "Task input must be a plain PNG filename without directories",
            details={"input": name},
        )
    return name


def _expect_int(value: Any, label: str, task_id: str, *, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise PipelineError(
            E_INVALID_TASKS,
            f"{label} must be an integer >= {minimum}",
            task_id=task_id,
            details={"field": label, "value": value},
        )
    return value


def parse_task(raw: Mapping[str, Any], *, default_latency_ms: int = 2000) -> TaskSpec:
    if not isinstance(raw, Mapping):
        raise PipelineError(E_INVALID_TASKS, "Each task must be a JSON object")
    task_id = validate_task_id(raw.get("task_id"))
    input_name = validate_input_filename(raw.get("input"))
    instruction = raw.get("instruction")
    if not isinstance(instruction, str) or not instruction.strip():
        raise PipelineError(
            E_INVALID_TASKS,
            "Task instruction must be a non-empty string",
            task_id=task_id,
        )
    allowed_ops_raw = raw.get("allowed_ops")
    if not isinstance(allowed_ops_raw, list) or not allowed_ops_raw:
        raise PipelineError(
            E_INVALID_TASKS,
            "allowed_ops must be a non-empty list",
            task_id=task_id,
        )
    allowed_ops: list[str] = []
    for item in allowed_ops_raw:
        if not isinstance(item, str) or not item.strip():
            raise PipelineError(
                E_INVALID_TASKS,
                "allowed_ops entries must be non-empty strings",
                task_id=task_id,
            )
        allowed_ops.append(item.strip())

    expected = raw.get("expected_output")
    if isinstance(expected, Mapping):
        width = expected.get("width")
        height = expected.get("height")
    elif isinstance(expected, (list, tuple)) and len(expected) == 2:
        width, height = expected[0], expected[1]
    else:
        raise PipelineError(
            E_INVALID_TASKS,
            "expected_output must be {width,height} or [width,height]",
            task_id=task_id,
        )
    expected_width = _expect_int(width, "expected_output.width", task_id, minimum=1)
    expected_height = _expect_int(height, "expected_output.height", task_id, minimum=1)
    expected_summary = raw.get("expected_summary")
    if not isinstance(expected_summary, str) or not expected_summary.strip():
        raise PipelineError(
            E_INVALID_TASKS,
            "expected_summary must be a non-empty string",
            task_id=task_id,
        )
    latency_budget_ms = _expect_int(
        raw.get("latency_budget_ms", default_latency_ms),
        "latency_budget_ms",
        task_id,
        minimum=0,
    )
    return TaskSpec(
        task_id=task_id,
        input_filename=input_name,
        instruction=instruction,
        allowed_ops=tuple(allowed_ops),
        expected_width=expected_width,
        expected_height=expected_height,
        expected_summary=expected_summary,
        latency_budget_ms=latency_budget_ms,
    )


def parse_task_manifest(raw: Any, *, default_latency_ms: int = 2000) -> list[TaskSpec]:
    if isinstance(raw, Mapping):
        tasks_raw = raw.get("tasks")
        if tasks_raw is None and "task_id" in raw:
            tasks_raw = [raw]
    else:
        tasks_raw = raw
    if not isinstance(tasks_raw, list):
        raise PipelineError(E_INVALID_TASKS, "Task manifest must be a list or an object with a tasks list")
    tasks = [parse_task(item, default_latency_ms=default_latency_ms) for item in tasks_raw]
    if not tasks:
        raise PipelineError(E_INVALID_TASKS, "Task manifest must contain at least one task")
    seen: set[str] = set()
    for task in tasks:
        if task.task_id in seen:
            raise PipelineError(
                E_INVALID_TASKS,
                f"Duplicate task_id {task.task_id}",
                task_id=task.task_id,
            )
        seen.add(task.task_id)
    return sorted(tasks, key=lambda task: task.task_id)


def load_tasks(path: str | Path, *, default_latency_ms: int = 2000) -> list[TaskSpec]:
    task_path = Path(path)
    try:
        raw = json.loads(task_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PipelineError(
            E_INVALID_TASKS,
            f"Unable to read task manifest: {exc}",
            details={"path": str(task_path)},
        ) from exc
    return parse_task_manifest(raw, default_latency_ms=default_latency_ms)


def tasks_to_manifest(tasks: Iterable[TaskSpec]) -> dict[str, Any]:
    return {
        "schema": "gspipe-tasks-v1",
        "tasks": [task.to_dict() for task in sorted(tasks, key=lambda item: item.task_id)],
    }
