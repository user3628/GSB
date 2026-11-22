"""Task manifest models and parsing."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from .errors import E_TASK_INVALID, WorkbenchError
from .util import validate_safe_name


def _input_filename(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise WorkbenchError(
            E_TASK_INVALID,
            "task input shard filename must be a non-empty string",
            reason="TASK_INPUT",
        )
    normalized = value.replace("\\", "/")
    path = PurePosixPath(normalized)
    if path.is_absolute() or ".." in path.parts:
        raise WorkbenchError(
            E_TASK_INVALID,
            "task input shard must be a relative path without '..'",
            reason="TASK_INPUT_UNSAFE",
            details={"input": value},
        )
    return normalized


def _expected_size(task: dict[str, Any]) -> tuple[int, int] | None:
    value = task.get("expected_output")
    if value is None:
        value = task.get("expected_output_size")
    if value is None:
        value = task.get("expected_size")
    if value is None:
        return None
    if isinstance(value, dict):
        width = value.get("width")
        height = value.get("height")
    elif isinstance(value, (list, tuple)) and len(value) == 2:
        width, height = value
    else:
        raise WorkbenchError(
            E_TASK_INVALID,
            "expected_output must be {width,height} or [width,height]",
            reason="TASK_EXPECTED_SIZE",
        )
    if (
        isinstance(width, bool)
        or isinstance(height, bool)
        or not isinstance(width, int)
        or not isinstance(height, int)
        or width <= 0
        or height <= 0
    ):
        raise WorkbenchError(
            E_TASK_INVALID,
            "expected output width and height must be positive integers",
            reason="TASK_EXPECTED_SIZE",
            details={"width": width, "height": height},
        )
    return int(width), int(height)


def _expected_digest(task: dict[str, Any]) -> str | None:
    value = task.get("expected_output_sha256")
    if value is None:
        value = task.get("expected_sha256")
    if value is None:
        value = task.get("expected_output_digest")
    if value is None and isinstance(task.get("expected_output"), dict):
        value = task["expected_output"].get("sha256")
    if value is None:
        return None
    if not isinstance(value, str) or len(value) != 64:
        raise WorkbenchError(
            E_TASK_INVALID,
            "expected output sha256 must be a 64-character hex string",
            reason="TASK_EXPECTED_DIGEST",
        )
    try:
        int(value, 16)
    except ValueError as exc:
        raise WorkbenchError(
            E_TASK_INVALID,
            "expected output sha256 must be hexadecimal",
            reason="TASK_EXPECTED_DIGEST",
        ) from exc
    return value.lower()


def _raw_operations(task: dict[str, Any]) -> list[Any]:
    for key in ("operations", "operator_chain", "op_chain", "operators"):
        value = task.get(key)
        if value is not None:
            if isinstance(value, str):
                raise WorkbenchError(
                    E_TASK_INVALID,
                    f"task field {key!r} must be a list",
                    reason="TASK_OPERATIONS",
                )
            if not isinstance(value, list):
                raise WorkbenchError(
                    E_TASK_INVALID,
                    f"task field {key!r} must be a list",
                    reason="TASK_OPERATIONS",
                )
            return value
    return []


@dataclass(frozen=True)
class TaskSpec:
    task_id: str
    input_file: str
    instruction: str
    allowed_operators: tuple[str, ...]
    operations: tuple[Any, ...]
    expected_size: tuple[int, int] | None
    expected_digest: str | None
    priority: int
    raw: dict[str, Any]

    @classmethod
    def from_mapping(cls, value: Any) -> "TaskSpec":
        if not isinstance(value, dict):
            raise WorkbenchError(
                E_TASK_INVALID,
                "each JSONL task record must be an object",
                reason="TASK_ROOT",
            )
        task_id = value.get("task_id")
        if not isinstance(task_id, str):
            raise WorkbenchError(
                E_TASK_INVALID,
                "task_id must be a string",
                reason="TASK_ID",
            )
        try:
            validate_safe_name(task_id, "task_id")
        except ValueError as exc:
            raise WorkbenchError(
                E_TASK_INVALID,
                str(exc),
                task_id=task_id,
                reason="TASK_ID_UNSAFE",
            ) from exc
        input_value = value.get(
            "input", value.get("input_shard", value.get("input_file", value.get("shard")))
        )
        input_file = _input_filename(input_value)
        instruction = value.get("instruction", "")
        if not isinstance(instruction, str):
            raise WorkbenchError(
                E_TASK_INVALID,
                "instruction must be a string",
                task_id=task_id,
                reason="TASK_INSTRUCTION",
            )
        allowed = value.get(
            "allowed_operators",
            value.get("allowed_operations", value.get("allowed_ops")),
        )
        if allowed is None:
            allowed = []
        if not isinstance(allowed, list) or not all(
            isinstance(item, str) for item in allowed
        ):
            raise WorkbenchError(
                E_TASK_INVALID,
                "allowed_operators must be a list of strings",
                task_id=task_id,
                reason="TASK_ALLOWED_OPERATORS",
            )
        priority = value.get("priority", 0)
        if isinstance(priority, bool) or not isinstance(priority, int):
            raise WorkbenchError(
                E_TASK_INVALID,
                "priority must be an integer",
                task_id=task_id,
                reason="TASK_PRIORITY",
            )
        return cls(
            task_id=task_id,
            input_file=input_file,
            instruction=instruction,
            allowed_operators=tuple(allowed),
            operations=tuple(_raw_operations(value)),
            expected_size=_expected_size(value),
            expected_digest=_expected_digest(value),
            priority=priority,
            raw=value,
        )

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "input": self.input_file,
            "instruction": self.instruction,
            "allowed_operators": list(self.allowed_operators),
            "operations": list(self.operations),
            "expected_output_size": list(self.expected_size)
            if self.expected_size
            else None,
            "expected_output_sha256": self.expected_digest,
            "priority": self.priority,
        }


def load_tasks(path: Path) -> list[TaskSpec]:
    path = Path(path)
    if not path.is_file():
        raise WorkbenchError(
            E_TASK_INVALID,
            f"task manifest not found: {path}",
            reason="TASK_FILE_MISSING",
        )
    tasks: list[TaskSpec] = []
    seen: set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise WorkbenchError(
                    E_TASK_INVALID,
                    f"invalid JSON on manifest line {line_number}: {exc}",
                    reason="TASK_JSON",
                    details={"line": line_number},
                ) from exc
            task = TaskSpec.from_mapping(value)
            if task.task_id in seen:
                raise WorkbenchError(
                    E_TASK_INVALID,
                    f"duplicate task_id {task.task_id!r}",
                    task_id=task.task_id,
                    reason="TASK_ID_DUPLICATE",
                )
            seen.add(task.task_id)
            tasks.append(task)
    tasks.sort(key=lambda task: task.task_id)
    return tasks
