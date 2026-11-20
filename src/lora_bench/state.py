"""Persistent workbench state.

All interactive queries read from files under the workspace (never from
in-memory guesses).  A single ``state.json`` document is replaced atomically
and guarded by a workspace lock; stop requests use a dedicated flag file that
workers observe between steps.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field, asdict
from typing import Any

from .determinism import atomic_write_json, file_lock, stable_json, sha256_text

STATE_NAME = "state.json"
STOP_NAME = "stop.flag"
RUN_STATE_NAME = "run-state"

PHASE_PENDING = "pending"
PHASE_RUNNING = "running"
PHASE_STOPPED = "stopped"
PHASE_COMPLETED = "completed"
PHASE_FAILED = "failed"
PHASE_NOT_STARTED = "not_started"

VALID_PHASES = {
    PHASE_PENDING,
    PHASE_RUNNING,
    PHASE_STOPPED,
    PHASE_COMPLETED,
    PHASE_FAILED,
    PHASE_NOT_STARTED,
}


def state_path(workspace: str) -> str:
    return os.path.join(workspace, STATE_NAME)


def lock_path(workspace: str) -> str:
    return os.path.join(workspace, "locks", "workspace.lock")


def stop_path(workspace: str) -> str:
    return os.path.join(workspace, STOP_NAME)


@dataclass
class TaskState:
    task_id: str
    index: int
    status: str = PHASE_PENDING
    config_fingerprint: str = ""
    sample_fingerprint: str = ""
    estimated_samples: int = 0
    consumed_samples: int = 0
    completed_steps: int = 0
    max_steps: int = 0
    last_sequence: int | None = None
    rejection_reason: str | None = None
    rejection_detail: dict[str, Any] = field(default_factory=dict)
    invalidation: dict[str, Any] = field(default_factory=dict)
    last_error: dict[str, Any] | None = None
    loss_history: list[float] = field(default_factory=list)
    adapter_digest: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TaskState":
        return cls(**payload)


@dataclass
class WorkspaceState:
    experiment: str = ""
    run_phase: str = PHASE_PENDING
    seed: int = 0
    config_fingerprint: str = ""
    budget_fingerprint: str = ""
    samples_fingerprint: str = ""
    budget: dict[str, Any] = field(default_factory=dict)
    global_cap: int = 0
    global_consumed: int = 0
    tasks: dict[str, TaskState] = field(default_factory=dict)
    task_order: list[str] = field(default_factory=list)
    summary_fingerprint: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["tasks"] = {tid: task.to_dict() for tid, task in self.tasks.items()}
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "WorkspaceState":
        tasks = {
            tid: TaskState.from_dict(item)
            for tid, item in payload.get("tasks", {}).items()
        }
        known = {
            "experiment",
            "run_phase",
            "seed",
            "config_fingerprint",
            "budget_fingerprint",
            "samples_fingerprint",
            "budget",
            "global_cap",
            "global_consumed",
            "task_order",
            "summary_fingerprint",
        }
        kwargs = {key: payload[key] for key in known if key in payload}
        return cls(tasks=tasks, **kwargs)


def load_state(workspace: str) -> WorkspaceState | None:
    path = state_path(workspace)
    if not os.path.exists(path):
        return None
    import json

    with open(path, "r", encoding="utf-8") as handle:
        return WorkspaceState.from_dict(json.load(handle))


def save_state(workspace: str, state: WorkspaceState) -> None:
    with file_lock(lock_path(workspace)):
        atomic_write_json(state_path(workspace), state.to_dict())


def mutate_state(workspace: str, fn) -> WorkspaceState:
    """Run ``fn(state)`` under the workspace lock and persist atomically."""
    with file_lock(lock_path(workspace)):
        state = load_state(workspace)
        if state is None:
            state = WorkspaceState()
        fn(state)
        atomic_write_json(state_path(workspace), state.to_dict())
        return state


def request_stop(workspace: str) -> None:
    os.makedirs(workspace, exist_ok=True)
    atomic_write_json(stop_path(workspace), {"requested": True})


def stop_requested(workspace: str) -> bool:
    return os.path.exists(stop_path(workspace))


def clear_stop(workspace: str) -> None:
    path = stop_path(workspace)
    if os.path.exists(path):
        os.unlink(path)


def state_document_fingerprint(state: WorkspaceState) -> str:
    return sha256_text(stable_json(state.to_dict()))
