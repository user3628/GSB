"""Deterministic task-level budget scheduler."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .chain import ParsedChain
from .config import Config
from .errors import (
    E_BUDGET_GLOBAL_EXCEEDED,
    E_CHAIN_INVALID,
    E_WORKER_QUOTA_EXCEEDED,
)
from .models import TaskSpec


@dataclass
class PreparedTask:
    task: TaskSpec
    input_path: str
    input_digest: str
    input_size: tuple[int, int] | None
    parsed: ParsedChain | None
    chain_error: dict[str, Any] | None
    versions: Any


@dataclass
class Slot:
    index: int
    used_steps: int = 0
    task_ids: list[str] = field(default_factory=list)


@dataclass
class ScheduleDecision:
    task_id: str
    decision: str
    slot: int | None
    chain_length: int | None
    input_size: list[int] | None
    estimated_steps: int | None
    reason_codes: list[str]
    reason_messages: list[str]
    error: dict[str, Any] | None = None

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "decision": self.decision,
            "slot": self.slot,
            "chain_length": self.chain_length,
            "input_size": self.input_size,
            "estimated_steps": self.estimated_steps,
            "reason_codes": self.reason_codes,
            "reason_messages": self.reason_messages,
            "error": self.error,
        }


@dataclass
class SchedulePlan:
    config: Config
    decisions: list[ScheduleDecision]
    slots: list[Slot]
    accepted_steps: int
    accepted_task_ids: list[str]
    rejected_task_ids: list[str]

    def decision_for(self, task_id: str) -> ScheduleDecision:
        for decision in self.decisions:
            if decision.task_id == task_id:
                return decision
        raise KeyError(task_id)

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "config_version": self.config.config_version,
            "policy": "priority_desc_then_task_id_asc; smallest_fitting_worker_slot",
            "cost_model": {
                "formula": "max(min_task_steps, chain_length*steps_per_operator + ceil(input_pixels/pixels_per_step))",
                "steps_per_operator": self.config.steps_per_operator,
                "pixels_per_step": self.config.pixels_per_step,
                "min_task_steps": self.config.min_task_steps,
            },
            "budget": {
                "max_workers": self.config.max_workers,
                "global_step_budget": self.config.global_step_budget,
                "worker_step_quota": self.config.worker_step_quota,
            },
            "decisions": [decision.to_public_dict() for decision in self.decisions],
            "accepted": [
                decision.to_public_dict()
                for decision in self.decisions
                if decision.decision == "accepted"
            ],
            "rejected": [
                decision.to_public_dict()
                for decision in self.decisions
                if decision.decision == "rejected"
            ],
            "workers": [
                {
                    "slot": slot.index,
                    "used_steps": slot.used_steps,
                    "remaining_steps": self.config.worker_step_quota - slot.used_steps,
                    "task_ids": list(slot.task_ids),
                }
                for slot in self.slots
            ],
            "totals": {
                "accepted_tasks": len(self.accepted_task_ids),
                "rejected_tasks": len(self.rejected_task_ids),
                "accepted_estimated_steps": self.accepted_steps,
                "global_remaining_steps": self.config.global_step_budget
                - self.accepted_steps,
            },
        }


def build_schedule(prepared_tasks: list[PreparedTask], config: Config) -> SchedulePlan:
    slots = [Slot(index=index) for index in range(config.max_workers)]
    decisions: list[ScheduleDecision] = []
    accepted_steps = 0
    accepted_ids: list[str] = []
    rejected_ids: list[str] = []
    ordered = sorted(prepared_tasks, key=lambda item: (-item.task.priority, item.task.task_id))
    for item in ordered:
        task = item.task
        if item.parsed is None:
            error = item.chain_error or {
                "code": E_CHAIN_INVALID,
                "message": "operator chain is invalid",
            }
            rejection_code = error.get("code") or E_CHAIN_INVALID
            decisions.append(
                ScheduleDecision(
                    task_id=task.task_id,
                    decision="rejected",
                    slot=None,
                    chain_length=None,
                    input_size=list(item.input_size) if item.input_size else None,
                    estimated_steps=None,
                    reason_codes=[rejection_code],
                    reason_messages=[error.get("message", "operator chain is invalid")],
                    error=error,
                )
            )
            rejected_ids.append(task.task_id)
            continue
        assert item.input_size is not None
        estimated = config.estimate_steps(item.input_size, len(item.parsed.operations))
        reasons: list[str] = []
        messages: list[str] = []
        global_remaining = config.global_step_budget - accepted_steps
        if estimated > global_remaining:
            reasons.append(E_BUDGET_GLOBAL_EXCEEDED)
            messages.append(
                f"estimated {estimated} steps exceeds global remaining budget {global_remaining}"
            )
        if estimated > config.worker_step_quota:
            reasons.append(E_WORKER_QUOTA_EXCEEDED)
            messages.append(
                f"estimated {estimated} steps exceeds per-worker quota {config.worker_step_quota}"
            )
        fitting = [
            slot
            for slot in slots
            if slot.used_steps + estimated <= config.worker_step_quota
        ]
        if not fitting and E_WORKER_QUOTA_EXCEEDED not in reasons:
            reasons.append(E_WORKER_QUOTA_EXCEEDED)
            messages.append("no worker slot has enough remaining step quota")
        if reasons:
            decisions.append(
                ScheduleDecision(
                    task_id=task.task_id,
                    decision="rejected",
                    slot=None,
                    chain_length=len(item.parsed.operations),
                    input_size=list(item.input_size),
                    estimated_steps=estimated,
                    reason_codes=reasons,
                    reason_messages=messages,
                    error={
                        "code": reasons[0],
                        "message": "; ".join(messages),
                        "task_id": task.task_id,
                        "stage": "scheduled",
                        "reason": "BUDGET_REJECTED",
                        "details": {
                            "estimated_steps": estimated,
                            "global_remaining_steps": global_remaining,
                            "worker_step_quota": config.worker_step_quota,
                        },
                    },
                )
            )
            rejected_ids.append(task.task_id)
            continue
        fitting.sort(key=lambda slot: (slot.used_steps, slot.index))
        slot = fitting[0]
        slot.used_steps += estimated
        slot.task_ids.append(task.task_id)
        accepted_steps += estimated
        accepted_ids.append(task.task_id)
        decisions.append(
            ScheduleDecision(
                task_id=task.task_id,
                decision="accepted",
                slot=slot.index,
                chain_length=len(item.parsed.operations),
                input_size=list(item.input_size),
                estimated_steps=estimated,
                reason_codes=[],
                reason_messages=[
                    f"assigned to worker slot {slot.index}; cumulative slot usage {slot.used_steps}/{config.worker_step_quota}"
                ],
            )
        )
    return SchedulePlan(
        config=config,
        decisions=decisions,
        slots=slots,
        accepted_steps=accepted_steps,
        accepted_task_ids=accepted_ids,
        rejected_task_ids=rejected_ids,
    )
