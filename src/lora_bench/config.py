"""Experiment configuration parsing, validation and version fingerprinting."""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any

import yaml

from .errors import InvalidTaskError


@dataclass(frozen=True)
class TaskConfig:
    task_id: str
    rank: int
    alpha: float
    target_modules: tuple[str, ...]
    learning_rate: float
    batch_size: int
    grad_accum: int
    max_steps: int
    sample_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "rank": self.rank,
            "alpha": self.alpha,
            "target_modules": list(self.target_modules),
            "learning_rate": self.learning_rate,
            "batch_size": self.batch_size,
            "grad_accum": self.grad_accum,
            "max_steps": self.max_steps,
            "sample_ids": list(self.sample_ids),
        }


@dataclass(frozen=True)
class Budget:
    max_workers: int
    per_task_sample_quota: int
    global_sample_cap: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExperimentConfig:
    experiment: str
    model_path: str
    samples_path: str
    seed: int
    checkpoint_every: int
    budget: Budget
    tasks: tuple[TaskConfig, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "experiment": self.experiment,
            "model_path": self.model_path,
            "samples_path": self.samples_path,
            "seed": self.seed,
            "checkpoint_every": self.checkpoint_every,
            "budget": self.budget.to_dict(),
            "tasks": [task.to_dict() for task in self.tasks],
        }


def validate_task(raw: dict[str, Any], index: int) -> TaskConfig:
    task_id = str(raw.get("task_id") or f"task-{index}")
    rank = raw.get("rank")
    alpha = raw.get("alpha")
    lr = raw.get("learning_rate")
    batch_size = raw.get("batch_size")
    grad_accum = raw.get("grad_accum", 1)
    max_steps = raw.get("max_steps")
    targets = raw.get("target_modules", [])

    if not isinstance(rank, int) or isinstance(rank, bool) or rank <= 0:
        raise InvalidTaskError(
            f"task {task_id}: rank must be a positive integer, got {rank!r}",
            task_id=task_id,
            field="rank",
        )
    if not isinstance(alpha, (int, float)) or alpha <= 0:
        raise InvalidTaskError(
            f"task {task_id}: alpha must be positive, got {alpha!r}",
            task_id=task_id,
            field="alpha",
        )
    if not isinstance(lr, (int, float)) or lr < 0:
        raise InvalidTaskError(
            f"task {task_id}: learning_rate must be >= 0, got {lr!r}",
            task_id=task_id,
            field="learning_rate",
        )
    if not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size <= 0:
        raise InvalidTaskError(
            f"task {task_id}: batch_size must be positive",
            task_id=task_id,
            field="batch_size",
        )
    if not isinstance(grad_accum, int) or grad_accum <= 0:
        raise InvalidTaskError(
            f"task {task_id}: grad_accum must be positive",
            task_id=task_id,
            field="grad_accum",
        )
    if not isinstance(max_steps, int) or max_steps <= 0:
        raise InvalidTaskError(
            f"task {task_id}: max_steps must be positive",
            task_id=task_id,
            field="max_steps",
        )
    if not targets or not all(isinstance(name, str) for name in targets):
        raise InvalidTaskError(
            f"task {task_id}: target_modules must be a non-empty list of strings",
            task_id=task_id,
            field="target_modules",
        )
    sample_ids = tuple(str(s) for s in raw.get("sample_ids", []))
    return TaskConfig(
        task_id=task_id,
        rank=rank,
        alpha=float(alpha),
        target_modules=tuple(sorted(targets)),
        learning_rate=float(lr),
        batch_size=batch_size,
        grad_accum=grad_accum,
        max_steps=max_steps,
        sample_ids=sample_ids,
    )


def parse_config(path: str) -> ExperimentConfig:
    with open(path, "r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    budget_raw = raw["budget"]
    budget = Budget(
        max_workers=int(budget_raw["max_workers"]),
        per_task_sample_quota=int(budget_raw["per_task_sample_quota"]),
        global_sample_cap=int(budget_raw["global_sample_cap"]),
    )
    if budget.max_workers <= 0 or budget.per_task_sample_quota <= 0 or budget.global_sample_cap <= 0:
        raise InvalidTaskError("budget limits must all be positive", field="budget")
    tasks = tuple(validate_task(item, idx) for idx, item in enumerate(raw["tasks"]))
    ids = [task.task_id for task in tasks]
    if len(set(ids)) != len(ids):
        raise InvalidTaskError("task_id values must be unique", field="task_id")
    return ExperimentConfig(
        experiment=str(raw["experiment"]),
        model_path=raw["model_path"],
        samples_path=raw["samples_path"],
        seed=int(raw.get("seed", 20240601)),
        checkpoint_every=int(raw.get("checkpoint_every", 4)),
        budget=budget,
        tasks=tasks,
    )


def task_config_fingerprint(task: TaskConfig) -> str:
    from .determinism import sha256_text, stable_json

    return sha256_text(stable_json(task.to_dict()))


def budget_fingerprint(budget: Budget) -> str:
    from .determinism import sha256_text, stable_json

    return sha256_text(stable_json(budget.to_dict()))
