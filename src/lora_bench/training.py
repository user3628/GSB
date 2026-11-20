"""Deterministic local LoRA training step.

Reproducibility contract: given the same model weights, adapter state,
samples, configuration and seed, every step produces bit-identical losses and
adapter tensors regardless of how many worker processes run concurrently.
Only LoRA parameters receive gradients.
"""
from __future__ import annotations

from dataclasses import dataclass

import torch
from torch import nn

from .config import TaskConfig
from .data import Sample, iter_step_batches
from .model import TinyTransformer, lora_parameters


@dataclass
class StepResult:
    step: int
    loss: float


def build_optimizer(model: nn.Module, task: TaskConfig) -> torch.optim.Optimizer:
    params = lora_parameters(model)
    return torch.optim.SGD(params, lr=task.learning_rate)


def run_step(
    model: TinyTransformer,
    optimizer: torch.optim.Optimizer,
    micro_batches: list[tuple[torch.Tensor, torch.Tensor]],
) -> float:
    """Execute one optimiser step over ``grad_accum`` micro-batches."""
    optimizer.zero_grad(set_to_none=True)
    accumulated = 0.0
    for inputs, targets in micro_batches:
        logits = model(inputs)
        loss = torch.nn.functional.cross_entropy(
            logits.reshape(-1, logits.size(-1)),
            targets.reshape(-1),
        )
        (loss / len(micro_batches)).backward()
        accumulated += float(loss.detach()) / len(micro_batches)
    optimizer.step()
    return accumulated


def train_task_stream(
    model: TinyTransformer,
    task: TaskConfig,
    samples: list[Sample],
    start_step: int,
    stop_check,
    max_seq_len: int,
    initial_cursor: int = 0,
):
    """Generator yielding :class:`StepResult` from ``start_step`` to completion.

    ``stop_check`` is a zero-argument callable; when it returns True the
    current (indivisible) step is completed first, then the stream stops.
    """
    optimizer = build_optimizer(model, task)
    stream = iter_step_batches(
        samples,
        task.batch_size,
        task.grad_accum,
        task.max_steps,
        max_seq_len,
        initial_cursor=initial_cursor,
        start_step=start_step,
    )
    for step_index, micro_batches in enumerate(stream, start=start_step + 1):
        loss = run_step(model, optimizer, micro_batches)
        yield StepResult(step=step_index, loss=loss)
        if stop_check():
            return
