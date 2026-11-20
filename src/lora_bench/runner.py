"""Top-level worker entry point (picklable for ``spawn`` on Windows).

Each worker rebuilds the frozen base model from the local weight file, injects
LoRA adapters, optionally restores the latest consistent checkpoint and then
runs training steps until completion, failure or an observed stop request.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import torch

from .checkpoint import (
    CheckpointRecord,
    cleanup_uncommitted,
    load_latest_consistent,
    publish_checkpoint,
    remove_corrupt_committed,
    reset_task_store,
)
from .config import TaskConfig, task_config_fingerprint
from .data import Sample, samples_fingerprint
from .determinism import set_global_seed
from .errors import BenchError, TaskError
from .model import (
    ModelConfig,
    TinyTransformer,
    adapter_summary,
    inject_lora,
    init_model_weights,
    lora_state_dict,
)
from .state import stop_requested
from .training import train_task_stream


@dataclass
class WorkerJob:
    workspace: str
    model_path: str
    model_config: dict[str, Any]
    task: dict[str, Any]
    samples: list[dict[str, Any]]
    checkpoint_every: int
    seed: int
    expected_config_fingerprint: str
    expected_sample_fingerprint: str

    @classmethod
    def build(
        cls,
        workspace: str,
        model_path: str,
        model_config: ModelConfig,
        task: TaskConfig,
        samples: list[Sample],
        checkpoint_every: int,
        seed: int,
        config_fp: str,
        sample_fp: str,
    ) -> "WorkerJob":
        return cls(
            workspace=workspace,
            model_path=model_path,
            model_config=model_config.to_dict(),
            task=task.to_dict(),
            samples=[sample.to_dict() for sample in samples],
            checkpoint_every=checkpoint_every,
            seed=seed,
            expected_config_fingerprint=config_fp,
            expected_sample_fingerprint=sample_fp,
        )


def load_base_model(model_path: str, model_config: ModelConfig) -> TinyTransformer:
    if os.path.exists(model_path):
        state = torch.load(model_path, map_location="cpu")
        model = TinyTransformer(model_config)
        model.load_state_dict(state)
    else:  # regenerate deterministically if the file is missing
        model = TinyTransformer(model_config)
        init_model_weights(model, model_seed_from_path(model_path))
    model.eval()
    for param in model.parameters():
        param.requires_grad_(False)
    return model


def model_seed_from_path(model_path: str) -> int:
    return 20240601


def run_worker(job: WorkerJob) -> dict[str, Any]:
    """Execute one task and return a plain dict result (safe across processes)."""
    from .config import TaskConfig as _TC

    task = _TC(**job.task)
    samples = [Sample.from_dict(item) for item in job.samples]
    cfg_fp = task_config_fingerprint(task)
    sample_fp = samples_fingerprint(samples)
    if cfg_fp != job.expected_config_fingerprint:
        raise BenchError("worker config fingerprint mismatch", task_id=task.task_id)
    if sample_fp != job.expected_sample_fingerprint:
        raise BenchError("worker sample fingerprint mismatch", task_id=task.task_id)

    set_global_seed(job.seed)
    model_config = ModelConfig(**job.model_config)
    model = load_base_model(job.model_path, model_config)
    replaced = inject_lora(
        model,
        task.target_modules,
        task.rank,
        task.alpha,
        base_seed=job.seed,
    )
    cleanup_uncommitted(job.workspace, task.task_id)
    remove_corrupt_committed(job.workspace, task.task_id)

    start_step = 0
    sequence = 0
    sample_cursor = 0
    loss_history: list[float] = []
    restored = load_latest_consistent(job.workspace, task.task_id)
    if restored is not None:
        record, adapter_state = restored
        if (
            record.config_fingerprint != cfg_fp
            or record.sample_fingerprint != sample_fp
        ):
            raise BenchError(
                "stale checkpoint fingerprint cannot resume",
                task_id=task.task_id,
                step=record.step,
            )
        model.load_state_dict(adapter_state, strict=False)
        start_step = record.step
        sequence = record.sequence
        sample_cursor = record.sample_cursor
        loss_history = list(record.loss_history)
    else:
        reset_task_store(job.workspace, task.task_id)

    if start_step >= task.max_steps:
        return {
            "task_id": task.task_id,
            "status": "completed",
            "completed_steps": start_step,
            "loss_history": loss_history,
            "adapter_digest": adapter_summary(model),
            "replaced_modules": replaced,
            "error": None,
            "config_fingerprint": cfg_fp,
            "sample_fingerprint": sample_fp,
        }

    status = "completed"
    failure: TaskError | None = None
    try:
        for result in train_task_stream(
            model,
            task,
            samples,
            start_step,
            lambda: stop_requested(job.workspace),
            model_config.max_seq_len,
            initial_cursor=sample_cursor,
        ):
            loss_history.append(result.loss)
            sample_cursor += task.batch_size * task.grad_accum
            if (
                result.step % job.checkpoint_every == 0
                or result.step == task.max_steps
            ):
                sequence += 1
                record = CheckpointRecord(
                    task_id=task.task_id,
                    step=result.step,
                    sequence=sequence,
                    sample_cursor=sample_cursor,
                    sample_fingerprint=sample_fp,
                    config_fingerprint=cfg_fp,
                    adapter_digest=adapter_summary(model),
                    loss_history=list(loss_history),
                    seed=job.seed,
                )
                publish_checkpoint(
                    job.workspace,
                    record,
                    lora_state_dict(model),
                    expected_previous_sequence=sequence - 1 if sequence > 1 else None,
                )
            if stop_requested(job.workspace):
                status = "stopped"
                break
    except BenchError as exc:
        failure = TaskError(
            code=exc.code,
            message=exc.message,
            task_id=task.task_id,
            step=exc.step,
            field=exc.field,
            detail=exc.detail,
        )
        status = "failed"
    except Exception as exc:  # worker failures must not kill the pool
        failure = TaskError(
            code="E_WORKER_FAILURE",
            message=repr(exc),
            task_id=task.task_id,
            step=start_step + len(loss_history) + 1,
        )
        status = "failed"

    final_digest = adapter_summary(model)
    return {
        "task_id": task.task_id,
        "status": status,
        "completed_steps": len(loss_history),
        "loss_history": loss_history,
        "adapter_digest": final_digest,
        "replaced_modules": replaced,
        "error": failure.to_dict() if failure else None,
        "config_fingerprint": cfg_fp,
        "sample_fingerprint": sample_fp,
    }
