"""Checkpoint persistence: atomic writes, monotonic sequence, corruption recovery."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from typing import Any

import torch

from .determinism import atomic_write, atomic_write_json, file_lock, tensor_digest
from .errors import (
    CorruptCheckpointError,
    StaleCheckpointError,
    WeightMismatchError,
)

META_NAME = "checkpoint.json"
WEIGHTS_NAME = "adapter.pt"
COMMIT_SUFFIX = ".committed"


@dataclass
class CheckpointRecord:
    task_id: str
    step: int
    sequence: int
    sample_cursor: int
    sample_fingerprint: str
    config_fingerprint: str
    adapter_digest: dict[str, str]
    loss_history: list[float]
    seed: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def task_dir(workspace: str, task_id: str) -> str:
    return os.path.join(workspace, "checkpoints", task_id)


def checkpoint_path(workspace: str, task_id: str, sequence: int) -> str:
    return os.path.join(task_dir(workspace, task_id), f"ckpt-{sequence:08d}")


def _read_json(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def publish_checkpoint(
    workspace: str,
    record: CheckpointRecord,
    adapter_state: dict[str, torch.Tensor],
    *,
    expected_previous_sequence: int | None = None,
) -> str:
    """Persist a checkpoint under a workspace-wide lock.

    Sequence numbers must advance by exactly one within a task.  Late,
    duplicate or cross-task misaligned publications raise
    :class:`StaleCheckpointError`.  Files land in a fresh directory and a
    commit marker is the last fsynced write, so partial directories are
    recognisable and ignored on recovery.
    """
    directory = task_dir(workspace, record.task_id)
    lock_path = os.path.join(workspace, "locks", f"{record.task_id}.lock")
    with file_lock(lock_path):
        latest = latest_sequence(workspace, record.task_id)
        if expected_previous_sequence is not None and latest != expected_previous_sequence:
            raise StaleCheckpointError(
                f"expected previous sequence {expected_previous_sequence}, store has {latest}",
                task_id=record.task_id,
                step=record.step,
                detail={"found_sequence": latest, "expected_sequence": expected_previous_sequence},
            )
        if latest is not None and record.sequence <= latest:
            raise StaleCheckpointError(
                f"checkpoint sequence {record.sequence} is not greater than {latest}",
                task_id=record.task_id,
                step=record.step,
                detail={"found_sequence": latest, "rejected_sequence": record.sequence},
            )
        target = checkpoint_path(workspace, record.task_id, record.sequence)
        if os.path.exists(target):
            raise StaleCheckpointError(
                f"duplicate checkpoint sequence {record.sequence}",
                task_id=record.task_id,
                step=record.step,
                detail={"sequence": record.sequence},
            )
        os.makedirs(target, exist_ok=False)
        weights_bytes = torch.save  # localise symbol for readability
        weights_io = os.path.join(target, WEIGHTS_NAME)
        buffer = os.path.join(target, ".adapter.tmp")
        torch.save(adapter_state, buffer)
        atomic_write(weights_io, _read_file(buffer))
        os.unlink(buffer)
        atomic_write_json(os.path.join(target, META_NAME), record.to_dict())
        atomic_write(os.path.join(target, COMMIT_SUFFIX), b"ok\n")
        return target


def _read_file(path: str) -> bytes:
    with open(path, "rb") as handle:
        return handle.read()


def is_committed(directory: str) -> bool:
    return all(
        os.path.exists(os.path.join(directory, name))
        for name in (META_NAME, WEIGHTS_NAME, COMMIT_SUFFIX)
    )


def list_committed(workspace: str, task_id: str) -> list[tuple[int, str]]:
    directory = task_dir(workspace, task_id)
    if not os.path.isdir(directory):
        return []
    committed = []
    for name in sorted(os.listdir(directory)):
        path = os.path.join(directory, name)
        if name.startswith("ckpt-") and os.path.isdir(path) and is_committed(path):
            try:
                sequence = int(name.split("-", 1)[1])
            except ValueError:
                continue
            committed.append((sequence, path))
    return committed


def latest_sequence(workspace: str, task_id: str) -> int | None:
    committed = list_committed(workspace, task_id)
    return committed[-1][0] if committed else None


def load_latest(workspace: str, task_id: str) -> tuple[CheckpointRecord, dict[str, torch.Tensor]] | None:
    committed = list_committed(workspace, task_id)
    if not committed:
        return None
    _, path = committed[-1]
    return load_checkpoint_dir(path)


def load_latest_consistent(
    workspace: str, task_id: str
) -> tuple[CheckpointRecord, dict[str, torch.Tensor]] | None:
    """Walk committed checkpoints newest -> oldest, skipping corrupt ones.

    If the newest checkpoint is corrupt the caller rebuilds from the last
    consistent predecessor, which preserves end-to-end determinism.
    """
    for _, path in reversed(list_committed(workspace, task_id)):
        try:
            return load_checkpoint_dir(path)
        except CorruptCheckpointError:
            continue
    return None


def remove_corrupt_committed(workspace: str, task_id: str) -> list[str]:
    """Delete committed-looking directories whose metadata/weights fail checks."""
    removed: list[str] = []
    for _, path in reversed(list_committed(workspace, task_id)):
        try:
            load_checkpoint_dir(path)
        except CorruptCheckpointError:
            _safe_rmtree(path)
            removed.append(path)
    return removed


def reset_task_store(workspace: str, task_id: str) -> None:
    """Remove every checkpoint directory for a task that must rebuild."""
    directory = task_dir(workspace, task_id)
    if not os.path.isdir(directory):
        return
    for name in sorted(os.listdir(directory)):
        path = os.path.join(directory, name)
        if name.startswith("ckpt-") and os.path.isdir(path):
            _safe_rmtree(path)


def load_checkpoint_dir(path: str) -> tuple[CheckpointRecord, dict[str, torch.Tensor]]:
    try:
        meta = _read_json(os.path.join(path, META_NAME))
        state = torch.load(os.path.join(path, WEIGHTS_NAME), map_location="cpu")
    except (OSError, json.JSONDecodeError, RuntimeError) as exc:
        raise CorruptCheckpointError(
            f"unreadable checkpoint at {path}: {exc}",
            task_id=meta.get("task_id") if "meta" in locals() else None,
        )
    record = CheckpointRecord(
        task_id=meta["task_id"],
        step=int(meta["step"]),
        sequence=int(meta["sequence"]),
        sample_cursor=int(meta.get("sample_cursor", 0)),
        sample_fingerprint=meta["sample_fingerprint"],
        config_fingerprint=meta["config_fingerprint"],
        adapter_digest=meta["adapter_digest"],
        loss_history=list(meta["loss_history"]),
        seed=int(meta["seed"]),
    )
    verify_adapter_digest(record, state)
    return record, state


def verify_adapter_digest(
    record: CheckpointRecord, state: dict[str, torch.Tensor]
) -> None:
    for name, tensor in state.items():
        actual = tensor_digest(tensor)
        expected = record.adapter_digest.get(name)
        if expected != actual:
            raise WeightMismatchError(
                f"adapter tensor {name} digest mismatch",
                task_id=record.task_id,
                step=record.step,
                field=name,
                detail={"expected": expected, "actual": actual},
            )


def cleanup_uncommitted(workspace: str, task_id: str) -> list[str]:
    """Remove half-written checkpoint directories; return removed paths."""
    directory = task_dir(workspace, task_id)
    removed: list[str] = []
    if not os.path.isdir(directory):
        return removed
    for name in sorted(os.listdir(directory)):
        path = os.path.join(directory, name)
        if name.startswith("ckpt-") and os.path.isdir(path) and not is_committed(path):
            _safe_rmtree(path)
            removed.append(path)
    return removed


def invalidate_from(workspace: str, task_id: str, sequence: int) -> list[str]:
    """Delete checkpoints whose sequence is >= ``sequence`` (config change)."""
    removed: list[str] = []
    for seq, path in list_committed(workspace, task_id):
        if seq >= sequence:
            _safe_rmtree(path)
            removed.append(path)
    return removed


def _safe_rmtree(path: str) -> None:
    import shutil

    shutil.rmtree(path, ignore_errors=True)
