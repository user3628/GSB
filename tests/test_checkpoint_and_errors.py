import os

import pytest
import torch

from lora_bench.checkpoint import (
    CheckpointRecord,
    cleanup_uncommitted,
    invalidate_from,
    latest_sequence,
    list_committed,
    load_latest,
    publish_checkpoint,
)
from lora_bench.determinism import tensor_digest
from lora_bench.errors import (
    E_STALE_CHECKPOINT,
    E_WEIGHT_MISMATCH,
    StaleCheckpointError,
    WeightMismatchError,
)


def _state():
    return {"lora_a": torch.zeros(2, 2), "lora_b": torch.ones(2, 2)}


def _real_digest():
    return {name: tensor_digest(tensor) for name, tensor in _state().items()}


def _record(sequence, step=1, digest=None):
    return CheckpointRecord(
        task_id="task-x",
        step=step,
        sequence=sequence,
        sample_cursor=step * 4,
        sample_fingerprint="sf",
        config_fingerprint="cf",
        adapter_digest=digest if digest is not None else _real_digest(),
        loss_history=[0.1] * step,
        seed=1,
    )


def test_publish_enforces_monotonic_sequence(tmp_path):
    ws = str(tmp_path)
    publish_checkpoint(ws, _record(1), _state())
    with pytest.raises(StaleCheckpointError) as exc:
        publish_checkpoint(ws, _record(1), _state())
    assert exc.value.code == E_STALE_CHECKPOINT
    assert exc.value.task_id == "task-x"
    publish_checkpoint(ws, _record(2, step=2), _state())
    assert latest_sequence(ws, "task-x") == 2


def test_expected_previous_sequence_guard(tmp_path):
    ws = str(tmp_path)
    publish_checkpoint(ws, _record(1), _state())
    with pytest.raises(StaleCheckpointError):
        publish_checkpoint(
            ws, _record(2, step=2), _state(), expected_previous_sequence=5
        )


def test_uncommitted_directory_ignored_and_cleaned(tmp_path):
    ws = str(tmp_path)
    publish_checkpoint(ws, _record(1), _state())
    half = os.path.join(ws, "checkpoints", "task-x", "ckpt-00000002")
    os.makedirs(half)
    with open(os.path.join(half, "checkpoint.json"), "w") as handle:
        handle.write("{broken")
    assert latest_sequence(ws, "task-x") == 1
    removed = cleanup_uncommitted(ws, "task-x")
    assert removed == [half]
    assert not os.path.exists(half)


def test_invalidation_and_reload(tmp_path):
    ws = str(tmp_path)
    for seq in range(1, 4):
        publish_checkpoint(ws, _record(seq, step=seq), _state())
    invalidate_from(ws, "task-x", 2)
    assert [seq for seq, _ in list_committed(ws, "task-x")] == [1]
    record, state = load_latest(ws, "task-x")
    assert record.sequence == 1 and record.step == 1


def test_weight_mismatch_detected(tmp_path):
    ws = str(tmp_path)
    bad_digest = dict(_real_digest())
    bad_digest["lora_a"] = "deadbeef"
    record = _record(1, digest=bad_digest)
    publish_checkpoint(ws, record, _state())
    with pytest.raises(WeightMismatchError) as exc:
        load_latest(ws, "task-x")
    assert exc.value.code == E_WEIGHT_MISMATCH
    assert exc.value.field == "lora_a"
