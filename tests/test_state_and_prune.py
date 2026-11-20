from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from checkpoint_recovery import CheckpointManager, ErrorCode, ManagerConfig, ServiceError
from checkpoint_recovery.models import CheckpointState
from checkpoint_recovery.state_machine import ALLOWED_TRANSITIONS, validate_transition


def test_state_machine_rejects_illegal_transition_without_mutation(tmp_path: Path, payload_factory) -> None:
    assert CheckpointState.COMMITTED in ALLOWED_TRANSITIONS[CheckpointState.WRITING]
    assert CheckpointState.COMMITTED not in ALLOWED_TRANSITIONS[CheckpointState.PRUNED]
    with pytest.raises(ServiceError) as caught:
        validate_transition(CheckpointState.INVALID, CheckpointState.COMMITTED)
    assert caught.value.code == ErrorCode.INVALID_TRANSITION

    config = ManagerConfig(data_dir=tmp_path / "data")
    with CheckpointManager(config) as manager:
        checkpoint = manager.submit_checkpoint(payload_factory(1), wait=True)
        before = manager.get_transitions(checkpoint.checkpoint_id)
        with pytest.raises(ServiceError) as caught:
            manager.db.transition(
                checkpoint.checkpoint_id,
                CheckpointState.COMMITTED,
                "illegal_self_transition",
            )
        assert caught.value.code == ErrorCode.INVALID_TRANSITION
        after = manager.get_transitions(checkpoint.checkpoint_id)
        assert len(after) == len(before)
        assert manager.get_checkpoint(checkpoint.checkpoint_id)["status"] == "COMMITTED"


def test_prune_uses_count_and_round_interval_and_keeps_latest(tmp_path: Path, payload_factory) -> None:
    config = ManagerConfig(
        data_dir=tmp_path / "data",
        max_checkpoints=3,
        min_retain_round_interval=2,
    )
    with CheckpointManager(config) as manager:
        created = [manager.submit_checkpoint(payload_factory(round_number), wait=True) for round_number in range(1, 6)]
        latest = created[-1]
        result = manager.prune()
        pruned_rounds = sorted(item["round"] for item in result["pruned"])
        retained_rounds = [item["round"] for item in result["retained"]]
        assert pruned_rounds == [2, 4]
        assert retained_rounds == [5, 3, 1]
        latest_state = manager.get_checkpoint(latest.checkpoint_id)
        assert latest_state["status"] == "COMMITTED"
        for item in created:
            state = manager.get_checkpoint(item.checkpoint_id)
            if item.round in pruned_rounds:
                assert state["status"] == "PRUNED"
                assert state["manifest_path"] is None
                assert state["shards"] == []
                assert not (tmp_path / "data" / "checkpoints" / item.checkpoint_id).exists()
        with sqlite3.connect(tmp_path / "data" / "checkpoints.db") as conn:
            intents = conn.execute(
                "SELECT state, COUNT(*) FROM intent_log GROUP BY state"
            ).fetchall()
        assert intents == [("DONE", 2)]
        with pytest.raises(ServiceError) as caught:
            manager.prune([latest.checkpoint_id])
        assert caught.value.code == ErrorCode.PRUNE_CONFLICT


def test_auto_prune_skips_runtime_protected_checkpoint(tmp_path: Path, payload_factory) -> None:
    config = ManagerConfig(data_dir=tmp_path / "data", max_checkpoints=1, min_retain_round_interval=0)
    with CheckpointManager(config) as manager:
        first = manager.submit_checkpoint(payload_factory(1), wait=True)
        second = manager.submit_checkpoint(payload_factory(2), wait=True)
        # In a serial mutation domain, the newest COMMITTED checkpoint is always protected.
        result = manager.prune()
        assert [item["round"] for item in result["pruned"]] == [1]
        assert [item["round"] for item in result["retained"]] == [2]
        assert manager.get_checkpoint(first.checkpoint_id)["status"] == "PRUNED"
        assert manager.get_checkpoint(second.checkpoint_id)["status"] == "COMMITTED"
        with pytest.raises(ServiceError) as caught:
            manager.prune([first.checkpoint_id])
        assert caught.value.code == ErrorCode.PRUNE_CONFLICT

