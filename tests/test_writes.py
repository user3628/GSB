from __future__ import annotations

import threading
from pathlib import Path

import pytest

from checkpoint_recovery import CheckpointManager, ErrorCode, ManagerConfig, ServiceError
from checkpoint_recovery.samples import generate_samples, load_sample_payload


def test_fixed_samples_are_deterministic(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    generate_samples(first, rounds=2)
    generate_samples(second, rounds=2)
    for round_number in (1, 2):
        first_meta = (first / f"round-{round_number:04d}" / "metadata.json").read_text(encoding="utf-8")
        second_meta = (second / f"round-{round_number:04d}" / "metadata.json").read_text(encoding="utf-8")
        assert first_meta == second_meta
        for kind in ("params", "optimizer"):
            for shard in sorted((first / f"round-{round_number:04d}" / kind).glob("*.bin")):
                counterpart = second / f"round-{round_number:04d}" / kind / shard.name
                assert shard.read_bytes() == counterpart.read_bytes()
    payload_a = load_sample_payload(first / "round-0001" / "metadata.json", "same")
    payload_b = load_sample_payload(second / "round-0001" / "metadata.json", "same")
    assert payload_a == payload_b


def test_async_write_is_visible_as_writing_then_commits(tmp_path: Path, payload_factory) -> None:
    config = ManagerConfig(data_dir=tmp_path / "data", max_writing=2, writer_delay_seconds=0.15)
    with CheckpointManager(config) as manager:
        submission = manager.submit_checkpoint(payload_factory(1), wait=False)
        assert submission.status == "WRITING"
        visible = manager.get_checkpoint(submission.checkpoint_id)
        assert visible["status"] == "WRITING"
        committed = manager.wait_for_request(submission.request_id)
        assert committed["status"] == "COMMITTED"
        assert committed["round"] == 1
        transitions = manager.get_transitions(submission.checkpoint_id)
        assert [item["to_state"] for item in transitions] == ["WRITING", "COMMITTED"]
        assert all(item["reason"] for item in transitions)
        assert Path(config.data_dir / committed["manifest_path"]).is_file()


def test_duplicate_request_id_executes_once_and_reuses_result(tmp_path: Path, payload_factory) -> None:
    config = ManagerConfig(data_dir=tmp_path / "data", max_writing=2, writer_delay_seconds=0.08)
    payload = payload_factory(1, request_id="idempotent-write")
    results: list = []
    failures: list[BaseException] = []
    barrier = threading.Barrier(6)

    def submit() -> None:
        try:
            barrier.wait(timeout=5)
            results.append(manager.submit_checkpoint(payload, wait=True))
        except BaseException as exc:  # pragma: no cover - surfaced below
            failures.append(exc)

    with CheckpointManager(config) as manager:
        threads = [threading.Thread(target=submit) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        assert not failures
        assert len(results) == 6
        assert len({(item.checkpoint_id, item.status) for item in results}) == 1
        assert sum(1 for item in results if not item.reused) == 1
        checkpoints = manager.list_checkpoints()
        assert len(checkpoints) == 1
        assert checkpoints[0]["status"] == "COMMITTED"


def test_duplicate_without_wait_flag_still_waits_and_reuses_result(tmp_path: Path, payload_factory) -> None:
    config = ManagerConfig(data_dir=tmp_path / "data", max_writing=2, writer_delay_seconds=0.12)
    payload = payload_factory(1, request_id="duplicate-wait")
    with CheckpointManager(config) as manager:
        first = manager.submit_checkpoint(payload, wait=False)
        duplicate = manager.submit_checkpoint(payload, wait=False)
        assert duplicate.reused is True
        assert duplicate.checkpoint_id == first.checkpoint_id
        assert duplicate.status == "COMMITTED"
        assert len(manager.list_checkpoints()) == 1


def test_declared_checksum_mismatch_preserves_previous_checkpoint(tmp_path: Path, payload_factory) -> None:
    config = ManagerConfig(data_dir=tmp_path / "data")
    with CheckpointManager(config) as manager:
        first = manager.submit_checkpoint(payload_factory(1), wait=True)
        bad = payload_factory(2)
        bad["params"][0]["sha256"] = "0" * 64
        with pytest.raises(ServiceError) as caught:
            manager.submit_checkpoint(bad, wait=False)
        assert caught.value.code == ErrorCode.CHECKSUM_MISMATCH
        assert [item["round"] for item in manager.list_checkpoints()] == [1]
        baseline = manager.get_recovery_baseline()
        assert baseline["checkpoint"]["checkpoint_id"] == first.checkpoint_id
        assert baseline["checkpoint"]["status"] == "COMMITTED"


def test_temp_file_corruption_returns_checksum_error_and_keeps_old_checkpoint(
    tmp_path: Path, payload_factory
) -> None:
    data_dir = tmp_path / "data"
    with CheckpointManager(ManagerConfig(data_dir=data_dir)) as manager:
        first = manager.submit_checkpoint(payload_factory(1), wait=True)
    fault_config = ManagerConfig(data_dir=data_dir, fault_corrupt_after_temp_write=True)
    with CheckpointManager(fault_config) as manager:
        with pytest.raises(ServiceError) as caught:
            manager.submit_checkpoint(payload_factory(2), wait=True)
        assert caught.value.code == ErrorCode.CHECKSUM_MISMATCH
        second = [item for item in manager.list_checkpoints() if item["round"] == 2][0]
        assert second["status"] == "INVALID"
        baseline = manager.get_recovery_baseline()
        assert baseline["checkpoint"]["checkpoint_id"] == first.checkpoint_id
        assert not (data_dir / "tmp" / second["checkpoint_id"]).exists()

