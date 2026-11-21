from __future__ import annotations

import multiprocessing
import threading

import pytest

from rag_orchestrator.checkpoints import CheckpointStore
from rag_orchestrator.errors import E_INVALID_STAGE, E_STALE_CHECKPOINT
from rag_orchestrator.util import atomic_write_json


def _append(store: CheckpointStore, query_id: str, stage: str, product_query_id: str | None = None) -> dict:
    return store.append(
        query_id=query_id,
        stage=stage,
        input_fingerprint=f"input-{stage}",
        component_fingerprints={"input": f"input-{stage}", "config": "cfg"},
        stage_fingerprint=f"fp-{stage}",
        budget_config_version="budget-v1",
        product_summary={"stage": stage},
        product={"query_id": product_query_id if product_query_id is not None else query_id, "stage": stage},
    )


def test_stage_jump_and_duplicate_sequence_are_rejected(tmp_path) -> None:
    store = CheckpointStore(tmp_path / "ws")
    with pytest.raises(Exception) as jump:
        _append(store, "q", "dispatch")
    assert jump.value.code == E_INVALID_STAGE
    first = _append(store, "q", "normalize")
    assert first["seq"] == 1 and first["stage_seq"] == 1
    with pytest.raises(Exception) as duplicate:
        _append(store, "q", "normalize")
    assert duplicate.value.code == E_STALE_CHECKPOINT
    assert len(store.records("q")) == 1


def test_cross_request_payload_is_rejected_as_stale(tmp_path) -> None:
    store = CheckpointStore(tmp_path / "ws")
    with pytest.raises(Exception) as mismatch:
        _append(store, "q-a", "normalize", product_query_id="q-b")
    assert mismatch.value.code == E_STALE_CHECKPOINT
    assert store.records("q-a") == []


def test_sequence_is_continuous_across_rebuild_epoch(tmp_path) -> None:
    store = CheckpointStore(tmp_path / "ws")
    for stage in ("normalize", "dispatch", "rerank", "assemble"):
        _append(store, "q", stage)
    store.begin_epoch("q", "rerank", [{"stage": "rerank", "reason": "test"}])
    rebuilt = _append(store, "q", "rerank")
    assembled = _append(store, "q", "assemble")
    assert rebuilt["seq"] == 5 and rebuilt["stage_seq"] == 2 and rebuilt["epoch"] == 2
    assert assembled["seq"] == 6 and assembled["stage_seq"] == 2
    ledger = store.read_ledger("q")
    assert ledger["last_seq"] == 6
    assert ledger["latest_by_stage"] == {"normalize": 1, "dispatch": 2, "rerank": 5, "assemble": 6}


def test_concurrent_publication_uses_file_lock_and_allows_one_winner(tmp_path) -> None:
    store = CheckpointStore(tmp_path / "ws")
    barrier = threading.Barrier(2)
    outcomes: list[str] = []

    def worker() -> None:
        barrier.wait()
        try:
            _append(store, "q", "normalize")
            outcomes.append("ok")
        except Exception as exc:  # noqa: BLE001
            outcomes.append(getattr(exc, "code", type(exc).__name__))

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert outcomes.count("ok") == 1
    assert outcomes.count(E_STALE_CHECKPOINT) == 1
    assert len(store.records("q")) == 1


def _process_publication(workspace: str, barrier, output) -> None:
    store = CheckpointStore(workspace)
    barrier.wait()
    try:
        result = _append(store, "q", "normalize")
        output.put(("ok", result["seq"]))
    except Exception as exc:  # noqa: BLE001
        output.put((getattr(exc, "code", type(exc).__name__), None))


def test_multiple_processes_only_allow_one_contiguous_publication(tmp_path) -> None:
    context = multiprocessing.get_context("fork")
    workspace = str(tmp_path / "process-ws")
    barrier = context.Barrier(2)
    output = context.Queue()
    processes = [
        context.Process(target=_process_publication, args=(workspace, barrier, output))
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0
    outcomes = [output.get(timeout=2), output.get(timeout=2)]
    assert [item[0] for item in outcomes].count("ok") == 1
    assert [item[0] for item in outcomes].count(E_STALE_CHECKPOINT) == 1
    assert len(CheckpointStore(workspace).records("q")) == 1


def test_atomic_failure_never_leaves_half_checkpoint(tmp_path, monkeypatch) -> None:
    store = CheckpointStore(tmp_path / "ws")
    import rag_orchestrator.checkpoints as checkpoint_module

    def fail(_path, _value, mode=0o600):
        raise OSError("simulated interruption")

    monkeypatch.setattr(checkpoint_module, "atomic_write_json", fail)
    with pytest.raises(OSError):
        _append(store, "q", "normalize")
    stage_dir = store._stage_dir("q", "normalize")
    assert not list(stage_dir.glob("*.json"))
    monkeypatch.undo()
    record = _append(store, "q", "normalize")
    assert record["seq"] == 1


def test_record_written_before_ledger_is_recovered(tmp_path, monkeypatch) -> None:
    store = CheckpointStore(tmp_path / "ws")
    import rag_orchestrator.checkpoints as checkpoint_module

    real_write = checkpoint_module.atomic_write_json
    ledger_path = store._ledger_path("q")

    def fail_ledger(path, value, mode=0o600):
        if str(path) == str(ledger_path):
            raise OSError("simulated crash before ledger write")
        return real_write(path, value, mode=mode)

    monkeypatch.setattr(checkpoint_module, "atomic_write_json", fail_ledger)
    with pytest.raises(OSError):
        _append(store, "q", "normalize")
    monkeypatch.undo()
    ledger = store.read_ledger("q")
    assert ledger["last_seq"] == 1
    assert ledger["stage_seq"]["normalize"] == 1
    assert ledger["cursor"] == 1


def test_atomic_writer_does_not_publish_partial_target(tmp_path, monkeypatch) -> None:
    target = tmp_path / "target.json"
    import rag_orchestrator.util as util_module

    def fail_replace(src, dst):
        raise OSError("simulated replace failure")

    monkeypatch.setattr(util_module.os, "replace", fail_replace)
    with pytest.raises(OSError):
        atomic_write_json(target, {"value": 1})
    assert not target.exists()
    assert not list(tmp_path.glob("*.tmp-*"))
