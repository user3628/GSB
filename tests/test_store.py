from __future__ import annotations

import json
import multiprocessing
from pathlib import Path

import pytest

from qworkbench.errors import BusinessError
from qworkbench.store import CheckpointStore


def _checkpoint_record(artifact_rel: str, digest: str, stage: str = "normalize") -> dict:
    return {
        "stage": stage,
        "input_fingerprint": "input-fp",
        "config_version": "test-config",
        "config_fingerprint": "config-fp",
        "upstream_fingerprint": "upstream-fp",
        "fingerprint": f"{stage}-fp",
        "output_fingerprint": "output-fp",
        "artifacts": {artifact_rel: digest},
        "summary": {"records": 1},
        "components": {"test": True},
    }


def _artifact(workdir: Path) -> tuple[str, str]:
    path = workdir / "artifacts" / "fixture.bin"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"deterministic-artifact\n")
    from qworkbench.util import digest_file

    return str(path.relative_to(workdir)), digest_file(path)


def _concurrent_publish(workdir: str, artifact_rel: str, digest: str, start, queue) -> None:
    store = CheckpointStore(Path(workdir))
    record = _checkpoint_record(artifact_rel, digest)
    start.wait(5)
    try:
        store.publish_stage(record, seq=1)
        queue.put("OK")
    except BusinessError as exc:
        queue.put(exc.code)
    except Exception as exc:  # pragma: no cover - diagnostic process boundary
        queue.put(type(exc).__name__)


def test_missing_predecessor_returns_invalid_stage(tmp_path) -> None:
    store = CheckpointStore(tmp_path)
    relative, digest = _artifact(tmp_path)
    record = _checkpoint_record(relative, digest, stage="score")
    with pytest.raises(BusinessError) as caught:
        store.publish_stage(record, seq=1)
    assert caught.value.code == "E_INVALID_STAGE"
    assert caught.value.details["required_predecessor"] == "normalize"


def test_stale_and_duplicate_sequence(tmp_path) -> None:
    store = CheckpointStore(tmp_path)
    relative, digest = _artifact(tmp_path)
    record = _checkpoint_record(relative, digest)
    store.publish_stage(record, seq=1)
    with pytest.raises(BusinessError) as caught:
        store.publish_stage(record, seq=1)
    assert caught.value.code == "E_STALE_CHECKPOINT"
    state = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    assert state["last_seq"] == 1
    with pytest.raises(BusinessError) as caught:
        store.publish_stage(record, seq=2)
    assert caught.value.code == "E_STALE_CHECKPOINT"
    assert caught.value.details["fingerprint"] == "normalize-fp"


def test_concurrent_publishers_only_one_wins(tmp_path) -> None:
    relative, digest = _artifact(tmp_path)
    context = multiprocessing.get_context("fork")
    start = context.Event()
    queue = context.Queue()
    processes = [
        context.Process(target=_concurrent_publish, args=(str(tmp_path), relative, digest, start, queue))
        for _ in range(6)
    ]
    for process in processes:
        process.start()
    start.set()
    results = [queue.get(timeout=15) for _ in processes]
    for process in processes:
        process.join(timeout=15)
        assert process.exitcode == 0
    assert results.count("OK") == 1
    assert results.count("E_STALE_CHECKPOINT") == 5
    state = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    assert state["last_seq"] == 1
    checkpoint_files = list((tmp_path / "checkpoints").glob("*.json"))
    assert len(checkpoint_files) == 1
    record = json.loads(checkpoint_files[0].read_text(encoding="utf-8"))
    assert record["seq"] == 1
