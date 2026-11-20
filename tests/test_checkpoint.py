import json

import pytest

from prefalign.core.checkpoint import (
    FileLock,
    STAGE_ORDER,
    checkpoint_record,
    latest_consistent,
    load_checkpoint,
    publish_checkpoint,
    truncate_from,
)
from prefalign.core.errors import InvalidStageError, StaleCheckpointError


def _record(stage, seq, digest="d"):
    return checkpoint_record(stage, seq, {"s": 1}, 1, "cfg", digest, f"fp_{stage}")


def test_publish_requires_monotonic_seq(tmp_path):
    with FileLock(tmp_path / "lock"):
        with pytest.raises(StaleCheckpointError):
            publish_checkpoint(tmp_path, _record("quality", 5))
        publish_checkpoint(tmp_path, _record("normalize", 1))
        with pytest.raises(InvalidStageError):
            publish_checkpoint(tmp_path, _record("sampling", 4))
        publish_checkpoint(tmp_path, _record("quality", 2))
        with pytest.raises(StaleCheckpointError):
            publish_checkpoint(tmp_path, _record("normalize", 2))


def test_corrupt_checkpoint_ignored(tmp_path):
    with FileLock(tmp_path / "lock"):
        publish_checkpoint(tmp_path, _record("normalize", 1))
    path = tmp_path / "normalize.checkpoint.json"
    path.write_text("{half-written", encoding="utf-8")
    assert load_checkpoint(tmp_path, "normalize") is None
    assert latest_consistent(tmp_path) == (None, 0)


def test_truncate_removes_downstream(tmp_path):
    with FileLock(tmp_path / "lock"):
        for index, stage in enumerate(STAGE_ORDER):
            publish_checkpoint(tmp_path, _record(stage, index + 1))
        truncate_from(tmp_path, "quality")
    assert load_checkpoint(tmp_path, "normalize") is not None
    assert load_checkpoint(tmp_path, "quality") is None
    assert load_checkpoint(tmp_path, "sampling") is None


def test_atomic_write_no_temp_leftover(built_pipeline):
    built_pipeline.run(reason="x")
    leftovers = list(built_pipeline.checkpoints_dir.glob("*.tmp"))
    assert leftovers == []


def test_stop_resume_roundtrip(built_pipeline):
    # A stop requested before the run is honored: no new stage unit executes,
    # then resume rebuilds the full chain from the intact prefix.
    built_pipeline.invalidate_chain("quality", "test_stop",
                                    built_pipeline.store.effective_samples()[1])
    built_pipeline.request_stop()
    outcome = built_pipeline.run_with_stop()
    assert outcome["status"] == "stopped"
    assert outcome["last_completed_stage"] == "normalize"
    built_pipeline.clear_stop()
    resumed = built_pipeline.resume()
    assert resumed["resumed_from"] == "normalize"
    status = built_pipeline.status()
    seqs = [info["seq"] for info in status["checkpoints"].values() if info["present"]]
    assert seqs == [1, 2, 3, 4]


def test_stop_after_first_stage_resumes_remaining(built_pipeline):
    built_pipeline.invalidate_chain("quality", "test_stop_2",
                                    built_pipeline.store.effective_samples()[1])
    calls = {"n": 0}
    original = built_pipeline._checkpoint

    def stop_after_first():
        calls["n"] += 1
        if calls["n"] == 1:
            built_pipeline.request_stop()
        original()

    built_pipeline._checkpoint = stop_after_first
    outcome = built_pipeline.run_with_stop()
    assert outcome["status"] == "stopped"
    assert outcome["last_completed_stage"] == "normalize"
    built_pipeline.clear_stop()
    built_pipeline.resume()
    seqs = [info["seq"] for info in
            built_pipeline.status()["checkpoints"].values() if info["present"]]
    assert seqs == [1, 2, 3, 4]
