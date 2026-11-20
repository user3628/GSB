from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from dialogue_engine.checkpoints import CheckpointStore
from dialogue_engine.engine import DialogueEngine
from dialogue_engine.errors import E_INVALID_STAGE, EngineError

from helpers import file_digest, tree_digests


def test_full_pipeline_is_deterministic(demo_data: Path, tmp_path: Path) -> None:
    left_ws = tmp_path / "left"
    right_ws = tmp_path / "right"
    left = DialogueEngine(left_ws).run(demo_data)
    right = DialogueEngine(right_ws).run(demo_data)

    assert left["status"] == "completed"
    assert left["sequence"] == 60
    assert left["metrics"]["emotion_accuracy"] == 1.0
    assert left["metrics"]["behavior_accuracy"] == 1.0
    assert left["metrics"]["interruption_recall"] == 1.0
    assert left["summary_digest"] == right["summary_digest"]
    assert (left_ws / "exports" / "summary.json").read_bytes() == (
        right_ws / "exports" / "summary.json"
    ).read_bytes()
    assert tree_digests(left_ws / "checkpoints" / "artifacts") == tree_digests(
        right_ws / "checkpoints" / "artifacts"
    )
    assert tree_digests(left_ws / "checkpoints" / "journal") == tree_digests(
        right_ws / "checkpoints" / "journal"
    )


def test_stop_between_turns_and_resume_are_byte_identical(
    demo_data: Path, tmp_path: Path
) -> None:
    fresh_ws = tmp_path / "fresh"
    resumed_ws = tmp_path / "resumed"
    fresh = DialogueEngine(fresh_ws).run(demo_data)

    interrupted_engine = DialogueEngine(resumed_ws)
    stopped = interrupted_engine.run(demo_data, stop_after_turns=2)
    assert stopped["status"] == "stopped"
    assert stopped["completed_state_count"] == 2
    assert stopped["sequence"] == 8

    try:
        interrupted_engine.query_turn("shard_01", 3)
    except EngineError as exc:
        assert exc.code == E_INVALID_STAGE
    else:  # pragma: no cover
        raise AssertionError("missing turn should raise E_INVALID_STAGE")

    resumed = interrupted_engine.resume(demo_data)
    assert resumed["status"] == "completed"
    assert resumed["sequence"] == 60
    assert resumed["reused_stages"] == {
        "features": 2,
        "emotion": 2,
        "behavior": 2,
        "state": 2,
    }
    assert resumed["summary_digest"] == fresh["summary_digest"]
    assert (fresh_ws / "exports" / "summary.json").read_bytes() == (
        resumed_ws / "exports" / "summary.json"
    ).read_bytes()
    assert tree_digests(fresh_ws / "checkpoints" / "artifacts") == tree_digests(
        resumed_ws / "checkpoints" / "artifacts"
    )
    assert tree_digests(fresh_ws / "checkpoints" / "journal") == tree_digests(
        resumed_ws / "checkpoints" / "journal"
    )

    store = CheckpointStore(resumed_ws)
    status = store.status()
    assert status["healthy"]
    assert status["sequence"] == 60
    assert sorted(int(path.stem) for path in store.journal_dir.glob("*.json")) == list(
        range(1, 61)
    )


def test_external_stop_request_completes_current_turn(demo_data: Path, tmp_path: Path) -> None:
    workspace = tmp_path / "external_stop"
    engine = DialogueEngine(workspace)
    engine.request_stop("test external stop")
    stopped = engine.run(demo_data)
    assert stopped["status"] == "stopped"
    assert stopped["completed_state_count"] == 1
    assert stopped["sequence"] == 4
    assert engine.stop_requested()
    resumed = engine.resume(demo_data)
    assert resumed["status"] == "completed"
    assert resumed["sequence"] == 60
    assert not engine.stop_requested()


def test_query_and_export_shapes(demo_data: Path, tmp_path: Path) -> None:
    workspace = tmp_path / "query"
    engine = DialogueEngine(workspace)
    engine.run(demo_data)
    turn = engine.query_turn("shard_01", 0)
    assert set(turn["emotion"]["scores"]) == {"平静", "高兴", "愤怒", "悲伤"}
    assert set(turn["behavior"]["scores"]) == {"陈述", "提问", "确认", "打断"}
    assert turn["state"]["state"]["step"] == 0
    assert turn["state"]["state"]["dominant_emotion"] in {"平静", "高兴", "愤怒", "悲伤"}
    summary = engine.export_summary()
    assert summary["metrics"]["turn_count"] == 15
    assert len(summary["shards"]) == 3
    assert summary["summary_digest"]
    assert file_digest(workspace / "exports" / "summary.json")


def test_two_full_engines_concurrently_converge(demo_data: Path, tmp_path: Path) -> None:
    workspace = tmp_path / "concurrent_engine"
    project_root = Path(__file__).resolve().parents[1]
    script = r"""
import json, sys
from dialogue_engine import DialogueEngine
result = DialogueEngine(sys.argv[1]).run(sys.argv[2])
print(json.dumps({
    "status": result["status"],
    "sequence": result["sequence"],
    "summary_digest": result["summary_digest"],
}))
"""
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", script, str(workspace), str(demo_data)],
            cwd=project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(2)
    ]
    results = []
    for process in processes:
        stdout, stderr = process.communicate(timeout=60)
        assert process.returncode == 0, stderr
        results.append(json.loads(stdout.strip().splitlines()[-1]))
    assert [result["status"] for result in results] == ["completed", "completed"]
    assert [result["sequence"] for result in results] == [60, 60]
    assert results[0]["summary_digest"] == results[1]["summary_digest"]
    status = CheckpointStore(workspace).status()
    assert status["healthy"]
    assert status["sequence"] == 60
    assert status["record_count"] == 60

