from __future__ import annotations

import io
import json
from pathlib import Path

import pytest
from PIL import Image

from storypipe.canonical import atomic_write_bytes, atomic_write_json, canonical_json
from storypipe.errors import PipelineError
from storypipe.pipeline import PipelineRunner
from storypipe.workspace import import_sample


def _read_script(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def _write_script(path: Path, records: list[dict]) -> None:
    atomic_write_bytes(
        path,
        b"".join(
            canonical_json(record, indent=None).encode("utf-8") + b"\n"
            for record in records
        ),
    )


def _new_workspace(sample_root: Path, path: Path) -> PipelineRunner:
    import_sample(sample_root / "small", path)
    return PipelineRunner(path)


def test_prompt_change_invalidates_from_compose(sample_root: Path, tmp_path: Path) -> None:
    runner = _new_workspace(sample_root, tmp_path / "prompt")
    runner.run(fresh=True)
    schedule_before = runner.checkpoints.read("schedule")
    records = _read_script(runner.workspace / "input" / "shots.jsonl")
    records[0]["prompt"] = "新的提示词 with an English suffix"
    _write_script(runner.workspace / "input" / "shots.jsonl", records)
    inspection = runner.inspect()
    assert inspection["earliest_invalid_stage"] == "compose"
    assert inspection["stages"]["schedule"]["status"] == "complete"
    assert inspection["stages"]["compose"]["status"] == "stale"
    assert "compose.prompts" in inspection["stages"]["compose"]["changed_components"]
    result = runner.run()
    assert result["ok"]
    assert runner.checkpoints.read("schedule")["sequence"] == schedule_before["sequence"]
    assert runner.checkpoints.read("schedule")["artifact_digest"] == schedule_before["artifact_digest"]


def test_image_content_change_invalidates_from_compose(sample_root: Path, tmp_path: Path) -> None:
    runner = _new_workspace(sample_root, tmp_path / "image")
    runner.run(fresh=True)
    image_path = runner.workspace / "input" / "images" / "shot_000.png"
    buffer = io.BytesIO()
    Image.new("RGB", (96, 54), (12, 34, 56)).save(buffer, format="PNG")
    atomic_write_bytes(image_path, buffer.getvalue())
    inspection = runner.inspect()
    assert inspection["earliest_invalid_stage"] == "compose"
    assert "compose.images" in inspection["stages"]["compose"]["changed_components"]
    assert runner.run()["ok"]


def test_fps_change_invalidates_from_schedule(sample_root: Path, tmp_path: Path) -> None:
    runner = _new_workspace(sample_root, tmp_path / "fps")
    runner.run(fresh=True)
    records = _read_script(runner.workspace / "input" / "shots.jsonl")
    records[0]["fps"] = 25
    _write_script(runner.workspace / "input" / "shots.jsonl", records)
    inspection = runner.inspect()
    assert inspection["earliest_invalid_stage"] == "schedule"
    assert "schedule.shots" in inspection["stages"]["schedule"]["changed_components"]
    assert runner.run()["ok"]


def test_duration_change_invalidates_from_schedule(sample_root: Path, tmp_path: Path) -> None:
    runner = _new_workspace(sample_root, tmp_path / "duration")
    runner.run(fresh=True)
    records = _read_script(runner.workspace / "input" / "shots.jsonl")
    records[0]["duration_frames"] = records[0]["duration_frames"] + 1
    _write_script(runner.workspace / "input" / "shots.jsonl", records)
    inspection = runner.inspect()
    assert inspection["earliest_invalid_stage"] == "schedule"
    assert runner.run()["ok"]


def test_camera_move_change_invalidates_from_compose(sample_root: Path, tmp_path: Path) -> None:
    runner = _new_workspace(sample_root, tmp_path / "camera")
    runner.run(fresh=True)
    records = _read_script(runner.workspace / "input" / "shots.jsonl")
    records[0]["camera_move"] = "zoom" if records[0]["camera_move"] != "zoom" else "pan"
    _write_script(runner.workspace / "input" / "shots.jsonl", records)
    inspection = runner.inspect()
    assert inspection["earliest_invalid_stage"] == "compose"
    assert "compose.camera_moves" in inspection["stages"]["compose"]["changed_components"]
    assert runner.run()["ok"]


def test_transition_change_invalidates_from_transition(sample_root: Path, tmp_path: Path) -> None:
    runner = _new_workspace(sample_root, tmp_path / "transition")
    runner.run(fresh=True)
    compose_before = runner.checkpoints.read("compose")
    config_path = runner.workspace / "input" / "config.json"
    config = json.loads(config_path.read_text())
    config["transition"]["crossfade"]["window_frames"] = 3
    atomic_write_json(config_path, config)
    inspection = runner.inspect()
    assert inspection["earliest_invalid_stage"] == "transition"
    assert inspection["stages"]["compose"]["status"] == "complete"
    assert "transition.config" in inspection["stages"]["transition"]["changed_components"]
    assert runner.run()["ok"]
    assert runner.checkpoints.read("compose")["sequence"] == compose_before["sequence"]
    manifest = runner.export_manifest()
    assert manifest["statistics"]["transition_frame_counts"]["crossfade"] == 12


def test_transition_type_change_invalidates_from_transition(
    sample_root: Path, tmp_path: Path
) -> None:
    runner = _new_workspace(sample_root, tmp_path / "transition-type")
    runner.run(fresh=True)
    records = _read_script(runner.workspace / "input" / "shots.jsonl")
    records[0]["transition"] = "hard_cut"
    _write_script(runner.workspace / "input" / "shots.jsonl", records)
    inspection = runner.inspect()
    assert inspection["earliest_invalid_stage"] == "transition"
    assert inspection["stages"]["compose"]["status"] == "complete"
    assert "transition.script" in inspection["stages"]["transition"]["changed_components"]
    assert runner.run()["ok"]


def test_cache_budget_change_invalidates_from_compose(sample_root: Path, tmp_path: Path) -> None:
    runner = _new_workspace(sample_root, tmp_path / "budget")
    runner.run(fresh=True)
    config_path = runner.workspace / "input" / "config.json"
    config = json.loads(config_path.read_text())
    config["cache"]["memory_budget_bytes"] = 7 * 1024 * 1024
    atomic_write_json(config_path, config)
    inspection = runner.inspect()
    assert inspection["earliest_invalid_stage"] == "compose"
    assert "compose.cache_config" in inspection["stages"]["compose"]["changed_components"]
    assert runner.run()["ok"]


def test_missing_image_error_and_no_compose_artifact(sample_root: Path, tmp_path: Path) -> None:
    runner = _new_workspace(sample_root, tmp_path / "missing")
    (runner.workspace / "input" / "images" / "shot_000.png").unlink()
    with pytest.raises(PipelineError) as caught:
        runner.run()
    assert caught.value.code == "E_MISSING_ASSET"
    assert caught.value.stage == "compose"
    assert caught.value.shot_id == "small-shot-000"
    assert not (runner.workspace / "stages" / "02_compose" / "compose.json").exists()
    assert runner.checkpoints.read("compose") is None


def test_transition_overflow_error_and_no_transition_artifact(sample_root: Path, tmp_path: Path) -> None:
    runner = _new_workspace(sample_root, tmp_path / "overflow")
    config_path = runner.workspace / "input" / "config.json"
    config = json.loads(config_path.read_text())
    config["transition"]["crossfade"]["window_frames"] = 100
    atomic_write_json(config_path, config)
    with pytest.raises(PipelineError) as caught:
        runner.run()
    assert caught.value.code == "E_TRANSITION_OVERFLOW"
    assert caught.value.stage == "transition"
    assert caught.value.shot_id == "small-shot-000"
    assert not (runner.workspace / "stages" / "03_transition" / "transition.json").exists()
    assert runner.checkpoints.read("transition") is None


@pytest.mark.parametrize(
    ("field", "value", "code"),
    [
        ("camera_move", "orbit", "E_INVALID_SHOT"),
        ("duration_frames", 0, "E_INVALID_TIMELINE"),
        ("fps", -1, "E_INVALID_TIMELINE"),
    ],
)
def test_invalid_script_errors(
    sample_root: Path, tmp_path: Path, field: str, value, code: str
) -> None:
    runner = _new_workspace(sample_root, tmp_path / code)
    records = _read_script(runner.workspace / "input" / "shots.jsonl")
    records[0][field] = value
    _write_script(runner.workspace / "input" / "shots.jsonl", records)
    with pytest.raises(PipelineError) as caught:
        runner.run()
    assert caught.value.code == code
    assert caught.value.shot_id == "small-shot-000"
    assert runner.checkpoints.read("schedule") is None
