from __future__ import annotations

from fractions import Fraction

import pytest

from storypipe.config import normalize_config
from storypipe.errors import PipelineError
from storypipe.models import parse_shot
from storypipe.schedule import build_schedule
from storypipe.generator import sample_records


def _shot(index: int, *, duration: int = 5, fps: float = 29.97, move: str = "static"):
    return parse_shot(
        {
            "shot_id": f"s{index}",
            "image": f"{index}.png",
            "prompt": "中文 prompt / English prompt",
            "camera_move": move,
            "duration_frames": duration,
            "fps": fps,
            "transition": "crossfade",
        },
        index,
    )


def test_mixed_fps_timeline_is_exact_and_stable() -> None:
    shots = [_shot(0, duration=7, fps=29.97), _shot(1, duration=5, fps=24), _shot(2, duration=9, fps=23.976)]
    config = normalize_config({})
    first = build_schedule(shots, config)
    second = build_schedule(shots, config)
    assert first == second
    assert first["total_frames"] == 21
    frames = first["frames"]
    assert [frame["global_frame"] for frame in frames] == list(range(21))
    for left, right in zip(frames, frames[1:]):
        assert left["end_exact"] == right["start_exact"]
        assert left["end_us"] == right["start_us"]
    assert any(frame["start_us"] % 1000 != 0 for frame in frames)
    assert frames[0]["start_exact"] == "0/1"
    assert first["shots"][0]["end_exact"] == first["shots"][1]["start_exact"]
    assert Fraction(frames[-1]["end_exact"]) > Fraction(frames[0]["start_exact"])


@pytest.mark.parametrize(
    "record",
    [
        {
            "shot_id": "bad-duration",
            "image": "x.png",
            "prompt": "x",
            "camera_move": "static",
            "duration_frames": 0,
            "fps": 24,
            "transition": "hard_cut",
        },
        {
            "shot_id": "bad-fps",
            "image": "x.png",
            "prompt": "x",
            "camera_move": "static",
            "duration_frames": 2,
            "fps": -24,
            "transition": "hard_cut",
        },
    ],
)
def test_invalid_timeline_error_has_shot_id(record: dict) -> None:
    with pytest.raises(PipelineError) as caught:
        parse_shot(record, 0)
    assert caught.value.code == "E_INVALID_TIMELINE"
    assert caught.value.shot_id == record["shot_id"]


def test_invalid_camera_move_code() -> None:
    record = {
        "shot_id": "bad-move",
        "image": "x.png",
        "prompt": "x",
        "camera_move": "orbit",
        "duration_frames": 2,
        "fps": 24,
        "transition": "hard_cut",
    }
    with pytest.raises(PipelineError) as caught:
        parse_shot(record, 0)
    assert caught.value.code == "E_INVALID_SHOT"
    assert caught.value.shot_id == "bad-move"


def test_generated_schedule_mixed_rates_align() -> None:
    records, config = sample_records("small")
    shots = [parse_shot(record, index) for index, record in enumerate(records)]
    schedule = build_schedule(shots, config)
    for shot in schedule["shots"]:
        assert shot["global_frame_end"] - shot["global_frame_start"] == shot["duration_frames"]
    assert schedule["frames"][-1]["global_frame"] == schedule["total_frames"] - 1
