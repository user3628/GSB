from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import numpy as np

from storypipe.cache_policy import select_cache_policy
from storypipe.config import normalize_config
from storypipe.frame_provider import ComposedFrameProvider
from storypipe.generator import sample_records
from storypipe.models import parse_shot
from storypipe.motion import bilinear_sample, compose_frame, transform_matrix
from storypipe.schedule import build_schedule


def _image() -> np.ndarray:
    yy, xx = np.indices((9, 13), dtype=np.float32)
    return np.stack(
        [xx / 12.0, yy / 8.0, ((xx + yy) % 11) / 10.0], axis=-1
    ).astype(np.float32)


def test_static_identity_and_boundary_clipping() -> None:
    image = _image()
    identity = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]]
    sampled = bilinear_sample(image, identity)
    np.testing.assert_array_equal(sampled, image)
    clipped = bilinear_sample(
        image, [[1.0, 0.0, -1000.0], [0.0, 1.0, -1000.0], [0.0, 0.0, 1.0]]
    )
    expected = np.broadcast_to(image[0, 0], clipped.shape)
    np.testing.assert_array_equal(clipped, expected)


def test_motion_output_is_deterministic() -> None:
    image = _image()
    config = normalize_config({})
    for move in ("pan", "zoom"):
        shot = parse_shot(
            {
                "shot_id": move,
                "image": "x.png",
                "prompt": "x / y",
                "camera_move": move,
                "duration_frames": 6,
                "fps": 24,
                "transition": "hard_cut",
            },
            0,
        )
        first, matrix_a = compose_frame(image, shot, 3, config)
        second, matrix_b = compose_frame(image, shot, 3, config)
        assert matrix_a == matrix_b
        np.testing.assert_array_equal(first, second)
        assert first.shape == image.shape
        assert 0.0 <= float(first.min()) <= float(first.max()) <= 1.0
        assert transform_matrix(shot, 0, 13, 9, config) != transform_matrix(
            shot, 5, 13, 9, config
        )


def test_cache_selection_scales_and_budget() -> None:
    config = normalize_config({})
    records, _ = sample_records("small")
    small_shots = [parse_shot(record, index) for index, record in enumerate(records)]
    small_schedule = build_schedule(small_shots, config)
    small_plan = select_cache_policy(
        shot_count=len(small_shots),
        total_frames=len(small_schedule["frames"]),
        width=96,
        height=54,
        max_shot_frames=max(shot.duration_frames for shot in small_shots),
        config=config,
    )
    assert small_plan["strategy"] == "per_frame_recompute"
    assert all(
        candidate["selected"] or candidate["eliminated_reason"]
        for candidate in small_plan["candidates"]
    )
    records, _ = sample_records("large")
    large_shots = [parse_shot(record, index) for index, record in enumerate(records)]
    large_schedule = build_schedule(large_shots, config)
    large_plan = select_cache_policy(
        shot_count=len(large_shots),
        total_frames=len(large_schedule["frames"]),
        width=96,
        height=54,
        max_shot_frames=max(shot.duration_frames for shot in large_shots),
        config=config,
    )
    assert large_plan["strategy"] == "segment_cache"
    selected = next(item for item in large_plan["candidates"] if item["selected"])
    assert selected["estimated_peak_bytes"] <= config["cache"]["memory_budget_bytes"]
    full = next(item for item in large_plan["candidates"] if item["strategy"] == "full_cache")
    assert not full["viable"]
    assert "exceeds budget" in full["eliminated_reason"]


def test_full_cache_provider_matches_recompute(sample_root: Path) -> None:
    records, config = sample_records("small")
    shots = [parse_shot(record, index) for index, record in enumerate(records)]
    schedule = build_schedule(shots, config)
    base_plan = select_cache_policy(
        shot_count=len(shots),
        total_frames=len(schedule["frames"]),
        width=96,
        height=54,
        max_shot_frames=max(shot.duration_frames for shot in shots),
        config=config,
    )
    shot_map = {shot.shot_id: shot for shot in shots}
    recompute_plan = deepcopy(base_plan)
    recompute_plan["strategy"] = "per_frame_recompute"
    full_plan = deepcopy(base_plan)
    full_plan["strategy"] = "full_cache"
    recompute = ComposedFrameProvider(
        shots=shot_map,
        image_dir=sample_root / "small" / "images",
        config=config,
        cache_plan=recompute_plan,
        stage="test",
    )
    full = ComposedFrameProvider(
        shots=shot_map,
        image_dir=sample_root / "small" / "images",
        config=config,
        cache_plan=full_plan,
        stage="test",
    )
    for shot in shots:
        for local_frame in range(shot.duration_frames):
            np.testing.assert_array_equal(
                recompute.get(shot.shot_id, local_frame),
                full.get(shot.shot_id, local_frame),
            )
    total_frame_bytes = sum(
        shot.duration_frames * 96 * 54 * 3 * 4 for shot in shots
    )
    assert full.peak_frame_buffer_bytes == total_frame_bytes
