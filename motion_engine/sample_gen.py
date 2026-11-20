"""Fixed-seed deterministic demo sample generator and content fingerprints."""

from __future__ import annotations

import io
import math
from pathlib import Path
from typing import Any

import numpy as np

from .manifest import MODEL_VERSION
from .model import initial_state_record, transition_state
from .render import render_frame
from .util import (
    atomic_write_bytes,
    digest_json,
    frame_fingerprint,
    pretty_json_bytes,
    sha256_bytes,
    sha256_file,
)

NO_CLAMP = {
    "x": [-1.0e9, 1.0e9],
    "y": [-1.0e9, 1.0e9],
    "vx": [-1.0e9, 1.0e9],
    "vy": [-1.0e9, 1.0e9],
    "deformation": [-1.0e9, 1.0e9],
}


def _scene_canvas(width: int, height: int, state: dict[str, Any], seed: int) -> np.ndarray:
    yy, xx = np.mgrid[0:height, 0:width]
    background = 12 + ((xx * 3 + yy * 5 + seed) % 11)
    canvas = np.empty((height, width, 3), dtype=np.float64)
    canvas[..., 0] = background
    canvas[..., 1] = background + 4
    canvas[..., 2] = background + 9

    center_x = float(state["x"])
    center_y = float(state["y"])
    radius_x = max(7.0, width * 0.16)
    radius_y = max(5.0, height * 0.15)
    blob = ((xx - center_x + radius_x * 0.9) / radius_x) ** 2 + (
        (yy - center_y + radius_y * 0.65) / radius_y
    ) ** 2
    blob_mask = blob <= 1.0
    bands = ((xx + yy + seed) // 3) % 2
    canvas[..., 0][blob_mask] = np.where(bands[blob_mask] == 0, 35, 48)
    canvas[..., 1][blob_mask] = np.where(bands[blob_mask] == 0, 174, 145)
    canvas[..., 2][blob_mask] = np.where(bands[blob_mask] == 0, 210, 194)

    half = 5
    left = int(round(center_x)) - half
    right = int(round(center_x)) + half + 1
    top = int(round(center_y)) - half
    bottom = int(round(center_y)) + half + 1
    x0, x1 = max(0, left), min(width, right)
    y0, y1 = max(0, top), min(height, bottom)
    canvas[y0:y1, x0:x1, 0] = 218
    canvas[y0:y1, x0:x1, 1] = 76
    canvas[y0:y1, x0:x1, 2] = 52
    border = max(0, 1)
    canvas[max(0, y0) : min(height, y0 + border), x0:x1, :] = 250
    canvas[max(0, y1 - border) : y1, x0:x1, :] = 250
    canvas[y0:y1, max(0, x0) : min(width, x0 + border), :] = 250
    canvas[y0:y1, max(0, x1 - border) : x1, :] = 250
    stripe_x = max(x0, min(x1 - 1, int(round(center_x))))
    canvas[y0:y1, stripe_x : min(x1, stripe_x + 2), 1] = 235
    canvas[y0:y1, stripe_x : min(x1, stripe_x + 2), 2] = 66
    luminance = (
        canvas[..., 0] * 0.299 + canvas[..., 1] * 0.587 + canvas[..., 2] * 0.114
    )
    return np.clip(np.rint(luminance), 0, 255).astype(np.uint8)


def _noise(rng: np.random.Generator, shape: tuple[int, ...]) -> np.ndarray:
    return rng.integers(-1, 2, size=shape, dtype=np.int16)


def _base_spec(
    *,
    series_id: str,
    frame_file: str,
    width: int,
    height: int,
    initial: dict[str, float],
    decay: float,
    event_step: int,
    vx_factor: float,
    vy_factor: float,
    amplitude: float,
    frequency: float,
    phase: float,
    prediction_steps: int,
    stride: int,
    render: dict[str, Any],
) -> dict[str, Any]:
    origin = 0
    frame_count = origin + prediction_steps * stride + 3
    return {
        "series_id": series_id,
        "frame_file": frame_file,
        "frame_count": frame_count,
        "frame_upper_bound": frame_count + 8,
        "prediction_steps": prediction_steps,
        "timeline": {"origin_frame": origin, "stride_frames": stride},
        "initial_state": initial,
        "dynamics": {
            "acceleration_decay": decay,
            "direction_event": {
                "step": event_step,
                "vx_factor": vx_factor,
                "vy_factor": vy_factor,
            },
        },
        "deformation": {
            "amplitude": amplitude,
            "frequency": frequency,
            "phase": phase,
        },
        "expected_state_ranges": [],
        "render": render,
    }


def _sequence_states(spec: dict[str, Any], count: int) -> list[dict[str, Any]]:
    states = [initial_state_record(spec)]
    for step in range(1, count + 1):
        state, _ = transition_state(spec, states[-1], step, ranges=NO_CLAMP)
        states.append(state)
    return states


def _expected_ranges(states: list[dict[str, Any]]) -> list[dict[str, list[float]]]:
    ranges: list[dict[str, list[float]]] = []
    margins = {
        "x": 24.0,
        "y": 24.0,
        "vx": 4.0,
        "vy": 4.0,
        "deformation": 0.25,
    }
    for state in states:
        ranges.append(
            {
                key: [float(state[key]) - margins[key], float(state[key]) + margins[key]]
                for key in ("x", "y", "vx", "vy", "deformation")
            }
        )
    return ranges


def _npy_bytes(array: np.ndarray) -> bytes:
    buffer = io.BytesIO()
    np.save(buffer, np.ascontiguousarray(array), allow_pickle=False)
    return buffer.getvalue()


def _generate_one(
    root: Path,
    *,
    seed: int,
    series_index: int,
    series_id: str,
    width: int,
    height: int,
    initial_xy: tuple[float, float],
    velocity: tuple[float, float],
    decay: float,
    event_step: int,
    vx_factor: float,
    vy_factor: float,
    amplitude: float,
    frequency: float,
    phase: float,
    prediction_steps: int,
    stride: int,
    render: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    initial = {
        "x": initial_xy[0],
        "y": initial_xy[1],
        "vx": velocity[0],
        "vy": velocity[1],
        "deformation": 0.0,
    }
    frame_file = f"{series_id}.npy"
    spec = _base_spec(
        series_id=series_id,
        frame_file=frame_file,
        width=width,
        height=height,
        initial=initial,
        decay=decay,
        event_step=event_step,
        vx_factor=vx_factor,
        vy_factor=vy_factor,
        amplitude=amplitude,
        frequency=frequency,
        phase=phase,
        prediction_steps=prediction_steps,
        stride=stride,
        render=render,
    )
    state_count = max(prediction_steps, math.ceil((spec["frame_count"] - 1) / stride) + 1)
    states = _sequence_states(spec, state_count)
    valid_states = states[1 : prediction_steps + 1]
    spec["expected_state_ranges"] = _expected_ranges(valid_states)

    scene_seed = seed + 1009 * (series_index + 1)
    clean_base = _scene_canvas(width, height, initial, scene_seed)
    base_rng = np.random.default_rng(scene_seed + 17)
    noisy_base = np.clip(
        clean_base.astype(np.int16) + _noise(base_rng, clean_base.shape), 0, 255
    ).astype(np.uint8)

    frames = np.empty((spec["frame_count"], height, width), dtype=np.uint8)
    frames[0] = noisy_base
    for frame_index in range(1, spec["frame_count"]):
        state_step = min(
            state_count,
            max(1, math.ceil(frame_index / spec["timeline"]["stride_frames"])),
        )
        rendered = render_frame(noisy_base, initial, states[state_step], render)
        noise_rng = np.random.default_rng(scene_seed + 7919 * frame_index)
        frames[frame_index] = np.clip(
            rendered.astype(np.int16) + _noise(noise_rng, rendered.shape), 0, 255
        ).astype(np.uint8)

    frame_path = root / frame_file
    frame_bytes = _npy_bytes(frames)
    atomic_write_bytes(frame_path, frame_bytes)
    fingerprint = {
        "series_id": series_id,
        "frame_file": frame_file,
        "shape": [int(part) for part in frames.shape],
        "file_sha256": sha256_bytes(frame_bytes),
        "frame_content_fingerprint": digest_json(
            {
                "schema": "motion-frame-sequence-v1",
                "shape": [int(part) for part in frames.shape],
                "frame_sha256": [frame_fingerprint(frame) for frame in frames],
            }
        ),
        "frame_sha256": [frame_fingerprint(frame) for frame in frames],
    }
    return spec, fingerprint


def generate_samples(output_dir: str | Path, *, seed: int = 20261121) -> dict[str, Any]:
    root = Path(output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    specs: list[dict[str, Any]] = []
    fingerprints: list[dict[str, Any]] = []
    common_render = {
        "translation_gain": 1.0,
        "deformation_scale": 1.0,
        "edge_mode": "clamp",
    }
    configurations = [
        dict(
            series_id="alpha_translate",
            width=64,
            height=48,
            initial_xy=(31.0, 24.0),
            velocity=(1.35, 0.22),
            decay=1.0,
            event_step=7,
            vx_factor=-0.85,
            vy_factor=1.3,
            amplitude=0.045,
            frequency=0.16,
            phase=0.2,
            prediction_steps=16,
            stride=1,
            render={"translation_gain": 1.0, "deformation_scale": 1.0, "edge_mode": "clamp"},
        ),
        dict(
            series_id="beta_deform",
            width=56,
            height=40,
            initial_xy=(27.0, 20.0),
            velocity=(0.92, -0.48),
            decay=0.982,
            event_step=9,
            vx_factor=-1.25,
            vy_factor=-0.72,
            amplitude=0.085,
            frequency=0.19,
            phase=0.55,
            prediction_steps=14,
            stride=2,
            render={"translation_gain": 1.0, "deformation_scale": 1.25, "edge_mode": "clamp"},
        ),
        dict(
            series_id="gamma_turn",
            width=48,
            height=36,
            initial_xy=(24.0, 18.0),
            velocity=(-1.08, 0.34),
            decay=0.994,
            event_step=6,
            vx_factor=-0.65,
            vy_factor=-1.1,
            amplitude=0.06,
            frequency=0.23,
            phase=1.1,
            prediction_steps=12,
            stride=1,
            render={"translation_gain": 0.98, "deformation_scale": 0.9, "edge_mode": "clamp"},
        ),
    ]
    for index, configuration in enumerate(configurations):
        spec, fingerprint = _generate_one(
            root,
            seed=seed,
            series_index=index,
            **configuration,
        )
        specs.append(spec)
        fingerprints.append(fingerprint)

    manifest = {
        "schema_version": 1,
        "seed": seed,
        "model_version": MODEL_VERSION,
        "render_defaults": common_render,
        "series": specs,
    }
    manifest_bytes = pretty_json_bytes(manifest)
    manifest_path = root / "manifest.json"
    atomic_write_bytes(manifest_path, manifest_bytes)
    fingerprint_document = {
        "schema_version": 1,
        "generator": "motion_engine.sample_gen",
        "seed": seed,
        "manifest_file_sha256": sha256_bytes(manifest_bytes),
        "manifest_fingerprint_after_load": sha256_file(manifest_path),
        "series": fingerprints,
    }
    atomic_write_bytes(
        root / "fingerprints.json", pretty_json_bytes(fingerprint_document)
    )
    return fingerprint_document
