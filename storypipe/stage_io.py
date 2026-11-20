from __future__ import annotations

import hashlib
import math
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Iterable

import numpy as np

from .canonical import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_json,
    digest_file,
    digest_paths,
    read_json,
)


def safe_slug(value: str) -> str:
    cleaned = "".join(character if character.isalnum() or character in "-_" else "_" for character in value)
    return cleaned[:80] or "shot"


def float_frame_sha256(frame: np.ndarray) -> str:
    data = np.ascontiguousarray(frame, dtype="<f4").tobytes(order="C")
    return "sha256:" + hashlib.sha256(data).hexdigest()


def float_frame_bytes(frame: np.ndarray) -> bytes:
    return np.ascontiguousarray(frame, dtype="<f4").tobytes(order="C")


def read_float_frame(path: Path, offset: int, frame_bytes: int, width: int, height: int) -> np.ndarray:
    with path.open("rb") as handle:
        handle.seek(offset)
        data = handle.read(frame_bytes)
    if len(data) != frame_bytes:
        raise ValueError("short frame read")
    return np.frombuffer(data, dtype="<f4").reshape(height, width, 3).copy()


def quantize(frame: np.ndarray) -> np.ndarray:
    scaled = np.clip(frame, 0.0, 1.0).astype(np.float64) * 255.0 + 0.5
    return np.floor(scaled).astype(np.uint8)


def frame_stats(quantized: np.ndarray) -> dict[str, Any]:
    raw = np.ascontiguousarray(quantized, dtype=np.uint8).tobytes(order="C")
    means = quantized.astype(np.float64).mean(axis=(0, 1)) / 255.0
    return {
        "pixel_sha256": "sha256:" + hashlib.sha256(raw).hexdigest(),
        "mean_rgb": [round(float(value), 8) for value in means],
    }


def build_tree_digest(paths: Iterable[Path], root: Path) -> str:
    filtered = [path for path in paths if path.is_file()]
    return digest_paths(filtered, root)


def validate_artifact(workspace: Path, artifact_path: str | None, expected_digest: str | None) -> bool:
    if not artifact_path or not expected_digest:
        return False
    path = workspace / artifact_path
    if not path.is_file():
        return False
    return digest_file(path) == expected_digest


def stage_directory(workspace: Path, stage: str) -> Path:
    order = {"schedule": "01_schedule", "compose": "02_compose", "transition": "03_transition", "encode": "04_encode"}
    return workspace / "stages" / order[stage]


def publish_directory(build_dir: Path, output_dir: Path) -> None:
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    backup = output_dir.parent / f".{output_dir.name}.old-{os.getpid()}"
    if backup.exists():
        shutil.rmtree(backup)
    had_output = output_dir.exists()
    if had_output:
        output_dir.replace(backup)
    try:
        build_dir.replace(output_dir)
    except BaseException:
        if had_output and backup.exists() and not output_dir.exists():
            backup.replace(output_dir)
        raise
    if backup.exists():
        shutil.rmtree(backup)


def make_build_dir(parent: Path, prefix: str) -> Path:
    parent.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=prefix, dir=parent))


def write_manifest(path: Path, value: dict[str, Any]) -> str:
    atomic_write_json(path, value)
    return digest_file(path)


def read_text_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        return [read_json_line(line) for line in handle if line.strip()]


def read_json_line(line: str) -> dict[str, Any]:
    import json

    return json.loads(line)


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    data = b"".join(
        canonical_json(record, indent=None).encode("utf-8") + b"\n"
        for record in records
    )
    atomic_write_bytes(path, data)
