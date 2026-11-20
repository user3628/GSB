from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from sampling_engine.config import EngineConfig


SMALL_VOCAB = ["的", "是", "在", "the", "token", "model", "sample", "world"]


def write_logits(path: Path, array: np.ndarray) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as handle:
        np.save(handle, np.asarray(array, dtype=np.float64), allow_pickle=False)
    return path


def request_entry(
    *,
    request_id: str = "req_a",
    logits_path: str,
    temperature: float = 0.8,
    top_k: int | None = 4,
    top_p: float = 0.9,
    repetition_penalty: float = 1.1,
    seed: int = 123,
    steps: int = 3,
    prompt_tokens: list[int] | None = None,
) -> dict[str, Any]:
    return {
        "request_id": request_id,
        "logits_path": logits_path,
        "temperature": temperature,
        "top_k": top_k,
        "top_p": top_p,
        "repetition_penalty": repetition_penalty,
        "seed": seed,
        "steps": steps,
        "prompt_tokens": prompt_tokens or [],
    }


def write_v2_manifest(
    path: Path,
    logits_path: Path,
    *,
    requests: list[dict[str, Any]] | None = None,
    vocab: list[str] | None = None,
) -> Path:
    payload = {
        "manifest_version": 2,
        "vocab": vocab or SMALL_VOCAB,
        "requests": requests
        or [request_entry(logits_path=str(logits_path))],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


@pytest.fixture
def simple_matrix() -> np.ndarray:
    return np.array(
        [
            [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
            [0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1],
            [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        ],
        dtype=np.float64,
    )


@pytest.fixture
def manifest_file(tmp_path: Path, simple_matrix: np.ndarray) -> Path:
    logits = write_logits(tmp_path / "logits.npy", simple_matrix)
    return write_v2_manifest(tmp_path / "manifest.json", logits)


@pytest.fixture
def engine_config() -> EngineConfig:
    return EngineConfig(max_temperature=4.0, v1_default_top_p=0.91)
