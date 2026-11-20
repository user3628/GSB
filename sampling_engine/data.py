"""Deterministic demonstration data generation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

from .canonical import digest_file, digest_json
from .errors import invalid_manifest
from .storage import atomic_write_json


DEFAULT_SEED = 20261121


def _small_vocab() -> list[str]:
    return [
        "的",
        "是",
        "在",
        "中",
        "国",
        "人",
        "了",
        "和",
        "与",
        "模型",
        "推理",
        "采样",
        "令牌",
        "你好",
        "世界",
        "文本",
        "the",
        "of",
        "and",
        "token",
        "model",
        "sample",
        "hello",
        "world",
    ]


def _large_vocab(size: int = 512) -> list[str]:
    base = _small_vocab()
    tokens = list(base)
    index = 0
    while len(tokens) < size:
        if index % 2 == 0:
            candidate = f"中文词{index:04d}"
        else:
            candidate = f"en_word_{index:04d}"
        if candidate not in tokens:
            tokens.append(candidate)
        index += 1
    return tokens[:size]


def _save_logits(path: Path, array: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as handle:
        np.save(handle, np.asarray(array, dtype=np.float64), allow_pickle=False)


def generate_demo_data(data_dir: str | Path, *, seed: int = DEFAULT_SEED) -> dict[str, Any]:
    root = Path(data_dir).resolve()
    logits_dir = root / "logits"
    root.mkdir(parents=True, exist_ok=True)
    logits_dir.mkdir(parents=True, exist_ok=True)
    small = _small_vocab()
    large = _large_vocab(512)
    rng = np.random.default_rng(seed)

    def normal_logits(steps: int, vocab_size: int, scale: float) -> np.ndarray:
        return np.round(rng.normal(0.0, scale, size=(steps, vocab_size)), 6)

    arrays: dict[str, np.ndarray] = {}
    arrays["r2_small"] = normal_logits(7, len(small), 1.2)
    arrays["r2_tie"] = normal_logits(6, len(small), 1.0)
    # Two exactly equal maxima at every step make the token-id tie-break visible.
    arrays["r2_tie"][:, 3] = np.round(4.25 + np.arange(6) * 0.03, 6)
    arrays["r2_tie"][:, 12] = arrays["r2_tie"][:, 3]
    arrays["r2_large"] = normal_logits(10, len(large), 2.4)
    arrays["r2_nan"] = normal_logits(5, len(small), 1.1)
    arrays["r2_nan"][2, 7] = np.nan
    arrays["r2_inf"] = normal_logits(5, len(small), 1.1)
    arrays["r2_inf"][0, 4] = np.inf
    arrays["r1_small"] = normal_logits(8, len(small), 1.3)
    arrays["r1_large"] = normal_logits(6, len(large), 2.0)

    for name, array in arrays.items():
        _save_logits(logits_dir / f"{name}.npy", array)

    manifest_v2 = {
        "manifest_version": 2,
        "vocab": small,
        "metadata": {
            "name": "deterministic-demo-v2",
            "generator_seed": seed,
            "notes": "small v2, maximum ties, large vocab, plus injected NaN and +Inf failures",
        },
        "requests": [
            {
                "request_id": "r2_small",
                "logits_path": "logits/r2_small.npy",
                "temperature": 0.7,
                "top_k": 8,
                "top_p": 0.85,
                "repetition_penalty": 1.15,
                "seed": 101,
                "steps": 7,
                "prompt_tokens": [1, 2],
            },
            {
                "request_id": "r2_tie",
                "logits_path": "logits/r2_tie.npy",
                "temperature": 1.0,
                "top_k": 5,
                "top_p": 0.95,
                "repetition_penalty": 1.0,
                "seed": 202,
                "steps": 6,
                "prompt_tokens": [0],
            },
            {
                "request_id": "r2_large",
                "logits_path": "logits/r2_large.npy",
                "temperature": 0.9,
                "top_k": 50,
                "top_p": 0.92,
                "repetition_penalty": 1.05,
                "seed": 303,
                "steps": 10,
                "prompt_tokens": [4, 9, 19],
            },
            {
                "request_id": "r2_nan",
                "logits_path": "logits/r2_nan.npy",
                "temperature": 0.8,
                "top_k": 10,
                "top_p": 0.9,
                "repetition_penalty": 1.1,
                "seed": 404,
                "steps": 5,
                "prompt_tokens": [0],
            },
            {
                "request_id": "r2_inf",
                "logits_path": "logits/r2_inf.npy",
                "temperature": 1.1,
                "top_k": 7,
                "top_p": 0.88,
                "repetition_penalty": 1.2,
                "seed": 505,
                "steps": 5,
                "prompt_tokens": [3],
            },
        ],
    }
    manifest_v1 = {
        "manifest_version": 1,
        "vocab": small,
        "metadata": {
            "name": "deterministic-demo-v1-legacy",
            "generator_seed": seed,
            "notes": "legacy integer temperature percent and no top_p field",
        },
        "requests": [
            {
                "request_id": "r1_small",
                "logits_path": "logits/r1_small.npy",
                "temp_percent": 80,
                "top_k": 10,
                "repetition_penalty": 1.1,
                "seed": 606,
                "steps": 8,
                "prompt_tokens": [4],
            },
            {
                "request_id": "r1_large",
                "logits_path": "logits/r1_large.npy",
                "temp_percent": 125,
                "top_k": 64,
                "repetition_penalty": 1.03,
                "seed": 707,
                "steps": 6,
                "prompt_tokens": [9, 21],
            },
        ],
    }
    # A manifest declares one vocabulary, so the large-vocabulary requests live
    # in their own v1/v2 files. This also exercises both schema versions.
    manifest_v2_large = {
        "manifest_version": 2,
        "vocab": large,
        "metadata": {
            "name": "deterministic-demo-v2-large-vocab",
            "generator_seed": seed,
        },
        "requests": [manifest_v2["requests"][2]],
    }
    manifest_v2["requests"] = [
        manifest_v2["requests"][0],
        manifest_v2["requests"][1],
        manifest_v2["requests"][3],
        manifest_v2["requests"][4],
    ]
    manifest_v1_large = {
        "manifest_version": 1,
        "vocab": large,
        "metadata": {
            "name": "deterministic-demo-v1-large-vocab",
            "generator_seed": seed,
        },
        "requests": [manifest_v1["requests"][1]],
    }
    manifest_v1["requests"] = [manifest_v1["requests"][0]]
    outputs = {
        "manifest_v2.json": manifest_v2,
        "manifest_v1.json": manifest_v1,
        "manifest_v2_large.json": manifest_v2_large,
        "manifest_v1_large.json": manifest_v1_large,
    }

    for filename, payload in outputs.items():
        atomic_write_json(root / filename, payload)

    generator_metadata = {
        "seed": seed,
        "numpy_rng": "numpy.random.default_rng(seed)",
        "dtypes": "float64 logits, npy",
        "files": {
            name: {
                "path": f"logits/{name}.npy",
                "shape": list(array.shape),
                "sha256": digest_file(logits_dir / f"{name}.npy"),
                "has_nan": bool(np.isnan(array).any()),
                "has_positive_inf": bool(np.isposinf(array).any()),
            }
            for name, array in sorted(arrays.items())
        },
        "manifest_digests": {
            filename: digest_json(payload) for filename, payload in sorted(outputs.items())
        },
        "small_vocab_size": len(small),
        "large_vocab_size": len(large),
        "request_count": sum(
            len(payload["requests"]) for payload in outputs.values()
        ),
    }
    atomic_write_json(root / "generator_metadata.json", generator_metadata)
    if not (root / "manifest_v2.json").exists():
        raise invalid_manifest("failed to write demonstration manifests")
    return generator_metadata


def load_data_summary(data_dir: str | Path = "data") -> dict[str, Any]:
    path = Path(data_dir) / "generator_metadata.json"
    if not path.exists():
        raise invalid_manifest("generator metadata not found", path=str(path))
    return json.loads(path.read_text(encoding="utf-8"))
