from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .utils import utc_now


MODEL_ID = "local-classifier"
KNOWN_VOCABS = ["tok-v1", "preproc-v1"]


def graph(
    *,
    width: int = 6,
    dtype: str = "float32",
    quant_bits: tuple[int, int, int] = (32, 32, 32),
) -> dict[str, Any]:
    return {
        "inputs": [{"name": "input", "shape": [2, 4], "dtype": dtype}],
        "nodes": [
            {
                "id": "linear",
                "op": "linear",
                "inputs": [
                    {"name": "x", "shape": [2, 4], "dtype": dtype},
                    {"name": "weight", "shape": [4, width], "dtype": dtype},
                ],
                "outputs": [{"name": "hidden", "shape": [2, width], "dtype": dtype}],
                "quant_bits": quant_bits[0],
                "preprocess": ["tokenize:tok-v1", "normalize:preproc-v1"],
                "postprocess": ["denormalize:preproc-v1"],
            },
            {
                "id": "norm",
                "op": "layernorm",
                "inputs": [{"name": "hidden", "shape": [2, width], "dtype": dtype}],
                "outputs": [{"name": "normalized", "shape": [2, width], "dtype": dtype}],
                "quant_bits": quant_bits[1],
                "preprocess": [],
                "postprocess": [],
            },
            {
                "id": "act",
                "op": "relu",
                "inputs": [{"name": "normalized", "shape": [2, width], "dtype": dtype}],
                "outputs": [{"name": "logits", "shape": [2, width], "dtype": dtype}],
                "quant_bits": quant_bits[2],
                "preprocess": [],
                "postprocess": ["topk:postproc-v1"],
            },
        ],
        "outputs": ["act"],
        "vocab_refs": ["tok-v1"],
    }


def v1_graph() -> dict[str, Any]:
    return graph(width=6, dtype="float32", quant_bits=(32, 32, 32))


def v2_quant_graph() -> dict[str, Any]:
    return graph(width=6, dtype="float32", quant_bits=(16, 8, 8))


def v3_shape_graph() -> dict[str, Any]:
    return graph(width=8, dtype="float32", quant_bits=(16, 8, 8))


def v4_dtype_graph() -> dict[str, Any]:
    return graph(width=8, dtype="float64", quant_bits=(16, 8, 8))


def race_graph() -> dict[str, Any]:
    return v3_shape_graph()


def initial_snapshot(generated_at: str | None = None, snapshot_seq: int = 1) -> dict[str, Any]:
    return {
        "snapshot_seq": snapshot_seq,
        "generated_at": generated_at or utc_now(),
        "known_vocabs": list(KNOWN_VOCABS),
        "models": [
            {
                "model_id": MODEL_ID,
                "version": "v1",
                "graph": v1_graph(),
            }
        ],
        "active_models": {
            MODEL_ID: {
                "version": "v1",
                "previous_version": "v1",
                "gray_ratio": 1.0,
            }
        },
    }


def quant_increment(
    base_version: str,
    snapshot_seq: int = 2,
    generated_at: str | None = None,
) -> dict[str, Any]:
    return {
        "base_version": base_version,
        "snapshot_seq": snapshot_seq,
        "generated_at": generated_at or utc_now(),
        "models": [
            {
                "model_id": MODEL_ID,
                "version": "v2-quant",
                "graph": v2_quant_graph(),
            }
        ],
        "active_models": {
            MODEL_ID: {
                "version": "v2-quant",
                "previous_version": "v1",
                "gray_ratio": 1.0,
            }
        },
    }


def shape_increment(
    base_version: str,
    snapshot_seq: int = 3,
    generated_at: str | None = None,
) -> dict[str, Any]:
    return {
        "base_version": base_version,
        "snapshot_seq": snapshot_seq,
        "generated_at": generated_at or utc_now(),
        "models": [
            {
                "model_id": MODEL_ID,
                "version": "v3-shape",
                "graph": v3_shape_graph(),
            }
        ],
        "active_models": {
            MODEL_ID: {
                "version": "v3-shape",
                "previous_version": "v2-quant",
                "gray_ratio": 0.5,
            }
        },
    }


def dtype_increment(
    base_version: str,
    snapshot_seq: int = 4,
    generated_at: str | None = None,
) -> dict[str, Any]:
    return {
        "base_version": base_version,
        "snapshot_seq": snapshot_seq,
        "generated_at": generated_at or utc_now(),
        "models": [
            {
                "model_id": MODEL_ID,
                "version": "v4-dtype",
                "graph": v4_dtype_graph(),
            }
        ],
        "active_models": {
            MODEL_ID: {
                "version": "v4-dtype",
                "previous_version": "v3-shape",
                "gray_ratio": 0,
            }
        },
    }


def race_increment(
    base_version: str,
    version: str,
    snapshot_seq: int,
    generated_at: str | None = None,
) -> dict[str, Any]:
    return {
        "base_version": base_version,
        "snapshot_seq": snapshot_seq,
        "generated_at": generated_at or utc_now(),
        "models": [
            {
                "model_id": MODEL_ID,
                "version": version,
                "graph": race_graph(),
            }
        ],
        "active_models": {
            MODEL_ID: {
                "version": version,
                "previous_version": "v3-shape",
                "gray_ratio": 0,
            }
        },
    }


def write_snapshot_file(path: str | Path, payload: dict[str, Any]) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return target


def read_snapshot_file(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))

