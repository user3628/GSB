#!/usr/bin/env python3
"""Create deterministic local NPZ shards and their shape manifest.

The generator uses only NumPy, a fixed seed, fixed ZIP timestamps, and no
network/dataset access. Running it twice produces byte-identical NPZ files.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import zipfile
from pathlib import Path
from typing import Any

import numpy as np

SEED = 20260217


def npy_bytes(array: np.ndarray) -> bytes:
    buffer = io.BytesIO()
    np.lib.format.write_array(buffer, np.ascontiguousarray(array), allow_pickle=False)
    return buffer.getvalue()


def write_deterministic_npz(path: Path, array: np.ndarray, *, key: str = "tensor") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        info = zipfile.ZipInfo(f"{key}.npy", date_time=(1980, 1, 1, 0, 0, 0))
        info.compress_type = zipfile.ZIP_DEFLATED
        info.external_attr = 0o644 << 16
        archive.writestr(info, npy_bytes(array), compress_type=zipfile.ZIP_DEFLATED)


def array_digest(array: np.ndarray) -> str:
    header = f"{array.dtype.str}|{','.join(str(dim) for dim in array.shape)}|".encode("ascii")
    return hashlib.sha256(header + np.ascontiguousarray(array).tobytes(order="C")).hexdigest()


def make_4d_gelu_swap(rng: np.random.Generator) -> np.ndarray:
    array = rng.normal(0.0, 0.85, size=(2, 3, 7, 5))
    array[0, 0, 0, 0] = 1.0e4
    array[0, 1, 1, 1] = -1.0e4
    array[1, 2, 6, 4] = 1.0e4
    array[1, 0, 3, 2] = -1.0e4
    # Make every row non-zero while retaining isolated exact zeros.
    for row in range(array.shape[0] * array.shape[1] * array.shape[2]):
        matrix = array.reshape(-1, array.shape[-1])
        if np.all(matrix[row] == 0.0):
            matrix[row, 0] = 0.125
    return array.astype(np.float64)


def make_2d_silu_tile(rng: np.random.Generator) -> np.ndarray:
    array = rng.normal(0.0, 1.25, size=(9, 11))
    array[1, 10] = 1.0e4
    array[7, 3] = -1.0e4
    array[4, 5] = 0.0
    return array.astype(np.float64)


def make_4d_sigmoid_tile(rng: np.random.Generator) -> np.ndarray:
    array = rng.normal(0.0, 1.1, size=(2, 3, 6, 7))
    array[0, 1, 2, 6] = 1.0e4
    array[1, 2, 0, 0] = -1.0e4
    return array.astype(np.float64)


def make_4d_gelu_diag(rng: np.random.Generator) -> np.ndarray:
    array = rng.normal(0.0, 0.7, size=(2, 2, 5, 3))
    array[0, 0, 1, 1] = np.nan
    array[0, 1, 2, 2] = np.inf
    array[1, 1, 4, 0] = -np.inf
    array[1, 0, 2, :] = 0.0
    array[0, 1, 4, 0] = 1.0e4
    array[1, 1, 0, 2] = -1.0e4
    return array.astype(np.float64)


def manifest_entry(
    *,
    sample_id: str,
    path: str,
    array: np.ndarray,
    target_shape: tuple[int, ...],
    layout: str,
    config: str,
    anomalies: list[str],
    notes: str,
) -> dict[str, Any]:
    return {
        "id": sample_id,
        "path": path,
        "layout": layout,
        "shape": [int(dim) for dim in array.shape],
        "target_shape": [int(dim) for dim in target_shape],
        "dtype": "float64",
        "seed": SEED,
        "config": config,
        "anomalies": anomalies,
        "odd_columns": int(array.shape[-1]) % 2 == 1,
        "non_divisible_tail": True,
        "content_digest": array_digest(array),
        "notes": notes,
    }


def generate(out_dir: Path) -> dict[str, Any]:
    rng = np.random.default_rng(SEED)
    samples = [
        (
            "tensor_4d_gelu_swap",
            make_4d_gelu_swap(rng),
            (2, 7, 3, 5),
            "4d_bhsd",
            "configs/gelu_swap.yaml",
            ["extreme_mixed_sign", "odd_head_dim"],
            "4-D [B,H,S,D], head/sequence swap, odd D=5, ragged blocks",
        ),
        (
            "matrix_silu_tile",
            make_2d_silu_tile(rng),
            (9, 11),
            "2d",
            "configs/silu_tile.yaml",
            ["extreme_mixed_sign", "odd_columns"],
            "2-D matrix, tile-grid reorder, 9x11, non-divisible row/col blocks",
        ),
        (
            "tensor_4d_sigmoid_tile",
            make_4d_sigmoid_tile(rng),
            (2, 3, 6, 7),
            "4d_bhsd",
            "configs/sigmoid_tile.yaml",
            ["extreme_mixed_sign", "odd_head_dim"],
            "4-D sigmoid tile mode, odd D=7, tail tiles",
        ),
        (
            "tensor_4d_gelu_diag",
            make_4d_gelu_diag(rng),
            (2, 5, 2, 3),
            "4d_bhsd",
            "configs/gelu_swap_diag.yaml",
            ["nan", "positive_inf", "negative_inf", "all_zero_row", "extreme_mixed_sign"],
            "Diagnostic shard: NaN/Inf and an all-zero row must surface E_NUMERIC",
        ),
    ]
    entries: list[dict[str, Any]] = []
    for sample_id, array, target_shape, layout, config, anomalies, notes in samples:
        relative = f"samples/{sample_id}.npz"
        write_deterministic_npz(out_dir / f"{sample_id}.npz", array)
        entries.append(
            manifest_entry(
                sample_id=sample_id,
                path=relative,
                array=array,
                target_shape=target_shape,
                layout=layout,
                config=config,
                anomalies=anomalies,
                notes=notes,
            )
        )
    manifest = {
        "schema": "blocklab.shape-manifest.v1",
        "seed": SEED,
        "generator": "samples/generator.py",
        "shards": entries,
    }
    manifest_path = out_dir / "shape_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate deterministic BlockLab samples")
    parser.add_argument("--out", default="samples", help="output directory (inside this repo)")
    args = parser.parse_args()
    root = Path.cwd().resolve()
    out_dir = Path(args.out)
    if not out_dir.is_absolute():
        out_dir = root / out_dir
    out_dir = out_dir.resolve()
    try:
        out_dir.relative_to(root)
    except ValueError as exc:
        raise SystemExit(f"output directory must remain inside {root}") from exc
    manifest = generate(out_dir)
    print(json.dumps({"ok": True, "seed": SEED, "shards": len(manifest["shards"])}, sort_keys=True))


if __name__ == "__main__":
    main()

