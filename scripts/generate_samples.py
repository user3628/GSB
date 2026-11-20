#!/usr/bin/env python3
"""Generate the deterministic local matrix corpus.

This script intentionally uses only Python's standard library and NumPy.  It
records shapes, transpose flags, generation seeds, anomaly injection positions,
and SHA-256 digests beside the binary ``.npy`` files.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

import numpy as np


DEFAULT_SEED = 20261120


SAMPLE_DEFINITIONS: list[dict[str, Any]] = [
    {
        "name": "small_64x64x64",
        "family": "small",
        "batch": 1,
        "m": 64,
        "n": 64,
        "k": 64,
        "trans_a": "N",
        "trans_b": "N",
    },
    {
        "name": "medium_512x256x384",
        "family": "medium",
        "batch": 1,
        "m": 512,
        "n": 256,
        "k": 384,
        "trans_a": "N",
        "trans_b": "N",
    },
    {
        "name": "large_1024x1024x1024",
        "family": "large",
        "batch": 1,
        "m": 1024,
        "n": 1024,
        "k": 1024,
        "trans_a": "N",
        "trans_b": "N",
    },
    {
        "name": "nonsquare_128x96x80",
        "family": "nonsquare",
        "batch": 1,
        "m": 128,
        "n": 96,
        "k": 80,
        "trans_a": "N",
        "trans_b": "N",
    },
    {
        "name": "trans_a_96x128x64",
        "family": "transpose",
        "batch": 1,
        "m": 96,
        "n": 128,
        "k": 64,
        "trans_a": "T",
        "trans_b": "N",
    },
    {
        "name": "trans_b_80x64x96",
        "family": "transpose",
        "batch": 1,
        "m": 80,
        "n": 64,
        "k": 96,
        "trans_a": "N",
        "trans_b": "T",
    },
    {
        "name": "trans_ab_72x88x64",
        "family": "transpose",
        "batch": 1,
        "m": 72,
        "n": 88,
        "k": 64,
        "trans_a": "T",
        "trans_b": "T",
    },
    {
        "name": "batch_4x32x24x40",
        "family": "batch",
        "batch": 4,
        "m": 32,
        "n": 24,
        "k": 40,
        "trans_a": "N",
        "trans_b": "N",
    },
    {
        "name": "batch_trans_3x17x29x23",
        "family": "batch_transpose",
        "batch": 3,
        "m": 17,
        "n": 29,
        "k": 23,
        "trans_a": "T",
        "trans_b": "T",
    },
    {
        "name": "anomaly_nan_inf_48x40x36",
        "family": "anomaly",
        "batch": 1,
        "m": 48,
        "n": 40,
        "k": 36,
        "trans_a": "N",
        "trans_b": "N",
        "anomaly_injections": {
            "A": [(0, 0, "nan"), (3, 4, "pos_inf"), (7, 8, "neg_inf")],
            "B": [(1, 2, "nan"), (4, 5, "pos_inf")],
        },
    },
    {
        "name": "batch_anomaly_2x12x10x8",
        "family": "batch_anomaly",
        "batch": 2,
        "m": 12,
        "n": 10,
        "k": 8,
        "trans_a": "N",
        "trans_b": "N",
        "anomaly_injections": {
            "A": [(0, 1, 2, "nan"), (1, 4, 5, "neg_inf")],
            "B": [(0, 3, 1, "pos_inf")],
        },
    },
]


ANOMALY_VALUES = {
    "nan": float("nan"),
    "pos_inf": float("inf"),
    "neg_inf": float("-inf"),
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_npy_save(path: Path, array: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".npy.tmp", dir=str(path.parent)
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            np.save(handle, array, allow_pickle=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _operand_shapes(definition: dict[str, Any]) -> tuple[tuple[int, ...], tuple[int, ...]]:
    batch = int(definition["batch"])
    m, n, k = int(definition["m"]), int(definition["n"]), int(definition["k"])
    a_shape: tuple[int, ...] = (k, m) if definition["trans_a"] == "T" else (m, k)
    b_shape: tuple[int, ...] = (n, k) if definition["trans_b"] == "T" else (k, n)
    if batch > 1:
        return (batch, *a_shape), (batch, *b_shape)
    return a_shape, b_shape


def _inject(array: np.ndarray, injections: list[tuple[Any, ...]]) -> None:
    for location in injections:
        value_kind = str(location[-1])
        index = tuple(int(value) for value in location[:-1])
        array[index] = ANOMALY_VALUES[value_kind]


def generate(output_root: Path, *, seed: int = DEFAULT_SEED) -> dict[str, Any]:
    samples_dir = output_root / "samples"
    samples_dir.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, Any]] = []
    for index, definition in enumerate(SAMPLE_DEFINITIONS):
        sample_seed = seed + index * 1009
        rng = np.random.default_rng(sample_seed)
        a_shape, b_shape = _operand_shapes(definition)
        a = rng.standard_normal(a_shape, dtype=np.float64)
        b = rng.standard_normal(b_shape, dtype=np.float64)
        injections = definition.get("anomaly_injections", {})
        if injections.get("A"):
            _inject(a, injections["A"])
        if injections.get("B"):
            _inject(b, injections["B"])
        a_name = f"{definition['name']}_a.npy"
        b_name = f"{definition['name']}_b.npy"
        a_path = samples_dir / a_name
        b_path = samples_dir / b_name
        _atomic_npy_save(a_path, a)
        _atomic_npy_save(b_path, b)
        mode = "bmm" if int(definition["batch"]) > 1 else "gemm"
        records.append(
            {
                "name": definition["name"],
                "family": definition["family"],
                "mode": mode,
                "batch": int(definition["batch"]),
                "m": int(definition["m"]),
                "n": int(definition["n"]),
                "k": int(definition["k"]),
                "trans_a": definition["trans_a"],
                "trans_b": definition["trans_b"],
                "a_shape": list(a.shape),
                "b_shape": list(b.shape),
                "a_file": f"samples/{a_name}",
                "b_file": f"samples/{b_name}",
                "a_sha256": _sha256(a_path),
                "b_sha256": _sha256(b_path),
                "sample_seed": sample_seed,
                "dtype": "float64",
                "contains_nonfinite": bool(injections),
                "anomaly_injections": injections,
            }
        )
    manifest = {
        "schema_version": 1,
        "generator": "scripts/generate_samples.py",
        "base_seed": seed,
        "generation_method": "numpy.random.default_rng(seed).standard_normal(shape, dtype=float64)",
        "numpy_version": np.__version__,
        "endianness": "little" if np.little_endian else "big",
        "samples": records,
    }
    manifest_path = output_root / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    shape_list = {
        "schema_version": 1,
        "samples": [
            {
                "name": record["name"],
                "family": record["family"],
                "mode": record["mode"],
                "batch": record["batch"],
                "logical_shape": [record["m"], record["n"], record["k"]],
                "a_shape": record["a_shape"],
                "b_shape": record["b_shape"],
                "trans_a": record["trans_a"],
                "trans_b": record["trans_b"],
                "contains_nonfinite": record["contains_nonfinite"],
            }
            for record in records
        ],
    }
    (output_root / "shape_list.json").write_text(
        json.dumps(shape_list, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    lines = [
        "# Deterministic sample shape list",
        "",
        f"Base seed: `{seed}`; dtype: `float64`; generator: `scripts/generate_samples.py`.",
        "",
        "| name | mode | logical M,N,K | batch | A shape | B shape | flags | nonfinite |",
        "|---|---|---:|---:|---|---|---|---|",
    ]
    for record in records:
        lines.append(
            "| {name} | {mode} | {m}x{n}x{k} | {batch} | {a_shape} | {b_shape} | {ta}/{tb} | {bad} |".format(
                name=record["name"],
                mode=record["mode"],
                m=record["m"],
                n=record["n"],
                k=record["k"],
                batch=record["batch"],
                a_shape=record["a_shape"],
                b_shape=record["b_shape"],
                ta=record["trans_a"],
                tb=record["trans_b"],
                bad="yes" if record["contains_nonfinite"] else "no",
            )
        )
    (output_root / "SHAPES.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data"))
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args()
    manifest = generate(args.output, seed=args.seed)
    print(
        json.dumps(
            {
                "ok": True,
                "output": str(args.output),
                "samples": len(manifest["samples"]),
                "base_seed": manifest["base_seed"],
                "numpy_version": manifest["numpy_version"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

