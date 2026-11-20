from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

import numpy as np

from .model import BOOL, FLOAT64, INT64, INT64_MAX, INT64_MIN


DEFAULT_SEED = 20_261_120
DEFAULT_SIZES: dict[str, int] = {
    "small": 5_000,
    "medium": 250_000,
    "large": 2_000_000,
}
COLUMN_SPECS = (
    ("id", INT64),
    ("i64_a", INT64),
    ("i64_b", INT64),
    ("f64_a", FLOAT64),
    ("f64_b", FLOAT64),
    ("flag", BOOL),
)


def generate_samples(
    data_dir: str | Path,
    *,
    seed: int = DEFAULT_SEED,
    sizes: dict[str, int] | None = None,
    force: bool = False,
) -> dict[str, Any]:
    data_dir = Path(data_dir).resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    sizes = dict(sizes or DEFAULT_SIZES)
    manifest_path = data_dir / "manifest.json"
    if manifest_path.exists() and not force:
        try:
            with manifest_path.open("r", encoding="utf-8") as handle:
                existing = json.load(handle)
            if all((data_dir / item["file"]).exists() for item in existing.get("datasets", {}).values()):
                return existing
        except (OSError, ValueError, KeyError):
            pass

    manifest: dict[str, Any] = {
        "version": 1,
        "seed": seed,
        "null_bitmap_convention": "true means value is valid; false means null",
        "datasets": {},
    }
    for tier_index, (tier, rows) in enumerate(sizes.items()):
        if rows < 16:
            raise ValueError(f"dataset {tier!r} must contain at least 16 rows")
        arrays = _build_dataset(rows, seed + tier_index * 1_000_003)
        file_name = f"{tier}.npz"
        archive_path = data_dir / file_name
        temp_path = archive_path.with_suffix(".tmp.npz")
        np.savez(
            temp_path,
            **{
                f"{name}__values": column["values"]
                for name, column in arrays.items()
            },
            **{f"{name}__valid": column["valid"] for name, column in arrays.items()},
        )
        temp_path.replace(archive_path)
        manifest["datasets"][tier] = {
            "rows": rows,
            "file": file_name,
            "columns": [{"name": name, "dtype": dtype} for name, dtype in COLUMN_SPECS],
            "null_counts": {name: int((~column["valid"]).sum()) for name, column in arrays.items()},
        }
    temp_manifest = manifest_path.with_suffix(".tmp.json")
    with temp_manifest.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    temp_manifest.replace(manifest_path)
    return manifest


def _build_dataset(rows: int, seed: int) -> dict[str, dict[str, np.ndarray]]:
    rng = np.random.default_rng(seed)
    identifiers = np.arange(rows, dtype=np.int64)
    i64_a = rng.integers(-1_000_000, 1_000_001, size=rows, dtype=np.int64)
    i64_b = rng.integers(-10_000, 10_001, size=rows, dtype=np.int64)
    f64_a = rng.normal(0.0, 10_000.0, size=rows).astype(np.float64)
    f64_b = rng.normal(0.0, 100.0, size=rows).astype(np.float64)
    flag = rng.integers(0, 2, size=rows, dtype=np.int8).astype(np.bool_)

    valid = {
        "id": np.ones(rows, dtype=np.bool_),
        "i64_a": np.ones(rows, dtype=np.bool_),
        "i64_b": np.ones(rows, dtype=np.bool_),
        "f64_a": np.ones(rows, dtype=np.bool_),
        "f64_b": np.ones(rows, dtype=np.bool_),
        "flag": np.ones(rows, dtype=np.bool_),
    }
    valid["id"][1009::1009] = False
    valid["i64_a"][997::997] = False
    valid["i64_b"][991::991] = False
    valid["f64_a"][1009::1009] = False
    valid["f64_b"][1011::1011] = False
    valid["flag"][991::991] = False

    # Deterministic boundary rows for null, overflow, comparison and division cases.
    identifiers[:15] = np.arange(15, dtype=np.int64)
    _assign(i64_a, 0, INT64_MAX)
    _assign(i64_a, 1, INT64_MIN)
    _assign(i64_a, 2, INT64_MAX - 1)
    _assign(i64_a, 3, INT64_MIN + 1)
    _assign(i64_a, 4, 3_037_000_500)
    _assign(i64_a, 5, -3_037_000_500)
    _assign(i64_a, 6, 1)
    _assign(i64_a, 7, -1)

    _assign(i64_b, 0, 1)
    _assign(i64_b, 1, -1)
    _assign(i64_b, 2, 1)
    _assign(i64_b, 3, -1)
    _assign(i64_b, 4, 3_037_000_500)
    _assign(i64_b, 5, -3_037_000_500)
    _assign(i64_b, 6, 0)
    _assign(i64_b, 7, 2)

    _assign(f64_a, 0, 1e300)
    _assign(f64_a, 1, 1.0)
    _assign(f64_a, 2, -1.0)
    _assign(f64_a, 3, 0.0)
    _assign(f64_a, 4, 1e-300)
    _assign(f64_a, 5, -1e-300)
    _assign(f64_a, 6, math.pi)
    _assign(f64_a, 7, 2.0)

    _assign(f64_b, 0, 1e10)
    _assign(f64_b, 1, 0.0)
    _assign(f64_b, 2, -0.0)
    _assign(f64_b, 3, 1.0)
    _assign(f64_b, 4, -1.0)
    _assign(f64_b, 5, 3.0)
    _assign(f64_b, 6, 2.0)
    _assign(f64_b, 7, 1.0)

    flag[:8] = np.array([True, False, True, False, True, False, True, False], dtype=np.bool_)
    # Explicit scattered nulls in the early rows exercise null arithmetic,
    # comparison, logical and conditional semantics.
    valid["f64_a"][9] = False
    valid["f64_b"][10] = False
    valid["flag"][11] = False
    valid["i64_a"][12] = False
    valid["i64_b"][13] = False
    valid["id"][14] = False

    return {
        "id": {"values": identifiers, "valid": valid["id"]},
        "i64_a": {"values": i64_a, "valid": valid["i64_a"]},
        "i64_b": {"values": i64_b, "valid": valid["i64_b"]},
        "f64_a": {"values": f64_a, "valid": valid["f64_a"]},
        "f64_b": {"values": f64_b, "valid": valid["f64_b"]},
        "flag": {"values": flag, "valid": valid["flag"]},
    }


def _assign(array: np.ndarray, index: int, value: Any) -> None:
    if index < array.shape[0]:
        array[index] = value


def parse_size_overrides(values: list[str] | None) -> dict[str, int] | None:
    if not values:
        return None
    result = dict(DEFAULT_SIZES)
    for item in values:
        name, separator, raw_rows = item.partition("=")
        if not separator or not name:
            raise argparse.ArgumentTypeError("size overrides must be NAME=ROWS")
        rows = int(raw_rows)
        if rows < 0:
            raise argparse.ArgumentTypeError("row counts must be non-negative")
        result[name] = rows
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate deterministic local columnar samples")
    parser.add_argument("--data-dir", default="data/samples")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--rows", action="append", metavar="TIER=ROWS")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    sizes = parse_size_overrides(args.rows)
    manifest = generate_samples(args.data_dir, seed=args.seed, sizes=sizes, force=args.force)
    print(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
