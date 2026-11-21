from __future__ import annotations

import argparse
import random
import shutil
from pathlib import Path
from typing import Any

from .codec import write_block_file
from .snapshot import SNAPSHOT_FORMAT
from .utils import atomic_write_json, sha256_hex, canonical_json_bytes


REGIONS = ("north", "south", "east", "west", "central", "northeast", "southwest")
LABELS = (
    "alpha",
    "bravo",
    "charlie",
    "delta",
    "echo",
    "foxtrot",
    "golf",
    "hotel",
    "india",
    "juliet",
    "kilo",
    "lima",
)


def _column_values(rows: int, seed: int) -> dict[str, tuple[str, str, bool, list[Any]]]:
    rng = random.Random(seed)
    ids: list[int] = []
    regions: list[str | None] = []
    shards: list[int | None] = []
    scores: list[float | None] = []
    labels: list[str | None] = []
    for index in range(rows):
        identifier = index + 1
        ids.append(identifier)
        region: str | None = REGIONS[rng.randrange(len(REGIONS))]
        if identifier % 97 == 0:
            region = None
        regions.append(region)
        shard: int | None = (index // 128) % 37
        if identifier % 211 == 0:
            shard = None
        shards.append(shard)
        score: float | None = round(rng.uniform(-1000.0, 1000.0), 9)
        if identifier % 89 == 0:
            score = None
        scores.append(score)
        label: str | None = LABELS[rng.randrange(len(LABELS))]
        if identifier % 113 == 0:
            label = None
        labels.append(label)
    return {
        "id": ("int64", "fixed", False, ids),
        "region": ("string", "dict", True, regions),
        "shard": ("int64", "run", True, shards),
        "score": ("float64", "fixed", True, scores),
        "label": ("string", "dict", True, labels),
    }


def _corrupt_block(path: Path) -> dict[str, Any]:
    size = path.stat().st_size
    if size <= 1:
        raise ValueError("cannot corrupt an empty block")
    with path.open("r+b") as handle:
        handle.seek(size - 1)
        original = handle.read(1)
        mutated = bytes([original[0] ^ 0x01])
        handle.seek(size - 1)
        handle.write(mutated)
        handle.flush()
    return {
        "file": path.name,
        "byte_offset": size - 1,
        "original_byte": original[0],
        "mutated_byte": mutated[0],
    }


def generate_snapshot(
    data_dir: str | Path,
    *,
    version: int,
    rows: int,
    block_size: int = 4096,
    seed: int = 20261122,
    generated_at: str = "2026-01-01T00:00:00.000000Z",
    corrupt_column: str | None = None,
    corrupt_block_no: int | None = None,
    replace: bool = True,
) -> dict[str, Any]:
    if version <= 0:
        raise ValueError("version must be positive")
    if rows <= 0:
        raise ValueError("rows must be positive")
    if block_size <= 0:
        raise ValueError("block_size must be positive")
    root = Path(data_dir).resolve() / "snapshots" / f"v{version:04d}"
    if root.exists():
        if not replace:
            raise FileExistsError(root)
        shutil.rmtree(root)
    (root / "blocks").mkdir(parents=True)

    generated = _column_values(rows, seed + version * 1_000_003)
    column_manifests: list[dict[str, Any]] = []
    encoding_counts = {"fixed": 0, "run": 0, "dict": 0}
    total_encoded_bytes = 0
    block_count = 0
    corrupt_info: dict[str, Any] | None = None

    for column_name, (dtype, encoding, nullable, all_values) in generated.items():
        blocks: list[dict[str, Any]] = []
        for block_no, start in enumerate(range(0, rows, block_size)):
            end = min(rows, start + block_size)
            values = all_values[start:end]
            ids = generated["id"][3][start:end]
            null_mask = [value is None for value in values]
            file_name = f"blocks/{column_name}_{block_no:08d}.bin"
            meta = write_block_file(
                root / file_name,
                block_no=block_no,
                column=column_name,
                dtype=dtype,
                encoding=encoding,
                row_offset=start,
                key_min=int(ids[0]),
                key_max=int(ids[-1]),
                values=values,
                null_mask=null_mask,
            )
            block_entry = {
                "block_no": block_no,
                "column": column_name,
                "dtype": dtype,
                "encoding": encoding,
                "row_offset": start,
                "row_count": len(values),
                "original_rows": len(values),
                "key_min": int(ids[0]),
                "key_max": int(ids[-1]),
                "file": file_name,
                "decompressed_bytes": meta["decompressed_bytes"],
                "encoded_bytes": meta["encoded_bytes"],
                "null_bitmap_len": meta["null_bitmap_len"],
                "null_count": meta["null_count"],
                "body_len": meta["body_len"],
                "checksum": meta["checksum"],
            }
            blocks.append(block_entry)
            encoding_counts[encoding] += 1
            total_encoded_bytes += meta["encoded_bytes"]
            block_count += 1
            if corrupt_column == column_name and corrupt_block_no == block_no:
                corrupt_info = {
                    "column": column_name,
                    "block_no": block_no,
                    **_corrupt_block(root / file_name),
                }
        column_manifests.append(
            {
                "name": column_name,
                "dtype": dtype,
                "nullable": nullable,
                "encoding": encoding,
                "null_bitmap": {"encoding": "bitmap", "bit_order": "lsb0", "null_is_one": True},
                "blocks": blocks,
                "statistics": {
                    "null_count": sum(block["null_count"] for block in blocks),
                    "block_count": len(blocks),
                },
            }
        )

    if corrupt_column is not None and corrupt_info is None:
        raise ValueError("requested corrupt block was not produced")
    payload = {
        "format": SNAPSHOT_FORMAT,
        "version": version,
        "generated_at": generated_at,
        "row_count": rows,
        "primary_key": "id",
        "block_size": block_size,
        "seed": seed,
        "columns": column_manifests,
        "summary": {
            "block_count": block_count,
            "encoding_counts": encoding_counts,
            "total_encoded_bytes": total_encoded_bytes,
            "corrupt_block_injected": corrupt_info is not None,
        },
    }
    digest = sha256_hex(canonical_json_bytes(payload))
    manifest = {**payload, "digest": digest}
    atomic_write_json(root / "manifest.json", manifest)
    return {
        "root": str(root),
        "version": version,
        "rows": rows,
        "block_size": block_size,
        "digest": digest,
        "block_count": block_count,
        "encoding_counts": encoding_counts,
        "corrupt_info": corrupt_info,
    }


def generate_query_samples(data_dir: str | Path, *, corrupt_key: int = 768) -> dict[str, Any]:
    query_dir = Path(data_dir).resolve() / "queries"
    query_dir.mkdir(parents=True, exist_ok=True)
    small = {
        "description": "three adjacent keys in the first small-snapshot block",
        "keys": [1, 2, 3],
        "columns": ["id", "region", "shard", "score", "label"],
        "page_size": 2,
    }
    large_keys = sorted(set([1, 4999, 5000] + list(range(1000, 120001, 900))))
    large = {
        "description": "spread across many large-snapshot blocks",
        "keys": large_keys,
        "columns": ["id", "region", "shard", "score", "label"],
        "page_size": 64,
    }
    corrupt = {
        "description": "the key falls into the deliberately corrupted block 1",
        "keys": [corrupt_key],
        "columns": ["id", "label"],
        "page_size": 10,
    }
    files = {"keys_small.json": small, "keys_large.json": large, "keys_corrupt.json": corrupt}
    for name, value in files.items():
        atomic_write_json(query_dir / name, value)
    return {"directory": str(query_dir), "files": sorted(files), "corrupt_key": corrupt_key}


def generate_suite(
    data_dir: str | Path,
    *,
    small_rows: int = 5000,
    large_rows: int = 150000,
    corrupt_rows: int = 5000,
    small_block_size: int = 512,
    large_block_size: int = 16384,
    corrupt_block_size: int = 512,
    seed: int = 20261122,
    replace: bool = True,
) -> dict[str, Any]:
    data_path = Path(data_dir).resolve()
    data_path.mkdir(parents=True, exist_ok=True)
    corrupt_key = corrupt_block_size + max(1, corrupt_block_size // 2)
    return {
        "v1_small": generate_snapshot(
            data_path,
            version=1,
            rows=small_rows,
            block_size=small_block_size,
            seed=seed,
            generated_at="2026-01-01T00:00:00.000000Z",
            replace=replace,
        ),
        "v2_large": generate_snapshot(
            data_path,
            version=2,
            rows=large_rows,
            block_size=large_block_size,
            seed=seed,
            generated_at="2026-01-02T00:00:00.000000Z",
            replace=replace,
        ),
        "v3_corrupt": generate_snapshot(
            data_path,
            version=3,
            rows=corrupt_rows,
            block_size=corrupt_block_size,
            seed=seed,
            generated_at="2026-01-03T00:00:00.000000Z",
            corrupt_column="label",
            corrupt_block_no=1,
            replace=replace,
        ),
        "queries": generate_query_samples(data_path, corrupt_key=corrupt_key),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate deterministic chunk-column snapshots")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--small-rows", type=int, default=5000)
    parser.add_argument("--large-rows", type=int, default=150000)
    parser.add_argument("--corrupt-rows", type=int, default=5000)
    parser.add_argument("--small-block-size", type=int, default=512)
    parser.add_argument("--large-block-size", type=int, default=16384)
    parser.add_argument("--corrupt-block-size", type=int, default=512)
    parser.add_argument("--seed", type=int, default=20261122)
    parser.add_argument("--no-replace", action="store_true")
    args = parser.parse_args(argv)
    result = generate_suite(
        args.data_dir,
        small_rows=args.small_rows,
        large_rows=args.large_rows,
        corrupt_rows=args.corrupt_rows,
        small_block_size=args.small_block_size,
        large_block_size=args.large_block_size,
        corrupt_block_size=args.corrupt_block_size,
        seed=args.seed,
        replace=not args.no_replace,
    )
    import json

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
