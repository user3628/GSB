from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from colscan.snapshot import build_snapshot_document, snapshot_checksum, write_snapshot_file


def sample_columns(version: int, row_count: int) -> list[dict[str, Any]]:
    names_pool = ["alice", "bob", "carol", "dave", "erin", "frank"]
    status_pool = ["active", "pending", "paused", "archived"]
    regions = ["east", "west", "north", "south"]
    ids: list[Any] = []
    names: list[Any] = []
    statuses: list[Any] = []
    scores: list[Any] = []
    active: list[Any] = []
    region: list[Any] = []
    for index in range(row_count):
        ids.append(1000 + version * 1000 + index)
        names.append(None if index % 17 == 0 else names_pool[(index * 3 + version) % len(names_pool)])
        status_run = (index // 3 + version) % 7
        if status_run in (0, 1):
            status_value = "active"
        elif status_run in (2, 3):
            status_value = "pending"
        elif status_run == 4:
            status_value = "paused"
        else:
            status_value = "archived"
        statuses.append(None if index % 29 == 0 else status_value)
        score = round(((index * 37 + version * 11) % 1000) / 10.0 - 20.0, 2)
        scores.append(None if index % 19 == 0 else score)
        active.append(None if index % 31 == 0 else (index % 4 in (0, 1)))
        region.append(regions[(index // 4 + version) % len(regions)])
    return [
        {"name": "id", "dtype": "int64", "encoding": "fixed", "segment_rows": 4, "values": ids},
        {"name": "name", "dtype": "string", "encoding": "dict", "segment_rows": 4, "values": names},
        {"name": "status", "dtype": "string", "encoding": "rle", "segment_rows": 4, "values": statuses},
        {"name": "score", "dtype": "float64", "encoding": "fixed", "segment_rows": 4, "values": scores},
        {"name": "active", "dtype": "bool", "encoding": "rle", "segment_rows": 4, "values": active},
        {"name": "region", "dtype": "string", "encoding": "dict", "segment_rows": 4, "values": region},
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate deterministic local columnar snapshots")
    parser.add_argument("--out", default="data/snapshots")
    parser.add_argument("--versions", type=int, nargs="+", default=[1, 3])
    parser.add_argument("--rows", type=int, default=96)
    parser.add_argument("--block-size", type=int, default=16)
    args = parser.parse_args()
    if args.rows < 1 or args.block_size < 1:
        raise SystemExit("rows and block-size must be positive")
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {"sample": "deterministic-local-columnar-v1", "files": []}
    for version in args.versions:
        document = build_snapshot_document(
            version=version,
            generated_at=f"2026-01-01T00:00:{version % 60:02d}Z",
            block_size=args.block_size,
            columns=sample_columns(version, args.rows),
        )
        path = out_dir / f"snapshot_v{version}.csnap"
        write_snapshot_file(path, document)
        manifest["files"].append(
            {
                "version": version,
                "path": str(path),
                "checksum": snapshot_checksum(document),
                "sha256_file": hashlib.sha256(path.read_bytes()).hexdigest(),
                "row_count": args.rows,
                "block_size": args.block_size,
            }
        )
    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"ok": True, "out": str(out_dir), "manifest": str(manifest_path), "files": manifest["files"]}, indent=2))


if __name__ == "__main__":
    main()

