#!/usr/bin/env python3
"""Start a foreground local demo service with deterministic data and snapshot."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shardroute.__main__ import main as shardroute_main  # noqa: E402
from shardroute.datastore import SQLiteDataStore  # noqa: E402
from shardroute.util import read_json  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Start a deterministic local demo server")
    parser.add_argument("--work-dir", default=str(ROOT / "artifacts" / "manual-demo"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--port-file")
    parser.add_argument("--init-data", action="store_true")
    parser.add_argument("--load-sample-snapshot", action="store_true")
    args = parser.parse_args(argv)

    work_dir = Path(args.work_dir).resolve()
    data_dir = work_dir / "data"
    metadata_db = work_dir / "metadata.sqlite3"
    work_dir.mkdir(parents=True, exist_ok=True)
    if args.init_data:
        SQLiteDataStore(data_dir).initialize(read_json(ROOT / "samples" / "seed_data.json"))
    port_file = Path(args.port_file) if args.port_file else work_dir / "port.json"
    command = [
        "serve",
        "--metadata-db",
        str(metadata_db),
        "--data-dir",
        str(data_dir),
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--port-file",
        str(port_file),
    ]
    if args.load_sample_snapshot:
        command.extend(
            [
                "--snapshot",
                str(ROOT / "samples" / "snapshot_v1.json"),
                "--auto-load-snapshot",
            ]
        )
    return shardroute_main(command)


if __name__ == "__main__":
    raise SystemExit(main())

