#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kvstore.store import Store  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Deterministic SIGKILL-style crash worker")
    parser.add_argument("data_dir")
    parser.add_argument("--crash-point", required=True)
    parser.add_argument("--import-file")
    parser.add_argument("--batch-id", default="crash-demo-batch")
    parser.add_argument("--put-key")
    parser.add_argument("--put-value", default="crash-value")
    parser.add_argument("--snapshot-file")
    args = parser.parse_args()
    os.environ["KVSTORE_CRASH_POINT"] = args.crash_point
    store = Store(args.data_dir)
    if args.import_file:
        store.import_data(
            Path(args.import_file).read_bytes(), batch_id=args.batch_id
        )
    elif args.put_key:
        store.put(args.put_key, args.put_value, batch_id=args.batch_id)
    elif args.snapshot_file:
        store.load_snapshot(Path(args.snapshot_file).read_bytes(), batch_id=args.batch_id)
    else:
        raise SystemExit("provide --import-file, --put-key or --snapshot-file")
    raise SystemExit("crash point was not reached")


if __name__ == "__main__":
    main()

