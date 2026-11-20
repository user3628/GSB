from __future__ import annotations

import argparse
import json
from pathlib import Path

from .store import KVStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Open the store, run recovery, and print reports")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--compact", action="store_true", help="print a compact index without key listings")
    args = parser.parse_args(argv)
    store = KVStore(Path(args.data_dir))
    summary = store.index_summary(limit=10000)
    if args.compact:
        summary.pop("keys", None)
    print(json.dumps({"recovery": store.current_recovery_report(), "index": summary}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
