from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Delete local DB, WAL/SHM and result artifacts")
    parser.add_argument("--db", default="artifacts/vecengine.db")
    parser.add_argument("--results-dir", default="artifacts/results")
    parser.add_argument("--samples-dir", default=None)
    args = parser.parse_args(argv)
    db = Path(args.db)
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(db) + suffix)
        if path.exists():
            path.unlink()
    results = Path(args.results_dir)
    if results.exists():
        shutil.rmtree(results)
    if args.samples_dir:
        samples = Path(args.samples_dir)
        if samples.exists():
            shutil.rmtree(samples)
    print("local state cleared")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
