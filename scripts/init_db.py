#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from replica_repair.db import Database  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Initialize the local SQLite database")
    parser.add_argument("--db", default="data/repair.sqlite3")
    args = parser.parse_args(argv)
    database = Database(Path(args.db))
    print(
        json.dumps(
            {
                "ok": True,
                "code": "OK",
                "db_path": database.path,
                "journal_mode": "WAL",
                "synchronous": "FULL",
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

