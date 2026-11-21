"""Command line entry point for offline initialisation and inspection."""

from __future__ import annotations

import argparse
import json
import os
import sys

from .persistence import Store
from .samplegen import generate_all


def init(base_dir: str) -> dict:
    generated = generate_all(base_dir)
    db_path = os.path.join(base_dir, "data", "workbench.sqlite3")
    store = Store(db_path)
    pending = store.get_pending_intents()
    store.close()
    return {
        "chains": generated,
        "db_path": db_path,
        "pending_intents": pending,
        "samples": os.path.join(base_dir, "samples", "handshake_samples.json"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TLS workbench CLI")
    sub = parser.add_subparsers(dest="command", required=True)
    init_parser = sub.add_parser("init", help="generate deterministic samples")
    init_parser.add_argument("--base-dir", default=os.getcwd())
    args = parser.parse_args(argv)
    if args.command == "init":
        print(json.dumps(init(args.base_dir), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
