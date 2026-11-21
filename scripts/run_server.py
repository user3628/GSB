from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import uvicorn

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from eventbus.api import create_app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the local deterministic event bus.")
    parser.add_argument(
        "--db", default=os.getenv("EVENTBUS_DB", "var/eventbus.db"), help="SQLite file path"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8765, type=int)
    parser.add_argument(
        "--enable-test-hooks",
        action="store_true",
        help="Enable crash-after-intent hooks for the end-to-end recovery demonstration.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    os.environ["EVENTBUS_DB"] = str(Path(args.db).expanduser().resolve())
    os.environ["EVENTBUS_ENABLE_TEST_HOOKS"] = "1" if args.enable_test_hooks else "0"
    app = create_app(args.db, test_hooks=args.enable_test_hooks)
    uvicorn.run(app, host=args.host, port=args.port, log_level="info", workers=1)


if __name__ == "__main__":
    main()
