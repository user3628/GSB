from __future__ import annotations

import argparse
import os
from pathlib import Path

import uvicorn

from .api import create_app


def _remove_state_files(database: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(database) + suffix)
        if path.exists():
            path.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local columnar scan HTTP service")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--state-db", default="data/state.db")
    parser.add_argument("--reset-state", action="store_true")
    parser.add_argument("--task-delay-ms", type=int, default=None)
    args = parser.parse_args()
    if args.host not in ("127.0.0.1", "localhost", "::1"):
        raise SystemExit("this service only binds loopback addresses")
    database = Path(args.state_db)
    database.parent.mkdir(parents=True, exist_ok=True)
    if args.reset_state:
        _remove_state_files(database)
    task_delay = args.task_delay_ms
    if task_delay is None:
        try:
            task_delay = int(os.environ.get("COLSCAN_TASK_DELAY_MS", "0"))
        except ValueError:
            task_delay = 0
    app = create_app(state_db=database, task_delay_ms=task_delay)
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()

