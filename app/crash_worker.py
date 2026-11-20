from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from app.service import WorkbenchService
from app.storage import Storage


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stage part of a simulation, signal readiness, then wait to be force-killed."
    )
    parser.add_argument("--db", required=True, help="SQLite database path")
    parser.add_argument("--batch-id", required=True, help="Prepared PENDING batch id")
    parser.add_argument("--stage-count", type=int, default=5)
    parser.add_argument("--ready-file", required=True)
    parser.add_argument("--hold-seconds", type=int, default=120)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    storage = Storage(args.db)
    service = WorkbenchService(storage)
    result = service.execute_prepared_batch(
        args.batch_id,
        stage_limit=args.stage_count,
        finalize=False,
    )
    ready_path = Path(args.ready_file).resolve()
    ready_path.parent.mkdir(parents=True, exist_ok=True)
    ready_path.write_text(
        json.dumps(
            {
                "batch_id": args.batch_id,
                "status": result["batch"]["status"],
                "staged_count": result["staged_count"],
                "pid": __import__("os").getpid(),
                "ready_at_unix": int(time.time()),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    time.sleep(args.hold_seconds)
    raise SystemExit(96)


if __name__ == "__main__":
    main()
