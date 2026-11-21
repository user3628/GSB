from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import database_path, load_settings
from app.db import Database
from app.models import ScheduleRoundRequest
from app.scheduler import Scheduler


async def main_async(args: argparse.Namespace) -> None:
    settings = load_settings(args.config)
    database = Database(database_path(args.db))
    database.initialize(settings)
    scheduler = Scheduler(database, settings)
    print(f"worker ready db={database.path} sessions={args.session_ids}", flush=True)
    await scheduler.run_round(
        ScheduleRoundRequest(
            request_id=args.request_id,
            session_ids=args.session_ids,
            time_budget_ms=60_000,
            queue_limit=8,
            write_timeout_ms=10_000,
            read_timeout_ms=10_000,
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a scheduler round until externally killed")
    parser.add_argument("--db", required=True)
    parser.add_argument("--config", default="samples/backends.json")
    parser.add_argument("--request-id", default="sigkill-round")
    parser.add_argument("--session-ids", nargs="+", required=True)
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
