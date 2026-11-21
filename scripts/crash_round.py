"""Start a round against a hanging endpoint; a parent test can SIGKILL this process."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from delay_queue.config import Settings  # noqa: E402
from delay_queue.service import DelayQueueService  # noqa: E402


async def _main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--intent-dir", required=True)
    parser.add_argument("--queue", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--hang-ms", type=int, default=60_000)
    args = parser.parse_args()

    settings = Settings(
        db_path=Path(args.db),
        intent_log_dir=Path(args.intent_dir),
        default_endpoint=args.endpoint,
        global_concurrency=4,
        retry_backoff_base_ms=20,
    )
    service = DelayQueueService(settings)
    service.startup()
    print("round-started", flush=True)
    await service.trigger_round(
        args.queue,
        args.round_id,
        {
            "max_concurrency": 1,
            "time_budget_ms": args.hang_ms + 5_000,
            "max_task_resource": 100,
            "waiting_queue_capacity": 4,
            "dispatch_timeout_ms": args.hang_ms,
        },
    )


if __name__ == "__main__":
    asyncio.run(_main())

