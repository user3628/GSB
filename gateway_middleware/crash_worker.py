from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .config import Settings
from .sample_data import generate_small
from .service import GatewayService


def main() -> None:
    parser = argparse.ArgumentParser(description="Crash after a durable intent commit")
    parser.add_argument("--db", required=True)
    parser.add_argument("--batch-id", default="crash-batch-1")
    parser.add_argument("--marker", required=True)
    parser.add_argument("--seed", type=int, default=20261121)
    args = parser.parse_args()

    settings = Settings.from_env().with_db(args.db)
    dataset = generate_small(settings, seed=args.seed)

    def crash_after_intent(context: dict) -> None:
        Path(args.marker).write_text(
            json.dumps({"forced_termination": True, **context}, ensure_ascii=False, sort_keys=True),
            encoding="utf-8",
        )
        # No Python atexit/finally handlers: this is the forced-termination case.
        os._exit(91)

    service = GatewayService(settings, crash_hook=crash_after_intent)
    service.submit_batch(dataset.batch(args.batch_id))


if __name__ == "__main__":
    main()
