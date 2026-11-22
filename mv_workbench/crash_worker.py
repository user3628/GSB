from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .service import WorkbenchService


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Start a refresh, persist its intent and REFRESHING states, then force "
            "the process to terminate at the configured crash point."
        )
    )
    parser.add_argument("--db", required=True)
    parser.add_argument("--request-id", required=True)
    parser.add_argument(
        "--crash-point", default="after_refresh_intent", choices=["after_refresh_intent"]
    )
    args = parser.parse_args(argv)
    service = WorkbenchService(
        Path(args.db), crash_point=args.crash_point, auto_seed=True
    )
    service.initialize()
    print(
        json.dumps(
            {
                "pid": os.getpid(),
                "message": "refresh intent will be committed before forced termination",
                "crash_point": args.crash_point,
            }
        ),
        flush=True,
    )
    service.refresh(args.request_id, mode="all")
    print("UNEXPECTED: crash point was not reached", flush=True)
    return 99


if __name__ == "__main__":
    raise SystemExit(main())
