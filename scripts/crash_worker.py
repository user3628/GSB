"""Fault-injection subprocess used by the demo and tests.

The process exits with os._exit after a durable 'prepared' intent is committed
and before the business mutation transaction is committed.  This models forced
termination rather than an exception-driven graceful shutdown.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from leasefair.clock import FixedClock
from leasefair.service import LeaseService


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["acquire", "renew"])
    parser.add_argument("db_path")
    parser.add_argument("now_ms", type=int)
    parser.add_argument("--version", type=int, default=1)
    args = parser.parse_args()

    service = LeaseService(args.db_path, clock=FixedClock(args.now_ms))
    if args.mode == "acquire":
        os.environ["LEASE_FAULT_AFTER_PREPARE"] = "acquire"
        service.acquire(
            "crash-lock",
            request_id="crash-acquire-request",
            holder_id="crash-holder",
            ttl_ms=30000,
            weight="0.5",
        )
    else:
        os.environ["LEASE_FAULT_AFTER_PREPARE"] = "renew"
        service.renew(
            "renew-lock",
            request_id="crash-renew-request",
            holder_id="renew-holder",
            version=args.version,
            ttl_ms=60000,
        )
    raise SystemExit(99)


if __name__ == "__main__":
    main()

