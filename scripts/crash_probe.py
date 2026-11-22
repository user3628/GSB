"""Write one durable intent, then terminate the process before the business txn.

This is a controlled local crash probe. It is intentionally not exposed over HTTP.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.service import GovernanceService  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--request-id", default="req-crash-probe")
    parser.add_argument("--tenant-id", default="starter")
    parser.add_argument("--arrival-ms", type=int, required=True)
    parser.add_argument("--cpu", type=int, default=100)
    parser.add_argument("--memory", type=int, default=2 * 1_048_576)
    args = parser.parse_args()

    service = GovernanceService(args.db, crash_after_intent=True)
    service.startup()
    print("CRASH_PROBE_READY", flush=True)
    service.submit_decision(
        {
            "request_id": args.request_id,
            "tenant_id": args.tenant_id,
            "namespace": "shared-prod",
            "arrival_ms": args.arrival_ms,
            "cpu_millicores": args.cpu,
            "memory_bytes": args.memory,
        }
    )
    print("CRASH_PROBE_UNEXPECTEDLY_COMPLETED", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
