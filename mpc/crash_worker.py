from __future__ import annotations

import argparse
import json
from pathlib import Path

from .service import CheckpointService


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one operation with crash injection")
    parser.add_argument("--db", required=True)
    parser.add_argument("--samples", required=True)
    parser.add_argument(
        "--operation", choices=("policy-increment", "checkpoint"), required=True
    )
    parser.add_argument("--payload", required=True, help="Path to a JSON request body")
    args = parser.parse_args(argv)
    service = CheckpointService(args.db, args.samples, recover_on_start=False)
    body = json.loads(Path(args.payload).read_text(encoding="utf-8"))
    if args.operation == "policy-increment":
        service.submit_policy_increment(body)
    else:
        service.submit_checkpoint(body)
    # Reaching here means the configured crash point was not hit.
    print(json.dumps({"ok": True, "note": "operation completed without crash"}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
