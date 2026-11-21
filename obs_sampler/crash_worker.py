"""Deliberately die in the middle of a policy transaction for recovery tests."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from .service import SamplerService


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--policy", required=True)
    parser.add_argument(
        "--stage",
        default="after_counts_and_reports_before_commit_marker",
        choices=(
            "after_decisions_before_counts",
            "after_counts_and_reports_before_commit_marker",
        ),
    )
    parser.add_argument("--exit-code", type=int, default=87)
    args = parser.parse_args()

    policy_document = json.loads(Path(args.policy).read_text(encoding="utf-8"))
    service = SamplerService(args.db, recover_on_start=False)

    def crash_hook(stage: str) -> None:
        if stage == args.stage:
            print(
                json.dumps(
                    {
                        "event": "HARD_PROCESS_EXIT",
                        "stage": stage,
                        "policy_version": policy_document.get("version"),
                    }
                ),
                flush=True,
            )
            # os._exit bypasses exception handling, atexit and graceful cleanup.
            # The already-committed intent remains RUNNING; the main transaction
            # is not committed and will be rolled back by SQLite on recovery.
            os._exit(args.exit_code)

    service.submit_policy(policy_document, crash_hook=crash_hook)
    print(json.dumps({"event": "UNEXPECTED_COMPLETION"}), flush=True)
    sys.exit(2)


if __name__ == "__main__":
    main()
