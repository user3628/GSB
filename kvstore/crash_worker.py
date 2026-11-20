from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

from .storage_utils import atomic_write_json
from .store import KVStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write an intent and wait to be forcibly terminated")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--marker", required=True)
    parser.add_argument("--key", default="crash:pending")
    parser.add_argument("--value", default="must-be-rolled-back")
    args = parser.parse_args(argv)

    store = KVStore(Path(args.data_dir))
    marker = Path(args.marker)

    def after_intent(intent: dict) -> None:
        atomic_write_json(
            marker,
            {
                "pid": os.getpid(),
                "state": "intent_persisted_before_segment_append",
                "intent": intent,
            },
        )
        while True:
            time.sleep(60)

    store.write(args.key, args.value, version=9001, crash_after_intent=after_intent)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
