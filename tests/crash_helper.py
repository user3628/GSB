"""Subprocess helper that forcibly exits after a durable APPLIED intent."""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mqmw.store import SQLiteStore  # noqa: E402


def main() -> int:
    db, mode = sys.argv[1], sys.argv[2]
    store = SQLiteStore(db)
    if mode == "dispatch":
        store.register_queue("crash-q", 1)
        store.register_consumer("crash-c", "crash-g", "http://127.0.0.1:9/consume", ["crash-q"])
        store.enqueue(
            "crash-q",
            [
                {
                    "message_id": "crash-dispatch",
                    "body": {"case": "dispatch"},
                    "visibility_timeout_ms": 60_000,
                    "max_retries": 3,
                }
            ],
        )
        os.environ["MQMW_FAULT_POINT"] = "dispatch_after_applied"
        store.claim("crash-q", "crash-c", "at_least_once", defer_commit=True)
        return 0
    if mode == "ack":
        store.register_queue("crash-q", 1)
        store.register_consumer("crash-c", "crash-g", "http://127.0.0.1:9/consume", ["crash-q"])
        store.enqueue(
            "crash-q",
            [
                {
                    "message_id": "crash-ack",
                    "body": {"case": "ack"},
                    "visibility_timeout_ms": 60_000,
                    "max_retries": 3,
                }
            ],
        )
        delivery = store.claim("crash-q", "crash-c", "exactly_once", defer_commit=True)
        assert delivery is not None
        store.complete_dispatch(
            "crash-q",
            delivery["intent_id"],
            delivery["delivery_id"],
            http_status=200,
            elapsed_ms=1,
        )
        os.environ["MQMW_FAULT_POINT"] = "ack_after_applied"
        store.ack(
            delivery["delivery_id"],
            delivery["lease_token"],
            "crash-c",
            "exactly_once",
            request_id="crash-ack-request",
        )
        return 0
    raise SystemExit(f"unknown mode: {mode}")


if __name__ == "__main__":
    raise SystemExit(main())
