"""Subprocess used by the cross-process single-writer test."""

from __future__ import annotations

import argparse
import json
import time

from leasecoord import BusinessError, Coordinator, CoordinatorConfig


class FixedClock:
    def __init__(self, value: int) -> None:
        self.value = value

    def now_ms(self) -> int:
        return self.value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("db")
    parser.add_argument("node_id")
    parser.add_argument("--now-ms", type=int, required=True)
    parser.add_argument("--start-at", type=float, required=True)
    args = parser.parse_args()
    while time.time() < args.start_at:
        time.sleep(0.001)
    config = CoordinatorConfig(
        exact_limit=8,
        heartbeat_timeout_ms=1000,
        min_lease_ttl_ms=1,
    )
    coordinator = Coordinator(
        args.db,
        config,
        clock=FixedClock(args.now_ms),
        auto_recover=False,
    )
    try:
        result = coordinator.campaign(
            args.node_id,
            expected_lease_version=0,
            now_ms=args.now_ms,
        )
        print(json.dumps({"outcome": "ok", "result": result}, ensure_ascii=False))
    except BusinessError as exc:
        print(json.dumps({"outcome": "error", "error": exc.to_dict()}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
