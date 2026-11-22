"""Child process used by the forced-termination recovery test."""

from __future__ import annotations

import argparse
import time

from leasecoord import Coordinator, CoordinatorConfig


class FixedClock:
    def __init__(self, now_ms: int) -> None:
        self.value = now_ms

    def now_ms(self) -> int:
        return self.value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("db")
    parser.add_argument("--now-ms", type=int, required=True)
    parser.add_argument("--node", default="alpha")
    parser.add_argument("--expected-version", type=int, default=1)
    args = parser.parse_args()
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
        crash_after_intent=True,
    )
    # This call persists the PENDING intent, then os._exit(91) happens before
    # the atomic lease transaction can begin.
    coordinator.renew(
        args.node,
        expected_lease_version=args.expected_version,
        now_ms=args.now_ms,
    )
    time.sleep(1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
