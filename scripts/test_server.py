#!/usr/bin/env python3
"""Run the deterministic local echo/hang test service."""

from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from diaglab.test_service import ProbeTestService  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["echo", "hang", "refuse"], default="echo")
    parser.add_argument("--probe-port", type=int, default=0)
    parser.add_argument("--admin-port", type=int, default=0)
    parser.add_argument("--hang-seconds", type=float, default=30.0)
    args = parser.parse_args()

    service = ProbeTestService(
        mode=args.mode,
        probe_port=args.probe_port,
        admin_port=args.admin_port,
        hang_seconds=args.hang_seconds,
    ).start()
    stopped = threading.Event()

    def stop_handler(signum: int, frame: object) -> None:
        stopped.set()

    signal.signal(signal.SIGINT, stop_handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, stop_handler)

    print(
        json.dumps(
            {
                "ready": True,
                "mode": args.mode,
                "probe_port": service.probe_port,
                "admin_port": service.admin_port,
                "metrics_url": f"http://127.0.0.1:{service.admin_port}/metrics",
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        flush=True,
    )
    try:
        while not stopped.wait(0.25):
            pass
    finally:
        service.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

