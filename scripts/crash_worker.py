from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from confighub.service import ConfigService, snapshot_digest


CONFIG_V1 = {
    "w0": 1.2,
    "w1": -0.5,
    "w2": 0.25,
    "normalization_scale": 1.5,
    "threshold": 0.1,
}
CONFIG_V2 = {
    "w0": 2.0,
    "w1": 0.7,
    "w2": -0.3,
    "normalization_scale": 0.8,
    "threshold": 0.25,
}
GENERATED_V1 = "2026-01-01T00:00:00+00:00"
GENERATED_V2 = "2026-01-01T00:00:01+00:00"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--crash-code", type=int, default=37)
    args = parser.parse_args()

    service = ConfigService(Path(args.db))
    service.register_client(
        "crash-client",
        list(CONFIG_V1),
        args.endpoint,
        timeout_seconds=2.0,
        sample_seed=7,
        sample_size=16,
    )
    service.submit_snapshot(
        1,
        CONFIG_V1,
        digest=snapshot_digest(1, GENERATED_V1, CONFIG_V1),
        generated_at=GENERATED_V1,
    )
    first = asyncio.run(service.push_version(1))
    if not first.get("advanced"):
        print("v1 did not apply", file=sys.stderr)
        return 2
    service.submit_snapshot(
        2,
        CONFIG_V2,
        digest=snapshot_digest(2, GENERATED_V2, CONFIG_V2),
        generated_at=GENERATED_V2,
    )

    def crash(stage: str) -> None:
        print(f"forced process exit during {stage}", flush=True)
        os._exit(args.crash_code)

    asyncio.run(service.push_version(2, crash_hook=crash))
    print("unreachable", file=sys.stderr)
    return 3


if __name__ == "__main__":
    raise SystemExit(main())

