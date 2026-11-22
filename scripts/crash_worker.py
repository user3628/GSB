#!/usr/bin/env python3
"""Create an uncommitted import, publish a marker, then wait to be force-killed."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from diaglab.models import DiagnosticRecord  # noqa: E402
from diaglab.store import WorkbenchStore  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--batch-id", default="crash-batch")
    parser.add_argument("--marker", required=True)
    parser.add_argument("--records", type=int, default=3)
    args = parser.parse_args()

    base = 1_704_067_300_000
    records = [
        DiagnosticRecord(
            timestamp_ms=base + index * 1_000,
            probe_id=f"crash-probe-{index + 1:03d}",
            path_node="crash-path",
            protocol="tcp",
            success=True,
            rtt_us=1_000 + index * 10,
            payload_bytes=32,
            failure_type="none",
            ordinal=index,
            source_format="simulated-crash",
            source_version=2,
        )
        for index in range(args.records)
    ]
    store = WorkbenchStore(args.db)
    store.initialize()
    store.crash_after_uncommitted_import(
        batch_id=args.batch_id,
        records=records,
        marker_path=args.marker,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
