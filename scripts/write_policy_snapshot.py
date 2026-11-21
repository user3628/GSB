from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Write a versioned policy snapshot file")
    parser.add_argument("--output", required=True)
    parser.add_argument("--snapshot-seq", type=int, required=True)
    parser.add_argument("--version", type=int, required=True)
    parser.add_argument("--loss-scale", type=float, default=64.0)
    parser.add_argument("--growth-factor", type=float, default=2.0)
    parser.add_argument(
        "--assignments",
        default='{"s0":"FP16","s1":"FP32","s2":"FP16","s3":"FP32"}',
        help="JSON object mapping shard ids to FP16 or FP32",
    )
    args = parser.parse_args()
    snapshot = {
        "format_version": 1,
        "snapshot_seq": args.snapshot_seq,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "policy": {
            "version": args.version,
            "loss_scale": args.loss_scale,
            "loss_scale_initial": args.loss_scale,
            "growth_factor": args.growth_factor,
            "shards": json.loads(args.assignments),
        },
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(snapshot, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(output.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
