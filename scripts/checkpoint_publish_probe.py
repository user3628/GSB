#!/usr/bin/env python3
"""Single-shot checkpoint publisher used to exercise cross-process locking."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from topk_pipeline.checkpoints import CheckpointStore
from topk_pipeline.errors import PipelineError


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("workspace")
    parser.add_argument("value")
    parser.add_argument("--shard-id", default="race_shard")
    parser.add_argument("--stage", default="filter")
    parser.add_argument("--sequence", type=int, default=1)
    args = parser.parse_args()
    store = CheckpointStore(args.workspace)
    try:
        record = store.publish(
            args.shard_id,
            sequence=args.sequence,
            stage=args.stage,
            input_fingerprint=f"input-{args.value}",
            config_version=f"config-{args.value}",
            content_fingerprint="content",
            artifact={"publisher": args.value},
        )
        print(json.dumps({"ok": True, "sequence": record["sequence"], "stage": record["stage"]}))
        return 0
    except PipelineError as exc:
        print(json.dumps({"ok": False, "error": exc.to_dict()}, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
