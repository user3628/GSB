#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mtlab.checkpoints import CheckpointStore
from mtlab.errors import MtlError


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--tier", required=True)
    parser.add_argument("--stage", required=True)
    parser.add_argument("--staged-dir", required=True)
    parser.add_argument("--descriptor-file", required=True)
    parser.add_argument("--decision-fingerprint", default="probe-decision")
    parser.add_argument("--expected-sequence", type=int, default=None)
    parser.add_argument("--start-file", default=None)
    args = parser.parse_args()
    if args.start_file:
        start_path = Path(args.start_file)
        deadline = time.monotonic() + 20.0
        while not start_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
    descriptor = json.loads(Path(args.descriptor_file).read_text(encoding="utf-8"))
    store = CheckpointStore(Path(args.workspace))
    try:
        record = store.publish_stage(
            tier=args.tier,
            stage=args.stage,
            staged_dir=Path(args.staged_dir),
            input_descriptor=descriptor,
            decision_fingerprint=args.decision_fingerprint,
            expected_sequence=args.expected_sequence,
        )
        print(json.dumps({"status": "published", "record": record}, ensure_ascii=False, sort_keys=True))
        return 0
    except MtlError as exc:
        print(json.dumps({"error": exc.as_dict()}, ensure_ascii=False, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

