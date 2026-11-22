#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.engine import Engine
from app.schemas import BatchRequest


def main() -> int:
    parser = argparse.ArgumentParser(description="Fault-injection worker for crash recovery verification")
    parser.add_argument("--db", required=True)
    parser.add_argument("--mode", choices=["ingest", "snapshot"], default="ingest")
    parser.add_argument("--batch-file")
    parser.add_argument("--crash-point", choices=["after_intent", "mid_transaction"], default="mid_transaction")
    args = parser.parse_args()

    engine = Engine(f"sqlite:///{Path(args.db).resolve()}", recover_on_startup=False)
    try:
        if args.mode == "ingest":
            if not args.batch_file:
                raise SystemExit("--batch-file is required for ingest mode")
            request = BatchRequest.model_validate(json.loads(Path(args.batch_file).read_text()))
            engine.ingest_batch(request, crash_point=args.crash_point)
        else:
            engine.create_snapshot(source="crash-test", crash_point=args.crash_point)
    finally:
        engine.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
