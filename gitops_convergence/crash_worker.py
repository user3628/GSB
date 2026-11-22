from __future__ import annotations

import argparse
import json

from .engine import Engine, EngineConfig


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Deliberately terminate after a convergence intent is durable"
    )
    parser.add_argument("--db", required=True)
    parser.add_argument("--snapshot", type=int, required=True)
    parser.add_argument("--mode", default="auto")
    parser.add_argument(
        "--crash-after",
        choices=["convergence_intent", "convergence_batches"],
        default="convergence_batches",
        help="terminate after the intent or after the BUILDING batch draft is durable",
    )
    parser.add_argument("--switch-threshold", type=int, default=1000)
    parser.add_argument("--max-batch-replicas", type=int, default=512)
    args = parser.parse_args(argv)
    engine = Engine(
        args.db,
        EngineConfig(
            switch_threshold=args.switch_threshold,
            max_batch_replicas=args.max_batch_replicas,
            crash_after=args.crash_after,
        ),
    )
    # The engine exits the process after a durable intent or after the
    # BUILDING batch draft, before any state or ACTIVE plan publication.
    engine.trigger_convergence(args.snapshot, {"mode": args.mode})
    print(json.dumps({"unexpected": "crash point was not reached"}))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
