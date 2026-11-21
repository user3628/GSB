from __future__ import annotations

import argparse
import sys
import traceback
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pdb_engine.service import EngineService  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Start a real drain, commit N steps, then terminate without cleanup."
    )
    parser.add_argument("--db", required=True)
    parser.add_argument("--sample", required=True)
    parser.add_argument("--node", required=True)
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--after-steps", type=int, default=1)
    args = parser.parse_args()

    try:
        service = EngineService(args.db)
        service.initialize()
        service.load_sample(args.sample)
        # This process is intentionally killed by EngineService via os._exit(97)
        # immediately after the requested committed step.
        service.drain_node(
            args.node,
            args.request_id,
            crash_after_steps=args.after_steps,
        )
    except Exception:
        traceback.print_exc()
        return 2
    print("crash was not triggered", flush=True)
    return 3


if __name__ == "__main__":
    raise SystemExit(main())

