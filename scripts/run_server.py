from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import uvicorn


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the mirror adjudicator on a loopback address")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--db", default=os.environ.get("MIRROR_DB_PATH", "runtime/mirror_judge.db"))
    parser.add_argument("--replay-target", default=os.environ.get("MIRROR_REPLAY_TARGET"))
    args = parser.parse_args()
    if args.host not in {"127.0.0.1", "localhost", "::1"}:
        parser.error("only loopback hosts are allowed")
    os.environ["MIRROR_DB_PATH"] = str(Path(args.db).resolve())
    if args.replay_target:
        os.environ["MIRROR_REPLAY_TARGET"] = args.replay_target
    elif "MIRROR_REPLAY_TARGET" not in os.environ:
        os.environ["MIRROR_REPLAY_TARGET"] = f"http://127.0.0.1:{args.port}/_test/replay"
    uvicorn.run("mirror_judge.main:app", host=args.host, port=args.port, log_level="info", reload=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
