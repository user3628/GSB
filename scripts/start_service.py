from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import uvicorn

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local MSMQ HTTP service")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--db", default=str(ROOT / "artifacts" / "msmq.sqlite3"))
    parser.add_argument("--enable-failpoints", action="store_true")
    args = parser.parse_args()
    if args.host not in {"127.0.0.1", "::1", "localhost"}:
        parser.error("the service may only bind to a loopback address")
    if not (1 <= args.port <= 65535):
        parser.error("port must be within 1..65535")
    os.environ["MSMQ_DB_PATH"] = str(Path(args.db).resolve())
    os.environ["MSMQ_ENABLE_FAILPOINTS"] = "1" if args.enable_failpoints else "0"
    from msmq.api import create_app

    application = create_app(args.db, enable_failpoints=args.enable_failpoints)
    uvicorn.run(application, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()

