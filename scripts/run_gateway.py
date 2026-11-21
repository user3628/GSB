from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import uvicorn


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the upload coordinator HTTP gateway")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18100)
    parser.add_argument("--db", default="var/gateway.sqlite3")
    parser.add_argument("--config", default="samples/backends.json")
    args = parser.parse_args()
    os.environ["GATEWAY_DB"] = args.db
    os.environ["GATEWAY_BACKENDS_FILE"] = args.config
    uvicorn.run("app.main:app", host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
