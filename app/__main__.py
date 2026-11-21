from __future__ import annotations

import argparse
import os
from pathlib import Path

import uvicorn


def main() -> None:
    parser = argparse.ArgumentParser(description="Run DeployLens on a local loopback port")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument(
        "--db",
        default=os.environ.get("DEPLOYLENS_DB", "data/workbench.db"),
        help="SQLite database path",
    )
    args = parser.parse_args()
    os.environ["DEPLOYLENS_DB"] = str(Path(args.db).resolve())
    uvicorn.run("app.main:app", host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
