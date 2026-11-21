from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import uvicorn


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one local deterministic backend simulator")
    parser.add_argument("--backend-id", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--config", default="samples/backends.json")
    args = parser.parse_args()
    os.environ["BACKEND_ID"] = args.backend_id
    os.environ["BACKEND_CONFIG_FILE"] = args.config
    uvicorn.run("app.backend_sim:app", host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
