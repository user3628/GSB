from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import uvicorn  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local deterministic join planner.")
    parser.add_argument("--host", default=os.environ.get("PLANNER_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PLANNER_PORT", "8765")))
    parser.add_argument(
        "--pid-file",
        type=Path,
        default=Path(os.environ.get("PLANNER_PID_FILE", str(ROOT / "runtime" / "server.pid"))),
    )
    args = parser.parse_args()
    args.pid_file.parent.mkdir(parents=True, exist_ok=True)
    args.pid_file.write_text(str(os.getpid()), encoding="ascii")
    try:
        uvicorn.run(
            "join_planner.main:app",
            host=args.host,
            port=args.port,
            log_level=os.environ.get("PLANNER_LOG_LEVEL", "info"),
            reload=False,
        )
    finally:
        try:
            if args.pid_file.read_text(encoding="ascii").strip() == str(os.getpid()):
                args.pid_file.unlink()
        except OSError:
            pass


if __name__ == "__main__":
    main()

