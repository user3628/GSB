from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    parser = argparse.ArgumentParser(description="Gracefully or forcibly stop a local planner process.")
    parser.add_argument("--pid", type=int)
    parser.add_argument("--pid-file", type=Path, default=ROOT / "runtime" / "server.pid")
    parser.add_argument("--force", action="store_true", help="Use SIGKILL/TerminateProcess instead of a graceful signal.")
    args = parser.parse_args()
    pid = args.pid
    if pid is None:
        try:
            pid = int(args.pid_file.read_text(encoding="ascii").strip())
        except (OSError, ValueError) as exc:
            raise SystemExit(f"cannot read PID from {args.pid_file}: {exc}") from exc
    if os.name == "nt":
        command = ["taskkill", "/PID", str(pid), "/T"]
        if args.force:
            command.append("/F")
        subprocess.run(command, check=True)
    else:
        os.kill(pid, signal.SIGKILL if args.force else signal.SIGTERM)
    print(f"sent {'forced termination' if args.force else 'graceful termination'} to pid {pid}")


if __name__ == "__main__":
    main()

