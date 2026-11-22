from __future__ import annotations

import argparse
import json
import socket
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


def is_free(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, port))
            return True
        except OSError:
            return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Check loopback workbench port reservations")
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()
    reservations = json.loads(
        (ROOT / "samples" / "loopback_ports.json").read_text(encoding="utf-8")
    )
    occupied: list[tuple[str, int]] = []
    for name, port in reservations.items():
        if not isinstance(port, int):
            continue
        free = is_free(args.host, port)
        print(f"{'FREE ' if free else 'USED '} {name:20s} {args.host}:{port}")
        if not free:
            occupied.append((name, port))
    if occupied:
        print("Port preflight failed; stop the listed listeners before starting the demo.")
        return 1
    print("Port preflight passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
