from __future__ import annotations

import argparse
import socket
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description="Fail if a loopback TCP port is occupied")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18081)
    args = parser.parse_args()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((args.host, args.port))
        except OSError as exc:
            print(f"OCCUPIED {args.host}:{args.port}: {exc}", file=sys.stderr)
            return 1
    print(f"FREE {args.host}:{args.port}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
