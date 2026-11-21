from __future__ import annotations

import argparse
import socket
import sys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify a loopback TCP port can be bound")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args(argv)
    if args.host not in {"127.0.0.1", "localhost", "::1"}:
        parser.error("only loopback hosts are supported")
    family = socket.AF_INET6 if ":" in args.host else socket.AF_INET
    with socket.socket(family, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((args.host, args.port))
            sock.listen(1)
        except OSError as exc:
            print(
                f"PORT_UNAVAILABLE host={args.host} port={args.port} error={exc}",
                file=sys.stderr,
            )
            return 2
    print(f"PORT_AVAILABLE host={args.host} port={args.port}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
