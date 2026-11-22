from __future__ import annotations

import argparse
import json
import socket
import sys
from pathlib import Path

import uvicorn

from .api import create_app
from .engine import EngineConfig


def port_is_available(host: str, port: int) -> bool:
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    with socket.socket(family, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((host, port))
        except OSError:
            return False
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the local GitOps adjudicator HTTP service")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--db", default="data/gitops.db")
    parser.add_argument("--switch-threshold", type=int, default=1000)
    parser.add_argument("--max-batch-replicas", type=int, default=512)
    parser.add_argument(
        "--check-port-only",
        action="store_true",
        help="exit 0 if the requested loopback endpoint is bindable",
    )
    args = parser.parse_args(argv)

    if args.host not in {"127.0.0.1", "localhost", "::1"}:
        print(
            json.dumps(
                {"code": "E_SPEC_INVALID", "message": "only loopback hosts are supported"}
            ),
            file=sys.stderr,
        )
        return 2
    if not port_is_available(args.host, args.port):
        print(
            json.dumps(
                {
                    "code": "E_PORT_UNAVAILABLE",
                    "message": "the requested port is already bound",
                    "context": {"host": args.host, "port": args.port},
                }
            ),
            file=sys.stderr,
        )
        return 2
    if args.check_port_only:
        print(json.dumps({"bindable": True, "host": args.host, "port": args.port}))
        return 0
    application = create_app(
        Path(args.db),
        EngineConfig(
            switch_threshold=args.switch_threshold,
            max_batch_replicas=args.max_batch_replicas,
        ),
    )
    uvicorn.run(application, host=args.host, port=args.port, log_level="info")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
