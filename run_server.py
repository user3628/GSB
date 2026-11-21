#!/usr/bin/env python3
"""Launch the workbench HTTP API on the local loopback interface only."""

from __future__ import annotations

import argparse
import os

from tls_workbench.webapp import create_app


def main() -> None:
    parser = argparse.ArgumentParser(description="TLS workbench local API server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument(
        "--db", default=os.path.join("data", "workbench.sqlite3")
    )
    args = parser.parse_args()
    app = create_app(base_dir=os.getcwd(), db_path=args.db)
    app.run(host=args.host, port=args.port, threaded=True)


if __name__ == "__main__":
    main()
