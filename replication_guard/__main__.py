from __future__ import annotations

import argparse

import uvicorn


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local replication lag governance service")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--db", default=None)
    parser.add_argument("--reload", action="store_true")
    args = parser.parse_args()

    if args.db:
        import os

        os.environ["RG_DB_PATH"] = args.db
    uvicorn.run(
        "replication_guard.api:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()

