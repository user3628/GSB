from __future__ import annotations

import argparse
from pathlib import Path

from .api import create_app
from .store import KVStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the local KV store HTTP API")
    parser.add_argument("--data-dir", required=True, help="directory containing segments, intents, and snapshots")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5050)
    args = parser.parse_args(argv)

    store = KVStore(Path(args.data_dir))
    app = create_app(store)
    app.run(host=args.host, port=args.port, threaded=True, debug=False, use_reloader=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
