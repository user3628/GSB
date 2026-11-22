from __future__ import annotations

import argparse
import json
from pathlib import Path

from .api import make_server
from .store import Store


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m kvstore",
        description="Local segment-backed key/value index with expiry and recovery",
    )
    parser.add_argument("--data-dir", required=True, help="directory for segments and SQLite")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--port-file", default=None)
    args = parser.parse_args()
    store = Store(args.data_dir)
    server = make_server(store, args.host, args.port)
    host, port = server.server_address[:2]
    address = {"ok": True, "host": host, "port": port, "base_url": f"http://{host}:{port}"}
    if args.port_file:
        Path(args.port_file).write_text(json.dumps(address), encoding="utf-8")
    print(json.dumps(address, sort_keys=True), flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        store.close()


if __name__ == "__main__":
    main()

