from __future__ import annotations

import argparse
import os
from pathlib import Path

from .api import create_app
from .samples import generate_sample_workspace


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Local mixed-precision checkpoint API")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--db", default=os.environ.get("MPC_DB_PATH", "var/checkpoints.sqlite3"))
    parser.add_argument(
        "--samples",
        default=os.environ.get("MPC_SAMPLES_DIR", "data/seed_20261122"),
    )
    parser.add_argument("--seed", type=int, default=20261122)
    args = parser.parse_args(argv)
    if args.host not in {"127.0.0.1", "localhost", "::1"}:
        parser.error("this single-machine service only binds to a loopback address")
    sample_dir = Path(args.samples)
    if not (sample_dir / "manifest.json").exists():
        generate_sample_workspace(sample_dir, seed=args.seed)
    app = create_app(args.db, sample_dir)
    print(
        f"MPC API listening on http://{args.host}:{args.port} "
        f"db={Path(args.db).resolve()} samples={sample_dir.resolve()}",
        flush=True,
    )
    app.run(host=args.host, port=args.port, debug=False, threaded=True, use_reloader=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
