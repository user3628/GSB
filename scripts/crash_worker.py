from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.service import ConsistencyService  # noqa: E402
from app.snapshot import load_snapshot_file  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Commit one snapshot and let the demo fault point terminate it.")
    parser.add_argument("--db", required=True)
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--base-version", type=int, required=True)
    args = parser.parse_args()

    service = ConsistencyService(args.db)
    service.initialize(recover=False)
    snapshot = load_snapshot_file(args.snapshot)
    result = service.commit_snapshot(snapshot.payload, base_version=args.base_version)
    print(f"unexpected complete commit: {result}", flush=True)


if __name__ == "__main__":
    main()
