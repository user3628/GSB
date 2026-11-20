from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from confighub.errors import ConfigHubError
from confighub.service import ConfigService


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify one persisted push batch")
    parser.add_argument("--db", required=True, help="SQLite database path")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--batch-id")
    group.add_argument("--version", type=int)
    parser.add_argument(
        "--strict",
        action="store_true",
        help="also require complete client coverage and consistent observed versions",
    )
    args = parser.parse_args()

    service = ConfigService(Path(args.db))
    batch_id = args.batch_id
    if batch_id is None:
        row = service.storage.query_one(
            "SELECT batch_id FROM batches WHERE target_version=?", (args.version,)
        )
        if row is None:
            print(f"no batch exists for version {args.version}", file=sys.stderr)
            return 2
        batch_id = row["batch_id"]
    try:
        report = service.verify_batch(batch_id)
    except ConfigHubError as exc:
        print(json.dumps(exc.as_dict(), indent=2, ensure_ascii=False), file=sys.stderr)
        return 2
    print(json.dumps(report, indent=2, ensure_ascii=False))
    passed = report["conclusion"] == "CONSISTENT"
    if args.strict:
        passed = passed and report["consistent"]
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())

