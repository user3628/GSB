from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from confighub.service import ConfigService


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan and recover incomplete committed intents")
    parser.add_argument("--db", required=True, help="SQLite database path")
    args = parser.parse_args()
    report = ConfigService(Path(args.db)).recover()
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())

