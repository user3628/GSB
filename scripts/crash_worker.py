from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from slowquery.config import WorkbenchConfig  # noqa: E402
from slowquery.service import SlowQueryService  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run one diagnosis/verification with a deterministic process-kill checkpoint."
    )
    parser.add_argument("--database", required=True)
    parser.add_argument("--sample-db", required=True)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--operation", choices=("diagnose", "verify"), required=True)
    parser.add_argument("--suggestion-id")
    parser.add_argument("--crash-point", required=True)
    args = parser.parse_args(argv)
    config = WorkbenchConfig(
        database_path=Path(args.database),
        sample_database_path=Path(args.sample_db),
    )
    service = SlowQueryService(
        config,
        recover_on_init=False,
        crash_point=args.crash_point,
    )
    if args.operation == "diagnose":
        service.diagnose(args.session_id)
    else:
        service.verify(args.session_id, suggestion_id=args.suggestion_id)
    print("Crash point was not reached; the operation committed normally.", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
