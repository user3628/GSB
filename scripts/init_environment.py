"""Create the local SQLite path and intent-log directory."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from capacity_ledger.service import DEFAULT_DB_PATH, DEFAULT_INTENT_DIR, CapacityLedger  # noqa: E402


def main() -> None:
    ledger = CapacityLedger()
    print(f"database: {ledger.store.db_path}")
    print(f"intent_dir: {ledger.store.intent_dir}")
    print(f"intent_log: {ledger.store.intent_log_path}")
    print("environment: READY")


if __name__ == "__main__":
    main()

