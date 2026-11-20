from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from alert_sieve.store import Store  # noqa: E402


def main() -> int:
    db_path = sys.argv[1]
    payload_path = sys.argv[2]
    body = json.loads(Path(payload_path).read_text(encoding="utf-8"))
    store = Store(db_path)
    store.submit_topology_increment(body, fault_crash_after_products=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
