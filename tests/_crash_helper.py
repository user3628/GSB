"""崩溃注入子进程助手（由 test_recovery.py 调用）。"""

from __future__ import annotations

import json
from pathlib import Path

from graph_bench.service import BenchService
from graph_bench.storage import Storage

ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"


def main() -> None:
    storage = Storage()
    service = BenchService(storage)
    if storage.get_current_version() is None:
        service.submit_snapshot(json.loads((SAMPLES / "snapshot_v1.json").read_text(encoding="utf-8")), "base-v1")
        return
    payload = json.loads((SAMPLES / "increment_weight_auth.json").read_text(encoding="utf-8"))
    service.submit_increment(payload, "crash-increment")


if __name__ == "__main__":
    main()
