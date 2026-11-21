"""崩溃工作进程：配合 GRAYSWITCH_CRASH_AFTER_INTENT=1 模拟强制终止。"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.service import GrayService  # noqa: E402
from app.storage import Storage  # noqa: E402


def main() -> None:
    db_path = sys.argv[1]
    storage = Storage(db_path)
    service = GrayService(storage)

    policy = {
        "version": 1,
        "generated_at_ms": 1000,
        "version_weights": {"stable": "0.70", "canary": "0.30"},
        "mirror_rate": "0.25",
        "gray_cap": "0.35",
        "relative_tolerance": "0.000001",
        "dedup_window_ms": 10000,
        "baseline_version": "stable",
        "seed": "crash-seed",
    }
    from app.models import canonical_digest

    policy["digest"] = canonical_digest(policy)
    service.submit_policy(policy)

    batch = {
        "batch_id": "crash-batch",
        "policy_version": 1,
        "events": [
            {
                "request_id": f"c-{i}",
                "timestamp_ms": 2000 + i,
                "route_key": "r",
                "client_id": f"cli-{i}",
                "weight": 100 + i,
                "target_version": "stable",
            }
            for i in range(5)
        ],
    }
    service.submit_batch(batch)  # 预期在意图落库后 os._exit(137)
    storage.close()


if __name__ == "__main__":
    main()
