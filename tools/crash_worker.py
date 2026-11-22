from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.config import Settings
from app.engine import RuntimeEngine


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one call and wait to be force-killed")
    parser.add_argument("--db", required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--specs", required=True)
    parser.add_argument("--scenario", choices=["create", "execute"], required=True)
    parser.add_argument("--call-id", required=True)
    args = parser.parse_args()
    settings = Settings(
        db_path=Path(args.db),
        log_path=Path(args.db).with_suffix(".log"),
        endpoint_base=args.endpoint,
        start_timeout_s=30.0,
        exec_timeout_s=30.0,
        default_queue_capacity=4,
        max_queue_wait_s=30.0,
    )
    engine = RuntimeEngine(settings)
    specs = json.loads(Path(args.specs).read_text(encoding="utf-8"))
    engine.load_specs(specs, source=args.specs)
    function_id = specs["functions"][0]["function_id"]
    injection = {"create_timeout": True} if args.scenario == "create" else {}
    engine.invoke(
        {
            "call_id": args.call_id,
            "function_id": function_id,
            "execution_us": 5_000_000,
            "injection": injection,
        }
    )


if __name__ == "__main__":
    main()

