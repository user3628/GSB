"""Force-kill a writer mid-rebuild, then recover from SQLite facts."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.store import Store  # noqa: E402


def pretty(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description="Run the forced-termination recovery drill")
    parser.add_argument("--db", default=str(ROOT / "data" / "recovery.db"))
    parser.add_argument("--fault", default="mid_rebuild", choices=["after_intent", "mid_rebuild"])
    args = parser.parse_args()

    db_path = Path(args.db).resolve()
    for suffix in ("", "-wal", "-shm"):
        Path(str(db_path) + suffix).unlink(missing_ok=True)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    command = [
        sys.executable,
        "-m",
        "app.chaos",
        "--db",
        str(db_path),
        "--fault",
        args.fault,
    ]
    print(f"EVENT spawning_writer command={' '.join(command)}")
    completed = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
    print(f"EVENT writer_exit_code {completed.returncode}")
    if completed.stdout.strip():
        print("writer stdout:\n" + completed.stdout)
    if completed.stderr.strip():
        print("writer stderr:\n" + completed.stderr)
    if completed.returncode == 0:
        raise RuntimeError("writer did not terminate abruptly")

    before = Store(db_path)
    before.initialize()
    pending = before.pending_intents()
    print("EVENT persisted_facts_before_recovery")
    print(
        pretty(
            {
                "current_version": before.current_version(),
                "pending_intent_count": len(pending),
                "pending_intents": pending,
            }
        )
    )
    if before.current_version() != 2 or len(pending) != 1:
        raise RuntimeError("unexpected pre-recovery state")

    print("EVENT restart_scan")
    recovered = before.recover_pending_intents()
    print(pretty(recovered))

    after = Store(db_path)
    after.initialize()
    report = after.recovery_report()
    prod_v2 = after.environment_bundle("prod", version=2)
    current_prod = after.environment_bundle("prod")
    print("EVENT facts_after_recovery")
    print(
        pretty(
            {
                "current_version": after.current_version(),
                "pending_intents": len(after.pending_intents()),
                "recovery_report": report,
                "committed_v2_prod_plan_services": [
                    item["service"] for item in prod_v2["plan"]
                ],
                "current_prod_environment_version": current_prod[
                    "environment_version"
                ],
            }
        )
    )
    if after.current_version() != 2 or after.pending_intents():
        raise RuntimeError("recovery did not converge to the last complete snapshot")
    print("RECOVERY DRILL OK")


if __name__ == "__main__":
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    main()
