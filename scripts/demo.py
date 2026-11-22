"""Run the fixed sample, numeric audits, and real forced-termination recovery."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from leasefair.clock import FixedClock
from leasefair.samples import build_demo_report, load_sample, seed_fixed_scenario
from leasefair.service import LeaseService

SAMPLE_PATH = ROOT / "data" / "fixed_lease_samples.json"
CRASH_WORKER = ROOT / "scripts" / "crash_worker.py"


def _fresh_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(path) + suffix)
        if candidate.exists():
            candidate.unlink()


def _crash_acquire(db_path: Path, now_ms: int) -> dict[str, Any]:
    _fresh_db(db_path)
    result = subprocess.run(
        [sys.executable, str(CRASH_WORKER), "acquire", str(db_path), str(now_ms)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    recovered = LeaseService(db_path, clock=FixedClock(now_ms))
    report = recovered.recovery_report()
    lock = recovered.get_lock("crash-lock", now_ms=now_ms)
    return {
        "worker_exit_code": result.returncode,
        "worker_stdout": result.stdout.strip(),
        "worker_stderr": result.stderr.strip(),
        "recovery_report": report,
        "lock_after_recovery": lock,
    }


def _crash_renew(db_path: Path, now_ms: int) -> dict[str, Any]:
    _fresh_db(db_path)
    parent = LeaseService(db_path, clock=FixedClock(now_ms))
    acquired = parent.acquire(
        "renew-lock",
        request_id="seed-renew-lock",
        holder_id="renew-holder",
        ttl_ms=30000,
        weight="0.5",
    )
    version = int(acquired["lease"]["version"])
    expiry_before = int(acquired["lease"]["expires_at_ms"])
    result = subprocess.run(
        [
            sys.executable,
            str(CRASH_WORKER),
            "renew",
            str(db_path),
            str(now_ms + 100),
            "--version",
            str(version),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    recovered = LeaseService(db_path, clock=FixedClock(now_ms + 100))
    report = recovered.recovery_report()
    lock = recovered.get_lock("renew-lock", now_ms=now_ms + 100)
    expiry_after = int(lock["lease"]["expires_at_ms"])
    return {
        "worker_exit_code": result.returncode,
        "version": version,
        "expiry_before": expiry_before,
        "expiry_after": expiry_after,
        "expiry_restored_exactly": expiry_before == expiry_after,
        "recovery_report": report,
        "lock_after_recovery": lock,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=str(ROOT / "artifacts" / "demo_report.json"))
    parser.add_argument("--with-crash", action="store_true", default=True)
    args = parser.parse_args()
    db_path = ROOT / "artifacts" / "demo_main.sqlite"
    _fresh_db(db_path)
    service, clock, sample = seed_fixed_scenario(db_path, SAMPLE_PATH)
    report = build_demo_report(service, clock, sample)
    if args.with_crash:
        report["forced_termination"] = {
            "grant_before_commit": _crash_acquire(
                ROOT / "artifacts" / "demo_crash_grant.sqlite", int(sample["origin_ms"])
            ),
            "renew_before_commit": _crash_renew(
                ROOT / "artifacts" / "demo_crash_renew.sqlite", int(sample["origin_ms"])
            ),
        }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    print(f"\nDemo report written to {output}", file=sys.stderr)


if __name__ == "__main__":
    main()

