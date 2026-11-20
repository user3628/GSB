from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from join_shuffle_engine.errors import EngineError  # noqa: E402
from join_shuffle_engine.service import EngineService  # noqa: E402


SNAPSHOT_ROOT = PROJECT_ROOT / "data" / "snapshots"
ARTIFACT_ROOT = PROJECT_ROOT / "artifacts"


def _clean_database(path: Path) -> None:
    for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
        candidate.unlink(missing_ok=True)


def _child(db_path: Path, phase: str, partitions: int, crash_at: int) -> None:
    service = EngineService(db_path)
    service.load_snapshots(
        dataset_id="crash-demo",
        left_path=SNAPSHOT_ROOT / "demo-left-v1",
        right_path=SNAPSHOT_ROOT / "demo-right-v1",
    )
    service.submit_join(
        "crash-drill-join",
        "crash-demo",
        partitions=partitions,
        join_type="inner",
        measure="left",
        precision_mode="high",
        wait=True,
        timeout=60.0,
    )


def run_drill(phase: str, *, partitions: int = 8, crash_at: int = 3) -> dict:
    if phase not in {"partition", "merge"}:
        raise ValueError("phase must be partition or merge")
    db_path = ARTIFACT_ROOT / f"crash-{phase}.db"
    _clean_database(db_path)
    env = os.environ.copy()
    env.pop("JOIN_ENGINE_CRASH_AT_PARTITION_START", None)
    env.pop("JOIN_ENGINE_CRASH_BEFORE_COMMIT", None)
    if phase == "partition":
        env["JOIN_ENGINE_CRASH_AT_PARTITION_START"] = str(crash_at)
    else:
        env["JOIN_ENGINE_CRASH_BEFORE_COMMIT"] = "1"
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--child",
        "--db",
        str(db_path),
        "--phase",
        phase,
        "--partitions",
        str(partitions),
        "--crash-at",
        str(crash_at),
    ]
    process = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    expected_exit = 96 if phase == "partition" else 98
    if process.returncode != expected_exit:
        raise RuntimeError(
            f"crash child exited {process.returncode}, expected {expected_exit}; "
            f"stdout={process.stdout!r} stderr={process.stderr!r}"
        )

    # Creating a new service triggers startup recovery before any new work is admitted.
    recovered = EngineService(db_path)
    report_bundle = recovered.recovery_report()
    request = recovered.get_request_status("crash-drill-join")
    result_error = None
    try:
        recovered.get_aggregates("crash-drill-join")
    except EngineError as error:
        result_error = error.code
    facts = recovered.repo.database_facts()
    report = report_bundle["reports"][0] if report_bundle["reports"] else None
    recovered.close()
    return {
        "phase": phase,
        "child_exit_code": process.returncode,
        "request_status": request["status"],
        "result_query_error": result_error,
        "recovery_report": report,
        "database_facts": facts,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Forced-termination and recovery drill")
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--db", help=argparse.SUPPRESS)
    parser.add_argument("--phase", choices=["partition", "merge", "all"], default="all")
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--crash-at", type=int, default=3)
    args = parser.parse_args(argv)
    if args.child:
        _child(Path(args.db), args.phase, args.partitions, args.crash_at)
        return 0
    phases = ["partition", "merge"] if args.phase == "all" else [args.phase]
    reports = [
        run_drill(phase, partitions=args.partitions, crash_at=args.crash_at)
        for phase in phases
    ]
    output = {
        "generated_at": "deterministic-local-run",
        "drills": reports,
        "claim_scope": (
            "Recovery reports are based only on SQLite rows committed before forced exit; "
            "no complete process exit report is claimed."
        ),
    }
    output_path = ARTIFACT_ROOT / "crash_recovery_report.json"
    output_path.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(output, indent=2, sort_keys=True))
    print(f"wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
