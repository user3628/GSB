from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import connect  # noqa: E402
from app.engine import calculate_metrics  # noqa: E402
from app.errors import BusinessError  # noqa: E402
from app.fixed import mixed_sum_diagnostics  # noqa: E402
from app.service import ConsistencyService  # noqa: E402
from app.snapshot import load_json_decimal, load_snapshot_file  # noqa: E402


DATA = ROOT / "data"
ARTIFACTS = ROOT / "artifacts"
DB_PATH = ARTIFACTS / "demo.sqlite3"


def dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def reset_demo_database() -> None:
    for suffix in ("", "-wal", "-shm", ".writer.lock"):
        path = Path(str(DB_PATH) + suffix)
        if path.exists():
            path.unlink()


def pending_intent_count(db_path: Path) -> int:
    connection = connect(db_path)
    try:
        row = connection.execute("SELECT COUNT(*) AS count FROM intents WHERE status = 'started'").fetchone()
        return int(row["count"])
    finally:
        connection.close()


def assert_complete_version(service: ConsistencyService, version: int) -> None:
    current = service.get_current()
    if current["current_version"] != version or not current["complete"]:
        raise AssertionError(f"expected complete current version {version}, got {current}")
    metrics = service.get_metrics(version)
    if metrics["snapshot_version"] != version:
        raise AssertionError("metrics query returned the wrong version")
    consistency = service.get_consistency(version)
    if consistency["snapshot_version"] != version:
        raise AssertionError("consistency query returned the wrong version")


def main() -> None:
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    reset_demo_database()

    baseline = load_snapshot_file(str(DATA / "baseline.json"))
    adjusted = load_snapshot_file(str(DATA / "adjusted.json"))
    crash_candidate = load_snapshot_file(str(DATA / "crash_candidate.json"))

    first_calculation = calculate_metrics(baseline)["metrics"]
    second_calculation = calculate_metrics(baseline)["metrics"]
    reproducible = json.dumps(first_calculation, sort_keys=True) == json.dumps(second_calculation, sort_keys=True)
    if not reproducible:
        raise AssertionError("repeated fixed-point calculation was not bit-identical")

    service = ConsistencyService(DB_PATH)
    startup_report = service.initialize()
    baseline_commit = service.commit_snapshot(baseline.payload, base_version=0)
    assert_complete_version(service, 1)
    baseline_metrics = service.get_metrics(1)
    baseline_errors = service.get_errors(1)

    adjusted_commit = service.commit_snapshot(adjusted.payload, base_version=1)
    assert_complete_version(service, 2)
    adjusted_metrics = service.get_metrics(2)
    adjusted_consistency = service.get_consistency(2)

    if service.get_metrics(1)["current"] is not False:
        raise AssertionError("historical version 1 unexpectedly became the current view")

    env = dict(os.environ)
    env["CONSISTENCY_FAULT_POINT"] = "after_intent_committed"
    process = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "crash_worker.py"),
            "--db",
            str(DB_PATH),
            "--snapshot",
            str(DATA / "crash_candidate.json"),
            "--base-version",
            "2",
        ],
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if process.returncode == 0:
        raise AssertionError("forced-termination child unexpectedly exited successfully")
    pending_before_recovery = pending_intent_count(DB_PATH)
    if pending_before_recovery != 1:
        raise AssertionError(f"expected one durable started intent, got {pending_before_recovery}")
    assert_complete_version(service, 2)

    recovery = service.recover_pending(reason="demo_forced_termination_restart")
    if recovery["current_version_after"] != 2:
        raise AssertionError("recovery changed the last complete current version")
    assert_complete_version(service, 2)
    recovery_report = service.get_recovery_report()
    try:
        service.get_metrics(crash_candidate.snapshot_version)
    except BusinessError as exc:
        if exc.code != "E_VERSION_NOT_FOUND":
            raise
        uncommitted_version_invisible = True
    else:
        uncommitted_version_invisible = False
    if not uncommitted_version_invisible:
        raise AssertionError("aborted crash version became visible after recovery")

    mixed_payload = load_json_decimal((DATA / "mixed_sum.json").read_text(encoding="utf-8"))
    mixed_report = mixed_sum_diagnostics(mixed_payload["values"])

    result = {
        "demo_seed": mixed_payload["seed"],
        "database": str(DB_PATH.relative_to(ROOT)),
        "startup_recovery_report": startup_report,
        "full_validation": {
            "commit": baseline_commit,
            "metrics": baseline_metrics,
            "error_explanation": baseline_errors,
            "repeated_calculation_bit_identical": reproducible,
        },
        "cascade_recalculation": {
            "commit": adjusted_commit,
            "before": {
                "quota_share_api": baseline_metrics["components"]["api"]["quota_share"]["value"],
                "weighted_consistency": baseline_metrics["weighted_consistency"]["value"],
                "status": baseline_metrics["conclusion"]["status"],
            },
            "after": {
                "quota_share_api": adjusted_metrics["components"]["api"]["quota_share"]["value"],
                "weighted_consistency": adjusted_metrics["weighted_consistency"]["value"],
                "status": adjusted_metrics["conclusion"]["status"],
            },
            "historical_version_1": {
                "quota_share_api": service.get_metrics(1)["components"]["api"]["quota_share"]["value"],
                "weighted_consistency": service.get_metrics(1)["weighted_consistency"]["value"],
                "current": service.get_metrics(1)["current"],
            },
            "difference_report": adjusted_consistency["difference_report"],
        },
        "forced_termination_recovery": {
            "child_exit_code": process.returncode,
            "child_stdout": process.stdout.strip(),
            "child_stderr_tail": process.stderr.strip().splitlines()[-5:],
            "pending_intents_before_restart_scan": pending_before_recovery,
            "recovery": recovery,
            "recovery_report_endpoint": recovery_report,
            "current_version_after_recovery": service.get_current()["current_version"],
            "aborted_crash_version_visible": False,
        },
        "mixed_sum_comparison": mixed_report,
        "acceptance_summary": {
            "snapshot_digest_validated": True,
            "fixed_point_precision_contract": baseline_metrics["precision_contract"],
            "error_bounds_covered": adjusted_metrics["all_error_bounds_covered"],
            "tolerance_decision_reproducible": True,
            "version_switch_atomic": service.get_current()["current_version"] == 2,
            "recovery_kept_last_complete_version": recovery["current_version_after"] == 2,
        },
    }

    output_path = ARTIFACTS / "demo_result.json"
    output_path.write_text(dumps(result) + "\n", encoding="utf-8")

    print("Configuration consistency demo")
    print(f"- repeated calculation bit-identical: {reproducible}")
    print(
        "- baseline v1: "
        f"{baseline_metrics['conclusion']['status']}, "
        f"weighted={baseline_metrics['weighted_consistency']['value']} +/- "
        f"{baseline_metrics['weighted_consistency']['cumulative_error']}"
    )
    print(
        "- adjusted v2: "
        f"{adjusted_metrics['conclusion']['status']}, "
        f"weighted={adjusted_metrics['weighted_consistency']['value']} +/- "
        f"{adjusted_metrics['weighted_consistency']['cumulative_error']}, "
        f"api_share={adjusted_metrics['components']['api']['quota_share']['value']}"
    )
    print(
        "- mixed sum float64 naive error="
        f"{mixed_report['float64_naive_measured_error']} (bound {mixed_report['float64_naive_error_bound']}), "
        f"Kahan error={mixed_report['float64_kahan_measured_error']} (bound {mixed_report['float64_kahan_error_bound']})"
    )
    print(f"- forced-termination child exit code: {process.returncode}")
    print(f"- recovery kept current version: {recovery['current_version_after']}, aborted version hidden: True")
    print(f"- artifact: {output_path}")


if __name__ == "__main__":
    main()
