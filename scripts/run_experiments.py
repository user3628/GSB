from __future__ import annotations

import argparse
import hashlib
import json
import math
import struct
import subprocess
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from join_shuffle_engine.errors import EngineError  # noqa: E402
from join_shuffle_engine.service import EngineService  # noqa: E402
from scripts.generate_samples import generate_samples  # noqa: E402


DATA_ROOT = PROJECT_ROOT / "data" / "snapshots"
ARTIFACT_ROOT = PROJECT_ROOT / "artifacts"
DB_PATH = ARTIFACT_ROOT / "experiment.db"
PARTITION_COUNTS = [1, 2, 3, 7, 17, 64]


def _clean_database(path: Path) -> None:
    for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
        candidate.unlink(missing_ok=True)


def _result_signature(results: list[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for result in sorted(results, key=lambda item: int(item["group_key"])):
        mean = result.get("mean_value")
        mean_bits = 0
        mean_flag = 0
        if mean is not None:
            mean_bits = struct.unpack("<Q", struct.pack("<d", float(mean)))[0]
            mean_flag = 1
        sum_bits = struct.unpack("<Q", struct.pack("<d", float(result["sum_value"])))[0]
        digest.update(
            struct.pack(
                "<qQQqQq",
                int(result["group_key"]),
                sum_bits,
                mean_bits,
                mean_flag,
                int(result["count"]),
                int(result["non_null_count"]),
            )
        )
    return digest.hexdigest()


def _result_by_key(results: list[dict[str, Any]], key: int) -> dict[str, Any]:
    for item in results:
        if int(item["group_key"]) == key:
            return item
    raise KeyError(key)


def _ensure_samples() -> None:
    required = [
        DATA_ROOT / "demo-left-v1",
        DATA_ROOT / "demo-right-v1",
        DATA_ROOT / "demo-left-v2",
        DATA_ROOT / "demo-right-v2",
        DATA_ROOT / "precision-left-v1",
        DATA_ROOT / "precision-right-v1",
        DATA_ROOT / "skew-left-v1",
        DATA_ROOT / "skew-right-v1",
    ]
    if not all(path.exists() for path in required):
        generate_samples(DATA_ROOT, force=False)


def _run_crash_drill() -> dict[str, Any]:
    process = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "scripts" / "crash_drill.py"), "--phase", "all"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=180,
    )
    if process.returncode != 0:
        raise RuntimeError(
            f"crash drill failed ({process.returncode}): {process.stderr[-2000:]}"
        )
    report_path = ARTIFACT_ROOT / "crash_recovery_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    return {
        "report_path": str(report_path),
        "drills": [
            {
                "phase": item["phase"],
                "child_exit_code": item["child_exit_code"],
                "request_status": item["request_status"],
                "result_query_error": item["result_query_error"],
                "last_complete_partition": item["recovery_report"][
                    "last_complete_partition"
                ],
                "persisted_partition_count": item["recovery_report"][
                    "persisted_partition_count"
                ],
                "pending_intents": item["recovery_report"]["pending_intents"],
                "aggregate_rows": item["database_facts"]["row_counts"]["aggregates"],
                "exit_report_complete": item["recovery_report"]["facts"][
                    "exit_report_complete"
                ],
            }
            for item in report["drills"]
        ],
    }


def run() -> dict[str, Any]:
    _ensure_samples()
    ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)
    _clean_database(DB_PATH)
    service = EngineService(DB_PATH)
    try:
        service.load_snapshots(
            dataset_id="demo",
            left_path=DATA_ROOT / "demo-left-v1",
            right_path=DATA_ROOT / "demo-right-v1",
        )
        partition_runs: list[dict[str, Any]] = []
        reference_results: list[dict[str, Any]] | None = None
        reference_signature: str | None = None
        all_identical = True
        for index, partitions in enumerate(PARTITION_COUNTS):
            request_id = f"exp-demo-p{partitions}"
            response = service.submit_join(
                request_id,
                "demo",
                partitions=partitions,
                join_type="inner",
                measure="left",
                skew_limit=0.35,
                skew_action="report",
                precision_mode="high",
            )
            result = response["result"]
            results = result["results"]
            signature = _result_signature(results)
            if index == 0:
                reference_results = results
                reference_signature = signature
            identical = signature == reference_signature
            all_identical = all_identical and identical
            first = results[0]
            partition_runs.append(
                {
                    "partitions": partitions,
                    "request_id": request_id,
                    "pair_version": response["pinned_pair_version"],
                    "selected_path": result["selected_path"],
                    "joined_rows": result["joined_rows"],
                    "group_count": len(results),
                    "first_group_key": first["group_key"],
                    "first_sum": first["sum_value"],
                    "first_mean": first["mean_value"],
                    "signature_sha256": signature,
                    "bitwise_identical_to_p1": identical,
                    "max_abs_sum_diff_vs_p1": max(
                        abs(float(a["sum_value"]) - float(b["sum_value"]))
                        for a, b in zip(results, reference_results or results)
                    ),
                    "max_abs_mean_diff_vs_p1": max(
                        (
                            abs(float(a["mean_value"]) - float(b["mean_value"]))
                            if a["mean_value"] is not None and b["mean_value"] is not None
                            else 0.0
                        )
                        for a, b in zip(results, reference_results or results)
                    ),
                }
            )
        idempotent = service.submit_join(
            "exp-demo-p7",
            "demo",
            partitions=7,
            join_type="inner",
            measure="left",
            skew_limit=0.35,
            skew_action="report",
            precision_mode="high",
        )
        duplicate_execution_count = service.repo.count_executions("exp-demo-p7")

        service.load_snapshots(
            dataset_id="demo",
            left_path=DATA_ROOT / "demo-left-v2",
            right_path=DATA_ROOT / "demo-right-v2",
        )
        v2 = service.submit_join(
            "exp-demo-v2",
            "demo",
            partitions=7,
            join_type="inner",
            measure="left",
            precision_mode="high",
        )
        old_request_pair = service.get_request_status("exp-demo-p1")["pair_version"]
        monotonic_error = None
        try:
            service.load_snapshots(
                dataset_id="demo",
                left_path=DATA_ROOT / "demo-left-v2",
                right_path=DATA_ROOT / "demo-right-v2",
            )
        except EngineError as error:
            monotonic_error = error.code

        service.load_snapshots(
            dataset_id="precision",
            left_path=DATA_ROOT / "precision-left-v1",
            right_path=DATA_ROOT / "precision-right-v1",
        )
        high = service.submit_join(
            "exp-precision-high",
            "precision",
            partitions=13,
            join_type="left",
            measure="left",
            precision_mode="high",
            target_abs_error=10.0,
        )
        fast = service.submit_join(
            "exp-precision-fast",
            "precision",
            partitions=13,
            join_type="left",
            measure="left",
            precision_mode="fast",
        )
        exact_reference = math.fsum([1.0e16] + [1.0] * 1000)
        high_group = _result_by_key(high["result"]["results"], 1001)
        fast_group = _result_by_key(fast["result"]["results"], 1001)
        high_contract = high["precision"]["contract"]
        fast_contract = fast["precision"]["contract"]
        high_error = abs(float(high_group["sum_value"]) - exact_reference)
        fast_error = abs(float(fast_group["sum_value"]) - exact_reference)
        high_bound = high_contract["high_path"]["bound"]["estimated_max_abs_error"]
        fast_bound = fast_contract["fast_path"]["bound"]["estimated_max_abs_error"]

        fast_budget_error = None
        try:
            service.submit_join(
                "exp-precision-fast-reject",
                "precision",
                partitions=13,
                join_type="left",
                measure="left",
                precision_mode="fast",
                target_abs_error=10.0,
            )
        except EngineError as error:
            fast_budget_error = {
                "code": error.code,
                "details": error.details,
            }
        auto_budget_error = None
        try:
            service.submit_join(
                "exp-precision-auto-budget",
                "precision",
                partitions=13,
                join_type="left",
                measure="left",
                precision_mode="auto",
                target_abs_error=10.0,
                max_operations=2000,
            )
        except EngineError as error:
            auto_budget_error = {"code": error.code, "details": error.details}

        null_semantics = service.submit_join(
            "exp-null-left-join",
            "precision",
            partitions=5,
            join_type="left",
            measure="right",
            precision_mode="high",
        )
        unmatched_group = _result_by_key(null_semantics["result"]["results"], 2002)

        service.load_snapshots(
            dataset_id="skew",
            left_path=DATA_ROOT / "skew-left-v1",
            right_path=DATA_ROOT / "skew-right-v1",
        )
        skew = service.submit_shuffle(
            "exp-skew-report",
            "skew",
            partitions=8,
            skew_limit=0.35,
            skew_action="report",
        )
        partition_rows = skew["partitions"]["partitions"]
        skew_rows = [item for item in partition_rows if item["skew"]]
        skew_reject = None
        try:
            service.submit_shuffle(
                "exp-skew-reject",
                "skew",
                partitions=8,
                skew_limit=0.35,
                skew_action="reject",
            )
        except EngineError as error:
            skew_reject = {"code": error.code, "details": error.details}

        contract = high_contract
        experiment = {
            "sample_seed": 20261121,
            "database": service.repo.database_facts(),
            "partition_independence": {
                "partition_counts": PARTITION_COUNTS,
                "runs": partition_runs,
                "bitwise_identical_across_all_partition_counts": all_identical,
                "canonical_signature_sha256": reference_signature,
            },
            "idempotency": {
                "request_id": "exp-demo-p7",
                "executions_after_duplicate_submit": duplicate_execution_count,
                "reused": idempotent["reused"],
            },
            "snapshot_versioning": {
                "old_request_pair_version": old_request_pair,
                "new_request_pair_version": v2["pinned_pair_version"],
                "reload_same_version_error": monotonic_error,
            },
            "kahan_vs_naive": {
                "fixed_sample": "key 1001 = 1e16 followed by 1000 additions of 1.0",
                "exact_reference_math_fsum": exact_reference,
                "high_path_sum": high_group["sum_value"],
                "high_path_abs_error": high_error,
                "high_path_error_bound": high_bound,
                "high_path_error_within_bound": high_error <= high_bound,
                "fast_path_sum": fast_group["sum_value"],
                "fast_path_abs_error": fast_error,
                "fast_path_error_bound": fast_bound,
                "fast_path_error_within_bound": fast_error <= fast_bound,
                "mean_derived_from_sum_and_non_null_count": (
                    high_group["mean_value"] == high_group["sum_value"] / high_group["non_null_count"]
                ),
                "selected_high_path": high["result"]["selected_path"],
                "selected_fast_path": fast["result"]["selected_path"],
            },
            "null_semantics": {
                "join_type": "left",
                "measure": "right",
                "group_key": 2002,
                "result": unmatched_group,
                "deterministic_empty_metric": (
                    unmatched_group["sum_value"] == 0.0
                    and unmatched_group["mean_value"] is None
                    and unmatched_group["non_null_count"] == 0
                ),
            },
            "precision_budget": {
                "high_target_10_passed": high["request"]["status"] == "committed",
                "fast_target_10_rejected": fast_budget_error,
                "auto_budget_2000_rejected": auto_budget_error,
                "high_path_operations": contract["high_path"]["estimated_operations"],
                "fast_path_operations": contract["fast_path"]["estimated_operations"],
                "cost_model": contract["cost_model"],
            },
            "skew": {
                "partitions": 8,
                "skew_limit": 0.35,
                "skew_partition_count": len(skew_rows),
                "skew_partitions": [
                    {
                        "partition_id": item["partition_id"],
                        "share": item["share"],
                        "total_count": item["total_count"],
                        "hotspot_keys": item["hotspot_keys"],
                    }
                    for item in skew_rows
                ],
                "reject_response": skew_reject,
            },
            "crash_recovery": _run_crash_drill(),
        }
        report_path = ARTIFACT_ROOT / "experiment_report.json"
        report_path.write_text(
            json.dumps(experiment, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        experiment["report_path"] = str(report_path)
        return experiment
    finally:
        service.close()


def _print_summary(report: dict[str, Any]) -> None:
    print("== Partition independence / high precision ==")
    for run in report["partition_independence"]["runs"]:
        print(
            f"p={run['partitions']:>2} groups={run['group_count']} rows={run['joined_rows']} "
            f"sum0={run['first_sum']!r} mean0={run['first_mean']!r} "
            f"signature={run['signature_sha256'][:16]} identical={run['bitwise_identical_to_p1']}"
        )
    print(
        "all_bitwise_identical=",
        report["partition_independence"]["bitwise_identical_across_all_partition_counts"],
    )
    print("== Kahan vs naive ==")
    comparison = report["kahan_vs_naive"]
    print(
        f"exact={comparison['exact_reference_math_fsum']!r} "
        f"high={comparison['high_path_sum']!r} err={comparison['high_path_abs_error']!r} "
        f"bound={comparison['high_path_error_bound']!r}"
    )
    print(
        f"fast={comparison['fast_path_sum']!r} err={comparison['fast_path_abs_error']!r} "
        f"bound={comparison['fast_path_error_bound']!r}"
    )
    print("== Null semantics ==")
    print(report["null_semantics"]["result"])
    print("== Precision budget ==")
    print(
        "fast_target_10=",
        report["precision_budget"]["fast_target_10_rejected"]["code"],
        "auto_budget_2000=",
        report["precision_budget"]["auto_budget_2000_rejected"]["code"],
    )
    print("== Skew ==")
    for item in report["skew"]["skew_partitions"]:
        print(
            f"partition={item['partition_id']} share={item['share']:.6f} "
            f"hotspot={item['hotspot_keys'][0]}"
        )
    print("reject_code=", report["skew"]["reject_response"]["code"])
    print("== Crash recovery ==")
    for item in report["crash_recovery"]["drills"]:
        print(
            f"phase={item['phase']} child_exit={item['child_exit_code']} "
            f"request={item['request_status']} last_complete={item['last_complete_partition']} "
            f"persisted={item['persisted_partition_count']} aggregates={item['aggregate_rows']} "
            f"result_error={item['result_query_error']}"
        )
    print("== Versioning / idempotency ==")
    print(report["snapshot_versioning"])
    print(report["idempotency"])
    print("wrote", report["report_path"])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run deterministic local experiments")
    parser.parse_args(argv)
    report = run()
    _print_summary(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
