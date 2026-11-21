"""Repeatable offline demonstration with deterministic samples and crash recovery."""

from __future__ import annotations

import hashlib
import json
import math
import os
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np

from .errors import BusinessError
from .models import BatchRequest
from .service import SamplerService

ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"
RUNTIME = ROOT / "runtime"
DB_PATH = RUNTIME / "demo.db"
BATCH_ID = "fixed-sample-batch-1"


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _clean_demo_files() -> None:
    RUNTIME.mkdir(parents=True, exist_ok=True)
    for name in ("demo.db", "demo.db-wal", "demo.db-shm"):
        path = RUNTIME / name
        if path.exists():
            path.unlink()


def _state_delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    before_commit = before.get("commit") or {}
    after_commit = after.get("commit") or {}
    return {
        "current_policy_version": {
            "before": before["policy_version"],
            "after": after["policy_version"],
            "unchanged": before["policy_version"] == after["policy_version"],
        },
        "decision_digest": {
            "before": before_commit.get("decision_digest"),
            "after": after_commit.get("decision_digest"),
            "unchanged": before_commit.get("decision_digest") == after_commit.get("decision_digest"),
        },
        "record_count": {
            "before": before_commit.get("record_count"),
            "after": after_commit.get("record_count"),
        },
        "decision_states": {
            "before": before.get("decision_states"),
            "after": after.get("decision_states"),
        },
    }


def _state_summary(state: dict[str, Any]) -> dict[str, Any]:
    commit = state.get("commit") or {}
    return {
        "policy_version": state["policy_version"],
        "is_current": state["is_current"],
        "decision_states": state.get("decision_states", {}),
        "record_count": commit.get("record_count"),
        "leader_count": commit.get("leader_count"),
        "duplicate_count": commit.get("duplicate_count"),
        "decision_digest": commit.get("decision_digest"),
        "counts_digest": commit.get("counts_digest"),
        "report_digest": commit.get("report_digest"),
        "revision": commit.get("revision"),
    }


def _bias_comparison(
    service: SamplerService, version: int, expected: dict[str, Any]
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    relative_errors: list[float] = []
    naive_count_relative_errors: list[float] = []
    population_matches = True

    for key, naive in sorted(expected["strata"].items()):
        service_name = naive["service"]
        level = naive["level"]
        response = service.get_bias_report(version, service_name, level)
        report = response["report"]
        population_matches = population_matches and (
            report["population_count"] == naive["population_count"]
            and abs(report["population_weight"] - naive["population_weight"]) < 1e-12
        )
        relative_bias = float(report["relative_bias"])
        relative_errors.append(abs(relative_bias))
        naive_relative = (
            (report["emitted_count"] - naive["population_count"])
            / naive["population_count"]
        )
        naive_count_relative_errors.append(abs(naive_relative))
        rows.append(
            {
                "stratum": key,
                "head_rate": report["head_rate"],
                "population_count": report["population_count"],
                "emitted_count": report["emitted_count"],
                "weighted_total": report["weighted_total"],
                "population_weight": report["population_weight"],
                "relative_bias": relative_bias,
                "naive_count_relative_bias": naive_relative,
                "relative_error_bound_95": report["relative_error_bound"],
                "configured_error_bound": report["configured_error_bound"],
                "within_error_bound": report["within_error_bound"],
                "kahan_compensation": report["weighted_total_compensation"],
                "numeric_error_bound": report["numeric_error_bound"],
            }
        )

    relative_array = np.asarray(relative_errors, dtype=np.float64)
    naive_array = np.asarray(naive_count_relative_errors, dtype=np.float64)
    return {
        "population_matches_naive_expected": population_matches,
        "strata_count": len(rows),
        "mean_absolute_corrected_bias": float(relative_array.mean()),
        "p95_absolute_corrected_bias": float(np.percentile(relative_array, 95)),
        "max_absolute_corrected_bias": float(relative_array.max()),
        "mean_absolute_naive_count_bias": float(naive_array.mean()),
        "all_within_configured_error_bound": all(row["within_error_bound"] for row in rows),
        "strata": rows,
    }


def _head_sample_check(batch_result: dict[str, Any], expected: dict[str, Any]) -> dict[str, Any]:
    sampled_uids = sorted(
        decision["record_uid"]
        for decision in batch_result["decisions"]
        if decision["head_sampled"] and not decision["duplicate"]
    )
    digest = hashlib.sha256()
    for record_uid in sampled_uids:
        digest.update(record_uid.encode("utf-8"))
        digest.update(b"\n")
    naive = expected["head_only_sample"]
    return {
        "expected_sampled_leader_count": naive["sampled_leader_count"],
        "actual_sampled_leader_count": len(sampled_uids),
        "expected_sampled_uids_sha256": naive["sampled_uids_sha256"],
        "actual_sampled_uids_sha256": digest.hexdigest(),
        "matches": (
            len(sampled_uids) == naive["sampled_leader_count"]
            and digest.hexdigest() == naive["sampled_uids_sha256"]
        ),
    }


def _crash_and_recover(db_path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    command = [
        sys.executable,
        "-m",
        "obs_sampler.crash_worker",
        "--db",
        str(db_path),
        "--policy",
        str(SAMPLES / "policy_v2.json"),
        "--stage",
        "after_counts_and_reports_before_commit_marker",
    ]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT) + (
        os.pathsep + environment["PYTHONPATH"] if environment.get("PYTHONPATH") else ""
    )
    completed = subprocess.run(
        command,
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if completed.returncode != 87:
        raise RuntimeError(
            "crash worker did not hard-exit as expected: "
            f"rc={completed.returncode} stdout={completed.stdout!r} stderr={completed.stderr!r}"
        )

    restarted = SamplerService(db_path)
    recovery = restarted.get_recovery_report()
    try:
        restarted.get_policy(2)
        v2_visible = True
    except BusinessError as exc:
        if exc.code != "E_POLICY_NOT_FOUND":
            raise
        v2_visible = False
    return restarted, {
        "worker_exit_code": completed.returncode,
        "worker_stdout": [line for line in completed.stdout.splitlines() if line.strip()],
        "worker_stderr": completed.stderr[-2000:],
        "v2_committed_after_crash": v2_visible,
        "recovery_scope": recovery["scope"],
        "forced_termination_exit_report_guaranteed": recovery[
            "forced_termination_exit_report_guaranteed"
        ],
        "aborted_intents": recovery["intents"]["aborted"],
        "current_policy_version": recovery["current_policy_version"],
        "committed_policy_versions": [
            row["version"] for row in recovery["committed_policy_versions"]
        ],
        "integrity": recovery["integrity"],
        "limitations": recovery["limitations"],
    }


def main() -> None:
    _clean_demo_files()
    logs = _load_jsonl(SAMPLES / "logs.jsonl")
    spans = _load_jsonl(SAMPLES / "spans.jsonl")
    policy_v1 = _load_json(SAMPLES / "policy_v1.json")
    policy_v2 = _load_json(SAMPLES / "policy_v2.json")
    expected = _load_json(SAMPLES / "naive_expected.json")
    batch_request = BatchRequest(batch_id=BATCH_ID, logs=logs, spans=spans)

    service = SamplerService(DB_PATH)
    policy_commit_v1 = service.submit_policy(policy_v1)
    batch_v1 = service.submit_batch(batch_request)
    v1_paths = Counter(item["path"] for item in batch_v1["decisions"])
    v1_promoted = [
        item for item in batch_v1["decisions"] if item["path"] == "TAIL_PROMOTED"
    ]
    # A promoted record must not use the old head-only weight. The stored
    # inclusion probability is the combined head+tail probability, so verify
    # the exact arithmetic relationship rather than a textual inequality.
    promoted_weight_check = all(
        math.isclose(
            item["effective_weight"],
            item["upstream_weight"] / item["inclusion_probability"],
            rel_tol=1e-12,
            abs_tol=1e-12,
        )
        for item in v1_promoted
    )
    state_v1 = service.get_policy_state(1)
    comparison_v1 = _bias_comparison(service, 1, expected["versions"]["1"])

    state_before_crash = state_v1
    service, crash = _crash_and_recover(DB_PATH)
    state_after_crash = service.get_policy_state(1)
    recovery_delta = _state_delta(state_before_crash, state_after_crash)

    policy_commit_v2 = service.submit_policy(policy_v2)
    state_v2 = service.get_policy_state(2)
    comparison_v2 = _bias_comparison(service, 2, expected["versions"]["2"])
    replay = service.submit_batch(batch_request)

    summary = {
        "parameters": {
            "seed_v1": policy_v1["seed"],
            "seed_v2": policy_v2["seed"],
            "dedup_window_ms_v1": policy_v1["dedup_window_ms"],
            "dedup_window_ms_v2": policy_v2["dedup_window_ms"],
            "error_bound_v1": policy_v1["error_bound"],
            "error_bound_v2": policy_v2["error_bound"],
            "min_samples_v1": policy_v1["min_samples"],
            "min_samples_v2": policy_v2["min_samples"],
            "tail_latency_threshold_us_v1": policy_v1["tail_sampling"]["latency_threshold_us"],
            "tail_latency_threshold_us_v2": policy_v2["tail_sampling"]["latency_threshold_us"],
            "crash_exit_code": 87,
        },
        "samples": {
            "logs": len(logs),
            "spans": len(spans),
            "batch_id": BATCH_ID,
            "batch_record_count": batch_v1["record_count"],
            "duplicates_reported_by_service": v1_paths["DEDUP_WINDOW"],
        },
        "policy_v1": {
            "submitted": policy_commit_v1["status"],
            "state": _state_summary(state_v1),
            "decision_paths": dict(sorted(v1_paths.items())),
            "tail_promoted_count": len(v1_promoted),
            "tail_promoted_weight_relationship_valid": promoted_weight_check,
            "naive_head_sample_check": _head_sample_check(
                batch_v1, expected["versions"]["1"]
            ),
        },
        "bias_comparison_v1": comparison_v1,
        "forced_termination_recovery": {
            **crash,
            "state_before": _state_summary(state_before_crash),
            "state_after_restart": _state_summary(state_after_crash),
            "state_delta": recovery_delta,
        },
        "policy_v2": {
            "submitted": policy_commit_v2["status"],
            "state": _state_summary(state_v2),
            "naive_head_sample_check": _head_sample_check(
                replay, expected["versions"]["2"]
            ),
        },
        "bias_comparison_v2": comparison_v2,
        "idempotent_replay": {
            "execution": replay["execution"],
            "record_count": replay["record_count"],
            "reported_policy_version": replay["reported_policy_version"],
            "decision_states": replay["decision_states"],
        },
    }

    output_path = RUNTIME / "demo_summary.json"
    output_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    print(f"\nWrote {output_path}")


if __name__ == "__main__":
    main()
