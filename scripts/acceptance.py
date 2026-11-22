"""End-to-end deterministic acceptance run using the fixed local samples."""

from __future__ import annotations

import json
import platform
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np

from leasecoord import BusinessError, Coordinator, CoordinatorConfig
from leasecoord.samples import SampleDataset, load_sample

SMALL_SAMPLE = ROOT / "samples" / "candidates_small.json"
LARGE_SAMPLE = ROOT / "samples" / "candidates_large.json"
OUTPUT = ROOT / "artifacts" / "acceptance_report.json"


class FixedClock:
    def __init__(self, now_ms: int) -> None:
        self.value = now_ms

    def now_ms(self) -> int:
        return self.value


def dataset_config(
    dataset: SampleDataset,
    *,
    exact_limit: int | None = None,
    heartbeat_timeout_ms: int | None = None,
) -> CoordinatorConfig:
    return CoordinatorConfig(
        exact_limit=dataset.exact_limit if exact_limit is None else exact_limit,
        heartbeat_timeout_ms=(
            dataset.heartbeat_timeout_ms
            if heartbeat_timeout_ms is None
            else heartbeat_timeout_ms
        ),
        min_lease_ttl_ms=1,
        max_lease_ttl_ms=60_000,
    )


def new_coordinator(
    path: Path,
    dataset: SampleDataset,
    *,
    now_ms: int | None = None,
    exact_limit: int | None = None,
    heartbeat_timeout_ms: int | None = None,
    auto_recover: bool = False,
) -> Coordinator:
    config = dataset_config(
        dataset,
        exact_limit=exact_limit,
        heartbeat_timeout_ms=heartbeat_timeout_ms,
    )
    return Coordinator(
        path,
        config,
        clock=FixedClock(dataset.base_epoch_ms if now_ms is None else now_ms),
        auto_recover=auto_recover,
    )


def register_dataset(co: Coordinator, dataset: SampleDataset) -> None:
    for node in dataset.nodes:
        co.register_node(
            node.id,
            node.priority,
            node.lease_ttl_ms,
            last_heartbeat_ms=node.last_heartbeat_ms,
            now_ms=dataset.base_epoch_ms,
        )


def compact_election(result: dict[str, Any]) -> dict[str, Any]:
    basis = result["election"]
    compact = {
        "algorithm": basis["algorithm"],
        "estimated": basis["estimated"],
        "candidate_count": basis["candidate_count"],
        "lease_version": result["lease_version"],
        "term": result["term"],
        "winner_id": result["leader"]["id"],
        "winner_priority": result["leader"]["priority"],
        "leader_lease_version": result["leader"]["leader_lease_version"],
        "criterion_order": basis["criterion_order"],
        "winner_evidence": basis["winner_evidence"],
        "candidate_set_digest": basis["candidate_set_digest"],
        "difference_vs_exact": basis["difference_vs_exact"],
    }
    if basis["algorithm"] == "priority_stratified_sampling":
        compact.update(
            {
                "sampling_rate": basis["sampling_rate"],
                "sampled_candidate_count": basis["sampled_candidate_count"],
                "fully_evaluated_candidate_count": basis[
                    "fully_evaluated_candidate_count"
                ],
                "eligible_count_estimate": basis["eligible_count_estimate"],
                "decisive_priority": basis["decisive_priority"],
                "bucket_count": len(basis["buckets"]),
                "buckets": basis["buckets"],
                "confidence": basis["confidence"],
                "certification": basis["certification"],
            }
        )
    return compact


def run_small_adaptive(work: Path) -> dict[str, Any]:
    dataset = load_sample(SMALL_SAMPLE)
    exact = new_coordinator(
        work / "small-exact.sqlite",
        dataset,
        exact_limit=len(dataset.nodes),
    )
    sampled = new_coordinator(
        work / "small-sampled.sqlite",
        dataset,
        exact_limit=dataset.exact_limit,
    )
    register_dataset(exact, dataset)
    register_dataset(sampled, dataset)
    exact_result = exact.campaign("bravo", now_ms=dataset.base_epoch_ms)
    sampled_result = sampled.campaign("bravo", now_ms=dataset.base_epoch_ms)
    assert exact_result["leader"]["id"] == sampled_result["leader"]["id"] == "alpha"

    renewed_at = dataset.base_epoch_ms + int(
        dataset.scenarios.get("renew_offset_ms", 2000)
    )
    sampled.heartbeat(
        "alpha", heartbeat_ms=renewed_at, now_ms=renewed_at
    )
    renewed = sampled.renew(
        "alpha", expected_lease_version=1, now_ms=renewed_at
    )
    assert renewed["lease_version"] == 2
    assert renewed["leader"]["leader_lease_version"] == 2
    return {
        "sample": dataset.name,
        "candidate_count": len(dataset.nodes),
        "exact_limit_for_exact_run": len(dataset.nodes),
        "exact_limit_for_sampled_run": dataset.exact_limit,
        "exact": compact_election(exact_result),
        "sampled": compact_election(sampled_result),
        "same_winner": exact_result["leader"]["id"] == sampled_result["leader"]["id"],
        "renew": {
            "lease_version_before": 1,
            "lease_version_after": renewed["lease_version"],
            "expires_at_ms": renewed["lease"]["expires_at_ms"],
            "leader_lease_version": renewed["leader"]["leader_lease_version"],
        },
    }


def run_large_adaptive(work: Path) -> dict[str, Any]:
    dataset = load_sample(LARGE_SAMPLE)
    started = time.perf_counter()
    seed_path = work / "large-seed.sqlite"
    seed = new_coordinator(
        seed_path,
        dataset,
        exact_limit=len(dataset.nodes),
    )
    register_dataset(seed, dataset)
    # Snapshot the same committed candidate state for both algorithm runs.
    with sqlite3.connect(seed_path) as conn:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    exact_path = work / "large-exact.sqlite"
    sampled_path = work / "large-sampled.sqlite"
    shutil.copy2(seed_path, exact_path)
    shutil.copy2(seed_path, sampled_path)
    exact = new_coordinator(
        exact_path,
        dataset,
        exact_limit=len(dataset.nodes),
    )
    sampled = new_coordinator(
        sampled_path,
        dataset,
        exact_limit=dataset.exact_limit,
    )
    registration_seconds = time.perf_counter() - started
    campaign_started = time.perf_counter()
    exact_result = exact.campaign("node-04095", now_ms=dataset.base_epoch_ms)
    sampled_result = sampled.campaign("node-04095", now_ms=dataset.base_epoch_ms)
    campaign_seconds = time.perf_counter() - campaign_started
    assert exact_result["leader"]["id"] == sampled_result["leader"]["id"]
    sampled_basis = sampled_result["election"]
    assert sampled_basis["algorithm"] == "priority_stratified_sampling"
    assert sampled_basis["estimated"] is True
    assert sampled_basis["sampling_rate"] < 1.0
    return {
        "sample": dataset.name,
        "candidate_count": len(dataset.nodes),
        "setup_seconds_single_registration_snapshot": round(registration_seconds, 3),
        "campaign_seconds_both_paths": round(campaign_seconds, 3),
        "exact": compact_election(exact_result),
        "sampled": compact_election(sampled_result),
        "same_winner": exact_result["leader"]["id"] == sampled_result["leader"]["id"],
    }


def run_concurrency(work: Path) -> dict[str, Any]:
    dataset = load_sample(SMALL_SAMPLE)
    co = new_coordinator(
        work / "concurrency.sqlite",
        dataset,
        exact_limit=len(dataset.nodes),
    )
    fresh_nodes = [
        node
        for node in dataset.nodes
        if dataset.base_epoch_ms - node.last_heartbeat_ms
        <= dataset.heartbeat_timeout_ms
    ]
    for node in fresh_nodes:
        co.register_node(
            node.id,
            node.priority,
            node.lease_ttl_ms,
            last_heartbeat_ms=node.last_heartbeat_ms,
            now_ms=dataset.base_epoch_ms,
        )

    requests = [fresh_nodes[index % len(fresh_nodes)].id for index in range(24)]

    def attempt(node_id: str) -> tuple[str, str | None, dict[str, Any]]:
        try:
            result = co.campaign(
                node_id,
                expected_lease_version=0,
                now_ms=dataset.base_epoch_ms,
            )
            return ("ok", None, result)
        except BusinessError as exc:
            return ("error", exc.code, exc.to_dict())

    with ThreadPoolExecutor(max_workers=12) as executor:
        outcomes = list(executor.map(attempt, requests))
    successes = [item for item in outcomes if item[0] == "ok"]
    failures = [item for item in outcomes if item[0] == "error"]
    error_counts: dict[str, int] = {}
    for _, code, _ in failures:
        error_counts[code or "UNKNOWN"] = error_counts.get(code or "UNKNOWN", 0) + 1
    assert len(successes) == 1
    assert error_counts == {"E_LEASE_HELD": len(failures)}
    winner = successes[0][2]["leader"]["id"]
    assert all(
        item[2]["context"]["holder_id"] == winner for item in failures
    )
    return {
        "concurrent_request_count": len(requests),
        "success_count": len(successes),
        "failure_count": len(failures),
        "failure_codes": error_counts,
        "lease_version": co.get_lease(now_ms=dataset.base_epoch_ms)["lease_version"],
        "winner_id": winner,
        "winner_status": co.get_leader(now_ms=dataset.base_epoch_ms)["leader"][
            "status"
        ],
    }


def run_multiprocess_campaign(work: Path) -> dict[str, Any]:
    dataset = load_sample(SMALL_SAMPLE)
    db_path = work / "multiprocess.sqlite"
    co = new_coordinator(
        db_path,
        dataset,
        exact_limit=len(dataset.nodes),
    )
    node_ids = ["alpha", "bravo", "charlie", "foxtrot", "golf", "hotel"]
    for node_id in node_ids:
        node = next(item for item in dataset.nodes if item.id == node_id)
        co.register_node(
            node.id,
            node.priority,
            node.lease_ttl_ms,
            last_heartbeat_ms=node.last_heartbeat_ms,
            now_ms=dataset.base_epoch_ms,
        )
    start_at = time.time() + 1.2
    processes = [
        subprocess.Popen(
            [
                sys.executable,
                "-m",
                "tests.campaign_child",
                str(db_path),
                node_id,
                "--now-ms",
                str(dataset.base_epoch_ms),
                "--start-at",
                str(start_at),
            ],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for node_id in node_ids
    ]
    payloads = []
    for process in processes:
        stdout, stderr = process.communicate(timeout=30)
        assert process.returncode == 0, stderr
        payloads.append(json.loads(stdout.strip().splitlines()[-1]))
    successes = [item for item in payloads if item["outcome"] == "ok"]
    failures = [item for item in payloads if item["outcome"] == "error"]
    assert len(successes) == 1
    assert len(failures) == len(node_ids) - 1
    assert {item["error"]["code"] for item in failures} == {"E_LEASE_HELD"}
    return {
        "process_count": len(node_ids),
        "success_count": len(successes),
        "failure_count": len(failures),
        "failure_codes": sorted({item["error"]["code"] for item in failures}),
        "lease_version": co.get_lease(now_ms=dataset.base_epoch_ms)["lease_version"],
        "winner_id": successes[0]["result"]["leader"]["id"],
    }


def run_failover(work: Path) -> dict[str, Any]:
    dataset = load_sample(SMALL_SAMPLE)
    co = new_coordinator(work / "failover.sqlite", dataset)
    register_dataset(co, dataset)
    campaign = co.campaign("bravo", now_ms=dataset.base_epoch_ms)
    assert campaign["lease_version"] == 1
    heartbeat_at = dataset.base_epoch_ms + int(
        dataset.scenarios.get("renew_offset_ms", 2000)
    )
    co.heartbeat("alpha", heartbeat_ms=heartbeat_at, now_ms=heartbeat_at)
    renewed = co.renew("alpha", expected_lease_version=1, now_ms=heartbeat_at)
    assert renewed["lease_version"] == 2

    failure_at = dataset.base_epoch_ms + int(
        dataset.scenarios.get("leader_failure_offset_ms", 5200)
    )
    try:
        co.failover(expected_lease_version=2, now_ms=failure_at)
    except BusinessError as exc:
        no_candidate = exc.to_dict()
    else:  # pragma: no cover - sample must make the leader unreachable
        raise AssertionError("sample failover unexpectedly found a candidate")
    assert no_candidate["code"] == "E_NO_ELIGIBLE_CANDIDATE"
    assert no_candidate["context"]["preserved_lease_version"] == 2
    assert co.get_lease(now_ms=failure_at)["lease_version"] == 2

    co.heartbeat("bravo", heartbeat_ms=failure_at, now_ms=failure_at)
    failover = co.failover(expected_lease_version=2, now_ms=failure_at)
    assert failover["lease_version"] == 3
    assert failover["leader"]["id"] == "bravo"
    assert failover["failover"]["trigger_reason"] == "leader_heartbeat_timeout"
    return {
        "lease_version_after_initial_election": campaign["lease_version"],
        "lease_version_after_renew": renewed["lease_version"],
        "no_candidate_error": no_candidate,
        "lease_version_preserved_when_no_candidate": 2,
        "failover_lease_version": failover["lease_version"],
        "new_leader_id": failover["leader"]["id"],
        "failover_record": failover["failover"],
        "election_excerpt": compact_election(failover),
        "old_leader_persisted_status": co.get_node(
            "alpha", now_ms=failure_at
        )["stored_status"],
    }


def run_forced_termination_recovery(work: Path) -> dict[str, Any]:
    dataset = load_sample(SMALL_SAMPLE)
    db_path = work / "forced-recovery.sqlite"
    setup = new_coordinator(db_path, dataset)
    register_dataset(setup, dataset)
    campaign = setup.campaign("bravo", now_ms=dataset.base_epoch_ms)
    assert campaign["lease_version"] == 1
    crash_heartbeat_at = dataset.base_epoch_ms + 2000
    setup.heartbeat(
        "alpha", heartbeat_ms=crash_heartbeat_at, now_ms=crash_heartbeat_at
    )

    child = subprocess.run(
        [
            sys.executable,
            "-m",
            "tests.crash_child",
            str(db_path),
            "--now-ms",
            str(crash_heartbeat_at + 100),
            "--node",
            "alpha",
            "--expected-version",
            "1",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert child.returncode == 91, child.stderr

    with sqlite3.connect(db_path) as conn:
        pre_recovery_version = conn.execute(
            "SELECT version FROM lease WHERE singleton = 1"
        ).fetchone()[0]
        pre_recovery_intent = conn.execute(
            "SELECT status, kind FROM intents ORDER BY created_at_ms DESC LIMIT 1"
        ).fetchone()
    assert pre_recovery_version == 1
    assert pre_recovery_intent == ("PENDING", "RENEW")

    recovery_at = dataset.base_epoch_ms + int(
        dataset.scenarios.get("recovery_offset_ms", 9000)
    )
    recovered = new_coordinator(
        db_path,
        dataset,
        now_ms=recovery_at,
        auto_recover=True,
    )
    report = recovered.get_recovery_report()
    assert report["pending_intents_aborted_count"] == 1
    assert report["last_complete_lease"]["version"] == 1
    assert report["heartbeat_recheck"]["requires_failover"] is True
    assert "alpha" in report["heartbeat_recheck"]["stale_lease_holder_ids"]
    assert recovered.get_lease(now_ms=recovery_at)["lease_version"] == 1

    recovered.heartbeat("bravo", heartbeat_ms=recovery_at, now_ms=recovery_at)
    version_after_recovery = recovered.get_lease(now_ms=recovery_at)["lease_version"]
    post_recovery_failover = recovered.failover(
        expected_lease_version=1, now_ms=recovery_at
    )
    assert post_recovery_failover["lease_version"] == 2
    return {
        "crash_returncode": child.returncode,
        "pre_recovery_intent": {
            "status": pre_recovery_intent[0],
            "kind": pre_recovery_intent[1],
        },
        "pre_recovery_lease_version": pre_recovery_version,
        "lease_version_after_recovery": version_after_recovery,
        "recovery_report": report,
        "failover_after_recovery": {
            "lease_version": post_recovery_failover["lease_version"],
            "new_leader_id": post_recovery_failover["leader"]["id"],
            "trigger_reason": post_recovery_failover["failover"][
                "trigger_reason"
            ],
        },
    }


def main() -> int:
    started = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="leasecoord-acceptance-") as temp:
        work = Path(temp)
        report: dict[str, Any] = {
            "acceptance_version": 1,
            "generated_at_epoch_ms": int(time.time() * 1000),
            "environment": {
                "python": platform.python_version(),
                "platform": platform.platform(),
                "sqlite": sqlite3.sqlite_version,
                "numpy": np.__version__,
            },
            "samples": {
                "small": str(SMALL_SAMPLE.relative_to(ROOT)),
                "large": str(LARGE_SAMPLE.relative_to(ROOT)),
            },
            "scenarios": {},
        }
        report["scenarios"]["small_adaptive_election"] = run_small_adaptive(work)
        report["scenarios"]["large_adaptive_election"] = run_large_adaptive(work)
        report["scenarios"]["concurrent_campaign"] = run_concurrency(work)
        report["scenarios"]["multiprocess_campaign"] = run_multiprocess_campaign(
            work
        )
        report["scenarios"]["failover"] = run_failover(work)
        report["scenarios"]["forced_termination_recovery"] = (
            run_forced_termination_recovery(work)
        )
        report["summary"] = {
            "all_passed": True,
            "adaptive_paths_same_winner_small": report["scenarios"][
                "small_adaptive_election"
            ]["same_winner"],
            "adaptive_paths_same_winner_large_4096": report["scenarios"][
                "large_adaptive_election"
            ]["same_winner"],
            "large_sampling_rate": report["scenarios"][
                "large_adaptive_election"
            ]["sampled"]["sampling_rate"],
            "concurrent_success_count": report["scenarios"][
                "concurrent_campaign"
            ]["success_count"],
            "multiprocess_success_count": report["scenarios"][
                "multiprocess_campaign"
            ]["success_count"],
            "failover_lease_version": report["scenarios"]["failover"][
                "failover_lease_version"
            ],
            "recovery_lease_version_preserved": report["scenarios"][
                "forced_termination_recovery"
            ]["lease_version_after_recovery"],
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "artifact": str(OUTPUT.relative_to(ROOT)),
                "summary": report["summary"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
