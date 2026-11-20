from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from collections import Counter
from dataclasses import replace
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from gateway_middleware.auth import make_token_digest  # noqa: E402
from gateway_middleware.config import Settings  # noqa: E402
from gateway_middleware.errors import E_RATE_LIMITED, DomainError  # noqa: E402
from gateway_middleware.models import BatchInput, EventInput, QuotaGroup, TokenState  # noqa: E402
from gateway_middleware.sample_data import (  # noqa: E402
    decision_distribution,
    generate_large,
    generate_small,
    seed_dataset,
)
from gateway_middleware.service import GatewayService  # noqa: E402


def _rate_counts(decisions, window_name: str) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for decision in decisions:
        window = getattr(decision, window_name)
        if window is None:
            continue
        counts[f"{window.sample_rate_num}/{window.sample_rate_den}"] += 1
    return dict(sorted(counts.items(), key=lambda item: float(item[0].split("/")[0]) / float(item[0].split("/")[1])))


def _ci_audit(decisions, window_name: str) -> dict[str, Any]:
    sampled = []
    for decision in decisions:
        window = getattr(decision, window_name)
        if window is None or window.method == "exact":
            continue
        sampled.append(
            {
                "covered": window.confidence_low <= window.exact_weight <= window.confidence_high,
                "width": window.confidence_high - window.confidence_low,
                "exact": window.exact_weight,
                "estimated": window.estimated_weight,
                "low": window.confidence_low,
                "high": window.confidence_high,
            }
        )
    if not sampled:
        return {"sampled_windows": 0, "coverage_ratio": None, "mean_interval_width": None}
    return {
        "sampled_windows": len(sampled),
        "coverage_ratio": sum(item["covered"] for item in sampled) / len(sampled),
        "mean_interval_width": sum(item["width"] for item in sampled) / len(sampled),
        "points": sampled[:3],
    }


def summarize_result(result, *, profile: str, duration_s: float, db_path: Path) -> dict[str, Any]:
    decisions = result.decisions
    trigger_counts = Counter(
        decision.trigger_window
        for decision in decisions
        if decision.decision_code == E_RATE_LIMITED
    )
    auth_reasons = Counter(
        decision.auth_reason for decision in decisions if not decision.auth_ok
    )
    sampled = [
        decision
        for decision in decisions
        if (decision.short and decision.short.method != "exact")
        or (decision.long and decision.long.method != "exact")
    ]
    sample_example = None
    if sampled:
        decision = sampled[0]
        sample_example = {
            "request_id": decision.request_id,
            "route_key": decision.route_key,
            "client_id": decision.client_id,
            "decision_code": decision.decision_code,
            "trigger_window": decision.trigger_window,
            "short": decision.short.to_dict() if decision.short else None,
            "long": decision.long.to_dict() if decision.long else None,
            "quotas": {
                "short": decision.short_quota,
                "long": decision.long_quota,
                "scale_ppm": decision.quota_scale_ppm,
            },
        }
    return {
        "profile": profile,
        "batch_id": result.batch_id,
        "event_count": result.event_count,
        "decision_distribution": decision_distribution(decisions),
        "trigger_windows": dict(sorted(trigger_counts.items())),
        "auth_failure_reasons": dict(sorted(auth_reasons.items())),
        "sampled_decision_count": len(sampled),
        "short_sample_rates": _rate_counts(decisions, "short"),
        "long_sample_rates": _rate_counts(decisions, "long"),
        "short_ci_audit": _ci_audit(decisions, "short"),
        "long_ci_audit": _ci_audit(decisions, "long"),
        "sample_example": sample_example,
        "base_version": result.base_version,
        "commit_version": result.commit_version,
        "duration_seconds": round(duration_s, 3),
        "sqlite_bytes": db_path.stat().st_size,
        "writer_queue_policy": "queue_as_next_batch",
    }


def run_profile(
    *,
    dataset_name: str,
    settings: Settings,
    db_path: Path,
    event_count: int | None,
) -> tuple[GatewayService, dict[str, Any]]:
    _reset_db(db_path)
    profile_settings = settings.with_db(db_path)
    service = GatewayService(profile_settings)
    if dataset_name == "small":
        dataset = generate_small(profile_settings)
    else:
        dataset = generate_large(profile_settings, event_count=event_count or 100_000)
    seeded_version = seed_dataset(service, dataset)
    start = time.perf_counter()
    result = service.submit_batch(dataset.batch(f"reproduce-{dataset_name}-1"))
    duration = time.perf_counter() - start
    summary = summarize_result(
        result,
        profile=dataset_name,
        duration_s=duration,
        db_path=db_path,
    )
    summary["seeded_version"] = seeded_version
    summary["sample_seed"] = dataset.seed
    return service, summary


def _audit_registry(settings: Settings):
    group = QuotaGroup("audit", short_limit_weight=60, long_limit_weight=1_000, target_weight_per_sec=60)
    expires = 1_700_000_100_000
    digest = make_token_digest(
        settings.local_secret,
        token_id="tok-audit",
        client_id="audit-client",
        quota_group="audit",
        expires_at_ms=expires,
        active=True,
        issued_at_ms=1_699_999_000_000,
    )
    token = TokenState(
        token_id="tok-audit",
        client_id="audit-client",
        quota_group="audit",
        expires_at_ms=expires,
        digest_hex=digest,
        active=True,
        issued_at_ms=1_699_999_000_000,
    )
    return group, token


def sampling_path_comparison(
    *,
    base_settings: Settings,
    workdir: Path,
    event_count: int = 600,
) -> dict[str, Any]:
    group, token = _audit_registry(base_settings)
    base_ts = 1_700_000_000_000
    events = [
        EventInput(
            event_seq=index,
            request_id=f"audit-{index:06d}",
            timestamp_ms=base_ts + (index % 10),
            route_key="/audit",
            client_id=token.client_id,
            token_id=token.token_id,
            token_digest=token.digest_hex,
            weight=1,
        )
        for index in range(event_count)
    ]
    batch = BatchInput(batch_id="sampling-path-comparison", events=events)

    runs = {}
    for name, overrides in (
        ("exact", {"short_exact_limit": 1_000_000, "long_exact_limit": 1_000_000}),
        ("sampled", {"short_exact_limit": 64, "long_exact_limit": 128}),
    ):
        db_path = workdir / f"comparison-{name}.sqlite3"
        _reset_db(db_path)
        settings = replace(base_settings, db_path=db_path, **overrides)
        service = GatewayService(settings)
        service.store.seed_local_registry(
            quota_groups=(group,),
            client_groups={token.client_id: token.quota_group},
            tokens=(token,),
        )
        started = time.perf_counter()
        result = service.submit_batch(batch)
        duration = time.perf_counter() - started
        runs[name] = {
            "service": service,
            "result": result,
            "summary": summarize_result(
                result,
                profile=f"sampling-comparison-{name}",
                duration_s=duration,
                db_path=db_path,
            ),
        }

    exact = runs["exact"]["result"]
    sampled = runs["sampled"]["result"]
    exact_by_request = {decision.request_id: decision for decision in exact.decisions}
    sampled_by_request = {decision.request_id: decision for decision in sampled.decisions}
    mismatch = []
    for request_id, exact_decision in exact_by_request.items():
        sampled_decision = sampled_by_request[request_id]
        if exact_decision.decision_code != sampled_decision.decision_code:
            mismatch.append(
                {
                    "request_id": request_id,
                    "exact": exact_decision.decision_code,
                    "sampled": sampled_decision.decision_code,
                }
            )

    last_ts = base_ts + 9
    exact_window = runs["exact"]["service"].query_windows(
        route_key="/audit", client_id=token.client_id, at_ms=last_ts
    )
    sampled_window = runs["sampled"]["service"].query_windows(
        route_key="/audit", client_id=token.client_id, at_ms=last_ts
    )
    return {
        "same_batch": batch.batch_id,
        "event_count": event_count,
        "exact": runs["exact"]["summary"],
        "sampled": runs["sampled"]["summary"],
        "decision_mismatch_count": len(mismatch),
        "decision_mismatch_examples": mismatch[:10],
        "final_short_window": {
            "exact_count": exact_window["short"]["exact_weight"],
            "sampled_estimate": sampled_window["short"]["estimated_weight"],
            "confidence_low": sampled_window["short"]["confidence_low"],
            "confidence_high": sampled_window["short"]["confidence_high"],
            "true_count_inside_interval": sampled_window["short"]["confidence_low"]
            <= exact_window["short"]["exact_weight"]
            <= sampled_window["short"]["confidence_high"],
            "sampling_rate": sampled_window["short"]["sample_rate"],
        },
        "explanation": (
            "Both runs used the same event set and threshold logic. The sampled run's "
            "differences are auditable against the reported Horvitz-Thompson interval; "
            "the exact run remains deterministic and uses integer prefix sums only."
        ),
    }


def recovery_demo(*, base_settings: Settings, workdir: Path) -> dict[str, Any]:
    db_path = workdir / "recovery-demo.sqlite3"
    marker = workdir / "recovery-crash-marker.json"
    _reset_db(db_path)
    settings = base_settings.with_db(db_path)
    service = GatewayService(settings)
    dataset = generate_small(settings)
    seeded_version = seed_dataset(service, dataset)
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "gateway_middleware.crash_worker",
            "--db",
            str(db_path),
            "--marker",
            str(marker),
            "--batch-id",
            "recovery-demo-batch",
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=60,
    )
    marker_payload = json.loads(marker.read_text(encoding="utf-8")) if marker.exists() else None
    restarted = GatewayService(settings)
    report = restarted.recover()
    version_after = restarted.snapshot_info()["version"]
    try:
        restarted.get_batch("recovery-demo-batch")
        aborted_code = None
    except DomainError as exc:
        aborted_code = exc.code
    return {
        "worker_exit_code": completed.returncode,
        "forced_termination_marker": marker_payload,
        "seeded_version": seeded_version,
        "version_after_recovery": version_after,
        "state_version_unchanged": seeded_version == version_after,
        "aborted_batch_lookup_code": aborted_code,
        "recovery_report": report.to_dict(),
        "worker_stderr_tail": completed.stderr[-1000:],
    }


def _reset_db(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    for candidate in (path, Path(str(path) + "-wal"), Path(str(path) + "-shm")):
        if candidate.exists():
            candidate.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(description="Reproduce local gateway experiments")
    parser.add_argument("--workdir", default="artifacts/experiment")
    parser.add_argument("--output", default="artifacts/experiment_results.json")
    parser.add_argument("--large-events", type=int, default=100_000)
    parser.add_argument("--comparison-events", type=int, default=600)
    parser.add_argument("--skip-large", action="store_true")
    parser.add_argument("--skip-recovery", action="store_true")
    args = parser.parse_args()

    root = Path.cwd()
    workdir = (root / args.workdir).resolve()
    output = (root / args.output).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    base_settings = Settings.from_env()

    started = time.perf_counter()
    _, small_summary = run_profile(
        dataset_name="small",
        settings=base_settings,
        db_path=workdir / "small.sqlite3",
        event_count=None,
    )
    print("small:", json.dumps(small_summary["decision_distribution"], ensure_ascii=False))

    large_summary = None
    if not args.skip_large:
        _, large_summary = run_profile(
            dataset_name="large",
            settings=base_settings,
            db_path=workdir / "large.sqlite3",
            event_count=args.large_events,
        )
        print("large:", json.dumps(large_summary["decision_distribution"], ensure_ascii=False))

    comparison = sampling_path_comparison(
        base_settings=base_settings,
        workdir=workdir,
        event_count=args.comparison_events,
    )
    print(
        "sampling comparison mismatch:",
        comparison["decision_mismatch_count"],
        "final interval:",
        comparison["final_short_window"],
    )

    recovery = None
    if not args.skip_recovery:
        recovery = recovery_demo(base_settings=base_settings, workdir=workdir)
        print(
            "recovery:",
            {
                "exit_code": recovery["worker_exit_code"],
                "rolled_back": recovery["recovery_report"]["rolled_back_batches"],
                "state_version_unchanged": recovery["state_version_unchanged"],
            },
        )

    payload = {
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "python": sys.version.split()[0],
        "settings": {
            "short_window": {
                "length_ms": base_settings.short_length_ms,
                "step_ms": base_settings.short_step_ms,
                "exact_limit": base_settings.short_exact_limit,
            },
            "long_window": {
                "length_ms": base_settings.long_length_ms,
                "step_ms": base_settings.long_step_ms,
                "exact_limit": base_settings.long_exact_limit,
            },
            "confidence_level": 0.99,
            "window_boundary": "left_open_right_closed:(at_ms-length_ms, at_ms]",
            "bucket_boundary": "left_closed_right_open:[start,start+step_ms)",
        },
        "small": small_summary,
        "large": large_summary,
        "sampling_path_comparison": comparison,
        "forced_termination_recovery": recovery,
        "total_duration_seconds": round(time.perf_counter() - started, 3),
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"results written to {output}")


if __name__ == "__main__":
    main()
