from __future__ import annotations

from collections import Counter
from typing import Any, Iterable

import numpy as np

from .models import MirrorRecord, sha256_json


def _round(value: float) -> float:
    return round(float(value), 6)


def _quantile(values: list[int], percentile: int) -> int:
    if not values:
        return 0
    return int(np.percentile(np.asarray(values, dtype=np.int64), percentile, method="nearest"))


def basic_stats(records: Iterable[MirrorRecord]) -> dict[str, Any]:
    values = list(records)
    total = len(values)
    statuses = Counter(str(record.status_code) for record in values)
    successes = sum(1 for record in values if record.status_code < 400)
    durations = [record.duration_us for record in values]
    return {
        "records": total,
        "success_count": successes,
        "failure_count": total - successes,
        "success_rate": _round(successes / total) if total else 0.0,
        "status_distribution": dict(sorted(statuses.items(), key=lambda item: int(item[0]))),
        "latency_us": {
            "p50": _quantile(durations, 50),
            "p95": _quantile(durations, 95),
            "p99": _quantile(durations, 99),
            "mean": _round(sum(durations) / total) if total else 0.0,
        },
        "response_bytes_total": sum(record.response_bytes for record in values),
        "duration_us_total": sum(durations),
    }


def _distribution_delta(left: dict[str, Any], right: dict[str, Any]) -> dict[str, int]:
    keys = sorted({int(key) for key in set(left) | set(right)})
    return {str(key): int(right.get(str(key), 0)) - int(left.get(str(key), 0)) for key in keys}


def build_bias(
    records: list[MirrorRecord],
    selection: dict[str, Any],
    batches: dict[str, Any],
    replay: dict[str, Any] | None = None,
) -> dict[str, Any]:
    selected_keys = set(selection["selected_keys"])
    selected = [record for record in records if record.record_key in selected_keys]
    online = basic_stats(records)
    sampled = basic_stats(selected)
    sample_delta = _round(sampled["success_rate"] - online["success_rate"])
    sampling_bias = {
        "numerator": "sampled records versus all online records",
        "record_coverage": selection["coverage"],
        "byte_coverage": _round(
            sampled["response_bytes_total"] / online["response_bytes_total"]
        )
        if online["response_bytes_total"]
        else 0.0,
        "success_rate_delta_vs_online": sample_delta,
        "latency_p95_delta_us_vs_online": sampled["latency_us"]["p95"] - online["latency_us"]["p95"],
        "latency_p99_delta_us_vs_online": sampled["latency_us"]["p99"] - online["latency_us"]["p99"],
        "status_distribution_delta_vs_online": _distribution_delta(
            online["status_distribution"], sampled["status_distribution"]
        ),
        "rejection_counts": selection["rejection_counts"],
        "sources": {
            "path_rule_effect": {
                "excluded": selection["rejection_counts"]["path_excluded"],
                "not_included": selection["rejection_counts"]["path_not_included"],
            },
            "global_sampling_effect": {
                "sampled_out": selection["rejection_counts"]["sampled_out"],
                "sampling_rate": selection["sampling_rate"],
            },
            "connection_dedup_effect": {
                "suppressed": selection["rejection_counts"]["dedup_window"],
            },
            "service_weight_effect": {
                service: {
                    "weight": stats["weight"],
                    "effective_rate": stats["effective_rate"],
                    "coverage": stats["coverage"],
                    "selected": stats["selected"],
                    "records": stats["records"],
                }
                for service, stats in selection["by_service"].items()
            },
        },
        "classification": "sampling",
    }
    result: dict[str, Any] = {
        "artifact_type": "bias",
        "policy_version": selection["policy_version"],
        "online_original": online,
        "sample": sampled,
        "sampling_bias": sampling_bias,
        "replay": replay,
        "replay_amplification_bias": None,
        "attribution": {
            "separated": True,
            "sampling_component": {
                "classification": "sampling",
                "success_rate_delta_vs_online": sample_delta,
                "record_coverage": selection["coverage"],
            },
            "replay_failure_component": None,
            "timeout_component": None,
        },
    }
    if replay is not None:
        replay_success = float(replay["success_rate"])
        replay_delta = _round(replay_success - sampled["success_rate"])
        result["replay_amplification_bias"] = {
            "target_multiplier": batches["target_multiplier"],
            "effective_multiplier": batches["effective_multiplier"],
            "downgrade_applied": batches["downshift"]["applied"],
            "downgrade_affected_samples": batches["downshift"]["affected_samples"],
            "selected_samples": sampled["records"],
            "attempted_operations": replay["attempted"],
            "attempted_amplification": _round(replay["attempted"] / sampled["records"])
            if sampled["records"]
            else 0.0,
            "success_rate_delta_vs_sample": replay_delta,
            "latency_p95_delta_us_vs_sample": replay["latency_us"]["p95"] - sampled["latency_us"]["p95"],
            "status_distribution_delta_vs_sample": _distribution_delta(
                sampled["status_distribution"], replay["status_distribution"]
            ),
            "classification": "replay",
        }
        result["attribution"] = {
            "separated": True,
            "sampling_component": {
                "classification": "sampling",
                "success_rate_delta_vs_online": sample_delta,
                "record_coverage": selection["coverage"],
                "explanation": "Online-versus-sampled delta is caused by policy filtering, weighted sampling and connection dedup.",
            },
            "replay_failure_component": {
                "classification": "replay_failure",
                "success_rate_delta_vs_sample": replay_delta,
                "failure_count": replay["failure_count"],
                "timeout_count": replay["timeout_count"],
                "explanation": "Sampled-versus-replay delta is caused by replay execution, not by sampling.",
            },
            "timeout_component": {
                "classification": "replay_timeout",
                "count": replay["timeout_count"],
                "error_code": "E_REPLAY_TIMEOUT" if replay["timeout_count"] else None,
                "counted_as_replay_failure": True,
            },
            "replay_run": {
                "run_id": replay.get("run_id"),
                "status": replay.get("status"),
                "source_version": replay.get("source_version"),
                "batch_ids": replay.get("batch_ids"),
            },
        }
    result["artifact_id"] = f"bias-{sha256_json(result)[:20]}"
    return result
