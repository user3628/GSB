from __future__ import annotations

import fnmatch
import hashlib
from collections import Counter, defaultdict
from typing import Any, Iterable

from .models import MirrorRecord, sha256_json


def _matches_any(path_template: str, patterns: Iterable[str]) -> bool:
    return any(fnmatch.fnmatchcase(path_template, pattern) for pattern in patterns)


def deterministic_unit(seed: int, record_key: str) -> float:
    digest = hashlib.blake2b(f"{seed}:{record_key}".encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") / float(2**64)


def _percent(part: int, total: int) -> float:
    return round(part / total, 6) if total else 0.0


def select_records(records: list[MirrorRecord], policy: dict[str, Any]) -> dict[str, Any]:
    ordered = sorted(records, key=lambda item: (item.timestamp_ms, item.sampling_sequence, item.record_key))
    include = policy["include_path_templates"]
    exclude = policy["exclude_path_templates"]
    weights = policy["service_weights"]
    global_rate = float(policy["sampling_rate"])
    seed = int(policy["seed"])
    dedup_window_ms = int(policy["dedup_window_ms"])

    selected: list[MirrorRecord] = []
    decisions: list[dict[str, Any]] = []
    reasons: Counter[str] = Counter()
    last_selected_at: dict[str, int] = {}
    service_stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"records": 0, "eligible": 0, "selected": 0, "random_selected": 0, "dedup_suppressed": 0}
    )

    for record in ordered:
        stats = service_stats[record.destination_service]
        stats["records"] += 1
        weight = float(weights.get(record.destination_service, 1.0))
        effective_rate = min(1.0, max(0.0, global_rate * weight))
        reason: str
        if _matches_any(record.path_template, exclude):
            reason = "path_excluded"
        elif include and not _matches_any(record.path_template, include):
            reason = "path_not_included"
        else:
            stats["eligible"] += 1
            draw = deterministic_unit(seed, record.sampling_sequence)
            if draw >= effective_rate:
                reason = "sampled_out"
            else:
                stats["random_selected"] += 1
                previous = last_selected_at.get(record.connection_id)
                if previous is not None and record.timestamp_ms - previous <= dedup_window_ms:
                    reason = "dedup_window"
                    stats["dedup_suppressed"] += 1
                else:
                    reason = "selected"
                    last_selected_at[record.connection_id] = record.timestamp_ms
                    stats["selected"] += 1
                    selected.append(record)
        reasons[reason] += 1
        decisions.append(
            {
                "record_key": record.record_key,
                "timestamp_ms": record.timestamp_ms,
                "connection_id": record.connection_id,
                "destination_service": record.destination_service,
                "path_template": record.path_template,
                "sampling_sequence": record.sampling_sequence,
                "weight": weight,
                "effective_rate": round(effective_rate, 8),
                "draw": round(deterministic_unit(seed, record.sampling_sequence), 10),
                "decision": reason,
            }
        )

    by_service: dict[str, Any] = {}
    for service in sorted(service_stats):
        value = service_stats[service]
        by_service[service] = {
            **value,
            "weight": float(weights.get(service, 1.0)),
            "effective_rate": round(min(1.0, global_rate * float(weights.get(service, 1.0))), 8),
            "coverage": _percent(value["selected"], value["records"]),
        }

    rejection_counts = {
        "path_excluded": reasons["path_excluded"],
        "path_not_included": reasons["path_not_included"],
        "sampled_out": reasons["sampled_out"],
        "dedup_window": reasons["dedup_window"],
    }
    content = {
        "seed": seed,
        "policy_version": policy["policy_version"],
        "record_keys": [record.record_key for record in ordered],
        "selected_keys": [record.record_key for record in selected],
        "decisions": decisions,
    }
    return {
        "artifact_type": "selection",
        "artifact_id": f"sel-{sha256_json(content)[:20]}",
        "policy_version": policy["policy_version"],
        "seed": seed,
        "sampling_rate": global_rate,
        "dedup_window_ms": dedup_window_ms,
        "total_records": len(ordered),
        "eligible_records": len(ordered) - rejection_counts["path_excluded"] - rejection_counts["path_not_included"],
        "selected_count": len(selected),
        "coverage": _percent(len(selected), len(ordered)),
        "rejection_counts": rejection_counts,
        "by_service": by_service,
        "selected_keys": [record.record_key for record in selected],
        "selected_records": [record.public_dict() for record in selected],
        "decisions": decisions,
    }


def selected_from_selection(records: list[MirrorRecord], selection: dict[str, Any]) -> list[MirrorRecord]:
    selected_keys = set(selection["selected_keys"])
    return [
        record
        for record in sorted(records, key=lambda item: (item.timestamp_ms, item.sampling_sequence, item.record_key))
        if record.record_key in selected_keys
    ]
