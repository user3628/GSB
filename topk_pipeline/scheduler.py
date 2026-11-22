"""Deterministic three-constraint admission and concurrency planning."""

from __future__ import annotations

from typing import Any

import numpy as np

from .errors import (
    E_BUDGET_EXCEEDED,
    E_INVALID_K,
    E_MEMORY_BUDGET,
    PipelineError,
)
from .manifest import estimate_shard


def _valid_k(k: Any, vocab_size: int) -> bool:
    return (
        isinstance(k, (int, np.integer))
        and not isinstance(k, bool)
        and 0 < int(k) <= vocab_size
    )


def _rejection(
    spec: dict[str, Any],
    *,
    code: str,
    stage: str,
    reason: str,
    message: str,
    estimate: dict[str, int],
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = dict(details or {})
    payload["reason"] = reason
    payload["estimate"] = estimate
    return {
        "shard_id": spec["shard_id"],
        "shard_ordinal": int(spec["shard_ordinal"]),
        "status": "unprocessed",
        "error": PipelineError(
            code=code,
            message=message,
            stage=stage,
            shard_id=spec["shard_id"],
            k=spec.get("k") if isinstance(spec.get("k"), int) else None,
            details=payload,
        ).to_dict(),
        "estimate": estimate,
    }


def schedule_shards(manifest: dict[str, Any]) -> dict[str, Any]:
    """Greedily accept shards in priority/declaration order under all limits."""

    budget = manifest["budget"]
    requested_workers = int(manifest["workers"])
    global_limit = int(budget["global_elements"])
    worker_limit = int(budget["worker_elements"])
    memory_limit = int(budget["peak_memory_bytes"])

    ordered = sorted(
        manifest["shards"],
        key=lambda spec: (-int(spec.get("priority", 0)), int(spec["shard_ordinal"])),
    )
    accepted: list[dict[str, Any]] = []
    unprocessed: list[dict[str, Any]] = []
    consumed_elements = 0

    for spec in ordered:
        estimate = estimate_shard(spec)
        rows, vocab_size = (int(value) for value in spec["shape"])
        if not _valid_k(spec.get("k"), vocab_size):
            unprocessed.append(
                _rejection(
                    spec,
                    code=E_INVALID_K,
                    stage="filter",
                    reason="invalid_k",
                    message=f"K must be in [1, {vocab_size}], received {spec.get('k')!r}",
                    estimate=estimate,
                    details={"vocab_size": vocab_size, "k": spec.get("k")},
                )
            )
            continue
        if estimate["memory_bytes"] > memory_limit:
            unprocessed.append(
                _rejection(
                    spec,
                    code=E_MEMORY_BUDGET,
                    stage="schedule",
                    reason="shard_peak_memory_exceeds_limit",
                    message="conservative shard memory estimate exceeds the peak memory limit",
                    estimate=estimate,
                    details={"peak_memory_bytes": memory_limit},
                )
            )
            continue
        if consumed_elements + estimate["elements"] > global_limit:
            unprocessed.append(
                _rejection(
                    spec,
                    code=E_BUDGET_EXCEEDED,
                    stage="schedule",
                    reason="global_element_budget",
                    message="shard would exceed the global element evaluation budget",
                    estimate=estimate,
                    details={
                        "global_elements": global_limit,
                        "consumed_elements": consumed_elements,
                    },
                )
            )
            continue

        # A shard is the smallest scheduling/evaluation unit and is handled by
        # one worker process, so its conservative element estimate must fit the
        # single-worker quota. This rule is independent of requested
        # concurrency, keeping admission identical for serial and parallel runs.
        if estimate["elements"] > worker_limit:
            unprocessed.append(
                _rejection(
                    spec,
                    code=E_BUDGET_EXCEEDED,
                    stage="schedule",
                    reason="worker_element_quota",
                    message="shard exceeds the single-worker element quota",
                    estimate=estimate,
                    details={"worker_elements": worker_limit},
                )
            )
            continue

        consumed_elements += estimate["elements"]
        accepted.append(
            {
                "spec": spec,
                "estimate": estimate,
                "acceptance_rank": len(accepted) + 1,
            }
        )

    concurrency = min(requested_workers, len(accepted))
    if accepted:
        memories = sorted(
            (int(item["estimate"]["memory_bytes"]) for item in accepted), reverse=True
        )
        while concurrency > 0 and sum(memories[:concurrency]) > memory_limit:
            concurrency -= 1
    if accepted and concurrency < 1:
        # Every shard individually fits; this is only reachable with an
        # inconsistent limit and is treated as a hard planning failure.
        raise PipelineError(
            code=E_MEMORY_BUDGET,
            message="no legal concurrency level exists for accepted shards",
            stage="schedule",
            details={"peak_memory_bytes": memory_limit},
        )

    accepted_memory = sorted(
        (int(item["estimate"]["memory_bytes"]) for item in accepted), reverse=True
    )
    return {
        "accepted": accepted,
        "unprocessed": sorted(unprocessed, key=lambda item: item["shard_ordinal"]),
        "concurrency": int(concurrency),
        "consumed_elements": int(consumed_elements),
        "global_elements": global_limit,
        "worker_elements": worker_limit,
        "peak_memory_bytes": memory_limit,
        "scheduled_peak_memory_bytes": int(sum(accepted_memory[:concurrency])),
        "acceptance_order": [item["spec"]["shard_id"] for item in accepted],
    }
