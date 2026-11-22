"""Deterministic per-layer sampling and mixed-set assembly."""

from __future__ import annotations

import hashlib
from collections import defaultdict
from typing import Any, Callable

from .config import ASSEMBLE_ALGORITHM
from .errors import E_VALIDATION_FAILED, PipelineError
from .util import sha256_bytes
from .workspace import mixed_product_bytes


def selection_score(seed: str, sample_id: str) -> str:
    payload = f"{seed}\0{sample_id}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def assemble(
    normalized_payload: dict[str, Any],
    allocation_payload: dict[str, Any],
    config: dict[str, Any],
    *,
    check_stop: Callable[[], None] | None = None,
) -> dict[str, Any]:
    seed = config["assembly"]["seed"]
    by_layer: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in normalized_payload["kept"]:
        by_layer[(record["source"], record["lang"])].append(record)

    quotas = {
        (row["source"], row["lang"]): int(row["allocated"])
        for row in allocation_payload["layers"]
    }
    selected_rows: list[dict[str, Any]] = []
    counts_by_layer: dict[str, int] = {}

    for source, lang in sorted(quotas):
        quota = quotas[(source, lang)]
        candidates = by_layer.get((source, lang), [])
        ranked = sorted(
            candidates,
            key=lambda record: (selection_score(seed, record["id"]), record["id"]),
        )
        if quota > len(ranked):
            layer_id = f"{source}::{lang}"
            raise PipelineError(
                E_VALIDATION_FAILED,
                "allocation requests more samples than the layer contains",
                {
                    "layer_id": layer_id,
                    "requested": quota,
                    "available": len(ranked),
                },
                http_status=409,
            )
        layer_id = f"{source}::{lang}"
        counts_by_layer[layer_id] = quota
        for record in ranked[:quota]:
            selected_rows.append(
                {
                    "id": record["id"],
                    "source": source,
                    "lang": lang,
                    "text": record["text"],
                    "selection_score": selection_score(seed, record["id"]),
                }
            )
            if check_stop is not None:
                check_stop()

    selected_rows.sort(
        key=lambda row: (
            row["source"],
            row["lang"],
            row["selection_score"],
            row["id"],
        )
    )
    payload: dict[str, Any] = {
        "algorithm": ASSEMBLE_ALGORITHM,
        "seed": seed,
        "rows": selected_rows,
        "ordered_ids": [row["id"] for row in selected_rows],
        "counts_by_layer": counts_by_layer,
        "total_selected": len(selected_rows),
    }
    payload["mixed_digest"] = sha256_bytes(mixed_product_bytes(payload))
    return payload
