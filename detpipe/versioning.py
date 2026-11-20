"""Deterministic content fingerprints.

All fingerprints are SHA-256 over canonical JSON (sorted keys, compact
separators, no whitespace).  They are stable across Python runs and across
processes because they never rely on object identity or dict iteration order.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_bytes(obj: Any) -> bytes:
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def fingerprint(obj: Any) -> str:
    return hashlib.sha256(canonical_bytes(obj)).hexdigest()


def short_fp(obj: Any, length: int = 12) -> str:
    return fingerprint(obj)[:length]


def threshold_version(iou_threshold: float, score_threshold: float) -> str:
    return fingerprint(
        {"iou_threshold": iou_threshold, "score_threshold": score_threshold}
    )


def budget_version(max_evaluations: int, worker_quota: int, workers: int) -> str:
    return fingerprint(
        {
            "max_evaluations": max_evaluations,
            "worker_quota": worker_quota,
            "workers": workers,
        }
    )


def sample_fingerprint(sample: dict) -> str:
    """Fingerprint of one image sample (image_id, size, gts, candidates)."""
    payload = {
        "image_id": sample["image_id"],
        "width": sample["width"],
        "height": sample["height"],
        "ground_truth": sample["ground_truth"],
        "candidates": sample["candidates"],
    }
    return fingerprint(payload)
