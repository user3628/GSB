"""Canonical JSON and content fingerprints used by checkpoints and invalidation."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable

import numpy as np


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def array_payload_digest(array: np.ndarray) -> str:
    """Digest logical array values independent of memory layout.

    The digest includes dtype, shape, and C-order payload bytes. NaN payload
    bits are intentionally not normalized.
    """

    contiguous = np.ascontiguousarray(array)
    digest = hashlib.sha256()
    digest.update(contiguous.dtype.str.encode("ascii"))
    digest.update(b"\x00")
    digest.update(canonical_json_bytes(list(contiguous.shape)))
    digest.update(b"\x00")
    digest.update(memoryview(contiguous).cast("B"))
    return digest.hexdigest()


def array_file_digest(array: np.ndarray) -> str:
    """Digest the deterministic in-memory .npy bytes of an array."""

    import io

    buffer = io.BytesIO()
    np.save(buffer, np.ascontiguousarray(array), allow_pickle=False)
    return sha256_bytes(buffer.getvalue())


def combine_digests(*parts: str | bytes | dict[str, Any] | list[Any]) -> str:
    digest = hashlib.sha256()
    for part in parts:
        if isinstance(part, bytes):
            payload = part
        elif isinstance(part, str):
            payload = part.encode("utf-8")
        else:
            payload = canonical_json_bytes(part)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def shape_fingerprint(
    *,
    kind: str,
    m: int,
    n: int,
    k: int,
    batch: int,
    dtype: str,
    a_transpose: bool,
    b_transpose: bool,
) -> str:
    return sha256_json(
        {
            "schema": "blockgemm.shape.v1",
            "kind": kind,
            "M": m,
            "N": n,
            "K": k,
            "batch": batch,
            "dtype": dtype,
            "a_transpose": bool(a_transpose),
            "b_transpose": bool(b_transpose),
        }
    )


def static_version(
    *,
    shape_fp: str,
    plan_fp: str,
    accumulation_strategy: str,
    l1_cache_kib: int,
    l2_cache_kib: int,
    verification: dict[str, Any],
    kernel_version: str,
) -> tuple[dict[str, Any], str]:
    components = {
        "shape_fingerprint": shape_fp,
        "plan_fingerprint": plan_fp,
        "accumulation_strategy": accumulation_strategy,
        "l1_cache_kib": int(l1_cache_kib),
        "l2_cache_kib": int(l2_cache_kib),
        "verification": verification,
        "kernel_version": kernel_version,
        "layout_schema": "batch-major-row-major-v1",
    }
    return components, sha256_json(components)


def version_fingerprint(
    static_components: dict[str, Any],
    *,
    a_content_digest: str,
    b_content_digest: str,
) -> str:
    return sha256_json(
        {
            "static_version": sha256_json(static_components),
            "a_content_digest": a_content_digest,
            "b_content_digest": b_content_digest,
            "version_schema": "blockgemm.version.v1",
        }
    )


def block_input_digest(
    *,
    a_stage_digest: str,
    b_stage_digest: str,
    m_range: Iterable[int],
    n_range: Iterable[int],
    k: int,
    batch_index: int | None,
) -> str:
    return sha256_json(
        {
            "a_stage_digest": a_stage_digest,
            "b_stage_digest": b_stage_digest,
            "m_range": list(m_range),
            "n_range": list(n_range),
            "K": int(k),
            "batch_index": batch_index,
            "schema": "blockgemm.block-input.v1",
        }
    )
