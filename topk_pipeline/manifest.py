"""Manifest loading, shape discovery and stage-version fingerprints."""

from __future__ import annotations

import copy
import re
from pathlib import Path
from typing import Any

import numpy as np

from .errors import E_INVALID_MANIFEST, PipelineError
from .util import digest_value, read_json


ALGORITHM_VERSION = "topk-sampling-budget-v1"
VALIDATION_VERSION = "validation-v1"
SHARD_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
STAGES = ("filter", "normalize", "sampling", "validation")


def _manifest_error(message: str, **details: Any) -> PipelineError:
    return PipelineError(E_INVALID_MANIFEST, message, "manifest", details=details)


def _as_int(value: Any, field: str, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, np.integer)):
        raise _manifest_error(f"{field} must be an integer", field=field, value=repr(value))
    result = int(value)
    if minimum is not None and result < minimum:
        raise _manifest_error(f"{field} must be >= {minimum}", field=field, value=result)
    return result


def _npy_header(path: Path) -> tuple[tuple[int, ...], np.dtype[Any]]:
    try:
        array = np.load(path, mmap_mode="r", allow_pickle=False)
    except (OSError, ValueError) as exc:
        raise _manifest_error("cannot open logits shard", path=str(path), error=str(exc)) from exc
    try:
        if array.ndim != 2:
            raise _manifest_error(
                "logits shard must be two-dimensional", path=str(path), shape=list(array.shape)
            )
        if not np.issubdtype(array.dtype, np.floating):
            raise _manifest_error(
                "logits shard must use a floating dtype", path=str(path), dtype=str(array.dtype)
            )
        return tuple(int(value) for value in array.shape), array.dtype
    finally:
        mmap = getattr(array, "_mmap", None)
        if mmap is not None:
            mmap.close()


def load_manifest(path: str | Path) -> dict[str, Any]:
    manifest_path = Path(path).resolve()
    try:
        raw = read_json(manifest_path)
    except (OSError, ValueError) as exc:
        raise _manifest_error("cannot read manifest", path=str(manifest_path), error=str(exc)) from exc
    if not isinstance(raw, dict):
        raise _manifest_error("manifest root must be an object")

    defaults = raw.get("defaults", {})
    budget = raw.get("budget", {})
    shards_raw = raw.get("shards")
    if not isinstance(defaults, dict):
        raise _manifest_error("defaults must be an object")
    if not isinstance(budget, dict):
        raise _manifest_error("budget must be an object")
    if not isinstance(shards_raw, list) or not shards_raw:
        raise _manifest_error("shards must be a non-empty list")

    budget_config = {
        "global_elements": _as_int(budget.get("global_elements"), "budget.global_elements", minimum=0),
        "worker_elements": _as_int(budget.get("worker_elements"), "budget.worker_elements", minimum=1),
        "peak_memory_bytes": _as_int(
            budget.get("peak_memory_bytes"), "budget.peak_memory_bytes", minimum=1
        ),
    }
    workers = _as_int(
        raw.get("workers", defaults.get("workers", 2)), "workers", minimum=1
    )

    base_dir = manifest_path.parent
    shards: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for ordinal, source in enumerate(shards_raw):
        if not isinstance(source, dict):
            raise _manifest_error("shard entry must be an object", ordinal=ordinal)
        shard_id = source.get("shard_id")
        if not isinstance(shard_id, str) or not SHARD_ID_RE.fullmatch(shard_id):
            raise _manifest_error(
                "shard_id must match [A-Za-z0-9_.-]+", ordinal=ordinal, shard_id=repr(shard_id)
            )
        if shard_id in seen_ids:
            raise _manifest_error("duplicate shard_id", shard_id=shard_id)
        seen_ids.add(shard_id)

        relative_path = source.get("path")
        if not isinstance(relative_path, str) or not relative_path:
            raise _manifest_error("shard path must be a non-empty string", shard_id=shard_id)
        shard_path = (base_dir / relative_path).resolve()
        shape, dtype = _npy_header(shard_path)
        if len(shape) != 2:
            raise _manifest_error("invalid shard shape", shard_id=shard_id, shape=list(shape))

        expected_shape = source.get("expected_shape")
        if (
            not isinstance(expected_shape, list)
            or len(expected_shape) != 2
            or any(isinstance(value, bool) or not isinstance(value, int) for value in expected_shape)
            or expected_shape[0] < 0
            or expected_shape[1] < 0
        ):
            raise _manifest_error(
                "expected_shape must be [non_negative_rows, non_negative_k]",
                shard_id=shard_id,
            )
        expected_range = source.get("expected_score_range")
        if (
            not isinstance(expected_range, list)
            or len(expected_range) != 2
            or any(isinstance(value, bool) or not isinstance(value, (int, float)) for value in expected_range)
            or float(expected_range[0]) > float(expected_range[1])
        ):
            raise _manifest_error(
                "expected_score_range must be [low, high]", shard_id=shard_id
            )

        row_block = _as_int(
            source.get("row_block", defaults.get("row_block", 8)),
            f"shards[{ordinal}].row_block",
            minimum=1,
        )
        col_block = _as_int(
            source.get("col_block", defaults.get("col_block", 256)),
            f"shards[{ordinal}].col_block",
            minimum=1,
        )
        nucleus_enabled = source.get("nucleus_enabled", False)
        if not isinstance(nucleus_enabled, bool):
            raise _manifest_error("nucleus_enabled must be boolean", shard_id=shard_id)
        top_p = source.get("top_p", 1.0)
        if isinstance(top_p, bool) or not isinstance(top_p, (int, float)):
            raise _manifest_error("top_p must be numeric", shard_id=shard_id)
        seed = source.get("seed", defaults.get("seed", 0))
        if isinstance(seed, bool) or not isinstance(seed, (int, np.integer)):
            raise _manifest_error("seed must be an integer", shard_id=shard_id)

        specification = {
            "shard_id": shard_id,
            "shard_ordinal": ordinal,
            "path": str(shard_path),
            "relative_path": relative_path,
            "shape": list(shape),
            "dtype": str(dtype),
            "dtype_itemsize": int(dtype.itemsize),
            "k": source.get("k"),
            "temperature": source.get("temperature"),
            "nucleus_enabled": nucleus_enabled,
            "top_p": float(top_p),
            "seed": int(seed),
            "expected_shape": [int(value) for value in expected_shape],
            "expected_score_range": [float(value) for value in expected_range],
            "row_block": row_block,
            "col_block": col_block,
            "priority": _as_int(source.get("priority", 0), "priority"),
        }
        shards.append(specification)

    public_manifest = copy.deepcopy(raw)
    return {
        "path": str(manifest_path),
        "raw": public_manifest,
        "manifest_digest": digest_value(public_manifest),
        "budget": budget_config,
        "budget_fingerprint": digest_value(budget_config),
        "workers": workers,
        "shards": shards,
    }


def estimate_shard(spec: dict[str, Any]) -> dict[str, int]:
    """Conservative pre-acceptance estimate derived only from shape metadata."""

    rows, vocab = (int(value) for value in spec["shape"])
    k_value = spec.get("k")
    k_for_estimate = int(k_value) if isinstance(k_value, (int, np.integer)) and k_value > 0 else 0
    candidate_k = min(k_for_estimate, vocab)
    elements = rows * vocab + 3 * rows * candidate_k
    # This deliberately models the full mapped logits plus output/diagnostic
    # working sets. It is conservative and never relies on catching OOM.
    memory_bytes = (
        rows * vocab * int(spec["dtype_itemsize"])
        + rows * candidate_k * 320
        + 65536
    )
    return {"elements": int(elements), "memory_bytes": int(memory_bytes)}


def stage_fingerprints(
    spec: dict[str, Any],
    *,
    logits_fingerprint: str,
    budget: dict[str, int],
) -> list[dict[str, Any]]:
    """Build the version chain used for earliest-stage invalidation.

    logits/K -> filter; temperature -> normalize; seed/top-p -> sampling;
    budget/schedule checks -> validation. Upstream fingerprints are folded into
    every downstream fingerprint so a change propagates to all later stages.
    """

    filter_core = {
        "algorithm_version": ALGORITHM_VERSION,
        "shard_id": spec["shard_id"],
        "logits_fingerprint": logits_fingerprint,
        "shape": spec["shape"],
        "k": spec["k"],
        "row_block": spec["row_block"],
        "col_block": spec["col_block"],
        "tie_break": "score_desc_token_asc",
    }
    filter_input = digest_value(filter_core)

    normalize_core = {
        "algorithm_version": ALGORITHM_VERSION,
        "filter_fingerprint": filter_input,
        "temperature": spec["temperature"],
        "math": "log_softmax_after_max_subtraction",
    }
    normalize_input = digest_value(normalize_core)

    sampling_core = {
        "algorithm_version": ALGORITHM_VERSION,
        "normalize_fingerprint": normalize_input,
        "seed": spec["seed"],
        "nucleus_enabled": spec["nucleus_enabled"],
        "top_p": spec["top_p"],
        "rng": "PCG64(SeedSequence([seed_u64, shard_ordinal, row]))",
        "tie_boundary": "rank_prefix_lower_token_first",
    }
    sampling_input = digest_value(sampling_core)

    validation_core = {
        "algorithm_version": ALGORITHM_VERSION,
        "sampling_fingerprint": sampling_input,
        "budget": budget,
        "expected_shape": spec["expected_shape"],
        "expected_score_range": spec["expected_score_range"],
        "validation_version": VALIDATION_VERSION,
    }
    validation_input = digest_value(validation_core)

    inputs = {
        "filter": filter_input,
        "normalize": normalize_input,
        "sampling": sampling_input,
        "validation": validation_input,
    }
    cores = {
        "filter": filter_core,
        "normalize": normalize_core,
        "sampling": sampling_core,
        "validation": validation_core,
    }
    return [
        {
            "stage": stage,
            "input_fingerprint": inputs[stage],
            "config_version": digest_value(
                {"algorithm_version": ALGORITHM_VERSION, "stage": stage, "core": cores[stage]}
            ),
        }
        for stage in STAGES
    ]
