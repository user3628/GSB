from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np

from .codec import write_manifest
from .digest import atomic_write_json, deterministic_npz_write, sha256_json
from .weights import weight_segment_digest


MAIN_SEED = 20261123


@dataclass(frozen=True)
class ShardPlan:
    shard_id: str
    row_start: int
    row_end: int
    pad_ranges: tuple[tuple[int, int], ...] = ()


def _quantize(block: np.ndarray, bits: int) -> tuple[np.ndarray, float]:
    qmax = (1 << (bits - 1)) - 1
    block_max = float(np.max(np.abs(block)))
    scale = max(block_max / qmax, 1e-9)
    q = np.clip(np.rint(block / scale), -qmax, qmax).astype(np.int16)
    return q, scale


def _write_batch(
    root: Path,
    batch_id: str,
    batch_number: int,
    table_id: str,
    ids: np.ndarray,
    *,
    gather_axis: int | None,
    weights: np.ndarray | None = None,
    include_axis: bool = True,
) -> dict[str, object]:
    payload: dict[str, np.ndarray] = {
        "ids": np.asarray(ids),
        "batch_id": np.asarray(batch_id),
        "batch_number": np.asarray(batch_number, dtype=np.int64),
        "table_id": np.asarray(table_id),
        "format_version": np.asarray(2, dtype=np.int64),
    }
    if include_axis and gather_axis is not None:
        payload["gather_axis"] = np.asarray(gather_axis, dtype=np.int64)
    if weights is not None:
        payload["weights"] = np.asarray(weights, dtype=np.float64)
    relative = f"index_batches/{batch_id}.npz"
    deterministic_npz_write(root / relative, payload)
    return {
        "batch_id": batch_id,
        "table_id": table_id,
        "file": relative,
        "batch_number": batch_number,
        "gather_axis": gather_axis,
    }


def generate_sample_set(
    root: str | Path,
    *,
    seed: int = MAIN_SEED,
    table_id: str = "demo-table",
    total_rows: int = 96,
    embedding_dim: int = 8,
    shard_plans: Iterable[ShardPlan] | None = None,
    quantization_bits: int = 12,
    include_error_batches: bool = True,
    v1_declared_dim: int = 16,
) -> dict[str, object]:
    root_path = Path(root)
    (root_path / "weights").mkdir(parents=True, exist_ok=True)
    (root_path / "index_batches").mkdir(parents=True, exist_ok=True)
    if shard_plans is None:
        shard_plans = (
            ShardPlan("shard-000", 0, 32, ((28, 32),)),
            ShardPlan("shard-001", 32, 64),
            ShardPlan("shard-002", 64, 96, ((92, 96),)),
        )
    plans = tuple(shard_plans)
    rng = np.random.default_rng(seed)
    table = rng.uniform(-0.85, 0.85, size=(total_rows, embedding_dim)).astype(np.float64)
    shard_links: list[dict[str, object]] = []
    shard_facts: dict[str, dict[str, object]] = {}
    for plan in plans:
        if not (0 <= plan.row_start < plan.row_end <= total_rows):
            raise ValueError(f"invalid shard plan: {plan}")
        block = table[plan.row_start : plan.row_end]
        q, scale = _quantize(block, quantization_bits)
        relative = f"weights/{plan.shard_id}.npz"
        digest = weight_segment_digest(
            q,
            scale,
            row_start=plan.row_start,
            row_end=plan.row_end,
            row_offset=plan.row_start,
            embedding_dim=embedding_dim,
            pad_ranges=plan.pad_ranges,
            pad_marker=1.0,
            quantization_bits=quantization_bits,
        )
        deterministic_npz_write(
            root_path / relative,
            {
                "weights_q": q,
                "scale": np.asarray(scale, dtype=np.float64),
                "row_start": np.asarray(plan.row_start, dtype=np.int64),
                "row_end": np.asarray(plan.row_end, dtype=np.int64),
                "row_offset": np.asarray(plan.row_start, dtype=np.int64),
                "embedding_dim": np.asarray(embedding_dim, dtype=np.int64),
                "segment_digest": np.asarray(digest),
                "pad_ranges": np.asarray(plan.pad_ranges, dtype=np.int64).reshape(-1, 2),
                "pad_marker": np.asarray(1.0, dtype=np.float64),
                "quantization_bits": np.asarray(quantization_bits, dtype=np.int64),
                "format_version": np.asarray(2, dtype=np.int64),
            },
        )
        shard_links.append(
            {
                "shard_id": plan.shard_id,
                "row_start": plan.row_start,
                "row_end": plan.row_end,
                "row_offset": plan.row_start,
                "embedding_dim": embedding_dim,
                "segment_digest": digest,
                "weight_file": relative,
                "pad_ranges": [list(item) for item in plan.pad_ranges],
                "pad_marker": 1.0,
            }
        )
        shard_facts[plan.shard_id] = {
            "row_start": plan.row_start,
            "row_end": plan.row_end,
            "pad_ranges": [list(item) for item in plan.pad_ranges],
            "scale": scale,
        }

    normal_batches = [
        _write_batch(
            root_path,
            "batch-ok",
            0,
            table_id,
            np.asarray([[0, 3, 5], [17, 33, 65]], dtype=np.int64),
            gather_axis=1,
            weights=np.asarray([[1.0, 0.5, 2.0], [0.25, 1.25, 0.75]], dtype=np.float64),
        ),
        _write_batch(
            root_path,
            "batch-cross-shard",
            1,
            table_id,
            np.asarray([[1, 31, 32], [63, 64, 91]], dtype=np.int64),
            gather_axis=0,
            weights=np.asarray([[1.0, 0.5, 2.0], [0.25, 1.25, 0.75]], dtype=np.float64),
        ),
        _write_batch(
            root_path,
            "batch-duplicate",
            2,
            table_id,
            np.asarray([3, 3, 3, 5], dtype=np.int64),
            gather_axis=0,
            weights=np.asarray([1.0, 2.0, 0.5, 1.0], dtype=np.float64),
            include_axis=False,
        ),
        _write_batch(
            root_path,
            "batch-negative",
            3,
            table_id,
            np.asarray([-1, -5, 0], dtype=np.int64),
            gather_axis=0,
            weights=np.asarray([1.0, 1.0, 1.0], dtype=np.float64),
        ),
        _write_batch(
            root_path,
            "batch-pad",
            4,
            table_id,
            np.asarray([[28, 31], [92, 95]], dtype=np.int64),
            gather_axis=1,
            weights=np.asarray([[1.0, 2.0], [0.5, 1.5]], dtype=np.float64),
        ),
    ]
    error_batches: list[dict[str, object]] = []
    if include_error_batches:
        error_batches = [
            _write_batch(
                root_path,
                "batch-empty",
                10,
                table_id,
                np.asarray([], dtype=np.int64),
                gather_axis=0,
                weights=np.asarray([], dtype=np.float64),
            ),
            _write_batch(
                root_path,
                "batch-oob",
                11,
                table_id,
                np.asarray([0, 999], dtype=np.int64),
                gather_axis=0,
            ),
            _write_batch(
                root_path,
                "batch-noninteger",
                12,
                table_id,
                np.asarray([1.0, 2.5], dtype=np.float64),
                gather_axis=0,
            ),
            _write_batch(
                root_path,
                "batch-zero-weight",
                13,
                table_id,
                np.asarray([1, 2, 3], dtype=np.int64),
                gather_axis=0,
                weights=np.zeros(3, dtype=np.float64),
            ),
        ]

    batches = normal_batches + error_batches
    manifest_v2 = {
        "schema_version": 2,
        "table_id": table_id,
        "embedding_dim": embedding_dim,
        "quantization_bits": quantization_bits,
        "shards": shard_links,
        "index_batches": batches,
        "provenance": [f"deterministic sample seed={seed}"],
    }
    write_manifest(root_path / "manifest_v2.json", manifest_v2)

    v1_shards = []
    for shard in shard_links:
        start = int(shard["row_start"])
        end = int(shard["row_end"])
        v1_shards.append(
            {
                "shard_id": shard["shard_id"],
                "row_start_pct": int(np.floor(start * 100.0 / total_rows)),
                "row_end_pct": 100 if end == total_rows else int(np.floor(end * 100.0 / total_rows)),
                "embedding_dim": v1_declared_dim,
                "segment_digest": shard["segment_digest"],
                "weight_file": shard["weight_file"],
            }
        )
    manifest_v1 = {
        "schema_version": 1,
        "table_id": table_id,
        "embedding_dim": v1_declared_dim,
        "shards": v1_shards,
        "index_batches": batches,
        "provenance": [
            "legacy version 1 fixture: percentage row ratios, no row_offset, pad rows counted as normal",
            f"declared embedding_dim is intentionally wrong ({v1_declared_dim}); weight shards declare {embedding_dim}",
        ],
    }
    write_manifest(root_path / "manifest_v1.json", manifest_v1)
    metadata = {
        "seed": seed,
        "table_id": table_id,
        "total_rows": total_rows,
        "embedding_dim": embedding_dim,
        "quantization_bits": quantization_bits,
        "shard_row_counts": [plan.row_end - plan.row_start for plan in plans],
        "shard_plans": [
            {
                "shard_id": plan.shard_id,
                "row_range": [plan.row_start, plan.row_end],
                "pad_ranges": [list(item) for item in plan.pad_ranges],
            }
            for plan in plans
        ],
        "normal_batches": [item["batch_id"] for item in normal_batches],
        "error_batches": [item["batch_id"] for item in error_batches],
        "batch_probe_digest": sha256_json(
            {str(item["batch_id"]): str(item["file"]) for item in batches}
        ),
    }
    atomic_write_json(root_path / "sample_metadata.json", metadata)
    return metadata


def generate_all(root: str | Path = "samples") -> dict[str, object]:
    root_path = Path(root)
    main = generate_sample_set(root_path / "main")
    ragged = generate_sample_set(
        root_path / "ragged",
        seed=MAIN_SEED + 1,
        table_id="ragged-table",
        total_rows=101,
        embedding_dim=6,
        shard_plans=(
            ShardPlan("ragged-000", 0, 37, ((34, 37),)),
            ShardPlan("ragged-001", 37, 70),
            ShardPlan("ragged-002", 70, 101, ((99, 101),)),
        ),
        include_error_batches=False,
        v1_declared_dim=12,
    )
    return {"main": main, "ragged": ragged}
