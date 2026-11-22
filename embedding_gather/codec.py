from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Mapping

from .digest import canonical_json_bytes, sha256_json
from .errors import E_MANIFEST_INVALID, E_DIM_MISMATCH, fail
from .model import (
    ActualShardFacts,
    BatchLink,
    IndexProject,
    ShardSpec,
    V1UpgradeConfig,
    normalize_pad_ranges,
)


def manifest_version(data: Mapping[str, Any]) -> int:
    value = data.get("schema_version", data.get("structure_version"))
    try:
        version = int(value)
    except (TypeError, ValueError) as exc:
        raise fail(E_MANIFEST_INVALID, "manifest has no integer schema_version") from exc
    if version not in (1, 2):
        raise fail(E_MANIFEST_INVALID, "unsupported manifest schema_version", schema_version=version)
    return version


def read_manifest_file(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise fail(E_MANIFEST_INVALID, "manifest root must be a JSON object", path=str(path))
    return value


def _as_int(value: Any, *, field_name: str, shard_id: str | None = None) -> int:
    if isinstance(value, bool):
        raise fail(E_MANIFEST_INVALID, f"{field_name} must be an integer", field=field_name, shard_id=shard_id)
    try:
        integer = int(value)
    except (TypeError, ValueError) as exc:
        raise fail(
            E_MANIFEST_INVALID,
            f"{field_name} must be an integer",
            field=field_name,
            value=value,
            shard_id=shard_id,
        ) from exc
    if integer != value:
        raise fail(
            E_MANIFEST_INVALID,
            f"{field_name} must have no fractional component",
            field=field_name,
            value=value,
            shard_id=shard_id,
        )
    return integer


def _row_range(raw: Mapping[str, Any], shard_id: str | None = None) -> tuple[int, int]:
    if "row_range" in raw and raw["row_range"] is not None:
        value = raw["row_range"]
        if not isinstance(value, (list, tuple)) or len(value) != 2:
            raise fail(E_MANIFEST_INVALID, "row_range must contain two integers", shard_id=shard_id)
        return (
            _as_int(value[0], field_name="row_range[0]", shard_id=shard_id),
            _as_int(value[1], field_name="row_range[1]", shard_id=shard_id),
        )
    return (
        _as_int(raw.get("row_start"), field_name="row_start", shard_id=shard_id),
        _as_int(raw.get("row_end"), field_name="row_end", shard_id=shard_id),
    )


def _decode_batches(raw_batches: Any) -> tuple[BatchLink, ...]:
    if raw_batches is None:
        return ()
    if not isinstance(raw_batches, list):
        raise fail(E_MANIFEST_INVALID, "index_batches must be a list")
    result: list[BatchLink] = []
    for raw in raw_batches:
        if not isinstance(raw, dict):
            raise fail(E_MANIFEST_INVALID, "each index batch link must be an object")
        batch_id = str(raw.get("batch_id", ""))
        axis = raw.get("gather_axis")
        result.append(
            BatchLink(
                batch_id=batch_id,
                table_id=str(raw.get("table_id", "")),
                file=str(raw.get("file", "")),
                batch_number=_as_int(raw.get("batch_number", 0), field_name="batch_number"),
                gather_axis=None if axis is None else _as_int(axis, field_name="gather_axis"),
            )
        )
    return tuple(result)


def decode_v2(data: Mapping[str, Any], facts: Mapping[str, ActualShardFacts] | None = None) -> IndexProject:
    table_id = str(data.get("table_id", ""))
    raw_shards = data.get("shards")
    if not isinstance(raw_shards, list):
        raise fail(E_MANIFEST_INVALID, "shards must be a list", table_id=table_id)
    shards: list[ShardSpec] = []
    for raw in raw_shards:
        if not isinstance(raw, dict):
            raise fail(E_MANIFEST_INVALID, "each shard must be an object", table_id=table_id)
        shard_id = str(raw.get("shard_id", ""))
        row_start, row_end = _row_range(raw, shard_id)
        shard = ShardSpec(
            shard_id=shard_id,
            row_start=row_start,
            row_end=row_end,
            row_offset=_as_int(raw.get("row_offset"), field_name="row_offset", shard_id=shard_id),
            embedding_dim=_as_int(raw.get("embedding_dim"), field_name="embedding_dim", shard_id=shard_id),
            segment_digest=str(raw.get("segment_digest", "")),
            weight_file=str(raw.get("weight_file", "")),
            pad_ranges=normalize_pad_ranges(raw.get("pad_ranges")),
            pad_marker=float(raw.get("pad_marker", 1.0)),
        )
        if facts is not None and shard_id in facts:
            actual = facts[shard_id]
            if actual.embedding_dim != shard.embedding_dim:
                raise fail(
                    E_DIM_MISMATCH,
                    "version 2 manifest dimension does not match weight shard metadata",
                    shard_id=shard_id,
                    manifest_dim=shard.embedding_dim,
                    actual_dim=actual.embedding_dim,
                )
        shards.append(shard)

    embedding_dim = _as_int(
        data.get("embedding_dim", shards[0].embedding_dim if shards else 0),
        field_name="embedding_dim",
    )
    quantization_bits = _as_int(data.get("quantization_bits", 12), field_name="quantization_bits")
    project = IndexProject(
        table_id=table_id,
        embedding_dim=embedding_dim,
        structure_version=2,
        shards=tuple(shards),
        index_batches=_decode_batches(data.get("index_batches")),
        quantization_bits=quantization_bits,
        provenance=tuple(str(item) for item in data.get("provenance", [])),
    )
    project.validate()
    return project


def _convert_percentage(value: int, total_rows: int, mode: str) -> int:
    scaled = total_rows * value / 100.0
    if mode == "floor":
        return int(math.floor(scaled))
    if mode == "ceil":
        return int(math.ceil(scaled))
    return int(math.floor(scaled + 0.5))


def _compat_section(data: Mapping[str, Any]) -> Mapping[str, Any]:
    compat = data.get("_compat")
    if compat is None:
        return {}
    if not isinstance(compat, dict):
        raise fail(E_MANIFEST_INVALID, "_compat must be an object")
    return compat


def _infer_total_rows(data: Mapping[str, Any], facts: Mapping[str, ActualShardFacts] | None) -> int | None:
    compat = _compat_section(data)
    candidates = [compat.get("total_rows"), data.get("total_rows")]
    for candidate in candidates:
        if candidate is not None:
            return _as_int(candidate, field_name="total_rows")
    if facts:
        return max(item.row_end for item in facts.values())
    return None


def decode_v1(
    data: Mapping[str, Any],
    facts: Mapping[str, ActualShardFacts] | None = None,
    config: V1UpgradeConfig | None = None,
) -> IndexProject:
    config = config or V1UpgradeConfig()
    table_id = str(data.get("table_id", ""))
    raw_shards = data.get("shards")
    if not isinstance(raw_shards, list):
        raise fail(E_MANIFEST_INVALID, "shards must be a list", table_id=table_id)
    compat = _compat_section(data)
    total_rows = config.total_rows or _infer_total_rows(data, facts)
    if total_rows is None:
        raise fail(
            E_MANIFEST_INVALID,
            "version 1 upgrade needs total_rows because ratios are percentages",
            table_id=table_id,
        )

    exact_ranges = compat.get("exact_row_ranges")
    compat_offsets = compat.get("row_offsets")
    compat_pad = compat.get("pad_ranges")
    compat_dims = compat.get("embedding_dims")
    notes: list[str] = [
        "upgraded from manifest structure version 1",
        f"percentage_rounding={config.percentage_rounding}",
        f"total_rows={total_rows}",
    ]
    if config.prefer_compat_extension and exact_ranges is not None:
        notes.append("used compatibility extension for exact row ranges")
    if config.prefer_compat_extension and compat_offsets is not None:
        notes.append("used compatibility extension for row offsets")

    shards: list[ShardSpec] = []
    previous_end = 0
    for index, raw in enumerate(raw_shards):
        if not isinstance(raw, dict):
            raise fail(E_MANIFEST_INVALID, "each shard must be an object", table_id=table_id)
        shard_id = str(raw.get("shard_id", ""))
        start_pct = _as_int(raw.get("row_start_pct"), field_name="row_start_pct", shard_id=shard_id)
        end_pct = _as_int(raw.get("row_end_pct"), field_name="row_end_pct", shard_id=shard_id)
        if not (0 <= start_pct < end_pct <= 100):
            raise fail(
                E_MANIFEST_INVALID,
                "version 1 percentage interval must satisfy 0 <= start < end <= 100",
                shard_id=shard_id,
                row_start_pct=start_pct,
                row_end_pct=end_pct,
            )
        computed_start = _convert_percentage(start_pct, total_rows, config.percentage_rounding)
        computed_end = _convert_percentage(end_pct, total_rows, config.percentage_rounding)
        row_start, row_end = computed_start, computed_end
        if (
            config.prefer_compat_extension
            and isinstance(exact_ranges, list)
            and index < len(exact_ranges)
        ):
            candidate = exact_ranges[index]
            if not isinstance(candidate, (list, tuple)) or len(candidate) != 2:
                raise fail(E_MANIFEST_INVALID, "exact_row_ranges entry must have two integers", shard_id=shard_id)
            row_start = _as_int(candidate[0], field_name="exact_row_ranges[0]", shard_id=shard_id)
            row_end = _as_int(candidate[1], field_name="exact_row_ranges[1]", shard_id=shard_id)

        if config.offset_strategy == "contiguous":
            row_offset = previous_end
        else:
            row_offset = row_start
        if config.prefer_compat_extension and isinstance(compat_offsets, list) and index < len(compat_offsets):
            row_offset = _as_int(compat_offsets[index], field_name="compat.row_offsets", shard_id=shard_id)
        notes.append(
            f"{shard_id}: {start_pct}%..{end_pct}% -> rows [{computed_start},{computed_end}) "
            f"using {config.percentage_rounding}; row_offset={row_offset}"
        )
        previous_end = max(previous_end, row_end)

        manifest_dim = _as_int(raw.get("embedding_dim"), field_name="embedding_dim", shard_id=shard_id)
        actual_dim = manifest_dim
        if isinstance(compat_dims, dict) and shard_id in compat_dims:
            actual_dim = _as_int(compat_dims[shard_id], field_name="compat.embedding_dims", shard_id=shard_id)
        if facts is not None and shard_id in facts:
            actual_dim = facts[shard_id].embedding_dim
        if actual_dim != manifest_dim:
            notes.append(
                f"{shard_id}: corrected embedding_dim {manifest_dim} -> {actual_dim} from shard declaration"
            )

        pad_ranges: tuple[tuple[int, int], ...] = ()
        pad_source = "none"
        if config.prefer_compat_extension and isinstance(compat_pad, dict) and shard_id in compat_pad:
            pad_ranges = normalize_pad_ranges(compat_pad[shard_id])
            pad_source = "compat_extension"
        elif config.recover_pad_from == "weight_metadata" and facts is not None and shard_id in facts:
            pad_ranges = facts[shard_id].pad_ranges
            pad_source = "weight_metadata"
        if pad_ranges:
            notes.append(
                f"{shard_id}: version 1 registered pad rows as normal; recovered {len(pad_ranges)} "
                f"pad range(s) from {pad_source}"
            )
        else:
            notes.append(f"{shard_id}: version 1 has no pad marker and no pad range was recoverable")

        shard = ShardSpec(
            shard_id=shard_id,
            row_start=row_start,
            row_end=row_end,
            row_offset=row_offset,
            embedding_dim=actual_dim,
            segment_digest=str(raw.get("segment_digest", "")),
            weight_file=str(raw.get("weight_file", "")),
            pad_ranges=pad_ranges,
            pad_marker=float(compat.get("pad_marker", 1.0)),
            notes=tuple(notes),
        )
        shards.append(shard)

    actual_dims = [shard.embedding_dim for shard in shards]
    if not actual_dims:
        raise fail(E_MANIFEST_INVALID, "version 1 manifest contains no shards", table_id=table_id)
    if len(set(actual_dims)) != 1:
        raise fail(
            E_DIM_MISMATCH,
            "corrected shard dimensions disagree",
            shard_dims=actual_dims,
            table_id=table_id,
        )
    quantization_bits = _as_int(
        compat.get("quantization_bits", data.get("quantization_bits", 12)),
        field_name="quantization_bits",
    )
    project = IndexProject(
        table_id=table_id,
        embedding_dim=actual_dims[0],
        structure_version=1,
        shards=tuple(shards),
        index_batches=_decode_batches(data.get("index_batches")),
        quantization_bits=quantization_bits,
        provenance=tuple(notes),
    )
    project.validate()
    return project


def decode_manifest(
    data: Mapping[str, Any],
    facts: Mapping[str, ActualShardFacts] | None = None,
    v1_config: V1UpgradeConfig | None = None,
) -> IndexProject:
    version = manifest_version(data)
    if version == 2:
        return decode_v2(data, facts=facts)
    return decode_v1(data, facts=facts, config=v1_config)


def encode_v2(project: IndexProject) -> dict[str, Any]:
    project.validate()
    return {
        "schema_version": 2,
        "table_id": project.table_id,
        "embedding_dim": project.embedding_dim,
        "quantization_bits": project.quantization_bits,
        "shards": [
            {
                "shard_id": shard.shard_id,
                "row_range": [shard.row_start, shard.row_end],
                "row_offset": shard.row_offset,
                "embedding_dim": shard.embedding_dim,
                "segment_digest": shard.segment_digest,
                "weight_file": shard.weight_file,
                "pad_ranges": [list(item) for item in shard.pad_ranges],
                "pad_marker": float(shard.pad_marker),
            }
            for shard in project.shards
        ],
        "index_batches": [
            {
                "batch_id": batch.batch_id,
                "table_id": batch.table_id,
                "file": batch.file,
                "batch_number": batch.batch_number,
                "gather_axis": batch.gather_axis,
            }
            for batch in project.index_batches
        ],
        "provenance": list(project.provenance),
    }


def _percentage(row: int, total_rows: int) -> int:
    return int(math.floor((row * 100.0 / total_rows) + 0.5))


def encode_v1(project: IndexProject, include_compat: bool = True) -> dict[str, Any]:
    project.validate()
    total_rows = project.total_rows
    if total_rows <= 0:
        raise fail(E_MANIFEST_INVALID, "cannot encode an empty table as version 1", table_id=project.table_id)
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "table_id": project.table_id,
        "embedding_dim": project.embedding_dim,
        "shards": [
            {
                "shard_id": shard.shard_id,
                "row_start_pct": _percentage(shard.row_start, total_rows),
                "row_end_pct": 100 if shard.row_end == total_rows else _percentage(shard.row_end, total_rows),
                "embedding_dim": shard.embedding_dim,
                "segment_digest": shard.segment_digest,
                "weight_file": shard.weight_file,
            }
            for shard in project.shards
        ],
        "index_batches": [
            {
                "batch_id": batch.batch_id,
                "table_id": batch.table_id,
                "file": batch.file,
                "batch_number": batch.batch_number,
                "gather_axis": batch.gather_axis,
            }
            for batch in project.index_batches
        ],
    }
    if include_compat:
        manifest["_compat"] = {
            "format": "embedding-gather-v1-lossless-downgrade",
            "total_rows": total_rows,
            "exact_row_ranges": [[shard.row_start, shard.row_end] for shard in project.shards],
            "row_offsets": [shard.row_offset for shard in project.shards],
            "embedding_dims": {shard.shard_id: shard.embedding_dim for shard in project.shards},
            "pad_ranges": {shard.shard_id: [list(item) for item in shard.pad_ranges] for shard in project.shards},
            "pad_marker": 1.0,
            "quantization_bits": project.quantization_bits,
            "downgrade_notes": [
                "explicit row offsets moved to _compat.row_offsets",
                "pad rows are counted as normal rows in schema_version 1 fields",
                "pad ranges are retained only as a compatibility supplement",
            ],
        }
    return manifest


def semantic_signature(project: IndexProject) -> dict[str, Any]:
    return {
        "table_id": project.table_id,
        "embedding_dim": project.embedding_dim,
        "quantization_bits": project.quantization_bits,
        "shards": [shard.semantic_dict() for shard in sorted(project.shards, key=lambda item: item.shard_id)],
        "index_batches": [
            batch.semantic_dict() for batch in sorted(project.index_batches, key=lambda item: item.batch_id)
        ],
    }


def semantic_digest(project: IndexProject) -> str:
    return sha256_json(semantic_signature(project))


def load_manifest(
    path: str | Path,
    facts: Mapping[str, ActualShardFacts] | None = None,
    v1_config: V1UpgradeConfig | None = None,
) -> IndexProject:
    return decode_manifest(read_manifest_file(path), facts=facts, v1_config=v1_config)


def load_project(
    path: str | Path,
    data_root: str | Path | None = None,
    *,
    facts: Mapping[str, ActualShardFacts] | None = None,
    v1_config: V1UpgradeConfig | None = None,
) -> IndexProject:
    manifest = read_manifest_file(path)
    root = Path(data_root) if data_root is not None else Path(path).resolve().parent
    if facts is None:
        from .weights import load_manifest_facts

        facts = load_manifest_facts(manifest, root)
    return decode_manifest(manifest, facts=facts, v1_config=v1_config)


def write_manifest(path: str | Path, value: Mapping[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(canonical_json_bytes(value))
