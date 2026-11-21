from __future__ import annotations

import copy
import io
import json
import math
import re
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable

import numpy as np

from .canonical import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_json_bytes,
    derive_stage_seed,
    digest_bytes,
    digest_object,
    hmac_rank,
    sha256_hex,
    stable_shard_id,
)
from .errors import PipelineError
from .models import STAGE_ORDER, InjectionOptions, StageName
from .policy import PolicySnapshot, validate_sample_id


@dataclass(frozen=True, slots=True)
class Record:
    timestamp_ms: int
    sample_id: str
    text_length: int
    label: str
    shard_key: str
    source_line_no: int

    def content(self) -> dict[str, Any]:
        return {
            "timestamp_ms": self.timestamp_ms,
            "sample_id": self.sample_id,
            "text_length": self.text_length,
            "label": self.label,
            "shard_key": self.shard_key,
        }

    def content_digest(self) -> str:
        return digest_object(self.content())


@dataclass(slots=True)
class StageArtifact:
    sequence: int
    name: StageName
    input_digest: str
    output_digest: str
    stats: dict[str, Any]
    details: dict[str, Any]


@dataclass(slots=True)
class ShardArtifact:
    shard_id: int
    sample_count: int
    byte_count: int
    relative_path: str
    manifest_sha256: str
    tensor_digest: str
    dtypes: dict[str, str]
    null_count: int
    warnings: list[dict[str, Any]]


@dataclass(slots=True)
class PipelineExecution:
    stages: list[StageArtifact]
    shards: list[ShardArtifact]
    summary: dict[str, Any]
    output_root: Path


StageStartCallback = Callable[[int, StageName, str], None]
StageCompleteCallback = Callable[[StageArtifact], None]
StageBoundaryCallback = Callable[[StageArtifact], None]


_UINT_RE = re.compile(r"^[0-9]+$")


def _record_payload(records: list[Record]) -> list[dict[str, Any]]:
    return [record.content() for record in records]


def _round_half_up(value: float) -> int:
    return int(math.floor(value + 0.5))


def _interrupt_if_requested(
    injection: InjectionOptions, stage_name: StageName, processed: int
) -> None:
    if (
        injection.interrupt_at_stage == stage_name
        and processed >= injection.interrupt_after_records
    ):
        raise PipelineError(
            "E_INJECTED_INTERRUPT",
            "injected interruption marker was reached inside a stage",
            {
                "stage": stage_name,
                "processed_records": processed,
                "interrupt_after_records": injection.interrupt_after_records,
                "durable_effect": "only previously completed stage facts are reusable",
            },
        )


def _record_invalid(line_no: int, reason: str, line: str, **extra: Any) -> PipelineError:
    return PipelineError(
        "E_RECORD_INVALID",
        "record line failed schema/cleaning validation",
        {
            "line_no": line_no,
            "reason": reason,
            "line_preview": line[:160],
            **extra,
        },
    )


def _apply_primary_injection(records: list[Record], injection: InjectionOptions) -> list[Record]:
    if injection.primary_count() == 0:
        return records
    if not records:
        raise PipelineError(
            "E_INJECTION_INVALID",
            "an anomaly injection was requested for an empty parsed batch",
            {"target_record_index": injection.target_record_index},
        )
    if injection.target_record_index >= len(records):
        raise PipelineError(
            "E_INJECTION_INVALID",
            "injection target_record_index is outside the parsed batch",
            {
                "target_record_index": injection.target_record_index,
                "record_count": len(records),
            },
        )

    output = list(records)
    index = injection.target_record_index
    if injection.invalid_label:
        output[index] = replace(output[index], label="injected_illegal_label")
    if injection.missing_shard_key:
        output[index] = replace(output[index], shard_key="")
    if injection.duplicate_conflict:
        counts: dict[str, int] = {}
        for record in output:
            counts[record.sample_id] = counts.get(record.sample_id, 0) + 1
        candidates = [sample_id for sample_id, count in counts.items() if count > 1]
        target_id = injection.duplicate_sample_id or (candidates[0] if candidates else None)
        if target_id is None or counts.get(target_id, 0) < 2:
            raise PipelineError(
                "E_INJECTION_INVALID",
                "duplicate_conflict injection requires an existing duplicate sample_id",
                {
                    "duplicate_sample_id": injection.duplicate_sample_id,
                    "available_duplicate_ids": candidates,
                },
            )
        seen = 0
        for position, record in enumerate(output):
            if record.sample_id != target_id:
                continue
            seen += 1
            if seen == 2:
                output[position] = replace(record, text_length=record.text_length + 1)
                break
    return output


def _parse_clean(
    source_bytes: bytes,
    policy: PolicySnapshot,
    injection: InjectionOptions,
) -> tuple[list[Record], StageArtifact]:
    source_digest = digest_bytes(source_bytes)
    input_digest = digest_object(
        {
            "stage": "parse_clean",
            "source_digest": source_digest,
            "rules": policy.cleaning.model_dump(mode="json"),
        }
    )
    try:
        text = source_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PipelineError(
            "E_RECORD_INVALID",
            "record file is not valid UTF-8",
            {"byte_offset": exc.start, "reason": "utf8_decode_error"},
        ) from exc

    lines = text.splitlines()
    if not lines:
        raise PipelineError(
            "E_RECORD_INVALID", "record file contains no lines", {"reason": "empty_input"}
        )

    parsed: list[Record] = []
    labels = set(policy.cleaning.allowed_labels)
    for line_no, line in enumerate(lines, start=1):
        _interrupt_if_requested(injection, "parse_clean", line_no - 1)
        fields = line.split("\t")
        if len(fields) != 5:
            raise _record_invalid(
                line_no,
                "expected exactly five tab-separated fields",
                line,
                field_count=len(fields),
            )
        values = [value.strip() for value in fields] if policy.cleaning.trim_whitespace else fields
        timestamp_raw, sample_id, length_raw, label_raw, shard_key = values
        if not _UINT_RE.fullmatch(timestamp_raw):
            raise _record_invalid(line_no, "timestamp_ms must be a non-negative integer", line)
        if not validate_sample_id(sample_id):
            raise _record_invalid(
                line_no,
                "sample_id must match [A-Za-z0-9_.:-]{1,128}",
                line,
                sample_id=sample_id,
            )
        if not _UINT_RE.fullmatch(length_raw):
            raise _record_invalid(line_no, "text_length must be a non-negative integer", line)
        if not label_raw:
            raise _record_invalid(line_no, "label must not be empty", line)
        timestamp_ms = int(timestamp_raw)
        text_length = int(length_raw)
        if timestamp_ms > 2**63 - 1 or text_length > 2**63 - 1:
            raise _record_invalid(line_no, "integer field exceeds int64 range", line)
        if text_length < policy.cleaning.min_text_length:
            raise _record_invalid(
                line_no,
                "text_length is below the cleaning minimum",
                line,
                text_length=text_length,
                minimum=policy.cleaning.min_text_length,
            )
        if text_length > policy.cleaning.max_text_length:
            raise _record_invalid(
                line_no,
                "text_length exceeds the cleaning maximum",
                line,
                text_length=text_length,
                maximum=policy.cleaning.max_text_length,
            )
        label = label_raw.lower() if policy.cleaning.lowercase_labels else label_raw
        parsed.append(
            Record(
                timestamp_ms=timestamp_ms,
                sample_id=sample_id,
                text_length=text_length,
                label=label,
                shard_key=shard_key,
                source_line_no=line_no,
            )
        )

    parsed = _apply_primary_injection(parsed, injection)
    for record in parsed:
        if not record.shard_key:
            raise PipelineError(
                "E_SHARD_KEY_MISSING",
                "shard_key is required",
                {"line_no": record.source_line_no, "sample_id": record.sample_id},
            )
        if record.label not in labels:
            raise PipelineError(
                "E_LABEL_INVALID",
                "label is not declared by the policy cleaning rules",
                {
                    "line_no": record.source_line_no,
                    "sample_id": record.sample_id,
                    "label": record.label,
                    "allowed_labels": sorted(labels),
                },
            )

    first_by_id: dict[str, Record] = {}
    cleaned: list[Record] = []
    duplicate_ids: list[str] = []
    duplicate_removed = 0
    for record in parsed:
        first = first_by_id.get(record.sample_id)
        if first is None:
            first_by_id[record.sample_id] = record
            cleaned.append(record)
            continue
        duplicate_removed += 1
        if record.sample_id not in duplicate_ids:
            duplicate_ids.append(record.sample_id)
        if record.content() != first.content():
            differing = sorted(
                key for key in record.content() if record.content()[key] != first.content()[key]
            )
            raise PipelineError(
                "E_CONFLICTING_RECORD",
                "duplicate sample_id has conflicting content",
                {
                    "sample_id": record.sample_id,
                    "first_line_no": first.source_line_no,
                    "duplicate_line_no": record.source_line_no,
                    "differing_fields": differing,
                    "first_content": first.content(),
                    "duplicate_content": record.content(),
                },
            )

    output_digest = digest_object(
        {
            "stage": "parse_clean",
            "records": _record_payload(cleaned),
            "duplicate_records_removed": duplicate_removed,
            "duplicate_ids": duplicate_ids,
        }
    )
    stats = {
        "raw_lines": len(lines),
        "records_parsed": len(parsed),
        "records_cleaned": len(cleaned),
        "duplicate_records_removed": duplicate_removed,
        "duplicate_ids": duplicate_ids,
        "source_size_bytes": len(source_bytes),
    }
    details = {
        "source_digest": source_digest,
        "cleaned_sample_ids": [record.sample_id for record in cleaned],
        "duplicate_ids": duplicate_ids,
    }
    return cleaned, StageArtifact(
        sequence=1,
        name="parse_clean",
        input_digest=input_digest,
        output_digest=output_digest,
        stats=stats,
        details=details,
    )


def _filter_records(
    records: list[Record],
    policy: PolicySnapshot,
    previous_digest: str,
    injection: InjectionOptions,
) -> tuple[list[Record], StageArtifact]:
    rules = policy.filtering.model_dump(mode="json")
    input_digest = digest_object(
        {"stage": "filter", "previous_output_digest": previous_digest, "rules": rules}
    )
    kept: list[Record] = []
    drop_reasons = {"timestamp": 0, "text_length": 0, "label": 0}
    drop_labels = set(policy.filtering.drop_labels)
    for index, record in enumerate(records):
        _interrupt_if_requested(injection, "filter", index)
        if not (
            policy.filtering.min_timestamp_ms
            <= record.timestamp_ms
            <= policy.filtering.max_timestamp_ms
        ):
            drop_reasons["timestamp"] += 1
            continue
        if not (
            policy.filtering.min_text_length
            <= record.text_length
            <= policy.filtering.max_text_length
        ):
            drop_reasons["text_length"] += 1
            continue
        if record.label in drop_labels:
            drop_reasons["label"] += 1
            continue
        kept.append(record)
    _interrupt_if_requested(injection, "filter", len(records))

    output_digest = digest_object({"stage": "filter", "records": _record_payload(kept)})
    stats = {
        "input_records": len(records),
        "output_records": len(kept),
        "dropped_records": len(records) - len(kept),
        "drop_reasons": drop_reasons,
    }
    details = {"kept_sample_ids": [record.sample_id for record in kept]}
    return kept, StageArtifact(
        sequence=2,
        name="filter",
        input_digest=input_digest,
        output_digest=output_digest,
        stats=stats,
        details=details,
    )


def _max_relative_deviation(values: list[int]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    if mean == 0:
        return 0.0
    return max(abs(value - mean) / mean for value in values)


def _shard_records(
    records: list[Record],
    policy: PolicySnapshot,
    previous_digest: str,
    injection: InjectionOptions,
) -> tuple[dict[int, list[Record]], StageArtifact, list[dict[str, Any]]]:
    rules = policy.sharding.model_dump(mode="json")
    input_digest = digest_object(
        {"stage": "shard", "previous_output_digest": previous_digest, "rules": rules}
    )
    shards: dict[int, list[Record]] = {
        shard_id: [] for shard_id in range(policy.sharding.shard_count)
    }
    shard_bytes = {shard_id: 0 for shard_id in range(policy.sharding.shard_count)}
    for index, record in enumerate(records):
        _interrupt_if_requested(injection, "shard", index)
        shard_id = stable_shard_id(
            record.shard_key, policy.sharding.shard_count, policy.sharding.salt
        )
        shards[shard_id].append(record)
        shard_bytes[shard_id] += len(
            canonical_json_bytes({"shard_id": shard_id, **record.content()})
        )
    _interrupt_if_requested(injection, "shard", len(records))

    input_ids = [record.sample_id for record in records]
    assigned_ids = [record.sample_id for shard in shards.values() for record in shard]
    if len(assigned_ids) != len(set(assigned_ids)) or set(assigned_ids) != set(input_ids):
        raise PipelineError(
            "E_INTERNAL",
            "shard assignment violated no-overlap/no-missing invariants",
            {
                "input_count": len(input_ids),
                "assigned_count": len(assigned_ids),
                "unique_assigned_count": len(set(assigned_ids)),
            },
        )

    counts = [len(shards[shard_id]) for shard_id in range(policy.sharding.shard_count)]
    byte_counts = [shard_bytes[shard_id] for shard_id in range(policy.sharding.shard_count)]
    sample_skew = _max_relative_deviation(counts)
    byte_skew = _max_relative_deviation(byte_counts)
    warnings: list[dict[str, Any]] = []
    if sample_skew > policy.sharding.sample_skew_threshold:
        warnings.append(
            {
                "code": "W_SHARD_SAMPLE_SKEW",
                "observed": sample_skew,
                "threshold": policy.sharding.sample_skew_threshold,
                "effect": "warning_only; assignments unchanged",
            }
        )
    if byte_skew > policy.sharding.byte_skew_threshold:
        warnings.append(
            {
                "code": "W_SHARD_BYTE_SKEW",
                "observed": byte_skew,
                "threshold": policy.sharding.byte_skew_threshold,
                "effect": "warning_only; assignments unchanged",
            }
        )

    output_digest = digest_object(
        {
            "stage": "shard",
            "shards": {
                str(shard_id): _record_payload(shards[shard_id])
                for shard_id in sorted(shards)
            },
        }
    )
    distribution = {
        "sample_counts": counts,
        "byte_counts": byte_counts,
        "sample_skew": sample_skew,
        "byte_skew": byte_skew,
        "sample_skew_threshold": policy.sharding.sample_skew_threshold,
        "byte_skew_threshold": policy.sharding.byte_skew_threshold,
    }
    stats = {
        "input_records": len(records),
        "output_records": sum(counts),
        "shard_count": policy.sharding.shard_count,
        "warnings": warnings,
        "distribution": distribution,
    }
    details = {
        "distribution": distribution,
        "shard_sample_ids": {
            str(shard_id): [record.sample_id for record in shards[shard_id]]
            for shard_id in sorted(shards)
        },
    }
    return shards, StageArtifact(
        sequence=3,
        name="shard",
        input_digest=input_digest,
        output_digest=output_digest,
        stats=stats,
        details=details,
    ), warnings


def _sample_shards(
    shards: dict[int, list[Record]],
    policy: PolicySnapshot,
    previous_digest: str,
    injection: InjectionOptions,
) -> tuple[dict[int, list[Record]], StageArtifact]:
    ratio = policy.sample_ratio if injection.sample_ratio is None else injection.sample_ratio
    if not (0 < ratio <= 1):
        raise PipelineError(
            "E_SAMPLE_RATIO_INVALID",
            "sample_ratio must be in (0, 1]",
            {"sample_ratio": ratio, "source": "request_injection" if injection.sample_ratio is not None else "policy"},
        )
    input_digest = digest_object(
        {
            "stage": "sample",
            "previous_output_digest": previous_digest,
            "sample_ratio": ratio,
        }
    )
    stage_seed = derive_stage_seed(policy.seed, "sample", input_digest)
    stage_seed_digest = digest_bytes(stage_seed)
    sampled: dict[int, list[Record]] = {}
    per_shard: dict[str, dict[str, Any]] = {}
    processed = 0
    for shard_id in sorted(shards):
        records = shards[shard_id]
        target_count = min(len(records), _round_half_up(len(records) * ratio))
        ranked = sorted(
            enumerate(records),
            key=lambda item: (
                hmac_rank(stage_seed, f"shard:{shard_id}", item[1].content_digest()),
                item[0],
            ),
        )
        chosen_positions = {position for position, _ in ranked[:target_count]}
        selected = [record for position, record in enumerate(records) if position in chosen_positions]
        sampled[shard_id] = selected
        processed += len(selected)
        _interrupt_if_requested(injection, "sample", processed)
        per_shard[str(shard_id)] = {
            "input_count": len(records),
            "selected_count": len(selected),
            "target_count": target_count,
            "selected_sample_ids": [record.sample_id for record in selected],
        }
    _interrupt_if_requested(injection, "sample", processed)

    output_digest = digest_object(
        {
            "stage": "sample",
            "shards": {
                str(shard_id): _record_payload(sampled[shard_id])
                for shard_id in sorted(sampled)
            },
            "stage_seed_digest": stage_seed_digest,
        }
    )
    stats = {
        "input_records": sum(len(records) for records in shards.values()),
        "output_records": sum(len(records) for records in sampled.values()),
        "sample_ratio": ratio,
        "rng_source": "hmac_sha256(policy.seed, stage_name + stage_input_digest)",
        "stage_seed_digest": stage_seed_digest,
        "per_shard": per_shard,
    }
    details = {"per_shard": per_shard, "stage_seed_digest": stage_seed_digest}
    return sampled, StageArtifact(
        sequence=4,
        name="sample",
        input_digest=input_digest,
        output_digest=output_digest,
        stats=stats,
        details=details,
    )


def _npy_bytes(array: np.ndarray) -> bytes:
    buffer = io.BytesIO()
    np.save(buffer, array, allow_pickle=False)
    return buffer.getvalue()


def _load_npy(path: Path) -> np.ndarray:
    return np.load(path, allow_pickle=False)


def _tensorize_shards(
    shards: dict[int, list[Record]],
    policy: PolicySnapshot,
    previous_digest: str,
    output_root: Path,
    shard_warnings: dict[int, list[dict[str, Any]]],
    injection: InjectionOptions,
) -> tuple[list[ShardArtifact], StageArtifact]:
    input_digest = digest_object(
        {
            "stage": "tensorize",
            "previous_output_digest": previous_digest,
            "normalization": policy.normalization.model_dump(mode="json"),
            "label_vocabulary": sorted(policy.cleaning.allowed_labels),
            "array_contract": {
                "features": "float32[n,2]",
                "timestamps": "int64[n]",
                "text_lengths": "int64[n]",
                "label_ids": "int64[n]",
                "shard_ids": "int64[n]",
                "null_mask": "uint8[n,2]",
            },
        }
    )
    label_vocabulary = sorted(policy.cleaning.allowed_labels)
    label_index = {label: index for index, label in enumerate(label_vocabulary)}
    normalizer = policy.normalization
    shards_root = output_root / "shards"
    artifacts: list[ShardArtifact] = []
    processed = 0
    for shard_id in sorted(shards):
        _interrupt_if_requested(injection, "tensorize", processed)
        records = shards[shard_id]
        n = len(records)
        timestamps = np.asarray([record.timestamp_ms for record in records], dtype=np.int64)
        text_lengths = np.asarray([record.text_length for record in records], dtype=np.int64)
        label_ids = np.asarray(
            [label_index[record.label] for record in records], dtype=np.int64
        )
        shard_ids = np.full(n, shard_id, dtype=np.int64)

        normalized = (text_lengths.astype(np.float64) - normalizer.length_min) / (
            normalizer.length_max - normalizer.length_min
        )
        if normalizer.clip:
            normalized = np.clip(normalized, 0.0, 1.0)
        denominator = max(1, len(label_vocabulary) - 1)
        label_fraction = label_ids.astype(np.float64) / denominator
        features = np.column_stack((normalized, label_fraction)).astype(
            np.dtype(normalizer.dtype)
        )
        null_mask = np.zeros((n, 2), dtype=np.uint8)

        arrays: dict[str, np.ndarray] = {
            "features": features,
            "timestamps": timestamps,
            "text_lengths": text_lengths,
            "label_ids": label_ids,
            "shard_ids": shard_ids,
            "null_mask": null_mask,
        }
        shard_relative = f"shards/shard_{shard_id:04d}"
        shard_dir = shards_root / f"shard_{shard_id:04d}"
        array_manifest: dict[str, dict[str, Any]] = {}
        for name, array in arrays.items():
            relative = f"{name}.npy"
            path = shard_dir / relative
            data = _npy_bytes(array)
            atomic_write_bytes(path, data)
            array_manifest[name] = {
                "path": relative,
                "dtype": array.dtype.name,
                "shape": list(array.shape),
                "sha256": digest_bytes(data),
                "bytes": len(data),
            }

        tensor_digest = digest_object(
            {
                "stage": "tensorize",
                "arrays": array_manifest,
                "sample_count": n,
                "null_count": int(null_mask.sum()),
            }
        )
        manifest_body: dict[str, Any] = {
            "format": "prep-tensor/v1",
            "shard_id": shard_id,
            "sample_count": n,
            "arrays": array_manifest,
            "dtypes": {name: array.dtype.name for name, array in arrays.items()},
            "null_mask": {
                "path": "null_mask.npy",
                "dtype": "uint8",
                "shape": [n, 2],
                "null_count": int(null_mask.sum()),
            },
            "normalization": normalizer.model_dump(mode="json"),
            "label_vocabulary": label_vocabulary,
            "tensor_digest": tensor_digest,
            "warnings": shard_warnings.get(shard_id, []),
        }
        manifest_body["manifest_checksum"] = digest_object(manifest_body)
        manifest_path = shard_dir / "manifest.json"
        manifest_bytes = atomic_write_json(manifest_path, manifest_body)

        loaded_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        checksum_target = copy.deepcopy(loaded_manifest)
        declared_manifest_checksum = checksum_target.pop("manifest_checksum")
        if declared_manifest_checksum != digest_object(checksum_target):
            raise PipelineError(
                "E_TENSOR_READBACK_MISMATCH",
                "tensor manifest checksum changed after write/readback",
                {"shard_id": shard_id, "path": str(manifest_path)},
            )
        for name, metadata in loaded_manifest["arrays"].items():
            path = shard_dir / metadata["path"]
            file_bytes = path.read_bytes()
            if digest_bytes(file_bytes) != metadata["sha256"]:
                raise PipelineError(
                    "E_TENSOR_READBACK_MISMATCH",
                    "tensor file digest changed after write/readback",
                    {"shard_id": shard_id, "array": name, "path": str(path)},
                )
            loaded = _load_npy(path)
            if loaded.dtype.name != metadata["dtype"] or list(loaded.shape) != metadata["shape"]:
                raise PipelineError(
                    "E_TENSOR_READBACK_MISMATCH",
                    "tensor dtype/shape changed after write/readback",
                    {
                        "shard_id": shard_id,
                        "array": name,
                        "expected_dtype": metadata["dtype"],
                        "actual_dtype": loaded.dtype.name,
                        "expected_shape": metadata["shape"],
                        "actual_shape": list(loaded.shape),
                    },
                )
        loaded_mask = _load_npy(shard_dir / "null_mask.npy")
        if int(loaded_mask.sum()) != int(loaded_manifest["null_mask"]["null_count"]):
            raise PipelineError(
                "E_TENSOR_READBACK_MISMATCH",
                "null-mask count changed after write/readback",
                {"shard_id": shard_id, "path": str(shard_dir / "null_mask.npy")},
            )

        artifacts.append(
            ShardArtifact(
                shard_id=shard_id,
                sample_count=n,
                byte_count=sum(metadata["bytes"] for metadata in array_manifest.values())
                + len(manifest_bytes),
                relative_path=shard_relative,
                manifest_sha256=digest_bytes(manifest_bytes),
                tensor_digest=tensor_digest,
                dtypes={name: array.dtype.name for name, array in arrays.items()},
                null_count=int(null_mask.sum()),
                warnings=shard_warnings.get(shard_id, []),
            )
        )
        processed += n
        _interrupt_if_requested(injection, "tensorize", processed)
    _interrupt_if_requested(injection, "tensorize", processed)

    output_digest = digest_object(
        {
            "stage": "tensorize",
            "shards": [
                {
                    "shard_id": artifact.shard_id,
                    "sample_count": artifact.sample_count,
                    "tensor_digest": artifact.tensor_digest,
                }
                for artifact in artifacts
            ],
        }
    )
    total_samples = sum(artifact.sample_count for artifact in artifacts)
    total_bytes = sum(artifact.byte_count for artifact in artifacts)
    stats = {
        "shards_written": len(artifacts),
        "sample_count": total_samples,
        "tensor_bytes": total_bytes,
        "null_count": sum(artifact.null_count for artifact in artifacts),
        "readback_verified": True,
        "dtypes": {
            "features": "float32",
            "timestamps": "int64",
            "text_lengths": "int64",
            "label_ids": "int64",
            "shard_ids": "int64",
            "null_mask": "uint8",
        },
    }
    details = {
        "shards": [
            {
                "shard_id": artifact.shard_id,
                "sample_count": artifact.sample_count,
                "byte_count": artifact.byte_count,
                "tensor_digest": artifact.tensor_digest,
                "manifest_sha256": artifact.manifest_sha256,
            }
            for artifact in artifacts
        ]
    }
    return artifacts, StageArtifact(
        sequence=5,
        name="tensorize",
        input_digest=input_digest,
        output_digest=output_digest,
        stats=stats,
        details=details,
    )


class PipelineComputer:
    def __init__(self, source_bytes: bytes, policy: PolicySnapshot, output_root: str | Path):
        self.source_bytes = source_bytes
        self.policy = policy
        self.output_root = Path(output_root)

    def execute(
        self,
        injection: InjectionOptions | None = None,
        *,
        on_stage_start: StageStartCallback | None = None,
        on_stage_complete: StageCompleteCallback | None = None,
        on_stage_boundary: StageBoundaryCallback | None = None,
    ) -> PipelineExecution:
        injection = injection or InjectionOptions()
        if injection.sample_ratio is not None and not (0 < injection.sample_ratio <= 1):
            raise PipelineError(
                "E_SAMPLE_RATIO_INVALID",
                "sample_ratio must be in (0, 1]",
                {"sample_ratio": injection.sample_ratio, "source": "request_injection"},
            )
        stages: list[StageArtifact] = []
        completed_names: list[str] = []

        def begin(name: StageName, sequence: int, input_digest: str) -> None:
            if on_stage_start is not None:
                on_stage_start(sequence, name, input_digest)

        def finish(artifact: StageArtifact) -> None:
            stages.append(artifact)
            completed_names.append(artifact.name)
            if on_stage_complete is not None:
                on_stage_complete(artifact)
            if on_stage_boundary is not None:
                on_stage_boundary(artifact)

        def annotate(error: PipelineError) -> PipelineError:
            error.context.setdefault("completed_stages", list(completed_names))
            return error

        parse_input = digest_object(
            {
                "stage": "parse_clean",
                "source_digest": digest_bytes(self.source_bytes),
                "rules": self.policy.cleaning.model_dump(mode="json"),
            }
        )
        begin("parse_clean", 1, parse_input)
        try:
            records, parse_stage = _parse_clean(self.source_bytes, self.policy, injection)
        except PipelineError as exc:
            raise annotate(exc) from exc
        finish(parse_stage)

        filter_input = digest_object(
            {
                "stage": "filter",
                "previous_output_digest": parse_stage.output_digest,
                "rules": self.policy.filtering.model_dump(mode="json"),
            }
        )
        begin("filter", 2, filter_input)
        try:
            filtered, filter_stage = _filter_records(
                records, self.policy, parse_stage.output_digest, injection
            )
        except PipelineError as exc:
            raise annotate(exc) from exc
        finish(filter_stage)

        shard_input = digest_object(
            {
                "stage": "shard",
                "previous_output_digest": filter_stage.output_digest,
                "rules": self.policy.sharding.model_dump(mode="json"),
            }
        )
        begin("shard", 3, shard_input)
        try:
            shards, shard_stage, warnings = _shard_records(
                filtered, self.policy, filter_stage.output_digest, injection
            )
        except PipelineError as exc:
            raise annotate(exc) from exc
        finish(shard_stage)

        shard_warnings = {
            shard_id: [
                warning
                for warning in warnings
                if warning["code"] == "W_SHARD_SAMPLE_SKEW"
                or warning["code"] == "W_SHARD_BYTE_SKEW"
            ]
            for shard_id in shards
        }
        sample_input = digest_object(
            {
                "stage": "sample",
                "previous_output_digest": shard_stage.output_digest,
                "sample_ratio": (
                    self.policy.sample_ratio
                    if injection.sample_ratio is None
                    else injection.sample_ratio
                ),
            }
        )
        begin("sample", 4, sample_input)
        try:
            sampled, sample_stage = _sample_shards(
                shards, self.policy, shard_stage.output_digest, injection
            )
        except PipelineError as exc:
            raise annotate(exc) from exc
        finish(sample_stage)

        tensor_input = digest_object(
            {
                "stage": "tensorize",
                "previous_output_digest": sample_stage.output_digest,
                "normalization": self.policy.normalization.model_dump(mode="json"),
                "label_vocabulary": sorted(self.policy.cleaning.allowed_labels),
                "array_contract": {
                    "features": "float32[n,2]",
                    "timestamps": "int64[n]",
                    "text_lengths": "int64[n]",
                    "label_ids": "int64[n]",
                    "shard_ids": "int64[n]",
                    "null_mask": "uint8[n,2]",
                },
            }
        )
        begin("tensorize", 5, tensor_input)
        try:
            shard_artifacts, tensor_stage = _tensorize_shards(
                sampled,
                self.policy,
                sample_stage.output_digest,
                self.output_root,
                shard_warnings,
                injection,
            )
        except PipelineError as exc:
            raise annotate(exc) from exc
        finish(tensor_stage)

        summary = {
            "policy_version": self.policy.version,
            "policy_checksum": self.policy.checksum,
            "source_sha256": digest_bytes(self.source_bytes),
            "source_size_bytes": len(self.source_bytes),
            "stage_order": list(STAGE_ORDER),
            "stage_digests": {stage.name: stage.output_digest for stage in stages},
            "records_raw": parse_stage.stats["raw_lines"],
            "records_parsed": parse_stage.stats["records_parsed"],
            "records_cleaned": parse_stage.stats["records_cleaned"],
            "duplicate_records_removed": parse_stage.stats["duplicate_records_removed"],
            "records_filtered": filter_stage.stats["output_records"],
            "samples_total": tensor_stage.stats["sample_count"],
            "shard_count": len(shard_artifacts),
            "tensor_bytes": tensor_stage.stats["tensor_bytes"],
            "null_count": tensor_stage.stats["null_count"],
            "warnings": warnings,
            "label_vocabulary": sorted(self.policy.cleaning.allowed_labels),
        }
        return PipelineExecution(
            stages=stages,
            shards=shard_artifacts,
            summary=summary,
            output_root=self.output_root,
        )
