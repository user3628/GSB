"""Checkpointed deterministic dataset evaluation."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .audio import Dataset, load_dataset
from .checkpoint import CheckpointStore, GenerationMeta, version_fingerprint
from .config import VADConfig
from .errors import VadError
from .metrics import (
    EndpointMetrics,
    aggregate_metrics,
    evaluation_fingerprint,
    match_endpoints,
)
from .stream import StreamingVAD, detect_samples
from .util import atomic_write_bytes, pretty_json, sha256_bytes


SUMMARY_SCHEMA = "vadkit.summary.v1"
CACHE_SCHEMA = "vadkit.chunk-cache.v1"
_STREAM_CHUNK_PATTERN = (1, 17, 160, 333, 1_024, 4_096)


@dataclass(frozen=True)
class EvaluationOutcome:
    status: str
    generation: int
    result: dict[str, Any] | None
    result_path: Path | None
    result_sha256: str | None
    computed_chunks: tuple[str, ...]
    cache_reused_chunks: tuple[str, ...]
    checkpoint_reused_chunks: tuple[str, ...]


def _endpoint_public(endpoint: dict[str, Any]) -> dict[str, Any]:
    return {
        "start_ms": endpoint["start_ms"],
        "end_ms": endpoint["end_ms"],
        "duration_ms": endpoint["duration_ms"],
    }


def _stream_detect(samples: Any, config: VADConfig, chunk_id: str) -> dict[str, Any]:
    session = StreamingVAD(config, chunk_id=chunk_id, expected_sample_count=len(samples))
    offset = 0
    chunk_index = 0
    while offset < len(samples):
        size = _STREAM_CHUNK_PATTERN[chunk_index % len(_STREAM_CHUNK_PATTERN)]
        session.feed_samples(samples[offset : offset + size])
        offset += size
        chunk_index += 1
    result = session.finalize()
    return {
        "endpoints": [endpoint.as_dict() for endpoint in result.endpoints],
        "frame_count": result.frame_count,
        "truncated_frame_count": result.truncated_frame_count,
    }


def _compute_chunk(
    samples: Any,
    config: VADConfig,
    chunk_id: str,
    sample_count: int,
) -> dict[str, Any]:
    batch = detect_samples(
        samples,
        config,
        chunk_id=chunk_id,
        expected_sample_count=sample_count,
    )
    stream = _stream_detect(samples, config, chunk_id)
    batch_endpoints = [endpoint.as_dict() for endpoint in batch.endpoints]
    if batch_endpoints != stream["endpoints"]:
        raise VadError(
            "E_INVALID_STAGE",
            "stream and batch endpoints diverged for the same chunk",
            chunk_id=chunk_id,
            sample_position=0,
            details={"batch": batch_endpoints, "stream": stream["endpoints"]},
        )
    return {
        "schema": CACHE_SCHEMA,
        "chunk_id": chunk_id,
        "sample_count": sample_count,
        "frame_count": batch.frame_count,
        "truncated_frame_count": batch.truncated_frame_count,
        "batch_endpoints": batch_endpoints,
        "stream_endpoints": stream["endpoints"],
    }


def _record_matches_chunk(record: dict[str, Any], dataset: Dataset) -> bool:
    order = int(record.get("order", -1))
    if order < 0 or order >= len(dataset.chunks):
        return False
    chunk = dataset.chunks[order]
    return (
        record.get("chunk_id") == chunk.chunk_id
        and record.get("input_sha256") == chunk.input_sha256
    )


def _chunk_result(
    chunk: Any,
    version: str,
    endpoint_summary: dict[str, Any],
    tolerance_ms: float,
) -> tuple[dict[str, Any], EndpointMetrics, EndpointMetrics]:
    references = chunk.reference_dicts()
    batch_endpoints = endpoint_summary["batch_endpoints"]
    stream_endpoints = endpoint_summary["stream_endpoints"]
    batch_metrics = match_endpoints(references, batch_endpoints, tolerance_ms)
    stream_metrics = match_endpoints(references, stream_endpoints, tolerance_ms)
    consistent = [
        _endpoint_public(endpoint) for endpoint in batch_endpoints
    ] == [_endpoint_public(endpoint) for endpoint in stream_endpoints]
    result = {
        "chunk_id": chunk.chunk_id,
        "order": chunk.order,
        "conditions": list(chunk.conditions),
        "sample_count": chunk.sample_count,
        "input_sha256": chunk.input_sha256,
        "version_fingerprint": version,
        "reference_endpoints_ms": references,
        "batch_endpoints_ms": [_endpoint_public(endpoint) for endpoint in batch_endpoints],
        "stream_endpoints_ms": [_endpoint_public(endpoint) for endpoint in stream_endpoints],
        "batch_metrics": batch_metrics.as_dict(),
        "stream_metrics": stream_metrics.as_dict(),
        "batch_stream_consistent": consistent,
    }
    return result, batch_metrics, stream_metrics


def evaluate_dataset(
    manifest_path: str | Path,
    workspace: str | Path,
    *,
    config: VADConfig | None = None,
    tolerance_ms: float = 80.0,
    stop_after_chunks: int | None = None,
    crash_after_chunks: int | None = None,
    clear_stop: bool = False,
) -> EvaluationOutcome:
    active_config = (config or VADConfig()).validate()
    if tolerance_ms < 0:
        raise VadError(
            "E_INVALID_CONFIG",
            "tolerance_ms must be non-negative",
            details={"tolerance_ms": tolerance_ms},
        )
    if stop_after_chunks is not None and stop_after_chunks <= 0:
        raise VadError(
            "E_INVALID_CONFIG",
            "stop_after_chunks must be positive",
            details={"stop_after_chunks": stop_after_chunks},
        )
    dataset = load_dataset(manifest_path)
    store = CheckpointStore(workspace)
    if clear_stop:
        store.clear_stop()
    meta = store.ensure_generation(
        dataset.fingerprint,
        active_config.fingerprint,
        len(dataset.chunks),
    )
    if store.stop_requested():
        return EvaluationOutcome(
            status="stopped",
            generation=meta.generation,
            result=None,
            result_path=None,
            result_sha256=None,
            computed_chunks=(),
            cache_reused_chunks=(),
            checkpoint_reused_chunks=(),
        )

    records = store.load_records(meta.generation)
    if len(records) > len(dataset.chunks):
        raise VadError(
            "E_INVALID_STAGE",
            "workspace has more checkpoints than dataset chunks",
            details={"records": len(records), "chunks": len(dataset.chunks)},
        )
    for index, record in enumerate(records):
        chunk = dataset.chunks[index]
        if not _record_matches_chunk(record, dataset):
            raise VadError(
                "E_INVALID_STAGE",
                "checkpoint record does not correspond to its dataset stage",
                chunk_id=chunk.chunk_id,
                details={"order": index},
            )
        expected_version = version_fingerprint(chunk.input_sha256, active_config.fingerprint)
        if record.get("version_fingerprint") != expected_version:
            raise VadError(
                "E_STALE_CHECKPOINT",
                "checkpoint version is stale for the active dataset and configuration",
                chunk_id=chunk.chunk_id,
                details={"order": index},
            )

    computed: list[str] = []
    cache_reused: list[str] = []
    checkpoint_reused: list[str] = []
    processed_this_run = 0
    published_this_run = 0

    for index, chunk in enumerate(dataset.chunks):
        version = version_fingerprint(chunk.input_sha256, active_config.fingerprint)
        if index < len(records):
            endpoint_summary = records[index]["endpoint_summary"]
            checkpoint_reused.append(chunk.chunk_id)
        else:
            cached = store.load_cache(version)
            if cached is not None and cached.get("chunk_id") == chunk.chunk_id:
                endpoint_summary = {
                    "batch_endpoints": cached["batch_endpoints"],
                    "stream_endpoints": cached["stream_endpoints"],
                }
                cache_reused.append(chunk.chunk_id)
            else:
                computed_result = _compute_chunk(
                    chunk.samples,
                    active_config,
                    chunk.chunk_id,
                    chunk.sample_count,
                )
                endpoint_summary = {
                    "batch_endpoints": computed_result["batch_endpoints"],
                    "stream_endpoints": computed_result["stream_endpoints"],
                }
                store.write_cache(version, computed_result)
                computed.append(chunk.chunk_id)
            store.publish(
                generation=meta.generation,
                sequence=index + 1,
                order=index,
                chunk_id=chunk.chunk_id,
                input_sha256=chunk.input_sha256,
                version_fingerprint=version,
                config_fingerprint=active_config.fingerprint,
                endpoint_summary=endpoint_summary,
            )
            published_this_run += 1

        processed_this_run += 1
        if stop_after_chunks is not None and processed_this_run >= stop_after_chunks:
            store.request_stop()
        if crash_after_chunks is not None and published_this_run >= crash_after_chunks:
            os._exit(70)
        if store.stop_requested() and index + 1 < len(dataset.chunks):
            return EvaluationOutcome(
                status="stopped",
                generation=meta.generation,
                result=None,
                result_path=None,
                result_sha256=None,
                computed_chunks=tuple(computed),
                cache_reused_chunks=tuple(cache_reused),
                checkpoint_reused_chunks=tuple(checkpoint_reused),
            )

    records = store.load_records(meta.generation)
    if len(records) != len(dataset.chunks):
        current = len(records)
        if store.stop_requested():
            return EvaluationOutcome(
                status="stopped",
                generation=meta.generation,
                result=None,
                result_path=None,
                result_sha256=None,
                computed_chunks=tuple(computed),
                cache_reused_chunks=tuple(cache_reused),
                checkpoint_reused_chunks=tuple(checkpoint_reused),
            )
        raise VadError(
            "E_INVALID_STAGE",
            "checkpointed run ended before every stage was published",
            details={"published": current, "chunks": len(dataset.chunks)},
        )

    chunk_summaries: list[dict[str, Any]] = []
    batch_metric_values: list[EndpointMetrics] = []
    stream_metric_values: list[EndpointMetrics] = []
    for index, chunk in enumerate(dataset.chunks):
        record = records[index]
        version = version_fingerprint(chunk.input_sha256, active_config.fingerprint)
        summary, batch_metrics, stream_metrics = _chunk_result(
            chunk,
            version,
            record["endpoint_summary"],
            tolerance_ms,
        )
        chunk_summaries.append(summary)
        batch_metric_values.append(batch_metrics)
        stream_metric_values.append(stream_metrics)

    aggregate_batch = aggregate_metrics(batch_metric_values, tolerance_ms)
    aggregate_stream = aggregate_metrics(stream_metric_values, tolerance_ms)
    all_consistent = all(item["batch_stream_consistent"] for item in chunk_summaries)
    if not all_consistent:
        raise VadError(
            "E_INVALID_STAGE",
            "batch and stream summaries are inconsistent",
            sample_position=0,
        )
    summary = {
        "schema": SUMMARY_SCHEMA,
        "dataset_fingerprint": dataset.fingerprint,
        "vad_config_fingerprint": active_config.fingerprint,
        "evaluation_fingerprint": evaluation_fingerprint(
            active_config.fingerprint, tolerance_ms
        ),
        "tolerance_ms": round(float(tolerance_ms), 6),
        "chunk_count": len(dataset.chunks),
        "total_samples": sum(chunk.sample_count for chunk in dataset.chunks),
        "batch_stream_consistent": all_consistent,
        "chunks": chunk_summaries,
        "aggregate": {
            "batch": aggregate_batch,
            "stream": aggregate_stream,
            "batch_stream_consistent": aggregate_batch == aggregate_stream,
        },
    }
    result_bytes = pretty_json(summary).encode("utf-8")
    result_path = store.root / "result.json"
    atomic_write_bytes(result_path, result_bytes)
    result_hash = sha256_bytes(result_bytes)
    atomic_write_bytes(store.root / "result.sha256", f"{result_hash}\n".encode("ascii"))
    return EvaluationOutcome(
        status="complete",
        generation=meta.generation,
        result=summary,
        result_path=result_path,
        result_sha256=result_hash,
        computed_chunks=tuple(computed),
        cache_reused_chunks=tuple(cache_reused),
        checkpoint_reused_chunks=tuple(checkpoint_reused),
    )


def workspace_status(
    manifest_path: str | Path,
    workspace: str | Path,
    *,
    config: VADConfig | None = None,
) -> dict[str, Any]:
    active_config = (config or VADConfig()).validate()
    dataset = load_dataset(manifest_path)
    store = CheckpointStore(workspace)
    meta = store.current_meta()
    records = store.load_records(meta.generation) if meta is not None else []
    records_by_order = {int(record["order"]): record for record in records}
    stages: list[dict[str, Any]] = []
    for chunk in dataset.chunks:
        version = version_fingerprint(chunk.input_sha256, active_config.fingerprint)
        record = records_by_order.get(chunk.order)
        if record is not None:
            if record.get("version_fingerprint") == version:
                state = "reusable"
            else:
                state = "invalidated"
            sequence = record.get("sequence")
        elif store.load_cache(version) is not None:
            state = "cache_reusable"
            sequence = None
        else:
            state = "rebuild"
            sequence = None
        stages.append(
            {
                "chunk_id": chunk.chunk_id,
                "order": chunk.order,
                "state": state,
                "sequence": sequence,
                "input_sha256": chunk.input_sha256,
                "version_fingerprint": version,
            }
        )
    return {
        "schema": "vadkit.workspace-status.v1",
        "workspace": str(Path(workspace)),
        "generation": meta.generation if meta else None,
        "dataset_fingerprint": dataset.fingerprint,
        "vad_config_fingerprint": active_config.fingerprint,
        "checkpoint_count": len(records),
        "stop_requested": store.stop_requested(),
        "stages": stages,
    }

