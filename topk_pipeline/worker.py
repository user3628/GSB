"""Picklable worker entry point for one shard and four checkpointed stages."""

from __future__ import annotations

import os
from typing import Any

import numpy as np

from .checkpoints import CheckpointStore
from .errors import E_INTERNAL, PipelineError, internal_error
from .manifest import stage_fingerprints
from .normalize_sampling import normalize_topk, nucleus_and_sample
from .topk import StopRequested, chunked_topk
from .util import array_fingerprint
from .validation import validate_shard_output


def _stop_file_exists(stop_file: str | None) -> bool:
    return bool(stop_file) and os.path.exists(str(stop_file))


def _checkpoint_summary(store: CheckpointStore, shard_id: str) -> dict[str, Any]:
    try:
        state = store.load_state(shard_id)
        return {
            "last_sequence": int(state["last_sequence"]),
            "completed_stages": [record["stage"] for record in state["records"]],
        }
    except PipelineError as exc:
        return {"state_error": exc.to_dict()}


def process_shard(task: dict[str, Any]) -> dict[str, Any]:
    """Run/reuse all stages for one shard and return a serializable result."""

    spec = task["spec"]
    shard_id = spec["shard_id"]
    store = CheckpointStore(task["workspace"])
    stop_file = task.get("stop_file")
    current_stage = "filter"
    preparation: dict[str, Any] | None = None

    try:
        if _stop_file_exists(stop_file):
            raise StopRequested(shard_id=shard_id, completed_rows=0, stage="filter")

        logits = np.load(spec["path"], mmap_mode="r", allow_pickle=False)
        if list(logits.shape) != list(spec["shape"]):
            raise PipelineError(
                code=E_INTERNAL,
                message="shard shape changed after manifest validation",
                stage="filter",
                shard_id=shard_id,
                details={"actual_shape": list(logits.shape), "manifest_shape": spec["shape"]},
            )
        content_fingerprint = array_fingerprint(logits)
        expected_stages = stage_fingerprints(
            spec,
            logits_fingerprint=content_fingerprint,
            budget=task["budget"],
        )
        expected_by_stage = {item["stage"]: item for item in expected_stages}
        preparation = store.prepare(
            shard_id,
            expected_stages,
            content_fingerprint=content_fingerprint,
        )
        artifacts = {record["stage"]: record["artifact"] for record in preparation["records"]}
        sequence = int(preparation["next_sequence"])

        def publish(stage: str, artifact: dict[str, Any]) -> None:
            nonlocal sequence
            expected = expected_by_stage[stage]
            store.publish(
                shard_id,
                sequence=sequence,
                stage=stage,
                input_fingerprint=expected["input_fingerprint"],
                config_version=expected["config_version"],
                content_fingerprint=content_fingerprint,
                artifact=artifact,
            )
            artifacts[stage] = artifact
            sequence += 1

        current_stage = "filter"
        if "filter" not in artifacts:
            if _stop_file_exists(stop_file):
                raise StopRequested(shard_id=shard_id, completed_rows=0, stage="filter")
            filter_artifact = chunked_topk(
                logits,
                k=spec["k"],
                shard_id=shard_id,
                row_block=spec["row_block"],
                col_block=spec["col_block"],
                stop_check=lambda: _stop_file_exists(stop_file),
            )
            publish("filter", filter_artifact)

        if _stop_file_exists(stop_file):
            raise StopRequested(shard_id=shard_id, completed_rows=0, stage="normalize")

        current_stage = "normalize"
        if "normalize" not in artifacts:
            normalize_artifact = normalize_topk(
                artifacts["filter"],
                temperature=spec["temperature"],
                shard_id=shard_id,
            )
            publish("normalize", normalize_artifact)

        if _stop_file_exists(stop_file):
            raise StopRequested(shard_id=shard_id, completed_rows=0, stage="sampling")

        current_stage = "sampling"
        if "sampling" not in artifacts:
            sampling_artifact = nucleus_and_sample(
                artifacts["normalize"],
                shard_id=shard_id,
                shard_ordinal=int(spec["shard_ordinal"]),
                seed=int(spec["seed"]),
                nucleus_enabled=bool(spec["nucleus_enabled"]),
                top_p=float(spec["top_p"]),
            )
            publish("sampling", sampling_artifact)

        if _stop_file_exists(stop_file):
            raise StopRequested(shard_id=shard_id, completed_rows=0, stage="validation")

        current_stage = "validation"
        if "validation" not in artifacts:
            validation_artifact = validate_shard_output(
                artifacts["filter"],
                artifacts["normalize"],
                artifacts["sampling"],
                shard_id=shard_id,
                shard_ordinal=int(spec["shard_ordinal"]),
                spec=spec,
            )
            publish("validation", validation_artifact)

        return {
            "shard_id": shard_id,
            "shard_ordinal": int(spec["shard_ordinal"]),
            "status": "success",
            "result": artifacts["validation"],
            "reused_stages": preparation["reused_stages"],
            "earliest_invalid_stage": preparation["earliest_invalid_stage"],
            "checkpoint": _checkpoint_summary(store, shard_id),
        }
    except StopRequested as exc:
        return {
            "shard_id": shard_id,
            "shard_ordinal": int(spec["shard_ordinal"]),
            "status": "stopped",
            "stage": exc.stage,
            "completed_rows_in_active_row_block_scan": int(exc.completed_rows),
            "reused_stages": preparation["reused_stages"] if preparation else [],
            "earliest_invalid_stage": preparation["earliest_invalid_stage"] if preparation else None,
            "checkpoint": _checkpoint_summary(store, shard_id),
        }
    except PipelineError as exc:
        return {
            "shard_id": shard_id,
            "shard_ordinal": int(spec["shard_ordinal"]),
            "status": "error",
            "stage": current_stage,
            "error": exc.to_dict(),
            "reused_stages": preparation["reused_stages"] if preparation else [],
            "earliest_invalid_stage": preparation["earliest_invalid_stage"] if preparation else None,
            "checkpoint": _checkpoint_summary(store, shard_id),
        }
    except Exception as exc:  # pragma: no cover - exercised by worker-failure tests
        structured = internal_error(exc, stage=current_stage, shard_id=shard_id)
        return {
            "shard_id": shard_id,
            "shard_ordinal": int(spec["shard_ordinal"]),
            "status": "error",
            "stage": current_stage,
            "error": structured.to_dict(),
            "reused_stages": preparation["reused_stages"] if preparation else [],
            "earliest_invalid_stage": preparation["earliest_invalid_stage"] if preparation else None,
            "checkpoint": _checkpoint_summary(store, shard_id),
        }
