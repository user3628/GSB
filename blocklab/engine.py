"""Three-stage deterministic execution engine with graceful stop and resume."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Callable

import numpy as np

from .activation import compute_activation_block, matrix_layout
from .checkpoints import (
    json_artifact,
    load_block_array,
    publish_block,
    scan_stage,
    stage_content_digest,
)
from .config import OperatorConfig
from .errors import (
    E_BUSY,
    E_CONFIG,
    E_INVALID_STAGE,
    E_NUMERIC,
    E_SHAPE_MISMATCH,
    E_STALE_CHECKPOINT,
    BusinessError,
)
from .paths import STAGE_INDEX, STAGES, FileLock
from .reduction import reduction_diagnostics, stable_aggregate
from .reorder import (
    compute_reorder_block,
    natural_target_shape,
    validate_declared_target,
)
from .tensorio import npy_bytes
from .util import atomic_write_bytes, atomic_write_json, digest_bytes, digest_json, json_values
from .workbench import Workbench


def _business_code_for_status(status: str) -> str | None:
    return E_NUMERIC if status == "completed_with_errors" else None


def _reorder_diagnostics(
    block: np.ndarray,
    *,
    row_range: list[int],
    col_range: list[int],
    context: dict[str, Any],
) -> dict[str, Any] | None:
    matrix = block.reshape(-1, block.shape[-1])
    mask = ~np.isfinite(matrix)
    zero_rows = np.flatnonzero(np.all(matrix == 0.0, axis=1))
    if not np.any(mask) and zero_rows.size == 0:
        return None
    positions = np.argwhere(mask)[:12]
    return {
        "code": E_NUMERIC,
        "stage": "reorder",
        **context,
        "row_range": [int(row_range[0]), int(row_range[1])],
        "col_range": [int(col_range[0]), int(col_range[1])],
        "nonfinite": {
            "count": int(np.count_nonzero(mask)),
            "positions": [[int(row), int(col)] for row, col in positions],
            "values": json_values([matrix[int(row), int(col)] for row, col in positions]),
        }
        if np.any(mask)
        else None,
        "zero_rows": {
            "count": int(zero_rows.size),
            "rows": [int(row) for row in zero_rows[:24]],
        }
        if zero_rows.size
        else None,
    }


class ExecutionEngine:
    def __init__(self, workbench: Workbench) -> None:
        self.wb = workbench

    def ensure_outputs(self, job_id: str) -> dict[str, Any]:
        """Recompose any complete stage outputs without recomputing blocks."""

        state = self.wb.sync_job(job_id)
        _job, paths, tensor, config, source_shape, _declared = self.wb.load_context(job_id)
        target_shape = tuple(state["actual_target_shape"])
        requested = list(STAGES)
        self._compose_completed_outputs(
            job_id=job_id,
            state=state,
            source_shape=source_shape,
            target_shape=target_shape,
            config=config,
            requested_stages=requested,
        )
        summary = paths.output_path("summary.json")
        return {
            "ok": True,
            "job_id": job_id,
            "summary_written": summary.exists(),
            "summary_sha256": digest_bytes(summary.read_bytes()) if summary.exists() else None,
        }

    def run_job(
        self,
        job_id: str,
        *,
        stages: list[str] | tuple[str, ...] | None = None,
        stop_after_stage: str | None = None,
        stop_after_blocks: int | None = None,
        block_delay_ms: int = 0,
        clear_stop: bool = False,
        external_stop: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        if stages is None or list(stages) == ["all"]:
            requested_stages = list(STAGES)
        else:
            requested_stages = list(stages)
            for stage in requested_stages:
                if stage not in STAGES:
                    raise BusinessError(
                        E_INVALID_STAGE,
                        "unknown stage",
                        requested_stage=stage,
                        allowed_stages=list(STAGES),
                    )
            requested_stages.sort(key=lambda stage: STAGE_INDEX[stage])
        if stop_after_stage is not None and stop_after_stage not in STAGES:
            raise BusinessError(
                E_INVALID_STAGE,
                "unknown stop_after_stage",
                requested_stage=stop_after_stage,
            )
        if stop_after_blocks is not None and stop_after_blocks < 0:
            raise BusinessError(E_CONFIG, "stop_after_blocks must be non-negative")

        paths = self.wb.paths.job(job_id)
        if not paths.job_file.exists():
            raise BusinessError(E_CONFIG, "job not found", job_id=job_id)
        try:
            run_lock = FileLock(paths.run_lock, blocking=False)
            run_lock.__enter__()
        except BlockingIOError as exc:
            raise BusinessError(E_BUSY, "another runner is active for this job", job_id=job_id) from exc

        try:
            if clear_stop:
                paths.stop_file.unlink(missing_ok=True)
            state = self.wb.sync_job(job_id)
            job, _paths, tensor, config, source_shape, declared_target = self.wb.load_context(job_id)
            target_shape = tuple(state["actual_target_shape"])
            # Reorder shape mismatch is checked before the reorder stage writes any block,
            # while activation remains a valid reusable product.
            if "reorder" in requested_stages or "reduce" in requested_stages:
                validate_declared_target(
                    source_shape=source_shape,
                    mode=config.reorder_mode,
                    declared_shape=declared_target,
                    config_shape=config.target_shape,
                )

            computed_blocks = 0
            reused_blocks = 0
            stage_results: dict[str, Any] = {}
            status = "completed"
            stopped_at: dict[str, Any] | None = None

            for stage in requested_stages:
                prerequisite_ok, prereq = self._prerequisite_ready(
                    job_id=job_id,
                    stage=stage,
                    state=state,
                    source_shape=source_shape,
                    target_shape=target_shape,
                    config=config,
                )
                if prerequisite_ok is False:
                    raise BusinessError(
                        E_INVALID_STAGE,
                        "requested stage is missing a completed prerequisite stage",
                        requested_stage=stage,
                        current_available_stage=prereq.get("available_stage"),
                        current_available_sequence=prereq.get("available_sequence", 0),
                        required_stage=STAGES[STAGE_INDEX[stage] - 1],
                    )
                if self._stop_requested(job_id, external_stop):
                    status = "stopped"
                    stopped_at = {
                        "stage": stage,
                        "reason": "stop_requested_before_stage",
                        "last_sequence": prereq.get("available_sequence", 0),
                    }
                    break

                stage_result = self._run_one_stage(
                    job_id=job_id,
                    stage=stage,
                    state=state,
                    source_shape=source_shape,
                    target_shape=target_shape,
                    config=config,
                    tensor_array=tensor.array,
                    block_delay_ms=block_delay_ms,
                    external_stop=external_stop,
                    max_computed_blocks=(
                        max(0, stop_after_blocks - computed_blocks)
                        if stop_after_blocks is not None
                        else None
                    ),
                )
                computed_blocks += stage_result["computed_blocks"]
                reused_blocks += stage_result["reused_blocks"]
                stage_results[stage] = stage_result
                alerts = stage_result["alerts"]
                if alerts:
                    status = "completed_with_errors"
                if stage_result["stopped"]:
                    status = "stopped"
                    stopped_at = {
                        "stage": stage,
                        "reason": stage_result["stop_reason"],
                        "last_sequence": stage_result["last_sequence"],
                    }
                    break
                if stop_after_stage == stage:
                    status = "stopped"
                    stopped_at = {
                        "stage": stage,
                        "reason": "stop_after_stage",
                        "last_sequence": stage_result["last_sequence"],
                    }
                    break
                if stop_after_blocks is not None and computed_blocks >= stop_after_blocks:
                    status = "stopped"
                    stopped_at = {
                        "stage": stage,
                        "reason": "stop_after_blocks",
                        "last_sequence": stage_result["last_sequence"],
                    }
                    break

            self._compose_completed_outputs(
                job_id=job_id,
                state=state,
                source_shape=source_shape,
                target_shape=target_shape,
                config=config,
                requested_stages=requested_stages,
            )
            fingerprint_bundle = self.wb.job_status(job_id)
            summary_digest = None
            summary_path = paths.output_path("summary.json")
            if summary_path.exists():
                summary_digest = digest_bytes(summary_path.read_bytes())
            result = {
                "ok": status in ("completed", "completed_with_errors"),
                "job_id": job_id,
                "status": status,
                "business_code": _business_code_for_status(status),
                "computed_blocks": computed_blocks,
                "reused_blocks": reused_blocks,
                "stages": stage_results,
                "stopped_at": stopped_at,
                "summary_sha256": summary_digest,
                "complete": fingerprint_bundle["complete"],
                "version_fingerprint": fingerprint_bundle["version_fingerprint"],
            }
            run_state = {
                "schema": "blocklab.run-state.v1",
                "job_id": job_id,
                "status": status,
                "business_code": result["business_code"],
                "computed_blocks": computed_blocks,
                "reused_blocks": reused_blocks,
                "stopped_at": stopped_at,
                "last_sequence": max(
                    (item["last_sequence"] for item in stage_results.values()), default=0
                ),
            }
            self.wb.write_run_state(job_id, run_state)
            if status in ("completed", "completed_with_errors"):
                paths.last_error_file.unlink(missing_ok=True)
            return result
        except BusinessError as error:
            self.wb.write_last_error(job_id, error)
            self.wb.write_run_state(
                job_id,
                {
                    "schema": "blocklab.run-state.v1",
                    "job_id": job_id,
                    "status": "failed",
                    "business_code": error.code,
                    "message": error.message,
                    "details": error.details,
                },
            )
            raise
        finally:
            run_lock.__exit__(None, None, None)

    def _stop_requested(
        self, job_id: str, external_stop: Callable[[], bool] | None
    ) -> bool:
        return self.wb.stop_requested(job_id) or bool(external_stop and external_stop())

    def _prerequisite_ready(
        self,
        *,
        job_id: str,
        stage: str,
        state: dict[str, Any],
        source_shape: tuple[int, ...],
        target_shape: tuple[int, ...],
        config: OperatorConfig,
    ) -> tuple[bool, dict[str, Any]]:
        if stage == "activation":
            return True, {}
        scans = self._scans_through(job_id, state, source_shape, target_shape, config, stop_before=stage)
        previous = STAGES[STAGE_INDEX[stage] - 1]
        scan = scans[previous]
        if scan["complete"]:
            return True, {
                "available_stage": previous,
                "available_sequence": scan["valid_blocks"],
            }
        return False, {
            "available_stage": previous,
            "available_sequence": scan["valid_blocks"],
        }

    def _scans_through(
        self,
        job_id: str,
        state: dict[str, Any],
        source_shape: tuple[int, ...],
        target_shape: tuple[int, ...],
        config: OperatorConfig,
        *,
        stop_before: str | None = None,
    ) -> dict[str, Any]:
        job, paths, tensor, _config, _source_shape, _declared = self.wb.load_context(job_id)
        plans = state["stage_plans"]
        epochs = state["stage_epochs"]
        scans: dict[str, Any] = {}
        for stage in STAGES:
            if stop_before is not None and STAGE_INDEX[stage] >= STAGE_INDEX[stop_before]:
                break
            schedule = self.wb.stage_schedule(
                stage=stage,
                source_shape=source_shape,
                target_shape=target_shape,
                config=config,
            )
            input_fp = self.wb.stage_input_fingerprint(
                stage=stage,
                input_digest=tensor.digest,
                activation_records=scans.get("activation", {}).get("records"),
                activation_plan=plans["activation"]["fingerprint"],
                reorder_records=scans.get("reorder", {}).get("records"),
                reorder_plan=plans["reorder"]["fingerprint"],
            )
            scan = scan_stage(
                paths,
                stage=stage,
                epoch=int(epochs[stage]),
                plan_fingerprint=plans[stage]["fingerprint"],
                input_fingerprint=input_fp,
                config_version=plans[stage]["config_version"],
                schedule=schedule,
            )
            if scan["complete"]:
                scan["content_digest"] = stage_content_digest(
                    scan["records"], stage=stage, plan_fingerprint=plans[stage]["fingerprint"]
                )
            else:
                scan["content_digest"] = None
            scans[stage] = scan
        return scans

    def _run_one_stage(
        self,
        *,
        job_id: str,
        stage: str,
        state: dict[str, Any],
        source_shape: tuple[int, ...],
        target_shape: tuple[int, ...],
        config: OperatorConfig,
        tensor_array: np.ndarray,
        block_delay_ms: int,
        external_stop: Callable[[], bool] | None,
        max_computed_blocks: int | None,
    ) -> dict[str, Any]:
        job, paths, tensor, _config, _source_shape, _declared = self.wb.load_context(job_id)
        plans = state["stage_plans"]
        epoch = int(state["stage_epochs"][stage])
        schedule = self.wb.stage_schedule(
            stage=stage,
            source_shape=source_shape,
            target_shape=target_shape,
            config=config,
        )
        upstream_scans = self._scans_through(
            job_id, state, source_shape, target_shape, config, stop_before=stage
        )
        input_fp = self.wb.stage_input_fingerprint(
            stage=stage,
            input_digest=tensor.digest,
            activation_records=upstream_scans.get("activation", {}).get("records"),
            activation_plan=plans["activation"]["fingerprint"],
            reorder_records=upstream_scans.get("reorder", {}).get("records"),
            reorder_plan=plans["reorder"]["fingerprint"],
        )
        scan = scan_stage(
            paths,
            stage=stage,
            epoch=epoch,
            plan_fingerprint=plans[stage]["fingerprint"],
            input_fingerprint=input_fp,
            config_version=plans[stage]["config_version"],
            schedule=schedule,
        )
        records = scan["records"]
        computed = 0
        reused = scan["valid_blocks"]
        stopped = False
        stop_reason = None
        last_sequence = scan["valid_blocks"]
        alerts = sum(1 for record in records if record.get("status") != "ok")

        activation_array = None
        reordered_array = None
        if stage == "reorder":
            activation_scan = upstream_scans["activation"]
            activation_array = self._assemble_activation(
                paths, epoch=int(state["stage_epochs"]["activation"]), records=activation_scan["records"], shape=source_shape
            )
        elif stage == "reduce":
            reorder_scan = upstream_scans["reorder"]
            reordered_array = self._assemble_reorder(
                paths,
                epoch=int(state["stage_epochs"]["reorder"]),
                records=reorder_scan["records"],
                target_shape=target_shape,
            )

        next_expected = scan["valid_blocks"] + 1
        if max_computed_blocks is not None and max_computed_blocks <= 0:
            return {
                "stage": stage,
                "epoch": epoch,
                "complete": scan["complete"],
                "computed_blocks": 0,
                "reused_blocks": scan["valid_blocks"],
                "valid_blocks": scan["valid_blocks"],
                "expected_blocks": len(schedule),
                "alerts": alerts,
                "stopped": True,
                "stop_reason": "stop_after_blocks",
                "last_sequence": scan["valid_blocks"],
            }
        while next_expected <= len(schedule):
            if self._stop_requested(job_id, external_stop):
                stopped = True
                stop_reason = "stop_requested"
                break
            item = schedule[next_expected - 1]
            artifact_bytes: bytes
            product_kind: str
            metadata: dict[str, Any]
            status = "ok"
            context = {
                "job_id": job_id,
                "shard_id": job["shard_id"],
                "block_key": item["key"],
                "block_index": int(item["index"]),
            }
            if stage == "activation":
                block, metadata = compute_activation_block(
                    tensor_array, item, config.activation, block_context=context
                )
                artifact_bytes = npy_bytes(block)
                product_kind = "npy"
            elif stage == "reorder":
                assert activation_array is not None
                block, metadata = compute_reorder_block(
                    activation_array, item, block_context=context
                )
                diagnostics = _reorder_diagnostics(
                    block,
                    row_range=item["row_range"],
                    col_range=item["col_range"],
                    context=context,
                )
                if diagnostics is not None:
                    metadata["status"] = "error"
                    metadata["diagnostics"] = diagnostics
                artifact_bytes = npy_bytes(block)
                product_kind = "npy"
            else:
                assert reordered_array is not None
                target_region = tuple(slice(a, b) for a, b in item["target_region"])
                block = np.ascontiguousarray(
                    reordered_array[target_region], dtype=np.float64
                )
                aggregate = stable_aggregate(block, config.sum_strategy)
                diagnostics = reduction_diagnostics(
                    block,
                    row_range=item["row_range"],
                    col_range=item["col_range"],
                    block_context=context,
                    tolerance={"abs_tol": config.abs_tol, "rel_tol": config.rel_tol},
                )
                block_status = "error" if diagnostics is not None else "ok"
                reduction_product = {
                    "schema": "blocklab.block-reduction.v1",
                    "stage": "reduce",
                    "block": item["key"],
                    "index": int(item["index"]),
                    "row_range": item["row_range"],
                    "col_range": item["col_range"],
                    "target_region": item["target_region"],
                    "aggregate": aggregate,
                    "tolerance": {"abs_tol": config.abs_tol, "rel_tol": config.rel_tol},
                    "status": block_status,
                    "diagnostics": diagnostics,
                }
                artifact_bytes = json_artifact(reduction_product)
                product_kind = "json"
                metadata = {
                    "block_key": item["key"],
                    "block_index": int(item["index"]),
                    "shape": [int(block.shape[0]), int(block.shape[1])]
                    if block.ndim == 2
                    else [int(dim) for dim in block.shape],
                    "aggregate": aggregate,
                    "source_region": item.get("source_region"),
                    "target_region": item.get("target_region"),
                    "status": block_status,
                    "diagnostics": diagnostics,
                }
            status = metadata.get("status", "ok")
            if status != "ok":
                alerts += 1
            if block_delay_ms:
                time.sleep(max(0, block_delay_ms) / 1000.0)
            try:
                record = publish_block(
                    paths,
                    stage=stage,
                    epoch=epoch,
                    requested_sequence=next_expected,
                    plan_fingerprint=plans[stage]["fingerprint"],
                    input_fingerprint=input_fp,
                    config_version=plans[stage]["config_version"],
                    workspace_config_version=config.version,
                    schedule=schedule,
                    schedule_item=item,
                    artifact_bytes=artifact_bytes,
                    product_kind=product_kind,
                    status=status,
                    metadata=metadata,
                )
                records.append(record)
                computed += 1
                last_sequence = next_expected
                next_expected += 1
                if max_computed_blocks is not None and computed >= max_computed_blocks:
                    stopped = True
                    stop_reason = "stop_after_blocks"
                    break
            except BusinessError as error:
                if error.code != E_STALE_CHECKPOINT:
                    raise
                refreshed = scan_stage(
                    paths,
                    stage=stage,
                    epoch=epoch,
                    plan_fingerprint=plans[stage]["fingerprint"],
                    input_fingerprint=input_fp,
                    config_version=plans[stage]["config_version"],
                    schedule=schedule,
                )
                if refreshed["valid_blocks"] >= next_expected:
                    records = refreshed["records"]
                    reused = refreshed["valid_blocks"]
                    last_sequence = refreshed["valid_blocks"]
                    next_expected = refreshed["valid_blocks"] + 1
                    continue
                raise

        complete = next_expected > len(schedule)
        if complete:
            self._compose_stage_output(
                job_id=job_id,
                stage=stage,
                state=state,
                target_shape=target_shape,
                config=config,
            )
        alerts = sum(1 for record in records if record.get("status") != "ok")
        return {
            "stage": stage,
            "epoch": epoch,
            "complete": complete,
            "computed_blocks": computed,
            "reused_blocks": reused,
            "valid_blocks": len(records),
            "expected_blocks": len(schedule),
            "alerts": alerts,
            "stopped": stopped,
            "stop_reason": stop_reason,
            "last_sequence": last_sequence,
        }

    def _assemble_activation(
        self, paths: Any, *, epoch: int, records: list[dict[str, Any]], shape: tuple[int, ...]
    ) -> np.ndarray:
        rows, cols = matrix_layout(shape)
        matrix = np.zeros((rows, cols), dtype=np.float64)
        mask = np.zeros((rows, cols), dtype=np.uint32)
        for record in records:
            block = load_block_array(paths, "activation", epoch, record)
            row_start, row_end = record["block"]["row_range"]
            col_start, col_end = record["block"]["col_range"]
            matrix[row_start:row_end, col_start:col_end] = block
            mask[row_start:row_end, col_start:col_end] += 1
        if not np.all(mask == 1):
            raise BusinessError(
                E_CONFIG,
                "activation checkpoint coverage is incomplete or overlapping",
                missing=int(np.count_nonzero(mask == 0)),
                overlapping=int(np.count_nonzero(mask > 1)),
            )
        return matrix.reshape(shape)

    def _assemble_reorder(
        self,
        paths: Any,
        *,
        epoch: int,
        records: list[dict[str, Any]],
        target_shape: tuple[int, ...],
    ) -> np.ndarray:
        output = np.zeros(target_shape, dtype=np.float64)
        mask = np.zeros(target_shape, dtype=np.uint32)
        for record in records:
            metadata = record.get("metadata", {})
            target_region = metadata.get("target_region")
            if not isinstance(target_region, list):
                raise BusinessError(E_CONFIG, "reorder checkpoint lacks target_region", block=record["block"]["key"])
            region = tuple(slice(a, b) for a, b in target_region)
            block = load_block_array(paths, "reorder", epoch, record)
            output[region] = block
            mask[region] += 1
        if not np.all(mask == 1):
            raise BusinessError(
                E_CONFIG,
                "reorder checkpoint coverage is incomplete or overlapping",
                missing=int(np.count_nonzero(mask == 0)),
                overlapping=int(np.count_nonzero(mask > 1)),
            )
        return output

    def _compose_completed_outputs(
        self,
        *,
        job_id: str,
        state: dict[str, Any],
        source_shape: tuple[int, ...],
        target_shape: tuple[int, ...],
        config: OperatorConfig,
        requested_stages: list[str],
    ) -> None:
        for stage in STAGES:
            if STAGE_INDEX[stage] > max(STAGE_INDEX[item] for item in requested_stages):
                break
            self._compose_stage_output(
                job_id=job_id,
                stage=stage,
                state=state,
                target_shape=target_shape,
                config=config,
            )

    def _compose_stage_output(
        self,
        *,
        job_id: str,
        stage: str,
        state: dict[str, Any],
        target_shape: tuple[int, ...],
        config: OperatorConfig,
    ) -> None:
        job, paths, tensor, _config, source_shape, _declared = self.wb.load_context(job_id)
        plans = state["stage_plans"]
        epochs = state["stage_epochs"]
        scans = self._scans_through(
            job_id,
            state,
            source_shape,
            target_shape,
            config,
            stop_before=None,
        )
        scan = scans[stage]
        if not scan["complete"]:
            return
        content_digest = stage_content_digest(
            scan["records"], stage=stage, plan_fingerprint=plans[stage]["fingerprint"]
        )
        if stage == "activation":
            output = self._assemble_activation(
                paths, epoch=int(epochs["activation"]), records=scan["records"], shape=source_shape
            )
            self._write_npy_output(
                paths=paths,
                name="activation",
                output=output,
                stage=stage,
                state=state,
                content_digest=content_digest,
                block_count=len(scan["records"]),
            )
        elif stage == "reorder":
            output = self._assemble_reorder(
                paths,
                epoch=int(epochs["reorder"]),
                records=scan["records"],
                target_shape=target_shape,
            )
            self._write_npy_output(
                paths=paths,
                name="reordered",
                output=output,
                stage=stage,
                state=state,
                content_digest=content_digest,
                block_count=len(scan["records"]),
            )
        else:
            self._write_summary(
                job_id=job_id,
                job=job,
                paths=paths,
                state=state,
                scans=scans,
                source_shape=source_shape,
                target_shape=target_shape,
                config=config,
            )

    def _write_npy_output(
        self,
        *,
        paths: Any,
        name: str,
        output: np.ndarray,
        stage: str,
        state: dict[str, Any],
        content_digest: str,
        block_count: int,
    ) -> None:
        meta_path = paths.output_path(f"{name}.meta.json")
        output_path = paths.output_path(f"{name}.npy")
        plan = state["stage_plans"][stage]["fingerprint"]
        if meta_path.exists() and output_path.exists():
            try:
                meta = __import__("json").loads(meta_path.read_text(encoding="utf-8"))
                if (
                    meta.get("stage_fingerprint") == plan
                    and meta.get("content_digest") == content_digest
                ):
                    return
            except Exception:
                pass
        data = npy_bytes(output)
        atomic_write_bytes(output_path, data)
        meta = {
            "schema": "blocklab.tensor-output.v1",
            "stage": stage,
            "stage_fingerprint": plan,
            "content_digest": content_digest,
            "sha256": digest_bytes(data),
            "shape": [int(dim) for dim in output.shape],
            "dtype": "float64",
            "block_count": block_count,
            "size_bytes": len(data),
        }
        atomic_write_json(meta_path, meta)

    def _write_summary(
        self,
        *,
        job_id: str,
        job: dict[str, Any],
        paths: Any,
        state: dict[str, Any],
        scans: dict[str, Any],
        source_shape: tuple[int, ...],
        target_shape: tuple[int, ...],
        config: OperatorConfig,
    ) -> None:
        if not scans["reduce"]["complete"] or not scans["reorder"]["complete"]:
            return
        reordered = self._assemble_reorder(
            paths,
            epoch=int(state["stage_epochs"]["reorder"]),
            records=scans["reorder"]["records"],
            target_shape=target_shape,
        )
        aggregate = stable_aggregate(reordered, config.sum_strategy)
        zero_rows = np.flatnonzero(np.all(reordered.reshape(-1, reordered.shape[-1]) == 0.0, axis=1))
        alerts: list[dict[str, Any]] = []
        block_reductions: list[dict[str, Any]] = []
        for stage in STAGES:
            for record in scans[stage]["records"]:
                if record.get("status") != "ok":
                    alerts.append(
                        {
                            "stage": stage,
                            "sequence": record["sequence"],
                            "block": record["block"],
                            "diagnostics": record.get("metadata", {}).get("diagnostics"),
                        }
                    )
                if stage == "reduce":
                    block_reductions.append(
                        {
                            "block": record["block"]["key"],
                            "sequence": record["sequence"],
                            "product_digest": record["product_digest"],
                            "metadata": record.get("metadata", {}),
                        }
                    )
        activation_meta = __import__("json").loads(
            paths.output_path("activation.meta.json").read_text(encoding="utf-8")
        )
        reorder_meta = __import__("json").loads(
            paths.output_path("reordered.meta.json").read_text(encoding="utf-8")
        )
        summary = {
            "schema": "blocklab.result.v1",
            "job_id": job_id,
            "shard_id": job["shard_id"],
            "source_path": job["source_path"],
            "config_path": job["config_path"],
            "config_version": config.version,
            "input_digest": state["input_digest"],
            "source_shape": list(source_shape),
            "declared_target_shape": state["declared_target_shape"],
            "actual_target_shape": list(target_shape),
            "fingerprints": {
                "version": self._version_fingerprint(state, scans),
                "stages": {
                    stage: {
                        "plan": state["stage_plans"][stage]["fingerprint"],
                        "content": scans[stage]["content_digest"],
                    }
                    for stage in STAGES
                },
            },
            "checkpoints": {
                stage: {
                    "epoch": state["stage_epochs"][stage],
                    "count": scans[stage]["valid_blocks"],
                    "last_sequence": scans[stage]["valid_blocks"],
                }
                for stage in STAGES
            },
            "outputs": {
                "activation": activation_meta,
                "reordered": reorder_meta,
            },
            "reduction": {
                **aggregate,
                "abs_tol": config.abs_tol,
                "rel_tol": config.rel_tol,
                "zero_rows": {
                    "count": int(zero_rows.size),
                    "rows": [int(row) for row in zero_rows[:32]],
                },
                "blocks": block_reductions,
            },
            "alerts": alerts,
            "business_code": E_NUMERIC if alerts else None,
            "deterministic": True,
        }
        atomic_write_json(paths.output_path("summary.json"), summary)

    def _version_fingerprint(self, state: dict[str, Any], scans: dict[str, Any]) -> str:
        return digest_json(
            {
                "schema": "blocklab.version-fingerprint.v1",
                "input_digest": state["input_digest"],
                "config_version": state["config_version"],
                "plans": {
                    stage: state["stage_plans"][stage]["fingerprint"] for stage in STAGES
                },
                "content": {stage: scans[stage].get("content_digest") for stage in STAGES},
            }
        )

