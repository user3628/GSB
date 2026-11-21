"""Four-stage checkpointed pipeline for blocked reductions."""

from __future__ import annotations

import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from .errors import (
    ChunkOpsError,
    E_INVALID_AXIS,
    E_INVALID_CONFIG,
    E_INVALID_SHAPE,
    E_INVALID_STATE,
    E_NUMERIC_MISMATCH,
    E_STALE_PUBLISH,
    E_STALE_VERSION,
)
from .fingerprint import digest_object, numeric_range, sha256_array, sha256_file
from .ops import (
    ExecutionInterrupted,
    build_schedule,
    execute_blocked_op,
    find_zero_rows,
    reject_nonfinite_or_zero_rows,
    scan_nonfinite,
    validate_axis,
)
from .storage import (
    CheckpointManager,
    FileLock,
    ResultCache,
    STAGES,
    VersionStore,
    atomic_save_npy,
    atomic_write_json,
    file_artifact,
    read_json,
)
from .strategies import ChunkPlan, build_auto_plan, build_requested_plan
from .validation import validate_output

DEFAULT_CACHE_BUDGET = 4 * 1024 * 1024
DEFAULT_MEMORY_BUDGET = 512 * 1024 * 1024


def _workspace_path(workspace: str | os.PathLike[str]) -> Path:
    return Path(workspace).resolve()


def _run_config_path(workspace: Path) -> Path:
    return workspace / "run.json"


def _stop_path(workspace: Path) -> Path:
    return workspace / "stop.json"


def _load_or_error(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ChunkOpsError(
            E_INVALID_STATE,
            "run has not been imported",
            {"path": str(path), "hint": "call import_fragment or POST /api/fragments/import"},
        )
    return read_json(path)


def _validate_config_values(config: dict[str, Any]) -> None:
    if not isinstance(config.get("tolerance"), dict):
        raise ChunkOpsError(
            E_INVALID_CONFIG,
            "tolerance must be an object with atol and rtol",
            {"tolerance": config.get("tolerance")},
        )
    for key in ("cache_budget_bytes", "memory_budget_bytes"):
        value = config.get(key)
        if not isinstance(value, int) or value <= 0:
            raise ChunkOpsError(
                E_INVALID_CONFIG,
                f"{key} must be a positive integer",
                {key: value},
            )
    tolerance = config.get("tolerance", {})
    for key in ("atol", "rtol"):
        value = tolerance.get(key)
        if value is None or float(value) < 0:
            raise ChunkOpsError(
                E_INVALID_CONFIG,
                f"tolerance.{key} must be non-negative",
                {"tolerance": tolerance},
            )
    if float(config.get("eps", 0.0)) < 0:
        raise ChunkOpsError(
            E_INVALID_CONFIG,
            "eps must be non-negative",
            {"eps": config.get("eps")},
        )
    if config.get("output_dtype"):
        try:
            dtype = np.dtype(config["output_dtype"])
        except (TypeError, ValueError) as exc:
            raise ChunkOpsError(
                E_INVALID_CONFIG,
                "output_dtype is not a valid NumPy dtype",
                {"output_dtype": config.get("output_dtype")},
            ) from exc
        if not np.issubdtype(dtype, np.floating):
            raise ChunkOpsError(
                E_INVALID_CONFIG,
                "output_dtype must be floating point",
                {"output_dtype": str(dtype)},
            )
    if config.get("op") not in ("softmax", "layernorm"):
        raise ChunkOpsError(
            E_INVALID_CONFIG,
            "op must be softmax or layernorm",
            {"op": config.get("op")},
        )
    if int(config.get("axis", -1)) not in (0, 1):
        raise ChunkOpsError(
            E_INVALID_AXIS,
            "axis must be 0 or 1",
            {"axis": config.get("axis")},
        )


def import_fragment(
    workspace: str | os.PathLike[str],
    fragment: str | os.PathLike[str],
    *,
    op: str,
    axis: int,
    fragment_id: str | None = None,
    cache_budget_bytes: int = DEFAULT_CACHE_BUDGET,
    memory_budget_bytes: int = DEFAULT_MEMORY_BUDGET,
    eps: float = 1e-5,
    tolerance: dict[str, float] | None = None,
    output_dtype: str | None = None,
    affine: bool = False,
    plan_override: dict[str, Any] | None = None,
    validation_max_sample_blocks: int = 10,
    validation_seed: int | None = None,
    reject_zero_rows: bool = False,
    reject_nonfinite: bool = False,
) -> dict[str, Any]:
    """Validate/register a fragment in a workspace under a real file lock."""

    from .data import load_fragment

    workspace_path = _workspace_path(workspace)
    workspace_path.mkdir(parents=True, exist_ok=True)
    source = Path(fragment).resolve()
    chosen_fragment_id = fragment_id or source.stem
    with FileLock(workspace_path / ".import.lock"):
        x, gamma, beta, metadata = load_fragment(source, fragment_id=chosen_fragment_id)
        validate_axis(x, int(axis), fragment_id=chosen_fragment_id)
        nonfinite = scan_nonfinite(x, fragment_id=chosen_fragment_id)
        zero_rows = find_zero_rows(x)
        reject_nonfinite_or_zero_rows(
            x,
            fragment_id=chosen_fragment_id,
            reject_nonfinite=reject_nonfinite,
            reject_zero_rows=reject_zero_rows,
        )
        config = {
            "schema_version": 1,
            "run_version": 1,
            "run_id": workspace_path.name,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "fragment": {
                **metadata,
                "fragment_id": chosen_fragment_id,
                "nonfinite": nonfinite,
                "zero_rows": zero_rows,
            },
            "op": op,
            "axis": int(axis),
            "eps": float(eps),
            "tolerance": {
                "atol": float((tolerance or {}).get("atol", 1e-4)),
                "rtol": float((tolerance or {}).get("rtol", 1e-4)),
            },
            "output_dtype": output_dtype,
            "affine": bool(affine),
            "cache_budget_bytes": int(cache_budget_bytes),
            "memory_budget_bytes": int(memory_budget_bytes),
            "plan_override": plan_override,
            "validation": {
                "max_sample_blocks": int(validation_max_sample_blocks),
                "seed": validation_seed,
                "reject_zero_rows": bool(reject_zero_rows),
                "reject_nonfinite": bool(reject_nonfinite),
            },
            "pipeline_stages": list(STAGES),
        }
        _validate_config_values(config)
        if plan_override is not None:
            if not isinstance(plan_override, dict) or not {
                "row_block",
                "col_block",
                "sum_strategy",
            } <= set(plan_override):
                raise ChunkOpsError(
                    E_INVALID_CONFIG,
                    "plan_override requires row_block, col_block, and sum_strategy",
                    {"plan_override": plan_override},
                )
        if affine and op == "layernorm":
            reduction_length = x.shape[1] if int(axis) == 1 else x.shape[0]
            for name, value in (("gamma", gamma), ("beta", beta)):
                if value is not None and np.asarray(value).shape != (reduction_length,):
                    raise ChunkOpsError(
                        E_INVALID_SHAPE,
                        f"{name} does not match the reduction length",
                        {
                            "fragment_id": chosen_fragment_id,
                            "shape": [int(v) for v in x.shape],
                            "axis": int(axis),
                            "expected": [reduction_length],
                            f"{name}_shape": list(np.asarray(value).shape),
                        },
                    )
        config_path = _run_config_path(workspace_path)
        if config_path.exists():
            existing = read_json(config_path)
            previous_shape = existing.get("fragment", {}).get("shape_fingerprint")
            observed_shape = config["fragment"]["shape_fingerprint"]
            if previous_shape != observed_shape:
                raise ChunkOpsError(
                    E_STALE_VERSION,
                    "workspace run is bound to a different shape",
                    {
                        "stale_reason": "shape_fingerprint_changed",
                        "workspace": str(workspace_path),
                        "previous_shape_fingerprint": previous_shape,
                        "observed_shape_fingerprint": observed_shape,
                    },
                )
            previous_value = existing.get("fragment", {}).get("value_fingerprint")
            if previous_value != config["fragment"]["value_fingerprint"]:
                raise ChunkOpsError(
                    E_STALE_VERSION,
                    "workspace run is bound to different tensor values",
                    {
                        "stale_reason": "value_fingerprint_changed",
                        "workspace": str(workspace_path),
                        "previous_value_fingerprint": previous_value,
                        "observed_value_fingerprint": config["fragment"]["value_fingerprint"],
                    },
                )
            if existing.get("op") != op or int(existing.get("axis", -1)) != int(axis):
                raise ChunkOpsError(
                    E_STALE_PUBLISH,
                    "workspace run is bound to a different operator scope",
                    {
                        "workspace": str(workspace_path),
                        "existing_scope": {"op": existing.get("op"), "axis": existing.get("axis")},
                        "requested_scope": {"op": op, "axis": int(axis)},
                    },
                )
            return existing
        atomic_write_json(config_path, config)
        return config


def request_stop(workspace: str | os.PathLike[str], reason: str = "user_request") -> dict[str, Any]:
    workspace_path = _workspace_path(workspace)
    payload = {
        "schema_version": 1,
        "requested_at_utc": datetime.now(timezone.utc).isoformat(),
        "reason": reason,
        "mode": "graceful",
    }
    atomic_write_json(_stop_path(workspace_path), payload)
    return payload


def clear_stop(workspace: str | os.PathLike[str]) -> None:
    path = _stop_path(_workspace_path(workspace))
    if path.exists():
        path.unlink()
        directory = path.parent
        try:
            fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            pass


def stop_requested(workspace: str | os.PathLike[str]) -> bool:
    return _stop_path(_workspace_path(workspace)).exists()


class Pipeline:
    def __init__(self, workspace: str | os.PathLike[str]) -> None:
        self.workspace = _workspace_path(workspace)
        self.config = _load_or_error(_run_config_path(self.workspace))
        _validate_config_values(self.config)
        self.checkpoints = CheckpointManager(self.workspace)
        self.plan_versions = VersionStore(self.workspace, "plans")
        self.result_versions = VersionStore(self.workspace, "results")
        self.cache = ResultCache(self.workspace)

    @property
    def fragment_id(self) -> str:
        return self.config["fragment"]["fragment_id"]

    def _load_fragment_checked(self) -> tuple[np.ndarray, np.ndarray | None, np.ndarray | None]:
        from .data import load_fragment

        x, gamma, beta, metadata = load_fragment(
            self.config["fragment"]["path"], fragment_id=self.fragment_id
        )
        expected_shape = self.config["fragment"]["shape_fingerprint"]
        observed_shape = metadata["shape_fingerprint"]
        if expected_shape != observed_shape:
            raise ChunkOpsError(
                E_STALE_VERSION,
                "fragment shape fingerprint changed since import",
                {
                    "stale_reason": "shape_fingerprint_changed",
                    "fragment_id": self.fragment_id,
                    "expected_shape_fingerprint": expected_shape,
                    "observed_shape_fingerprint": observed_shape,
                },
            )
        expected_value = self.config["fragment"]["value_fingerprint"]
        observed_value = metadata["value_fingerprint"]
        if expected_value != observed_value:
            raise ChunkOpsError(
                E_STALE_VERSION,
                "fragment value fingerprint changed since import",
                {
                    "stale_reason": "value_fingerprint_changed",
                    "fragment_id": self.fragment_id,
                    "expected_value_fingerprint": expected_value,
                    "observed_value_fingerprint": observed_value,
                },
            )
        if not self.config.get("affine", False):
            gamma = None
            beta = None
        return x, gamma, beta

    def status(self) -> dict[str, Any]:
        checkpoint_info = self.checkpoints.discover()
        latest_stage = checkpoint_info["latest_stage"]
        state = "imported"
        state = {
            None: "imported",
            "plan": "planned",
            "reduce": "reduced",
            "validate": "validated",
            "summary": "complete",
        }.get(latest_stage, "invalid")
        if checkpoint_info.get("invalid"):
            state = "invalid_checkpoint"
        if stop_requested(self.workspace) and state != "complete":
            state = "stopped"
        output = None
        output_path = self.workspace / "outputs" / "output.npy"
        if output_path.exists():
            output = file_artifact(output_path)
        report_path = self.workspace / "reports" / "errors.json"
        summary_path = self.workspace / "summary.json"
        report_status = None
        if report_path.exists():
            try:
                report_status = read_json(report_path).get("status")
            except ChunkOpsError:
                report_status = "corrupt"
        stage_status = {
            stage: {
                "completed": any(
                    record["stage"] == stage
                    for record in checkpoint_info["valid_checkpoints"]
                ),
                "seq": next(
                    (
                        record["seq"]
                        for record in checkpoint_info["valid_checkpoints"]
                        if record["stage"] == stage
                    ),
                    None,
                ),
                "digest": next(
                    (
                        record["digest"]
                        for record in checkpoint_info["valid_checkpoints"]
                        if record["stage"] == stage
                    ),
                    None,
                ),
            }
            for stage in STAGES
        }
        return {
            "workspace": str(self.workspace),
            "run_id": self.config.get("run_id", self.workspace.name),
            "fragment_id": self.fragment_id,
            "fragment": {
                "path": self.config["fragment"]["path"],
                "shape": self.config["fragment"]["shape"],
                "dtype": self.config["fragment"]["dtype"],
                "shape_fingerprint": self.config["fragment"]["shape_fingerprint"],
                "value_fingerprint": self.config["fragment"]["value_fingerprint"],
            },
            "op": self.config["op"],
            "axis": self.config["axis"],
            "state": state,
            "latest_stage": latest_stage,
            "latest_seq": checkpoint_info["latest_seq"],
            "next_stage": checkpoint_info["resume_stage"],
            "stages": stage_status,
            "checkpoint_invalid": checkpoint_info.get("invalid"),
            "stop_requested": stop_requested(self.workspace),
            "versions": {
                "plans": self.plan_versions.inspect(),
                "results": self.result_versions.inspect(),
            },
            "version_fingerprint": {
                "plans": self.plan_versions.inspect(),
                "results": self.result_versions.inspect(),
            },
            "output": output,
            "error_report_available": report_path.exists(),
            "error_report_status": report_status,
            "summary_available": summary_path.exists(),
            "config": {
                "cache_budget_bytes": self.config["cache_budget_bytes"],
                "memory_budget_bytes": self.config["memory_budget_bytes"],
                "eps": self.config["eps"],
                "tolerance": self.config["tolerance"],
                "affine": self.config.get("affine", False),
            },
        }

    def plan_info(self) -> dict[str, Any]:
        record = self._stage_payload("plan")
        if record is None:
            raise ChunkOpsError(
                E_INVALID_STATE,
                "plan stage has not completed",
                {"workspace": str(self.workspace), "next_stage": self.checkpoints.discover()["resume_stage"]},
            )
        return record

    def _stage_payload(self, stage: str) -> dict[str, Any] | None:
        for record in self.checkpoints.discover()["valid_checkpoints"]:
            if record["stage"] == stage:
                return record["payload"]
        return None

    def error_report(self) -> dict[str, Any]:
        path = self.workspace / "reports" / "errors.json"
        if not path.exists():
            raise ChunkOpsError(
                E_INVALID_STATE,
                "error/validation report is not available",
                {"path": str(path), "next_stage": self.checkpoints.discover()["resume_stage"]},
            )
        return read_json(path)

    def summary(self) -> dict[str, Any]:
        path = self.workspace / "summary.json"
        if not path.exists():
            raise ChunkOpsError(
                E_INVALID_STATE,
                "summary has not been generated",
                {"path": str(path), "next_stage": self.checkpoints.discover()["resume_stage"]},
            )
        return read_json(path)

    def request_stop(self, reason: str = "user_request") -> dict[str, Any]:
        return request_stop(self.workspace, reason)

    def resume(
        self,
        *,
        max_stages: int | None = None,
        stop_after_stage: str | None = None,
    ) -> dict[str, Any]:
        clear_stop(self.workspace)
        return self.run(max_stages=max_stages, stop_after_stage=stop_after_stage)

    def run_stage(self, stage: str) -> dict[str, Any]:
        if stage not in STAGES:
            raise ChunkOpsError(E_INVALID_STATE, f"unknown stage: {stage}", {"stage": stage})
        info = self.checkpoints.discover()
        completed = {record["stage"] for record in info["valid_checkpoints"]}
        if stage in completed:
            return self.status()
        if info["resume_stage"] != stage:
            raise ChunkOpsError(
                E_INVALID_STATE,
                "requested stage is not the next executable stage",
                {
                    "requested_stage": stage,
                    "next_stage": info["resume_stage"],
                    "latest_stage": info["latest_stage"],
                },
            )
        return self.run(until_stage=stage)

    def run(
        self,
        *,
        until_stage: str | None = None,
        max_stages: int | None = None,
        stop_after_stage: str | None = None,
        clear_stop_request: bool = False,
    ) -> dict[str, Any]:
        if until_stage is not None and until_stage not in STAGES:
            raise ChunkOpsError(
                E_INVALID_CONFIG, f"unknown until_stage: {until_stage}", {"stage": until_stage}
            )
        if stop_after_stage is not None and stop_after_stage not in STAGES:
            raise ChunkOpsError(
                E_INVALID_CONFIG,
                f"unknown stop_after_stage: {stop_after_stage}",
                {"stage": stop_after_stage},
            )
        with FileLock(self.workspace / ".run.lock", timeout=300.0):
            if clear_stop_request:
                clear_stop(self.workspace)
            if stop_requested(self.workspace):
                return self.status()
            # A run/resume must never continue from a replaced or rolled-back
            # version store, even if the checkpoint chain itself is intact.
            self.plan_versions.verify()
            self.result_versions.verify()
            info = self.checkpoints.discover()
            if info.get("invalid"):
                # Preserve the evidence, then continue from the longest valid
                # prefix. Writing that stage atomically replaces the damaged
                # file and CheckpointManager removes stale successors.
                recovery_path = self.workspace / "reports" / "checkpoint_recovery.json"
                atomic_write_json(
                    recovery_path,
                    {
                        "schema_version": 1,
                        "action": "resume_from_last_consistent_prefix",
                        "latest_valid_stage": info.get("latest_stage"),
                        "latest_valid_seq": info.get("latest_seq"),
                        "invalid": info.get("invalid"),
                        "recovered_at_utc": datetime.now(timezone.utc).isoformat(),
                    },
                )
            stage_list = list(STAGES)
            next_index = len(info["valid_checkpoints"])
            executed = 0
            while next_index < len(stage_list):
                stage = stage_list[next_index]
                if until_stage is not None and STAGES.index(stage) > STAGES.index(until_stage):
                    break
                if max_stages is not None and executed >= max_stages:
                    break
                if stop_requested(self.workspace):
                    break
                try:
                    if stage == "plan":
                        self._execute_plan()
                    elif stage == "reduce":
                        self._execute_reduce()
                    elif stage == "validate":
                        self._execute_validate()
                    elif stage == "summary":
                        self._execute_summary()
                except ExecutionInterrupted:
                    return self.status()
                executed += 1
                next_index += 1
                if stop_after_stage == stage:
                    request_stop(self.workspace, reason=f"stop_after_stage:{stage}")
                    break
                if stop_requested(self.workspace):
                    break
            return self.status()

    def _build_plan(self, x: np.ndarray) -> ChunkPlan:
        anomaly_ratio = float(self.config["fragment"].get("nonfinite", {}).get("anomaly_ratio", 0.0))
        output_itemsize = (
            np.dtype(self.config["output_dtype"]).itemsize
            if self.config.get("output_dtype")
            else x.dtype.itemsize
        )
        override = self.config.get("plan_override")
        if override:
            plan = build_requested_plan(
                op=self.config["op"],
                axis=self.config["axis"],
                rows=int(x.shape[0]),
                cols=int(x.shape[1]),
                row_block=int(override["row_block"]),
                col_block=int(override["col_block"]),
                sum_strategy=str(override["sum_strategy"]),
                cache_budget_bytes=int(self.config["cache_budget_bytes"]),
                anomaly_ratio=anomaly_ratio,
                input_itemsize=x.dtype.itemsize,
                output_itemsize=output_itemsize,
            )
        else:
            plan = build_auto_plan(
                op=self.config["op"],
                axis=self.config["axis"],
                rows=int(x.shape[0]),
                cols=int(x.shape[1]),
                cache_budget_bytes=int(self.config["cache_budget_bytes"]),
                anomaly_ratio=anomaly_ratio,
                input_itemsize=x.dtype.itemsize,
                output_itemsize=output_itemsize,
            )
        expected_shape = (int(x.shape[0]), int(x.shape[1]))
        if (plan.rows, plan.cols) != expected_shape:
            raise ChunkOpsError(
                E_INVALID_SHAPE,
                "plan shape does not match fragment",
                {"plan_shape": [plan.rows, plan.cols], "fragment_shape": list(expected_shape)},
            )
        return plan

    def _execute_plan(self) -> None:
        x, _gamma, _beta = self._load_fragment_checked()
        plan = self._build_plan(x)
        output_itemsize = (
            np.dtype(self.config["output_dtype"]).itemsize
            if self.config.get("output_dtype")
            else x.dtype.itemsize
        )
        schedule = build_schedule(
            plan,
            input_itemsize=x.dtype.itemsize,
            output_itemsize=output_itemsize,
            memory_budget_bytes=int(self.config["memory_budget_bytes"]),
        )
        payload = {
            "scope": {"op": plan.op, "axis": plan.axis},
            "shape_fingerprint": self.config["fragment"]["shape_fingerprint"],
            "value_fingerprint": self.config["fragment"]["value_fingerprint"],
            "fragment_id": self.fragment_id,
            "cache_budget_bytes": int(self.config["cache_budget_bytes"]),
            "memory_budget_bytes": int(self.config["memory_budget_bytes"]),
            "plan": plan.to_dict(),
            "schedule_summary": schedule.to_dict(include_steps=False),
        }
        current = self.plan_versions.verify()
        if current["status"] == "healthy" and current["payload"] == payload:
            version = {
                "kind": "plans",
                "seq": current["seq"],
                "digest": current["digest"],
                "payload_digest": current["payload_digest"],
                "status": "reused",
            }
        else:
            version = self.plan_versions.publish(
                payload,
                expected_seq=int(current["seq"]),
                scope={"op": plan.op, "axis": plan.axis},
                publisher="pipeline.plan",
            )
        checkpoint_payload = {
            "plan": plan.to_dict(),
            "plan_version": version,
            "schedule": schedule.to_dict(include_steps=False),
            "fragment": {
                "fragment_id": self.fragment_id,
                "shape": [int(v) for v in x.shape],
                "shape_fingerprint": self.config["fragment"]["shape_fingerprint"],
                "value_fingerprint": self.config["fragment"]["value_fingerprint"],
            },
            "requested_by": "auto" if not self.config.get("plan_override") else "user",
        }
        self.checkpoints.write("plan", checkpoint_payload)

    def _effective_affine(
        self, gamma: np.ndarray | None, beta: np.ndarray | None
    ) -> tuple[np.ndarray | None, np.ndarray | None]:
        if self.config["op"] != "layernorm" or not self.config.get("affine", False):
            return None, None
        return gamma, beta

    def _cache_key(self, plan: ChunkPlan, schedule: Any, gamma: np.ndarray | None, beta: np.ndarray | None) -> str:
        return plan.cache_key(
            shape_fp=self.config["fragment"]["shape_fingerprint"],
            value_fp=self.config["fragment"]["value_fingerprint"],
            input_dtype=self.config["fragment"]["dtype"],
            output_dtype=self.config.get("output_dtype") or self.config["fragment"]["dtype"],
            eps=float(self.config["eps"]),
            gamma_fp=digest_object(gamma.tolist()) if gamma is not None else None,
            beta_fp=digest_object(beta.tolist()) if beta is not None else None,
            memory_budget_bytes=int(self.config["memory_budget_bytes"]),
            effective_row_block=int(schedule.effective_row_block),
            effective_col_block=int(schedule.effective_col_block),
        )

    def _execute_reduce(self) -> None:
        plan_payload = self._stage_payload("plan")
        if plan_payload is None:
            raise ChunkOpsError(E_INVALID_STATE, "plan checkpoint is required before reduce")
        plan = ChunkPlan.from_dict(plan_payload["plan"])
        x, gamma, beta = self._load_fragment_checked()
        gamma, beta = self._effective_affine(gamma, beta)
        output_itemsize = (
            np.dtype(self.config["output_dtype"]).itemsize
            if self.config.get("output_dtype")
            else x.dtype.itemsize
        )
        schedule = build_schedule(
            plan,
            input_itemsize=x.dtype.itemsize,
            output_itemsize=output_itemsize,
            memory_budget_bytes=int(self.config["memory_budget_bytes"]),
        )
        cache_key = self._cache_key(plan, schedule, gamma, beta)
        hit = self.cache.get(cache_key)
        cache_hit = hit is not None
        if hit is None:
            result = execute_blocked_op(
                x,
                plan,
                gamma=gamma,
                beta=beta,
                eps=float(self.config["eps"]),
                output_dtype=self.config.get("output_dtype"),
                memory_budget_bytes=int(self.config["memory_budget_bytes"]),
                fragment_id=self.fragment_id,
                stop_check=lambda: stop_requested(self.workspace),
            )
            output = result.output
            output_digest = result.output_digest
            execution_steps = result.execution_steps
            elapsed_seconds = result.elapsed_seconds
            self.cache.put(
                cache_key,
                output,
                {
                    "fragment_id": self.fragment_id,
                    "op": plan.op,
                    "axis": plan.axis,
                    "shape_fingerprint": self.config["fragment"]["shape_fingerprint"],
                    "value_fingerprint": self.config["fragment"]["value_fingerprint"],
                    "sum_strategy": plan.sum_strategy,
                    "row_block": plan.row_block,
                    "col_block": plan.col_block,
                    "cache_budget_bytes": plan.cache_budget_bytes,
                    "memory_budget_bytes": self.config["memory_budget_bytes"],
                    "effective_row_block": schedule.effective_row_block,
                    "effective_col_block": schedule.effective_col_block,
                },
            )
        else:
            output, cache_meta = hit
            output_digest = cache_meta["array_digest"]
            execution_steps = 0
            elapsed_seconds = 0.0
        output_path = self.workspace / "outputs" / "output.npy"
        atomic_save_npy(output_path, output)
        output_artifact = file_artifact(output_path)
        result_payload = {
            "scope": {"op": plan.op, "axis": plan.axis},
            "shape_fingerprint": self.config["fragment"]["shape_fingerprint"],
            "value_fingerprint": self.config["fragment"]["value_fingerprint"],
            "fragment_id": self.fragment_id,
            "plan_version_seq": plan_payload["plan_version"]["seq"],
            "cache_key": cache_key,
            "sum_strategy": plan.sum_strategy,
            "row_block": plan.row_block,
            "col_block": plan.col_block,
            "cache_budget_bytes": plan.cache_budget_bytes,
            "memory_budget_bytes": self.config["memory_budget_bytes"],
            "effective_row_block": schedule.effective_row_block,
            "effective_col_block": schedule.effective_col_block,
            "output_digest": output_digest,
        }
        current = self.result_versions.verify()
        if current["status"] == "healthy" and current["payload"] == result_payload:
            result_version = {
                "kind": "results",
                "seq": current["seq"],
                "digest": current["digest"],
                "payload_digest": current["payload_digest"],
                "status": "reused",
            }
        else:
            result_version = self.result_versions.publish(
                result_payload,
                expected_seq=int(current["seq"]),
                scope={"op": plan.op, "axis": plan.axis},
                publisher="pipeline.reduce",
            )
        checkpoint_payload = {
            "output": output_artifact,
            "output_digest": output_digest,
            "cache_key": cache_key,
            "cache_hit": cache_hit,
            "execution_steps": execution_steps,
            "elapsed_seconds": elapsed_seconds,
            "schedule": schedule.to_dict(include_steps=True),
            "plan_version": plan_payload["plan_version"],
            "result_version": result_version,
            "nonfinite": {
                key: self.config["fragment"]["nonfinite"].get(key)
                for key in ("first_any", "first_nan", "first_pos_inf", "first_neg_inf", "anomaly_ratio")
            },
        }
        self.checkpoints.write("reduce", checkpoint_payload)

    def _execute_validate(self) -> None:
        reduce_payload = self._stage_payload("reduce")
        plan_payload = self._stage_payload("plan")
        if reduce_payload is None or plan_payload is None:
            raise ChunkOpsError(
                E_INVALID_STATE,
                "reduce and plan checkpoints are required before validation",
            )
        output_path = Path(reduce_payload["output"]["path"])
        if not output_path.exists():
            raise ChunkOpsError(
                E_INVALID_STATE,
                "reduced output artifact is missing",
                {"path": str(output_path), "position": "output"},
            )
        observed_output_digest = sha256_file(output_path)
        if observed_output_digest != reduce_payload["output"]["sha256"]:
            raise ChunkOpsError(
                E_STALE_VERSION,
                "reduced output artifact digest changed",
                {
                    "stale_reason": "output_tamper",
                    "path": str(output_path),
                    "expected_sha256": reduce_payload["output"]["sha256"],
                    "observed_sha256": observed_output_digest,
                },
            )
        x, gamma, beta = self._load_fragment_checked()
        gamma, beta = self._effective_affine(gamma, beta)
        plan = ChunkPlan.from_dict(plan_payload["plan"])
        output = np.load(output_path, mmap_mode="r", allow_pickle=False)
        report = validate_output(
            x,
            np.asarray(output),
            plan,
            gamma=gamma,
            beta=beta,
            eps=float(self.config["eps"]),
            atol=float(self.config["tolerance"]["atol"]),
            rtol=float(self.config["tolerance"]["rtol"]),
            max_sample_blocks=int(self.config["validation"]["max_sample_blocks"]),
            seed=self.config["validation"].get("seed"),
            fragment_id=self.fragment_id,
            raise_on_mismatch=False,
            effective_row_block=int(reduce_payload["schedule"]["effective_row_block"]),
            effective_col_block=int(reduce_payload["schedule"]["effective_col_block"]),
        )
        report_path = self.workspace / "reports" / "errors.json"
        atomic_write_json(report_path, report)
        report_artifact = file_artifact(report_path)
        if report["status"] != "ok":
            raise ChunkOpsError(
                E_NUMERIC_MISMATCH,
                "sampled output blocks exceed configured tolerance",
                {
                    "fragment_id": self.fragment_id,
                    "failed_blocks": report["failed_blocks"],
                    "tolerance": report["tolerance"],
                    "report_path": str(report_path),
                    "report_sha256": report_artifact["sha256"],
                },
            )
        checkpoint_payload = {
            "status": report["status"],
            "report": report_artifact,
            "sampled_block_count": report["sampled_block_count"],
            "failed_block_count": len(report["failed_blocks"]),
            "tolerance": report["tolerance"],
            "error_estimate_count": len(report["error_estimates"]),
            "nonfinite": report["input_nonfinite"],
            "zero_rows": report["all_zero_rows"],
            "output_digest": report["output_digest"],
        }
        self.checkpoints.write("validate", checkpoint_payload)

    def _execute_summary(self) -> None:
        plan_payload = self._stage_payload("plan")
        reduce_payload = self._stage_payload("reduce")
        validate_payload = self._stage_payload("validate")
        if not (plan_payload and reduce_payload and validate_payload):
            raise ChunkOpsError(
                E_INVALID_STATE,
                "summary requires plan, reduce, and validate checkpoints",
            )
        plan = ChunkPlan.from_dict(plan_payload["plan"])
        checkpoint_records = self.checkpoints.discover()["valid_checkpoints"]
        stage_records = [
            {
                "stage": record["stage"],
                "seq": record["seq"],
                "digest": record["digest"],
                "payload_digest": digest_object(record["payload"]),
            }
            for record in checkpoint_records
        ]
        summary = {
            "schema_version": 1,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "run_id": self.config.get("run_id", self.workspace.name),
            "fragment": self.config["fragment"],
            "operation": {"op": plan.op, "axis": plan.axis},
            "stage_chain": stage_records,
            "plan": {
                "row_block": plan.row_block,
                "col_block": plan.col_block,
                "sum_strategy": plan.sum_strategy,
                "reason": plan.selection_reason,
                "score": plan.score,
                "score_details": plan.score_details,
                "candidate_count": len(plan.candidates),
                "rejected_candidates": plan.candidates,
                "workspace_bytes": plan.workspace_bytes,
                "estimated_traffic_bytes": plan.estimated_traffic_bytes,
                "predicted_error_bound": plan.predicted_error_bound,
            },
            "memory_schedule": reduce_payload["schedule"],
            "cache": {
                "cache_key": reduce_payload["cache_key"],
                "cache_hit": reduce_payload["cache_hit"],
            },
            "result": {
                "output": reduce_payload["output"],
                "output_digest": reduce_payload["output_digest"],
                "shape": self.config["fragment"]["shape"],
                "dtype": self.config.get("output_dtype")
                or self.config["fragment"]["dtype"],
                "nonfinite": reduce_payload["nonfinite"],
            },
            "validation": {
                "report": validate_payload["report"],
                "sampled_block_count": validate_payload["sampled_block_count"],
                "tolerance": validate_payload["tolerance"],
                "error_estimate_count": validate_payload["error_estimate_count"],
                "zero_rows": validate_payload["zero_rows"],
            },
            "versions": {
                "plans": self.plan_versions.inspect(),
                "results": self.result_versions.inspect(),
            },
            "resume_protocol": {
                "stages": list(STAGES),
                "latest_consistent_stage": checkpoint_records[-1]["stage"],
                "checkpoint_chain_head": checkpoint_records[-1]["digest"],
                "incremental_result_rule": "resumed output is compared by file sha256 to a full-build output",
            },
        }
        summary_path = self.workspace / "summary.json"
        atomic_write_json(summary_path, summary)
        summary_checkpoint = self.checkpoints.write(
            "summary",
            {
                "summary_path": str(summary_path.resolve()),
                "output_digest": reduce_payload["output_digest"],
                "validation_status": validate_payload["status"],
                "version_fingerprint": summary["versions"],
            },
        )
        summary["stage_chain"].append(
            {
                "stage": "summary",
                "seq": summary_checkpoint["seq"],
                "digest": summary_checkpoint["digest"],
                "payload_digest": digest_object(summary_checkpoint["payload"]),
            }
        )
        summary["summary_checkpoint"] = {
            "seq": summary_checkpoint["seq"],
            "digest": summary_checkpoint["digest"],
            "payload_digest": digest_object(summary_checkpoint["payload"]),
        }
        atomic_write_json(summary_path, summary)


def copy_output_for_comparison(source: str | os.PathLike[str], destination: str | os.PathLike[str]) -> str:
    """Copy an output artifact and return its sha256 for byte-identity tests."""

    target = Path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)
    return sha256_file(target)


def load_output(workspace: str | os.PathLike[str]) -> np.ndarray:
    path = _workspace_path(workspace) / "outputs" / "output.npy"
    if not path.exists():
        raise ChunkOpsError(E_INVALID_STATE, "output is not available", {"path": str(path)})
    return np.load(path, allow_pickle=False)
