"""Stage execution, graceful stop, recovery, and export orchestration."""

from __future__ import annotations

import math
import signal
import time
import uuid
from io import BytesIO
from pathlib import Path
from typing import Any, Iterable

import numpy as np

from .canonical import npy_bytes, sha256_bytes
from .checkpoints import CheckpointStore
from .errors import InvalidRequestError, InvalidStageError, NotFoundError, ShapeMismatchError
from .manifest import ChainPlan, Workspace
from .ops import ExecOptions, execute_operator
from .storage import atomic_write_bytes, atomic_write_json, read_json, remove_if_exists


STOP_REQUEST = "STOP_REQUEST.json"


def request_stop(run_dir: str | Path, reason: str = "user request") -> dict[str, Any]:
    run_path = Path(run_dir).resolve()
    payload = {
        "format_version": 1,
        "request_id": uuid.uuid4().hex,
        "reason": reason,
    }
    atomic_write_json(run_path / STOP_REQUEST, payload)
    return payload


class StopController:
    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir
        self.request_path = run_dir / STOP_REQUEST
        self.signal_name: str | None = None
        self._old_handlers: dict[int, Any] = {}

    def install_signal_handlers(self) -> None:
        for number in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
            if number is None:
                continue
            try:
                self._old_handlers[number] = signal.getsignal(number)
                signal.signal(number, self._handle_signal)
            except (ValueError, OSError):
                continue

    def _handle_signal(self, signum: int, _frame: Any) -> None:
        self.signal_name = signal.Signals(signum).name

    def restore_signal_handlers(self) -> None:
        for number, handler in self._old_handlers.items():
            try:
                signal.signal(number, handler)
            except (ValueError, OSError):
                continue
        self._old_handlers.clear()

    def requested(self) -> bool:
        return self.signal_name is not None or self.request_path.is_file()

    def consume(self) -> dict[str, Any] | None:
        if not self.request_path.is_file():
            return None
        try:
            payload = read_json(self.request_path)
        except (OSError, ValueError):
            payload = {"format_version": 1, "reason": "unreadable stop request"}
        remove_if_exists(self.request_path)
        return payload


class Workbench:
    """Run and inspect deterministic shard operator chains."""

    def __init__(
        self,
        workspace_root: str | Path,
        run_dir: str | Path,
        *,
        exec_options: ExecOptions | None = None,
    ) -> None:
        self.workspace = Workspace(workspace_root)
        self.run_dir = Path(run_dir).resolve()
        self.exec_options = exec_options or ExecOptions()
        self.exec_options.validate()

    def _store(self, shard_id: str) -> CheckpointStore:
        return CheckpointStore(self.run_dir, shard_id)

    def _selected_shard_ids(self, shard_ids: Iterable[str] | None) -> list[str]:
        manifest = self.workspace.load_manifest()
        available = [shard["id"] for shard in manifest["shards"]]
        if shard_ids is None:
            return available
        selected = list(shard_ids)
        if not selected:
            raise InvalidRequestError("at least one shard must be selected")
        missing = [shard_id for shard_id in selected if shard_id not in available]
        if missing:
            raise NotFoundError("one or more shards were not found", shard_ids=missing)
        return list(dict.fromkeys(selected))

    def _expected_stage_payloads(self, plan: ChainPlan) -> list[dict[str, Any]]:
        return [
            {
                "operator_index": index,
                "chain_prefix": plan.chain_prefixes[index],
                "op_fingerprint": spec.fingerprint,
                "op_name": spec.name,
                "operator_summary": spec.summary,
                "config_version": plan.config_version,
            }
            for index, spec in enumerate(plan.specs)
        ]

    def _load_artifact(self, record: dict[str, Any]) -> np.ndarray:
        path = self.run_dir / str(record["artifact_path"])
        try:
            data = path.read_bytes()
        except OSError as exc:
            raise InvalidStageError(
                "required upstream checkpoint artifact is missing",
                shard_id=record.get("shard_id"),
                operator_index=record.get("operator_index"),
                artifact_path=str(path),
            ) from exc
        if sha256_bytes(data) != record.get("artifact_digest"):
            raise InvalidStageError(
                "required upstream checkpoint artifact is corrupted",
                shard_id=record.get("shard_id"),
                operator_index=record.get("operator_index"),
                artifact_path=str(path),
            )
        try:
            array = np.load(BytesIO(data), allow_pickle=False)
        except Exception as exc:
            raise InvalidStageError(
                "required upstream checkpoint artifact cannot be decoded",
                shard_id=record.get("shard_id"),
                operator_index=record.get("operator_index"),
            ) from exc
        if list(array.shape) != list(record.get("shape", [])):
            raise InvalidStageError(
                "upstream artifact shape does not match its checkpoint",
                shard_id=record.get("shard_id"),
                operator_index=record.get("operator_index"),
                artifact_shape=list(array.shape),
                checkpoint_shape=record.get("shape"),
            )
        return np.ascontiguousarray(array, dtype=np.float64)

    def _find_block(self, info: dict[str, Any], index: tuple[int, ...], shape: tuple[int, ...]) -> dict[str, Any]:
        for block in info.get("blocks", []):
            rows = block.get("output_rows", [0, shape[1]])
            cols = block.get("output_cols", [0, shape[2]])
            channels = block.get("output_channels", [0, shape[3]])
            if (
                rows[0] <= index[1] < rows[1]
                and cols[0] <= index[2] < cols[1]
                and channels[0] <= index[3] < channels[1]
            ):
                return block
        return {
            "output_rows": [0, int(shape[1])],
            "output_cols": [0, int(shape[2])],
            "output_channels": [0, int(shape[3])],
        }

    def _validate_output(
        self,
        plan: ChainPlan,
        stage_index: int,
        output: np.ndarray,
        execution: dict[str, Any],
    ) -> dict[str, Any]:
        expectations = self.workspace.stage_expectations(plan.shard_id)
        if stage_index >= len(expectations):
            raise ShapeMismatchError(
                "stage has no declared expected shape and numeric range",
                shard_id=plan.shard_id,
                operator_index=stage_index,
                block={"output_rows": [0, int(output.shape[1])], "output_cols": [0, int(output.shape[2])]},
            )
        declaration = expectations[stage_index]
        if not isinstance(declaration, dict):
            raise ShapeMismatchError(
                "stage expectation is not an object",
                shard_id=plan.shard_id,
                operator_index=stage_index,
            )
        declared_index = declaration.get("operator_index", stage_index)
        if declared_index != stage_index:
            raise ShapeMismatchError(
                "stage expectation has the wrong operator_index",
                shard_id=plan.shard_id,
                operator_index=stage_index,
                declared_operator_index=declared_index,
            )
        declared_shape = declaration.get("shape")
        if not isinstance(declared_shape, list) or len(declared_shape) != 4:
            raise ShapeMismatchError(
                "stage expectation requires a rank-4 shape",
                shard_id=plan.shard_id,
                operator_index=stage_index,
                block={"output_rows": [0, int(output.shape[1])], "output_cols": [0, int(output.shape[2])]},
                declared_shape=declared_shape,
                actual_shape=list(output.shape),
            )
        declared_shape_tuple = tuple(int(item) for item in declared_shape)
        if declared_shape_tuple != tuple(output.shape):
            block = execution.get("blocks", [{}])[0] if execution.get("blocks") else {}
            raise ShapeMismatchError(
                "computed output shape does not match the declared shape",
                shard_id=plan.shard_id,
                operator_index=stage_index,
                block=block,
                expected_shape=list(declared_shape_tuple),
                actual_shape=list(output.shape),
            )

        value_range = declaration.get("range", declaration.get("value_range"))
        if not isinstance(value_range, list) or len(value_range) != 2:
            raise ShapeMismatchError(
                "stage expectation requires a [min, max] numeric range",
                shard_id=plan.shard_id,
                operator_index=stage_index,
                block={"output_rows": [0, int(output.shape[1])], "output_cols": [0, int(output.shape[2])]},
            )
        try:
            lower = float(value_range[0])
            upper = float(value_range[1])
            tolerance = float(declaration.get("tolerance", 0.0))
        except (TypeError, ValueError) as exc:
            raise ShapeMismatchError(
                "stage numeric range is invalid",
                shard_id=plan.shard_id,
                operator_index=stage_index,
                declared_range=value_range,
            ) from exc
        if not (math.isfinite(lower) and math.isfinite(upper) and lower <= upper and tolerance >= 0.0):
            raise ShapeMismatchError(
                "stage numeric range is invalid",
                shard_id=plan.shard_id,
                operator_index=stage_index,
                declared_range=value_range,
            )
        if not np.all(np.isfinite(output)):
            nonfinite = np.argwhere(~np.isfinite(output))
            position = tuple(int(item) for item in nonfinite[0])
            raise ShapeMismatchError(
                "computed output contains a non-finite value",
                shard_id=plan.shard_id,
                operator_index=stage_index,
                block=self._find_block(execution, position, output.shape),
                output_index=list(position),
            )

        outside = np.argwhere((output < lower - tolerance) | (output > upper + tolerance))
        if outside.size:
            position = tuple(int(item) for item in outside[0])
            value = float(output[position])
            raise ShapeMismatchError(
                "computed output value is outside the declared numeric range",
                shard_id=plan.shard_id,
                operator_index=stage_index,
                block=self._find_block(execution, position, output.shape),
                output_index=list(position),
                value=value,
                declared_range=[lower, upper],
                actual_range=[float(np.min(output)), float(np.max(output))],
            )
        return {
            "shape": list(declared_shape_tuple),
            "range": [lower, upper],
            "actual_range": [float(np.min(output)), float(np.max(output))],
            "tolerance": tolerance,
        }

    def _execute_or_reuse_stage(
        self,
        plan: ChainPlan,
        stage_index: int,
        records: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], bool, dict[str, Any] | None]:
        """Return (checkpoint record, executed_now, execution_info)."""

        store = self._store(plan.shard_id)
        if stage_index < 0 or stage_index >= len(plan.specs):
            raise InvalidStageError(
                "stage index is outside the operator chain",
                shard_id=plan.shard_id,
                operator_index=stage_index,
                stage_count=len(plan.specs),
            )
        if stage_index > len(records):
            raise InvalidStageError(
                "cannot execute a stage without its immediate predecessor",
                shard_id=plan.shard_id,
                operator_index=stage_index,
                required_operator_index=stage_index - 1,
                published_checkpoint_seq=records[-1]["checkpoint_seq"] if records else 0,
            )
        if stage_index < len(records):
            record = records[stage_index]
            if record.get("chain_prefix") != plan.chain_prefixes[stage_index]:
                raise InvalidStageError(
                    "stored checkpoint is stale for the current chain",
                    shard_id=plan.shard_id,
                    operator_index=stage_index,
                )
            # Current truth declarations may have changed independently of the
            # computation parameters, so re-check reused artifacts as well.
            reused_output = self._load_artifact(record)
            self._validate_output(plan, stage_index, reused_output, record.get("execution") or {})
            return record, False, record.get("execution")

        spec = plan.specs[stage_index]
        if stage_index == 0:
            shard = self.workspace.shard_entry(plan.shard_id)[1]
            stage_input = self.workspace.load_tensor(shard)
            input_fingerprint = plan.tensor_fingerprint
        else:
            if not records:
                raise InvalidStageError(
                    "cannot execute a downstream stage without predecessor artifacts",
                    shard_id=plan.shard_id,
                    operator_index=stage_index,
                    required_operator_index=stage_index - 1,
                )
            stage_input = self._load_artifact(records[-1])
            input_fingerprint = records[-1]["artifact_digest"]

        output, execution = execute_operator(spec, stage_input, self.exec_options)
        expected = self._validate_output(plan, stage_index, output, execution)
        artifact = npy_bytes(output)
        template = {
            "tensor_fingerprint": plan.tensor_fingerprint,
            "op_name": spec.name,
            "op_fingerprint": spec.fingerprint,
            "input_fingerprint": input_fingerprint,
            "config_version": plan.config_version,
            "chain_prefix": plan.chain_prefixes[stage_index],
            "operator_summary": spec.summary,
            "shape": list(output.shape),
            "dtype": str(output.dtype),
            "expected_range": expected,
            "execution": execution,
        }
        record = store.publish_stage(
            stage_index=stage_index,
            artifact_bytes=artifact,
            template=template,
        )
        return record, True, execution

    def _stop_report(
        self,
        *,
        reason: str,
        plan: ChainPlan | None,
        records: list[dict[str, Any]],
        selected: list[str],
        completed_shards: list[str],
        request: dict[str, Any] | None,
    ) -> dict[str, Any]:
        report = {
            "format_version": 1,
            "status": "stopped",
            "reason": reason,
            "message": "the current indivisible stage completed before exit",
            "current_shard": plan.shard_id if plan else None,
            "last_checkpoint_seq": records[-1]["checkpoint_seq"] if records else 0,
            "pending_shards": [shard_id for shard_id in selected if shard_id not in completed_shards],
            "completed_shards": completed_shards,
            "stop_request": request,
        }
        atomic_write_json(self.run_dir / "reports" / "stop.json", report)
        return report

    def run(
        self,
        shard_ids: Iterable[str] | None = None,
        *,
        stop_after: int | None = None,
        delay_between_stages: float = 0.0,
        install_signal_handlers: bool = True,
    ) -> dict[str, Any]:
        if stop_after is not None and stop_after < 0:
            raise InvalidRequestError("stop_after must be non-negative")
        if delay_between_stages < 0:
            raise InvalidRequestError("delay_between_stages must be non-negative")
        selected = self._selected_shard_ids(shard_ids)
        # Plan every selected chain before publishing or executing any stage.
        plans = [self.workspace.plan_shard(shard_id) for shard_id in selected]
        controller = StopController(self.run_dir)
        if install_signal_handlers:
            controller.install_signal_handlers()
        executed = 0
        reused: dict[str, list[int]] = {shard_id: [] for shard_id in selected}
        invalidations: list[dict[str, Any]] = []
        last_records: list[dict[str, Any]] = []
        current_plan: ChainPlan | None = None
        completed_shards: list[str] = []
        stopped_reason: str | None = None
        stop_request: dict[str, Any] | None = None

        try:
            for plan in plans:
                current_plan = plan
                store = self._store(plan.shard_id)
                records, invalid_event = store.reconcile(
                    self._expected_stage_payloads(plan),
                    tensor_fingerprint=plan.tensor_fingerprint,
                )
                if invalid_event is not None:
                    invalidations.append(invalid_event)
                last_records = records
                for stage_index, _spec in enumerate(plan.specs):
                    if controller.requested():
                        stop_request = controller.consume()
                        stopped_reason = "stop_request" if stop_request is not None else "signal"
                        break
                    if stop_after is not None and executed >= stop_after:
                        stopped_reason = "stop_after"
                        break
                    if stage_index < len(records):
                        record, _executed, _execution = self._execute_or_reuse_stage(plan, stage_index, records)
                        if record.get("chain_prefix") != plan.chain_prefixes[stage_index]:
                            raise InvalidStageError(
                                "stored checkpoint is stale for the current chain",
                                shard_id=plan.shard_id,
                                operator_index=stage_index,
                            )
                        reused[plan.shard_id].append(stage_index)
                        continue
                    record, _executed, _execution = self._execute_or_reuse_stage(plan, stage_index, records)
                    records.append(record)
                    last_records = records
                    executed += 1
                    if delay_between_stages:
                        time.sleep(delay_between_stages)

                if stopped_reason is not None:
                    break
                completed_shards.append(plan.shard_id)

            if stopped_reason is not None:
                report = self._stop_report(
                    reason=stopped_reason,
                    plan=current_plan,
                    records=last_records,
                    selected=selected,
                    completed_shards=completed_shards,
                    request=stop_request,
                )
                return {
                    "ok": True,
                    "status": "stopped",
                    "report": report,
                    "executed_stages": executed,
                    "reused_stages": reused,
                    "invalidations": invalidations,
                }

            output_info = self.export(shard_ids=selected, write_trace=True)
            report = {
                "format_version": 1,
                "status": "complete",
                "executed_stages": executed,
                "reused_stages": reused,
                "invalidations": invalidations,
                "outputs": output_info["outputs"],
                "trace_path": output_info["trace_path"],
                "final_digests": {shard_id: info["artifact_digest"] for shard_id, info in output_info["outputs"].items()},
            }
            atomic_write_json(self.run_dir / "reports" / "run.json", report)
            return {"ok": True, "status": "complete", **report}
        finally:
            if install_signal_handlers:
                controller.restore_signal_handlers()

    def execute_stage(self, shard_id: str, stage_index: int) -> dict[str, Any]:
        """Request one stage.

        Missing immediate predecessors are a business validation error rather
        than an attempt to silently reconstruct a partial downstream state.
        """

        plan = self.workspace.plan_shard(shard_id)
        store = self._store(shard_id)
        records, invalid_event = store.reconcile(
            self._expected_stage_payloads(plan),
            tensor_fingerprint=plan.tensor_fingerprint,
        )
        record, executed, execution = self._execute_or_reuse_stage(plan, stage_index, records)
        return {
            "ok": True,
            "shard_id": shard_id,
            "operator_index": stage_index,
            "executed": executed,
            "checkpoint": record,
            "execution": execution,
            "invalidation": invalid_event,
        }

    def export(
        self,
        *,
        shard_ids: Iterable[str] | None = None,
        write_trace: bool = True,
    ) -> dict[str, Any]:
        selected = self._selected_shard_ids(shard_ids)
        outputs: dict[str, Any] = {}
        trace_shards: dict[str, Any] = {}
        for shard_id in selected:
            plan = self.workspace.plan_shard(shard_id)
            store = self._store(shard_id)
            records = store.load_records()
            if len(records) != len(plan.specs):
                raise InvalidStageError(
                    "operator chain is incomplete; export requires every stage",
                    shard_id=shard_id,
                    required_stages=len(plan.specs),
                    completed_stages=len(records),
                    missing_operator_index=len(records),
                )
            for index, record in enumerate(records):
                if record.get("chain_prefix") != plan.chain_prefixes[index]:
                    raise InvalidStageError(
                        "checkpoint chain does not match the current manifest; run recovery first",
                        shard_id=shard_id,
                        operator_index=index,
                    )
            final = self._load_artifact(records[-1])
            final_bytes = records[-1]
            destination = self.run_dir / "outputs" / f"{shard_id}.npy"
            artifact_data = (self.run_dir / records[-1]["artifact_path"]).read_bytes()
            atomic_write_bytes(destination, artifact_data)
            outputs[shard_id] = {
                "path": destination.relative_to(self.run_dir).as_posix(),
                "shape": list(final.shape),
                "dtype": str(final.dtype),
                "artifact_digest": final_bytes["artifact_digest"],
                "checkpoint_seq": final_bytes["checkpoint_seq"],
            }
            trace_shards[shard_id] = self._shard_trace(plan, records, outputs[shard_id])

        trace_path: str | None = None
        if write_trace:
            trace = {
                "format_version": 1,
                "config_version": self.workspace.load_manifest()["config_version"],
                "shards": trace_shards,
            }
            trace_file = self.run_dir / "outputs" / "trace.json"
            atomic_write_json(trace_file, trace)
            trace_path = trace_file.relative_to(self.run_dir).as_posix()
        return {"ok": True, "status": "complete", "outputs": outputs, "trace_path": trace_path}

    def _shard_trace(
        self,
        plan: ChainPlan,
        records: list[dict[str, Any]],
        output_info: dict[str, Any],
    ) -> dict[str, Any]:
        stages = []
        for index, record in enumerate(records):
            stages.append(
                {
                    "operator_index": index,
                    "operator_name": record["op_name"],
                    "checkpoint_seq": record["checkpoint_seq"],
                    "input_fingerprint": record["input_fingerprint"],
                    "op_fingerprint": record["op_fingerprint"],
                    "chain_prefix": record["chain_prefix"],
                    "shape": record["shape"],
                    "artifact_digest": record["artifact_digest"],
                    "artifact_path": record["artifact_path"],
                    "expected_range": record.get("expected_range"),
                }
            )
        return {
            "shard_id": plan.shard_id,
            "tensor": {
                "path": plan.tensor_path.relative_to(self.workspace.root).as_posix(),
                "fingerprint": plan.tensor_fingerprint,
                "shape": list(plan.tensor_shape),
            },
            "shape_trajectory": [list(plan.tensor_shape)] + [list(shape) for shape in plan.output_shapes],
            "stages": stages,
            "final_output": output_info,
        }

    def status(self, shard_id: str | None = None) -> dict[str, Any]:
        selected = self._selected_shard_ids([shard_id] if shard_id else None)
        result = {
            "format_version": 1,
            "run_dir": str(self.run_dir),
            "stop_requested": (self.run_dir / STOP_REQUEST).is_file(),
            "shards": {},
        }
        for selected_id in selected:
            plan = self.workspace.plan_shard(selected_id)
            store = self._store(selected_id)
            records = store.load_records()
            stages = []
            blocked = False
            valid_prefix = 0
            declarations = self.workspace.stage_expectations(selected_id)
            for index, spec in enumerate(plan.specs):
                record = records[index] if index < len(records) else None
                expected_range = None
                if index < len(declarations) and isinstance(declarations[index], dict):
                    expected_range = declarations[index].get("range", declarations[index].get("value_range"))
                if record is not None and not blocked and record.get("chain_prefix") == plan.chain_prefixes[index]:
                    state = "complete"
                    valid_prefix += 1
                elif record is not None:
                    state = "needs_rebuild"
                    blocked = True
                elif blocked:
                    state = "needs_rebuild"
                elif index == len(records):
                    state = "pending"
                else:
                    state = "not_started"
                stages.append(
                    {
                        "operator_index": index,
                        "operator_name": spec.name,
                        "state": state,
                        "planned_shape": list(plan.output_shapes[index]),
                        "expected_range": expected_range,
                        "op_fingerprint": spec.fingerprint,
                        "chain_prefix": plan.chain_prefixes[index],
                        "checkpoint": (
                            {
                                "checkpoint_seq": record["checkpoint_seq"],
                                "input_fingerprint": record["input_fingerprint"],
                                "artifact_digest": record["artifact_digest"],
                                "shape": record["shape"],
                            }
                            if record is not None
                            else None
                        ),
                    }
                )
            result["shards"][selected_id] = {
                "tensor_fingerprint": plan.tensor_fingerprint,
                "tensor_shape": list(plan.tensor_shape),
                "config_version": plan.config_version,
                "valid_prefix_stages": valid_prefix,
                "complete": valid_prefix == len(plan.specs),
                "stages": stages,
                "invalidations": store.invalidations(),
            }
        return {"ok": True, **result}

    def fingerprints(self, shard_id: str) -> dict[str, Any]:
        plan = self.workspace.plan_shard(shard_id)
        store = self._store(shard_id)
        records = store.load_records()
        declarations = self.workspace.stage_expectations(shard_id)
        chain = []
        for index, spec in enumerate(plan.specs):
            declaration = declarations[index] if index < len(declarations) else {}
            chain.append(
                {
                    "operator_index": index,
                    "operator_name": spec.name,
                    "op_fingerprint": spec.fingerprint,
                    "chain_prefix": plan.chain_prefixes[index],
                    "input_shape": list(plan.tensor_shape if index == 0 else plan.output_shapes[index - 1]),
                    "output_shape": list(plan.output_shapes[index]),
                    "declared_shape": declaration.get("shape") if isinstance(declaration, dict) else None,
                    "declared_range": (
                        declaration.get("range", declaration.get("value_range"))
                        if isinstance(declaration, dict)
                        else None
                    ),
                    "operator_summary": spec.summary,
                    "active_checkpoint": (
                        {
                            "checkpoint_seq": records[index]["checkpoint_seq"],
                            "input_fingerprint": records[index]["input_fingerprint"],
                            "artifact_digest": records[index]["artifact_digest"],
                            "chain_prefix": records[index]["chain_prefix"],
                        }
                        if index < len(records)
                        else None
                    ),
                }
            )
        return {
            "ok": True,
            "shard_id": shard_id,
            "config_version": plan.config_version,
            "tensor_fingerprint": plan.tensor_fingerprint,
            "tensor_shape": list(plan.tensor_shape),
            "chain": chain,
            "invalidations": store.invalidations(),
        }

    def self_check(self) -> dict[str, Any]:
        manifest = self.workspace.load_manifest()
        plans = [self.workspace.plan_shard(shard["id"]) for shard in manifest["shards"]]
        return {
            "ok": True,
            "workspace": str(self.workspace.root),
            "config_version": manifest["config_version"],
            "shards": [
                {
                    "id": plan.shard_id,
                    "tensor_shape": list(plan.tensor_shape),
                    "operators": [spec.name for spec in plan.specs],
                    "shape_trajectory": [list(plan.tensor_shape)] + [list(shape) for shape in plan.output_shapes],
                }
                for plan in plans
            ],
        }

