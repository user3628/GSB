"""Four-stage deterministic CTR/CVR pipeline orchestration."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

from .checkpoints import STAGES, CheckpointStore
from .config import Config
from .data import (
    apply_data_injection,
    load_manifest,
    load_rows,
    make_data_bundle,
    register_manifest_observation,
    validate_manifest_truth,
    validate_rows,
)
from .errors import ErrorCode, PipelineError
from .features import (
    FEATURE_FORMAT,
    assemble_features,
    feature_schema_fingerprint,
)
from .funnel import FUNNEL_FORMAT, evaluate_scores, label_diagnostics
from .model import (
    MODEL_FORMAT,
    DualParams,
    execute_tasks,
    initial_params,
    make_train_tasks,
    reduce_epoch,
    score_tasks,
)
from .scheduler import SchedulePlan, plan_shards
from .training import (
    epoch_orders,
    initial_rates,
    materialize_train_data,
    objective_metrics,
    score_digest,
    score_records,
)
from .util import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_hash,
    canonical_json_line,
    jsonable,
    read_json,
    resolve_under,
    sha256_bytes,
)


SUMMARY_FORMAT = "ctr-cvr-experiment-summary-v1"
TRAIN_PROGRESS_FORMAT = "ctr-cvr-train-progress-v1"
TRAINING_SUMMARY_FORMAT = "ctr-cvr-training-summary-v1"
INFERENCE_FORMAT = "ctr-cvr-inference-summary-v1"


def feature_stage_signature(schema_fingerprint: str, data_fingerprint: str) -> str:
    return canonical_hash(
        {
            "stage": "features",
            "format": FEATURE_FORMAT,
            "schema_fingerprint": schema_fingerprint,
            "data_content_fingerprint": data_fingerprint,
        }
    )


def train_stage_signature(
    config: Config,
    feature_artifact_digest: str,
    plan: SchedulePlan,
) -> str:
    return canonical_hash(
        {
            "stage": "train",
            "format": MODEL_FORMAT,
            "feature_artifact_digest": feature_artifact_digest,
            "seed": config.seed,
            "training": config.raw["training"],
            "scheduler_budget": config.global_round_budget,
            "scheduler_quota": config.shard_quota,
            "accepted_shards": [
                {"shard_id": item.shard_id, "index": item.index, "rows": item.accepted_rows}
                for item in plan.accepted
            ],
        }
    )


def funnel_stage_signature(
    model_digest: str,
    feature_artifact_digest: str,
    confidence_threshold: float,
) -> str:
    return canonical_hash(
        {
            "stage": "funnel",
            "format": FUNNEL_FORMAT,
            "model_artifact_digest": model_digest,
            "feature_artifact_digest": feature_artifact_digest,
            "confidence_threshold": confidence_threshold,
            "probability_policy": "no_clipping",
        }
    )


def infer_stage_signature(
    model_digest: str,
    feature_artifact_digest: str,
    funnel_digest: str,
    config: Config,
) -> str:
    return canonical_hash(
        {
            "stage": "infer",
            "format": INFERENCE_FORMAT,
            "model_artifact_digest": model_digest,
            "feature_artifact_digest": feature_artifact_digest,
            "funnel_artifact_digest": funnel_digest,
            "confidence_threshold": config.confidence_threshold,
            "score_format": config.score_format,
        }
    )


def public_schedule(plan: SchedulePlan) -> dict[str, Any]:
    payload = plan.to_dict()
    payload.pop("workers_requested", None)
    payload.pop("workers_effective", None)
    return payload


class Pipeline:
    def __init__(
        self,
        config_path: str,
        *,
        base_dir: str | Path = ".",
        injection: str | None = None,
        stop_event: Any | None = None,
        stop_after_epochs: int | None = None,
        worker_override: int | None = None,
        execution_mode: str | None = None,
    ):
        self.config_path = config_path
        self.base_dir = Path(base_dir).resolve()
        self.injection = injection
        self.stop_event = stop_event
        self.stop_after_epochs = stop_after_epochs
        self.worker_override = worker_override
        self.execution_mode = execution_mode
        self.config: Config | None = None
        self.store: CheckpointStore | None = None
        self.stage_status: dict[str, Any] = {}
        self.schedule: SchedulePlan | None = None
        self.last_artifacts: dict[str, str | None] = {}

    def run(self) -> dict[str, Any]:
        try:
            return self._run()
        except PipelineError as exc:
            self._write_last_run("failed", error=exc.to_dict())
            raise
        except BaseException as exc:
            error = PipelineError(
                ErrorCode.E_IO,
                f"unexpected pipeline failure: {exc}",
                stage="pipeline",
                details={"error_type": type(exc).__name__},
            )
            self._write_last_run("failed", error=error.to_dict())
            raise error from exc

    def _run(self) -> dict[str, Any]:
        config = Config.load(self.config_path, self.base_dir)
        manifest_path = resolve_under(self.base_dir, config.manifest_path, "paths.manifest")
        manifest = load_manifest(manifest_path)
        first_shard = manifest.shards[0].shard_id if manifest.shards else None
        config = config.injected(self.injection)
        config_stage = "train" if self.injection in {"negative_lr", "negative_epochs"} else "feature"
        config.validate(stage=config_stage, shard_id=first_shard, row_number=1)
        self.config = config
        self.store = CheckpointStore(config.checkpoints_root, self.base_dir)
        if self.stop_after_epochs is not None and self.stop_after_epochs < 0:
            raise PipelineError(
                ErrorCode.E_INVALID_PARAM,
                "stop_after_epochs must be non-negative",
                stage="train",
                shard_id=first_shard,
                row_number=1,
                details={"stop_after_epochs": self.stop_after_epochs},
            )

        register_manifest_observation(manifest, config.state_root)
        shards = load_rows(manifest)
        apply_data_injection(shards, self.injection)
        validation = validate_rows(shards)
        if validation.label_errors:
            output = config.outputs_root / "funnel" / "label-diagnostics.json"
            diagnostics = label_diagnostics(validation.label_errors)
            atomic_write_json(output, diagnostics)
            first = validation.label_errors[0]
            raise PipelineError(
                ErrorCode.E_LABEL_INVALID,
                str(first["message"]),
                stage="feature",
                shard_id=first.get("shard_id"),
                row_number=first.get("row_number"),
                impression_id=first.get("impression_id"),
                details={"diagnostics_path": str(output), "violations": validation.label_errors},
            )
        if validation.funnel_violations:
            output = config.outputs_root / "funnel" / "label-diagnostics.json"
            diagnostics = label_diagnostics(validation.funnel_violations)
            atomic_write_json(output, diagnostics)
            first = validation.funnel_violations[0]
            raise PipelineError(
                ErrorCode.E_FUNNEL_VIOLATION,
                str(first["message"]),
                stage="funnel",
                shard_id=first.get("shard_id"),
                row_number=first.get("row_number"),
                impression_id=first.get("impression_id"),
                details={"diagnostics_path": str(output), "violations": validation.funnel_violations},
            )
        validate_manifest_truth(shards, validation, injection=self.injection)
        data = make_data_bundle(manifest, shards)
        feature_schema = feature_schema_fingerprint(config)
        feature_signature = feature_stage_signature(feature_schema, data.data_content_fingerprint)

        features = assemble_features(data, config)
        feature_bytes = features.artifact_bytes()
        feature_digest = sha256_bytes(feature_bytes)
        feature_path = config.outputs_root / "features" / "features.json"
        self._invalidate_downstream("features", feature_signature, "feature inputs/configuration changed")
        feature_record = self.store.latest_completion("features", signature=feature_signature)
        if feature_record is None or feature_record.artifact_digest != feature_digest:
            feature_record = self.store.write_artifact_and_stage(
                stage="features",
                stage_signature=feature_signature,
                config_version=config.version,
                shard_input_fingerprint=data.data_content_fingerprint,
                artifact_path=feature_path,
                artifact_bytes=feature_bytes,
                details={
                    "schema_fingerprint": feature_schema,
                    "aggregate_fingerprint": features.aggregate_fingerprint,
                    "row_count": sum(len(shard.rows) for shard in features.shards),
                },
            )
            feature_reused = False
        else:
            feature_reused = True
        self.stage_status["features"] = self._record_status(feature_record, feature_reused)
        self.last_artifacts["features"] = feature_record.artifact_path

        plan = plan_shards(manifest.shards, config, worker_override=self.worker_override)
        self.schedule = plan
        if not plan.accepted:
            raise PipelineError(
                ErrorCode.E_SCHEDULE_EMPTY,
                "no shard fits the training budget",
                stage="train",
                shard_id=first_shard,
                row_number=1,
                details={"schedule": plan.to_dict()},
            )
        train_data = materialize_train_data(features, data, plan, config)
        train_signature = train_stage_signature(config, feature_digest, plan)
        model_path = config.outputs_root / "train" / "model.json"
        metrics_path = config.outputs_root / "train" / "metrics.json"
        training_summary_path = config.outputs_root / "train" / "training_summary.json"
        train_record = self.store.latest_completion("train", signature=train_signature)
        if train_record is not None:
            model_payload = read_json(self.store.resolve_artifact(train_record.artifact_path or ""))
            params = DualParams.from_fixed_model(model_payload)
            summary_relative = train_record.details.get("training_summary_path")
            if not summary_relative:
                raise PipelineError(
                    ErrorCode.E_CHECKPOINT_CORRUPT,
                    "train checkpoint is missing its training summary path",
                    stage="train",
                    details={"checkpoint_seq": train_record.seq},
                )
            training_summary = read_json(self.store.resolve_artifact(str(summary_relative)))
            train_reused = True
            self.stage_status["train"] = self._record_status(train_record, True)
        else:
            self._invalidate_downstream("train", train_signature, "training configuration or accepted data changed")
            progress_record = self.store.latest_progress("train", signature=train_signature)
            if progress_record is not None:
                progress_payload = read_json(self.store.resolve_artifact(progress_record.artifact_path or ""))
                params = DualParams.from_fixed_model(progress_payload["params_fixed"])
                completed_epochs = int(progress_payload["epochs_completed"])
                metrics = list(progress_payload["metrics"])
            else:
                ctr_rate, cvr_rate = initial_rates(train_data)
                params = initial_params(config.feature_dimension, ctr_rate, cvr_rate, config.quantization_scale)
                completed_epochs = 0
                metrics = []
            train_reused = False
            stop_reason: str | None = None
            while completed_epochs < config.epochs:
                epoch_number = completed_epochs + 1
                orders, traversal = epoch_orders(train_data, config.seed, epoch_number)
                tasks = make_train_tasks(train_data, orders, params)
                mode, workers = self._execution_controls(plan)
                from .model import _train_epoch_shard

                successes, errors = execute_tasks(_train_epoch_shard, tasks, workers, mode)
                if errors:
                    error_path = config.outputs_root / "train" / "worker_errors.json"
                    atomic_write_json(
                        error_path,
                        {
                            "format": "ctr-cvr-worker-errors-v1",
                            "stage": "train",
                            "epoch": epoch_number,
                            "completed_shards": [item["shard_id"] for item in successes],
                            "completed_shard_results": jsonable(successes),
                            "errors": errors,
                        },
                    )
                    raise PipelineError(
                        ErrorCode.E_WORKER_FAILED,
                        errors[0]["message"],
                        stage="train",
                        shard_id=errors[0].get("shard_id"),
                        row_number=errors[0].get("row_number"),
                        details={
                            "worker_errors_path": str(error_path),
                            "completed_shards": [item["shard_id"] for item in successes],
                            "errors": errors,
                        },
                    )
                params, epoch_metrics = reduce_epoch(
                    successes,
                    params,
                    config.learning_rate,
                    config.combo_weight,
                    config.quantization_scale,
                )
                epoch_metrics["epoch"] = epoch_number
                epoch_metrics["sample_traversal"] = traversal
                epoch_metrics["model_fingerprint"] = params.fingerprint(
                    config.feature_dimension, config.quantization_scale
                )
                metrics.append(epoch_metrics)
                completed_epochs = epoch_number
                stop_requested = False
                if completed_epochs < config.epochs:
                    if self.stop_event is not None and self.stop_event.is_set():
                        stop_requested = True
                        stop_reason = "cooperative stop requested by workbench"
                    elif self.stop_after_epochs is not None and completed_epochs >= self.stop_after_epochs:
                        stop_requested = True
                        stop_reason = "stop_after_epochs threshold reached"
                progress_status = "stopped" if stop_requested else "running"
                progress_payload = {
                    "format": TRAIN_PROGRESS_FORMAT,
                    "train_signature": train_signature,
                    "epochs_completed": completed_epochs,
                    "status": progress_status,
                    "params_fixed": params.fixed_model(config.feature_dimension, config.quantization_scale),
                    "metrics": metrics,
                }
                progress_path = (
                    config.outputs_root
                    / "train"
                    / "progress"
                    / f"epoch-{completed_epochs:04d}-{progress_status}.json"
                )
                progress_bytes = canonical_json_line(progress_payload)
                progress_record = self.store.write_progress(
                    stage="train",
                    stage_signature=train_signature,
                    config_version=config.version,
                    shard_input_fingerprint=data.data_content_fingerprint,
                    artifact_path=progress_path,
                    artifact_bytes=progress_bytes,
                    status=progress_status,
                    details={"epochs_completed": completed_epochs, "reason": stop_reason},
                )
                self.stage_status["train"] = self._record_status(progress_record, False)
                if stop_requested:
                    outcome = {
                        "status": "stopped",
                        "reason": stop_reason,
                        "epochs_completed": completed_epochs,
                        "epochs_total": config.epochs,
                        "checkpoint_seq": progress_record.seq,
                        "artifact_path": progress_record.artifact_path,
                    }
                    self._write_last_run("stopped", extra=outcome)
                    return outcome

            model_payload = params.fixed_model(config.feature_dimension, config.quantization_scale)
            model_bytes = canonical_json_line(model_payload)
            model_digest = sha256_bytes(model_bytes)
            metrics_bytes = b"".join(canonical_json_line(metric) for metric in metrics)
            training_summary = {
                "format": TRAINING_SUMMARY_FORMAT,
                "train_signature": train_signature,
                "epochs_completed": completed_epochs,
                "loss_curve_digest": canonical_hash(metrics),
                "final_model_fingerprint": model_digest,
                "final_epoch": metrics[-1] if metrics else None,
                "schedule": public_schedule(plan),
                "learning_rate": config.learning_rate,
                "combo_weight": config.combo_weight,
                "quantization_scale": config.quantization_scale,
            }
            summary_bytes = canonical_json_line(training_summary)
            atomic_write_bytes(metrics_path, metrics_bytes)
            atomic_write_bytes(training_summary_path, summary_bytes)
            train_record = self.store.write_artifact_and_stage(
                stage="train",
                stage_signature=train_signature,
                config_version=config.version,
                shard_input_fingerprint=data.data_content_fingerprint,
                artifact_path=model_path,
                artifact_bytes=model_bytes,
                details={
                    "metrics_path": str(metrics_path.relative_to(self.base_dir).as_posix()),
                    "metrics_digest": sha256_bytes(metrics_bytes),
                    "training_summary_path": str(training_summary_path.relative_to(self.base_dir).as_posix()),
                    "training_summary_digest": sha256_bytes(summary_bytes),
                    "epochs_completed": completed_epochs,
                },
            )
            self.stage_status["train"] = self._record_status(train_record, False)
        self.last_artifacts["train"] = train_record.artifact_path
        model_digest = train_record.artifact_digest or sha256_bytes(canonical_json_line(params.fixed_model(config.feature_dimension, config.quantization_scale)))

        mode, workers = self._execution_controls(plan)
        scoring_tasks = score_tasks(train_data, params)
        from .model import _score_shard

        score_successes, score_errors = execute_tasks(_score_shard, scoring_tasks, workers, mode)
        if score_errors:
            error_path = config.outputs_root / "funnel" / "worker_errors.json"
            atomic_write_json(
                error_path,
                {
                    "format": "ctr-cvr-worker-errors-v1",
                    "stage": "funnel",
                    "completed_shards": [item["shard_id"] for item in score_successes],
                    "completed_shard_results": jsonable(score_successes),
                    "errors": score_errors,
                },
            )
            raise PipelineError(
                ErrorCode.E_WORKER_FAILED,
                score_errors[0]["message"],
                stage="funnel",
                shard_id=score_errors[0].get("shard_id"),
                row_number=score_errors[0].get("row_number"),
                details={
                    "worker_errors_path": str(error_path),
                    "completed_shards": [item["shard_id"] for item in score_successes],
                    "errors": score_errors,
                },
            )
        records = score_records(train_data, score_successes)
        diagnostics = evaluate_scores(records, config.confidence_threshold)
        diagnostics_path = config.outputs_root / "funnel" / "diagnostics.json"
        diagnostics_bytes = canonical_json_line(diagnostics)
        diagnostics_digest = sha256_bytes(diagnostics_bytes)
        funnel_signature = funnel_stage_signature(
            model_digest, feature_digest, config.confidence_threshold
        )
        self._invalidate_downstream("funnel", funnel_signature, "model or confidence threshold changed")
        funnel_record = self.store.latest_completion("funnel", signature=funnel_signature)
        if diagnostics["violation_count"]:
            atomic_write_bytes(diagnostics_path, diagnostics_bytes)
            first = diagnostics["violations"][0]
            raise PipelineError(
                ErrorCode.E_FUNNEL_VIOLATION,
                str(first["message"]),
                stage="funnel",
                shard_id=first.get("shard_id"),
                row_number=first.get("row_number"),
                impression_id=first.get("impression_id"),
                details={
                    "diagnostics_path": str(diagnostics_path),
                    "violation_count": diagnostics["violation_count"],
                    "violations": diagnostics["violations"],
                },
            )
        if funnel_record is None or funnel_record.artifact_digest != diagnostics_digest:
            funnel_record = self.store.write_artifact_and_stage(
                stage="funnel",
                stage_signature=funnel_signature,
                config_version=config.version,
                shard_input_fingerprint=data.data_content_fingerprint,
                artifact_path=diagnostics_path,
                artifact_bytes=diagnostics_bytes,
                details={
                    "violation_count": 0,
                    "checked_rows": diagnostics["score_checks"]["checked_rows"],
                },
            )
            funnel_reused = False
        else:
            funnel_reused = True
        self.stage_status["funnel"] = self._record_status(funnel_record, funnel_reused)
        self.last_artifacts["funnel"] = funnel_record.artifact_path

        threshold_log = math.log(config.confidence_threshold)
        score_rows = [record.score_dict(threshold_log) for record in records]
        scores_bytes = b"".join(canonical_json_line(row) for row in score_rows)
        scores_path = config.outputs_root / "infer" / "scores.jsonl"
        inference_summary = {
            "format": INFERENCE_FORMAT,
            "rows": len(score_rows),
            "score_digest": score_digest(records),
            "metrics": objective_metrics(records, config.confidence_threshold),
            "score_format": config.score_format,
            "probability_policy": "computed in log domain; no clipping",
        }
        inference_bytes = canonical_json_line(inference_summary)
        inference_summary_path = config.outputs_root / "infer" / "inference_summary.json"
        infer_signature = infer_stage_signature(model_digest, feature_digest, diagnostics_digest, config)
        self._invalidate_downstream("infer", infer_signature, "model, funnel diagnostics or inference configuration changed")
        infer_record = self.store.latest_completion("infer", signature=infer_signature)
        if infer_record is None or infer_record.artifact_digest != sha256_bytes(scores_bytes):
            atomic_write_bytes(inference_summary_path, inference_bytes)
            infer_record = self.store.write_artifact_and_stage(
                stage="infer",
                stage_signature=infer_signature,
                config_version=config.version,
                shard_input_fingerprint=data.data_content_fingerprint,
                artifact_path=scores_path,
                artifact_bytes=scores_bytes,
                details={
                    "score_count": len(score_rows),
                    "score_digest": inference_summary["score_digest"],
                    "inference_summary_path": str(inference_summary_path.relative_to(self.base_dir).as_posix()),
                    "inference_summary_digest": sha256_bytes(inference_bytes),
                },
            )
            infer_reused = False
        else:
            infer_reused = True
        self.stage_status["infer"] = self._record_status(infer_record, infer_reused)
        self.last_artifacts["infer"] = infer_record.artifact_path

        summary = {
            "format": SUMMARY_FORMAT,
            "experiment": config.experiment_name,
            "seed": config.seed,
            "config_version": config.version,
            "data_content_fingerprint": data.data_content_fingerprint,
            "feature_schema_fingerprint": feature_schema,
            "feature_artifact_digest": feature_digest,
            "model_artifact_digest": model_digest,
            "funnel_artifact_digest": diagnostics_digest,
            "inference_artifact_digest": sha256_bytes(scores_bytes),
            "schedule": public_schedule(plan),
            "training": {
                "epochs_completed": training_summary["epochs_completed"],
                "loss_curve_digest": training_summary["loss_curve_digest"],
                "final_epoch": training_summary["final_epoch"],
                "quantization_scale": training_summary["quantization_scale"],
                "learning_rate": training_summary["learning_rate"],
                "combo_weight": training_summary["combo_weight"],
            },
            "funnel": {
                "violation_count": 0,
                "checked_rows": diagnostics["score_checks"]["checked_rows"],
                "confidence_threshold": config.confidence_threshold,
            },
            "inference": inference_summary["metrics"],
        }
        summary_path = config.outputs_root / "experiment_summary.json"
        summary_bytes = canonical_json_line(summary)
        atomic_write_bytes(summary_path, summary_bytes)
        result = {
            "status": "completed",
            "config_version": config.version,
            "data_content_fingerprint": data.data_content_fingerprint,
            "summary": summary,
            "summary_path": str(summary_path),
            "summary_digest": sha256_bytes(summary_bytes),
            "losses_path": str(metrics_path),
            "funnel_path": str(diagnostics_path),
            "scores_path": str(scores_path),
            "model_path": str(model_path),
            "stages": self.stage_status,
            "schedule": plan.to_dict(),
        }
        self._write_last_run("completed", extra=result)
        return result

    def _execution_controls(self, plan: SchedulePlan) -> tuple[str, int]:
        requested = self.worker_override if self.worker_override is not None else (self.config.max_workers if self.config else 1)
        workers = min(int(requested), max(1, len(plan.accepted)))
        mode = self.execution_mode
        if mode is None:
            mode = "sequential" if workers <= 1 else "parallel"
        if mode not in {"sequential", "parallel"}:
            raise PipelineError(
                ErrorCode.E_INVALID_PARAM,
                "execution_mode must be sequential or parallel",
                stage="train",
                details={"execution_mode": mode},
            )
        if mode == "sequential":
            workers = 1
        return mode, workers

    def _invalidate_downstream(self, stage: str, expected_signature: str, reason: str) -> None:
        assert self.store is not None and self.config is not None
        current = self.store.latest_completion(stage, signature=expected_signature)
        if current is not None:
            return
        progress = self.store.latest_progress(stage, signature=expected_signature)
        if progress is not None:
            return
        prior_stage_records = [
            record
            for record in self.store.records()
            if record.stage == stage and record.kind in {"stage_complete", "stage_progress"}
        ]
        if not prior_stage_records:
            return
        latest_invalidations = self.store.invalidations()
        invalidated = list(STAGES[STAGES.index(stage) :])
        if latest_invalidations:
            latest = latest_invalidations[-1]
            if (
                latest.details.get("earliest_stage") == stage
                and latest.details.get("invalidated_stages") == invalidated
                and latest.details.get("reason") == reason
            ):
                return
        self.store.invalidate(
            earliest_stage=stage,
            invalidated_stages=invalidated,
            reason=reason,
            config_version=self.config.version,
            details={"expected_signature": expected_signature},
        )

    @staticmethod
    def _record_status(record: Any, reused: bool) -> dict[str, Any]:
        return {
            "status": "valid",
            "reused": bool(reused),
            "seq": record.seq,
            "signature": record.stage_signature,
            "artifact_digest": record.artifact_digest,
            "artifact_path": record.artifact_path,
        }

    def _write_last_run(self, status: str, error: dict[str, Any] | None = None, extra: dict[str, Any] | None = None) -> None:
        state_root = self.config.state_root if self.config is not None else self.base_dir / "state"
        config_version = self.config.version if self.config is not None else None
        payload = {
            "format": "ctr-cvr-last-run-v1",
            "status": status,
            "config_version": config_version,
            "stages": self.stage_status,
            "artifacts": self.last_artifacts,
            "schedule": self.schedule.to_dict() if self.schedule is not None else None,
            "error": error,
            "result": extra,
        }
        try:
            atomic_write_json(state_root / "last_run.json", payload)
        except OSError:
            pass

    @classmethod
    def status(cls, config_path: str, *, base_dir: str | Path = ".") -> dict[str, Any]:
        base = Path(base_dir).resolve()
        config = Config.load(config_path, base)
        manifest = load_manifest(resolve_under(base, config.manifest_path, "paths.manifest"))
        shards = load_rows(manifest)
        data = make_data_bundle(manifest, shards)
        schema = feature_schema_fingerprint(config)
        feature_sig = feature_stage_signature(schema, data.data_content_fingerprint)
        store = CheckpointStore(config.checkpoints_root, base)
        stages: dict[str, Any] = {}
        feature_record = store.latest_completion("features", signature=feature_sig)
        stages["features"] = store.stage_status("features", feature_sig)
        feature_digest = feature_record.artifact_digest if feature_record else None
        plan = plan_shards(manifest.shards, config)
        plan_payload = plan.to_dict()
        if not feature_digest:
            stages["train"] = {
                "stage": "train",
                "status": "blocked",
                "reason": "features checkpoint is not valid for the current inputs",
            }
            stages["funnel"] = {"stage": "funnel", "status": "blocked", "reason": "train is blocked"}
            stages["infer"] = {"stage": "infer", "status": "blocked", "reason": "funnel is blocked"}
        else:
            train_sig = train_stage_signature(config, feature_digest, plan)
            train_record = store.latest_completion("train", signature=train_sig)
            stages["train"] = store.stage_status("train", train_sig)
            model_digest = train_record.artifact_digest if train_record else None
            if not model_digest:
                stages["funnel"] = {"stage": "funnel", "status": "blocked", "reason": "train is not complete"}
                stages["infer"] = {"stage": "infer", "status": "blocked", "reason": "funnel is blocked"}
            else:
                funnel_sig = funnel_stage_signature(model_digest, feature_digest, config.confidence_threshold)
                funnel_record = store.latest_completion("funnel", signature=funnel_sig)
                stages["funnel"] = store.stage_status("funnel", funnel_sig)
                funnel_digest = funnel_record.artifact_digest if funnel_record else None
                if not funnel_digest:
                    stages["infer"] = {"stage": "infer", "status": "blocked", "reason": "funnel is not complete"}
                else:
                    infer_sig = infer_stage_signature(model_digest, feature_digest, funnel_digest, config)
                    stages["infer"] = store.stage_status("infer", infer_sig)
        invalidations = [
            {
                "seq": record.seq,
                "earliest_stage": record.details.get("earliest_stage"),
                "invalidated_stages": record.details.get("invalidated_stages"),
                "reason": record.details.get("reason"),
            }
            for record in store.invalidations()
        ]
        return {
            "config_version": config.version,
            "config_source_digest": config.source_digest,
            "data_content_fingerprint": data.data_content_fingerprint,
            "feature_schema_fingerprint": schema,
            "schedule": plan_payload,
            "stages": stages,
            "invalidations": invalidations,
            "latest_checkpoint_seq": max((record.seq for record in store.records()), default=0),
        }
