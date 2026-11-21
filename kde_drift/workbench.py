"""Stage orchestration, invalidation analysis, resume, and report export."""

from __future__ import annotations

import copy
import dataclasses
from pathlib import Path
from typing import Any, Callable, Iterable

import numpy as np

from .batch import Batch, BatchAccumulator, ingest_batches, split_into_batches
from .canonical import canonical_json_bytes, digest_object, file_sha256
from .checkpoints import STAGE_ORDER, CheckpointStore
from .config import config_fingerprint, normalize_config
from .data import Dataset, compare_column_schemas
from .drift import compute_drift
from .errors import WorkbenchError
from .kde import compute_bandwidths, compute_densities, prepare_column
from .metrics import compute_metrics
from .stop import StopController


STAGE_CONFIG_KEYS: dict[str, tuple[str, ...]] = {
    "bandwidth": ("kernel", "bandwidth", "imputation"),
    "density": ("kernel", "imputation", "grid", "chunk_size", "summation"),
    "metrics": ("imputation", "mmd", "kl"),
    "drift": ("drift",),
}


def _stage_config(config: dict[str, Any], stage: str) -> dict[str, Any]:
    return {key: copy.deepcopy(config[key]) for key in STAGE_CONFIG_KEYS[stage]}


def _flatten_config(value: Any, prefix: str = "") -> dict[str, Any]:
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key in sorted(value):
            path = f"{prefix}.{key}" if prefix else str(key)
            result.update(_flatten_config(value[key], path))
        return result
    return {prefix: value}


def _changed_config_paths(previous: dict[str, Any], current: dict[str, Any]) -> list[str]:
    previous_flat = _flatten_config(previous)
    current_flat = _flatten_config(current)
    paths = sorted(set(previous_flat) | set(current_flat))
    return [path for path in paths if previous_flat.get(path) != current_flat.get(path)]


def _artifact_valid(store: CheckpointStore, record: dict[str, Any]) -> tuple[bool, str | None]:
    artifact = record.get("artifact", {})
    relative = artifact.get("path")
    if not relative:
        return False, "artifact pointer is missing"
    path = store.workdir / relative
    if not path.exists():
        return False, "artifact file is missing"
    actual = file_sha256(path)
    if actual != artifact.get("sha256"):
        return False, "artifact checksum mismatch"
    return True, None


def _stage_input(
    *,
    dataset_fingerprint: str,
    config: dict[str, Any],
    stage: str,
    upstream: dict[str, str],
) -> str:
    return digest_object(
        {
            "dataset_fingerprint": dataset_fingerprint,
            "stage": stage,
            "stage_config": _stage_config(config, stage),
            "upstream": upstream,
        }
    )


def analyze_invalidation(
    store: CheckpointStore,
    dataset: Dataset,
    config: dict[str, Any],
) -> dict[str, Any]:
    config_version = config_fingerprint(config, dataset.dataset_fingerprint)
    stages: dict[str, Any] = {}
    reusable: list[str] = []
    affected: list[str] = []
    earliest: str | None = None
    valid_records: dict[str, dict[str, Any]] = {}
    expected_stage_inputs: dict[str, str | None] = {}
    upstream_results: dict[str, str] = {}

    for stage in STAGE_ORDER:
        record = store.read_record(stage)
        current_stage_config = _stage_config(config, stage)
        if earliest is not None:
            expected_input = None
            reasons = [
                {
                    "code": "UPSTREAM_INVALID",
                    "message": f"earliest invalid stage is {earliest}",
                }
            ]
            state = "invalid" if record is not None else "missing"
            stages[stage] = {
                "state": state,
                "reasons": reasons,
                "expected_stage_input_fingerprint": expected_input,
                "stage_config": current_stage_config,
                "checkpoint": store.public_record(record),
            }
            affected.append(stage)
            continue

        prerequisites = {
            name: upstream_results[name]
            for name in ("bandwidth", "density", "metrics")
            if name in upstream_results and name in _prerequisite_names(stage)
        }
        expected_input = _stage_input(
            dataset_fingerprint=dataset.dataset_fingerprint,
            config=config,
            stage=stage,
            upstream=prerequisites,
        )
        expected_stage_inputs[stage] = expected_input
        reasons: list[dict[str, Any]] = []
        if record is None:
            reasons.append({"code": "MISSING_STAGE", "message": "stage has no checkpoint"})
        else:
            if record.get("stage_input_fingerprint") != expected_input:
                changed_paths = _changed_config_paths(
                    record.get("stage_config", {}), current_stage_config
                )
                if record.get("dataset_fingerprint") != dataset.dataset_fingerprint:
                    reasons.append(
                        {
                            "code": "DATASET_CONTENT_CHANGED",
                            "message": "reference/current sample content changed",
                        }
                    )
                if changed_paths:
                    reasons.append(
                        {
                            "code": "STAGE_CONFIG_CHANGED",
                            "message": "stage configuration changed",
                            "changed_paths": changed_paths,
                        }
                    )
                recorded_upstream = {
                    key: value.get("result_fingerprint")
                    for key, value in record.get("upstream", {}).items()
                }
                if recorded_upstream != prerequisites:
                    reasons.append(
                        {
                            "code": "UPSTREAM_FINGERPRINT_CHANGED",
                            "message": "an upstream stage fingerprint changed",
                            "previous": recorded_upstream,
                            "current": prerequisites,
                        }
                    )
                if not reasons:
                    reasons.append(
                        {
                            "code": "STAGE_INPUT_FINGERPRINT_CHANGED",
                            "message": "composed stage input fingerprint changed",
                        }
                    )
            else:
                artifact_ok, artifact_reason = _artifact_valid(store, record)
                if not artifact_ok:
                    reasons.append(
                        {
                            "code": "ARTIFACT_INVALID",
                            "message": artifact_reason,
                        }
                    )
        if reasons:
            earliest = stage
            affected.append(stage)
            stages[stage] = {
                "state": "invalid" if record is not None else "missing",
                "reasons": reasons,
                "expected_stage_input_fingerprint": expected_input,
                "stage_config": current_stage_config,
                "checkpoint": store.public_record(record),
            }
        else:
            reusable.append(stage)
            valid_records[stage] = record  # type: ignore[assignment]
            upstream_results[stage] = record["result_fingerprint"]  # type: ignore[index]
            stages[stage] = {
                "state": "valid",
                "reasons": [],
                "expected_stage_input_fingerprint": expected_input,
                "stage_config": current_stage_config,
                "checkpoint": store.public_record(record),
            }

    pipeline_fingerprint = digest_object(
        {
            "config_version": config_version,
            "expected_stage_inputs": expected_stage_inputs,
        }
    )
    return {
        "dataset": dataset.name,
        "config_version": config_version,
        "pipeline_fingerprint": pipeline_fingerprint,
        "valid": earliest is None,
        "earliest_invalid_stage": earliest,
        "affected_stages": affected,
        "reusable_stages": reusable,
        "stages": stages,
        "latest_sequence": store.latest_sequence(),
    }


def _prerequisite_names(stage: str) -> tuple[str, ...]:
    if stage == "bandwidth":
        return ()
    if stage == "density":
        return ("bandwidth",)
    if stage == "metrics":
        return ("density",)
    return ("metrics",)


def _accumulator_summary(stage: str, payload: dict[str, Any]) -> dict[str, Any]:
    if stage == "bandwidth":
        return {
            "kind": "per_column_bandwidth_accumulator",
            "columns": len(payload["columns"]),
            "digest": digest_object(payload["columns"]),
        }
    if stage == "density":
        maximum_bound = max(
            max(
                column["reference_diagnostics"]["verified_upper_bound"],
                column["current_diagnostics"]["verified_upper_bound"],
            )
            for column in payload["columns"]
        )
        return {
            "kind": f"normalized_density_{payload['summation']}",
            "chunk_size": payload["chunk_size"],
            "columns": len(payload["columns"]),
            "maximum_verified_upper_bound": maximum_bound,
            "digest": payload["diagnostics"]["curve_digest"],
        }
    if stage == "metrics":
        return {
            "kind": "chunked_kernel_mmd_and_trapezoid_kl",
            "columns": len(payload["columns"]),
            "digest": digest_object(payload["columns"]),
        }
    return {
        "kind": "fixed_seed_pooled_bootstrap",
        "columns": len(payload["columns"]),
        "resamples": payload["columns"][0]["significance"]["resamples"] if payload["columns"] else 0,
        "drift_digest": payload["summary"]["drift_digest"],
    }


def _load_prepared_columns(
    dataset: Dataset, config: dict[str, Any]
) -> dict[str, dict[str, np.ndarray]]:
    prepared: dict[str, dict[str, np.ndarray]] = {}
    method = config["imputation"]["method"]
    for index, name in enumerate(dataset.column_names):
        reference, _ = prepare_column(
            dataset.reference_features[:, index], method=method, column_name=name
        )
        current, _ = prepare_column(
            dataset.current_features[:, index], method=method, column_name=name
        )
        prepared[name] = {"reference": reference, "current": current}
    return prepared


def _replace_current_features(dataset: Dataset, current_features: np.ndarray) -> Dataset:
    from .canonical import dataset_arrays_digest

    matrix = np.ascontiguousarray(current_features, dtype=np.float64)
    current_fp = dataset_arrays_digest(
        matrix, dataset.current_labels, dataset.column_names, canonical_columns=True
    )
    combined_fp = dataset_arrays_digest(
        np.concatenate([dataset.reference_features, matrix], axis=0),
        np.concatenate([dataset.reference_labels, dataset.current_labels], axis=0),
        dataset.column_names,
        canonical_columns=True,
    )
    return dataclasses.replace(
        dataset,
        current_features=matrix,
        current_fingerprint=current_fp,
        dataset_fingerprint=combined_fp,
    )


class Workbench:
    def __init__(
        self,
        dataset: Dataset,
        config: dict[str, Any] | None,
        workdir: str | Path,
    ) -> None:
        self.dataset = dataset
        self.config = normalize_config(config)
        self.workdir = Path(workdir)
        self.store = CheckpointStore(self.workdir)

    def analyze(self, dataset: Dataset | None = None) -> dict[str, Any]:
        return analyze_invalidation(self.store, dataset or self.dataset, self.config)

    def _publish(
        self,
        *,
        stage: str,
        payload: dict[str, Any],
        expected_previous_revision: int,
        dataset: Dataset,
        config_version: str,
        stage_input_fingerprint: str,
        upstream: dict[str, str],
    ) -> dict[str, Any]:
        return self.store.publish(
            stage=stage,
            payload=payload,
            expected_previous_revision=expected_previous_revision,
            config_version=config_version,
            stage_input_fingerprint=stage_input_fingerprint,
            dataset_fingerprint=dataset.dataset_fingerprint,
            stage_config=_stage_config(self.config, stage),
            upstream=upstream,
            accumulator=_accumulator_summary(stage, payload),
        )

    def run(
        self,
        *,
        resume: bool = True,
        force: bool = False,
        stop: StopController | None = None,
        progress: Callable[[dict[str, Any]], None] | None = None,
        current_batches: Iterable[np.ndarray | Batch] | None = None,
        batch_size: int | None = None,
        batch_order: str = "forward",
    ) -> dict[str, Any]:
        stop = stop or StopController()
        batch_summary: dict[str, Any] | None = None
        dataset = self.dataset
        if batch_order not in {"forward", "reverse"}:
            raise WorkbenchError(
                "E_INVALID_BATCH",
                "batch order must be forward or reverse",
                stage="batch_ingest",
                location="batch_order",
            )
        if batch_size is not None and batch_size < 0:
            raise WorkbenchError(
                "E_INVALID_BATCH",
                "batch size must be positive or zero for a single batch",
                stage="batch_ingest",
                location="batch_size",
            )
        if current_batches is not None:
            matrix, batch_summary = ingest_batches(
                current_batches,
                expected_columns=dataset.columns,
                column_names=dataset.column_names,
                expected_rows=dataset.rows_current,
            )
            dataset = _replace_current_features(dataset, matrix)
        elif batch_size is not None and batch_size > 0:
            batches = split_into_batches(
                dataset.current_features, batch_size=batch_size, order=batch_order
            )
            matrix, batch_summary = ingest_batches(
                batches,
                expected_columns=dataset.columns,
                column_names=dataset.column_names,
                expected_rows=dataset.rows_current,
            )
            dataset = _replace_current_features(dataset, matrix)

        analysis = self.analyze(dataset)
        if not resume and analysis["earliest_invalid_stage"] is None:
            force = True
        earliest = "bandwidth" if force else analysis["earliest_invalid_stage"]

        records = self.store.records()
        payloads: dict[str, dict[str, Any]] = {}
        for stage in STAGE_ORDER:
            record = records.get(stage)
            if record is not None and stage in analysis["reusable_stages"] and not force:
                try:
                    payloads[stage] = self.store.load_artifact(record)
                except WorkbenchError:
                    earliest = earliest or stage
                    payloads.pop(stage, None)

        if earliest is None:
            summary = self._summary(dataset, payloads, analysis["config_version"])
            return {
                "status": "completed",
                "dataset": dataset.name,
                "config_version": analysis["config_version"],
                "pipeline_fingerprint": analysis["pipeline_fingerprint"],
                "outer_sequence": self.store.latest_sequence(),
                "batch_evaluation": batch_summary,
                "invalidation": analysis,
                "summary": summary,
                "payloads": payloads,
                "records": {stage: self.store.public_record(self.store.read_record(stage)) for stage in STAGE_ORDER},
            }

        if force:
            payloads = {}
        start_index = STAGE_ORDER.index(earliest)
        config_version = analysis["config_version"]
        upstream: dict[str, str] = {}
        if start_index > 0:
            for prerequisite in STAGE_ORDER[:start_index]:
                record = self.store.read_record(prerequisite)
                if record is None or prerequisite not in payloads:
                    raise WorkbenchError(
                        "E_INVALID_STAGE",
                        "cannot resume because a reusable upstream artifact is unavailable",
                        stage=earliest,
                        location=f"checkpoint.prerequisite.{prerequisite}",
                    )
                upstream[prerequisite] = record["result_fingerprint"]

        expected_revisions = {
            stage: int((records.get(stage) or {}).get("stage_revision", 0))
            for stage in STAGE_ORDER
        }
        def input_fingerprint(stage: str) -> str:
            prerequisites = {
                name: upstream[name] for name in _prerequisite_names(stage) if name in upstream
            }
            return _stage_input(
                dataset_fingerprint=dataset.dataset_fingerprint,
                config=self.config,
                stage=stage,
                upstream=prerequisites,
            )

        stop.stage_boundary(stage=earliest, location="run.start")
        if progress:
            progress({"event": "stage_start", "stage": earliest, "config_version": config_version})

        if start_index <= 0:
            bandwidth_payload, _ = compute_bandwidths(dataset, self.config, stop=stop)
            record = self._publish(
                stage="bandwidth",
                payload=bandwidth_payload,
                expected_previous_revision=expected_revisions["bandwidth"],
                dataset=dataset,
                config_version=config_version,
                stage_input_fingerprint=input_fingerprint("bandwidth"),
                upstream={},
            )
            payloads["bandwidth"] = bandwidth_payload
            records["bandwidth"] = record
            upstream["bandwidth"] = record["result_fingerprint"]
            expected_revisions["bandwidth"] = record["stage_revision"]
            stop.stage_boundary(stage="density", location="after.bandwidth.checkpoint")
            if progress:
                progress({"event": "stage_complete", "stage": "bandwidth", "sequence": record["sequence"]})

        if start_index <= 1:
            bandwidth_payload = payloads["bandwidth"]
            density_payload, _ = compute_densities(
                dataset, bandwidth_payload, self.config, stop=stop
            )
            record = self._publish(
                stage="density",
                payload=density_payload,
                expected_previous_revision=expected_revisions["density"],
                dataset=dataset,
                config_version=config_version,
                stage_input_fingerprint=input_fingerprint("density"),
                upstream={"bandwidth": upstream["bandwidth"]},
            )
            payloads["density"] = density_payload
            records["density"] = record
            upstream["density"] = record["result_fingerprint"]
            expected_revisions["density"] = record["stage_revision"]
            stop.stage_boundary(stage="metrics", location="after.density.checkpoint")
            if progress:
                progress({"event": "stage_complete", "stage": "density", "sequence": record["sequence"]})

        if start_index <= 2:
            metrics_payload = compute_metrics(
                dataset,
                payloads["bandwidth"],
                payloads["density"],
                self.config,
                stop=stop,
            )
            record = self._publish(
                stage="metrics",
                payload=metrics_payload,
                expected_previous_revision=expected_revisions["metrics"],
                dataset=dataset,
                config_version=config_version,
                stage_input_fingerprint=input_fingerprint("metrics"),
                upstream={"density": upstream["density"]},
            )
            payloads["metrics"] = metrics_payload
            records["metrics"] = record
            upstream["metrics"] = record["result_fingerprint"]
            expected_revisions["metrics"] = record["stage_revision"]
            stop.stage_boundary(stage="drift", location="after.metrics.checkpoint")
            if progress:
                progress({"event": "stage_complete", "stage": "metrics", "sequence": record["sequence"]})

        if start_index <= 3:
            drift_payload = compute_drift(
                dataset,
                payloads["metrics"],
                self.config,
                config_version=config_version,
                stop=stop,
            )
            record = self._publish(
                stage="drift",
                payload=drift_payload,
                expected_previous_revision=expected_revisions["drift"],
                dataset=dataset,
                config_version=config_version,
                stage_input_fingerprint=input_fingerprint("drift"),
                upstream={"metrics": upstream["metrics"]},
            )
            payloads["drift"] = drift_payload
            records["drift"] = record
            upstream["drift"] = record["result_fingerprint"]
            if progress:
                progress({"event": "stage_complete", "stage": "drift", "sequence": record["sequence"]})

        summary = self._summary(dataset, payloads, config_version)
        return {
            "status": "completed",
            "dataset": dataset.name,
            "config_version": config_version,
            "pipeline_fingerprint": self.analyze(dataset)["pipeline_fingerprint"],
            "outer_sequence": self.store.latest_sequence(),
            "batch_evaluation": batch_summary,
            "invalidation": analysis,
            "summary": summary,
            "payloads": payloads,
            "records": {stage: self.store.public_record(self.store.read_record(stage)) for stage in STAGE_ORDER},
        }

    def _summary(
        self,
        dataset: Dataset,
        payloads: dict[str, dict[str, Any]],
        config_version: str,
    ) -> dict[str, Any]:
        drift = payloads["drift"]
        metrics = payloads["metrics"]
        density = payloads["density"]
        drift_names = list(drift["summary"]["drift_columns"])
        mmd_sum = float(sum(column["mmd"]["score"] for column in metrics["columns"]))
        kl_sum = float(sum(column["kl"]["value"] for column in metrics["columns"]))
        summary = {
            "dataset": dataset.name,
            "rows_reference": dataset.rows_reference,
            "rows_current": dataset.rows_current,
            "columns": dataset.columns,
            "config_version": config_version,
            "drift_count": len(drift_names),
            "drift_columns": drift_names,
            "mmd_sum": mmd_sum,
            "kl_sum": kl_sum,
            "density_curve_digest": density["diagnostics"]["curve_digest"],
            "metrics_digest": digest_object(metrics),
            "drift_digest": drift["summary"]["drift_digest"],
        }
        summary["output_digest"] = digest_object(summary)
        return summary

    def status(self) -> dict[str, Any]:
        return self.analyze()

    def require_complete_payloads(self) -> dict[str, dict[str, Any]]:
        analysis = self.analyze()
        if analysis["earliest_invalid_stage"] is not None:
            raise WorkbenchError(
                "E_INVALID_STAGE",
                "a complete valid stage chain is not available",
                stage=analysis["earliest_invalid_stage"],
                location="workbench.export",
                details={"affected_stages": analysis["affected_stages"]},
            )
        payloads: dict[str, dict[str, Any]] = {}
        for stage in STAGE_ORDER:
            record = self.store.read_record(stage)
            payloads[stage] = self.store.load_artifact(record)  # type: ignore[arg-type]
        return payloads

    def export_density(self) -> dict[str, Any]:
        payloads = self.require_complete_payloads()
        density = payloads["density"]
        bandwidth = payloads["bandwidth"]
        bandwidth_by_name = {column["name"]: column for column in bandwidth["columns"]}
        columns = []
        for column in density["columns"]:
            bandwidth_column = bandwidth_by_name[column["name"]]
            columns.append(
                {
                    "name": column["name"],
                    "grid": column["grid"],
                    "reference_density": column["reference_density"],
                    "current_density": column["current_density"],
                    "kernel_parameters": column["kernel_parameters"],
                    "sample_sizes": {
                        "reference": bandwidth_column["reference"]["sample_size"],
                        "current": bandwidth_column["current"]["sample_size"],
                    },
                    "imputation": column["imputation"],
                    "error_bounds": {
                        "reference": column["reference_diagnostics"],
                        "current": column["current_diagnostics"],
                    },
                }
            )
        return {
            "schema_version": 1,
            "kind": "density_curves",
            "dataset": self.dataset.name,
            "config_version": self.analyze()["config_version"],
            "kernel": density["kernel"],
            "grid_points": density["grid_points"],
            "summation": density["summation"],
            "chunk_size": density["chunk_size"],
            "columns": columns,
        }

    def export_report(self) -> dict[str, Any]:
        payloads = self.require_complete_payloads()
        analysis = self.analyze()
        density_diagnostics = [
            {
                "name": column["name"],
                "kernel_parameters": column["kernel_parameters"],
                "reference": column["reference_diagnostics"],
                "current": column["current_diagnostics"],
                "imputation": column["imputation"],
            }
            for column in payloads["density"]["columns"]
        ]
        return {
            "schema_version": 1,
            "kind": "drift_report",
            "dataset": self.dataset.name,
            "config": self.config,
            "config_version": analysis["config_version"],
            "pipeline_fingerprint": analysis["pipeline_fingerprint"],
            "summary": self._summary(self.dataset, payloads, analysis["config_version"]),
            "bandwidth": payloads["bandwidth"],
            "density_diagnostics": density_diagnostics,
            "metrics": payloads["metrics"],
            "drift": payloads["drift"],
            "checkpoints": {
                stage: self.store.public_record(self.store.read_record(stage))
                for stage in STAGE_ORDER
            },
        }

    def export_bytes(self, kind: str) -> bytes:
        if kind == "density":
            value = self.export_density()
        elif kind == "report":
            value = self.export_report()
        else:
            raise WorkbenchError(
                "E_NOT_FOUND",
                "export kind must be density or report",
                location="workbench.export.kind",
            )
        return canonical_json_bytes(value, strict=True)


def evaluate_batches(
    dataset: Dataset,
    batches: Iterable[np.ndarray | Batch],
    config: dict[str, Any],
    workdir: str | Path,
    *,
    stop: StopController | None = None,
) -> dict[str, Any]:
    return Workbench(dataset, config, workdir).run(
        current_batches=batches,
        stop=stop,
    )
