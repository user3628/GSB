from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from . import PIPELINE_VERSION, SCHEMA_VERSION
from .attribution import compute_attribution
from .calibration import compute_layer_calibration, prepare_layer_inputs
from .checkpoints import (
    CheckpointStore,
    StagePublisher,
    clear_stop,
    load_cached_stage,
    request_stop,
    stage_cache_available,
    stop_requested,
)
from .config import (
    STAGE_LABELS,
    STAGES,
    compute_stage_fingerprints,
    diff_configs,
    earliest_invalidated_stage,
    ensure_state_layout,
    import_configuration,
    list_versions,
    load_config_by_id,
    load_current_config,
    state_root,
)
from .errors import (
    Interrupted,
    InvalidParam,
    InvalidStage,
    QPipeError,
    StaleCheckpoint,
    ToleranceExceeded,
    VerifyFailed,
)
from .forward import compute_layer_forward, forward_input_fingerprint
from .util import (
    FileLock,
    atomic_write_bytes,
    atomic_write_json,
    canonical_json_bytes,
    digest_bytes,
    digest_obj,
    exclusive_directory_publish,
    json_ready,
    load_json,
    temporary_directory,
)
from .validation import tolerance_error, validate_attribution


@dataclass
class RunOutcome:
    run_id: str
    status: str
    code: str | None
    exit_code: int
    output_dir: str
    final_digest: str
    result: dict[str, Any]
    invalidation: dict[str, Any]


def _stage_input_fingerprint(
    stage: str, stage_fingerprint: str, upstream_digest: str | None
) -> str:
    return digest_obj(
        {
            "stage": stage,
            "stage_fingerprint": stage_fingerprint,
            "upstream_artifact_digest": upstream_digest,
            "pipeline_version": PIPELINE_VERSION,
        }
    )


class Pipeline:
    def __init__(
        self,
        workspace: Path,
        *,
        state_dir: str | Path | None = None,
    ) -> None:
        self.workspace = workspace.resolve()
        self.root = state_root(self.workspace, state_dir)
        ensure_state_layout(self.root)

    def import_config(
        self,
        *,
        manifest_path: Path,
        shapes_path: Path,
        version: str = "v1",
    ) -> dict[str, Any]:
        return import_configuration(
            self.workspace,
            manifest_path=manifest_path,
            shapes_path=shapes_path,
            version=version,
            state_dir=self.root,
        )

    def _run_dir(self, run_id: str) -> Path:
        if not run_id or "/" in run_id or "\\" in run_id or run_id in (".", ".."):
            raise InvalidParam("run_id contains invalid path characters", parameter="run_id")
        return self.root / "runs" / run_id

    def _active_path(self) -> Path:
        return self.root / "state" / "active.json"

    def _load_active(self) -> dict[str, Any] | None:
        path = self._active_path()
        return load_json(path) if path.is_file() else None

    def _write_active(self, value: dict[str, Any]) -> None:
        atomic_write_json(self._active_path(), value)

    def _cache_availability(self, fingerprints: dict[str, str]) -> dict[str, bool]:
        return {
            stage: stage_cache_available(self.root, stage, fingerprints[stage])
            for stage in STAGES
        }

    def _compute_invalidation(
        self, config: dict[str, Any], fingerprints: dict[str, Any]
    ) -> dict[str, Any]:
        active = self._load_active()
        old_fingerprints = active.get("stage_fingerprints") if active else None
        old_config = (
            load_config_by_id(self.root, active.get("config_id")) if active else None
        )
        availability = self._cache_availability(fingerprints["stages"])
        earliest, cache_reasons = earliest_invalidated_stage(
            old_fingerprints,
            fingerprints["stages"],
            cache_available=availability,
        )
        reasons = diff_configs(old_config, config)
        if earliest is not None and cache_reasons and not reasons:
            reasons = cache_reasons
        if earliest is None:
            reasons = []
        return {
            "earliest_stage": earliest,
            "reasons": reasons,
            "old_config_id": active.get("config_id") if active else None,
            "old_run_id": active.get("run_id") if active else None,
            "new_config_id": config["config_id"],
            "old_fingerprints": old_fingerprints,
            "new_fingerprints": fingerprints["stages"],
            "cache_available_before_run": availability,
        }

    def _write_run_state(self, run_dir: Path, state: dict[str, Any]) -> None:
        atomic_write_json(run_dir / "run_state.json", state)

    def _read_run_state(self, run_id: str) -> dict[str, Any] | None:
        path = self._run_dir(run_id) / "run_state.json"
        return load_json(path) if path.is_file() else None

    def _publisher(
        self,
        *,
        run_dir: Path,
        run_id: str,
        config: dict[str, Any],
        fingerprints: dict[str, Any],
    ) -> StagePublisher:
        return StagePublisher(
            root=self.root,
            run_dir=run_dir,
            run_id=run_id,
            config_id=config["config_id"],
            config_version=config["config_version"],
            fingerprints=fingerprints["stages"],
            layer_ids=list(config["manifest"]["operator_chain"]),
        )

    def _published_stage_payload(
        self, publisher: StagePublisher, stage: str
    ) -> dict[str, Any]:
        ledger = publisher.load_ledger()
        entry = ledger["stages"][stage]
        if entry["status"] != "complete":
            raise InvalidParam(
                "stage is not complete", parameter="stage", details={"stage": stage}
            )
        return {
            "stage": stage,
            "input_fingerprint": _stage_input_fingerprint(
                stage,
                publisher.fingerprints[stage],
                None,
            ),
            "layers": {
                record["layer"]: publisher.load_published_layer(stage, index)
                for index, record in enumerate(entry["published_layers"], start=1)
            },
        }

    def _after_unit(
        self,
        *,
        root: Path,
        completed_units: int,
        stop_after_units: int | None,
        run_id: str,
        stage: str,
    ) -> None:
        if stop_after_units is not None and completed_units >= stop_after_units:
            request_stop(root, reason="stop_after_units")
        if stop_requested(root):
            raise Interrupted(
                run_id=run_id,
                completed_layer_units=completed_units,
                stage=stage,
                resume_required=True,
            )

    def _reuse_cached_stage(
        self,
        *,
        publisher: StagePublisher,
        stage: str,
        cached: dict[str, Any],
        stop_after_units: int | None,
        completed_units: int,
        run_id: str,
    ) -> tuple[dict[str, Any], int]:
        payload = cached["payload"]
        ledger = publisher.load_ledger()
        entry = ledger["stages"][stage]
        if entry["status"] == "complete":
            return payload, completed_units
        published_count = len(entry["published_layers"])
        for index, layer_id in enumerate(publisher.layer_ids, start=1):
            layer_payload = payload["layers"][layer_id]
            digest = digest_obj(layer_payload)
            if index <= published_count:
                record = entry["published_layers"][index - 1]
                if record.get("artifact_digest") != digest:
                    raise StaleCheckpoint(
                        "cached stage differs from already published layer",
                        layer=layer_id,
                        details={"stage": stage},
                    )
                continue
            publisher.publish_layer(
                stage=stage,
                layer_seq=index,
                layer_id=layer_id,
                input_fingerprint=layer_payload["input_fingerprint"],
                artifact_digest=digest,
                payload=layer_payload,
            )
            completed_units += 1
            self._after_unit(
                root=self.root,
                completed_units=completed_units,
                stop_after_units=stop_after_units,
                run_id=run_id,
                stage=stage,
            )
        publisher.complete_stage(
            stage=stage,
            payload=payload,
            input_fingerprint=payload["input_fingerprint"],
        )
        return payload, completed_units

    def _execute_calibration(
        self,
        *,
        config: dict[str, Any],
        fingerprints: dict[str, Any],
        publisher: StagePublisher,
        stop_after_units: int | None,
        completed_units: int,
        run_id: str,
    ) -> tuple[dict[str, Any], int]:
        stage_fingerprint = fingerprints["stages"]["calibration"]
        upstream_digest = None
        stage_input_fp = _stage_input_fingerprint(
            "calibration", stage_fingerprint, upstream_digest
        )
        prepared_layers = prepare_layer_inputs(config)
        ledger = publisher.load_ledger()
        published_count = len(
            ledger["stages"]["calibration"]["published_layers"]
        )
        layers: dict[str, Any] = {}
        for index, prepared in enumerate(prepared_layers, start=1):
            layer_id = prepared["layer_id"]
            if index <= published_count:
                layer_result = publisher.load_published_layer("calibration", index)
            else:
                layer_result = compute_layer_calibration(
                    prepared,
                    config=config,
                    stage_fingerprint=stage_fingerprint,
                )
                publisher.publish_layer(
                    stage="calibration",
                    layer_seq=index,
                    layer_id=layer_id,
                    input_fingerprint=layer_result["input_fingerprint"],
                    artifact_digest=digest_obj(layer_result),
                    payload=layer_result,
                )
                completed_units += 1
                self._after_unit(
                    root=self.root,
                    completed_units=completed_units,
                    stop_after_units=stop_after_units,
                    run_id=run_id,
                    stage="calibration",
                )
            layers[layer_id] = layer_result
        payload = {
            "stage": "calibration",
            "input_fingerprint": stage_input_fp,
            "layers": layers,
        }
        publisher.complete_stage(
            stage="calibration", payload=payload, input_fingerprint=stage_input_fp
        )
        return payload, completed_units

    def _execute_forward(
        self,
        *,
        config: dict[str, Any],
        fingerprints: dict[str, Any],
        calibration_payload: dict[str, Any],
        publisher: StagePublisher,
        stop_after_units: int | None,
        completed_units: int,
        run_id: str,
    ) -> tuple[dict[str, Any], int]:
        stage_fingerprint = fingerprints["stages"]["forward"]
        stage_input_fp = _stage_input_fingerprint(
            "forward", stage_fingerprint, digest_obj(calibration_payload)
        )
        prepared_layers = prepare_layer_inputs(config)
        calibrations = calibration_payload["layers"]
        ledger = publisher.load_ledger()
        published_count = len(ledger["stages"]["forward"]["published_layers"])
        layers: dict[str, Any] = {}
        activation = prepared_layers[0]["activation"]
        for index, prepared in enumerate(prepared_layers, start=1):
            layer = prepared["layer"]
            layer_id = prepared["layer_id"]
            if index <= published_count:
                layer_result = publisher.load_published_layer("forward", index)
            else:
                calibration = calibrations[layer_id]
                input_fp = forward_input_fingerprint(
                    stage_fingerprint=stage_fingerprint,
                    calibration=calibration,
                    activation=activation,
                    accumulator_bits=int(layer["accumulator_bits"]),
                )
                layer_result = compute_layer_forward(
                    layer=layer,
                    calibration=calibration,
                    activation=activation,
                    weight=prepared["weight"],
                    input_fingerprint=input_fp,
                )
                publisher.publish_layer(
                    stage="forward",
                    layer_seq=index,
                    layer_id=layer_id,
                    input_fingerprint=layer_result["input_fingerprint"],
                    artifact_digest=digest_obj(layer_result),
                    payload=layer_result,
                )
                completed_units += 1
                self._after_unit(
                    root=self.root,
                    completed_units=completed_units,
                    stop_after_units=stop_after_units,
                    run_id=run_id,
                    stage="forward",
                )
            layers[layer_id] = layer_result
            activation = np.asarray(layer_result["output"], dtype=np.float64)
        payload = {
            "stage": "forward",
            "input_fingerprint": stage_input_fp,
            "layers": layers,
        }
        publisher.complete_stage(
            stage="forward", payload=payload, input_fingerprint=stage_input_fp
        )
        return payload, completed_units

    def _execute_attribution(
        self,
        *,
        config: dict[str, Any],
        fingerprints: dict[str, Any],
        calibration_payload: dict[str, Any],
        forward_payload: dict[str, Any],
        publisher: StagePublisher,
        stop_after_units: int | None,
        completed_units: int,
        run_id: str,
    ) -> tuple[dict[str, Any], int]:
        stage_fingerprint = fingerprints["stages"]["attribution"]
        stage_input_fp = _stage_input_fingerprint(
            "attribution", stage_fingerprint, digest_obj(forward_payload)
        )
        calculated = compute_attribution(
            config=config,
            calibration_payload=calibration_payload,
            forward_payload=forward_payload,
            attribution_stage_fingerprint=stage_fingerprint,
        )
        calculated["input_fingerprint"] = stage_input_fp
        ledger = publisher.load_ledger()
        published_count = len(
            ledger["stages"]["attribution"]["published_layers"]
        )
        layers: dict[str, Any] = {}
        for index, layer_id in enumerate(publisher.layer_ids, start=1):
            layer_result = calculated["layers"][layer_id]
            digest = digest_obj(layer_result)
            if index <= published_count:
                stored = publisher.load_published_layer("attribution", index)
                if digest_obj(stored) != digest:
                    raise VerifyFailed(
                        "resumed attribution layer is not reproducible",
                        layer=layer_id,
                    )
                layer_result = stored
            else:
                publisher.publish_layer(
                    stage="attribution",
                    layer_seq=index,
                    layer_id=layer_id,
                    input_fingerprint=layer_result["input_fingerprint"],
                    artifact_digest=digest,
                    payload=layer_result,
                )
                completed_units += 1
                self._after_unit(
                    root=self.root,
                    completed_units=completed_units,
                    stop_after_units=stop_after_units,
                    run_id=run_id,
                    stage="attribution",
                )
            layers[layer_id] = layer_result
        payload = dict(calculated)
        payload["layers"] = layers
        publisher.complete_stage(
            stage="attribution", payload=payload, input_fingerprint=stage_input_fp
        )
        return payload, completed_units

    def _execute_validation(
        self,
        *,
        config: dict[str, Any],
        fingerprints: dict[str, Any],
        attribution_payload: dict[str, Any],
        publisher: StagePublisher,
        stop_after_units: int | None,
        completed_units: int,
        run_id: str,
    ) -> tuple[dict[str, Any], int]:
        stage_fingerprint = fingerprints["stages"]["validation"]
        stage_input_fp = _stage_input_fingerprint(
            "validation", stage_fingerprint, digest_obj(attribution_payload)
        )
        calculated = validate_attribution(
            config=config, attribution_payload=attribution_payload
        )
        calculated["input_fingerprint"] = stage_input_fp
        for layer_id, layer_result in calculated["layers"].items():
            layer_result["input_fingerprint"] = digest_obj(
                {
                    "stage_fingerprint": stage_fingerprint,
                    "layer": layer_id,
                    "attribution_layer": attribution_payload["layers"][layer_id][
                        "input_fingerprint"
                    ],
                }
            )
        ledger = publisher.load_ledger()
        published_count = len(
            ledger["stages"]["validation"]["published_layers"]
        )
        layers: dict[str, Any] = {}
        for index, layer_id in enumerate(publisher.layer_ids, start=1):
            layer_result = calculated["layers"][layer_id]
            digest = digest_obj(layer_result)
            if index <= published_count:
                stored = publisher.load_published_layer("validation", index)
                if digest_obj(stored) != digest:
                    raise VerifyFailed(
                        "resumed validation layer is not reproducible",
                        layer=layer_id,
                    )
                layer_result = stored
            else:
                publisher.publish_layer(
                    stage="validation",
                    layer_seq=index,
                    layer_id=layer_id,
                    input_fingerprint=layer_result["input_fingerprint"],
                    artifact_digest=digest,
                    payload=layer_result,
                )
                completed_units += 1
                self._after_unit(
                    root=self.root,
                    completed_units=completed_units,
                    stop_after_units=stop_after_units,
                    run_id=run_id,
                    stage="validation",
                )
            layers[layer_id] = layer_result
        payload = dict(calculated)
        payload["layers"] = layers
        publisher.complete_stage(
            stage="validation", payload=payload, input_fingerprint=stage_input_fp
        )
        return payload, completed_units

    def _build_outputs(
        self,
        *,
        config: dict[str, Any],
        fingerprints: dict[str, Any],
        validation_payload: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        status = validation_payload["status"]
        result = {
            "schema_version": SCHEMA_VERSION,
            "pipeline_version": PIPELINE_VERSION,
            "config_version": config["config_version"],
            "config_id": config["config_id"],
            "pipeline_fingerprint": fingerprints["pipeline"],
            "stage_fingerprints": fingerprints["stages"],
            "status": status,
            "code": validation_payload["code"],
            "layers": [
                validation_payload["layers"][layer_id]
                for layer_id in config["manifest"]["operator_chain"]
            ],
            "end_to_end": validation_payload["end_to_end"],
            "closest_to_tolerance": validation_payload["closest_to_tolerance"],
            "violations": validation_payload["violations"],
            "attribution_consistency": validation_payload[
                "attribution_consistency"
            ],
            "review_required_layers": validation_payload["review_required_layers"],
        }
        if status == "fail":
            error_report = tolerance_error(validation_payload)
            error_report["config_id"] = config["config_id"]
            error_report["pipeline_fingerprint"] = fingerprints["pipeline"]
        else:
            error_report = {
                "schema_version": SCHEMA_VERSION,
                "status": "pass",
                "code": None,
                "message": "all layer and end-to-end tolerances passed",
                "config_id": config["config_id"],
                "pipeline_fingerprint": fingerprints["pipeline"],
                "violations": [],
                "out_of_tolerance_samples": {},
                "closest_to_tolerance": validation_payload["closest_to_tolerance"],
            }
        layer_errors = {
            "schema_version": SCHEMA_VERSION,
            "config_id": config["config_id"],
            "pipeline_fingerprint": fingerprints["pipeline"],
            "layers": result["layers"],
            "review_required_layers": result["review_required_layers"],
        }
        return json_ready(result), json_ready(error_report), json_ready(layer_errors)

    def _publish_outputs(
        self,
        *,
        run_dir: Path,
        result: dict[str, Any],
        error_report: dict[str, Any],
        layer_errors: dict[str, Any],
    ) -> tuple[Path, str]:
        result_bytes = canonical_json_bytes(result, pretty=True)
        error_bytes = canonical_json_bytes(error_report, pretty=True)
        layer_bytes = canonical_json_bytes(layer_errors, pretty=True)
        final_digest = digest_bytes(result_bytes + error_bytes + layer_bytes)
        digest_bytes_value = (
            f"{digest_bytes(result_bytes)}  result.json\n"
            f"{digest_bytes(error_bytes)}  error_report.json\n"
            f"{digest_bytes(layer_bytes)}  layer_errors.json\n"
            f"{final_digest}  pipeline-output\n"
        ).encode("utf-8")
        final_dir = run_dir / "output"
        if final_dir.exists():
            checks = {
                "result.json": result_bytes,
                "error_report.json": error_bytes,
                "layer_errors.json": layer_bytes,
                "SHA256SUMS": digest_bytes_value,
            }
            for name, expected in checks.items():
                path = final_dir / name
                if not path.is_file() or path.read_bytes() != expected:
                    raise StaleCheckpoint(
                        "existing output is not byte-identical to regenerated output",
                        details={"file": name},
                    )
            return final_dir, final_digest
        with temporary_directory(run_dir, ".tmp-output-") as temp_dir:
            atomic_write_bytes(temp_dir / "result.json", result_bytes)
            atomic_write_bytes(temp_dir / "error_report.json", error_bytes)
            atomic_write_bytes(temp_dir / "layer_errors.json", layer_bytes)
            atomic_write_bytes(temp_dir / "SHA256SUMS", digest_bytes_value)
            if exclusive_directory_publish(temp_dir, final_dir):
                return final_dir, final_digest
        for name, expected in (
            ("result.json", result_bytes),
            ("error_report.json", error_bytes),
            ("layer_errors.json", layer_bytes),
            ("SHA256SUMS", digest_bytes_value),
        ):
            if (final_dir / name).read_bytes() != expected:
                raise StaleCheckpoint(
                    "concurrent output publication conflicts with existing output",
                    details={"file": name},
                )
        return final_dir, final_digest

    def run(
        self,
        *,
        run_id: str = "default",
        version: str | None = None,
        resume: bool = False,
        stop_after_units: int | None = None,
    ) -> RunOutcome:
        ensure_state_layout(self.root)
        config, _ = load_current_config(self.root, version=version)
        fingerprints = compute_stage_fingerprints(config)
        invalidation = self._compute_invalidation(config, fingerprints)
        run_dir = self._run_dir(run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        with FileLock(run_dir / ".run.lock"):
            if stop_requested(self.root):
                if not resume:
                    raise Interrupted(
                        run_id=run_id,
                        resume_required=True,
                        message="stop request is pending; use resume to clear it",
                    )
                clear_stop(self.root)
            if stop_after_units is not None and stop_after_units <= 0:
                request_stop(self.root, reason="stop_after_units")
                raise Interrupted(
                    run_id=run_id,
                    completed_layer_units=0,
                    resume_required=True,
                )
            publisher = self._publisher(
                run_dir=run_dir,
                run_id=run_id,
                config=config,
                fingerprints=fingerprints,
            )
            publisher.ensure_ledger()
            publisher.repair_checkpoints_from_ledger()
            state: dict[str, Any] = {
                "schema_version": SCHEMA_VERSION,
                "run_id": run_id,
                "config_version": config["config_version"],
                "config_id": config["config_id"],
                "pipeline_fingerprint": fingerprints["pipeline"],
                "stage_fingerprints": fingerprints["stages"],
                "invalidation": invalidation,
                "status": "running",
                "current_stage": None,
                "completed_stages": [],
                "stage_sources": {},
                "completed_layer_units": 0,
                "checkpoint_next_sequence": publisher.checkpoints.next_sequence(),
            }
            self._write_run_state(run_dir, state)
            stage_payloads: dict[str, dict[str, Any]] = {}
            completed_units = 0
            try:
                for stage in STAGES:
                    state["current_stage"] = stage
                    self._write_run_state(run_dir, state)
                    cached = load_cached_stage(
                        self.root, stage, fingerprints["stages"][stage]
                    )
                    if cached is not None:
                        payload, completed_units = self._reuse_cached_stage(
                            publisher=publisher,
                            stage=stage,
                            cached=cached,
                            stop_after_units=stop_after_units,
                            completed_units=completed_units,
                            run_id=run_id,
                        )
                        state["stage_sources"][stage] = "cache"
                    elif stage == "calibration":
                        payload, completed_units = self._execute_calibration(
                            config=config,
                            fingerprints=fingerprints,
                            publisher=publisher,
                            stop_after_units=stop_after_units,
                            completed_units=completed_units,
                            run_id=run_id,
                        )
                        state["stage_sources"][stage] = "recomputed"
                    elif stage == "forward":
                        payload, completed_units = self._execute_forward(
                            config=config,
                            fingerprints=fingerprints,
                            calibration_payload=stage_payloads["calibration"],
                            publisher=publisher,
                            stop_after_units=stop_after_units,
                            completed_units=completed_units,
                            run_id=run_id,
                        )
                        state["stage_sources"][stage] = "recomputed"
                    elif stage == "attribution":
                        payload, completed_units = self._execute_attribution(
                            config=config,
                            fingerprints=fingerprints,
                            calibration_payload=stage_payloads["calibration"],
                            forward_payload=stage_payloads["forward"],
                            publisher=publisher,
                            stop_after_units=stop_after_units,
                            completed_units=completed_units,
                            run_id=run_id,
                        )
                        state["stage_sources"][stage] = "recomputed"
                    else:
                        payload, completed_units = self._execute_validation(
                            config=config,
                            fingerprints=fingerprints,
                            attribution_payload=stage_payloads["attribution"],
                            publisher=publisher,
                            stop_after_units=stop_after_units,
                            completed_units=completed_units,
                            run_id=run_id,
                        )
                        state["stage_sources"][stage] = "recomputed"
                    stage_payloads[stage] = payload
                    state["completed_stages"].append(stage)
                    state["completed_layer_units"] = completed_units
                    state["checkpoint_next_sequence"] = (
                        publisher.checkpoints.next_sequence()
                    )
                    self._write_run_state(run_dir, state)

                validation_payload = stage_payloads["validation"]
                result, error_report, layer_errors = self._build_outputs(
                    config=config,
                    fingerprints=fingerprints,
                    validation_payload=validation_payload,
                )
                output_dir, final_digest = self._publish_outputs(
                    run_dir=run_dir,
                    result=result,
                    error_report=error_report,
                    layer_errors=layer_errors,
                )
                status = "pass" if validation_payload["status"] == "pass" else "tolerance_failed"
                state.update(
                    {
                        "status": status,
                        "code": result["code"],
                        "current_stage": None,
                        "output_dir": str(output_dir),
                        "final_digest": final_digest,
                        "completed_layer_units": completed_units,
                        "checkpoint_next_sequence": publisher.checkpoints.next_sequence(),
                    }
                )
                self._write_run_state(run_dir, state)
                self._write_active(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "config_id": config["config_id"],
                        "config_version": config["config_version"],
                        "run_id": run_id,
                        "status": status,
                        "stage_fingerprints": fingerprints["stages"],
                        "pipeline_fingerprint": fingerprints["pipeline"],
                        "invalidation": invalidation,
                        "output_dir": str(output_dir),
                        "final_digest": final_digest,
                    }
                )
                outcome = RunOutcome(
                    run_id=run_id,
                    status=status,
                    code=result["code"],
                    exit_code=4 if status == "tolerance_failed" else 0,
                    output_dir=str(output_dir),
                    final_digest=final_digest,
                    result=result,
                    invalidation=invalidation,
                )
                if status == "tolerance_failed":
                    raise ToleranceExceeded(
                        "one or more tolerance checks failed",
                        layer=error_report.get("layer"),
                        details={
                            "run_id": run_id,
                            "output_dir": str(output_dir),
                            "final_digest": final_digest,
                            "error_report": error_report,
                            "invalidation": invalidation,
                        },
                    )
                return outcome
            except Interrupted as exc:
                state.update(
                    {
                        "status": "interrupted",
                        "code": exc.code,
                        "completed_layer_units": exc.details.get(
                            "completed_layer_units", completed_units
                        ),
                        "checkpoint_next_sequence": publisher.checkpoints.next_sequence(),
                        "resume_required": True,
                    }
                )
                self._write_run_state(run_dir, state)
                raise
            except QPipeError as exc:
                if exc.code != "E_TOLERANCE_EXCEEDED":
                    state.update(
                        {
                            "status": "error",
                            "code": exc.code,
                            "error": exc.to_dict(),
                            "current_stage": state.get("current_stage"),
                            "completed_layer_units": completed_units,
                            "checkpoint_next_sequence": publisher.checkpoints.next_sequence(),
                        }
                    )
                    self._write_run_state(run_dir, state)
                    atomic_write_json(run_dir / "error.json", exc.to_dict())
                raise

    def status(
        self, *, version: str | None = None, run_id: str | None = None
    ) -> dict[str, Any]:
        config, registry = load_current_config(self.root, version=version)
        fingerprints = compute_stage_fingerprints(config)
        invalidation = self._compute_invalidation(config, fingerprints)
        active = self._load_active()
        active_run = (
            active.get("run_id")
            if active and active.get("config_id") == config["config_id"]
            else None
        )
        selected_run = run_id or active_run
        candidate_state = self._read_run_state(selected_run) if selected_run else None
        run_config_mismatch = bool(
            candidate_state
            and candidate_state.get("config_id") != config["config_id"]
        )
        run_state = None if run_config_mismatch else candidate_state
        run_dir = self._run_dir(selected_run) if selected_run else None
        ledger = None
        checkpoint_count = 0
        if run_dir is not None:
            ledger_path = run_dir / "publish_ledger.json"
            if ledger_path.is_file():
                candidate_ledger = load_json(ledger_path)
                if candidate_ledger.get("config_id") == config["config_id"]:
                    ledger = candidate_ledger
            checkpoint_dir = run_dir / "checkpoints"
            if checkpoint_dir.is_dir() and not run_config_mismatch:
                checkpoint_count = len(
                    list(
                        checkpoint_dir.glob(
                            "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9].json"
                        )
                    )
                )
        stages: dict[str, Any] = {}
        for stage in STAGES:
            fingerprint = fingerprints["stages"][stage]
            entry = ledger["stages"][stage] if ledger else None
            stages[stage] = {
                "fingerprint": fingerprint,
                "cache_available": stage_cache_available(
                    self.root, stage, fingerprint
                ),
                "ledger_status": entry["status"] if entry else "absent",
                "next_layer_seq": entry["next_layer_seq"] if entry else 1,
                "published_layers": (
                    [record["layer"] for record in entry["published_layers"]]
                    if entry
                    else []
                ),
                "cache_path": (
                    str(self.root / "cache" / STAGE_LABELS[stage] / fingerprint)
                    if entry is None
                    else entry.get("cache_path")
                ),
            }
        return {
            "schema_version": SCHEMA_VERSION,
            "config_version": config["config_version"],
            "config_id": config["config_id"],
            "pipeline_fingerprint": fingerprints["pipeline"],
            "stage_fingerprints": fingerprints["stages"],
            "invalidation": invalidation,
            "stages": stages,
            "run_id": selected_run,
            "run_state": run_state,
            "run_config_mismatch": run_config_mismatch,
            "run_state_config_id": (
                candidate_state.get("config_id") if candidate_state else None
            ),
            "checkpoint_count": checkpoint_count,
            "stop_requested": stop_requested(self.root),
            "versions": list_versions(self.root),
            "registry_current": registry.get("current"),
        }

    def layers(self, *, run_id: str | None = None) -> dict[str, Any]:
        selected = run_id
        if selected is None:
            active = self._load_active()
            selected = active.get("run_id") if active else None
        if selected is None:
            raise InvalidParam("no completed run is available", parameter="run_id")
        path = self._run_dir(selected) / "output" / "layer_errors.json"
        if not path.is_file():
            raise InvalidParam(
                "layer error report is not available",
                parameter="run_id",
                details={"run_id": selected},
            )
        return load_json(path)

    def publish_layer_probe(
        self,
        *,
        run_id: str,
        stage: str,
        layer_seq: int,
        layer_id: str,
        input_fingerprint: str,
        artifact_digest: str,
        version: str | None = None,
    ) -> dict[str, Any]:
        """Low-level publication entry point used by concurrency tests/tools."""

        config, _ = load_current_config(self.root, version=version)
        fingerprints = compute_stage_fingerprints(config)
        run_dir = self._run_dir(run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        publisher = self._publisher(
            run_dir=run_dir,
            run_id=run_id,
            config=config,
            fingerprints=fingerprints,
        )
        publisher.ensure_ledger()
        return publisher.publish_layer(
            stage=stage,
            layer_seq=layer_seq,
            layer_id=layer_id,
            input_fingerprint=input_fingerprint,
            artifact_digest=artifact_digest,
        )

    def run_stage(
        self,
        *,
        stage: str,
        run_id: str = "default",
        version: str | None = None,
        resume: bool = False,
        stop_after_units: int | None = None,
    ) -> dict[str, Any]:
        """Execute exactly one stage, requiring all prior stages in the run."""

        if stage not in STAGES:
            raise InvalidParam(
                "unknown pipeline stage",
                parameter="stage",
                details={"stage": stage, "allowed": list(STAGES)},
            )
        ensure_state_layout(self.root)
        config, _ = load_current_config(self.root, version=version)
        fingerprints = compute_stage_fingerprints(config)
        invalidation = self._compute_invalidation(config, fingerprints)
        run_dir = self._run_dir(run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        stage_index = STAGES.index(stage)
        with FileLock(run_dir / ".run.lock"):
            if stop_requested(self.root):
                if not resume:
                    raise Interrupted(
                        run_id=run_id,
                        resume_required=True,
                        message="stop request is pending; use --resume to clear it",
                    )
                clear_stop(self.root)
            if stop_after_units is not None and stop_after_units <= 0:
                request_stop(self.root, reason="stop_after_units")
                raise Interrupted(
                    run_id=run_id,
                    completed_layer_units=0,
                    stage=stage,
                    resume_required=True,
                )
            publisher = self._publisher(
                run_dir=run_dir,
                run_id=run_id,
                config=config,
                fingerprints=fingerprints,
            )
            publisher.ensure_ledger()
            publisher.repair_checkpoints_from_ledger()
            state: dict[str, Any] = {
                "schema_version": SCHEMA_VERSION,
                "run_id": run_id,
                "config_version": config["config_version"],
                "config_id": config["config_id"],
                "pipeline_fingerprint": fingerprints["pipeline"],
                "stage_fingerprints": fingerprints["stages"],
                "invalidation": invalidation,
                "status": "running",
                "current_stage": stage,
                "completed_stages": [],
                "stage_sources": {},
                "completed_layer_units": 0,
                "checkpoint_next_sequence": publisher.checkpoints.next_sequence(),
                "execution_mode": "run_stage",
            }
            self._write_run_state(run_dir, state)
            stage_payloads: dict[str, dict[str, Any]] = {}
            completed_units = 0
            try:
                ledger = publisher.load_ledger()
                for prior in STAGES[:stage_index]:
                    prior_entry = ledger["stages"][prior]
                    if prior_entry["status"] != "complete":
                        raise InvalidStage(
                            "preceding stage is missing from this run",
                            details={"stage": stage, "required_stage": prior},
                        )
                    prior_cached = load_cached_stage(
                        self.root, prior, fingerprints["stages"][prior]
                    )
                    if prior_cached is None:
                        raise InvalidStage(
                            "preceding stage artifact is missing",
                            details={"stage": stage, "required_stage": prior},
                        )
                    stage_payloads[prior] = prior_cached["payload"]
                    state["completed_stages"].append(prior)

                cached = load_cached_stage(
                    self.root, stage, fingerprints["stages"][stage]
                )
                if cached is not None:
                    payload, completed_units = self._reuse_cached_stage(
                        publisher=publisher,
                        stage=stage,
                        cached=cached,
                        stop_after_units=stop_after_units,
                        completed_units=completed_units,
                        run_id=run_id,
                    )
                    state["stage_sources"][stage] = "cache"
                elif stage == "calibration":
                    payload, completed_units = self._execute_calibration(
                        config=config,
                        fingerprints=fingerprints,
                        publisher=publisher,
                        stop_after_units=stop_after_units,
                        completed_units=completed_units,
                        run_id=run_id,
                    )
                    state["stage_sources"][stage] = "recomputed"
                elif stage == "forward":
                    payload, completed_units = self._execute_forward(
                        config=config,
                        fingerprints=fingerprints,
                        calibration_payload=stage_payloads["calibration"],
                        publisher=publisher,
                        stop_after_units=stop_after_units,
                        completed_units=completed_units,
                        run_id=run_id,
                    )
                    state["stage_sources"][stage] = "recomputed"
                elif stage == "attribution":
                    payload, completed_units = self._execute_attribution(
                        config=config,
                        fingerprints=fingerprints,
                        calibration_payload=stage_payloads["calibration"],
                        forward_payload=stage_payloads["forward"],
                        publisher=publisher,
                        stop_after_units=stop_after_units,
                        completed_units=completed_units,
                        run_id=run_id,
                    )
                    state["stage_sources"][stage] = "recomputed"
                else:
                    payload, completed_units = self._execute_validation(
                        config=config,
                        fingerprints=fingerprints,
                        attribution_payload=stage_payloads["attribution"],
                        publisher=publisher,
                        stop_after_units=stop_after_units,
                        completed_units=completed_units,
                        run_id=run_id,
                    )
                    state["stage_sources"][stage] = "recomputed"
                stage_payloads[stage] = payload
                state["completed_stages"].append(stage)
                state["completed_layer_units"] = completed_units
                state["checkpoint_next_sequence"] = publisher.checkpoints.next_sequence()

                if stage != "validation":
                    state.update(
                        {
                            "status": "stage_complete",
                            "current_stage": None,
                            "completed_stage": stage,
                            "stage_artifact_digest": digest_obj(payload),
                        }
                    )
                    self._write_run_state(run_dir, state)
                    return {
                        "ok": True,
                        "run_id": run_id,
                        "stage": stage,
                        "status": "stage_complete",
                        "stage_fingerprint": fingerprints["stages"][stage],
                        "artifact_digest": digest_obj(payload),
                        "completed_layer_units": completed_units,
                        "invalidation": invalidation,
                    }

                result, error_report, layer_errors = self._build_outputs(
                    config=config,
                    fingerprints=fingerprints,
                    validation_payload=payload,
                )
                output_dir, final_digest = self._publish_outputs(
                    run_dir=run_dir,
                    result=result,
                    error_report=error_report,
                    layer_errors=layer_errors,
                )
                status = "pass" if payload["status"] == "pass" else "tolerance_failed"
                state.update(
                    {
                        "status": status,
                        "code": result["code"],
                        "current_stage": None,
                        "completed_stage": stage,
                        "stage_artifact_digest": digest_obj(payload),
                        "output_dir": str(output_dir),
                        "final_digest": final_digest,
                        "completed_layer_units": completed_units,
                        "checkpoint_next_sequence": publisher.checkpoints.next_sequence(),
                    }
                )
                self._write_run_state(run_dir, state)
                self._write_active(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "config_id": config["config_id"],
                        "config_version": config["config_version"],
                        "run_id": run_id,
                        "status": status,
                        "stage_fingerprints": fingerprints["stages"],
                        "pipeline_fingerprint": fingerprints["pipeline"],
                        "invalidation": invalidation,
                        "output_dir": str(output_dir),
                        "final_digest": final_digest,
                    }
                )
                if status == "tolerance_failed":
                    raise ToleranceExceeded(
                        "one or more tolerance checks failed",
                        layer=error_report.get("layer"),
                        details={
                            "run_id": run_id,
                            "output_dir": str(output_dir),
                            "final_digest": final_digest,
                            "error_report": error_report,
                            "invalidation": invalidation,
                        },
                    )
                return {
                    "ok": True,
                    "run_id": run_id,
                    "stage": stage,
                    "status": "pass",
                    "code": None,
                    "stage_fingerprint": fingerprints["stages"][stage],
                    "artifact_digest": digest_obj(payload),
                    "completed_layer_units": completed_units,
                    "output_dir": str(output_dir),
                    "final_digest": final_digest,
                    "invalidation": invalidation,
                    "result": result,
                }
            except Interrupted as exc:
                state.update(
                    {
                        "status": "interrupted",
                        "code": exc.code,
                        "completed_layer_units": exc.details.get(
                            "completed_layer_units", completed_units
                        ),
                        "checkpoint_next_sequence": publisher.checkpoints.next_sequence(),
                        "resume_required": True,
                    }
                )
                self._write_run_state(run_dir, state)
                raise
            except QPipeError as exc:
                if exc.code != "E_TOLERANCE_EXCEEDED":
                    state.update(
                        {
                            "status": "error",
                            "code": exc.code,
                            "error": exc.to_dict(),
                            "completed_layer_units": completed_units,
                            "checkpoint_next_sequence": publisher.checkpoints.next_sequence(),
                        }
                    )
                    self._write_run_state(run_dir, state)
                    atomic_write_json(run_dir / "error.json", exc.to_dict())
                raise

    def errors(self, *, run_id: str | None = None) -> dict[str, Any]:
        selected = run_id
        if selected is None:
            active = self._load_active()
            selected = active.get("run_id") if active else None
        if selected is None:
            raise InvalidParam("no run is available", parameter="run_id")
        path = self._run_dir(selected) / "output" / "error_report.json"
        if path.is_file():
            return load_json(path)
        error_path = self._run_dir(selected) / "error.json"
        if error_path.is_file():
            return load_json(error_path)
        state = self._read_run_state(selected)
        if state and state.get("error"):
            return state["error"]
        raise InvalidParam(
            "no error report is available",
            parameter="run_id",
            details={"run_id": selected},
        )
