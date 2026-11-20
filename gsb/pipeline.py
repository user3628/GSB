"""Four-stage deterministic training, calibration, diagnostics and batching."""

from __future__ import annotations

import base64
import shutil
import tempfile
from pathlib import Path
from typing import Any

import numpy as np

from .batch import PredictionAccumulator, comparable_core, comparable_digest, make_batches
from .binning import BinSpec, binned_from_payload, build_binning_payload
from .calibrators import Calibrator, apply_calibrator, fit_calibrator
from .config import ALGORITHM_VERSION, Config
from .data import Dataset, load_dataset
from .errors import StopRequested, WorkbenchError
from .precision import (
    dequantize,
    high_precision_error_report,
    int64_digest,
    probability_terms,
    quantize_probabilities,
    quantize_raw_scores,
    reliability_table,
    scale_for_decimals,
)
from .tree import GBDTModel, base_score_for_labels, predict_model_raw, train_one_round
from .util import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_json_bytes,
    ensure_within,
    hex_to_floats,
    sha256_bytes,
    sha256_json,
    stage_index,
)
from .workspace import STAGES, Workspace


def _encode_u16(values: np.ndarray) -> str:
    return base64.b64encode(np.asarray(values, dtype="<u2").tobytes(order="C")).decode("ascii")


def _decode_u16(encoded: str, expected_length: int) -> np.ndarray:
    data = base64.b64decode(encoded.encode("ascii"), validate=True)
    if len(data) != expected_length * 2:
        raise WorkbenchError("E_INVALID_STAGE", "fold assignment payload has invalid length", {"stage": "calibration"})
    return np.frombuffer(data, dtype="<u2").astype(np.uint16, copy=True)


def float_vector_digest(values: np.ndarray) -> str:
    return sha256_bytes(np.asarray(values, dtype="<f8").tobytes(order="C"))


def make_stratified_folds(y: np.ndarray, folds: int, seed: int) -> np.ndarray:
    y = np.asarray(y, dtype=np.uint8)
    if not isinstance(folds, int) or folds < 2:
        raise WorkbenchError("E_INVALID_PARAM", "calibration_folds must be at least 2", {"folds": folds})
    assignment = np.full(y.shape[0], -1, dtype=np.int64)
    for label in (0, 1):
        indices = np.flatnonzero(y == label)
        if indices.size < folds:
            raise WorkbenchError(
                "E_INVALID_PARAM",
                "calibration_folds exceeds the number of samples in a class",
                {"class": label, "class_count": int(indices.size), "folds": folds},
            )
        ranked = sorted(
            (sha256_bytes(f"{seed}:{label}:{int(index)}".encode("ascii")), int(index)) for index in indices
        )
        for rank, (_, index) in enumerate(ranked):
            assignment[index] = rank % folds
    if np.any(assignment < 0):
        raise WorkbenchError("E_CALIBRATION_INFEASIBLE", "could not assign every sample to a calibration fold")
    return assignment


class Pipeline:
    def __init__(
        self,
        workspace: Workspace,
        dataset: Dataset,
        *,
        stop_after_rounds: int | None = None,
    ):
        self.workspace = workspace
        self.dataset = dataset
        self.config = workspace.config
        self.stop_after_rounds = stop_after_rounds
        self._completed_rounds = 0

    def _validate_binding(self) -> None:
        registry = self.workspace._read_registry()
        stored = registry.get("dataset")
        if stored is None:
            raise WorkbenchError("E_INVALID_STAGE", "no dataset has been imported", {"stage": "binning"})
        if stored.get("data_fingerprint") != self.dataset.data_fingerprint:
            raise WorkbenchError(
                "E_INVALID_PARAM",
                "loaded dataset does not match the imported workspace dataset",
                {
                    "workspace_fingerprint": stored.get("data_fingerprint"),
                    "dataset_fingerprint": self.dataset.data_fingerprint,
                },
            )

    def _check_stop(self, stage: str, round_number: int | None = None) -> None:
        if self.workspace.stop_requested():
            raise StopRequested(stage, round_number)

    def _after_round(self, stage: str, round_number: int) -> None:
        self._completed_rounds += 1
        if self.stop_after_rounds is not None and self._completed_rounds >= self.stop_after_rounds:
            self.workspace.request_stop("deterministic_demo_stop_after_rounds")
            raise StopRequested(stage, round_number)
        self._check_stop(stage, round_number)

    def run_all(self, *, resume: bool = False, until_stage: str = "diagnostics") -> dict[str, Any]:
        stage_index(until_stage)
        limit = STAGES.index(until_stage) + 1
        with self.workspace.build_lock():
            if resume:
                self.workspace.clear_stop()
            self._validate_binding()
            self.config = self.workspace.config
            self.config.require_training_ready()
            self.workspace.set_run_status("running", {"until_stage": until_stage})
            try:
                self._check_stop("binning")
                for stage in STAGES[:limit]:
                    self._check_stop(stage)
                    if stage == "binning":
                        self._ensure_binning()
                    elif stage == "boosting":
                        self._ensure_boosting()
                    elif stage == "calibration":
                        self._ensure_calibration()
                    elif stage == "diagnostics":
                        self._ensure_diagnostics()
                    self._check_stop(stage)
                status = self.workspace.status()
                digest = self.run_digest() if all(status["stages"][stage]["status"] == "valid" for stage in STAGES) else None
                self.workspace.set_run_status(
                    "complete" if digest is not None else "stage_complete",
                    {"until_stage": until_stage, "run_digest": digest},
                )
                status = self.workspace.status()
                return {"ok": True, "status": status, "run_digest": digest}
            except StopRequested as exc:
                self.workspace.set_run_status(
                    "stopped",
                    {"stage": exc.stage, "completed_rounds_this_run": self._completed_rounds},
                )
                return {
                    "ok": True,
                    "status": self.workspace.status(),
                    "stopped": True,
                    "stopped_stage": exc.stage,
                    "completed_rounds_this_run": self._completed_rounds,
                }
            except WorkbenchError as exc:
                self.workspace.set_run_status("error", {"code": exc.code, "message": exc.message, "details": exc.details})
                raise

    def _expected_binning_input(self) -> str:
        return sha256_json(
            {
                "schema": 1,
                "stage": "binning",
                "algorithm": ALGORITHM_VERSION,
                "data_fingerprint": self.dataset.data_fingerprint,
                "bins": self.config.bins,
            }
        )

    def _invalidate_current_mismatch(self, stage: str, expected_input: str, expected_upstream: str | None) -> None:
        record = self.workspace.current_record(stage)
        if record is not None and not self.workspace.record_matches(
            stage, input_fingerprint=expected_input, upstream_fingerprint=expected_upstream
        ):
            self.workspace.invalidate_from(
                stage,
                {
                    "stage": stage,
                    "code": "stage_fingerprint_changed",
                    "previous_input_fingerprint": record.get("input_fingerprint"),
                    "current_input_fingerprint": expected_input,
                },
            )

    def _ensure_binning(self) -> tuple[dict[str, Any], np.ndarray, BinSpec]:
        expected_input = self._expected_binning_input()
        if self.workspace.record_matches("binning", input_fingerprint=expected_input, upstream_fingerprint=None):
            _, payload = self.workspace.read_stage_payload("binning")
            spec, binned = binned_from_payload(payload)
            self.workspace.clear_progress("binning")
            return payload, binned, spec
        self._invalidate_current_mismatch("binning", expected_input, None)
        self._check_stop("binning")
        payload, _, binned = build_binning_payload(self.dataset.X, self.config.bins)
        payload = {
            **payload,
            "data_fingerprint": self.dataset.data_fingerprint,
            "dataset_key": self.dataset.key,
            "sample_count": self.dataset.rows,
        }
        record = self.workspace.publish_checkpoint(
            stage="binning",
            input_fingerprint=expected_input,
            upstream_fingerprint=None,
            config_version=self.config.stage_config_version("binning"),
            payload=payload,
            accumulator={
                "batches": 1,
                "samples": self.dataset.rows,
                "matrix_sha256": payload["binned_matrix_sha256"],
                "state_digest": payload["content_digest"],
            },
        )
        payload["_checkpoint"] = {
            "seq": record["seq"],
            "version_fingerprint": record["version_fingerprint"],
        }
        self.workspace.clear_progress("binning")
        return payload, binned, BinSpec.from_payload(payload["spec"])

    def _expected_boosting_input(self, binning_record: dict[str, Any]) -> str:
        return sha256_json(
            {
                "schema": 1,
                "stage": "boosting",
                "algorithm": ALGORITHM_VERSION,
                "upstream_fingerprint": binning_record["version_fingerprint"],
                "config": {
                    "rounds": self.config.rounds,
                    "learning_rate": self.config.learning_rate,
                    "max_depth": self.config.max_depth,
                    "min_leaf": self.config.min_leaf,
                    "l2": self.config.l2,
                },
            }
        )

    def _train_boosted_with_progress(
        self,
        binned: np.ndarray,
        y: np.ndarray,
        *,
        stage: str,
        input_fingerprint: str,
        progress: dict[str, Any],
        save_progress,
    ) -> tuple[GBDTModel, np.ndarray]:
        rounds_total = self.config.rounds
        rounds_done = int(progress.get("rounds_done", 0))
        if progress.get("input_fingerprint") != input_fingerprint or progress.get("sample_count") != int(y.size):
            progress.clear()
            progress.update(
                {
                    "schema": 1,
                    "stage": stage,
                    "input_fingerprint": input_fingerprint,
                    "sample_count": int(y.size),
                    "rounds_total": rounds_total,
                    "rounds_done": 0,
                    "base_score_hex": float(base_score_for_labels(y)).hex(),
                    "trees": [],
                    "raw_scores_hex": [],
                }
            )
            rounds_done = 0
        if (
            "trees" in progress
            and "raw_scores_hex" in progress
            and len(progress.get("raw_scores_hex", [])) == int(y.size)
        ):
            model = GBDTModel(
                base_score=float.fromhex(progress["base_score_hex"]),
                trees=list(progress.get("trees", [])),
            )
            raw_scores = np.asarray(hex_to_floats(progress.get("raw_scores_hex", [])), dtype=np.float64)
            rounds_done = int(progress.get("rounds_done", len(model.trees)))
        else:
            model = GBDTModel(base_score=base_score_for_labels(y), trees=[])
            raw_scores = np.full(y.size, model.base_score, dtype=np.float64)
            rounds_done = 0
        while rounds_done < rounds_total:
            train_one_round(model, raw_scores, binned, y, self.config)
            rounds_done += 1
            progress.update(
                {
                    "rounds_done": rounds_done,
                    "base_score_hex": float(model.base_score).hex(),
                    "trees": model.trees,
                    "raw_scores_hex": [float(value).hex() for value in raw_scores.tolist()],
                }
            )
            save_progress(progress)
            self._after_round(stage, rounds_done)
        return model, raw_scores

    def _ensure_boosting(self) -> tuple[dict[str, Any], GBDTModel]:
        binning_record, binning_payload = self.workspace.read_stage_payload("binning")
        expected_input = self._expected_boosting_input(binning_record)
        expected_upstream = binning_record["version_fingerprint"]
        if self.workspace.record_matches("boosting", input_fingerprint=expected_input, upstream_fingerprint=expected_upstream):
            _, payload = self.workspace.read_stage_payload("boosting")
            model = GBDTModel.from_payload(payload["model"])
            self.workspace.clear_progress("boosting")
            return payload, model
        self._invalidate_current_mismatch("boosting", expected_input, expected_upstream)
        self._check_stop("boosting")
        _, binned = binned_from_payload(binning_payload)
        progress = self.workspace.read_progress("boosting") or {}

        def save_progress(value: dict[str, Any]) -> None:
            self.workspace.write_progress("boosting", value)

        model, raw_scores = self._train_boosted_with_progress(
            binned,
            self.dataset.y,
            stage="boosting",
            input_fingerprint=expected_input,
            progress=progress,
            save_progress=save_progress,
        )
        raw_digest = float_vector_digest(raw_scores)
        payload = {
            "schema": 1,
            "algorithm": "hist-gradient-boosting-v1",
            "sample_count": self.dataset.rows,
            "rounds": self.config.rounds,
            "config": {
                "learning_rate": self.config.learning_rate,
                "max_depth": self.config.max_depth,
                "min_leaf": self.config.min_leaf,
                "l2": self.config.l2,
            },
            "model": model.to_payload(),
            "raw_scores_hex": [float(value).hex() for value in raw_scores.tolist()],
            "raw_score_digest": raw_digest,
        }
        record = self.workspace.publish_checkpoint(
            stage="boosting",
            input_fingerprint=expected_input,
            upstream_fingerprint=expected_upstream,
            config_version=self.config.stage_config_version("boosting"),
            payload=payload,
            accumulator={
                "batches": 1,
                "samples": self.dataset.rows,
                "raw_score_digest": raw_digest,
            },
        )
        payload["_checkpoint"] = {"seq": record["seq"], "version_fingerprint": record["version_fingerprint"]}
        self.workspace.clear_progress("boosting")
        return payload, model

    def _expected_calibration_input(self, boosting_record: dict[str, Any], boosting_payload: dict[str, Any]) -> str:
        return sha256_json(
            {
                "schema": 1,
                "stage": "calibration",
                "algorithm": ALGORITHM_VERSION,
                "upstream_fingerprint": boosting_record["version_fingerprint"],
                "boosting_raw_score_digest": boosting_payload["raw_score_digest"],
                "sample_count": self.dataset.rows,
                "config": {
                    "calibrator": self.config.calibrator,
                    "calibration_folds": self.config.calibration_folds,
                    "calibration_seed": self.config.calibration_seed,
                    "platt_max_iter": self.config.platt_max_iter,
                    "platt_tol": self.config.platt_tol,
                    "quantize_decimals": self.config.quantize_decimals,
                },
            }
        )

    def _ensure_calibration(self) -> tuple[dict[str, Any], dict[str, Any]]:
        boosting_record, boosting_payload = self.workspace.read_stage_payload("boosting")
        _, binning_payload = self.workspace.read_stage_payload("binning")
        expected_input = self._expected_calibration_input(boosting_record, boosting_payload)
        expected_upstream = boosting_record["version_fingerprint"]
        if self.workspace.record_matches("calibration", input_fingerprint=expected_input, upstream_fingerprint=expected_upstream):
            _, payload = self.workspace.read_stage_payload("calibration")
            self.workspace.clear_progress("calibration")
            return payload, payload["calibrator"]
        self._invalidate_current_mismatch("calibration", expected_input, expected_upstream)
        self._check_stop("calibration")
        classes = np.unique(self.dataset.y.astype(np.uint8))
        if classes.size < 2:
            raise WorkbenchError(
                "E_CALIBRATION_INFEASIBLE",
                "calibration requires both classes",
                {"sample_count": self.dataset.rows, "classes": [int(value) for value in classes]},
            )
        folds = make_stratified_folds(
            self.dataset.y,
            self.config.calibration_folds,
            self.config.calibration_seed,
        )
        _, binned = binned_from_payload(binning_payload)
        progress = self.workspace.read_progress("calibration") or {}
        if progress.get("input_fingerprint") != expected_input:
            progress = {
                "schema": 1,
                "stage": "calibration",
                "input_fingerprint": expected_input,
                "fold_count": self.config.calibration_folds,
                "fold_index": 0,
                "rounds_total": self.config.rounds,
                "completed_oof_q": {},
                "model": None,
            }
            self.workspace.write_progress("calibration", progress)
        completed = {int(key): list(value) for key, value in progress.get("completed_oof_q", {}).items()}
        fold_index = int(progress.get("fold_index", 0))
        decimals = self.config.quantize_decimals
        while fold_index < self.config.calibration_folds:
            self._check_stop("calibration")
            train_mask = folds != fold_index
            validation_mask = ~train_mask
            train_indices = np.flatnonzero(train_mask)
            validation_indices = np.flatnonzero(validation_mask)
            y_train = self.dataset.y[train_indices]
            model_state = progress.get("model")
            if model_state:
                model = GBDTModel(base_score=float.fromhex(model_state["base_score_hex"]), trees=list(model_state["trees"]))
                raw_scores = np.asarray(hex_to_floats(model_state["raw_scores_hex"]), dtype=np.float64)
                rounds_done = int(model_state.get("rounds_done", len(model.trees)))
                if raw_scores.shape != (train_indices.size,) or len(model.trees) != rounds_done:
                    progress["model"] = None
                    continue
            else:
                model = GBDTModel(base_score=base_score_for_labels(y_train), trees=[])
                raw_scores = np.full(train_indices.size, model.base_score, dtype=np.float64)
                rounds_done = 0
            train_binned = binned[train_indices]
            while rounds_done < self.config.rounds:
                train_one_round(model, raw_scores, train_binned, y_train, self.config)
                rounds_done += 1
                progress["model"] = {
                    "rounds_done": rounds_done,
                    "base_score_hex": float(model.base_score).hex(),
                    "trees": model.trees,
                    "raw_scores_hex": [float(value).hex() for value in raw_scores.tolist()],
                }
                self.workspace.write_progress("calibration", progress)
                self._after_round("calibration", rounds_done)
            validation_raw = predict_model_raw(model, binned[validation_indices])
            validation_q = quantize_raw_scores(validation_raw, decimals, stage="calibration")
            completed[fold_index] = [int(value) for value in validation_q.tolist()]
            progress["completed_oof_q"] = {str(key): value for key, value in sorted(completed.items())}
            progress["fold_index"] = fold_index + 1
            progress["model"] = None
            self.workspace.write_progress("calibration", progress)
            fold_index += 1
            self._check_stop("calibration")

        oof_q = np.empty(self.dataset.rows, dtype=np.int64)
        filled = np.zeros(self.dataset.rows, dtype=bool)
        for fold in range(self.config.calibration_folds):
            indices = np.flatnonzero(folds == fold)
            values = np.asarray(completed.get(fold, []), dtype=np.int64)
            if values.size != indices.size:
                raise WorkbenchError(
                    "E_INVALID_STAGE",
                    "calibration fold progress is incomplete",
                    {"fold": fold, "expected": int(indices.size), "actual": int(values.size)},
                )
            oof_q[indices] = values
            filled[indices] = True
        if not np.all(filled):
            raise WorkbenchError("E_INVALID_STAGE", "calibration out-of-fold scores are incomplete", {"stage": "calibration"})
        oof_scores = dequantize(oof_q, decimals)
        calibrator = fit_calibrator(self.config, oof_scores, self.dataset.y)
        probability = apply_calibrator(calibrator, oof_scores, stage="calibration")
        probability_q = quantize_probabilities(probability, decimals, stage="calibration")
        fold_assignment_digest = sha256_bytes(np.asarray(folds, dtype="<i8").tobytes(order="C"))
        payload = {
            "schema": 1,
            "algorithm": "out-of-fold-calibration-v1",
            "sample_count": self.dataset.rows,
            "fold_count": self.config.calibration_folds,
            "fold_sizes": [int(np.sum(folds == index)) for index in range(self.config.calibration_folds)],
            "fold_assignment_b64": _encode_u16(folds.astype(np.uint16)),
            "fold_assignment_digest": fold_assignment_digest,
            "calibrator": calibrator.to_payload(),
            "calibrator_human_parameters": calibrator.human_parameters(),
            "precision": {
                "effective_decimals": decimals,
                "scale": scale_for_decimals(decimals),
            },
            "oof_raw_scores_q": oof_q.tolist(),
            "oof_probabilities_q": probability_q.tolist(),
            "oof_raw_score_digest": int64_digest(oof_q),
            "oof_probability_digest": int64_digest(probability_q),
        }
        record = self.workspace.publish_checkpoint(
            stage="calibration",
            input_fingerprint=expected_input,
            upstream_fingerprint=expected_upstream,
            config_version=self.config.stage_config_version("calibration"),
            payload=payload,
            accumulator={
                "batches": self.config.calibration_folds,
                "samples": self.dataset.rows,
                "fit_iterations": calibrator.iterations,
                "oof_raw_score_digest": payload["oof_raw_score_digest"],
                "oof_probability_digest": payload["oof_probability_digest"],
            },
        )
        payload["_checkpoint"] = {"seq": record["seq"], "version_fingerprint": record["version_fingerprint"]}
        self.workspace.clear_progress("calibration")
        return payload, payload["calibrator"]

    def _expected_diagnostics_input(self, calibration_record: dict[str, Any], calibration_payload: dict[str, Any]) -> str:
        return sha256_json(
            {
                "schema": 1,
                "stage": "diagnostics",
                "algorithm": ALGORITHM_VERSION,
                "upstream_fingerprint": calibration_record["version_fingerprint"],
                "calibration_probability_digest": calibration_payload["oof_probability_digest"],
                "sample_count": self.dataset.rows,
                "config": {"diagnostic_bins": self.config.diagnostic_bins},
            }
        )

    def _ensure_diagnostics(self) -> tuple[dict[str, Any], dict[str, Any]]:
        calibration_record, calibration_payload = self.workspace.read_stage_payload("calibration")
        expected_input = self._expected_diagnostics_input(calibration_record, calibration_payload)
        expected_upstream = calibration_record["version_fingerprint"]
        if self.workspace.record_matches("diagnostics", input_fingerprint=expected_input, upstream_fingerprint=expected_upstream):
            _, payload = self.workspace.read_stage_payload("diagnostics")
            self.workspace.clear_progress("diagnostics")
            return payload, payload
        self._invalidate_current_mismatch("diagnostics", expected_input, expected_upstream)
        self._check_stop("diagnostics")
        decimals = int(calibration_payload["precision"]["effective_decimals"])
        oof_q = np.asarray(calibration_payload["oof_raw_scores_q"], dtype=np.int64)
        stored_probability_q = np.asarray(calibration_payload["oof_probabilities_q"], dtype=np.int64)
        calibrator = Calibrator.from_payload(calibration_payload["calibrator"])
        raw_scores = dequantize(oof_q, decimals)
        reference_probability = apply_calibrator(calibrator, raw_scores, stage="diagnostics")
        recomputed_probability_q = quantize_probabilities(reference_probability, decimals, stage="diagnostics")
        if not np.array_equal(recomputed_probability_q, stored_probability_q):
            mismatch = int(np.flatnonzero(recomputed_probability_q != stored_probability_q)[0])
            raise WorkbenchError(
                "E_STALE_CHECKPOINT",
                "stored calibrated probabilities do not match the current raw scores and calibrator",
                {"stage": "calibration", "sample_index": mismatch},
            )
        loss_q, brier_q = probability_terms(stored_probability_q, self.dataset.y, decimals, stage="diagnostics")
        reliability = reliability_table(stored_probability_q, self.dataset.y, self.config.diagnostic_bins, decimals)
        precision_report = high_precision_error_report(
            reference_probability,
            self.dataset.y,
            stored_probability_q,
            loss_q,
            brier_q,
            decimals,
        )
        total_loss_q = int(sum(int(value) for value in loss_q))
        total_brier_q = int(sum(int(value) for value in brier_q))
        scale = float(scale_for_decimals(decimals))
        payload = {
            "schema": 1,
            "stage": "diagnostics",
            "sample_count": self.dataset.rows,
            "metrics": {
                "log_loss_q": total_loss_q,
                "brier_q": total_brier_q,
                "total_log_loss": total_loss_q / scale,
                "total_brier": total_brier_q / scale,
                "log_loss": total_loss_q / (scale * self.dataset.rows),
                "brier": total_brier_q / (scale * self.dataset.rows),
                "ece": reliability["ece"],
                "max_bin_deviation": reliability["max_bin_deviation"],
            },
            "reliability": reliability,
            "precision": precision_report,
            "calibrator": {
                "type": calibrator.kind,
                "iterations": calibrator.iterations,
                "converged": calibrator.converged,
                "parameters": calibrator.human_parameters(),
            },
            "fold_summary": {
                "count": calibration_payload["fold_count"],
                "sizes": calibration_payload["fold_sizes"],
                "assignment_digest": calibration_payload["fold_assignment_digest"],
            },
            "digests": {
                "oof_raw_scores": calibration_payload["oof_raw_score_digest"],
                "oof_probabilities": calibration_payload["oof_probability_digest"],
                "probability_vector": int64_digest(stored_probability_q),
            },
            "numeric_check": {"status": "passed", "invalid_samples": 0},
        }
        record = self.workspace.publish_checkpoint(
            stage="diagnostics",
            input_fingerprint=expected_input,
            upstream_fingerprint=expected_upstream,
            config_version=self.config.stage_config_version("diagnostics"),
            payload=payload,
            accumulator={
                "batches": 1,
                "samples": self.dataset.rows,
                "loss_sum_q": total_loss_q,
                "brier_sum_q": total_brier_q,
                "probability_digest": payload["digests"]["probability_vector"],
            },
        )
        payload["_checkpoint"] = {"seq": record["seq"], "version_fingerprint": record["version_fingerprint"]}
        self.workspace.clear_progress("diagnostics")
        return payload, payload

    def run_digest(self) -> str:
        status = self.workspace.status()
        if not all(status["stages"][stage]["status"] == "valid" for stage in STAGES):
            raise WorkbenchError("E_INVALID_STAGE", "run digest requires all stages to be valid")
        return sha256_json(
            {
                "data_fingerprint": self.dataset.data_fingerprint,
                "config_version": status["config_version"],
                "stage_versions": {stage: status["stages"][stage]["version_fingerprint"] for stage in STAGES},
                "payloads": {stage: status["stages"][stage]["payload_sha256"] for stage in STAGES},
            }
        )

    def _require_complete(self) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        _, binning_payload = self.workspace.read_stage_payload("binning")
        _, boosting_payload = self.workspace.read_stage_payload("boosting")
        _, calibration_payload = self.workspace.read_stage_payload("calibration")
        self.workspace.read_stage_payload("diagnostics")
        return binning_payload, boosting_payload, calibration_payload

    def predict_batches(self, batch_size: int, *, reverse: bool = False) -> dict[str, Any]:
        binning_payload, boosting_payload, calibration_payload = self._require_complete()
        _, binned = binned_from_payload(binning_payload)
        if binned.shape[0] != self.dataset.rows:
            raise WorkbenchError("E_INVALID_STAGE", "binned matrix does not match imported dataset", {"stage": "binning"})
        model = GBDTModel.from_payload(boosting_payload["model"])
        calibrator = Calibrator.from_payload(calibration_payload["calibrator"])
        accumulator = PredictionAccumulator(self.config, calibrator)
        batches = make_batches(self.dataset.rows, batch_size, reverse=reverse)
        for indices in batches:
            raw_scores = predict_model_raw(model, binned[indices])
            accumulator.add_raw_batch(
                raw_scores,
                self.dataset.y[indices],
                self.dataset.sample_ids[indices].tolist(),
                batch_size=int(indices.size),
                stage="batch_prediction",
            )
        result = accumulator.finalize()
        if result["accumulator"]["samples"] != self.dataset.rows:
            raise WorkbenchError(
                "E_BATCH_STATE",
                "batch reduction did not cover every dataset sample",
                {"expected": self.dataset.rows, "reduced": result["accumulator"]["samples"]},
            )
        return {
            **result,
            "dataset": self.dataset.key,
            "batch_size": batch_size,
            "batch_order": "reverse" if reverse else "forward",
            "batch_count": len(batches),
        }

    def compare_batch_modes(self, batch_sizes: list[int]) -> dict[str, Any]:
        if not batch_sizes:
            raise WorkbenchError("E_BATCH_STATE", "at least one batch size is required")
        runs: list[dict[str, Any]] = []
        for batch_size in batch_sizes:
            for reverse in (False, True):
                result = self.predict_batches(batch_size, reverse=reverse)
                runs.append(
                    {
                        "batch_size": batch_size,
                        "batch_order": result["batch_order"],
                        "batch_count": result["batch_count"],
                        "result_digest": comparable_digest(result),
                        "probability_vector_digest": result["probability_vector_digest"],
                        "log_loss": result["log_loss"],
                        "brier": result["brier"],
                        "sample_count": result["accumulator"]["samples"],
                    }
                )
        reference = runs[0]["result_digest"]
        all_equal = all(run["result_digest"] == reference for run in runs)
        return {
            "all_equal": all_equal,
            "reference_digest": reference,
            "runs": runs,
            "probability_vector_digest": runs[0]["probability_vector_digest"],
        }

    def export_report(self, *, batch_size: int = 257) -> dict[str, Any]:
        status = self.workspace.status()
        _, diagnostics = self.workspace.read_stage_payload("diagnostics")
        prediction = self.predict_batches(batch_size)
        return {
            "schema": 1,
            "dataset": status["dataset"],
            "config": status["config"],
            "config_version": status["config_version"],
            "run_digest": self.run_digest(),
            "stage_fingerprints": status["fingerprints"],
            "invalidation_reasons": {
                stage: status["stages"][stage].get("reasons", [])
                for stage in STAGES
                if status["stages"][stage]["status"] == "invalid"
            },
            "diagnostics": diagnostics,
            "probability_export": {
                "batch_size": batch_size,
                "sample_count": prediction["accumulator"]["samples"],
                "probability_vector_digest": prediction["probability_vector_digest"],
                "result_digest": comparable_digest(prediction),
                "log_loss": prediction["log_loss"],
                "brier": prediction["brier"],
            },
        }

    def write_exports(self, out_dir: Path, *, batch_size: int = 257) -> dict[str, Any]:
        out_dir = ensure_within(self.workspace.project_root, out_dir, label="out_dir")
        out_dir.mkdir(parents=True, exist_ok=True)
        report = self.export_report(batch_size=batch_size)
        prediction = self.predict_batches(batch_size)
        decimals = int(self.workspace.config.quantize_decimals)
        scale = scale_for_decimals(decimals)

        def fixed(value: int) -> str:
            sign = "-" if value < 0 else ""
            value = abs(int(value))
            if decimals == 0:
                return f"{sign}{value}"
            whole, fraction = divmod(value, scale)
            return f"{sign}{whole}.{fraction:0{decimals}d}"

        lines = ["sample_id,label,raw_score_q,probability_q,scale,raw_score,probability"]
        for sample_id, label, raw_q, probability_q in zip(
            prediction["sample_ids"],
            prediction["labels"],
            prediction["raw_scores_q"],
            prediction["probabilities_q"],
        ):
            lines.append(
                f"{sample_id},{int(label)},{int(raw_q)},{int(probability_q)},{scale},{fixed(raw_q)},{fixed(probability_q)}"
            )
        csv_bytes = ("\n".join(lines) + "\n").encode("utf-8")
        report_bytes = canonical_json_bytes(report)
        probability_path = out_dir / "probabilities.csv"
        report_path = out_dir / "diagnostics_report.json"
        atomic_write_bytes(probability_path, csv_bytes)
        atomic_write_bytes(report_path, report_bytes)
        return {
            "probabilities_path": str(probability_path),
            "report_path": str(report_path),
            "probabilities_sha256": sha256_bytes(csv_bytes),
            "report_sha256": sha256_bytes(report_bytes),
            "run_digest": report["run_digest"],
        }

    def compare_with_full_rebuild(self) -> dict[str, Any]:
        """Build the same dataset/config from scratch and compare affected artifacts."""

        _, incremental_boost = self.workspace.read_stage_payload("boosting")
        _, incremental_cal = self.workspace.read_stage_payload("calibration")
        _, incremental_diag = self.workspace.read_stage_payload("diagnostics")
        temporary_parent = self.workspace.state_dir.parent
        temporary_parent.mkdir(parents=True, exist_ok=True)
        temporary_root = Path(tempfile.mkdtemp(prefix=".incremental-verify-", dir=temporary_parent))
        try:
            full_workspace = Workspace(
                self.workspace.project_root,
                state_dir=temporary_root,
                data_dir=self.workspace.data_dir,
            )
            full_dataset = load_dataset(self.dataset.key, project_root=self.workspace.project_root, data_dir=self.workspace.data_dir)
            full_workspace.import_dataset(full_dataset)
            full_workspace.set_config(self.workspace.config.to_dict())
            full_pipeline = Pipeline(full_workspace, full_dataset)
            full_pipeline.run_all()
            _, full_boost = full_workspace.read_stage_payload("boosting")
            _, full_cal = full_workspace.read_stage_payload("calibration")
            _, full_diag = full_workspace.read_stage_payload("diagnostics")
            differences: list[str] = []
            if canonical_json_bytes({"raw_score_digest": incremental_boost["raw_score_digest"]}) != canonical_json_bytes(
                {"raw_score_digest": full_boost["raw_score_digest"]}
            ):
                differences.append("boosting")
            incremental_calibrator = canonical_json_bytes(
                {
                    "calibrator": incremental_cal["calibrator"],
                    "fold_assignment_digest": incremental_cal["fold_assignment_digest"],
                    "oof_raw_score_digest": incremental_cal["oof_raw_score_digest"],
                }
            )
            full_calibrator = canonical_json_bytes(
                {
                    "calibrator": full_cal["calibrator"],
                    "fold_assignment_digest": full_cal["fold_assignment_digest"],
                    "oof_raw_score_digest": full_cal["oof_raw_score_digest"],
                }
            )
            if incremental_calibrator != full_calibrator:
                differences.append("calibration")
            if canonical_json_bytes(incremental_diag) != canonical_json_bytes(full_diag):
                differences.append("diagnostics")
            incremental_digest = self.run_digest()
            full_digest = full_pipeline.run_digest()
            if incremental_digest != full_digest and not differences:
                differences.append("run_digest")
            if differences:
                raise WorkbenchError(
                    "E_INCREMENTAL_MISMATCH",
                    "incremental and from-scratch artifacts differ",
                    {
                        "differences": differences,
                        "incremental_run_digest": incremental_digest,
                        "full_run_digest": full_digest,
                    },
                )
            return {
                "ok": True,
                "differences": [],
                "incremental_run_digest": incremental_digest,
                "full_run_digest": full_digest,
                "raw_score_digest": incremental_boost["raw_score_digest"],
                "calibrator_sha256": sha256_bytes(incremental_calibrator),
                "diagnostics_sha256": sha256_bytes(canonical_json_bytes(incremental_diag)),
            }
        finally:
            shutil.rmtree(temporary_root, ignore_errors=True)
