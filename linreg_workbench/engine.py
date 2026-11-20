"""Four-stage deterministic training engine with checkpointed recovery."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np

from .cache import ArtifactCache
from .canonical import atomic_write_json, coefficient_summary, digest
from .checkpoints import CheckpointStore
from .config import config_version, task_candidates
from .data import DatasetRegistry
from .errors import (
    InvalidStage,
    NumericError,
    StopRequested,
)
from .folds import folds_fingerprint, make_folds
from .preprocess import preprocess, preprocessing_fingerprint
from .solver import solve_logistic, validation_metrics

def solve_task_fingerprint(
    *,
    preprocess_fp: str,
    folds_fp: str,
    role: str,
    fold: int,
    strength: float,
    l1_ratio: float,
    solver: dict[str, Any],
    training_indices: list[int],
    validation_indices: list[int],
) -> str:
    return digest(
        {
            "kind": "solve-task-v1",
            "preprocess_fingerprint": preprocess_fp,
            "folds_fingerprint": folds_fp,
            "role": role,
            "fold": fold,
            "strength": float(strength),
            "l1_ratio": float(l1_ratio),
            "solver": solver,
            "training_indices": training_indices,
            "validation_indices": validation_indices,
        }
    )


def aggregate_content_fingerprint(
    *,
    folds_fp: str,
    task_fingerprints: list[str],
    metric: str,
    selection: dict[str, Any],
) -> str:
    return digest(
        {
            "kind": "aggregate-v1",
            "folds_fingerprint": folds_fp,
            "task_fingerprints": task_fingerprints,
            "metric": metric,
            "selection": selection,
        }
    )


def build_solve_tasks(
    preprocess_artifact: dict[str, Any],
    folds_artifact: dict[str, Any],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    candidates = task_candidates(config)
    folds = folds_artifact["folds"]
    task_index = 1
    for fold in folds:
        for candidate in candidates:
            fp = solve_task_fingerprint(
                preprocess_fp=preprocess_artifact["fingerprint"],
                folds_fp=folds_artifact["fingerprint"],
                role="solve",
                fold=int(fold["fold"]),
                strength=candidate["strength"],
                l1_ratio=candidate["l1_ratio"],
                solver=config["solver"],
                training_indices=fold["training_indices"],
                validation_indices=fold["validation_indices"],
            )
            tasks.append(
                {
                    "task_index": task_index,
                    "role": "solve",
                    "fold": int(fold["fold"]),
                    "strength": candidate["strength"],
                    "l1_ratio": candidate["l1_ratio"],
                    "fingerprint": fp,
                    "candidate_index": None,
                }
            )
            task_index += 1
    final_fp = solve_task_fingerprint(
        preprocess_fp=preprocess_artifact["fingerprint"],
        folds_fp=folds_artifact["fingerprint"],
        role="final",
        fold=0,
        strength=0.0,
        l1_ratio=0.0,
        solver=config["solver"],
        training_indices=list(range(preprocess_artifact["row_count"])),
        validation_indices=[],
    )
    # The candidate-specific final fingerprint is regenerated after selection.
    tasks.append(
        {
            "task_index": task_index,
            "role": "final",
            "fold": 0,
            "strength": None,
            "l1_ratio": None,
            "fingerprint": final_fp,
            "candidate_index": None,
        }
    )
    return tasks


def final_task_fingerprint(
    preprocess_artifact: dict[str, Any],
    folds_artifact: dict[str, Any],
    config: dict[str, Any],
    candidate: dict[str, float],
) -> str:
    return solve_task_fingerprint(
        preprocess_fp=preprocess_artifact["fingerprint"],
        folds_fp=folds_artifact["fingerprint"],
        role="final",
        fold=0,
        strength=candidate["strength"],
        l1_ratio=candidate["l1_ratio"],
        solver=config["solver"],
        training_indices=list(range(preprocess_artifact["row_count"])),
        validation_indices=[],
    )


def preprocess_stage_fingerprint(preprocess_artifact: dict[str, Any]) -> str:
    return preprocess_artifact["fingerprint"]


def solve_stage_fingerprint(
    folds_fp: str, task_fingerprints: list[str]
) -> str:
    return digest(
        {
            "kind": "solve-stage-v1",
            "folds_fingerprint": folds_fp,
            "task_fingerprints": task_fingerprints,
        }
    )


class RunEngine:
    def __init__(
        self,
        *,
        run_dir: Path,
        run: dict[str, Any],
        registry: DatasetRegistry,
        cache: ArtifactCache,
        stop_after_tasks: int | None = None,
    ) -> None:
        self.run_dir = run_dir
        self.run = run
        self.registry = registry
        self.cache = cache
        self.checkpoints = CheckpointStore(run_dir, run["run_id"])
        self.stop_after_tasks = stop_after_tasks
        self.completed_tasks_this_invocation = 0
        self.artifacts = run_dir / "artifacts"
        self.artifacts.mkdir(parents=True, exist_ok=True)
        self.config = run["config"]
        self.config_version = run["config_version"]
        self.spec = registry.get(run["dataset_id"])

    def _save_run(self) -> None:
        atomic_write_json(self.run_dir / "run.json", self.run)

    def _check_stop(self) -> None:
        if (self.run_dir / "STOP").exists():
            raise StopRequested

    def _stop_callback(self, _iteration: int, _objective: float) -> None:
        self._check_stop()

    def _write_artifact(self, name: str, artifact: dict[str, Any]) -> tuple[str, str]:
        path = self.artifacts / name
        atomic_write_json(path, artifact)
        relative = str(path.relative_to(self.run_dir))
        return relative, str(path)

    def _load_record_artifact(self, record: dict[str, Any]) -> dict[str, Any]:
        _, artifact = self.checkpoints.artifact_for(record)
        return artifact

    def _set_progress(self, **values: Any) -> None:
        self.run.update(values)
        self._save_run()

    def _prepare_resume(self) -> dict[str, Any]:
        state = self.checkpoints.inspect()
        if state["ignored"]:
            state = self.checkpoints.repair_from_gap()
        return state

    def _ensure_preprocess(
        self, state: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, Any] | None]:
        records = state["records"]
        expected_fp = preprocessing_fingerprint(self.spec, self.config)
        if records:
            record = records[0]
            if (
                record["stage"] != "preprocess"
                or record["input_fingerprint"] != expected_fp
                or record["config_version"] != self.config_version
            ):
                self.checkpoints.truncate_at(1)
                state = self.checkpoints.inspect()
                records = state["records"]
            else:
                try:
                    artifact = self._load_record_artifact(record)
                    if artifact.get("fingerprint") != expected_fp:
                        raise InvalidStage("Preprocess artifact fingerprint mismatch")
                    return artifact, record
                except (InvalidStage, OSError, ValueError):
                    self.checkpoints.truncate_at(record["sequence"])
                    state = self.checkpoints.inspect()
                    records = state["records"]
        self._check_stop()
        self._set_progress(stage="preprocess", current_fold=None, current_task=None)
        artifact, source = self.cache.get("preprocess", expected_fp)
        reused_from = source
        if artifact is None:
            artifact = preprocess(self.spec, self.config)
            self.cache.put("preprocess", expected_fp, artifact)
            reused_from = None
        elif artifact.get("fingerprint") != expected_fp:
            artifact = preprocess(self.spec, self.config)
            self.cache.put("preprocess", expected_fp, artifact)
            reused_from = None
        relative, _ = self._write_artifact("preprocess.json", artifact)
        record = self.checkpoints.publish(
            stage="preprocess",
            fold=None,
            task_index=None,
            role="preprocess",
            input_fingerprint=expected_fp,
            config_version=self.config_version,
            coefficient_summary=None,
            artifact_path=relative,
            reused_from=reused_from,
        )
        self.run["stage_fingerprints"]["preprocess"] = expected_fp
        self.run["reuse"].append(
            {"stage": "preprocess", "reused": reused_from is not None, "from": reused_from}
        )
        self._set_progress(
            stage="preprocess",
            completed_preprocess=True,
            empty_feature_errors=artifact.get("empty_feature_errors", []),
        )
        return artifact, record

    def _ensure_folds(
        self,
        preprocess_artifact: dict[str, Any],
        state: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any] | None]:
        records = state["records"]
        expected_fp = folds_fingerprint(preprocess_artifact["fingerprint"], self.config)
        if len(records) >= 2:
            record = records[1]
            if (
                record["stage"] != "folds"
                or record["input_fingerprint"] != expected_fp
                or record["config_version"] != self.config_version
            ):
                self.checkpoints.truncate_at(record["sequence"])
                state = self.checkpoints.inspect()
                records = state["records"]
            else:
                try:
                    artifact = self._load_record_artifact(record)
                    if artifact.get("fingerprint") != expected_fp:
                        raise InvalidStage("Folds artifact fingerprint mismatch")
                    return artifact, record
                except (InvalidStage, OSError, ValueError):
                    self.checkpoints.truncate_at(record["sequence"])
                    state = self.checkpoints.inspect()
                    records = state["records"]
        self._check_stop()
        self._set_progress(stage="folds", current_fold=None, current_task=None)
        artifact, source = self.cache.get("folds", expected_fp)
        reused_from = source
        if artifact is None or artifact.get("fingerprint") != expected_fp:
            artifact = make_folds(
                preprocess_artifact["y"], self.config, preprocess_artifact["fingerprint"]
            )
            self.cache.put("folds", expected_fp, artifact)
            reused_from = None
        relative, _ = self._write_artifact("folds.json", artifact)
        record = self.checkpoints.publish(
            stage="folds",
            fold=None,
            task_index=None,
            role="folds",
            input_fingerprint=expected_fp,
            config_version=self.config_version,
            coefficient_summary=None,
            artifact_path=relative,
            reused_from=reused_from,
        )
        self.run["stage_fingerprints"]["folds"] = expected_fp
        self.run["reuse"].append(
            {"stage": "folds", "reused": reused_from is not None, "from": reused_from}
        )
        self._set_progress(stage="folds", completed_folds=True, folds_count=artifact["folds_count"])
        return artifact, record

    def _task_metadata(self, tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [
            {
                "task_index": item["task_index"],
                "role": item["role"],
                "fold": item["fold"],
                "strength": item["strength"],
                "l1_ratio": item["l1_ratio"],
                "fingerprint": item["fingerprint"],
            }
            for item in tasks
        ]

    def _solve_one(
        self,
        *,
        X: np.ndarray,
        y: np.ndarray,
        feature_names: list[str],
        task: dict[str, Any],
        training_indices: list[int],
        validation_indices: list[int],
    ) -> tuple[dict[str, Any], str | None]:
        cached, source = self.cache.get("solve", task["fingerprint"])
        if cached is not None and cached.get("fingerprint") == task["fingerprint"]:
            return cached, source
        if task["role"] == "final":
            candidate = task["candidate"]
            X_train = X
            y_train = y
            X_validation = None
            y_validation = None
        else:
            candidate = {"strength": task["strength"], "l1_ratio": task["l1_ratio"]}
            X_train = X[training_indices]
            y_train = y[training_indices]
            X_validation = X[validation_indices]
            y_validation = y[validation_indices]
        self._set_progress(
            stage="solve",
            current_fold=task["fold"],
            current_task=task["task_index"],
            current_role=task["role"],
            current_hyperparameters=candidate,
        )
        solved = solve_logistic(
            np.asarray(X_train, dtype=np.float64),
            np.asarray(y_train, dtype=np.float64),
            strength=candidate["strength"],
            l1_ratio=candidate["l1_ratio"],
            config=self.config,
            fold=task["fold"],
            feature_names=feature_names,
            stop_callback=self._stop_callback,
        )
        task_record = {
            "artifact_type": "solve",
            "fingerprint": task["fingerprint"],
            "role": task["role"],
            "fold": task["fold"],
            "task_index": task["task_index"],
            "strength": candidate["strength"],
            "l1_ratio": candidate["l1_ratio"],
            "training_size": int(len(training_indices)) if task["role"] != "final" else int(len(y)),
            "validation_size": int(len(validation_indices)),
            "feature_names": feature_names,
            "coefficients": solved["coefficients"],
            "intercept": solved["intercept"],
            "trajectory": solved["trajectory"],
            "trajectory_summary": solved["trajectory_summary"],
            "validation": None,
        }
        if task["role"] != "final":
            assert X_validation is not None and y_validation is not None
            try:
                metrics = validation_metrics(
                    X_validation,
                    y_validation,
                    np.asarray(solved["coefficients"], dtype=np.float64),
                    solved["intercept"],
                )
            except NumericError as exc:
                exc.details.setdefault("fold", task["fold"])
                exc.details.setdefault("iteration", solved["trajectory_summary"]["iterations"])
                exc.details.setdefault("strength", candidate["strength"])
                exc.details.setdefault("l1_ratio", candidate["l1_ratio"])
                raise
            task_record["validation"] = metrics
        self.cache.put("solve", task["fingerprint"], task_record)
        return task_record, None

    def _restore_solve_records(
        self,
        state: dict[str, Any],
        tasks: list[dict[str, Any]],
        preprocess_artifact: dict[str, Any],
        folds_artifact: dict[str, Any],
        *,
        allow_final: bool,
    ) -> tuple[dict[int, dict[str, Any]], dict[str, Any]]:
        solve_records = [record for record in state["records"] if record["stage"] == "solve"]
        restored: dict[int, dict[str, Any]] = {}
        for record in solve_records:
            if record["role"] == "final" and not allow_final:
                # The final task's fingerprint embeds the selected hyperparameters,
                # which are only known after CV selection.
                continue
            index = record["task_index"]
            if index is None or index < 1 or index > len(tasks):
                self.checkpoints.truncate_at(record["sequence"])
                return restored, self.checkpoints.inspect()
            task = tasks[index - 1]
            if (
                record["input_fingerprint"] != task["fingerprint"]
                or record["role"] != task["role"]
                or record["fold"] != task["fold"]
            ):
                self.checkpoints.truncate_at(record["sequence"])
                return restored, self.checkpoints.inspect()
            try:
                artifact = self._load_record_artifact(record)
                if (
                    artifact.get("fingerprint") != task["fingerprint"]
                    or artifact.get("task_index") != index
                ):
                    raise InvalidStage("Solve artifact/task mismatch")
            except (InvalidStage, OSError, ValueError):
                self.checkpoints.truncate_at(record["sequence"])
                return restored, self.checkpoints.inspect()
            restored[index] = artifact
        return restored, state

    def _fold_status_entry(self, artifact: dict[str, Any]) -> dict[str, Any]:
        validation = artifact.get("validation") or {}
        summary = artifact.get("trajectory_summary", {})
        return {
            "task_index": artifact["task_index"],
            "role": artifact["role"],
            "fold": artifact["fold"],
            "strength": artifact["strength"],
            "l1_ratio": artifact["l1_ratio"],
            "status": "complete",
            "validation": validation,
            "trajectory_summary": {
                "iterations": summary.get("iterations"),
                "converged": summary.get("converged"),
                "initial_objective": summary.get("initial_objective"),
                "final_objective": summary.get("final_objective"),
                "coefficient_change": summary.get("coefficient_change"),
                "trajectory_digest": summary.get("trajectory_digest"),
            },
            "coefficient_summary": summary.get("coefficient_summary"),
        }

    def _publish_solve(
        self,
        task: dict[str, Any],
        artifact: dict[str, Any],
        reused_from: str | None,
    ) -> dict[str, Any]:
        name = (
            f"final-{task['task_index']:04d}.json"
            if task["role"] == "final"
            else f"solve-{task['task_index']:04d}.json"
        )
        relative, _ = self._write_artifact(name, artifact)
        summary = coefficient_summary(artifact["coefficients"], artifact["intercept"])
        record = self.checkpoints.publish(
            stage="solve",
            fold=task["fold"],
            task_index=task["task_index"],
            role=task["role"],
            input_fingerprint=task["fingerprint"],
            config_version=self.config_version,
            coefficient_summary=summary,
            artifact_path=relative,
            reused_from=reused_from,
            expected_task={
                "stage": "solve",
                "fold": task["fold"],
                "task_index": task["task_index"],
                "role": task["role"],
                "input_fingerprint": task["fingerprint"],
            },
        )
        self.completed_tasks_this_invocation += 1
        if (
            self.stop_after_tasks is not None
            and self.completed_tasks_this_invocation >= self.stop_after_tasks
        ):
            self.checkpoints.append_stop_marker(
                {"requested": True, "reason": "stop_after_tasks demonstration hook"}
            )
        return record

    def _select(self, candidate_results: list[dict[str, Any]]) -> dict[str, Any]:
        metric = self.config["cv"]["metric"]
        candidates: dict[tuple[float, float], list[dict[str, Any]]] = {}
        for item in candidate_results:
            key = (float(item["strength"]), float(item["l1_ratio"]))
            candidates.setdefault(key, []).append(item)
        summaries: list[dict[str, Any]] = []
        for (strength, ratio), items in sorted(candidates.items()):
            values: list[float] = []
            log_losses: list[float] = []
            for item in sorted(items, key=lambda row: row["fold"]):
                value = item["validation"][metric]
                if not isinstance(value, (int, float)) or not np.isfinite(float(value)):
                    raise NumericError(
                        "Cross-validation metric is not finite",
                        details={
                            "fold": item["fold"],
                            "iteration": item.get("trajectory_summary", {}).get("iterations"),
                            "strength": strength,
                            "l1_ratio": ratio,
                            "metric": metric,
                            "value": value,
                            "reason": "NaN or Inf in cross-validation metric",
                        },
                    )
                values.append(float(value))
                log_losses.append(float(item["validation"]["log_loss"]))
            mean = float(np.mean(values))
            std = float(np.std(values))
            if not np.isfinite(mean) or not np.isfinite(std):
                raise NumericError(
                    "Cross-validation aggregate is not finite",
                    details={
                        "fold": None,
                        "iteration": None,
                        "strength": strength,
                        "l1_ratio": ratio,
                        "metric": metric,
                        "reason": "NaN or Inf in mean or standard deviation",
                    },
                )
            summaries.append(
                {
                    "strength": strength,
                    "l1_ratio": ratio,
                    "metric": metric,
                    "mean": mean,
                    "std": std,
                    "mean_log_loss": float(np.mean(log_losses)),
                    "folds": [
                        {
                            "fold": int(item["fold"]),
                            "value": float(item["validation"][metric]),
                            "log_loss": float(item["validation"]["log_loss"]),
                            "accuracy": float(item["validation"]["accuracy"]),
                        }
                        for item in sorted(items, key=lambda row: row["fold"])
                    ],
                }
            )
        if metric == "log_loss":
            best = min(
                summaries,
                key=lambda item: (item["mean"], item["strength"], -item["l1_ratio"]),
            )
        else:
            best = min(
                summaries,
                key=lambda item: (
                    -item["mean"],
                    item["mean_log_loss"],
                    item["strength"],
                    -item["l1_ratio"],
                ),
            )
        return {"best": best, "candidates": summaries}

    def _build_result(
        self,
        aggregate_artifact: dict[str, Any],
        final_artifact: dict[str, Any],
        preprocess_artifact: dict[str, Any],
        folds_artifact: dict[str, Any],
        tasks: list[dict[str, Any]],
    ) -> dict[str, Any]:
        result = {
            "schema_version": 1,
            "dataset_id": self.run["dataset_id"],
            "data_sha256": self.spec.sha256,
            "version_fingerprint": self.run["version_fingerprint"],
            "config_version": self.config_version,
            "stage_fingerprints": {
                "preprocess": preprocess_artifact["fingerprint"],
                "folds": folds_artifact["fingerprint"],
                "solve": self.run["stage_fingerprints"]["solve"],
                "aggregate": aggregate_artifact["fingerprint"],
            },
            "config": self.config,
            "selected_hyperparameters": aggregate_artifact["selection"]["best"],
            "cv": {
                "metric": self.config["cv"]["metric"],
                "folds": folds_artifact["folds_count"],
                "mean": aggregate_artifact["selection"]["best"]["mean"],
                "std": aggregate_artifact["selection"]["best"]["std"],
                "fold_values": aggregate_artifact["selection"]["best"]["folds"],
                "selection": aggregate_artifact["selection"],
            },
            "feature_names": final_artifact["feature_names"],
            "coefficients": final_artifact["coefficients"],
            "intercept": final_artifact["intercept"],
            "trajectory_summary": final_artifact["trajectory_summary"],
            "empty_feature_errors": preprocess_artifact.get("empty_feature_errors", []),
            "diagnostics": preprocess_artifact.get("diagnostics", []),
            "checkpoint_tasks": len(tasks),
            "result_digest": "",
        }
        unsigned = {key: value for key, value in result.items() if key != "result_digest"}
        result["result_digest"] = digest(unsigned)
        return result

    def execute(self) -> dict[str, Any]:
        self.run["status"] = "running"
        self.run["error"] = None
        self._save_run()
        state = self._prepare_resume()
        preprocess_artifact, _ = self._ensure_preprocess(state)
        state = self.checkpoints.inspect()
        folds_artifact, _ = self._ensure_folds(preprocess_artifact, state)
        tasks = build_solve_tasks(preprocess_artifact, folds_artifact, self.config)
        X = np.asarray(preprocess_artifact["X"], dtype=np.float64)
        y = np.asarray(preprocess_artifact["y"], dtype=np.float64)
        feature_names = list(preprocess_artifact["feature_names"])
        folds_by_number = {int(item["fold"]): item for item in folds_artifact["folds"]}

        state = self.checkpoints.inspect()
        restored, state = self._restore_solve_records(
            state, tasks, preprocess_artifact, folds_artifact, allow_final=False
        )
        candidate_results: list[dict[str, Any]] = []
        # Restore validation results from already checkpointed CV solves.
        for task in tasks:
            if task["role"] != "solve":
                continue
            artifact = restored.get(task["task_index"])
            if artifact is not None:
                candidate_results.append(artifact)
        fold_status = [
            self._fold_status_entry(artifact)
            for _, artifact in sorted(
                (
                    (task["task_index"], restored[task["task_index"]])
                    for task in tasks
                    if task["role"] == "solve" and task["task_index"] in restored
                ),
                key=lambda pair: pair[0],
            )
        ]
        completed_cv = len(candidate_results)
        total_cv = sum(1 for task in tasks if task["role"] == "solve")
        self._set_progress(
            completed_solve_tasks=len(restored),
            total_solve_tasks=len(tasks),
            completed_cv_tasks=completed_cv,
            total_cv_tasks=total_cv,
            solve_task_metadata=self._task_metadata(tasks),
            fold_status=fold_status,
        )
        self.run["stage_fingerprints"]["solve"] = solve_stage_fingerprint(
            folds_artifact["fingerprint"], [task["fingerprint"] for task in tasks]
        )
        self._save_run()

        for task in tasks:
            if task["role"] != "solve":
                continue
            if task["task_index"] in restored:
                continue
            self._check_stop()
            fold = folds_by_number[task["fold"]]
            artifact, source = self._solve_one(
                X=X,
                y=y,
                feature_names=feature_names,
                task=task,
                training_indices=fold["training_indices"],
                validation_indices=fold["validation_indices"],
            )
            self._publish_solve(task, artifact, source)
            candidate_results.append(artifact)
            fold_status.append(self._fold_status_entry(artifact))
            self.run["reuse"].append(
                {"stage": "solve", "task_index": task["task_index"], "reused": source is not None, "from": source}
            )
            self._set_progress(
                completed_solve_tasks=task["task_index"],
                completed_cv_tasks=len(candidate_results),
                fold_status=fold_status,
                current_task=None,
                current_fold=None,
            )
            if (self.run_dir / "STOP").exists():
                raise StopRequested

        if len(candidate_results) != total_cv:
            raise InvalidStage("Cross-validation results are incomplete")
        selection = self._select(candidate_results)
        self.run["selection"] = selection["best"]
        self._save_run()
        best = selection["best"]
        candidate = {"strength": best["strength"], "l1_ratio": best["l1_ratio"]}
        final_task = tasks[-1]
        final_task["strength"] = candidate["strength"]
        final_task["l1_ratio"] = candidate["l1_ratio"]
        final_task["candidate"] = candidate
        final_task["fingerprint"] = final_task_fingerprint(
            preprocess_artifact, folds_artifact, self.config, candidate
        )
        self.run["stage_fingerprints"]["solve"] = solve_stage_fingerprint(
            folds_artifact["fingerprint"], [task["fingerprint"] for task in tasks]
        )
        self.run["solve_task_metadata"] = self._task_metadata(tasks)
        self.run["stage_fingerprints"]["aggregate"] = aggregate_content_fingerprint(
            folds_fp=folds_artifact["fingerprint"],
            task_fingerprints=[task["fingerprint"] for task in tasks],
            metric=self.config["cv"]["metric"],
            selection=self.config["selection"],
        )
        self._save_run()

        state = self.checkpoints.inspect()
        restored, state = self._restore_solve_records(
            state, tasks, preprocess_artifact, folds_artifact, allow_final=True
        )
        if final_task["task_index"] not in restored:
            self._check_stop()
            artifact, source = self._solve_one(
                X=X,
                y=y,
                feature_names=feature_names,
                task=final_task,
                training_indices=list(range(len(y))),
                validation_indices=[],
            )
            self._publish_solve(final_task, artifact, source)
            restored[final_task["task_index"]] = artifact
            fold_status.append(self._fold_status_entry(artifact))
            self.run["reuse"].append(
                {"stage": "final", "task_index": final_task["task_index"], "reused": source is not None, "from": source}
            )
            self._set_progress(
                completed_solve_tasks=final_task["task_index"],
                fold_status=fold_status,
                solve_task_metadata=self._task_metadata(tasks),
                current_task=None,
                current_fold=0,
            )
            if (self.run_dir / "STOP").exists():
                raise StopRequested
        else:
            fold_status.append(self._fold_status_entry(restored[final_task["task_index"]]))
            self._set_progress(
                fold_status=fold_status,
                solve_task_metadata=self._task_metadata(tasks),
            )
        final_artifact = restored[final_task["task_index"]]

        aggregate_fp = self.run["stage_fingerprints"]["aggregate"]
        state = self.checkpoints.inspect()
        existing_aggregate = next(
            (record for record in state["records"] if record["stage"] == "aggregate"), None
        )
        aggregate_artifact: dict[str, Any]
        aggregate_source: str | None = None
        if existing_aggregate is not None:
            try:
                aggregate_artifact = self._load_record_artifact(existing_aggregate)
                if aggregate_artifact.get("fingerprint") != aggregate_fp:
                    raise InvalidStage("Aggregate fingerprint mismatch")
            except (InvalidStage, OSError, ValueError):
                self.checkpoints.truncate_at(existing_aggregate["sequence"])
                existing_aggregate = None
        if existing_aggregate is None:
            self._check_stop()
            self._set_progress(stage="aggregate", current_fold=None, current_task=None)
            cached, source = self.cache.get("aggregate", aggregate_fp)
            aggregate_source = source
            if cached is not None and cached.get("fingerprint") == aggregate_fp:
                aggregate_artifact = cached
            else:
                aggregate_artifact = {
                    "artifact_type": "aggregate",
                    "fingerprint": aggregate_fp,
                    "selection": selection,
                    "final_task_fingerprint": final_task["fingerprint"],
                    "final_coefficient_summary": coefficient_summary(
                        final_artifact["coefficients"], final_artifact["intercept"]
                    ),
                    "task_fingerprints": [task["fingerprint"] for task in tasks],
                }
                self.cache.put("aggregate", aggregate_fp, aggregate_artifact)
            relative, _ = self._write_artifact("aggregate.json", aggregate_artifact)
            self.checkpoints.publish(
                stage="aggregate",
                fold=None,
                task_index=None,
                role="aggregate",
                input_fingerprint=aggregate_fp,
                config_version=self.config_version,
                coefficient_summary=coefficient_summary(
                    final_artifact["coefficients"], final_artifact["intercept"]
                ),
                artifact_path=relative,
                reused_from=aggregate_source,
            )
            self.run["reuse"].append(
                {"stage": "aggregate", "reused": aggregate_source is not None, "from": aggregate_source}
            )
        result = self._build_result(
            aggregate_artifact, final_artifact, preprocess_artifact, folds_artifact, tasks
        )
        atomic_write_json(self.artifacts / "result.json", result)
        atomic_write_json(self.artifacts / "result-summary.json", self.summary_from_result(result))
        self.run["status"] = "complete"
        self.run["stage"] = "complete"
        self.run["result_path"] = str((self.artifacts / "result.json").relative_to(self.run_dir))
        self.run["result_digest"] = result["result_digest"]
        self.run["completed_solve_tasks"] = len(tasks)
        self._save_run()
        return result

    @staticmethod
    def summary_from_result(result: dict[str, Any]) -> dict[str, Any]:
        best = result["selected_hyperparameters"]
        folds = result["cv"]["selection"]["best"]["folds"]
        return {
            "dataset_id": result["dataset_id"],
            "data_sha256": result["data_sha256"],
            "version_fingerprint": result["version_fingerprint"],
            "result_digest": result["result_digest"],
            "selected": {
                "strength": best["strength"],
                "l1_ratio": best["l1_ratio"],
                "metric": best["metric"],
                "mean": best["mean"],
                "std": best["std"],
            },
            "cv_folds": folds,
            "intercept": result["intercept"],
            "coefficient_count": len(result["coefficients"]),
            "coefficient_digest": digest(result["coefficients"]),
            "final_trajectory": result["trajectory_summary"],
            "stage_fingerprints": result["stage_fingerprints"],
            "empty_feature_codes": [
                item["code"] for item in result.get("empty_feature_errors", [])
            ],
        }
