"""Threaded application service shared by the HTTP workstation and CLI demos."""

from __future__ import annotations

import copy
import threading
import time
from pathlib import Path
from typing import Any, Iterable

import numpy as np

from .batch import Batch, ingest_batches
from .canonical import json_safe
from .config import DEFAULT_CONFIG, normalize_config
from .data import DatasetRegistry
from .errors import GracefulStop, WorkbenchError
from .stop import StopController
from .workbench import Workbench, analyze_invalidation


class WorkbenchService:
    def __init__(
        self,
        data_dir: str | Path,
        workdir_base: str | Path,
        *,
        dataset: str = "small",
        config: dict[str, Any] | None = None,
    ) -> None:
        self.registry = DatasetRegistry(data_dir)
        self.data_dir = Path(data_dir)
        self.workdir_base = Path(workdir_base)
        self.workdir_base.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._stop_controller: StopController | None = None
        self._state = "idle"
        self._progress: list[dict[str, Any]] = []
        self._last_error: dict[str, Any] | None = None
        self._last_stop: dict[str, Any] | None = None
        self._last_summary: dict[str, Any] | None = None
        self._started_monotonic: float | None = None
        self._elapsed_seconds: float | None = None
        self._dataset_name = dataset
        self._config = normalize_config(config)
        self._dataset = self.registry.load(dataset)

    @property
    def workdir(self) -> Path:
        return self.workdir_base / self._dataset_name

    def _workbench(self, dataset_name: str | None = None, config: dict[str, Any] | None = None) -> Workbench:
        name = dataset_name or self._dataset_name
        dataset = self.registry.load(name)
        cfg = normalize_config(config if config is not None else self._config)
        return Workbench(dataset, cfg, self.workdir_base / name)

    def datasets(self) -> list[dict[str, Any]]:
        return self.registry.summaries()

    def default_config(self) -> dict[str, Any]:
        return copy.deepcopy(DEFAULT_CONFIG)

    def session(self) -> dict[str, Any]:
        with self._lock:
            return {
                "dataset": self._dataset_name,
                "config": copy.deepcopy(self._config),
                "workdir": str(self.workdir),
                "state": self._state,
            }

    def set_session(self, *, dataset: str, config: dict[str, Any] | None = None) -> dict[str, Any]:
        with self._lock:
            if self._state in {"running", "stopping"}:
                raise WorkbenchError(
                    "E_BUSY",
                    "cannot change session while an evaluation is running",
                    stage="session",
                )
            loaded = self.registry.load(dataset)
            normalized = normalize_config(config if config is not None else self._config)
            self._dataset_name = dataset
            self._dataset = loaded
            self._config = normalized
            self._last_error = None
            self._last_stop = None
            return self.session()

    def start_run(
        self,
        *,
        dataset: str | None = None,
        config: dict[str, Any] | None = None,
        resume: bool = True,
        force: bool = False,
        batch_size: int | None = None,
        batch_order: str = "forward",
        current_batches: Iterable[np.ndarray | Batch] | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            if self._state in {"running", "stopping"}:
                raise WorkbenchError(
                    "E_BUSY",
                    "an evaluation is already running",
                    stage="service",
                )
            if dataset is not None:
                self._dataset_name = dataset
                self._dataset = self.registry.load(dataset)
            else:
                self._dataset = self.registry.load(self._dataset_name)
            if config is not None:
                self._config = normalize_config(config)
            if current_batches is not None:
                materialized_batches = list(current_batches)
                ingest_batches(
                    materialized_batches,
                    expected_columns=self._dataset.columns,
                    column_names=self._dataset.column_names,
                    expected_rows=self._dataset.rows_current,
                )
                current_batches = materialized_batches
            self._state = "running"
            self._last_error = None
            self._last_stop = None
            self._last_summary = None
            self._progress = []
            self._started_monotonic = time.monotonic()
            self._elapsed_seconds = None
            workbench = Workbench(self._dataset, self._config, self.workdir)
            stop = StopController(stop_file=self.workdir / "STOP", reset_stop_file=True)
            self._stop_controller = stop

            def on_progress(event: dict[str, Any]) -> None:
                with self._lock:
                    self._progress.append(event)

            def worker() -> None:
                try:
                    result = workbench.run(
                        resume=resume,
                        force=force,
                        stop=stop,
                        progress=on_progress,
                        current_batches=current_batches,
                        batch_size=batch_size,
                        batch_order=batch_order,
                    )
                    with self._lock:
                        self._state = "completed"
                        self._last_summary = copy.deepcopy(result["summary"])
                        self._elapsed_seconds = (
                            time.monotonic() - self._started_monotonic
                            if self._started_monotonic is not None
                            else None
                        )
                        self._progress.append({"event": "run_complete", "status": "completed"})
                except GracefulStop as exc:
                    with self._lock:
                        self._state = "stopped"
                        self._last_stop = exc.to_dict()
                        self._elapsed_seconds = (
                            time.monotonic() - self._started_monotonic
                            if self._started_monotonic is not None
                            else None
                        )
                        self._progress.append({"event": "run_stopped", **exc.to_dict()})
                except WorkbenchError as exc:
                    with self._lock:
                        self._state = "error"
                        self._last_error = json_safe(exc.to_dict())
                        self._elapsed_seconds = (
                            time.monotonic() - self._started_monotonic
                            if self._started_monotonic is not None
                            else None
                        )
                        self._progress.append(
                            {"event": "run_error", "error": self._last_error["error"]}
                        )
                except Exception as exc:  # pragma: no cover - defensive boundary
                    with self._lock:
                        self._state = "error"
                        self._last_error = {
                            "ok": False,
                            "error": {
                                "code": "E_IO",
                                "message": str(exc),
                                "stage": "service",
                                "column": None,
                                "location": "service.worker",
                                "details": {"type": type(exc).__name__},
                            },
                        }
                        self._progress.append({"event": "run_error", "error": self._last_error["error"]})

            thread = threading.Thread(target=worker, name="kde-drift-run", daemon=True)
            self._thread = thread
            thread.start()
            return self.status()

    def stop(self) -> dict[str, Any]:
        with self._lock:
            if self._state != "running" or self._stop_controller is None:
                return {"accepted": False, "state": self._state, "message": "no running evaluation"}
            self._state = "stopping"
            self._stop_controller.request_stop()
            self._progress.append({"event": "stop_requested"})
            return {"accepted": True, "state": "stopping"}

    def resume(self) -> dict[str, Any]:
        return self.start_run(resume=True)

    def status(self) -> dict[str, Any]:
        with self._lock:
            dataset_name = self._dataset_name
            config = copy.deepcopy(self._config)
            state = self._state
            last_error = copy.deepcopy(self._last_error)
            last_stop = copy.deepcopy(self._last_stop)
            last_summary = copy.deepcopy(self._last_summary)
            progress = copy.deepcopy(self._progress[-100:])
            elapsed = self._elapsed_seconds
            if state == "running" and self._started_monotonic is not None:
                elapsed = time.monotonic() - self._started_monotonic
        dataset = self.registry.load(dataset_name)
        workbench = Workbench(dataset, config, self.workdir_base / dataset_name)
        analysis = workbench.analyze()
        result = {
            "dataset": dataset_name,
            "config": config,
            "state": state,
            "elapsed_seconds": elapsed,
            "last_error": last_error,
            "last_stop": last_stop,
            "last_summary": last_summary,
            "progress": progress,
            "workdir": str(self.workdir_base / dataset_name),
            "dataset_summary": dataset.summary(),
            "analysis": analysis,
        }
        return json_safe(result)

    def invalidation(self) -> dict[str, Any]:
        status = self.status()
        return status["analysis"]

    def results(self, *, stage: str | None = None, include_full: bool = True) -> dict[str, Any]:
        with self._lock:
            dataset_name = self._dataset_name
            config = copy.deepcopy(self._config)
        dataset = self.registry.load(dataset_name)
        workbench = Workbench(dataset, config, self.workdir_base / dataset_name)
        analysis = workbench.analyze()
        response: dict[str, Any] = {
            "dataset": dataset_name,
            "analysis": analysis,
            "summary": None,
            "payloads": {},
        }
        if analysis["earliest_invalid_stage"] is not None:
            return json_safe(response)
        payloads = workbench.require_complete_payloads()
        response["summary"] = workbench._summary(dataset, payloads, analysis["config_version"])
        if stage is None or stage == "all":
            selected = payloads if include_full else {}
        else:
            if stage not in payloads:
                raise WorkbenchError(
                    "E_NOT_FOUND", f"unknown stage: {stage}", stage=stage, location="results.stage"
                )
            selected = {stage: payloads[stage]}
        response["payloads"] = selected
        return json_safe(response)

    def export(self, kind: str) -> bytes:
        with self._lock:
            dataset_name = self._dataset_name
            config = copy.deepcopy(self._config)
        dataset = self.registry.load(dataset_name)
        workbench = Workbench(dataset, config, self.workdir_base / dataset_name)
        return workbench.export_bytes(kind)

    def wait(self, timeout: float | None = None) -> None:
        thread = self._thread
        if thread is not None:
            thread.join(timeout)


ERROR_CATALOG = [
    {
        "code": "E_NUMERIC",
        "meaning": "non-finite or unusable numeric kernel parameter; includes stage, column, and location",
    },
    {
        "code": "E_COLUMN_MISMATCH",
        "meaning": "reference/current column count or names differ; details list each difference",
    },
    {
        "code": "E_INVALID_BATCH",
        "meaning": "empty batch, wrong feature dimension, or malformed batch metadata",
    },
    {
        "code": "E_STALE_CHECKPOINT",
        "meaning": "a concurrent, stale, or duplicate stage publication was fenced out",
    },
    {
        "code": "E_INVALID_STAGE",
        "meaning": "required upstream checkpoint/artifact is absent or inconsistent",
    },
]
