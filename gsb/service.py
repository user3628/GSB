"""Application service shared by the CLI, HTTP API and demo."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from .data import list_datasets, load_dataset
from .errors import WorkbenchError
from .pipeline import Pipeline
from .workspace import STAGES, Workspace


class WorkbenchService:
    def __init__(
        self,
        project_root: Path,
        *,
        state_dir: Path | None = None,
        data_dir: Path | None = None,
    ):
        self.project_root = Path(project_root).resolve()
        self.workspace = Workspace(self.project_root, state_dir=state_dir, data_dir=data_dir)
        self._thread: threading.Thread | None = None
        self._thread_lock = threading.Lock()
        self._last_background_result: dict[str, Any] | None = None
        self._last_background_error: dict[str, Any] | None = None

    @property
    def data_dir(self) -> Path:
        return self.workspace.data_dir

    def datasets(self) -> dict[str, Any]:
        listing = list_datasets(self.data_dir)
        current = self.workspace.status().get("dataset")
        return {**listing, "current": current}

    def import_dataset(self, key: str) -> dict[str, Any]:
        self._ensure_not_training()
        dataset = load_dataset(key, project_root=self.project_root, data_dir=self.data_dir)
        status = self.workspace.import_dataset(dataset)
        return {"ok": True, "dataset": dataset.summary(), "status": status}

    def load_current_dataset(self):
        status = self.workspace.status()
        dataset = status.get("dataset")
        if dataset is None:
            raise WorkbenchError("E_INVALID_STAGE", "no dataset has been imported", {"stage": "binning"})
        return load_dataset(dataset["key"], project_root=self.project_root, data_dir=self.data_dir)

    def status(self) -> dict[str, Any]:
        status = self.workspace.status()
        with self._thread_lock:
            alive = bool(self._thread and self._thread.is_alive())
            last_error = self._last_background_error
            last_result = self._last_background_result
        status["service"] = {
            "training": alive,
            "background_thread": bool(self._thread),
            "last_background_error": last_error,
            "last_background_result": last_result,
        }
        return status

    def get_config(self) -> dict[str, Any]:
        return {"ok": True, "config": self.workspace.config.to_dict(), "status": self.workspace.status()}

    def set_config(self, patch: dict[str, Any]) -> dict[str, Any]:
        self._ensure_not_training()
        result = self.workspace.set_config(patch)
        return {"ok": True, **result}

    def build(
        self,
        *,
        resume: bool = False,
        until_stage: str = "diagnostics",
        stop_after_rounds: int | None = None,
    ) -> dict[str, Any]:
        dataset = self.load_current_dataset()
        pipeline = Pipeline(self.workspace, dataset, stop_after_rounds=stop_after_rounds)
        result = pipeline.run_all(resume=resume, until_stage=until_stage)
        result["demand"] = {"resume": resume, "until_stage": until_stage}
        return result

    def _ensure_not_training(self) -> None:
        with self._thread_lock:
            if self._thread and self._thread.is_alive():
                raise WorkbenchError(
                    "E_TRAIN_BUSY",
                    "another training run is active in this process",
                    {"hint": "request stop or wait for completion"},
                )

    def train_background(
        self,
        *,
        resume: bool = False,
        until_stage: str = "diagnostics",
        stop_after_rounds: int | None = None,
    ) -> dict[str, Any]:
        with self._thread_lock:
            if self._thread and self._thread.is_alive():
                raise WorkbenchError("E_TRAIN_BUSY", "another training run is active in this process")
            self._last_background_result = None
            self._last_background_error = None

        def runner() -> None:
            try:
                result = self.build(resume=resume, until_stage=until_stage, stop_after_rounds=stop_after_rounds)
                with self._thread_lock:
                    self._last_background_result = {
                        "run_digest": result.get("run_digest"),
                        "status": result.get("status", {}).get("overall"),
                        "stopped": bool(result.get("stopped")),
                    }
            except WorkbenchError as exc:
                with self._thread_lock:
                    self._last_background_error = exc.to_dict()["error"]
            except Exception as exc:  # pragma: no cover - defensive background boundary
                with self._thread_lock:
                    self._last_background_error = {"code": "E_INTERNAL", "message": str(exc)}

        thread = threading.Thread(target=runner, name="gsb-training", daemon=True)
        with self._thread_lock:
            self._thread = thread
        thread.start()
        return {
            "ok": True,
            "accepted": True,
            "resume": resume,
            "until_stage": until_stage,
            "status": self.workspace.status(),
        }

    def wait_for_training(self, timeout: float | None = None) -> dict[str, Any]:
        with self._thread_lock:
            thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout)
        return self.status()

    def request_stop(self, reason: str = "http_or_cli_request") -> dict[str, Any]:
        self.workspace.request_stop(reason)
        return {"ok": True, "stop_requested": True, "scope": "finish_current_boosting_round", "status": self.workspace.status()}

    def resume(self, *, until_stage: str = "diagnostics") -> dict[str, Any]:
        return self.build(resume=True, until_stage=until_stage)

    def reliability(self) -> dict[str, Any]:
        _, diagnostics = self.workspace.read_stage_payload("diagnostics")
        return {"ok": True, "diagnostics": diagnostics, "status": self.workspace.status()}

    def diagnostics(self) -> dict[str, Any]:
        return self.reliability()

    def compare_batches(self, batch_sizes: list[int]) -> dict[str, Any]:
        dataset = self.load_current_dataset()
        pipeline = Pipeline(self.workspace, dataset)
        result = pipeline.compare_batch_modes(batch_sizes)
        return {"ok": True, **result}

    def export(self, out_dir: Path | None = None) -> dict[str, Any]:
        dataset = self.load_current_dataset()
        pipeline = Pipeline(self.workspace, dataset)
        destination = out_dir if out_dir is not None else self.project_root / "out"
        result = pipeline.write_exports(destination)
        return {"ok": True, **result}

    def export_report(self) -> dict[str, Any]:
        dataset = self.load_current_dataset()
        return {"ok": True, "report": Pipeline(self.workspace, dataset).export_report()}

    def verify_incremental(self) -> dict[str, Any]:
        dataset = self.load_current_dataset()
        result = Pipeline(self.workspace, dataset).compare_with_full_rebuild()
        return {"ok": True, **result}

    def reconfigure(
        self,
        patch: dict[str, Any],
        *,
        rebuild: bool = True,
        verify_incremental: bool = True,
    ) -> dict[str, Any]:
        self._ensure_not_training()
        config_result = self.workspace.set_config(patch)
        response: dict[str, Any] = {"ok": True, "config": config_result["config"], "changes": config_result["changes"], "invalidation": config_result["status"]}
        if rebuild:
            build_result = self.build(resume=True)
            response["build"] = {"run_digest": build_result.get("run_digest"), "status": build_result.get("status", {}).get("overall")}
        if verify_incremental:
            response["verification"] = self.verify_incremental()
        response["status"] = self.workspace.status()
        return response

    def ensure_complete(self) -> None:
        status = self.workspace.status()
        missing = [stage for stage in STAGES if status["stages"][stage]["status"] != "valid"]
        if missing:
            raise WorkbenchError("E_INVALID_STAGE", "all stages must be valid", {"invalid_or_missing": missing})
