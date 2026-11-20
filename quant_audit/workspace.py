"""Workspace orchestration for imports, calibration, queries and rollback."""

from __future__ import annotations

import copy
import shutil
from pathlib import Path
from typing import Any

from .canonical import (
    atomic_write_bytes,
    atomic_write_json,
    read_json,
)
from .config import QuantConfig, load_config
from .data import CalibrationBundle, WeightBundle, load_calibration, load_weights
from .engine import AuditEngine
from .errors import (
    E_INVALID_INPUT,
    E_NO_VALID_VERSION,
    E_STALE_VERSION,
    E_WORKSPACE_EXISTS,
    AuditError,
)
from .store import AuditStore


class Workspace:
    schema_version = 1

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.metadata_path = self.root / "workspace.json"
        self.audit_root = self.root / "audit"
        self.anchor_path = self.root / "audit.anchor.json"
        self.data_dir = self.root / "data"
        self.store = AuditStore(self.audit_root, self.anchor_path)

    @classmethod
    def initialize(
        cls,
        root: str | Path,
        weights_path: str | Path,
        calibration_path: str | Path,
        config_path: str | Path,
        *,
        force: bool = False,
    ) -> Workspace:
        workspace = cls(root)
        if workspace.metadata_path.exists() and not force:
            raise AuditError(
                E_WORKSPACE_EXISTS,
                "workspace already contains imported inputs; use --force to replace them",
                workspace=str(workspace.root),
            )
        workspace._ensure_writable()
        workspace.root.mkdir(parents=True, exist_ok=True)
        workspace.data_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_bytes(workspace.data_dir / "weights.npz", Path(weights_path).read_bytes())
        atomic_write_bytes(workspace.data_dir / "calibration.npy", Path(calibration_path).read_bytes())
        atomic_write_bytes(workspace.root / "quant_config.yaml", Path(config_path).read_bytes())
        metadata = {
            "schema_version": cls.schema_version,
            "weights": "data/weights.npz",
            "calibration": "data/calibration.npy",
            "config": "quant_config.yaml",
        }
        atomic_write_json(workspace.metadata_path, metadata)
        if force and workspace.audit_root.exists():
            shutil.rmtree(workspace.audit_root)
        if force:
            workspace.anchor_path.unlink(missing_ok=True)
        workspace.store.initialize()
        return workspace

    @classmethod
    def open(cls, root: str | Path) -> Workspace:
        workspace = cls(root)
        if not workspace.metadata_path.exists():
            raise AuditError(
                E_INVALID_INPUT,
                "workspace is not initialized; run the import/init command first",
                workspace=str(workspace.root),
            )
        return workspace

    def metadata(self) -> dict[str, Any]:
        metadata = read_json(self.metadata_path)
        if metadata.get("schema_version") != self.schema_version:
            raise AuditError(E_INVALID_INPUT, "unsupported workspace metadata schema")
        return metadata

    def import_inputs(
        self,
        weights_path: str | Path,
        calibration_path: str | Path,
        config_path: str | Path,
    ) -> dict[str, Any]:
        replaced = Workspace.initialize(
            self.root,
            weights_path,
            calibration_path,
            config_path,
            force=True,
        )
        return replaced.metadata()

    def calibrate(
        self,
        *,
        mode: str = "full",
        base_version: int | None = None,
        config_path: str | Path | None = None,
        calibration_path: str | Path | None = None,
    ) -> dict[str, Any]:
        if mode not in {"full", "incremental"}:
            raise AuditError(E_INVALID_INPUT, "calibration mode must be full or incremental", mode=mode)
        self._ensure_writable()
        engine, source_info = self._load_engine(config_path, calibration_path)
        publication_base = self.store.latest_version()

        incremental_base: int | None = None
        if mode == "incremental":
            if publication_base == 0:
                raise AuditError(
                    E_NO_VALID_VERSION,
                    "incremental calibration requires a previously published version",
                )
            active_version = self.store.active_version()
            if base_version is not None and base_version != active_version:
                raise AuditError(
                    E_STALE_VERSION,
                    "incremental base version is not the current active version",
                    base_version=base_version,
                    active_version=active_version,
                )
            incremental_base = base_version if base_version is not None else active_version
            if incremental_base <= 0:
                raise AuditError(
                    E_NO_VALID_VERSION,
                    "incremental calibration needs a budget-compliant base version",
                )
            previous_record, previous_arrays = self.store.load_stage(incremental_base)
            engine_result = engine.run_incremental(
                previous_record,
                previous_arrays,
            )
        else:
            engine_result = engine.run_full()

        execution = {
            "mode": mode,
            "incremental_base_version": incremental_base,
            "earliest_recomputed_layer": engine_result.earliest_recomputed_layer,
            "reused_layers": list(engine_result.reused_layers),
            "recomputed_layers": list(engine_result.recomputed_layers),
            "incremental_verified_against_full": mode == "incremental",
        }
        inputs = {
            "layer_names": list(engine.layer_names),
            "weights_fingerprint": engine.weights.fingerprint,
            "calibration_fingerprint": engine.calibration.fingerprint,
            "calibration_source_shape": list(engine.calibration.source_shape),
            "calibration_selected_samples": int(engine.calibration.samples.shape[0]),
            "config_fingerprint": engine.config.fingerprint,
            "weights_source": source_info["weights"],
            "calibration_source": source_info["calibration"],
            "config_source": source_info["config"],
        }
        published = self.store.publish_calibration(
            base_version=publication_base,
            result=engine_result.result,
            arrays=engine_result.arrays,
            inputs=inputs,
            layer_fingerprints=engine_result.layer_fingerprints,
            budget_config_fingerprint=engine.config.budget_fingerprint,
            execution=execution,
        )
        record = published["record"]
        return {
            "published_version": int(record["version"]),
            "active_version": int(published["active_version"]),
            "rolled_back": not bool(record["usable"]),
            "usable": bool(record["usable"]),
            "result_summary": record["result_summary"],
            "record": record,
            "execution": copy.deepcopy(execution),
        }

    def query(self, version: int | None = None) -> dict[str, Any]:
        record = self.store.load_record(version)
        active_version = self.store.active_version()
        return {
            "version": int(record["version"]),
            "active_version": active_version,
            "usable": bool(record["usable"]),
            "is_active": int(record["version"]) == active_version,
            "result_summary": record["result_summary"],
            "over_budget_layers": list(record.get("over_budget_layers", [])),
            "result": copy.deepcopy(record["result"]),
            "inputs": copy.deepcopy(record["inputs"]),
            "execution": copy.deepcopy(record.get("execution", {})),
        }

    def layer_errors(self, version: int | None = None) -> list[dict[str, Any]]:
        query = self.query(version)
        attribution_by_name = {
            layer["name"]: layer for layer in query["result"]["attribution"]["layers"]
        }
        budget_by_name = {
            layer["name"]: layer for layer in query["result"]["budget"]["per_layer"]
        }
        return [
            {
                "name": layer["name"],
                "error": layer["error"],
                "attribution": attribution_by_name[layer["name"]],
                "budget": budget_by_name[layer["name"]],
            }
            for layer in query["result"]["layers"]
        ]

    def history(self) -> dict[str, Any]:
        return self.store.history()

    def rollback(self, to_version: int | None = None) -> dict[str, Any]:
        result = self.store.rollback(to_version)
        record = result["record"]
        return {
            "previous_active_version": result["previous_active_version"],
            "active_version": result["active_version"],
            "version": int(record["version"]),
            "result_summary": record["result_summary"],
            "result": copy.deepcopy(record["result"]),
        }

    def _load_engine(
        self,
        config_path: str | Path | None,
        calibration_path: str | Path | None,
    ) -> tuple[AuditEngine, dict[str, str]]:
        metadata = self.metadata()
        resolved_weights = self._resolve(metadata["weights"])
        resolved_calibration = (
            Path(calibration_path).resolve()
            if calibration_path is not None
            else self._resolve(metadata["calibration"])
        )
        resolved_config = (
            Path(config_path).resolve()
            if config_path is not None
            else self._resolve(metadata["config"])
        )
        weights: WeightBundle = load_weights(resolved_weights)
        config: QuantConfig = load_config(resolved_config, weights.layer_names)
        calibration: CalibrationBundle = load_calibration(
            resolved_calibration,
            config.calibration_ratio,
        )
        engine = AuditEngine(config=config, weights=weights, calibration=calibration)
        return engine, {
            "weights": self._display_source(resolved_weights),
            "calibration": self._display_source(resolved_calibration),
            "config": self._display_source(resolved_config),
        }

    def _resolve(self, relative: str) -> Path:
        path = (self.root / relative).resolve()
        try:
            path.relative_to(self.root)
        except ValueError as exc:
            raise AuditError(E_INVALID_INPUT, "workspace metadata path escapes workspace root", path=relative) from exc
        return path

    def _display_source(self, path: Path) -> str:
        try:
            return path.relative_to(self.root).as_posix()
        except ValueError:
            return path.name

    def _ensure_writable(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        probe = self.root / ".write_probe"
        atomic_write_bytes(probe, b"ok")
        probe.unlink(missing_ok=True)

