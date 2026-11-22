from __future__ import annotations

import csv
import io
import multiprocessing as mp
import os
import re
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from .budget import plan_admitted_specs, plan_schedule
from .checkpoints import CheckpointStore, make_record
from .config import load_config, load_versions_baseline, update_config_file
from .dataset import (
    ShardSpec,
    build_shard_specs,
    infer_type_schema,
    load_manifest,
    load_table,
)
from .encoding import encode_worker
from .errors import WorkbenchError
from .selection import project_matrix, select_features
from .util import (
    atomic_write_bytes,
    atomic_write_json,
    atomic_write_npz,
    atomic_write_text,
    digest_object as util_digest_object,
    load_npz,
    read_json,
    stable_matrix_digest,
    stable_vector_digest,
)


STAGE_ORDER = ("type_inference", "encoding", "selection", "aggregate")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _safe_name(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", value)
    return cleaned or "tier"


def _csv_number(value: float) -> str:
    number = float(value)
    if number == 0.0:
        return "0"
    return f"{number:.15g}"


def _csv_bytes(header: list[str], rows: list[list[float]]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(header)
    for row in rows:
        writer.writerow([_csv_number(value) for value in row])
    return buffer.getvalue().encode("utf-8")


def _label_csv_bytes(labels: np.ndarray) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(["label"])
    for value in labels.tolist():
        writer.writerow([str(int(value))])
    return buffer.getvalue().encode("utf-8")


class Workbench:
    def __init__(
        self,
        *,
        root: Path | str | None = None,
        data_dir: Path | str | None = None,
        config_path: Path | str | None = None,
        manifest_path: Path | str | None = None,
        state_dir: Path | str | None = None,
        export_dir: Path | str | None = None,
    ) -> None:
        self.root = Path(root or Path.cwd()).resolve()
        self.data_dir = Path(data_dir or self.root / "data").resolve()
        self.config_path = Path(config_path or self.data_dir / "config.yaml").resolve()
        self.manifest_path = Path(manifest_path or self.data_dir / "manifest.json").resolve()
        self.state_dir = Path(state_dir or self.root / ".workbench").resolve()
        self.export_dir = Path(export_dir or self.root / "exports").resolve()
        self.artifacts_dir = self.state_dir / "artifacts"
        self.store = CheckpointStore(self.state_dir)
        self.versions_path = self.state_dir / "versions.json"
        self.runtime_path = self.state_dir / "runtime.json"
        self._run_lock = threading.Lock()
        self._control_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Context, versions, and status
    # ------------------------------------------------------------------
    def _load_context(self) -> dict[str, Any]:
        config = load_config(self.config_path)
        manifest = load_manifest(self.manifest_path)
        specs = build_shard_specs(manifest, self.data_dir)
        plan = plan_schedule(specs, config)
        admitted = plan_admitted_specs(specs, plan)
        admitted_indices = {spec.shard_index for spec in admitted}
        tier_rank = {str(tier): index for index, tier in enumerate(manifest.get("tiers", {}))}
        tiers_with_admitted = sorted(
            {spec.tier for spec in admitted},
            key=lambda tier: (tier_rank.get(tier, len(tier_rank)), tier),
        )
        manifest_hash = util_digest_object(manifest)
        shard_inputs = [
            {
                "file": spec.file,
                "tier": spec.tier,
                "shard_index": spec.shard_index,
                "content_sha256": spec.content_sha256,
            }
            for spec in sorted(specs, key=lambda item: item.shard_index)
        ]
        data_fingerprint = util_digest_object(shard_inputs)
        type_version = util_digest_object(
            {
                "stage": "type_inference",
                "manifest": manifest_hash,
                "shards": shard_inputs,
            }
        )
        encoding_version = util_digest_object(
            {
                "stage": "encoding",
                "type_inference": type_version,
                "config": config["encoding"],
                "seed": config["seed"],
                "admitted_shards": sorted(admitted_indices),
            }
        )
        selection_version = util_digest_object(
            {
                "stage": "selection",
                "encoding": encoding_version,
                "config": config["selection"],
            }
        )
        aggregate_version = util_digest_object(
            {
                "stage": "aggregate",
                "selection": selection_version,
                "budget": config["budget"],
                "admission_plan": plan["plan_digest"],
            }
        )
        stage_versions = {
            "type_inference": type_version,
            "encoding": encoding_version,
            "selection": selection_version,
            "aggregate": aggregate_version,
        }
        pipeline_version = util_digest_object({"stage_versions": stage_versions})
        baseline = load_versions_baseline(self.versions_path)
        affected = self._affected_stage(baseline, stage_versions)
        return {
            "config": config,
            "manifest": manifest,
            "specs": specs,
            "specs_by_index": {spec.shard_index: spec for spec in specs},
            "plan": plan,
            "admitted_specs": admitted,
            "admitted_indices": admitted_indices,
            "tiers_with_admitted": tiers_with_admitted,
            "stage_versions": stage_versions,
            "pipeline_version": pipeline_version,
            "manifest_hash": manifest_hash,
            "data_fingerprint": data_fingerprint,
            "type_input_fingerprint": util_digest_object(
                {"manifest": manifest_hash, "data": data_fingerprint}
            ),
            "affected_earliest_stage": affected,
            "baseline": baseline,
        }

    @staticmethod
    def _affected_stage(
        baseline: dict[str, Any] | None,
        stage_versions: dict[str, str],
    ) -> str | None:
        if not baseline:
            return "type_inference"
        previous = baseline.get("stage_versions", {})
        for stage in STAGE_ORDER:
            if previous.get(stage) != stage_versions[stage]:
                return stage
        return None

    def _artifact_path(self, relative: str) -> Path:
        relative_path = Path(relative)
        if relative_path.is_absolute():
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Artifact path must be relative to the state directory",
                {"path": relative},
            )
        path = (self.state_dir / relative_path).resolve()
        try:
            path.relative_to(self.state_dir)
        except ValueError as exc:
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Artifact path escapes the state directory",
                {"path": relative},
            ) from exc
        return path

    def _store_relative(self, path: Path) -> str:
        return path.resolve().relative_to(self.state_dir).as_posix()

    @staticmethod
    def _record_dependency(stage: str, stage_version: str, scope: str) -> dict[str, str]:
        return {"stage": stage, "stage_version": stage_version, "scope": scope}

    def _publish(self, context: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
        expected = int(context["next_sequence"])
        published = self.store.publish(record, expected_sequence=expected)
        context["next_sequence"] = expected + 1
        return published

    def _record_artifact_exists(self, record: dict[str, Any]) -> bool:
        payload = record.get("payload", {})
        relative_paths = []
        for key in ("artifact_path", "summary_path", "data_path"):
            if payload.get(key):
                relative_paths.append(str(payload[key]))
        if not relative_paths:
            return False
        for relative in relative_paths:
            try:
                if not self._artifact_path(relative).is_file():
                    return False
            except WorkbenchError:
                return False
        return True

    def status(self) -> dict[str, Any]:
        started = time.perf_counter()
        context = self._load_context()
        records = self.store.list_records()
        stage_versions = context["stage_versions"]
        admitted = context["admitted_specs"]
        tiers = context["tiers_with_admitted"]
        type_record = self.store.latest_record(
            stage="type_inference",
            stage_version=stage_versions["type_inference"],
            scope="ALL",
            records=records,
        )
        encoding_records = {
            spec.shard_index: self.store.latest_record(
                stage="encoding",
                stage_version=stage_versions["encoding"],
                scope=f"shard:{spec.shard_index}",
                records=records,
            )
            for spec in admitted
        }
        encoding_stage_record = self.store.latest_record(
            stage="encoding",
            stage_version=stage_versions["encoding"],
            scope="STAGE",
            records=records,
        )
        missing_encoding = [
            spec.shard_index
            for spec in admitted
            if encoding_records[spec.shard_index] is None
            or not self._record_artifact_exists(encoding_records[spec.shard_index])
        ]
        selection_records = {
            tier: self.store.latest_record(
                stage="selection",
                stage_version=stage_versions["selection"],
                scope=f"tier:{tier}",
                records=records,
            )
            for tier in tiers
        }
        selection_stage_record = self.store.latest_record(
            stage="selection",
            stage_version=stage_versions["selection"],
            scope="STAGE",
            records=records,
        )
        missing_selection = [
            tier
            for tier in tiers
            if selection_records[tier] is None
            or not self._record_artifact_exists(selection_records[tier])
        ]
        aggregate_record = self.store.latest_record(
            stage="aggregate",
            stage_version=stage_versions["aggregate"],
            scope="ALL",
            records=records,
        )
        aggregate_complete = bool(
            aggregate_record is not None and self._record_artifact_exists(aggregate_record)
        )
        type_complete = bool(
            type_record is not None and self._record_artifact_exists(type_record)
        )
        encoding_complete = bool(
            type_complete
            and not missing_encoding
            and encoding_stage_record is not None
            and self._record_artifact_exists(encoding_stage_record)
        )
        selection_complete = bool(
            encoding_complete
            and not missing_selection
            and selection_stage_record is not None
            and self._record_artifact_exists(selection_stage_record)
        )
        runtime = self._read_runtime()
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        latency_budget = float(context["config"]["budget"]["query_latency_ms"])
        result = {
            "ok": True,
            "status": runtime.get("status", "idle") if runtime else (
                "complete" if aggregate_complete else "idle"
            ),
            "pipeline_version": context["pipeline_version"],
            "stage_versions": stage_versions,
            "affected_earliest_stage": context["affected_earliest_stage"],
            "baseline": {
                "pipeline_version": (context["baseline"] or {}).get("pipeline_version"),
                "stage_versions": (context["baseline"] or {}).get("stage_versions", {}),
            },
            "stages": {
                "type_inference": {
                    "complete": type_complete,
                    "record_sequence": type_record.get("sequence") if type_record else None,
                    "artifact_digest": type_record.get("artifact_digest") if type_record else None,
                },
                "encoding": {
                    "complete": encoding_complete,
                    "stage_record_sequence": (
                        encoding_stage_record.get("sequence")
                        if encoding_stage_record
                        else None
                    ),
                    "completed_shards": len(admitted) - len(missing_encoding),
                    "total_shards": len(admitted),
                    "missing_shards": missing_encoding,
                },
                "selection": {
                    "complete": selection_complete,
                    "stage_record_sequence": (
                        selection_stage_record.get("sequence")
                        if selection_stage_record
                        else None
                    ),
                    "completed_tiers": len(tiers) - len(missing_selection),
                    "total_tiers": len(tiers),
                    "missing_tiers": missing_selection,
                },
                "aggregate": {
                    "complete": aggregate_complete,
                    "record_sequence": aggregate_record.get("sequence") if aggregate_record else None,
                    "result_digest": aggregate_record.get("artifact_digest") if aggregate_record else None,
                },
            },
            "budget": context["plan"],
            "checkpoints": self.store.sequence_summary(),
            "runtime": runtime,
            "query": {
                "latency_ms": round(elapsed_ms, 3),
                "budget_ms": latency_budget,
                "within_budget": elapsed_ms <= latency_budget,
            },
        }
        return result

    def version_report(self) -> dict[str, Any]:
        status = self.status()
        return {
            "ok": True,
            "pipeline_version": status["pipeline_version"],
            "stage_versions": status["stage_versions"],
            "affected_earliest_stage": status["affected_earliest_stage"],
            "baseline": status["baseline"],
            "stages": status["stages"],
            "checkpoints": status["checkpoints"],
            "query": status["query"],
        }

    def budget_report(self) -> dict[str, Any]:
        started = time.perf_counter()
        context = self._load_context()
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        latency_budget = float(context["config"]["budget"]["query_latency_ms"])
        return {
            "ok": True,
            "budget": context["plan"],
            "sampling_plan": {
                "admitted": [entry["shard_index"] for entry in context["plan"]["admitted"]],
                "unprocessed": context["plan"]["unprocessed"],
            },
            "query": {
                "latency_ms": round(elapsed_ms, 3),
                "budget_ms": latency_budget,
                "within_budget": elapsed_ms <= latency_budget,
            },
        }

    # ------------------------------------------------------------------
    # Runtime and controls
    # ------------------------------------------------------------------
    def _read_runtime(self) -> dict[str, Any] | None:
        if not self.runtime_path.exists():
            return None
        try:
            value = read_json(self.runtime_path)
        except (OSError, ValueError):
            return None
        return value if isinstance(value, dict) else None

    def _write_runtime(
        self,
        status: str,
        *,
        stage: str | None = None,
        progress: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
    ) -> None:
        previous = self._read_runtime() or {}
        payload = {
            "schema_version": 1,
            "status": status,
            "stage": stage,
            "pid": os.getpid(),
            "stop_requested": self.store.stop_requested(),
            "stop_reason": self.store.stop_reason(),
            "progress": progress or previous.get("progress", {}),
            "last_error": error,
            "updated_at": _utc_now(),
        }
        atomic_write_json(self.runtime_path, payload)

    def request_stop(self, reason: str = "user_requested") -> dict[str, Any]:
        with self._control_lock:
            self.store.request_stop(reason)
            runtime = self._read_runtime() or {}
            runtime["stop_requested"] = True
            runtime["stop_reason"] = reason
            runtime["updated_at"] = _utc_now()
            if runtime.get("status") not in {"running"}:
                runtime["status"] = runtime.get("status", "idle")
            atomic_write_json(self.runtime_path, runtime)
        return {
            "ok": True,
            "stop_requested": True,
            "reason": reason,
            "stop_file": str(self.store.stop_path),
        }

    def clear_stop(self) -> None:
        with self._control_lock:
            self.store.clear_stop()

    # ------------------------------------------------------------------
    # Configuration and sample import
    # ------------------------------------------------------------------
    def get_config(self) -> dict[str, Any]:
        return load_config(self.config_path)

    def update_config(self, changes: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(changes, dict) or not changes:
            raise WorkbenchError(
                "E_CONFIG_INVALID",
                "Configuration changes must be a non-empty object",
            )
        updated = update_config_file(self.config_path, changes)
        return {
            "ok": True,
            "config": updated,
            "path": str(self.config_path),
        }

    def import_samples(self, source_dir: Path | str) -> dict[str, Any]:
        source_dir = Path(source_dir).resolve()
        source_manifest_path = source_dir / "manifest.json"
        manifest = load_manifest(source_manifest_path)
        source_config_name = str(manifest.get("config_file", "config.yaml"))
        source_config_path = source_dir / source_config_name
        if not source_config_path.is_file():
            raise WorkbenchError(
                "E_NOT_FOUND",
                "Imported sample configuration was not found",
                {"path": str(source_config_path)},
            )
        load_config(source_config_path)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        copied: list[str] = []
        for entry in manifest["files"]:
            relative_name = str(entry.get("file", ""))
            source_path = (source_dir / relative_name).resolve()
            try:
                source_path.relative_to(source_dir)
            except ValueError as exc:
                raise WorkbenchError(
                    "E_INPUT_INVALID",
                    "Imported shard path escapes the source directory",
                    {"file": relative_name},
                ) from exc
            if not source_path.is_file():
                raise WorkbenchError(
                    "E_NOT_FOUND",
                    "Imported shard CSV was not found",
                    {"path": str(source_path)},
                )
        for entry in manifest["files"]:
            relative_name = Path(str(entry["file"]))
            if relative_name.is_absolute() or ".." in relative_name.parts:
                raise WorkbenchError(
                    "E_INPUT_INVALID",
                    "Imported shard file name is unsafe",
                    {"file": str(entry["file"])},
                )
            source_path = source_dir / relative_name
            target_path = self.data_dir / relative_name
            atomic_write_bytes(target_path, source_path.read_bytes())
            copied.append(relative_name.as_posix())
        manifest_target = self.data_dir / "manifest.json"
        atomic_write_bytes(manifest_target, source_manifest_path.read_bytes())
        config_target = self.data_dir / "config.yaml"
        atomic_write_bytes(config_target, source_config_path.read_bytes())
        normalized = load_config(config_target)
        return {
            "ok": True,
            "source_dir": str(source_dir),
            "data_dir": str(self.data_dir),
            "copied_files": copied,
            "manifest_path": str(manifest_target),
            "config_path": str(config_target),
            "config_digest": util_digest_object(normalized),
        }

    # ------------------------------------------------------------------
    # Pipeline execution
    # ------------------------------------------------------------------
    def _ensure_type_inference(
        self,
        context: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any], bool]:
        stage_version = context["stage_versions"]["type_inference"]
        record = self.store.latest_record(
            stage="type_inference",
            stage_version=stage_version,
            scope="ALL",
        )
        if record is not None:
            schema = self._load_type_artifact(record)
            return schema, record, True
        self._write_runtime("running", stage="type_inference")
        tables = {spec.shard_index: load_table(spec) for spec in context["specs"]}
        schema = infer_type_schema(context["manifest"], context["specs"], tables)
        artifact_path = self._artifact_path(
            f"artifacts/type_inference/{stage_version}.json"
        )
        atomic_write_json(artifact_path, schema)
        record = make_record(
            stage="type_inference",
            scope="ALL",
            pipeline_version=context["pipeline_version"],
            stage_version=stage_version,
            input_fingerprint=context["type_input_fingerprint"],
            config_version=context["pipeline_version"],
            artifact_digest=schema["schema_digest"],
            payload={"artifact_path": self._store_relative(artifact_path)},
        )
        record = self._publish(context, record)
        return schema, record, False

    def _load_type_artifact(self, record: dict[str, Any]) -> dict[str, Any]:
        relative = record.get("payload", {}).get("artifact_path")
        if not relative:
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Type inference checkpoint has no artifact path",
                {"record_id": record.get("record_id")},
            )
        path = self._artifact_path(relative)
        if not path.is_file():
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Type inference artifact is missing",
                {"path": str(path), "record_id": record.get("record_id")},
            )
        try:
            schema = read_json(path)
        except (OSError, ValueError) as exc:
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Type inference artifact is unreadable",
                {"path": str(path), "reason": str(exc)},
            ) from exc
        check = dict(schema)
        claimed = check.pop("schema_digest", None)
        actual = util_digest_object(check)
        if claimed != actual or claimed != record.get("artifact_digest"):
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Type inference artifact digest does not match its checkpoint",
                {"path": str(path), "record_id": record.get("record_id")},
            )
        return schema

    def _encoding_record(
        self,
        context: dict[str, Any],
        spec: ShardSpec,
    ) -> dict[str, Any] | None:
        return self.store.latest_record(
            stage="encoding",
            stage_version=context["stage_versions"]["encoding"],
            scope=f"shard:{spec.shard_index}",
        )

    def _load_encoding_artifact(
        self,
        context: dict[str, Any],
        spec: ShardSpec,
        record: dict[str, Any],
    ) -> dict[str, Any]:
        payload = record.get("payload", {})
        summary_path = payload.get("summary_path")
        data_path = payload.get("data_path")
        if not summary_path or not data_path:
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Encoding checkpoint is missing artifact paths",
                {"record_id": record.get("record_id")},
            )
        summary_file = self._artifact_path(summary_path)
        data_file = self._artifact_path(data_path)
        if not summary_file.is_file() or not data_file.is_file():
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Encoding checkpoint artifact is missing",
                {"record_id": record.get("record_id")},
            )
        try:
            summary = read_json(summary_file)
        except (OSError, ValueError) as exc:
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Encoding summary is unreadable",
                {"path": str(summary_file), "reason": str(exc)},
            ) from exc
        check = dict(summary)
        claimed = check.pop("encoding_digest", None)
        if claimed != record.get("artifact_digest") or claimed != util_digest_object(check):
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Encoding summary digest does not match its checkpoint",
                {"record_id": record.get("record_id"), "summary_path": summary_path},
            )
        if int(summary.get("shard_index", -1)) != spec.shard_index:
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Encoding artifact belongs to a different shard",
                {"expected": spec.shard_index, "observed": summary.get("shard_index")},
            )
        if summary.get("input_sha256") != spec.content_sha256:
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Encoding artifact input fingerprint is stale",
                {"shard_index": spec.shard_index},
            )
        try:
            arrays = load_npz(data_file)
            matrix = arrays["X"]
            labels = arrays["y"]
        except (OSError, KeyError, ValueError) as exc:
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Encoding matrix artifact is unreadable",
                {"path": str(data_file), "reason": str(exc)},
            ) from exc
        if stable_matrix_digest(matrix) != summary.get("matrix_digest"):
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Encoding matrix digest does not match its summary",
                {"shard_index": spec.shard_index},
            )
        if stable_vector_digest(labels) != summary.get("label_digest"):
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Encoding label digest does not match its summary",
                {"shard_index": spec.shard_index},
            )
        return {"summary": summary, "matrix": matrix, "labels": labels}

    def _store_encoding_result(
        self,
        context: dict[str, Any],
        spec: ShardSpec,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        stage_version = context["stage_versions"]["encoding"]
        summary = result["summary"]
        base = self._artifact_path(f"artifacts/encoding/{stage_version}")
        summary_path = base / f"{spec.shard_index:04d}.summary.json"
        data_path = base / f"{spec.shard_index:04d}.data.npz"
        atomic_write_npz(data_path, X=np.asarray(result["matrix"]), y=np.asarray(result["labels"]))
        atomic_write_json(summary_path, summary)
        dependencies = [
            self._record_dependency(
                "type_inference",
                context["stage_versions"]["type_inference"],
                "ALL",
            )
        ]
        input_fingerprint = util_digest_object(
            {
                "file": spec.file,
                "content_sha256": spec.content_sha256,
                "type_inference": context["stage_versions"]["type_inference"],
            }
        )
        record = make_record(
            stage="encoding",
            scope=f"shard:{spec.shard_index}",
            pipeline_version=context["pipeline_version"],
            stage_version=stage_version,
            input_fingerprint=input_fingerprint,
            config_version=context["pipeline_version"],
            artifact_digest=summary["encoding_digest"],
            shard_id=spec.shard_index,
            tier=spec.tier,
            payload={
                "summary_path": self._store_relative(summary_path),
                "data_path": self._store_relative(data_path),
                "file": spec.file,
            },
            dependencies=dependencies,
        )
        self._publish(context, record)
        return result

    def _worker_task(
        self,
        context: dict[str, Any],
        spec: ShardSpec,
        schema: dict[str, Any],
    ) -> dict[str, Any]:
        tier_schema = dict(schema["tiers"][spec.tier])
        tier_schema["schema_digest"] = schema["schema_digest"]
        return {
            "spec": spec,
            "tier_schema": tier_schema,
            "config": context["config"],
        }

    @staticmethod
    def _worker_error_dict(spec: ShardSpec, exc: BaseException) -> dict[str, Any]:
        return {
            "code": "E_WORKER_FAILED",
            "message": "Worker process failed while encoding a shard",
            "details": {
                "shard_index": spec.shard_index,
                "file": spec.file,
                "error_type": type(exc).__name__,
                "reason": str(exc),
            },
        }

    def _run_encoding(
        self,
        context: dict[str, Any],
        schema: dict[str, Any],
        *,
        worker_mode: str,
        stop_after_shards: int | None,
    ) -> tuple[dict[int, dict[str, Any]], dict[str, Any]]:
        admitted = context["admitted_specs"]
        encoding_results: dict[int, dict[str, Any]] = {}
        pending: list[ShardSpec] = []
        reused = 0
        for spec in admitted:
            record = self._encoding_record(context, spec)
            if record is not None:
                encoding_results[spec.shard_index] = self._load_encoding_artifact(
                    context, spec, record
                )
                reused += 1
            else:
                pending.append(spec)
        if not pending:
            return encoding_results, {"reused": reused, "computed": 0, "failed": []}

        max_workers = max(1, min(
            int(context["config"]["budget"]["max_workers"]),
            max(1, int(context["plan"]["concurrency"])),
            len(pending),
        ))
        wave_size = 1 if worker_mode == "sequential" else max_workers
        newly_computed = 0
        failed: list[dict[str, Any]] = []
        index = 0
        while index < len(pending):
            if self.store.stop_requested():
                break
            if stop_after_shards is not None and newly_computed >= stop_after_shards:
                self.store.request_stop("stop_after_shards")
                break
            wave_specs = pending[index:index + wave_size]
            tasks = [self._worker_task(context, spec, schema) for spec in wave_specs]
            wave_results: dict[int, dict[str, Any]] = {}
            if worker_mode == "sequential":
                for spec, task in zip(wave_specs, tasks):
                    wave_results[spec.shard_index] = encode_worker(task)
            elif worker_mode == "process":
                try:
                    with ProcessPoolExecutor(
                        max_workers=max(1, len(wave_specs)),
                        mp_context=self._process_context(),
                    ) as executor:
                        futures = {
                            executor.submit(encode_worker, task): spec
                            for spec, task in zip(wave_specs, tasks)
                        }
                        wait(futures, return_when="ALL_COMPLETED")
                        for future, spec in futures.items():
                            try:
                                wave_results[spec.shard_index] = future.result()
                            except BaseException as exc:
                                wave_results[spec.shard_index] = {
                                    "ok": False,
                                    "shard_index": spec.shard_index,
                                    "error": self._worker_error_dict(spec, exc),
                                }
                except BaseException as exc:
                    for spec in wave_specs:
                        wave_results[spec.shard_index] = {
                            "ok": False,
                            "shard_index": spec.shard_index,
                            "error": self._worker_error_dict(spec, exc),
                        }
            else:
                raise WorkbenchError(
                    "E_CONFIG_INVALID",
                    "worker_mode must be process or sequential",
                    {"value": worker_mode},
                )

            for spec in sorted(wave_specs, key=lambda item: item.shard_index):
                result = wave_results[spec.shard_index]
                if result.get("ok"):
                    stored = self._store_encoding_result(context, spec, result)
                    encoding_results[spec.shard_index] = stored
                    newly_computed += 1
                    if stop_after_shards is not None and newly_computed >= stop_after_shards:
                        self.store.request_stop("stop_after_shards")
                else:
                    error = result.get("error") or {
                        "code": "E_WORKER_FAILED",
                        "message": "Worker returned no structured result",
                        "details": {"shard_index": spec.shard_index},
                    }
                    failed.append(error)
            self._write_runtime(
                "running",
                stage="encoding",
                progress={
                    "encoding_completed": len(encoding_results),
                    "encoding_total": len(admitted),
                    "unprocessed": context["plan"]["unprocessed"],
                },
            )
            index += len(wave_specs)
            if failed or self.store.stop_requested():
                break

        if failed:
            first = failed[0]
            raise WorkbenchError(
                str(first.get("code", "E_WORKER_FAILED")),
                str(first.get("message", "Encoding worker failed")),
                {
                    **dict(first.get("details", {})),
                    "failed_shards": failed,
                    "successful_shards": sorted(encoding_results),
                    "preserved_checkpoints": True,
                },
            )
        return encoding_results, {
            "reused": reused,
            "computed": newly_computed,
            "failed": failed,
        }

    @staticmethod
    def _process_context() -> mp.context.BaseContext:
        if os.name == "nt":
            return mp.get_context("spawn")
        argv0 = Path(sys.argv[0]).resolve()
        if argv0.is_file():
            return mp.get_context("forkserver")
        return mp.get_context("fork")

    def _load_selection_artifact(
        self,
        tier: str,
        record: dict[str, Any],
    ) -> dict[str, Any]:
        relative = record.get("payload", {}).get("artifact_path")
        if not relative:
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Selection checkpoint has no artifact path",
                {"record_id": record.get("record_id")},
            )
        path = self._artifact_path(relative)
        if not path.is_file():
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Selection artifact is missing",
                {"path": str(path), "record_id": record.get("record_id")},
            )
        try:
            selection = read_json(path)
        except (OSError, ValueError) as exc:
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Selection artifact is unreadable",
                {"path": str(path), "reason": str(exc)},
            ) from exc
        check = dict(selection)
        claimed = check.pop("selection_digest", None)
        if claimed != record.get("artifact_digest") or claimed != util_digest_object(check):
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Selection artifact digest does not match its checkpoint",
                {"record_id": record.get("record_id")},
            )
        if selection.get("tier") != tier:
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Selection artifact belongs to a different tier",
                {"expected": tier, "observed": selection.get("tier")},
            )
        return selection

    def _run_selection(
        self,
        context: dict[str, Any],
        encoding_results: dict[int, dict[str, Any]],
    ) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
        selections: dict[str, dict[str, Any]] = {}
        reused = 0
        computed = 0
        stage_version = context["stage_versions"]["selection"]
        for tier in context["tiers_with_admitted"]:
            if self.store.stop_requested():
                break
            record = self.store.latest_record(
                stage="selection",
                stage_version=stage_version,
                scope=f"tier:{tier}",
            )
            if record is not None:
                selections[tier] = self._load_selection_artifact(tier, record)
                reused += 1
                continue
            tier_specs = [
                spec for spec in context["admitted_specs"] if spec.tier == tier
            ]
            tier_results = []
            for spec in sorted(tier_specs, key=lambda item: item.shard_index):
                if spec.shard_index not in encoding_results:
                    raise WorkbenchError(
                        "E_STATE_INVALID",
                        "Selection cannot run without all encoded shards in the tier",
                        {"tier": tier, "missing_shard": spec.shard_index},
                    )
                tier_results.append(encoding_results[spec.shard_index])
            self._write_runtime(
                "running",
                stage="selection",
                progress={
                    "selection_tier": tier,
                    "encoding_completed": len(encoding_results),
                    "encoding_total": len(context["admitted_specs"]),
                },
            )
            selection = select_features(tier_results, context["config"])
            artifact_path = self._artifact_path(
                f"artifacts/selection/{stage_version}/{_safe_name(tier)}.json"
            )
            atomic_write_json(artifact_path, selection)
            dependencies = []
            encoding_digests = []
            for spec in sorted(tier_specs, key=lambda item: item.shard_index):
                encoding_record = self.store.latest_record(
                    stage="encoding",
                    stage_version=context["stage_versions"]["encoding"],
                    scope=f"shard:{spec.shard_index}",
                )
                if encoding_record is None:
                    raise WorkbenchError(
                        "E_STALE_CHECKPOINT",
                        "Encoding dependency disappeared before selection publication",
                        {"tier": tier, "shard_index": spec.shard_index},
                    )
                dependencies.append(
                    self._record_dependency(
                        "encoding",
                        context["stage_versions"]["encoding"],
                        f"shard:{spec.shard_index}",
                    )
                )
                encoding_digests.append(
                    [spec.shard_index, encoding_record.get("artifact_digest")]
                )
            record = make_record(
                stage="selection",
                scope=f"tier:{tier}",
                pipeline_version=context["pipeline_version"],
                stage_version=stage_version,
                input_fingerprint=util_digest_object(
                    {
                        "encoding_stage_version": context["stage_versions"]["encoding"],
                        "encoding_digests": encoding_digests,
                    }
                ),
                config_version=context["pipeline_version"],
                artifact_digest=selection["selection_digest"],
                tier=tier,
                payload={"artifact_path": self._store_relative(artifact_path)},
                dependencies=dependencies,
            )
            self._publish(context, record)
            selections[tier] = selection
            computed += 1
        return selections, {"reused": reused, "computed": computed}

    def _ensure_encoding_stage_record(
        self,
        context: dict[str, Any],
        encoding_results: dict[int, dict[str, Any]],
    ) -> dict[str, Any] | None:
        admitted = context["admitted_specs"]
        if len(encoding_results) != len(admitted):
            return None
        stage_version = context["stage_versions"]["encoding"]
        existing = self.store.latest_record(
            stage="encoding",
            stage_version=stage_version,
            scope="STAGE",
        )
        if existing is not None:
            return existing
        shard_entries = []
        dependencies = []
        for spec in sorted(admitted, key=lambda item: item.shard_index):
            result = encoding_results[spec.shard_index]
            shard_entries.append(
                {
                    "shard_index": spec.shard_index,
                    "file": spec.file,
                    "encoding_digest": result["summary"]["encoding_digest"],
                    "matrix_digest": result["summary"]["matrix_digest"],
                }
            )
            dependencies.append(
                self._record_dependency(
                    "encoding",
                    stage_version,
                    f"shard:{spec.shard_index}",
                )
            )
        stage_summary = {
            "schema_version": 1,
            "stage": "encoding",
            "stage_version": stage_version,
            "shards": shard_entries,
        }
        artifact_path = self._artifact_path(
            f"artifacts/encoding/{stage_version}/stage.json"
        )
        atomic_write_json(artifact_path, stage_summary)
        record = make_record(
            stage="encoding",
            scope="STAGE",
            pipeline_version=context["pipeline_version"],
            stage_version=stage_version,
            input_fingerprint=util_digest_object(
                [[entry["shard_index"], entry["encoding_digest"]] for entry in shard_entries]
            ),
            config_version=context["pipeline_version"],
            artifact_digest=util_digest_object(stage_summary),
            payload={"artifact_path": self._store_relative(artifact_path)},
            dependencies=dependencies,
        )
        return self._publish(context, record)

    def _ensure_selection_stage_record(
        self,
        context: dict[str, Any],
        selections: dict[str, dict[str, Any]],
    ) -> dict[str, Any] | None:
        tiers = context["tiers_with_admitted"]
        if len(selections) != len(tiers):
            return None
        stage_version = context["stage_versions"]["selection"]
        existing = self.store.latest_record(
            stage="selection",
            stage_version=stage_version,
            scope="STAGE",
        )
        if existing is not None:
            return existing
        tier_entries = []
        dependencies = [
            self._record_dependency(
                "encoding",
                context["stage_versions"]["encoding"],
                "STAGE",
            )
        ]
        for tier in tiers:
            selection = selections[tier]
            tier_entries.append(
                {
                    "tier": tier,
                    "selection_digest": selection["selection_digest"],
                    "selected": selection["selected"],
                    "eliminated": selection["eliminated"],
                }
            )
            dependencies.append(
                self._record_dependency("selection", stage_version, f"tier:{tier}")
            )
        stage_summary = {
            "schema_version": 1,
            "stage": "selection",
            "stage_version": stage_version,
            "tiers": tier_entries,
        }
        artifact_path = self._artifact_path(
            f"artifacts/selection/{stage_version}/stage.json"
        )
        atomic_write_json(artifact_path, stage_summary)
        record = make_record(
            stage="selection",
            scope="STAGE",
            pipeline_version=context["pipeline_version"],
            stage_version=stage_version,
            input_fingerprint=util_digest_object(
                [[entry["tier"], entry["selection_digest"]] for entry in tier_entries]
            ),
            config_version=context["pipeline_version"],
            artifact_digest=util_digest_object(stage_summary),
            payload={"artifact_path": self._store_relative(artifact_path)},
            dependencies=dependencies,
        )
        return self._publish(context, record)

    def _build_aggregate(
        self,
        context: dict[str, Any],
        encoding_results: dict[int, dict[str, Any]],
        selections: dict[str, dict[str, Any]],
    ) -> tuple[dict[str, Any], dict[str, np.ndarray], dict[str, np.ndarray]]:
        tier_summaries: dict[str, Any] = {}
        selected_matrices: dict[str, np.ndarray] = {}
        labels_by_tier: dict[str, np.ndarray] = {}
        export_relative: dict[str, Any] = {
            "summary": "exports/selection.json",
            "tiers": {},
        }
        for tier in context["tiers_with_admitted"]:
            if tier not in selections:
                raise WorkbenchError(
                    "E_STATE_INVALID",
                    "Aggregate cannot run without every tier selection",
                    {"tier": tier},
                )
            specs = sorted(
                [spec for spec in context["admitted_specs"] if spec.tier == tier],
                key=lambda item: item.shard_index,
            )
            selection = selections[tier]
            feature_names = list(selection["feature_names"])
            selected = list(selection["selected"])
            selected_positions = {name: index for index, name in enumerate(feature_names)}
            indices = [selected_positions[name] for name in selected]
            matrices = []
            labels = []
            shard_summaries = []
            per_shard_selected_digests = []
            for spec in specs:
                result = encoding_results[spec.shard_index]
                matrix = np.asarray(result["matrix"])
                label = np.asarray(result["labels"])
                projected = np.ascontiguousarray(matrix[:, indices], dtype=np.float64)
                matrices.append(projected)
                labels.append(label)
                per_shard_selected_digests.append(
                    {
                        "shard_index": spec.shard_index,
                        "selected_matrix_digest": stable_matrix_digest(projected),
                    }
                )
                shard_summaries.append(
                    {
                        "shard_index": spec.shard_index,
                        "file": spec.file,
                        "rows": int(matrix.shape[0]),
                        "encoded_feature_count": int(matrix.shape[1]),
                        "encoding_digest": result["summary"]["encoding_digest"],
                        "matrix_digest": result["summary"]["matrix_digest"],
                        "selected_matrix_digest": stable_matrix_digest(projected),
                    }
                )
            selected_matrix = (
                np.concatenate(matrices, axis=0)
                if matrices
                else np.empty((0, len(selected)), dtype=np.float64)
            )
            labels_array = (
                np.concatenate(labels, axis=0) if labels else np.empty((0,), dtype=np.int64)
            )
            selected_matrices[tier] = selected_matrix
            labels_by_tier[tier] = labels_array
            tier_export = f"exports/{_safe_name(tier)}"
            export_relative["tiers"][tier] = {
                "selected_csv": f"{tier_export}/selected.csv",
                "labels_csv": f"{tier_export}/labels.csv",
                "encoding_summaries": f"{tier_export}/encoding_summaries.json",
            }
            tier_summaries[tier] = {
                "tier": tier,
                "rows": int(selected_matrix.shape[0]),
                "selected_feature_count": len(selected),
                "eliminated_feature_count": len(selection["eliminated"]),
                "selected": selected,
                "eliminated": selection["eliminated"],
                "scores": selection["scores"],
                "ranking": selection["ranking"],
                "method": selection["method"],
                "threshold": selection["threshold"],
                "selection_digest": selection["selection_digest"],
                "encoded_feature_count": selection["encoded_feature_count"],
                "selected_matrix_digest": stable_matrix_digest(selected_matrix),
                "labels_digest": stable_vector_digest(labels_array),
                "shards": shard_summaries,
                "per_shard_selected_digests": per_shard_selected_digests,
                "exports": export_relative["tiers"][tier],
            }

        summary: dict[str, Any] = {
            "schema_version": 1,
            "status": "complete",
            "pipeline_version": context["pipeline_version"],
            "stage_versions": context["stage_versions"],
            "tiers": tier_summaries,
            "budget": context["plan"],
            "unprocessed_shards": context["plan"]["unprocessed"],
            "exports": export_relative,
            "determinism": {
                "row_order": "tier, then shard_index, then source row order",
                "feature_order": "encoded feature order; selected list preserves encoded order",
                "target_encoding": "out-of-fold training labels only; deterministic fold seed",
            },
        }
        summary = to_jsonable_summary(summary)
        summary["result_digest"] = util_digest_object(summary)
        return summary, selected_matrices, labels_by_tier

    def _write_exports(
        self,
        context: dict[str, Any],
        summary: dict[str, Any],
        encoding_results: dict[int, dict[str, Any]],
        selected_matrices: dict[str, np.ndarray],
        labels_by_tier: dict[str, np.ndarray],
    ) -> None:
        for tier, matrix in selected_matrices.items():
            tier_summary = summary["tiers"][tier]
            selected = list(tier_summary["selected"])
            tier_dir = self.export_dir / _safe_name(tier)
            selected_path = tier_dir / "selected.csv"
            labels_path = tier_dir / "labels.csv"
            encoding_path = tier_dir / "encoding_summaries.json"
            atomic_write_bytes(selected_path, _csv_bytes(selected, matrix.tolist()))
            atomic_write_bytes(labels_path, _label_csv_bytes(labels_by_tier[tier]))
            shard_summaries = []
            for spec in sorted(
                [item for item in context["admitted_specs"] if item.tier == tier],
                key=lambda item: item.shard_index,
            ):
                shard_summaries.append(encoding_results[spec.shard_index]["summary"])
                shard_matrix = np.asarray(encoding_results[spec.shard_index]["matrix"])
                positions = {
                    name: index
                    for index, name in enumerate(
                        encoding_results[spec.shard_index]["summary"]["feature_names"]
                    )
                }
                shard_selected = np.ascontiguousarray(
                    shard_matrix[:, [positions[name] for name in selected]],
                    dtype=np.float64,
                )
                shard_dir = tier_dir / "shards"
                atomic_write_bytes(
                    shard_dir / f"{spec.shard_index:04d}.selected.csv",
                    _csv_bytes(selected, shard_selected.tolist()),
                )
            atomic_write_json(
                encoding_path,
                {
                    "tier": tier,
                    "schema_version": 1,
                    "shards": shard_summaries,
                },
            )
        atomic_write_json(self.export_dir / "selection.json", summary)

    def _ensure_aggregate(
        self,
        context: dict[str, Any],
        encoding_results: dict[int, dict[str, Any]],
        selections: dict[str, dict[str, Any]],
    ) -> tuple[dict[str, Any], bool]:
        summary, selected_matrices, labels_by_tier = self._build_aggregate(
            context, encoding_results, selections
        )
        stage_version = context["stage_versions"]["aggregate"]
        existing = self.store.latest_record(
            stage="aggregate",
            stage_version=stage_version,
            scope="ALL",
        )
        if existing is not None:
            relative = existing.get("payload", {}).get("summary_path")
            if not relative:
                raise WorkbenchError(
                    "E_STATE_INVALID",
                    "Aggregate checkpoint has no summary path",
                    {"record_id": existing.get("record_id")},
                )
            path = self._artifact_path(relative)
            if not path.is_file():
                raise WorkbenchError(
                    "E_STATE_INVALID",
                    "Aggregate artifact is missing",
                    {"path": str(path)},
                )
            stored = read_json(path)
            if stored.get("result_digest") != summary["result_digest"]:
                raise WorkbenchError(
                    "E_STATE_INVALID",
                    "Stored aggregate result differs from deterministic reconstruction",
                    {
                        "stored_digest": stored.get("result_digest"),
                        "rebuilt_digest": summary["result_digest"],
                    },
                )
            self._write_exports(
                context, summary, encoding_results, selected_matrices, labels_by_tier
            )
            self._write_baseline(context, summary["result_digest"])
            return summary, True

        artifact_path = self._artifact_path(
            f"artifacts/aggregate/{stage_version}/summary.json"
        )
        atomic_write_json(artifact_path, summary)
        self._write_exports(
            context, summary, encoding_results, selected_matrices, labels_by_tier
        )
        dependencies = []
        selection_digests = []
        for tier in context["tiers_with_admitted"]:
            selection_record = self.store.latest_record(
                stage="selection",
                stage_version=context["stage_versions"]["selection"],
                scope=f"tier:{tier}",
            )
            if selection_record is None:
                raise WorkbenchError(
                    "E_STALE_CHECKPOINT",
                    "Selection dependency disappeared before aggregate publication",
                    {"tier": tier},
                )
            dependencies.append(
                self._record_dependency(
                    "selection",
                    context["stage_versions"]["selection"],
                    f"tier:{tier}",
                )
            )
            selection_digests.append([tier, selection_record.get("artifact_digest")])
        dependencies.append(
            self._record_dependency(
                "selection",
                context["stage_versions"]["selection"],
                "STAGE",
            )
        )
        record = make_record(
            stage="aggregate",
            scope="ALL",
            pipeline_version=context["pipeline_version"],
            stage_version=stage_version,
            input_fingerprint=util_digest_object(
                {
                    "selection_stage_version": context["stage_versions"]["selection"],
                    "selection_digests": selection_digests,
                    "budget": context["config"]["budget"],
                }
            ),
            config_version=context["pipeline_version"],
            artifact_digest=summary["result_digest"],
            payload={
                "summary_path": self._store_relative(artifact_path),
                "export_dir": str(self.export_dir.relative_to(self.root))
                if self.export_dir.is_relative_to(self.root)
                else str(self.export_dir),
            },
            dependencies=dependencies,
        )
        self._publish(context, record)
        self._write_baseline(context, summary["result_digest"])
        return summary, False

    def _write_baseline(self, context: dict[str, Any], result_digest: str) -> None:
        baseline = {
            "schema_version": 1,
            "pipeline_version": context["pipeline_version"],
            "stage_versions": context["stage_versions"],
            "result_digest": result_digest,
        }
        atomic_write_json(self.versions_path, baseline)

    def _result_payload(
        self,
        context: dict[str, Any],
        status: str,
        *,
        schema: dict[str, Any] | None,
        encoding_stats: dict[str, Any] | None,
        selection_stats: dict[str, Any] | None,
        summary: dict[str, Any] | None,
        stage: str,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ok": True,
            "status": status,
            "stage": stage,
            "pipeline_version": context["pipeline_version"],
            "stage_versions": context["stage_versions"],
            "affected_earliest_stage": context["affected_earliest_stage"],
            "budget": context["plan"],
            "encodings": encoding_stats or {},
            "selections": selection_stats or {},
            "stop_requested": self.store.stop_requested(),
            "stop_reason": self.store.stop_reason(),
            "checkpoints": self.store.sequence_summary(),
        }
        if schema is not None:
            payload["type_inference"] = {
                "schema_digest": schema.get("schema_digest"),
                "tiers": {
                    tier: {
                        "rows": info["rows"],
                        "columns": info["columns"],
                        "shards": info["shards"],
                        "source_columns": info["source_columns"],
                    }
                    for tier, info in schema.get("tiers", {}).items()
                },
            }
        if summary is not None:
            payload["result_digest"] = summary["result_digest"]
            payload["exports"] = summary["exports"]
            payload["tiers"] = {
                tier: {
                    "rows": info["rows"],
                    "selected_feature_count": info["selected_feature_count"],
                    "selected": info["selected"],
                    "selected_matrix_digest": info["selected_matrix_digest"],
                    "selection_digest": info["selection_digest"],
                }
                for tier, info in summary["tiers"].items()
            }
        return payload

    def run(
        self,
        *,
        until_stage: str = "aggregate",
        worker_mode: str = "process",
        stop_after_shards: int | None = None,
        resume: bool = False,
    ) -> dict[str, Any]:
        if worker_mode not in {"process", "sequential"}:
            raise WorkbenchError(
                "E_CONFIG_INVALID",
                "worker_mode must be process or sequential",
                {"value": worker_mode},
            )
        until_stage = self._normalize_stage(until_stage)
        if stop_after_shards is not None:
            if isinstance(stop_after_shards, bool) or not isinstance(stop_after_shards, int) or stop_after_shards < 1:
                raise WorkbenchError(
                    "E_CONFIG_INVALID",
                    "stop_after_shards must be a positive integer",
                    {"value": stop_after_shards},
                )
        if not self._run_lock.acquire(blocking=False):
            raise WorkbenchError(
                "E_RUN_IN_PROGRESS",
                "Another pipeline run is already active",
            )
        try:
            self.clear_stop()
            context = self._load_context()
            context["next_sequence"] = self.store.next_sequence()
            self._write_runtime(
                "running",
                stage="prepare",
                progress={
                    "admitted_shards": [entry["shard_index"] for entry in context["plan"]["admitted"]],
                    "unprocessed_shards": context["plan"]["unprocessed"],
                },
            )
            schema, type_record, type_reused = self._ensure_type_inference(context)
            if self.store.stop_requested():
                status = "stopped"
                payload = self._result_payload(
                    context,
                    status,
                    schema=schema,
                    encoding_stats=None,
                    selection_stats=None,
                    summary=None,
                    stage="type_inference",
                )
                self._write_runtime(status, stage="type_inference")
                return payload
            if until_stage == "type_inference":
                status = "partial"
                payload = self._result_payload(
                    context,
                    status,
                    schema=schema,
                    encoding_stats={},
                    selection_stats={},
                    summary=None,
                    stage="type_inference",
                )
                self._write_runtime(status, stage="type_inference")
                return payload
            if self.store.stop_requested():
                status = "stopped"
                payload = self._result_payload(
                    context,
                    status,
                    schema=schema,
                    encoding_stats=None,
                    selection_stats=None,
                    summary=None,
                    stage="encoding",
                )
                self._write_runtime(status, stage="encoding")
                return payload
            encoding_results, encoding_stats = self._run_encoding(
                context,
                schema,
                worker_mode=worker_mode,
                stop_after_shards=stop_after_shards,
            )
            self._ensure_encoding_stage_record(context, encoding_results)
            if self.store.stop_requested():
                payload = self._result_payload(
                    context,
                    "stopped",
                    schema=schema,
                    encoding_stats=encoding_stats,
                    selection_stats=None,
                    summary=None,
                    stage="encoding",
                )
                self._write_runtime("stopped", stage="encoding")
                return payload
            if until_stage == "encoding":
                payload = self._result_payload(
                    context,
                    "partial",
                    schema=schema,
                    encoding_stats=encoding_stats,
                    selection_stats={},
                    summary=None,
                    stage="encoding",
                )
                self._write_runtime("partial", stage="encoding")
                return payload
            selections, selection_stats = self._run_selection(context, encoding_results)
            self._ensure_selection_stage_record(context, selections)
            if self.store.stop_requested():
                payload = self._result_payload(
                    context,
                    "stopped",
                    schema=schema,
                    encoding_stats=encoding_stats,
                    selection_stats=selection_stats,
                    summary=None,
                    stage="selection",
                )
                self._write_runtime("stopped", stage="selection")
                return payload
            if until_stage == "selection":
                payload = self._result_payload(
                    context,
                    "partial",
                    schema=schema,
                    encoding_stats=encoding_stats,
                    selection_stats=selection_stats,
                    summary=None,
                    stage="selection",
                )
                self._write_runtime("partial", stage="selection")
                return payload
            summary, aggregate_reused = self._ensure_aggregate(
                context, encoding_results, selections
            )
            if aggregate_reused:
                encoding_stats["aggregate_reused"] = True
            result = self._result_payload(
                context,
                "complete",
                schema=schema,
                encoding_stats=encoding_stats,
                selection_stats=selection_stats,
                summary=summary,
                stage="aggregate",
            )
            result["result_digest"] = summary["result_digest"]
            result["reused"] = {
                "type_inference": type_reused,
                "encoding_records": int(encoding_stats.get("reused", 0)),
                "selection_records": int(selection_stats.get("reused", 0)),
                "aggregate": aggregate_reused,
            }
            self._write_runtime("complete", stage="aggregate")
            return result
        except WorkbenchError as exc:
            self._write_runtime("failed", error=exc.to_dict()["error"])
            raise
        except BaseException as exc:
            error = {
                "code": "E_INTERNAL",
                "message": "Unexpected workbench failure",
                "details": {"error_type": type(exc).__name__, "reason": str(exc)},
            }
            self._write_runtime("failed", error=error)
            raise WorkbenchError(error["code"], error["message"], error["details"]) from exc
        finally:
            self._run_lock.release()

    @staticmethod
    def _normalize_stage(stage: str) -> str:
        aliases = {
            "type": "type_inference",
            "types": "type_inference",
            "type_inference": "type_inference",
            "column_type": "type_inference",
            "encoding": "encoding",
            "selection": "selection",
            "aggregate": "aggregate",
            "summary": "aggregate",
            "result": "aggregate",
        }
        normalized = aliases.get(str(stage).strip().lower())
        if normalized is None:
            raise WorkbenchError(
                "E_CONFIG_INVALID",
                "until_stage must be type_inference, encoding, selection, or aggregate",
                {"value": stage},
            )
        return normalized

    def export_result(self) -> dict[str, Any]:
        result = self.run(until_stage="aggregate", worker_mode="process")
        context = self._load_context()
        record = self.store.latest_record(
            stage="aggregate",
            stage_version=context["stage_versions"]["aggregate"],
            scope="ALL",
        )
        if record is None:
            raise WorkbenchError(
                "E_STATE_INVALID",
                "Aggregate checkpoint is not available after export run",
            )
        relative = record.get("payload", {}).get("summary_path")
        summary = read_json(self._artifact_path(str(relative)))
        return {
            "ok": True,
            "status": result["status"],
            "result_digest": summary["result_digest"],
            "exports": summary["exports"],
            "summary": summary,
        }

    def read_selection_summary(self) -> dict[str, Any] | None:
        context = self._load_context()
        record = self.store.latest_record(
            stage="aggregate",
            stage_version=context["stage_versions"]["aggregate"],
            scope="ALL",
        )
        if record is None:
            return None
        relative = record.get("payload", {}).get("summary_path")
        if not relative:
            return None
        path = self._artifact_path(str(relative))
        if not path.is_file():
            return None
        return read_json(path)


def to_jsonable_summary(value: Any) -> Any:
    from .util import to_jsonable

    return to_jsonable(value)

