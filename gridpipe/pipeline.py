"""Parallel four-stage orchestration with deterministic checkpoint reduction."""

from __future__ import annotations

import concurrent.futures
import copy
import json
import multiprocessing
import os
import shutil
import threading
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from PIL import Image

from .canonical import (
    atomic_write_json,
    digest_file,
    digest_json,
    dump_bytes,
    read_json,
    rounded,
    sha256_bytes,
)
from .chart_structure import extract_chart
from .checkpoints import CheckpointStore, FileLock
from .config import (
    PIPELINE_SCHEMA_VERSION,
    STAGE_INDEX,
    STAGES,
    config_version,
    default_configs,
    merge_configs,
    stage_config_version,
)
from .detection import detect_regions
from .errors import (
    E_INVALID_INPUT,
    E_INVALID_STAGE,
    E_STALE_CHECKPOINT,
    E_WORKER,
    PipelineError,
    error_payload,
    success_payload,
)
from .imaging import load_rgb_and_gray
from .scheduler import build_schedule
from .table_structure import restore_table
from .validation import validate_shard


PIPELINE_VERSION = "1.0.0"


def _process_context() -> multiprocessing.context.BaseContext:
    if os.name == "nt":  # pragma: no cover - Windows branch
        return multiprocessing.get_context("spawn")
    if threading.current_thread() is not threading.main_thread():
        # forking from a local HTTP worker thread can inherit unrelated locks.
        return multiprocessing.get_context("forkserver")
    return multiprocessing.get_context("fork")


def _normalize_to_truth(value: Any) -> Any:
    return copy.deepcopy(value)


def execute_stage_task(task: Dict[str, Any]) -> Dict[str, Any]:
    """Process-pool entry point.  Every failure becomes a structured payload."""

    stage = str(task["stage"])
    shard = task["shard"]
    shard_id = str(shard["id"])
    try:
        if stage == "detection":
            _, gray = load_rgb_and_gray(task["image_path"])
            result = detect_regions(gray, shard_id=shard_id, config=task["config"])
            return success_payload(result)
        if stage == "table":
            rgb, gray = load_rgb_and_gray(task["image_path"])
            detection = task["prerequisites"]["detection"]
            if not detection.get("ok", False) or not detection.get("result"):
                raise PipelineError(
                    E_INVALID_STAGE,
                    "table stage requires a successful detection artifact",
                    shard_id=shard_id,
                    diagnostics={"reason": "missing_or_failed_detection"},
                )
            region = detection["result"]["table"]["pixel_rect"]
            result = restore_table(
                rgb,
                gray,
                region=region,
                declared=shard["truth"]["table"],
                config=task["config"],
                shard_id=shard_id,
            )
            return success_payload(result)
        if stage == "chart":
            rgb, gray = load_rgb_and_gray(task["image_path"])
            detection = task["prerequisites"]["detection"]
            if not detection.get("ok", False) or not detection.get("result"):
                raise PipelineError(
                    E_INVALID_STAGE,
                    "chart stage requires a successful detection artifact",
                    shard_id=shard_id,
                    diagnostics={"reason": "missing_or_failed_detection"},
                )
            region = detection["result"]["chart"]["pixel_rect"]
            result = extract_chart(
                rgb,
                gray,
                region=region,
                config=task["config"],
                shard_id=shard_id,
            )
            return success_payload(result)
        if stage == "validation":
            detection = task["prerequisites"].get("detection")
            table = task["prerequisites"].get("table")
            chart = task["prerequisites"].get("chart")
            detection_result = detection.get("result") if detection and detection.get("ok") else None
            table_result = table.get("result") if table and table.get("ok") else None
            chart_result = None
            if chart and chart.get("ok"):
                chart_result = chart.get("result")
            elif chart and chart.get("partial"):
                valid_series = chart["partial"].get("valid_series", [])
                if valid_series:
                    chart_result = {"series": valid_series}
            result = validate_shard(shard, detection_result, table_result, chart_result)
            return success_payload(result)
        raise PipelineError(
            E_INVALID_STAGE,
            f"unknown stage: {stage}",
            shard_id=shard_id,
            diagnostics={"stage": stage, "allowed": list(STAGES)},
        )
    except PipelineError as exc:
        return error_payload(exc)
    except Exception as exc:  # pragma: no cover - defensive worker boundary
        return error_payload(
            PipelineError(
                E_WORKER,
                "worker raised an unhandled exception",
                shard_id=shard_id,
                diagnostics={
                    "stage": stage,
                    "exception_type": type(exc).__name__,
                    "message": str(exc),
                },
            )
        )


class Pipeline:
    def __init__(
        self,
        root: str | Path,
        manifest_path: str | Path,
        *,
        work_dir: Optional[str | Path] = None,
        output_dir: Optional[str | Path] = None,
        config_overrides: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.root = Path(root).resolve()
        self.manifest_path = Path(manifest_path).resolve()
        if not self.manifest_path.is_file():
            raise PipelineError(
                E_INVALID_INPUT,
                "manifest file does not exist",
                diagnostics={"manifest": str(self.manifest_path)},
            )
        self.work_dir = (
            Path(work_dir).resolve() if work_dir is not None else self.root / "work"
        )
        self.output_dir = (
            Path(output_dir).resolve() if output_dir is not None else self.root / "output"
        )
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.stop_path = self.work_dir / "stop.request"
        self.state_dir = self.work_dir / "state"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.manifest = self._load_manifest()
        self.configs = default_configs()
        manifest_configs = self.manifest.get("configs", {})
        for key in ("detection", "table", "chart"):
            if isinstance(manifest_configs.get(key), dict):
                self.configs[key].update(copy.deepcopy(manifest_configs[key]))
        if isinstance(self.manifest.get("budget"), dict):
            self.configs["budget"].update(copy.deepcopy(self.manifest["budget"]))
        self.configs = merge_configs(self.configs, config_overrides)
        self.config_version = config_version(self.configs)
        self.shards: List[Dict[str, Any]] = self.manifest["shards"]
        self.shard_by_id = {str(shard["id"]): shard for shard in self.shards}
        if len(self.shard_by_id) != len(self.shards):
            raise PipelineError(E_INVALID_INPUT, "shard ids must be unique")
        self.image_sha256: Dict[str, str] = {}
        self._validate_images()
        self.schedule = build_schedule(self.shards, self.configs["budget"])
        self.accepted_ids = [entry["shard_id"] for entry in self.schedule["accepted"]]
        self.accepted_shards = [self.shard_by_id[shard_id] for shard_id in self.accepted_ids]
        self.run_id = self._compute_run_id()
        self.store = CheckpointStore(self.work_dir, self.run_id)
        self.stats: Dict[str, Any] = {
            "computed": {stage: 0 for stage in STAGES},
            "cache_reused": {stage: 0 for stage in STAGES},
            "checkpoint_reused": {stage: 0 for stage in STAGES},
        }
        self.previous_state = self._read_previous_state()
        self.invalidation = self._compute_invalidation()
        self.stage_outputs: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._load_current_outputs()

    def _load_manifest(self) -> Dict[str, Any]:
        try:
            manifest = read_json(self.manifest_path)
        except Exception as exc:
            raise PipelineError(
                E_INVALID_INPUT,
                f"cannot parse manifest: {exc}",
                diagnostics={"manifest": str(self.manifest_path)},
            ) from exc
        if not isinstance(manifest, dict) or not isinstance(manifest.get("shards"), list):
            raise PipelineError(E_INVALID_INPUT, "manifest must contain a shards array")
        if int(manifest.get("schema_version", -1)) != 1:
            raise PipelineError(E_INVALID_INPUT, "unsupported manifest schema_version")
        for index, shard in enumerate(manifest["shards"]):
            for key in ("id", "index", "image", "width", "height", "size_tier", "truth"):
                if key not in shard:
                    raise PipelineError(
                        E_INVALID_INPUT,
                        f"shard is missing required key: {key}",
                        shard_id=str(shard.get("id", f"position-{index}")),
                    )
            if not isinstance(shard["truth"].get("table"), dict) or not isinstance(
                shard["truth"].get("chart"), dict
            ):
                raise PipelineError(
                    E_INVALID_INPUT,
                    "shard truth must declare table and chart objects",
                    shard_id=str(shard["id"]),
                )
        return manifest

    def _validate_images(self) -> None:
        seen_indexes = set()
        for shard in self.shards:
            shard_id = str(shard["id"])
            index = int(shard["index"])
            if index in seen_indexes:
                raise PipelineError(
                    E_INVALID_INPUT,
                    "shard indexes must be unique",
                    shard_id=shard_id,
                    diagnostics={"index": index},
                )
            seen_indexes.add(index)
            image_value = str(shard["image"])
            if image_value.startswith(("http://", "https://")):
                raise PipelineError(
                    E_INVALID_INPUT,
                    "remote image URLs are not accepted",
                    shard_id=shard_id,
                )
            image_path = (self.manifest_path.parent / image_value).resolve()
            try:
                with Image.open(image_path) as image:
                    actual_size = [int(image.width), int(image.height)]
            except Exception as exc:
                raise PipelineError(
                    E_INVALID_INPUT,
                    f"cannot open shard image: {exc}",
                    shard_id=shard_id,
                    diagnostics={"image": str(image_path)},
                ) from exc
            expected_size = [int(shard["width"]), int(shard["height"])]
            if actual_size != expected_size:
                raise PipelineError(
                    E_INVALID_INPUT,
                    "declared image dimensions do not match the PNG",
                    shard_id=shard_id,
                    diagnostics={"declared": expected_size, "actual": actual_size},
                )
            shard["_image_path"] = str(image_path)
            self.image_sha256[shard_id] = digest_file(image_path)

    def _schedule_signature(self) -> Dict[str, Any]:
        return {
            "accepted": [
                [entry["shard_id"], entry["estimated_pixels"]] for entry in self.schedule["accepted"]
            ],
            "unprocessed": [
                [entry["shard_id"], entry["reason"]] for entry in self.schedule["unprocessed"]
            ],
            "total_pixel_budget": self.schedule["total_pixel_budget"],
            "worker_pixel_quota": self.schedule["worker_pixel_quota"],
        }

    def _compute_run_id(self) -> str:
        payload = {
            "schema_version": PIPELINE_SCHEMA_VERSION,
            "pipeline_version": PIPELINE_VERSION,
            "manifest_sha256": digest_file(self.manifest_path),
            "image_sha256": self.image_sha256,
            "config_version": self.config_version,
            "schedule": self._schedule_signature(),
        }
        return "run-" + digest_json(payload)[:24]

    def _stage_fingerprints(self, shard: Dict[str, Any]) -> Dict[str, Any]:
        shard_id = str(shard["id"])
        detection_components = {
            "image_sha256": self.image_sha256[shard_id],
            "detection": self.configs["detection"],
            "pipeline_version": PIPELINE_VERSION,
        }
        detection_fp = digest_json({"stage": "detection", "components": detection_components})
        table_components = {
            "upstream_detection": detection_fp,
            "table": self.configs["table"],
            "declared_rows": int(shard["truth"]["table"]["rows"]),
            "declared_cols": int(shard["truth"]["table"]["cols"]),
        }
        table_fp = digest_json({"stage": "table", "components": table_components})
        chart_components = {
            "upstream_table": table_fp,
            "detection": detection_fp,
            "chart": self.configs["chart"],
        }
        chart_fp = digest_json({"stage": "chart", "components": chart_components})
        validation_components = {
            "upstream_table": table_fp,
            "upstream_chart": chart_fp,
            "budget": self.configs["budget"],
            "truth": shard["truth"],
            "schedule": self._schedule_signature(),
        }
        validation_fp = digest_json(
            {"stage": "validation", "components": validation_components}
        )
        return {
            "detection": {
                "input_fingerprint": detection_fp,
                "config_version": stage_config_version(self.configs, "detection"),
                "components": {
                    "image_sha256": self.image_sha256[shard_id],
                    "detection_config": self.configs["detection"],
                },
            },
            "table": {
                "input_fingerprint": table_fp,
                "config_version": stage_config_version(self.configs, "table"),
                "components": {
                    "table_config": self.configs["table"],
                    "declared_rows": int(shard["truth"]["table"]["rows"]),
                    "declared_cols": int(shard["truth"]["table"]["cols"]),
                },
            },
            "chart": {
                "input_fingerprint": chart_fp,
                "config_version": stage_config_version(self.configs, "chart"),
                "components": {"chart_config": self.configs["chart"]},
            },
            "validation": {
                "input_fingerprint": validation_fp,
                "config_version": stage_config_version(self.configs, "validation"),
                "components": {
                    "budget_config": self.configs["budget"],
                    "truth_sha256": digest_json(shard["truth"]),
                    "schedule": self._schedule_signature(),
                },
            },
        }

    def fingerprints(self, shard: Optional[Dict[str, Any]] = None) -> Any:
        if shard is not None:
            return self._stage_fingerprints(shard)
        return {
            "run_id": self.run_id,
            "pipeline_config_version": self.config_version,
            "shards": {
                str(item["id"]): self._stage_fingerprints(item)
                for item in self.accepted_shards
            },
        }

    def _version_snapshot(self) -> Dict[str, Any]:
        return {
            "schema_version": 1,
            "run_id": self.run_id,
            "config_version": self.config_version,
            "shards": {
                str(shard["id"]): self._stage_fingerprints(shard)
                for shard in self.accepted_shards
            },
        }

    def _read_previous_state(self) -> Optional[Dict[str, Any]]:
        path = self.state_dir / "last-run.json"
        if not path.is_file():
            return None
        try:
            return read_json(path)
        except Exception:
            return None

    def _compute_invalidation(self) -> List[Dict[str, Any]]:
        if self.previous_state and self.previous_state.get("run_id") == self.run_id:
            prior_report = self.previous_state.get("invalidation")
            if isinstance(prior_report, list):
                return copy.deepcopy(prior_report)
        current = self._version_snapshot()
        previous = self.previous_state
        report: List[Dict[str, Any]] = []
        previous_shards = previous.get("shards", {}) if previous else {}
        for shard in self.accepted_shards:
            shard_id = str(shard["id"])
            current_stages = current["shards"][shard_id]
            prior_stages = previous_shards.get(shard_id)
            if not prior_stages:
                report.append(
                    {
                        "shard_id": shard_id,
                        "earliest_stage": "detection",
                        "reason": "new_shard",
                        "invalidated_stages": list(STAGES),
                    }
                )
                continue
            earliest: Optional[str] = None
            reason = ""
            for stage in STAGES:
                prior = prior_stages.get(stage)
                if prior is None:
                    earliest = stage
                    reason = "missing_previous_stage"
                    break
                if prior.get("components") != current_stages[stage]["components"]:
                    earliest = stage
                    if stage == "detection":
                        reason = "image_or_detection_config_changed"
                    elif stage == "table":
                        reason = "table_config_or_declared_dimensions_changed"
                    elif stage == "chart":
                        reason = "chart_config_changed"
                    else:
                        reason = "budget_or_truth_changed"
                    break
                if prior.get("input_fingerprint") != current_stages[stage]["input_fingerprint"]:
                    earliest = stage
                    reason = "stage_fingerprint_changed"
                    break
            if earliest is None:
                report.append(
                    {
                        "shard_id": shard_id,
                        "earliest_stage": None,
                        "reason": "unchanged",
                        "invalidated_stages": [],
                    }
                )
            else:
                report.append(
                    {
                        "shard_id": shard_id,
                        "earliest_stage": earliest,
                        "reason": reason,
                        "invalidated_stages": list(STAGES[STAGE_INDEX[earliest] - 1 :]),
                    }
                )
        return report

    def _write_last_run_state(self) -> None:
        snapshot = self._version_snapshot()
        snapshot["invalidation"] = self.invalidation
        atomic_write_json(self.state_dir / "last-run.json", snapshot)

    def _load_current_outputs(self) -> None:
        stages = self.store.stages_by_shard()
        self.stage_outputs = {}
        for shard_id, stage_records in stages.items():
            for stage, record in stage_records.items():
                try:
                    artifact = self.store.read_artifact(record)
                except PipelineError:
                    continue
                expected = self._stage_fingerprints(self.shard_by_id[shard_id])[stage][
                    "input_fingerprint"
                ]
                if artifact.get("input_fingerprint") != expected:
                    raise PipelineError(
                        E_STALE_CHECKPOINT,
                        "checkpoint fingerprint does not match current stage inputs",
                        shard_id=shard_id,
                        diagnostics={
                            "stage": stage,
                            "recorded": artifact.get("input_fingerprint"),
                            "expected": expected,
                        },
                    )
                payload = artifact["payload"]
                self.stage_outputs[(shard_id, stage)] = payload
                self.stats["checkpoint_reused"][stage] = (
                    self.stats["checkpoint_reused"].get(stage, 0) + 1
                )

    def request_stop(self) -> Dict[str, Any]:
        atomic_write_json(self.stop_path, {"requested": True})
        return {"stop_requested": True, "path": str(self.stop_path)}

    def clear_stop(self) -> None:
        try:
            self.stop_path.unlink()
        except FileNotFoundError:
            pass

    def stop_requested(self) -> bool:
        return self.stop_path.is_file()

    def _stage_config(self, stage: str) -> Dict[str, Any]:
        return self.configs["budget"] if stage == "validation" else self.configs[stage]

    def _cache_path(self, stage: str, fingerprint: str) -> Path:
        return self.work_dir / "cache" / stage / f"{fingerprint}.json"

    def _read_cache(self, stage: str, shard_id: str, fingerprint: str) -> Optional[Dict[str, Any]]:
        path = self._cache_path(stage, fingerprint)
        if not path.is_file():
            return None
        try:
            artifact = read_json(path)
            if (
                artifact.get("stage") != stage
                or artifact.get("shard_id") != shard_id
                or artifact.get("input_fingerprint") != fingerprint
                or "payload" not in artifact
            ):
                raise ValueError("cache metadata mismatch")
            return artifact["payload"]
        except Exception:
            try:
                path.unlink()
            except OSError:
                pass
            return None

    def _artifact_document(
        self,
        shard: Dict[str, Any],
        stage: str,
        fingerprint: str,
        payload: Dict[str, Any],
    ) -> bytes:
        return dump_bytes(
            {
                "schema_version": 1,
                "shard_id": shard["id"],
                "shard_index": int(shard["index"]),
                "stage": stage,
                "input_fingerprint": fingerprint,
                "config_version": stage_config_version(self.configs, stage),
                "payload": payload,
            }
        )

    def _prerequisite_payloads(
        self, shard_id: str, stage: str
    ) -> Dict[str, Dict[str, Any]]:
        index = STAGE_INDEX[stage]
        result: Dict[str, Dict[str, Any]] = {}
        for prior_stage in STAGES[: index - 1]:
            payload = self.stage_outputs.get((shard_id, prior_stage))
            if payload is not None:
                result[prior_stage] = payload
        return result

    def _can_run_stage(self, shard_id: str, stage: str) -> Tuple[bool, str]:
        prior_stages = STAGES[: STAGE_INDEX[stage] - 1]
        for prior_stage in prior_stages:
            if (shard_id, prior_stage) not in self.stage_outputs:
                return False, f"missing_{prior_stage}_artifact"
        if stage in ("table", "chart"):
            detection = self.stage_outputs.get((shard_id, "detection"))
            if not detection or not detection.get("ok"):
                return False, "failed_detection_artifact"
        return True, ""

    def _check_explicit_stage_prerequisites(self, stage: str) -> None:
        if stage == "detection":
            return
        missing: List[Dict[str, Any]] = []
        for shard in self.accepted_shards:
            shard_id = str(shard["id"])
            for prior_stage in STAGES[: STAGE_INDEX[stage] - 1]:
                if (shard_id, prior_stage) not in self.stage_outputs:
                    missing.append({"shard_id": shard_id, "missing_stage": prior_stage})
        if missing:
            raise PipelineError(
                E_INVALID_STAGE,
                "requested stage is missing prerequisite artifacts",
                shard_id=missing[0]["shard_id"],
                diagnostics={
                    "requested_stage": stage,
                    "missing": missing,
                    "reason": "illegal_stage_jump",
                },
            )

    def _task_for(
        self, shard: Dict[str, Any], stage: str, fingerprint: str
    ) -> Dict[str, Any]:
        return {
            "stage": stage,
            "shard": shard,
            "image_path": shard["_image_path"],
            "config": self._stage_config(stage),
            "input_fingerprint": fingerprint,
            "prerequisites": self._prerequisite_payloads(str(shard["id"]), stage),
        }

    def _write_status(
        self,
        *,
        status: str,
        current_stage: Optional[str],
        stop_reason: Optional[str] = None,
    ) -> None:
        records = self.store.load_valid_records()
        stage_counts = {stage: 0 for stage in STAGES}
        for record in records:
            stage_counts[record["stage"]] += 1
        errors = []
        for (shard_id, stage), payload in sorted(self.stage_outputs.items()):
            if not payload.get("ok", False):
                error = dict(payload.get("error", {}))
                error.setdefault("stage", stage)
                error.setdefault("shard_id", shard_id)
                errors.append(error)
        value = {
            "run_id": self.run_id,
            "status": status,
            "current_stage": current_stage,
            "stop_requested": self.stop_requested(),
            "stop_reason": stop_reason,
            "checkpoint_sequence": records[-1]["sequence"] if records else 0,
            "stage_counts": stage_counts,
            "stats": self.stats,
            "accepted_shard_ids": self.accepted_ids,
            "unprocessed": self.schedule["unprocessed"],
            "errors": errors,
        }
        with FileLock(self.state_dir / ".state.lock"):
            atomic_write_json(self.state_dir / "status.json", value)

    def run(
        self,
        *,
        stage: str = "all",
        resume: bool = False,
        fresh: bool = False,
        stop_after: Optional[int] = None,
        execution_workers: Optional[int] = None,
    ) -> Dict[str, Any]:
        if stage != "all" and stage not in STAGE_INDEX:
            raise PipelineError(
                E_INVALID_STAGE,
                f"unknown stage: {stage}",
                diagnostics={"allowed": ["all", *STAGES]},
            )
        if fresh:
            run_root = self.store.root
            if run_root.exists():
                shutil.rmtree(run_root)
            self.store.records_dir.mkdir(parents=True, exist_ok=True)
            self.clear_stop()
            self.stage_outputs = {}
            for stage_name in STAGES:
                self.stats["checkpoint_reused"][stage_name] = 0
        if resume:
            self.clear_stop()
        if stop_after is not None and int(stop_after) < 0:
            raise PipelineError(E_INVALID_STAGE, "stop_after must be non-negative")

        self._write_last_run_state()
        stages_to_run = list(STAGES) if stage == "all" else [stage]
        if stage != "all":
            self._check_explicit_stage_prerequisites(stage)
        else:
            self._check_explicit_stage_prerequisites("detection")
            self._load_current_outputs()

        max_workers = (
            int(execution_workers)
            if execution_workers is not None
            else int(self.schedule["effective_workers"])
        )
        max_workers = min(max_workers, int(self.schedule["max_workers"]))
        if max_workers < 1:
            max_workers = 1

        completed_tasks = 0
        stopped = stop_after is not None and int(stop_after) == 0
        stop_reason: Optional[str] = "stop_after_reached" if stopped else None
        current_stage: Optional[str] = None
        self._write_status(status="running", current_stage=None)

        for stage_name in stages_to_run:
            if stopped:
                break
            current_stage = stage_name
            self._write_status(status="running", current_stage=stage_name)
            results: List[Tuple[Dict[str, Any], str, Dict[str, Any], bool]] = []
            pending: List[Tuple[Dict[str, Any], str, Dict[str, Any]]] = []
            for shard in self.accepted_shards:
                shard_id = str(shard["id"])
                existing = self.stage_outputs.get((shard_id, stage_name))
                if existing is not None:
                    continue
                can_run, reason = self._can_run_stage(shard_id, stage_name)
                if not can_run:
                    continue
                fingerprints = self._stage_fingerprints(shard)[stage_name]
                fingerprint = fingerprints["input_fingerprint"]
                cached = self._read_cache(stage_name, shard_id, fingerprint)
                if cached is not None:
                    self.stats["cache_reused"][stage_name] += 1
                    results.append((shard, fingerprint, cached, True))
                else:
                    pending.append((shard, fingerprint, self._task_for(shard, stage_name, fingerprint)))

            pending.sort(key=lambda item: (int(item[0]["index"]), str(item[0]["id"])))
            if pending and not self.stop_requested():
                if max_workers == 1:
                    with concurrent.futures.ProcessPoolExecutor(
                        max_workers=1, mp_context=_process_context()
                    ) as executor:
                        for start in range(0, len(pending), 1):
                            shard, fingerprint, task = pending[start]
                            future = executor.submit(execute_stage_task, task)
                            try:
                                payload = future.result()
                            except Exception as exc:
                                payload = error_payload(
                                    PipelineError(
                                        E_WORKER,
                                        "worker process failed",
                                        shard_id=str(shard["id"]),
                                        diagnostics={
                                            "stage": stage_name,
                                            "exception_type": type(exc).__name__,
                                            "message": str(exc),
                                        },
                                    )
                                )
                            results.append((shard, fingerprint, payload, False))
                            self.stats["computed"][stage_name] += 1
                            completed_tasks += 1
                            if self.stop_requested():
                                stopped = True
                                stop_reason = "stop_request"
                                break
                            if stop_after is not None and completed_tasks >= int(stop_after):
                                stopped = True
                                stop_reason = "stop_after_reached"
                                break
                else:
                    with concurrent.futures.ProcessPoolExecutor(
                        max_workers=max_workers, mp_context=_process_context()
                    ) as executor:
                        for start in range(0, len(pending), max_workers):
                            if self.stop_requested():
                                stopped = True
                                stop_reason = "stop_request"
                                break
                            batch = pending[start : start + max_workers]
                            future_map = {
                                executor.submit(execute_stage_task, task): (shard, fingerprint)
                                for shard, fingerprint, task in batch
                            }
                            for future, (shard, fingerprint) in future_map.items():
                                try:
                                    payload = future.result()
                                except Exception as exc:
                                    payload = error_payload(
                                        PipelineError(
                                            E_WORKER,
                                            "worker process failed",
                                            shard_id=str(shard["id"]),
                                            diagnostics={
                                                "stage": stage_name,
                                                "exception_type": type(exc).__name__,
                                                "message": str(exc),
                                            },
                                        )
                                    )
                                results.append((shard, fingerprint, payload, False))
                                self.stats["computed"][stage_name] += 1
                                completed_tasks += 1
                            if self.stop_requested():
                                stopped = True
                                stop_reason = "stop_request"
                                break
                            if stop_after is not None and completed_tasks >= int(stop_after):
                                stopped = True
                                stop_reason = "stop_after_reached"
                                break
            elif self.stop_requested():
                stopped = True
                stop_reason = "stop_request"

            results.sort(key=lambda item: (int(item[0]["index"]), str(item[0]["id"])))
            for shard, fingerprint, payload, _from_cache in results:
                artifact_bytes = self._artifact_document(shard, stage_name, fingerprint, payload)
                record = self.store.publish(
                    shard_id=str(shard["id"]),
                    shard_index=int(shard["index"]),
                    stage=stage_name,
                    input_fingerprint=fingerprint,
                    config_version=stage_config_version(self.configs, stage_name),
                    artifact_bytes=artifact_bytes,
                )
                self.stage_outputs[(str(shard["id"]), stage_name)] = payload
            self._write_status(status="stopped" if stopped else "running", current_stage=stage_name, stop_reason=stop_reason)
            if stopped:
                break

        if stopped:
            final_status = "stopped"
        else:
            all_complete = True
            any_error = False
            for shard in self.accepted_shards:
                shard_id = str(shard["id"])
                for stage_name in STAGES:
                    payload = self.stage_outputs.get((shard_id, stage_name))
                    if payload is None:
                        all_complete = False
                    elif not payload.get("ok", False):
                        any_error = True
            if all_complete and not any_error:
                final_status = "completed"
            elif any_error:
                final_status = "completed_with_errors"
            else:
                final_status = "partial"
        self._write_status(status=final_status, current_stage=None, stop_reason=stop_reason)
        return {
            "run_id": self.run_id,
            "status": final_status,
            "stage_requested": stage,
            "stopped": stopped,
            "stop_reason": stop_reason,
            "completed_stage_tasks": completed_tasks,
            "pipeline_config_version": self.config_version,
            "accepted_shard_ids": self.accepted_ids,
            "unprocessed": self.schedule["unprocessed"],
            "invalidation": self.invalidation,
            "stats": self.stats,
            "checkpoint_sequence": self.store.last_sequence(),
        }

    def status(self) -> Dict[str, Any]:
        status_path = self.state_dir / "status.json"
        if status_path.is_file():
            try:
                status = read_json(status_path)
                if status.get("run_id") == self.run_id:
                    return status
            except Exception:
                pass
        records = self.store.load_valid_records()
        stage_counts = {stage: 0 for stage in STAGES}
        for record in records:
            stage_counts[record["stage"]] += 1
        return {
            "run_id": self.run_id,
            "status": "not_started" if not records else "partial",
            "current_stage": None,
            "stop_requested": self.stop_requested(),
            "stop_reason": None,
            "checkpoint_sequence": records[-1]["sequence"] if records else 0,
            "stage_counts": stage_counts,
            "stats": self.stats,
            "accepted_shard_ids": self.accepted_ids,
            "unprocessed": self.schedule["unprocessed"],
            "errors": [],
        }

    def budget_report(self) -> Dict[str, Any]:
        return copy.deepcopy(self.schedule)

    def _collect_records(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        return self.store.stages_by_shard()

    def export_data(self) -> Dict[str, Any]:
        records_by_shard = self._collect_records()
        shard_exports: List[Dict[str, Any]] = []
        validation_values: List[Dict[str, Any]] = []
        for shard in sorted(self.accepted_shards, key=lambda item: int(item["index"])):
            shard_id = str(shard["id"])
            stages = records_by_shard.get(shard_id, {})
            stage_exports: Dict[str, Any] = {}
            errors: List[Dict[str, Any]] = []
            for stage_name in STAGES:
                record = stages.get(stage_name)
                if record is None:
                    stage_exports[stage_name] = None
                    continue
                artifact = self.store.read_artifact(record)
                payload = artifact["payload"]
                if payload.get("ok", False):
                    stage_exports[stage_name] = payload.get("result")
                else:
                    stage_exports[stage_name] = None
                    error = dict(payload.get("error", {}))
                    error.setdefault("stage", stage_name)
                    error.setdefault("shard_id", shard_id)
                    errors.append(error)
            complete = all(stage_exports.get(name) is not None for name in STAGES)
            shard_status = "complete" if complete else ("error" if errors else "partial")
            validation = stage_exports.get("validation")
            if validation:
                validation_values.append(validation)
            shard_exports.append(
                {
                    "shard_id": shard_id,
                    "shard_index": int(shard["index"]),
                    "status": shard_status,
                    "image_sha256": self.image_sha256[shard_id],
                    "fingerprints": self._stage_fingerprints(shard),
                    "detection": stage_exports.get("detection"),
                    "table": stage_exports.get("table"),
                    "chart": stage_exports.get("chart"),
                    "validation": stage_exports.get("validation"),
                    "errors": errors,
                }
            )

        table_ious = [item["table_region_iou"] for item in validation_values]
        chart_ious = [item["chart_region_iou"] for item in validation_values]
        cell_accuracies = [item["cell_row_col_accuracy"] for item in validation_values]
        chart_errors = [
            item["chart"]["mean_abs_error"]
            for item in validation_values
            if item["chart"]["mean_abs_error"] is not None
        ]
        all_errors: List[Dict[str, Any]] = []
        for shard in shard_exports:
            all_errors.extend(shard["errors"])
        summary = {
            "schema_version": 1,
            "run_id": self.run_id,
            "pipeline_config_version": self.config_version,
            "seed": self.manifest.get("seed"),
            "accepted_shard_count": len(self.accepted_shards),
            "processed_shard_count": len(validation_values),
            "unprocessed_count": len(self.schedule["unprocessed"]),
            "table_region_iou_mean": rounded(sum(table_ious) / len(table_ious))
            if table_ious
            else None,
            "chart_region_iou_mean": rounded(sum(chart_ious) / len(chart_ious))
            if chart_ious
            else None,
            "cell_row_col_accuracy_mean": rounded(sum(cell_accuracies) / len(cell_accuracies))
            if cell_accuracies
            else None,
            "chart_series_mae_mean": rounded(sum(chart_errors) / len(chart_errors))
            if chart_errors
            else None,
            "error_count": len(all_errors),
            "errors": sorted(
                all_errors,
                key=lambda item: (
                    str(item.get("shard_id", "")),
                    str(item.get("stage", "")),
                    str(item.get("code", "")),
                ),
            ),
        }
        return {
            "schema_version": 1,
            "run_id": self.run_id,
            "manifest": {
                "path": str(self.manifest_path.relative_to(self.root))
                if self.manifest_path.is_relative_to(self.root)
                else str(self.manifest_path),
                "sha256": digest_file(self.manifest_path),
                "seed": self.manifest.get("seed"),
            },
            "pipeline_config_version": self.config_version,
            "budget": self.schedule,
            "invalidation": self.invalidation,
            "shards": shard_exports,
            "unprocessed": self.schedule["unprocessed"],
            "summary": summary,
        }
