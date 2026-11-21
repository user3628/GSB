"""Job lifecycle, version plans, invalidation propagation, and status queries."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from .activation import activation_schedule
from .checkpoints import scan_stage, stage_content_digest
from .config import OperatorConfig, load_config
from .errors import (
    E_CONFIG,
    E_INPUT,
    E_INVALID_STAGE,
    E_NOT_FOUND,
    E_PATH,
    BusinessError,
)
from .manifest import (
    find_manifest_entry,
    load_manifest,
    manifest_shape,
    validate_source_against_manifest,
)
from .paths import STAGES, FileLock, JobPaths, WorkspacePaths
from .reorder import natural_target_shape, reorder_schedule
from .tensorio import TensorInput, load_npz_tensor
from .util import (
    append_jsonl_locked,
    atomic_write_json,
    canonical_json,
    digest_json,
    read_json,
)

JOB_SCHEMA = "blocklab.job.v1"
STATE_SCHEMA = "blocklab.job-state.v1"


def _stage_plans(
    *,
    config: OperatorConfig,
    input_digest: str,
    source_shape: tuple[int, ...],
    declared_target_shape: tuple[int, ...],
    actual_target_shape: tuple[int, ...],
) -> dict[str, dict[str, Any]]:
    activation_plan = digest_json(
        {
            "stage": "activation",
            "input_digest": input_digest,
            "source_shape": list(source_shape),
            "activation": config.activation,
            "row_block": config.row_block,
            "col_block": config.col_block,
            "config_version": config.stage_config_version("activation"),
        }
    )
    reorder_plan = digest_json(
        {
            "stage": "reorder",
            "upstream_activation_plan": activation_plan,
            "source_shape": list(source_shape),
            "declared_target_shape": list(declared_target_shape),
            "actual_target_shape": list(actual_target_shape),
            "config_target_shape": list(config.target_shape) if config.target_shape else None,
            "reorder_mode": config.reorder_mode,
            "row_block": config.row_block,
            "col_block": config.col_block,
            "config_version": config.stage_config_version("reorder"),
        }
    )
    reduce_plan = digest_json(
        {
            "stage": "reduce",
            "upstream_reorder_plan": reorder_plan,
            "sum_strategy": config.sum_strategy,
            "abs_tol": config.abs_tol,
            "rel_tol": config.rel_tol,
            "config_version": config.stage_config_version("reduce"),
        }
    )
    return {
        "activation": {
            "fingerprint": activation_plan,
            "config_version": config.stage_config_version("activation"),
        },
        "reorder": {
            "fingerprint": reorder_plan,
            "config_version": config.stage_config_version("reorder"),
        },
        "reduce": {
            "fingerprint": reduce_plan,
            "config_version": config.stage_config_version("reduce"),
        },
    }


def _changed_reasons(
    *,
    old_state: dict[str, Any] | None,
    old_config: OperatorConfig | None,
    new_config: OperatorConfig,
    input_digest: str,
    source_shape: tuple[int, ...],
    declared_target_shape: tuple[int, ...],
) -> list[str]:
    if old_state is None:
        return ["initial_import"]
    reasons: list[str] = []
    if old_state.get("input_digest") != input_digest:
        reasons.append("input_content_changed")
    if tuple(old_state.get("source_shape", [])) != tuple(source_shape):
        reasons.append("source_shape_changed")
    if old_state.get("declared_target_shape") != list(declared_target_shape):
        reasons.append("declared_target_shape_changed")
    if old_config is not None:
        if old_config.activation != new_config.activation:
            reasons.append("activation_type_changed")
        if (old_config.row_block, old_config.col_block) != (
            new_config.row_block,
            new_config.col_block,
        ):
            reasons.append("block_size_changed")
        if old_config.reorder_mode != new_config.reorder_mode:
            reasons.append("reorder_mode_changed")
        if old_config.target_shape != new_config.target_shape:
            reasons.append("config_target_shape_changed")
        if (
            old_config.sum_strategy,
            old_config.abs_tol,
            old_config.rel_tol,
        ) != (
            new_config.sum_strategy,
            new_config.abs_tol,
            new_config.rel_tol,
        ):
            reasons.append("reduction_policy_changed")
    return reasons or ["fingerprint_changed"]


class Workbench:
    def __init__(self, root: Path, workdir: Path | str = "work") -> None:
        self.root = Path(root).resolve()
        self.paths = WorkspacePaths(self.root, workdir)
        self.paths.ensure()

    # ------------------------------------------------------------------ catalog
    def _read_catalog(self) -> dict[str, Any]:
        if not self.paths.catalog_file.exists():
            return {"schema": "blocklab.catalog.v1", "jobs": []}
        try:
            value = read_json(self.paths.catalog_file)
        except Exception:
            return {"schema": "blocklab.catalog.v1", "jobs": []}
        if not isinstance(value, dict) or not isinstance(value.get("jobs"), list):
            return {"schema": "blocklab.catalog.v1", "jobs": []}
        return value

    def _update_catalog(self, job_record: dict[str, Any]) -> None:
        with FileLock(self.paths.workdir_lock):
            catalog = self._read_catalog()
            jobs = [item for item in catalog["jobs"] if item.get("job_id") != job_record["job_id"]]
            jobs.append(
                {
                    "job_id": job_record["job_id"],
                    "source_path": job_record["source_path"],
                    "config_path": job_record["config_path"],
                    "manifest_path": job_record.get("manifest_path"),
                    "shard_id": job_record["shard_id"],
                }
            )
            jobs.sort(key=lambda item: item["job_id"])
            atomic_write_json(
                self.paths.catalog_file,
                {"schema": "blocklab.catalog.v1", "jobs": jobs},
            )

    def list_jobs(self) -> list[dict[str, Any]]:
        return self._read_catalog()["jobs"]

    def load_job(self, job_id: str) -> dict[str, Any]:
        paths = self.paths.job(job_id)
        if not paths.job_file.exists():
            raise BusinessError(E_NOT_FOUND, "job not found", job_id=job_id)
        try:
            job = read_json(paths.job_file)
        except Exception as exc:
            raise BusinessError(E_CONFIG, f"invalid job record: {exc}", job_id=job_id) from exc
        if job.get("schema") != JOB_SCHEMA:
            raise BusinessError(E_CONFIG, "unsupported job schema", job_id=job_id)
        return job

    # ------------------------------------------------------------------- import
    def import_job(
        self,
        *,
        source_path: str | Path,
        config_path: str | Path,
        job_id: str | None = None,
        manifest_path: str | Path | None = None,
        shard_id: str | None = None,
    ) -> dict[str, Any]:
        source = self.paths.resolve_root_path(source_path)
        config_file = self.paths.resolve_root_path(config_path)
        manifest_file = (
            self.paths.resolve_root_path(manifest_path)
            if manifest_path is not None
            else self.default_manifest()
        )
        tensor = load_npz_tensor(source)
        config = load_config(config_file)
        entry = None
        if manifest_file is not None:
            manifest = load_manifest(manifest_file)
            entry = find_manifest_entry(
                manifest,
                source_relative_path=source.relative_to(self.root).as_posix(),
                shard_id=shard_id,
            )
            validate_source_against_manifest(tuple(tensor.array.shape), entry)
        declared_target = manifest_shape(entry, "target_shape")
        actual_target = natural_target_shape(tuple(tensor.array.shape), config.reorder_mode)
        if declared_target is None:
            declared_target = config.target_shape or actual_target
        inferred_job_id = job_id or Path(source).stem
        paths = self.paths.job(inferred_job_id)
        paths.job_dir.mkdir(parents=True, exist_ok=True)
        job = {
            "schema": JOB_SCHEMA,
            "job_id": inferred_job_id,
            "shard_id": (entry or {}).get("id") or shard_id or Path(source).stem,
            "source_path": source.relative_to(self.root).as_posix(),
            "config_path": config_file.relative_to(self.root).as_posix(),
            "manifest_path": (
                manifest_file.relative_to(self.root).as_posix()
                if manifest_file is not None
                else None
            ),
            "source_key": tensor.key,
            "declared_source_shape": list(tensor.array.shape),
            "declared_target_shape": list(declared_target),
            "layout": "4d_bhsd" if tensor.array.ndim == 4 else "2d",
        }
        atomic_write_json(paths.job_file, job)
        self._update_catalog(job)
        self.sync_job(inferred_job_id)
        return self.fingerprints(inferred_job_id)

    def default_manifest(self) -> Path | None:
        candidate = self.root / "samples" / "shape_manifest.json"
        return candidate if candidate.exists() else None

    # -------------------------------------------------------------- load context
    def load_context(self, job_id: str) -> tuple[dict[str, Any], JobPaths, TensorInput, OperatorConfig, tuple[int, ...], tuple[int, ...]]:
        job = self.load_job(job_id)
        paths = self.paths.job(job_id)
        source = self.paths.resolve_root_path(job["source_path"])
        config_file = self.paths.resolve_root_path(job["config_path"])
        tensor = load_npz_tensor(source)
        if tuple(tensor.array.shape) != tuple(job.get("declared_source_shape", [])):
            # A changed source shape remains a detectable input change; the manifest is the
            # authority for the declared shape and reorder will report a shape mismatch.
            pass
        config = load_config(config_file)
        actual_target = natural_target_shape(tuple(tensor.array.shape), config.reorder_mode)
        declared_target = tuple(job["declared_target_shape"])
        return job, paths, tensor, config, tuple(tensor.array.shape), declared_target

    def _load_state(self, paths: JobPaths) -> dict[str, Any] | None:
        if not paths.state_file.exists():
            return None
        try:
            return read_json(paths.state_file)
        except Exception:
            return None

    def _old_config_from_state(
        self, job: dict[str, Any], state: dict[str, Any] | None
    ) -> OperatorConfig | None:
        if state is None:
            return None
        # The previous normalized config is stored beside its digest so reason reporting
        # remains meaningful even after job.json has already been updated.
        raw = state.get("config_normalized")
        if isinstance(raw, dict):
            try:
                from .config import parse_config

                return parse_config(
                    {
                        "activation": raw.get("activation"),
                        "reorder": {
                            "mode": raw.get("reorder_mode"),
                            "target_shape": raw.get("target_shape"),
                        },
                        "blocking": {
                            "row_block": raw.get("row_block"),
                            "col_block": raw.get("col_block"),
                        },
                        "reduction": {
                            "strategy": raw.get("sum_strategy"),
                            "abs_tol": raw.get("abs_tol"),
                            "rel_tol": raw.get("rel_tol"),
                        },
                        "version": raw.get("version", 1),
                    }
                )
            except Exception:
                return None
        return None

    # ----------------------------------------------------------------- sync/state
    def sync_job(self, job_id: str) -> dict[str, Any]:
        job, paths, tensor, config, source_shape, declared_target = self.load_context(job_id)
        actual_target = natural_target_shape(source_shape, config.reorder_mode)
        plans = _stage_plans(
            config=config,
            input_digest=tensor.digest,
            source_shape=source_shape,
            declared_target_shape=declared_target,
            actual_target_shape=actual_target,
        )
        with FileLock(paths.meta_lock):
            old_state = self._load_state(paths)
            changed = []
            if old_state is not None:
                old_plans = old_state.get("stage_plans", {})
                for stage in STAGES:
                    if old_plans.get(stage, {}).get("fingerprint") != plans[stage]["fingerprint"]:
                        changed.append(stage)
            if old_state is None:
                epochs = {stage: 1 for stage in STAGES}
                state = self._new_state(
                    job=job,
                    config=config,
                    input_digest=tensor.digest,
                    source_shape=source_shape,
                    declared_target=declared_target,
                    actual_target=actual_target,
                    plans=plans,
                    epochs=epochs,
                )
                paths.job_dir.mkdir(parents=True, exist_ok=True)
                atomic_write_json(paths.state_file, state)
                return state
            if not changed:
                if old_state.get("config_digest") != config.digest:
                    # Only non-behavioural metadata changed. Keep epochs, refresh audit fields.
                    old_state["config_digest"] = config.digest
                    old_state["config_version"] = config.version
                    old_state["config_normalized"] = config.normalized()
                    old_state["stage_plans"] = plans
                    atomic_write_json(paths.state_file, old_state)
                return old_state

            earliest = min(STAGES.index(stage) for stage in changed)
            affected = list(STAGES[earliest:])
            old_plans = old_state.get("stage_plans", {})
            old_config = self._old_config_from_state(job, old_state)
            reasons = _changed_reasons(
                old_state=old_state,
                old_config=old_config,
                new_config=config,
                input_digest=tensor.digest,
                source_shape=source_shape,
                declared_target_shape=declared_target,
            )
            epochs = dict(old_state.get("stage_epochs", {}))
            affected_blocks: dict[str, Any] = {}
            for stage in affected:
                old_epoch = int(epochs.get(stage, 1))
                archive_stage_dir(paths, stage, old_epoch, old_plans.get(stage, {}).get("fingerprint"))
                epochs[stage] = old_epoch + 1
                count = self.stage_block_count(
                    stage=stage,
                    source_shape=source_shape,
                    target_shape=actual_target,
                    config=config,
                )
                affected_blocks[stage] = {
                    "count": count,
                    "ranges": [[0, count - 1]] if count else [],
                    "interval": f"[0,{count - 1}]" if count else "[]",
                }
            self._remove_invalid_outputs(paths, affected)
            event_seq = self._next_invalidation_sequence(paths)
            state = self._new_state(
                job=job,
                config=config,
                input_digest=tensor.digest,
                source_shape=source_shape,
                declared_target=declared_target,
                actual_target=actual_target,
                plans=plans,
                epochs=epochs,
            )
            event = {
                "schema": "blocklab.invalidation.v1",
                "event_sequence": event_seq,
                "job_id": job_id,
                "earliest_stage": affected[0],
                "affected_stages": affected,
                "unaffected_stages": list(STAGES[:earliest]),
                "reasons": reasons,
                "old_fingerprints": {
                    stage: old_plans.get(stage, {}).get("fingerprint") for stage in STAGES
                },
                "new_fingerprints": {stage: plans[stage]["fingerprint"] for stage in STAGES},
                "old_config_version": old_state.get("config_version"),
                "new_config_version": config.version,
                "old_input_digest": old_state.get("input_digest"),
                "new_input_digest": tensor.digest,
                "affected_blocks": affected_blocks,
            }
            atomic_write_json(paths.state_file, state)
            append_jsonl_locked(paths.invalidations_file, event)
            paths.last_error_file.unlink(missing_ok=True)
            return state

    def _new_state(
        self,
        *,
        job: dict[str, Any],
        config: OperatorConfig,
        input_digest: str,
        source_shape: tuple[int, ...],
        declared_target: tuple[int, ...],
        actual_target: tuple[int, ...],
        plans: dict[str, dict[str, Any]],
        epochs: dict[str, int],
    ) -> dict[str, Any]:
        return {
            "schema": STATE_SCHEMA,
            "job_id": job["job_id"],
            "input_digest": input_digest,
            "config_digest": config.digest,
            "config_version": config.version,
            "config_normalized": config.normalized(),
            "source_shape": list(source_shape),
            "declared_target_shape": list(declared_target),
            "actual_target_shape": list(actual_target),
            "stage_plans": plans,
            "stage_epochs": epochs,
        }

    def _next_invalidation_sequence(self, paths: JobPaths) -> int:
        if not paths.invalidations_file.exists():
            return 1
        count = 0
        with paths.invalidations_file.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    json.loads(line)
                    count += 1
                except Exception:
                    continue
        return count + 1

    def _remove_invalid_outputs(self, paths: JobPaths, affected: list[str]) -> None:
        if "activation" in affected:
            for name in ("activation.npy", "activation.meta.json", "reordered.npy", "reordered.meta.json", "summary.json"):
                paths.output_path(name).unlink(missing_ok=True)
        elif "reorder" in affected:
            for name in ("reordered.npy", "reordered.meta.json", "summary.json"):
                paths.output_path(name).unlink(missing_ok=True)
        elif "reduce" in affected:
            paths.output_path("summary.json").unlink(missing_ok=True)

    # ------------------------------------------------------------------ helpers
    def state(self, job_id: str) -> dict[str, Any]:
        return self.sync_job(job_id)

    def source(self, job_id: str) -> TensorInput:
        _, _, tensor, _, _, _ = self.load_context(job_id)
        return tensor

    def config(self, job_id: str) -> OperatorConfig:
        _, _, _, config, _, _ = self.load_context(job_id)
        return config

    def target_shape(self, job_id: str) -> tuple[int, ...]:
        state = self.sync_job(job_id)
        return tuple(state["actual_target_shape"])

    def stage_block_count(
        self,
        *,
        stage: str,
        source_shape: tuple[int, ...],
        target_shape: tuple[int, ...],
        config: OperatorConfig,
    ) -> int:
        if stage == "activation":
            return len(activation_schedule(source_shape, config.row_block, config.col_block))
        return len(reorder_schedule(source_shape, config, target_shape))

    def stage_schedule(
        self,
        *,
        stage: str,
        source_shape: tuple[int, ...],
        target_shape: tuple[int, ...],
        config: OperatorConfig,
    ) -> list[dict[str, Any]]:
        if stage == "activation":
            return activation_schedule(source_shape, config.row_block, config.col_block)
        return reorder_schedule(source_shape, config, target_shape)

    def stage_input_fingerprint(
        self,
        *,
        stage: str,
        input_digest: str,
        activation_records: list[dict[str, Any]] | None = None,
        activation_plan: str | None = None,
        reorder_records: list[dict[str, Any]] | None = None,
        reorder_plan: str | None = None,
    ) -> str:
        if stage == "activation":
            return input_digest
        if stage == "reorder":
            if activation_records is None or activation_plan is None:
                return "<activation-incomplete>"
            return stage_content_digest(
                activation_records, stage="activation", plan_fingerprint=activation_plan
            )
        if reorder_records is None or reorder_plan is None:
            return "<reorder-incomplete>"
        return stage_content_digest(
            reorder_records, stage="reorder", plan_fingerprint=reorder_plan
        )

    def scan_all_stages(self, job_id: str) -> dict[str, Any]:
        state = self.sync_job(job_id)
        job, paths, tensor, config, source_shape, declared_target = self.load_context(job_id)
        target_shape = tuple(state["actual_target_shape"])
        epochs = state["stage_epochs"]
        plans = state["stage_plans"]
        scans: dict[str, Any] = {}
        for stage in STAGES:
            schedule = self.stage_schedule(
                stage=stage,
                source_shape=source_shape,
                target_shape=target_shape,
                config=config,
            )
            input_fp = self.stage_input_fingerprint(
                stage=stage,
                input_digest=tensor.digest,
                activation_records=scans.get("activation", {}).get("records"),
                activation_plan=plans["activation"]["fingerprint"],
                reorder_records=scans.get("reorder", {}).get("records"),
                reorder_plan=plans["reorder"]["fingerprint"],
            )
            scan = scan_stage(
                paths,
                stage=stage,
                epoch=int(epochs[stage]),
                plan_fingerprint=plans[stage]["fingerprint"],
                input_fingerprint=input_fp,
                config_version=plans[stage]["config_version"],
                schedule=schedule,
            )
            if scan["complete"]:
                scan["content_digest"] = stage_content_digest(
                    scan["records"], stage=stage, plan_fingerprint=plans[stage]["fingerprint"]
                )
            else:
                scan["content_digest"] = None
            scans[stage] = scan
        version_fingerprint = digest_json(
            {
                "schema": "blocklab.version-fingerprint.v1",
                "input_digest": tensor.digest,
                "config_version": config.version,
                "plans": {stage: plans[stage]["fingerprint"] for stage in STAGES},
                "content": {stage: scans[stage]["content_digest"] for stage in STAGES},
            }
        )
        return {
            "state": state,
            "scans": scans,
            "version_fingerprint": version_fingerprint,
        }

    def job_status(self, job_id: str) -> dict[str, Any]:
        scan_bundle = self.scan_all_stages(job_id)
        state = scan_bundle["state"]
        job = self.load_job(job_id)
        paths = self.paths.job(job_id)
        stages: dict[str, Any] = {}
        total_alerts = 0
        for stage in STAGES:
            scan = scan_bundle["scans"][stage]
            alerts = sum(1 for record in scan["records"] if record.get("status") != "ok")
            total_alerts += alerts
            stages[stage] = {
                "epoch": scan["epoch"],
                "complete": scan["complete"],
                "valid_blocks": scan["valid_blocks"],
                "expected_blocks": scan["expected_blocks"],
                "last_sequence": scan["valid_blocks"],
                "next_sequence": scan["valid_blocks"] + 1,
                "ignored_records": scan["ignored"],
                "content_digest": scan["content_digest"],
                "fingerprint": state["stage_plans"][stage]["fingerprint"],
                "alerts": alerts,
            }
        complete = all(item["complete"] for item in stages.values())
        run_state = None
        if paths.run_state_file.exists():
            try:
                run_state = read_json(paths.run_state_file)
            except Exception:
                run_state = None
        last_error = None
        if paths.last_error_file.exists():
            try:
                last_error = read_json(paths.last_error_file)
            except Exception:
                last_error = None
        return {
            "ok": True,
            "job": job,
            "complete": complete,
            "business_code": "E_NUMERIC" if total_alerts else None,
            "alerts": total_alerts,
            "version_fingerprint": scan_bundle["version_fingerprint"],
            "input_digest": state["input_digest"],
            "config_version": state["config_version"],
            "source_shape": state["source_shape"],
            "declared_target_shape": state["declared_target_shape"],
            "actual_target_shape": state["actual_target_shape"],
            "stage_epochs": state["stage_epochs"],
            "stages": stages,
            "run_state": run_state,
            "last_error": last_error,
            "stop_requested": paths.stop_file.exists(),
        }

    def fingerprints(self, job_id: str) -> dict[str, Any]:
        scan_bundle = self.scan_all_stages(job_id)
        state = scan_bundle["state"]
        return {
            "ok": True,
            "job_id": job_id,
            "input_digest": state["input_digest"],
            "config_digest": state["config_digest"],
            "config_version": state["config_version"],
            "stage_plans": state["stage_plans"],
            "stage_epochs": state["stage_epochs"],
            "stage_content": {
                stage: scan_bundle["scans"][stage]["content_digest"] for stage in STAGES
            },
            "version_fingerprint": scan_bundle["version_fingerprint"],
        }

    def diagnostics(self, job_id: str, *, stage: str | None = None) -> dict[str, Any]:
        if stage is not None and stage not in STAGES:
            raise BusinessError(E_INVALID_STAGE, "unknown stage", requested_stage=stage)
        scan_bundle = self.scan_all_stages(job_id)
        records: list[dict[str, Any]] = []
        for current_stage in STAGES:
            if stage is not None and current_stage != stage:
                continue
            scan = scan_bundle["scans"][current_stage]
            for record in scan["records"]:
                if record.get("status") == "ok" and not record.get("metadata", {}).get("diagnostics"):
                    continue
                records.append(
                    {
                        "stage": current_stage,
                        "sequence": record["sequence"],
                        "epoch": record["epoch"],
                        "block": record["block"],
                        "status": record["status"],
                        "diagnostics": record.get("metadata", {}).get("diagnostics"),
                        "metadata": record.get("metadata", {}),
                        "product_digest": record["product_digest"],
                    }
                )
        return {
            "ok": True,
            "job_id": job_id,
            "business_code": "E_NUMERIC" if records else None,
            "diagnostics": records,
        }

    def invalidations(self, job_id: str) -> dict[str, Any]:
        self.sync_job(job_id)
        paths = self.paths.job(job_id)
        events: list[dict[str, Any]] = []
        corrupted = 0
        if paths.invalidations_file.exists():
            with paths.invalidations_file.open("r", encoding="utf-8") as handle:
                for line in handle:
                    try:
                        events.append(json.loads(line))
                    except Exception:
                        corrupted += 1
        return {
            "ok": True,
            "job_id": job_id,
            "events": events,
            "corrupted_lines": corrupted,
        }

    def request_stop(self, job_id: str) -> dict[str, Any]:
        paths = self.paths.job(job_id)
        if not paths.job_file.exists():
            raise BusinessError(E_NOT_FOUND, "job not found", job_id=job_id)
        paths.stop_file.write_text("stop\n", encoding="utf-8")
        return {"ok": True, "job_id": job_id, "stop_requested": True}

    def clear_stop(self, job_id: str) -> None:
        self.paths.job(job_id).stop_file.unlink(missing_ok=True)

    def stop_requested(self, job_id: str) -> bool:
        return self.paths.job(job_id).stop_file.exists()

    def write_last_error(self, job_id: str, error: BusinessError) -> None:
        paths = self.paths.job(job_id)
        if paths.job_file.exists():
            atomic_write_json(paths.last_error_file, error.to_dict())

    def write_run_state(self, job_id: str, value: dict[str, Any]) -> None:
        atomic_write_json(self.paths.job(job_id).run_state_file, value)


def archive_stage_dir(
    paths: JobPaths, stage: str, epoch: int, plan_fingerprint: str | None
) -> None:
    source = paths.stage_dir(stage, epoch)
    if not source.exists():
        return
    with FileLock(paths.stage_lock(stage)):
        paths.archive_dir.mkdir(parents=True, exist_ok=True)
        suffix = (plan_fingerprint or "unknown")[:12]
        destination = paths.archive_dir / f"{stage}-e{epoch:06d}-{suffix}"
        if destination.exists():
            destination = paths.archive_dir / f"{stage}-e{epoch:06d}-{suffix}-{len(list(paths.archive_dir.iterdir()))}"
        shutil.move(str(source), str(destination))

