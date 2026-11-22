"""High-level workbench orchestration."""

from __future__ import annotations

import multiprocessing as mp
import shutil
from pathlib import Path
from typing import Any

from .chain import ParsedChain, parse_chain
from .checkpoint import CheckpointStore
from .config import Config
from .errors import (
    E_INCOMPLETE,
    E_INPUT_MISSING,
    E_STOPPED,
    E_TASK_INVALID,
    E_WORKER_PROCESS_EXITED,
    WorkbenchError,
)
from .filelock import FileLock
from .imaging import load_rgb
from .models import TaskSpec, load_tasks
from .runner import (
    ensure_parsed_checkpoint,
    process_single_job,
    slot_worker_entry,
    validate_stage_name,
    worker_result_path,
)
from .scheduler import PreparedTask, SchedulePlan, build_schedule
from .util import (
    atomic_write_bytes,
    atomic_write_json,
    ensure_dir,
    path_within,
    pretty_json_bytes,
    read_json_file,
    relative_posix,
    remove_file_if_exists,
    reset_runtime_dirs,
    sha256_bytes,
    sha256_json,
)
from .versioning import (
    STAGES,
    STAGE_INDEX,
    TaskVersionSet,
    changed_component_names,
    compute_task_versions,
)


class Workbench:
    def __init__(
        self,
        workspace: str | Path,
        *,
        tasks_file: str | Path | None = None,
        config_file: str | Path | None = None,
        shards_dir: str | Path | None = None,
    ):
        self.workspace = Path(workspace).resolve()
        self.tasks_file = Path(tasks_file) if tasks_file else self.workspace / "tasks.jsonl"
        if config_file:
            self.config_file = Path(config_file)
        else:
            preferred = self.workspace / "config.yaml"
            alternate = self.workspace / "schedule.yaml"
            self.config_file = preferred if preferred.exists() else alternate
        self.shards_dir = Path(shards_dir) if shards_dir else self.workspace / "inputs"
        self.stop_file = self.workspace / "control" / "STOP"
        self.reports_dir = self.workspace / "reports"
        self.metadata_dir = self.workspace / "metadata"
        self.history_file = self.metadata_dir / "invalidation_events.json"

    def load_config(self) -> Config:
        return Config.load(self.config_file)

    def load_tasks(self) -> list[TaskSpec]:
        return load_tasks(self.tasks_file)

    def _input_paths(self, task: TaskSpec) -> tuple[Path, str]:
        shard_root = self.shards_dir.resolve()
        absolute = (shard_root / task.input_file).resolve()
        if not path_within(shard_root, absolute):
            raise WorkbenchError(
                E_TASK_INVALID,
                "input shard escapes the configured shard directory",
                task_id=task.task_id,
                stage="parsed",
                reason="INPUT_PATH_ESCAPE",
                details={"input": task.input_file},
            )
        if path_within(self.workspace, absolute):
            return absolute, relative_posix(self.workspace, absolute)
        return absolute, str(absolute)

    def prepare(self) -> tuple[Config, list[PreparedTask]]:
        config = self.load_config()
        tasks = self.load_tasks()
        cache: dict[Path, Any] = {}
        prepared: list[PreparedTask] = []
        for task in tasks:
            input_path: Path | None = None
            input_relative: str = task.input_file
            input_size: tuple[int, int] | None = None
            input_digest = "unavailable"
            chain_error: dict[str, Any] | None = None
            parsed: ParsedChain | None = None
            try:
                input_path, input_relative = self._input_paths(task)
                loaded = cache.get(input_path)
                if loaded is None:
                    loaded = load_rgb(input_path)
                    cache[input_path] = loaded
                input_size = (loaded.width, loaded.height)
                input_digest = loaded.content_digest
                parsed = parse_chain(task, input_size, config)
            except WorkbenchError as exc:
                if exc.task_id is None:
                    exc.task_id = task.task_id
                if exc.stage is None:
                    exc.stage = "parsed"
                chain_error = exc.to_dict()
                if exc.code == E_INPUT_MISSING:
                    input_digest = "unavailable:" + sha256_json(
                        {"task_id": task.task_id, "input": task.input_file, "error": exc.code}
                    )
            versions = compute_task_versions(
                task,
                parsed,
                input_digest,
                config,
                chain_error=chain_error,
            )
            prepared.append(
                PreparedTask(
                    task=task,
                    input_path=input_relative,
                    input_digest=input_digest,
                    input_size=input_size,
                    parsed=parsed,
                    chain_error=chain_error,
                    versions=versions,
                )
            )
        return config, prepared

    def plan(self) -> tuple[Config, list[PreparedTask], SchedulePlan]:
        config, prepared = self.prepare()
        return config, prepared, build_schedule(prepared, config)

    def budget(self) -> dict[str, Any]:
        _, _, plan = self.plan()
        return plan.to_public_dict()

    def _latest_records(self, task_id: str) -> tuple[CheckpointStore, list[dict[str, Any]], dict[str, dict[str, Any]]]:
        store = CheckpointStore(self.workspace, task_id)
        records = store.read_consistent()
        return store, records, store.latest_by_stage(records)

    def _current_record(
        self,
        store: CheckpointStore,
        latest: dict[str, dict[str, Any]],
        versions: TaskVersionSet,
        stage: str,
    ) -> dict[str, Any] | None:
        expected = versions.fingerprint_for(stage)
        if expected is None:
            return None
        record = latest.get(stage)
        if record is None or record.get("stage_fingerprint") != expected:
            return None
        if stage != "parsed":
            previous = STAGES[STAGE_INDEX[stage] - 1]
            previous_expected = versions.fingerprint_for(previous)
            if previous_expected is None or record.get("upstream_fingerprint") != previous_expected:
                return None
        if not store.artifact_ok(record):
            return None
        return record

    def _detect_invalidations(self, prepared: list[PreparedTask]) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for item in prepared:
            store, _, latest = self._latest_records(item.task.task_id)
            for stage in STAGES:
                expected = item.versions.fingerprint_for(stage)
                if expected is None:
                    break
                record = latest.get(stage)
                if record is None:
                    break
                reason: str | None = None
                changed: list[str] = []
                if record.get("stage_fingerprint") != expected:
                    reason = "fingerprint_mismatch"
                    changed = changed_component_names(
                        item.versions.components_for(stage),
                        record.get("fingerprint_components"),
                    )
                elif stage != "parsed":
                    previous = STAGES[STAGE_INDEX[stage] - 1]
                    previous_expected = item.versions.fingerprint_for(previous)
                    if record.get("upstream_fingerprint") != previous_expected:
                        reason = "upstream_mismatch"
                if reason is None and not store.artifact_ok(record):
                    reason = "artifact_missing_or_corrupt"
                if reason is None:
                    continue
                event_body = {
                    "task_id": item.task.task_id,
                    "earliest_stage": stage,
                    "affected_stages": list(STAGES[STAGE_INDEX[stage] :]),
                    "reason": reason,
                    "changed_components": changed,
                    "expected_fingerprint": expected,
                    "recorded_fingerprint": record.get("stage_fingerprint"),
                    "expected_fingerprints": {
                        candidate: item.versions.fingerprint_for(candidate)
                        for candidate in STAGES
                    },
                    "recorded_fingerprints": {
                        candidate: latest.get(candidate, {}).get("stage_fingerprint")
                        if latest.get(candidate)
                        else None
                        for candidate in STAGES
                    },
                }
                event_body["event_key"] = sha256_json(event_body)
                events.append(event_body)
                break
        return events

    def _load_history(self) -> list[dict[str, Any]]:
        if not self.history_file.is_file():
            return []
        try:
            value = read_json_file(self.history_file)
        except Exception:
            return []
        events = value.get("events", []) if isinstance(value, dict) else []
        return events if isinstance(events, list) else []

    def _record_history(self, new_events: list[dict[str, Any]]) -> None:
        if not new_events:
            return
        lock_path = self.workspace / ".locks" / "invalidation.lock"
        with FileLock(lock_path):
            history = self._load_history()
            existing = {event.get("event_key") for event in history}
            next_seq = max((int(event.get("event_seq", 0)) for event in history), default=0) + 1
            appended = False
            for event in new_events:
                if event.get("event_key") in existing:
                    continue
                record = dict(event)
                record["event_seq"] = next_seq
                next_seq += 1
                history.append(record)
                existing.add(record["event_key"])
                appended = True
            if appended:
                atomic_write_json(self.history_file, {"events": history})

    def versions(self) -> dict[str, Any]:
        config, prepared, plan = self.plan()
        task_states: dict[str, Any] = {}
        for item in prepared:
            store, _, latest = self._latest_records(item.task.task_id)
            decision = plan.decision_for(item.task.task_id)
            stage_states: dict[str, Any] = {}
            for stage in STAGES:
                expected = item.versions.fingerprint_for(stage)
                record = latest.get(stage)
                if expected is None:
                    state = "not_applicable"
                elif decision.decision == "rejected" and stage != "parsed":
                    state = "not_scheduled"
                elif record is None:
                    state = "missing"
                elif self._current_record(store, latest, item.versions, stage) is not None:
                    state = "current"
                else:
                    state = "invalidated"
                stage_states[stage] = {
                    "state": state,
                    "expected_fingerprint": expected,
                    "recorded_fingerprint": record.get("stage_fingerprint") if record else None,
                    "changed_components": changed_component_names(
                        item.versions.components_for(stage),
                        record.get("fingerprint_components") if record else None,
                    )
                    if record
                    else [],
                }
            earliest = None
            for stage in STAGES:
                if stage_states[stage]["state"] == "invalidated":
                    earliest = stage
                    break
            task_states[item.task.task_id] = {
                "decision": decision.to_public_dict(),
                "chain_valid": item.parsed is not None,
                "chain_error": item.chain_error,
                "stages": stage_states,
                "earliest_invalid_stage": earliest,
            }
        return {
            "config_version": config.config_version,
            "tasks": task_states,
            "invalidation_history": self._load_history(),
        }

    def request_stop(self) -> dict[str, Any]:
        atomic_write_bytes(self.stop_file, b"stop\n")
        return {"stop_requested": True, "stop_file": str(self.stop_file)}

    def clear_stop(self) -> None:
        remove_file_if_exists(self.stop_file)

    def reset(self) -> dict[str, Any]:
        reset_runtime_dirs(self.workspace)
        return {"reset": True, "workspace": str(self.workspace)}

    def _versions_dict(self, versions: TaskVersionSet) -> dict[str, Any]:
        return {
            "parse_components": versions.parse_components,
            "parse_fingerprint": versions.parse_fingerprint,
            "executed_components": versions.executed_components,
            "executed_fingerprint": versions.executed_fingerprint,
            "size_checked_components": versions.size_checked_components,
            "size_checked_fingerprint": versions.size_checked_fingerprint,
            "exported_components": versions.exported_components,
            "exported_fingerprint": versions.exported_fingerprint,
        }

    def _job_for(
        self,
        item: PreparedTask,
        config: Config,
        until_stage: str,
    ) -> dict[str, Any]:
        assert item.parsed is not None
        return {
            "workspace": str(self.workspace),
            "config": config.raw,
            "task": item.task.raw,
            "parsed": item.parsed.to_public_dict(),
            "versions": self._versions_dict(item.versions),
            "input_path": item.input_path,
            "input_digest": item.input_digest,
            "until_stage": until_stage,
        }

    def _clear_worker_results(self) -> None:
        target = self.metadata_dir / "worker_results"
        if target.exists():
            shutil.rmtree(target)
        ensure_dir(target)

    def _execute_accepted(
        self,
        config: Config,
        prepared_by_id: dict[str, PreparedTask],
        plan: SchedulePlan,
        until_stage: str,
        *,
        sequential: bool,
        task_delay_ms: int,
    ) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
        accepted = [task_id for task_id in plan.accepted_task_ids]
        if not accepted:
            return {}, []
        self._clear_worker_results()
        jobs_by_task = {
            task_id: self._job_for(prepared_by_id[task_id], config, until_stage)
            for task_id in accepted
        }
        if sequential:
            results: dict[str, dict[str, Any]] = {}
            for task_id in accepted:
                if self.stop_file.exists():
                    break
                processed = process_single_job(
                    jobs_by_task[task_id],
                    self.workspace,
                    stop_path=self.stop_file,
                    task_delay_ms=task_delay_ms,
                )
                if not processed:
                    break
                result_path = worker_result_path(self.workspace, task_id)
                if result_path.is_file():
                    try:
                        result = read_json_file(result_path)
                        if isinstance(result, dict):
                            results[task_id] = result
                    except Exception:
                        pass
            return results, []
        process_groups: list[tuple[int | None, list[dict[str, Any]]]] = []
        slot_tasks: dict[int, list[str]] = {}
        for decision in plan.decisions:
            if decision.decision == "accepted" and decision.slot is not None:
                slot_tasks.setdefault(decision.slot, []).append(decision.task_id)
        for slot in sorted(slot_tasks):
            process_groups.append(
                (slot, [jobs_by_task[task_id] for task_id in sorted(slot_tasks[slot])])
            )
        context = mp.get_context("spawn")
        processes: list[tuple[mp.Process, int | None, list[str]]] = []
        launch_errors: list[dict[str, Any]] = []
        for slot, jobs in process_groups:
            task_ids = [job["task"]["task_id"] for job in jobs]
            process = context.Process(
                target=slot_worker_entry,
                args=(slot if slot is not None else 0, jobs, str(self.stop_file), task_delay_ms),
                name=f"image-workbench-slot-{slot if slot is not None else 'seq'}",
            )
            try:
                process.start()
            except Exception as exc:
                launch_errors.append(
                    {
                        "slot": slot,
                        "task_ids": task_ids,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
                continue
            processes.append((process, slot, task_ids))
        for process, _, _ in processes:
            process.join()
        results: dict[str, dict[str, Any]] = {}
        for path in sorted((self.metadata_dir / "worker_results").glob("*.json")):
            try:
                value = read_json_file(path)
            except Exception:
                continue
            if isinstance(value, dict) and value.get("task_id") in jobs_by_task:
                results[value["task_id"]] = value
        for process, slot, task_ids in processes:
            for task_id in task_ids:
                if task_id in results:
                    continue
                if self.stop_file.exists():
                    continue
                error = WorkbenchError(
                    E_WORKER_PROCESS_EXITED,
                    "worker process ended before publishing a task result",
                    task_id=task_id,
                    stage="worker",
                    reason="WORKER_PROCESS_EXITED",
                    details={"slot": slot, "exitcode": process.exitcode},
                )
                result = {
                    "task_id": task_id,
                    "ok": False,
                    "status": "failed",
                    "stages": {},
                    "error": error.to_dict(),
                }
                results[task_id] = result
                atomic_write_json(worker_result_path(self.workspace, task_id), result)
        launch_error_by_task = {
            task_id: launch
            for launch in launch_errors
            for task_id in launch.get("task_ids", [])
        }
        for task_id in accepted:
            if task_id in results or self.stop_file.exists():
                continue
            launch = launch_error_by_task.get(task_id)
            error = WorkbenchError(
                E_WORKER_PROCESS_EXITED,
                "worker process could not be launched or ended without a result",
                task_id=task_id,
                stage="worker",
                reason="WORKER_PROCESS_UNAVAILABLE",
                details={"launch_error": launch.get("error") if launch else None},
            )
            result = {
                "task_id": task_id,
                "ok": False,
                "status": "failed",
                "stages": {},
                "error": error.to_dict(),
            }
            results[task_id] = result
            atomic_write_json(worker_result_path(self.workspace, task_id), result)
        return results, launch_errors

    @staticmethod
    def _public_error(error: dict[str, Any] | None) -> dict[str, Any] | None:
        if not error:
            return None
        details = dict(error.get("details") or {})
        details.pop("traceback", None)
        return {
            "code": error.get("code"),
            "message": error.get("message"),
            "task_id": error.get("task_id"),
            "stage": error.get("stage"),
            "position": error.get("position"),
            "reason": error.get("reason"),
            "details": details,
        }

    def _build_verification(
        self,
        config: Config,
        prepared: list[PreparedTask],
        plan: SchedulePlan,
        worker_results: dict[str, dict[str, Any]],
    ) -> tuple[dict[str, Any], bytes]:
        task_reports: list[dict[str, Any]] = []
        for item in sorted(prepared, key=lambda value: value.task.task_id):
            task = item.task
            decision = plan.decision_for(task.task_id)
            store, _, latest = self._latest_records(task.task_id)
            current: dict[str, dict[str, Any]] = {}
            for stage in STAGES:
                record = self._current_record(store, latest, item.versions, stage)
                if record is not None:
                    current[stage] = record
            actual_size = None
            actual_digest = None
            artifact_path = None
            artifact_sha = None
            for stage in ("exported", "executed"):
                record = current.get(stage)
                if record is not None:
                    actual_size = record.get("output_size")
                    actual_digest = record.get("output_digest")
                    artifact_path = record.get("artifact_path")
                    artifact_sha = record.get("artifact_sha256")
                    break
            size_match: bool | None = None
            if task.expected_size is not None and actual_size is not None:
                size_match = list(task.expected_size) == list(actual_size)
            digest_match: bool | None = None
            if task.expected_digest is not None and actual_digest is not None:
                digest_match = task.expected_digest == actual_digest
            error: dict[str, Any] | None = None
            status: str
            if decision.decision == "rejected":
                error = decision.error
                if item.parsed is None:
                    status = "rejected_chain"
                else:
                    status = "rejected_budget"
            elif "exported" in current:
                exported = current["exported"]
                if exported.get("ok") and current.get("size_checked", {}).get("ok"):
                    status = "verified"
                else:
                    status = "failed"
                    error = exported.get("error") or current.get("size_checked", {}).get(
                        "error"
                    )
            elif "size_checked" in current and not current["size_checked"].get("ok"):
                status = "failed"
                error = current["size_checked"].get("error")
            else:
                status = "incomplete"
                result = worker_results.get(task.task_id)
                error = result.get("error") if result else None
                if error is None:
                    error = WorkbenchError(
                        E_INCOMPLETE,
                        "task did not reach the exported stage",
                        task_id=task.task_id,
                        stage="exported",
                        reason="TASK_INCOMPLETE",
                    ).to_dict()
            stage_reports = [
                {
                    "stage": stage,
                    "ok": current[stage].get("ok"),
                    "fingerprint": current[stage].get("stage_fingerprint"),
                    "upstream_fingerprint": current[stage].get("upstream_fingerprint"),
                    "artifact_path": current[stage].get("artifact_path"),
                    "artifact_sha256": current[stage].get("artifact_sha256"),
                    "output_size": current[stage].get("output_size"),
                    "output_digest": current[stage].get("output_digest"),
                }
                for stage in STAGES
                if stage in current
            ]
            task_reports.append(
                {
                    "task_id": task.task_id,
                    "input": {
                        "file": task.input_file,
                        "content_sha256": item.input_digest,
                    },
                    "instruction": task.instruction,
                    "chain": item.parsed.public_operations() if item.parsed else None,
                    "decision": {
                        "accepted": decision.decision == "accepted",
                        "slot": decision.slot,
                        "estimated_steps": decision.estimated_steps,
                        "reason_codes": list(decision.reason_codes),
                        "reason_messages": list(decision.reason_messages),
                    },
                    "expected": {
                        "size": list(task.expected_size) if task.expected_size else None,
                        "sha256": task.expected_digest,
                    },
                    "actual": {
                        "size": actual_size,
                        "sha256": actual_digest,
                    },
                    "size_match": size_match,
                    "digest_match": digest_match,
                    "status": status,
                    "error": self._public_error(error),
                    "stages": stage_reports,
                    "artifact": {
                        "path": artifact_path,
                        "sha256": artifact_sha,
                    }
                    if artifact_path
                    else None,
                }
            )
        verification = {
            "schema_version": 1,
            "config_version": config.config_version,
            "tasks": task_reports,
        }
        data = pretty_json_bytes(verification)
        return verification, data

    def _write_final_reports(
        self,
        config: Config,
        prepared: list[PreparedTask],
        plan: SchedulePlan,
        worker_results: dict[str, dict[str, Any]],
        *,
        until_stage: str,
    ) -> dict[str, Any]:
        ensure_dir(self.reports_dir)
        schedule_path = self.reports_dir / "schedule.json"
        schedule_bytes = pretty_json_bytes(plan.to_public_dict())
        atomic_write_bytes(schedule_path, schedule_bytes)
        versions = self.versions()
        versions_path = self.reports_dir / "versions.json"
        versions_bytes = pretty_json_bytes(versions)
        atomic_write_bytes(versions_path, versions_bytes)
        report_paths = {
            "schedule": relative_posix(self.workspace, schedule_path),
            "versions": relative_posix(self.workspace, versions_path),
        }
        if until_stage != "exported":
            return {
                "reports": report_paths,
                "schedule_sha256": sha256_bytes(schedule_bytes),
                "versions_sha256": sha256_bytes(versions_bytes),
            }
        verification, verification_bytes = self._build_verification(
            config, prepared, plan, worker_results
        )
        verification_path = self.reports_dir / "verification.json"
        atomic_write_bytes(verification_path, verification_bytes)
        task_reports = verification["tasks"]
        counts = {
            "total": len(task_reports),
            "accepted": sum(1 for task in task_reports if task["decision"]["accepted"]),
            "verified": sum(1 for task in task_reports if task["status"] == "verified"),
            "failed": sum(1 for task in task_reports if task["status"] == "failed"),
            "incomplete": sum(1 for task in task_reports if task["status"] == "incomplete"),
            "rejected_chain": sum(
                1 for task in task_reports if task["status"] == "rejected_chain"
            ),
            "rejected_budget": sum(
                1 for task in task_reports if task["status"] == "rejected_budget"
            ),
        }
        output_digests = {
            task["task_id"]: task["actual"]["sha256"]
            for task in task_reports
            if task["actual"]["sha256"]
        }
        operator_steps = sum(
            len(task["chain"] or []) for task in task_reports if task["decision"]["accepted"]
        )
        complete = counts["incomplete"] == 0
        summary_core = {
            "schema_version": 1,
            "config_version": config.config_version,
            "task_counts": counts,
            "budget": {
                "global_step_budget": config.global_step_budget,
                "worker_step_quota": config.worker_step_quota,
                "max_workers": config.max_workers,
                "accepted_estimated_steps": plan.accepted_steps,
                "global_remaining_steps": config.global_step_budget
                - plan.accepted_steps,
                "accepted_operator_steps": operator_steps,
            },
            "output_digests": output_digests,
            "complete": complete,
        }
        summary = {
            **summary_core,
            "verification_sha256": sha256_bytes(verification_bytes),
            "summary_sha256": sha256_json(summary_core),
        }
        summary_bytes = pretty_json_bytes(summary)
        summary_path = self.reports_dir / "run_summary.json"
        atomic_write_bytes(summary_path, summary_bytes)
        report_paths.update(
            {
                "verification": relative_posix(self.workspace, verification_path),
                "run_summary": relative_posix(self.workspace, summary_path),
            }
        )
        return {
            "reports": report_paths,
            "verification_sha256": sha256_bytes(verification_bytes),
            "summary_sha256": summary["summary_sha256"],
            "run_summary_sha256": sha256_bytes(summary_bytes),
            "task_counts": counts,
            "complete": complete,
        }

    def _remove_final_reports(self) -> None:
        for name in ("schedule.json", "versions.json", "verification.json", "run_summary.json"):
            remove_file_if_exists(self.reports_dir / name)

    def run(
        self,
        *,
        until_stage: str = "exported",
        sequential: bool = False,
        task_delay_ms: int = 0,
        clear_stop: bool = False,
    ) -> dict[str, Any]:
        until_stage = validate_stage_name(until_stage)
        if clear_stop:
            self.clear_stop()
        ensure_dir(self.workspace / ".locks")
        with FileLock(self.workspace / ".locks" / "run.lock"):
            self._remove_final_reports()
            if self.stop_file.exists():
                return {
                    "status": "stopped",
                    "error": WorkbenchError(
                        E_STOPPED,
                        "stop was requested before the run started",
                        reason="STOP_REQUESTED",
                    ).to_dict(),
                    "reports": {},
                }
            config, prepared = self.prepare()
            prepared_by_id = {item.task.task_id: item for item in prepared}
            events = self._detect_invalidations(prepared)
            self._record_history(events)
            for item in prepared:
                if self.stop_file.exists():
                    return {
                        "status": "stopped",
                        "stage": "parsed",
                        "error": WorkbenchError(
                            E_STOPPED,
                            "stop was requested while preparing checkpoints",
                            reason="STOP_REQUESTED",
                        ).to_dict(),
                        "reports": {},
                    }
                ensure_parsed_checkpoint(
                    self.workspace,
                    config,
                    item.task,
                    item.input_digest,
                    item.parsed,
                    item.versions,
                    item.chain_error,
                )
            plan = build_schedule(prepared, config)
            if self.stop_file.exists():
                return {
                    "status": "stopped",
                    "stage": "parsed",
                    "error": WorkbenchError(
                        E_STOPPED,
                        "stop was requested before task execution",
                        reason="STOP_REQUESTED",
                    ).to_dict(),
                    "reports": {},
                }
            worker_results: dict[str, dict[str, Any]] = {}
            launch_errors: list[dict[str, Any]] = []
            if until_stage != "parsed":
                worker_results, launch_errors = self._execute_accepted(
                    config,
                    prepared_by_id,
                    plan,
                    until_stage,
                    sequential=sequential,
                    task_delay_ms=task_delay_ms,
                )
            if self.stop_file.exists():
                return {
                    "status": "stopped",
                    "stage": until_stage,
                    "error": WorkbenchError(
                        E_STOPPED,
                        "stop was requested; completed checkpoints are retained",
                        reason="STOP_REQUESTED",
                    ).to_dict(),
                    "reports": {},
                    "invalidation_events": events,
                }
            report_info = self._write_final_reports(
                config,
                prepared,
                plan,
                worker_results,
                until_stage=until_stage,
            )
            status = "partial" if until_stage != "exported" else "completed"
            return {
                "status": status,
                "stage": until_stage,
                "config_version": config.config_version,
                "schedule": plan.to_public_dict(),
                "accepted_task_ids": plan.accepted_task_ids,
                "rejected_task_ids": plan.rejected_task_ids,
                "invalidation_events": events,
                "worker_launch_errors": launch_errors,
                **report_info,
            }

    def status(self) -> dict[str, Any]:
        config, prepared, plan = self.plan()
        tasks_status: dict[str, Any] = {}
        for item in prepared:
            store, _, latest = self._latest_records(item.task.task_id)
            result_path = worker_result_path(self.workspace, item.task.task_id)
            worker_result: dict[str, Any] | None = None
            if result_path.is_file():
                try:
                    value = read_json_file(result_path)
                    if isinstance(value, dict):
                        worker_result = {
                            "ok": value.get("ok"),
                            "status": value.get("status"),
                            "error": self._public_error(value.get("error")),
                        }
                except Exception:
                    worker_result = None
            tasks_status[item.task.task_id] = {
                "input": item.task.input_file,
                "chain_valid": item.parsed is not None,
                "chain_error": item.chain_error,
                "decision": plan.decision_for(item.task.task_id).to_public_dict(),
                "checkpoints": store.snapshot(),
                "latest_by_stage": {
                    stage: {
                        "seq": record.get("seq"),
                        "ok": record.get("ok"),
                        "fingerprint": record.get("stage_fingerprint"),
                        "artifact_path": record.get("artifact_path"),
                    }
                    for stage, record in latest.items()
                },
                "worker_result": worker_result,
            }
        reports = {
            name: (self.reports_dir / name).is_file()
            for name in ("schedule.json", "versions.json", "verification.json", "run_summary.json")
        }
        return {
            "config": config.to_public_dict(),
            "stop_requested": self.stop_file.exists(),
            "budget": plan.to_public_dict(),
            "tasks": tasks_status,
            "reports": reports,
            "invalidation_history": self._load_history(),
        }

    def reports(self) -> dict[str, Any]:
        result: dict[str, Any] = {"reports": {}}
        for name in ("schedule.json", "versions.json", "verification.json", "run_summary.json"):
            path = self.reports_dir / name
            if path.is_file():
                try:
                    result["reports"][name] = read_json_file(path)
                except Exception as exc:
                    result["reports"][name] = {"error": f"{type(exc).__name__}: {exc}"}
        return result


def import_assets(
    workspace: str | Path,
    shards_dir: str | Path,
    tasks_file: str | Path,
    config_file: str | Path,
) -> dict[str, Any]:
    workspace = Path(workspace).resolve()
    shards_dir = Path(shards_dir).resolve()
    tasks_file = Path(tasks_file).resolve()
    config_file = Path(config_file).resolve()
    if not shards_dir.is_dir():
        raise WorkbenchError(
            E_INPUT_MISSING,
            f"shards directory not found: {shards_dir}",
            reason="SHARDS_DIR_MISSING",
        )
    if not tasks_file.is_file():
        raise WorkbenchError(
            E_TASK_INVALID,
            f"task manifest not found: {tasks_file}",
            reason="TASK_FILE_MISSING",
        )
    if not config_file.is_file():
        raise WorkbenchError(
            "E_CONFIG_INVALID",
            f"config file not found: {config_file}",
            reason="CONFIG_FILE_MISSING",
        )
    Config.load(config_file)
    load_tasks(tasks_file)
    reset_runtime_dirs(workspace)
    ensure_dir(workspace / "inputs")
    copied_shards: list[str] = []
    for source in sorted(shards_dir.rglob("*.png")):
        if not source.is_file():
            continue
        relative = source.relative_to(shards_dir)
        target = workspace / "inputs" / relative
        atomic_write_bytes(target, source.read_bytes())
        copied_shards.append(relative.as_posix())
    target_tasks = workspace / "tasks.jsonl"
    atomic_write_bytes(target_tasks, tasks_file.read_bytes())
    target_config = workspace / "config.yaml"
    atomic_write_bytes(target_config, config_file.read_bytes())
    Config.load(target_config)
    load_tasks(target_tasks)
    return {
        "imported": True,
        "workspace": str(workspace),
        "shards": copied_shards,
        "tasks_file": relative_posix(workspace, target_tasks),
        "config_file": relative_posix(workspace, target_config),
    }
