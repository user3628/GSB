"""Stage execution and persistent worker-slot processes."""

from __future__ import annotations

import os
import time
import traceback
from pathlib import Path
from typing import Any

from .chain import ParsedChain
from .checkpoint import CheckpointStore
from .config import Config
from .errors import (
    E_ARTIFACT_INVALID,
    E_DIGEST_MISMATCH,
    E_EXPORT_VERIFY,
    E_INVALID_STAGE,
    E_SIZE_MISMATCH,
    E_TASK_INVALID,
    E_TASK_TIMEOUT,
    WorkbenchError,
    as_error_dict,
)
from .imaging import encode_png_rgb, load_rgb, pixel_digest
from .models import TaskSpec
from .operators import apply_chain
from .util import atomic_write_bytes, atomic_write_json, pretty_json_bytes, sha256_bytes
from .versioning import STAGES, STAGE_INDEX, TaskVersionSet


def validate_stage_name(stage: str) -> str:
    if stage not in STAGES:
        raise WorkbenchError(
            E_INVALID_STAGE,
            f"unknown stage {stage!r}",
            stage=stage,
            reason="STAGE_UNKNOWN",
            details={"known_stages": list(STAGES)},
        )
    return stage


def stage_artifact_path(workspace: Path, task_id: str, stage: str) -> Path:
    suffix = ".png" if stage in {"executed", "exported"} else ".json"
    return Path(workspace) / "artifacts" / stage / f"{task_id}{suffix}"


def _matching_record(
    store: CheckpointStore,
    stage: str,
    expected_fingerprint: str,
    upstream_fingerprint: str | None = None,
) -> dict[str, Any] | None:
    latest = store.latest_by_stage(store.read_consistent()).get(stage)
    if latest is None:
        return None
    if latest.get("stage_fingerprint") != expected_fingerprint:
        return None
    if upstream_fingerprint is not None and latest.get("upstream_fingerprint") != upstream_fingerprint:
        return None
    if not store.artifact_ok(latest):
        return None
    return latest


def _publish_with_race_retry(
    store: CheckpointStore,
    stage: str,
    payload: dict[str, Any],
    expected_fingerprint: str,
    upstream_fingerprint: str | None = None,
) -> dict[str, Any]:
    for _ in range(3):
        existing = _matching_record(store, stage, expected_fingerprint, upstream_fingerprint)
        if existing is not None:
            return existing
        expected_seq = store.next_seq()
        try:
            return store.publish(stage, payload, expected_seq=expected_seq)
        except WorkbenchError as exc:
            if exc.code != "E_STALE_CHECKPOINT":
                raise
    existing = _matching_record(store, stage, expected_fingerprint, upstream_fingerprint)
    if existing is not None:
        return existing
    raise WorkbenchError(
        "E_STALE_CHECKPOINT",
        "concurrent checkpoint publishers kept winning the sequence",
        task_id=store.task_id,
        stage=stage,
        reason="PUBLISH_RETRY_EXHAUSTED",
    )


def ensure_parsed_checkpoint(
    workspace: Path,
    config: Config,
    task: TaskSpec,
    input_digest: str,
    parsed: ParsedChain | None,
    versions: TaskVersionSet,
    chain_error: dict[str, Any] | None,
) -> tuple[dict[str, Any], bool]:
    store = CheckpointStore(workspace, task.task_id)
    existing = _matching_record(store, "parsed", versions.parse_fingerprint)
    if existing is not None:
        return existing, True
    artifact = stage_artifact_path(workspace, task.task_id, "parsed")
    artifact_body = {
        "task_id": task.task_id,
        "ok": parsed is not None,
        "input_digest": input_digest,
        "input_size": list(parsed.input_size) if parsed else None,
        "output_size": list(parsed.output_size) if parsed else None,
        "operations": parsed.public_operations() if parsed else None,
        "error": chain_error,
    }
    data = pretty_json_bytes(artifact_body)
    atomic_write_bytes(artifact, data)
    payload = {
        "input_digest": input_digest,
        "config_version": config.config_version,
        "stage_fingerprint": versions.parse_fingerprint,
        "upstream_fingerprint": None,
        "fingerprint_components": versions.parse_components,
        "artifact_path": artifact.relative_to(workspace).as_posix(),
        "artifact_sha256": sha256_bytes(data),
        "output_digest": None,
        "output_size": list(parsed.output_size) if parsed else None,
        "ok": parsed is not None,
        "error": chain_error,
    }
    record = _publish_with_race_retry(
        store, "parsed", payload, versions.parse_fingerprint
    )
    return record, False


def _deadline_check(deadline: float | None, task_id: str) -> None:
    if deadline is not None and time.monotonic() > deadline:
        raise WorkbenchError(
            E_TASK_TIMEOUT,
            "task exceeded its execution time budget",
            task_id=task_id,
            stage="executed",
            reason="TASK_TIME_BUDGET_EXCEEDED",
        )


def _maybe_test_crash(task_id: str, stage: str) -> None:
    """Test-only fault injection, active only when an env var is explicitly set."""
    crash_stage = os.environ.get("IMAGE_WORKBENCH_TEST_CRASH_AFTER_STAGE")
    if not crash_stage or crash_stage != stage:
        return
    crash_task = os.environ.get("IMAGE_WORKBENCH_TEST_CRASH_TASK")
    if crash_task and crash_task != task_id:
        return
    os._exit(137)


def _error(code: str, message: str, task_id: str, stage: str, reason: str, **details: Any) -> dict[str, Any]:
    return WorkbenchError(
        code,
        message,
        task_id=task_id,
        stage=stage,
        reason=reason,
        details=details,
    ).to_dict()


def execute_task_stages(
    workspace: Path,
    config: Config,
    task: TaskSpec,
    parsed: ParsedChain,
    versions: TaskVersionSet,
    input_path: Path,
    input_digest: str,
    until_stage: str,
) -> dict[str, Any]:
    workspace = Path(workspace).resolve()
    store = CheckpointStore(workspace, task.task_id)
    target_index = STAGE_INDEX[validate_stage_name(until_stage)]
    parsed_record = _matching_record(store, "parsed", versions.parse_fingerprint)
    if parsed_record is None:
        raise WorkbenchError(
            E_INVALID_STAGE,
            "cannot execute task without a current parsed checkpoint",
            task_id=task.task_id,
            stage="executed",
            reason="PARSED_CHECKPOINT_MISSING",
        )
    if not parsed_record.get("ok"):
        raise WorkbenchError(
            E_INVALID_STAGE,
            "cannot execute an invalid operator chain",
            task_id=task.task_id,
            stage="executed",
            reason="PARSED_CHECKPOINT_INVALID",
        )
    deadline = time.monotonic() + config.task_timeout_seconds
    stage_records: dict[str, dict[str, Any]] = {"parsed": parsed_record}
    stage_reused: dict[str, bool] = {"parsed": True}

    executed = _matching_record(
        store,
        "executed",
        versions.executed_fingerprint or "",
        versions.parse_fingerprint,
    )
    if executed is None:
        _deadline_check(deadline, task.task_id)
        loaded = load_rgb(input_path)
        if loaded.content_digest != input_digest:
            raise WorkbenchError(
                E_ARTIFACT_INVALID,
                "input shard changed after parsing",
                task_id=task.task_id,
                stage="executed",
                reason="INPUT_CHANGED_DURING_RUN",
            )
        output_pixels = apply_chain(
            loaded.pixels,
            parsed.public_operations(),
            deadline=deadline,
            task_id=task.task_id,
        )
        output_size = (int(output_pixels.shape[1]), int(output_pixels.shape[0]))
        digest = pixel_digest(output_pixels)
        png = encode_png_rgb(output_pixels)
        artifact = stage_artifact_path(workspace, task.task_id, "executed")
        atomic_write_bytes(artifact, png)
        payload = {
            "input_digest": input_digest,
            "config_version": config.config_version,
            "stage_fingerprint": versions.executed_fingerprint,
            "upstream_fingerprint": versions.parse_fingerprint,
            "fingerprint_components": versions.executed_components,
            "artifact_path": artifact.relative_to(workspace).as_posix(),
            "artifact_sha256": sha256_bytes(png),
            "output_digest": digest,
            "output_size": list(output_size),
            "ok": True,
            "error": None,
        }
        executed = _publish_with_race_retry(
            store,
            "executed",
            payload,
            versions.executed_fingerprint or "",
            versions.parse_fingerprint,
        )
        stage_reused["executed"] = False
        _maybe_test_crash(task.task_id, "executed")
    else:
        stage_reused["executed"] = True
    stage_records["executed"] = executed
    _deadline_check(deadline, task.task_id)
    if target_index == STAGE_INDEX["executed"]:
        return _worker_outcome(task.task_id, stage_records, stage_reused, None)

    size_record = _matching_record(
        store,
        "size_checked",
        versions.size_checked_fingerprint or "",
        versions.executed_fingerprint or "",
    )
    if size_record is None:
        actual_size = tuple(executed["output_size"])
        expected_size = task.expected_size
        if expected_size is None:
            size_ok = False
            size_error = _error(
                E_TASK_INVALID,
                "task has no expected output size declaration",
                task.task_id,
                "size_checked",
                "EXPECTED_SIZE_MISSING",
            )
        else:
            size_ok = tuple(expected_size) == tuple(actual_size)
            size_error = None
            if not size_ok:
                size_error = _error(
                    E_SIZE_MISMATCH,
                    "actual output size does not match the declared size",
                    task.task_id,
                    "size_checked",
                    "SIZE_MISMATCH",
                    expected=list(expected_size),
                    actual=list(actual_size),
                )
        artifact = stage_artifact_path(workspace, task.task_id, "size_checked")
        size_body = {
            "task_id": task.task_id,
            "expected_size": list(expected_size) if expected_size else None,
            "actual_size": list(actual_size),
            "match": size_ok,
            "actual_sha256": executed.get("output_digest"),
        }
        data = pretty_json_bytes(size_body)
        atomic_write_bytes(artifact, data)
        payload = {
            "input_digest": input_digest,
            "config_version": config.config_version,
            "stage_fingerprint": versions.size_checked_fingerprint,
            "upstream_fingerprint": versions.executed_fingerprint,
            "fingerprint_components": versions.size_checked_components,
            "artifact_path": artifact.relative_to(workspace).as_posix(),
            "artifact_sha256": sha256_bytes(data),
            "output_digest": executed.get("output_digest"),
            "output_size": list(actual_size),
            "ok": size_ok,
            "error": size_error,
        }
        size_record = _publish_with_race_retry(
            store,
            "size_checked",
            payload,
            versions.size_checked_fingerprint or "",
            versions.executed_fingerprint or "",
        )
        stage_reused["size_checked"] = False
        _maybe_test_crash(task.task_id, "size_checked")
    else:
        stage_reused["size_checked"] = True
    stage_records["size_checked"] = size_record
    _deadline_check(deadline, task.task_id)
    if target_index == STAGE_INDEX["size_checked"]:
        return _worker_outcome(task.task_id, stage_records, stage_reused, None)
    if not size_record.get("ok"):
        return _worker_outcome(
            task.task_id, stage_records, stage_reused, size_record.get("error")
        )

    exported = _matching_record(
        store,
        "exported",
        versions.exported_fingerprint or "",
        versions.size_checked_fingerprint or "",
    )
    if exported is None:
        executed_artifact = workspace / executed["artifact_path"]
        exported_artifact = stage_artifact_path(workspace, task.task_id, "exported")
        exported_bytes = executed_artifact.read_bytes()
        atomic_write_bytes(exported_artifact, exported_bytes)
        digest = executed.get("output_digest")
        export_ok = True
        export_error = None
        if task.expected_digest is None:
            export_ok = False
            export_error = _error(
                E_TASK_INVALID,
                "task has no expected output digest declaration",
                task.task_id,
                "exported",
                "EXPECTED_DIGEST_MISSING",
            )
        elif digest != task.expected_digest:
            export_ok = False
            export_error = _error(
                E_DIGEST_MISMATCH,
                "actual output digest does not match the declared digest",
                task.task_id,
                "exported",
                "DIGEST_MISMATCH",
                expected=task.expected_digest,
                actual=digest,
            )
        try:
            exported_loaded = load_rgb(exported_artifact)
            decoded_digest = pixel_digest(exported_loaded.pixels)
        except Exception as exc:
            decoded_digest = None
            export_ok = False
            export_error = _error(
                E_EXPORT_VERIFY,
                f"exported PNG cannot be decoded: {exc}",
                task.task_id,
                "exported",
                "EXPORT_DECODE_FAILED",
            )
        if decoded_digest is not None and decoded_digest != digest:
            export_ok = False
            export_error = _error(
                E_EXPORT_VERIFY,
                "decoded exported pixels differ from the executed artifact",
                task.task_id,
                "exported",
                "EXPORT_PIXEL_DIGEST_MISMATCH",
                executed_digest=digest,
                exported_digest=decoded_digest,
            )
        payload = {
            "input_digest": input_digest,
            "config_version": config.config_version,
            "stage_fingerprint": versions.exported_fingerprint,
            "upstream_fingerprint": versions.size_checked_fingerprint,
            "fingerprint_components": versions.exported_components,
            "artifact_path": exported_artifact.relative_to(workspace).as_posix(),
            "artifact_sha256": sha256_bytes(exported_bytes),
            "output_digest": digest,
            "output_size": executed.get("output_size"),
            "ok": export_ok,
            "error": export_error,
        }
        exported = _publish_with_race_retry(
            store,
            "exported",
            payload,
            versions.exported_fingerprint or "",
            versions.size_checked_fingerprint or "",
        )
        stage_reused["exported"] = False
        _maybe_test_crash(task.task_id, "exported")
    else:
        stage_reused["exported"] = True
    stage_records["exported"] = exported
    error = exported.get("error")
    return _worker_outcome(task.task_id, stage_records, stage_reused, error)


def _worker_outcome(
    task_id: str,
    records: dict[str, dict[str, Any]],
    reused: dict[str, bool],
    error: dict[str, Any] | None,
) -> dict[str, Any]:
    latest = records.get("exported") or records.get("size_checked") or records.get("executed") or records.get("parsed")
    ok = bool(latest and latest.get("ok")) and error is None
    return {
        "task_id": task_id,
        "ok": ok,
        "status": "verified" if ok else "failed",
        "stages": {
            stage: {
                "seq": record.get("seq"),
                "ok": record.get("ok"),
                "reused": reused.get(stage, True),
                "fingerprint": record.get("stage_fingerprint"),
            }
            for stage, record in records.items()
        },
        "error": error,
    }


def worker_result_path(workspace: Path, task_id: str) -> Path:
    return Path(workspace) / "metadata" / "worker_results" / f"{task_id}.json"


def _write_worker_result(workspace: Path, result: dict[str, Any]) -> None:
    atomic_write_json(worker_result_path(workspace, result["task_id"]), result)


def _task_from_job(job: dict[str, Any]) -> TaskSpec:
    return TaskSpec.from_mapping(job["task"])


def _parsed_from_job(job: dict[str, Any]) -> ParsedChain:
    chain = job["parsed"]
    return ParsedChain(
        task_id=chain["task_id"],
        operations=tuple(chain["operations"]),
        input_size=tuple(chain["input_size"]),
        output_size=tuple(chain["output_size"]),
    )


def slot_worker_entry(
    slot_index: int,
    jobs: list[dict[str, Any]],
    stop_file: str,
    task_delay_ms: int,
) -> None:
    workspace = Path(jobs[0]["workspace"]) if jobs else None
    if workspace is None:
        return
    stop_path = Path(stop_file)
    for job in jobs:
        processed = process_single_job(
            job, workspace, stop_path=stop_path, task_delay_ms=task_delay_ms
        )
        if not processed:
            break


def process_single_job(
    job: dict[str, Any],
    workspace: Path,
    *,
    stop_path: Path | None = None,
    task_delay_ms: int = 0,
) -> bool:
    """Process one task and atomically publish its worker result.

    Returns False when a stop request prevented the task from starting.
    """
    workspace = Path(workspace)
    if stop_path is not None and stop_path.exists():
        return False
    if task_delay_ms > 0:
        time.sleep(task_delay_ms / 1000.0)
    if stop_path is not None and stop_path.exists():
        return False
    task_id = job["task"]["task_id"]
    try:
        config = Config.from_mapping(job["config"])
        task = _task_from_job(job)
        parsed = _parsed_from_job(job)
        input_path = Path(job["input_path"])
        if not input_path.is_absolute():
            input_path = workspace / input_path
        versions = _versions_from_job(job)
        result = execute_task_stages(
            workspace,
            config,
            task,
            parsed,
            versions,
            input_path,
            job["input_digest"],
            job["until_stage"],
        )
    except WorkbenchError as exc:
        if exc.task_id is None:
            exc.task_id = task_id
        result = {
            "task_id": task_id,
            "ok": False,
            "status": "failed",
            "stages": {},
            "error": exc.to_dict(),
        }
    except BaseException as exc:
        error = as_error_dict(exc)
        error["task_id"] = task_id
        error["details"] = {
            **(error.get("details") or {}),
            "traceback": traceback.format_exc(limit=8),
        }
        result = {
            "task_id": task_id,
            "ok": False,
            "status": "failed",
            "stages": {},
            "error": error,
        }
    _write_worker_result(workspace, result)
    return True


def _versions_from_job(job: dict[str, Any]) -> TaskVersionSet:
    value = job["versions"]
    return TaskVersionSet(
        parse_components=value["parse_components"],
        parse_fingerprint=value["parse_fingerprint"],
        executed_components=value.get("executed_components"),
        executed_fingerprint=value.get("executed_fingerprint"),
        size_checked_components=value.get("size_checked_components"),
        size_checked_fingerprint=value.get("size_checked_fingerprint"),
        exported_components=value.get("exported_components"),
        exported_fingerprint=value.get("exported_fingerprint"),
    )
