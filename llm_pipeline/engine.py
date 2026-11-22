"""Pipeline orchestration, incremental invalidation, and verification."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .allocation import allocate
from .assembly import assemble
from .checkpoints import CheckpointState, CheckpointStore
from .config import (
    STAGES,
    allocate_components,
    assemble_components,
    canonical_stage,
    config_digest,
    dedup_components,
    stage_index,
    validate_components,
)
from .dedup import deduplicate
from .errors import (
    E_INCREMENTAL_MISMATCH,
    E_STOPPED,
    E_STOP_REQUESTED,
    PipelineError,
    StopRequested,
    invalid_stage,
)
from .util import (
    canonical_json_bytes,
    canonical_json_line,
    deep_merge,
    first_difference,
    set_dotted,
    sha256_bytes,
    sha256_obj,
)
from .validation import validate
from .workspace import (
    Workspace,
    allocation_product_bytes,
    mixed_product_bytes,
    record_keep_product_bytes,
)


@dataclass
class Snapshot:
    config: dict[str, Any]
    shards: list[dict[str, Any]]
    records: list[dict[str, Any]]
    config_version: str
    config_revision: int
    input_shard_fingerprint: str


class StopController:
    """Cooperative stop checks executed after each indivisible work unit."""

    def __init__(
        self,
        workspace: Workspace,
        *,
        stop_after_samples: int | None = None,
        delay_ms: int = 0,
    ) -> None:
        self.workspace = workspace
        self.stop_after_samples = stop_after_samples
        self.delay_seconds = max(0, delay_ms) / 1000.0
        self.units = 0

    def request_check(self) -> None:
        if self.workspace.stop_requested():
            raise StopRequested()

    def before_stage(self) -> None:
        if self.stop_after_samples is not None and self.stop_after_samples <= 0:
            raise StopRequested()
        self.request_check()

    def after_unit(self) -> None:
        self.units += 1
        if self.delay_seconds:
            time.sleep(self.delay_seconds)
        if (
            self.stop_after_samples is not None
            and self.units >= self.stop_after_samples
        ):
            raise StopRequested()
        if self.workspace.stop_requested():
            raise StopRequested()

    def before_publish(self) -> None:
        self.request_check()


def stage_fingerprint(components: dict[str, Any]) -> str:
    return sha256_obj(components)


def shard_payload_fingerprint(shards: list[dict[str, Any]]) -> str:
    return sha256_obj(
        [
            {"name": item["name"], "digest": item["digest"]}
            for item in sorted(shards, key=lambda row: row["name"])
        ]
    )


def _components_for_stage(
    stage: str,
    config: dict[str, Any],
    shards: list[dict[str, Any]],
    *,
    normalized_digest: str | None,
    allocation_digest: str | None,
    assembly_digest: str | None,
    normalized_fingerprint: str | None,
    allocation_fingerprint: str | None,
    assembly_fingerprint: str | None,
) -> dict[str, Any]:
    if stage == "normalize":
        return dedup_components(config, shards)
    if stage == "allocate":
        return allocate_components(
            config,
            normalized_digest,  # type: ignore[arg-type]
            normalized_fingerprint,
        )
    if stage == "assemble":
        return assemble_components(
            config,
            normalized_digest,  # type: ignore[arg-type]
            allocation_digest,  # type: ignore[arg-type]
            normalized_fingerprint,
            allocation_fingerprint,
        )
    if stage == "validate":
        return validate_components(
            config,
            assembly_digest,  # type: ignore[arg-type]
            assembly_fingerprint,
        )
    raise invalid_stage(f"unknown stage: {stage}")


def diff_components(
    before: Any,
    after: Any,
    prefix: str = "",
) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    if isinstance(before, dict) and isinstance(after, dict):
        for key in sorted(set(before) | set(after)):
            child_prefix = f"{prefix}.{key}" if prefix else key
            changes.extend(diff_components(before.get(key), after.get(key), child_prefix))
        return changes
    if isinstance(before, list) and isinstance(after, list):
        if all(isinstance(item, dict) and "name" in item for item in before + after):
            before_map = {item["name"]: item for item in before}
            after_map = {item["name"]: item for item in after}
            for name in sorted(set(before_map) | set(after_map)):
                changes.extend(
                    diff_components(
                        before_map.get(name),
                        after_map.get(name),
                        f"{prefix}[{name}]" if prefix else f"[{name}]",
                    )
                )
            return changes
    if canonical_json_bytes(before) != canonical_json_bytes(after):
        changes.append(
            {
                "component": prefix or "<root>",
                "before": before,
                "after": after,
            }
        )
    return changes


class PipelineEngine:
    def __init__(self, workspace: Workspace | str | Path) -> None:
        self.workspace = workspace if isinstance(workspace, Workspace) else Workspace(workspace)
        self.store = CheckpointStore(self.workspace)

    def snapshot(self) -> Snapshot:
        self.workspace.require_initialized()
        config = self.workspace.load_config()
        shards, records = self.workspace.load_shards()
        return Snapshot(
            config=config,
            shards=shards,
            records=records,
            config_version=config_digest(config),
            config_revision=int(config.get("revision", 0)),
            input_shard_fingerprint=shard_payload_fingerprint(shards),
        )

    def _inspect_snapshot(
        self, snapshot: Snapshot, state: CheckpointState
    ) -> dict[str, Any]:
        expected: dict[str, dict[str, Any]] = {}
        normalized_record = state.item("normalize")
        allocation_record = state.item("allocate")
        assembly_record = state.item("assemble")
        normalized_digest = (
            normalized_record["artifact_digest"] if normalized_record else None
        )
        allocation_digest = (
            allocation_record["artifact_digest"] if allocation_record else None
        )
        assembly_digest = (
            assembly_record["artifact_digest"] if assembly_record else None
        )
        normalized_fingerprint = (
            normalized_record["stage_fingerprint"] if normalized_record else None
        )
        allocation_fingerprint = (
            allocation_record["stage_fingerprint"] if allocation_record else None
        )
        assembly_fingerprint = (
            assembly_record["stage_fingerprint"] if assembly_record else None
        )

        stages: dict[str, dict[str, Any]] = {}
        earliest: str | None = None
        reasons: list[dict[str, Any]] = []
        upstream_ready = True
        for stage in STAGES:
            components = _components_for_stage(
                stage,
                snapshot.config,
                snapshot.shards,
                normalized_digest=normalized_digest,
                allocation_digest=allocation_digest,
                assembly_digest=assembly_digest,
                normalized_fingerprint=normalized_fingerprint,
                allocation_fingerprint=allocation_fingerprint,
                assembly_fingerprint=assembly_fingerprint,
            )
            expected_fp = stage_fingerprint(components)
            expected[stage] = {
                "fingerprint": expected_fp,
                "components": components,
            }
            record = state.item(stage)
            fingerprint_matches = bool(
                record and record["stage_fingerprint"] == expected_fp
            )
            ready = upstream_ready and fingerprint_matches
            if not ready and earliest is None:
                earliest = stage
                if record is None:
                    reasons.append(
                        {
                            "stage": stage,
                            "component": "checkpoint",
                            "reason": "missing_checkpoint",
                            "before": None,
                            "after": expected_fp,
                        }
                    )
                elif not fingerprint_matches:
                    stage_changes = diff_components(
                        record["fingerprint_components"], components
                    )
                    if not stage_changes:
                        stage_changes = [
                            {
                                "component": "<stage_fingerprint>",
                                "before": record["stage_fingerprint"],
                                "after": expected_fp,
                            }
                        ]
                    for change in stage_changes:
                        reasons.append({"stage": stage, **change})
            if record is not None and upstream_ready and not fingerprint_matches:
                status = "stale"
            elif record is not None and ready:
                status = "ready"
            elif record is not None:
                status = "blocked_by_upstream"
            else:
                status = "missing"
            stages[stage] = {
                "status": status,
                "reusable": ready,
                "seq": record["seq"] if record else None,
                "stage_seq": record["stage_seq"] if record else None,
                "artifact_digest": record["artifact_digest"] if record else None,
                "artifact_path": record["artifact_path"] if record else None,
                "fingerprint": record["stage_fingerprint"] if record else None,
                "expected_fingerprint": expected_fp,
                "fingerprint_matches": fingerprint_matches,
                "checkpoint_config_version": record["config_version"] if record else None,
                "checkpoint_config_revision": record["config_revision"] if record else None,
                "checkpoint_file": record["_file"] if record else None,
            }
            upstream_ready = ready

        current_pipeline_fingerprint = sha256_obj(
            {
                stage: (state.item(stage) or {}).get("stage_fingerprint")
                for stage in STAGES
            }
        )
        expected_pipeline_fingerprint = sha256_obj(
            {stage: expected[stage]["fingerprint"] for stage in STAGES}
        )
        invalidation = None
        if earliest is not None:
            invalidation = {
                "earliest_stage": earliest,
                "reasons": reasons,
                "current_fingerprint": (state.item(earliest) or {}).get("stage_fingerprint"),
                "expected_fingerprint": expected[earliest]["fingerprint"],
                "already_invalidated_stages": [
                    stage
                    for stage in STAGES[stage_index(earliest) :]
                ],
            }

        empty_errors: list[dict[str, Any]] = []
        if normalized_record is not None:
            try:
                normalized_payload = self.store.artifact_payload(normalized_record)
                empty_errors = normalized_payload.get("empty_errors", [])
            except PipelineError:
                empty_errors = []

        return {
            "ok": True,
            "config_version": snapshot.config_version,
            "config_revision": snapshot.config_revision,
            "input_shards": snapshot.shards,
            "input_shard_fingerprint": snapshot.input_shard_fingerprint,
            "stages": stages,
            "stage_order": list(STAGES),
            "invalidation": invalidation,
            "pipeline_fingerprint_current": current_pipeline_fingerprint,
            "pipeline_fingerprint_expected": expected_pipeline_fingerprint,
            "checkpoint_sequence": {
                "current_seq": state.current_seq,
                "last_stage": state.records[-1]["stage"] if state.records else None,
                "seqs": [record["seq"] for record in state.records],
                "stage_sequence": {
                    stage: (state.item(stage) or {}).get("stage_seq", 0)
                    for stage in STAGES
                },
                "max_seq_seen": state.max_seq_seen,
                "issues": state.issues,
                "continuous": True,
            },
            "empty_errors": empty_errors,
            "stop": self.workspace.stop_status(),
            "last_run": self.workspace.last_run(),
        }

    def inspect(self) -> dict[str, Any]:
        snapshot = self.snapshot()
        state = self.store.load()
        return self._inspect_snapshot(snapshot, state)

    def fingerprints(self) -> dict[str, Any]:
        status = self.inspect()
        return {
            "ok": True,
            "config_version": status["config_version"],
            "config_revision": status["config_revision"],
            "input_shard_fingerprint": status["input_shard_fingerprint"],
            "pipeline_fingerprint_current": status["pipeline_fingerprint_current"],
            "pipeline_fingerprint_expected": status["pipeline_fingerprint_expected"],
            "stages": {
                stage: {
                    "current": details["fingerprint"],
                    "expected": details["expected_fingerprint"],
                    "status": details["status"],
                }
                for stage, details in status["stages"].items()
            },
            "invalidation": status["invalidation"],
        }

    def _execute_stage(
        self,
        stage: str,
        snapshot: Snapshot,
        payloads: dict[str, dict[str, Any]],
        controller: StopController,
    ) -> dict[str, Any]:
        if stage == "normalize":
            return deduplicate(
                snapshot.records,
                snapshot.config,
                check_stop=controller.after_unit,
            )
        if stage == "allocate":
            return allocate(
                payloads["normalize"],
                snapshot.config,
                check_stop=controller.after_unit,
            )
        if stage == "assemble":
            return assemble(
                payloads["normalize"],
                payloads["allocate"],
                snapshot.config,
                check_stop=controller.after_unit,
            )
        if stage == "validate":
            normalized_digest = sha256_obj(payloads["normalize"])
            allocation_digest = sha256_obj(payloads["allocate"])
            assembly_digest = sha256_obj(payloads["assemble"])
            return validate(
                payloads["normalize"],
                payloads["allocate"],
                payloads["assemble"],
                snapshot.config,
                normalized_artifact_digest=normalized_digest,
                allocation_artifact_digest=allocation_digest,
                assembly_artifact_digest=assembly_digest,
                check_stop=controller.after_unit,
            )
        raise invalid_stage(f"unknown stage: {stage}", {"stage": stage})

    def _publish_stage(
        self,
        stage: str,
        snapshot: Snapshot,
        state: CheckpointState,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        normalized_digest = None
        allocation_digest = None
        assembly_digest = None
        normalized_fingerprint = None
        allocation_fingerprint = None
        assembly_fingerprint = None
        if stage in {"allocate", "assemble", "validate"}:
            normalized_record = state.item("normalize")
            if normalized_record is None:
                raise invalid_stage(
                    "missing prerequisite stage artifact: normalize",
                    {"stage": stage, "missing_prerequisite": "normalize"},
                )
            normalized_digest = normalized_record["artifact_digest"]
            normalized_fingerprint = normalized_record["stage_fingerprint"]
        if stage in {"assemble", "validate"}:
            allocation_record = state.item("allocate")
            if allocation_record is None:
                raise invalid_stage(
                    "missing prerequisite stage artifact: allocate",
                    {"stage": stage, "missing_prerequisite": "allocate"},
                )
            allocation_digest = allocation_record["artifact_digest"]
            allocation_fingerprint = allocation_record["stage_fingerprint"]
        if stage == "validate":
            assembly_record = state.item("assemble")
            if assembly_record is None:
                raise invalid_stage(
                    "missing prerequisite stage artifact: assemble",
                    {"stage": stage, "missing_prerequisite": "assemble"},
                )
            assembly_digest = assembly_record["artifact_digest"]
            assembly_fingerprint = assembly_record["stage_fingerprint"]
        components = _components_for_stage(
            stage,
            snapshot.config,
            snapshot.shards,
            normalized_digest=normalized_digest,
            allocation_digest=allocation_digest,
            assembly_digest=assembly_digest,
            normalized_fingerprint=normalized_fingerprint,
            allocation_fingerprint=allocation_fingerprint,
            assembly_fingerprint=assembly_fingerprint,
        )
        return self.store.publish(
            stage=stage,
            payload=payload,
            stage_fingerprint=stage_fingerprint(components),
            fingerprint_components=components,
            input_shards=snapshot.shards,
            input_shard_fingerprint=snapshot.input_shard_fingerprint,
            config_version=snapshot.config_version,
            config_revision=snapshot.config_revision,
            expected_seq=state.current_seq + 1,
        )

    def _stop_error(
        self,
        snapshot: Snapshot,
        *,
        stage: str | None,
        rebuilt: list[dict[str, Any]],
        reused: list[str],
    ) -> PipelineError:
        state = self.store.load()
        try:
            status = self._inspect_snapshot(snapshot, state)
            resume_from = (
                status["invalidation"]["earliest_stage"]
                if status["invalidation"]
                else None
            )
        except Exception:
            resume_from = None
        return PipelineError(
            E_STOPPED,
            "stop request observed after the current indivisible sample unit",
            {
                "stage_interrupted": stage,
                "published_stages": rebuilt,
                "reused_stages": reused,
                "checkpoint_seq": state.current_seq,
                "resume_from": resume_from,
            },
            http_status=202,
        )

    def run(
        self,
        *,
        force: bool = False,
        stop_after_samples: int | None = None,
        delay_ms: int = 0,
    ) -> dict[str, Any]:
        snapshot = self.snapshot()
        state = self.store.load()
        inspected_before = self._inspect_snapshot(snapshot, state)
        if self.workspace.stop_requested():
            raise PipelineError(
                E_STOP_REQUESTED,
                "a stop request is pending; use resume to clear it and continue",
                {"stop": self.workspace.stop_status()},
                http_status=409,
            )
        if not force and inspected_before["invalidation"] is None:
            result = {
                "ok": True,
                "status": "up_to_date",
                "invalidated_from": None,
                "invalidation": None,
                "reused_stages": list(STAGES),
                "rebuilt_stages": [],
                "checkpoint_seq_before": state.current_seq,
                "checkpoint_seq_after": state.current_seq,
                "published_checkpoints": [],
                "stages": inspected_before["stages"],
                "digests": self._current_validation_digests(state),
                "empty_errors": inspected_before["empty_errors"],
                "pipeline_fingerprint_expected": inspected_before[
                    "pipeline_fingerprint_expected"
                ],
            }
            self.workspace.write_last_run(result)
            return result

        earliest = (
            "normalize"
            if force
            else inspected_before["invalidation"]["earliest_stage"]  # type: ignore[index]
        )
        earliest_index = stage_index(earliest)
        controller = StopController(
            self.workspace,
            stop_after_samples=stop_after_samples,
            delay_ms=delay_ms,
        )
        payloads: dict[str, dict[str, Any]] = {}
        rebuilt: list[dict[str, Any]] = []
        reused: list[str] = []
        rebuild_started = force
        current_stage: str | None = None
        seq_before = state.current_seq
        try:
            for index, stage in enumerate(STAGES):
                current_stage = stage
                record = state.item(stage)
                if not force and not rebuild_started and record is not None:
                    # Upstream stages outside the invalidated suffix are safely
                    # reusable only when their own fingerprint still matches.
                    upstream_components = self._components_from_state(
                        stage, snapshot, state
                    )
                    expected_fp = stage_fingerprint(upstream_components)
                    if record["stage_fingerprint"] == expected_fp:
                        payloads[stage] = self.store.artifact_payload(record)
                        reused.append(stage)
                        continue
                if not force and not rebuild_started and index < earliest_index:
                    # Defensive fallback: if a supposedly reusable prefix no
                    # longer matches, rebuild from here rather than mixing.
                    rebuild_started = True
                controller.before_stage()
                payload = self._execute_stage(stage, snapshot, payloads, controller)
                controller.before_publish()
                record = self._publish_stage(stage, snapshot, state, payload)
                payloads[stage] = payload
                rebuilt.append(
                    {
                        "stage": stage,
                        "seq": record["seq"],
                        "stage_seq": record["stage_seq"],
                        "artifact_digest": record["artifact_digest"],
                    }
                )
                state = self.store.load()
                rebuild_started = True
            current_stage = None
        except StopRequested as exc:
            raise self._stop_error(
                snapshot,
                stage=current_stage,
                rebuilt=rebuilt,
                reused=reused,
            ) from exc

        final_inspected = self._inspect_snapshot(snapshot, self.store.load())
        validation_payload = payloads.get("validate")
        digests = (
            validation_payload.get("digests", {})
            if validation_payload is not None
            else self._current_validation_digests(self.store.load())
        )
        result = {
            "ok": True,
            "status": "rebuilt",
            "invalidated_from": earliest,
            "invalidation": inspected_before["invalidation"],
            "reused_stages": reused,
            "rebuilt_stages": rebuilt,
            "checkpoint_seq_before": seq_before,
            "checkpoint_seq_after": self.store.load().current_seq,
            "published_checkpoints": [item["seq"] for item in rebuilt],
            "stages": final_inspected["stages"],
            "digests": digests,
            "empty_errors": final_inspected["empty_errors"],
            "pipeline_fingerprint_expected": final_inspected[
                "pipeline_fingerprint_expected"
            ],
        }
        self.workspace.write_last_run(result)
        return result

    def _components_from_state(
        self,
        stage: str,
        snapshot: Snapshot,
        state: CheckpointState,
    ) -> dict[str, Any]:
        normalized = state.item("normalize")
        allocation = state.item("allocate")
        assembly = state.item("assemble")
        return _components_for_stage(
            stage,
            snapshot.config,
            snapshot.shards,
            normalized_digest=normalized["artifact_digest"] if normalized else None,
            allocation_digest=allocation["artifact_digest"] if allocation else None,
            assembly_digest=assembly["artifact_digest"] if assembly else None,
            normalized_fingerprint=normalized["stage_fingerprint"] if normalized else None,
            allocation_fingerprint=allocation["stage_fingerprint"] if allocation else None,
            assembly_fingerprint=assembly["stage_fingerprint"] if assembly else None,
        )

    def _current_validation_digests(self, state: CheckpointState) -> dict[str, Any]:
        record = state.item("validate")
        if record is None:
            return {}
        try:
            payload = self.store.artifact_payload(record)
            return payload.get("digests", {})
        except PipelineError:
            return {}

    def run_stage(
        self,
        stage_name: str,
        *,
        stop_after_samples: int | None = None,
        delay_ms: int = 0,
    ) -> dict[str, Any]:
        stage = canonical_stage(stage_name)
        snapshot = self.snapshot()
        state = self.store.load()
        inspected = self._inspect_snapshot(snapshot, state)
        if self.workspace.stop_requested():
            raise PipelineError(
                E_STOP_REQUESTED,
                "a stop request is pending; use resume to clear it and continue",
                {"stop": self.workspace.stop_status()},
                http_status=409,
            )
        target_index = stage_index(stage)
        for prerequisite in STAGES[:target_index]:
            details = inspected["stages"][prerequisite]
            if not details["reusable"]:
                raise invalid_stage(
                    f"cannot run {stage}: prerequisite stage {prerequisite} is not reusable",
                    {
                        "stage": stage,
                        "missing_or_stale_prerequisite": prerequisite,
                        "prerequisite_status": details["status"],
                        "earliest_invalidated_stage": (
                            inspected["invalidation"]["earliest_stage"]
                            if inspected["invalidation"]
                            else None
                        ),
                    },
                )
        components = self._components_from_state(stage, snapshot, state)
        expected_fp = stage_fingerprint(components)
        record = state.item(stage)
        if record is not None and record["stage_fingerprint"] == expected_fp:
            payload = self.store.artifact_payload(record)
            return {
                "ok": True,
                "status": "up_to_date",
                "stage": stage,
                "checkpoint_seq": record["seq"],
                "artifact_digest": record["artifact_digest"],
                "payload_digest": sha256_obj(payload),
            }

        controller = StopController(
            self.workspace,
            stop_after_samples=stop_after_samples,
            delay_ms=delay_ms,
        )
        payloads: dict[str, dict[str, Any]] = {
            prerequisite: self.store.artifact_payload(state.item(prerequisite))  # type: ignore[arg-type]
            for prerequisite in STAGES[:target_index]
        }
        try:
            controller.before_stage()
            payload = self._execute_stage(stage, snapshot, payloads, controller)
            controller.before_publish()
            published = self._publish_stage(stage, snapshot, state, payload)
        except StopRequested as exc:
            raise self._stop_error(snapshot, stage=stage, rebuilt=[], reused=[]) from exc
        result = {
            "ok": True,
            "status": "rebuilt",
            "stage": stage,
            "checkpoint_seq": published["seq"],
            "stage_seq": published["stage_seq"],
            "artifact_digest": published["artifact_digest"],
            "payload_digest": sha256_obj(payload),
            "invalidated_from": stage,
        }
        return result

    def status(self) -> dict[str, Any]:
        status = self.inspect()
        status["workspace"] = str(self.workspace.root)
        return status

    def report(self) -> dict[str, Any]:
        state = self.store.load()
        record = state.item("validate")
        if record is None:
            raise invalid_stage("validation report is not available")
        return self.store.artifact_payload(record)

    def export_report(self, output_path: str | Path) -> dict[str, Any]:
        report = self.report()
        data = canonical_json_line(report)
        path = Path(output_path)
        from .util import atomic_write_bytes

        atomic_write_bytes(path, data)
        return {
            "ok": True,
            "path": str(path.resolve()),
            "bytes": len(data),
            "sha256": sha256_bytes(data),
        }

    def mixed_bytes(self) -> bytes:
        state = self.store.load()
        record = state.item("assemble")
        if record is None:
            raise invalid_stage("mixed set is not available")
        payload = self.store.artifact_payload(record)
        return mixed_product_bytes(payload)

    def export_mixed(self, output_path: str | Path) -> dict[str, Any]:
        data = self.mixed_bytes()
        path = Path(output_path)
        from .util import atomic_write_bytes

        atomic_write_bytes(path, data)
        return {
            "ok": True,
            "path": str(path.resolve()),
            "bytes": len(data),
            "sha256": sha256_bytes(data),
        }

    def change_config(
        self,
        *,
        patch: dict[str, Any] | None = None,
        set_values: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        current = self.workspace.load_config()
        next_config = deep_merge(current, patch or {})
        for key, value in (set_values or {}).items():
            set_dotted(next_config, key, value)
        before_version = config_digest(current)
        saved = self.workspace.replace_config(next_config)
        return {
            "ok": True,
            "changed": config_digest(saved) != before_version,
            "config_version_before": before_version,
            "config_version_after": config_digest(saved),
            "config_revision": saved["revision"],
            "config": saved,
        }

    def replace_shard(self, name: str, source_path: str | Path) -> dict[str, Any]:
        return self.workspace.import_shard_from_path(name, source_path, replace=True)

    def import_shard(self, name: str, source_path: str | Path) -> dict[str, Any]:
        return self.workspace.import_shard_from_path(name, source_path, replace=False)

    def resume(
        self,
        *,
        stop_after_samples: int | None = None,
        delay_ms: int = 0,
    ) -> dict[str, Any]:
        self.workspace.clear_stop()
        return self.run(stop_after_samples=stop_after_samples, delay_ms=delay_ms)

    def verify_incremental(self, *, run_if_needed: bool = True) -> dict[str, Any]:
        run_result = None
        snapshot = self.snapshot()
        state = self.store.load()
        inspected = self._inspect_snapshot(snapshot, state)
        if run_if_needed and inspected["invalidation"] is not None:
            run_result = self.run()
            snapshot = self.snapshot()
            state = self.store.load()
            inspected = self._inspect_snapshot(snapshot, state)
        if inspected["invalidation"] is not None:
            raise invalid_stage(
                "incremental pipeline is not fully built; run it before verification",
                {"invalidation": inspected["invalidation"]},
            )

        incremental = {
            stage: self.store.artifact_payload(state.item(stage))  # type: ignore[arg-type]
            for stage in STAGES
        }
        full_normalize = deduplicate(snapshot.records, snapshot.config)
        full_allocate = allocate(full_normalize, snapshot.config)
        full_assemble = assemble(full_normalize, full_allocate, snapshot.config)
        full_validate = validate(
            full_normalize,
            full_allocate,
            full_assemble,
            snapshot.config,
            normalized_artifact_digest=sha256_obj(full_normalize),
            allocation_artifact_digest=sha256_obj(full_allocate),
            assembly_artifact_digest=sha256_obj(full_assemble),
        )

        product_specs = [
            (
                "keep_list",
                record_keep_product_bytes(incremental["normalize"]),
                record_keep_product_bytes(full_normalize),
            ),
            (
                "allocation_table",
                allocation_product_bytes(incremental["allocate"]),
                allocation_product_bytes(full_allocate),
            ),
            (
                "mixed_set",
                mixed_product_bytes(incremental["assemble"]),
                mixed_product_bytes(full_assemble),
            ),
        ]
        product_digests: dict[str, Any] = {}
        mismatch: dict[str, Any] | None = None
        for product, inc_bytes, full_bytes in product_specs:
            inc_digest = sha256_bytes(inc_bytes)
            full_digest = sha256_bytes(full_bytes)
            equal = inc_bytes == full_bytes
            product_digests[product] = {
                "incremental": inc_digest,
                "full": full_digest,
                "bytes_equal": equal,
                "incremental_bytes": len(inc_bytes),
                "full_bytes": len(full_bytes),
            }
            if not equal and mismatch is None:
                mismatch = self._first_product_difference(
                    product, inc_bytes, full_bytes
                )

        full_artifact_digests = {
            "normalize": sha256_obj(full_normalize),
            "allocate": sha256_obj(full_allocate),
            "assemble": sha256_obj(full_assemble),
            "validate": sha256_obj(full_validate),
        }
        result = {
            "ok": True,
            "status": "consistent",
            "products": product_digests,
            "incremental_stage_digests": {
                stage: state.item(stage)["artifact_digest"]  # type: ignore[index]
                for stage in STAGES
            },
            "full_stage_digests": full_artifact_digests,
            "incremental_run": {
                "status": run_result["status"] if run_result else "not_needed",
                "invalidated_from": run_result.get("invalidated_from") if run_result else None,
                "rebuilt_stages": [item["stage"] for item in run_result["rebuilt_stages"]]
                if run_result
                else [],
                "reused_stages": run_result.get("reused_stages", []) if run_result else [],
            }
            if run_result
            else {"status": "not_needed", "rebuilt_stages": [], "reused_stages": list(STAGES)},
            "checkpoint": {
                "current_seq": state.current_seq,
                "stage_sequence": {
                    stage: (state.item(stage) or {}).get("stage_seq", 0)
                    for stage in STAGES
                },
                "continuous": True,
            },
        }
        if mismatch is not None:
            raise PipelineError(
                E_INCREMENTAL_MISMATCH,
                "incremental products are not byte-identical to a full build",
                {
                    "first_difference": mismatch,
                    "products": product_digests,
                    "incremental_stage_digests": result["incremental_stage_digests"],
                    "full_stage_digests": full_artifact_digests,
                },
                http_status=409,
            )
        return result

    @staticmethod
    def _first_product_difference(
        product: str, incremental: bytes, full: bytes
    ) -> dict[str, Any]:
        def rows(data: bytes) -> list[Any]:
            result = []
            for line in data.splitlines():
                try:
                    result.append(json.loads(line))
                except json.JSONDecodeError:
                    result.append({"raw": line.decode("utf-8", errors="replace")})
            return result

        difference = first_difference(rows(incremental), rows(full), product=product)
        if difference is not None:
            return difference
        return {
            "product": product,
            "kind": "byte",
            "incremental_length": len(incremental),
            "full_length": len(full),
        }
