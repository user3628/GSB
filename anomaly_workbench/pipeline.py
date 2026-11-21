"""Four-stage orchestration, invalidation, stop/resume and verification."""

from __future__ import annotations

import copy
import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from .checkpoints import CheckpointStore
from .config import (
    STAGES,
    changed_leaf_paths,
    config_version,
    default_config,
    earliest_stage_for_config_change,
    merge_config,
    stage_config_fingerprints,
    validate_config,
)
from .data import TierData, import_dataset as data_import_dataset, load_tier, replace_shard as data_replace_shard
from .detectors import fit_detectors
from .diagnostics import build_diagnostics
from .errors import WorkbenchError
from .preprocessing import matrix_from_preprocess, preprocess_tier
from .scoring import score_and_decide
from .util import (
    atomic_write_json,
    canonical_json_bytes,
    digest_object,
    file_lock,
    read_json,
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_stage_artifact(
    *,
    stage: str,
    tier: str,
    input_fingerprint: str,
    stage_config_fingerprint: str,
    payload: dict[str, Any],
) -> bytes:
    artifact = {
        "schema_version": 1,
        "stage": stage,
        "tier": tier,
        "input_fingerprint": input_fingerprint,
        "stage_config_fingerprint": stage_config_fingerprint,
        "payload": payload,
    }
    return canonical_json_bytes(artifact)


class Pipeline:
    def __init__(self, root: str | os.PathLike[str], *, config: dict[str, Any] | None = None) -> None:
        self.root = Path(root)
        self._ensure_layout()
        self.config_path = self.root / "config.json"
        if config is not None:
            self.config = validate_config(config)
            atomic_write_json(self.config_path, self.config)
        elif self.config_path.exists():
            self.config = validate_config(read_json(self.config_path))
        else:
            self.config = default_config()
            atomic_write_json(self.config_path, self.config)
        self.store = CheckpointStore(self.root)
        self.stop_path = self.root / "control" / "stop.json"
        self.execution_path = self.root / "control" / "execution.json"
        self.invalidation_path = self.root / "state" / "invalidations.json"
        self.reference_path = self.root / "reference" / "reference.json"
        self.exports_dir = self.root / "exports"

    def _ensure_layout(self) -> None:
        for relative in ("data", "checkpoints", "locks", "control", "exports", "reference", "state"):
            (self.root / relative).mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # configuration and fingerprinting
    # ------------------------------------------------------------------
    def refresh(self) -> None:
        if self.config_path.exists():
            self.config = validate_config(read_json(self.config_path))

    def _stage_config_fingerprints(self, config: dict[str, Any] | None = None) -> dict[str, str]:
        return stage_config_fingerprints(config or self.config)

    def _preprocess_input(self, tier: str, data: TierData, config: dict[str, Any]) -> str:
        return digest_object(
            {
                "stage": "preprocess",
                "schema_version": 1,
                "tier": tier,
                "data_fingerprint": data.data_fingerprint,
                "config_fingerprint": self._stage_config_fingerprints(config)["preprocess"],
            }
        )

    def _next_input(
        self,
        stage: str,
        tier: str,
        config: dict[str, Any],
        upstream: dict[str, dict[str, Any]],
    ) -> str | None:
        configs = self._stage_config_fingerprints(config)
        if stage == "fit":
            if "preprocess" not in upstream:
                return None
            return digest_object(
                {
                    "stage": "fit",
                    "schema_version": 1,
                    "tier": tier,
                    "preprocess_input": upstream["preprocess"]["input_fingerprint"],
                    "preprocess_artifact": upstream["preprocess"]["artifact_digest"],
                    "config_fingerprint": configs["fit"],
                }
            )
        if stage == "score":
            if not {"preprocess", "fit"}.issubset(upstream):
                return None
            return digest_object(
                {
                    "stage": "score",
                    "schema_version": 1,
                    "tier": tier,
                    "preprocess_artifact": upstream["preprocess"]["artifact_digest"],
                    "fit_artifact": upstream["fit"]["artifact_digest"],
                    "config_fingerprint": configs["score"],
                }
            )
        if stage == "diagnostics":
            if "score" not in upstream:
                return None
            return digest_object(
                {
                    "stage": "diagnostics",
                    "schema_version": 1,
                    "tier": tier,
                    "score_artifact": upstream["score"]["artifact_digest"],
                    "config_fingerprint": configs["diagnostics"],
                }
            )
        raise WorkbenchError("E_INVALID_STAGE", f"unknown stage {stage}", stage=stage)

    def _latest_record(
        self, records: list[dict[str, Any]], stage: str, tier: str
    ) -> dict[str, Any] | None:
        for record in reversed(records):
            if record["stage"] == stage and record["tier"] == tier:
                return record
        return None

    def _matching_history(
        self,
        records: list[dict[str, Any]],
        stage: str,
        tier: str,
        input_fingerprint: str,
    ) -> dict[str, Any] | None:
        for record in reversed(records):
            if (
                record["stage"] == stage
                and record["tier"] == tier
                and record["input_fingerprint"] == input_fingerprint
            ):
                return record
        return None

    # ------------------------------------------------------------------
    # stage execution
    # ------------------------------------------------------------------
    def _build_payload(
        self,
        stage: str,
        data: TierData,
        upstream_artifacts: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        if stage == "preprocess":
            return preprocess_tier(data, self.config)
        if stage == "fit":
            matrix = matrix_from_preprocess(upstream_artifacts["preprocess"])
            return fit_detectors(matrix, self.config["detectors"])
        if stage == "score":
            return score_and_decide(
                upstream_artifacts["preprocess"],
                upstream_artifacts["fit"],
                self.config,
                data.declaration,
            )
        if stage == "diagnostics":
            return build_diagnostics(
                upstream_artifacts["score"], data.declaration, self.config
            )
        raise WorkbenchError("E_INVALID_STAGE", f"unknown stage: {stage}", stage=stage)

    def _ensure_stage(
        self,
        *,
        stage: str,
        data: TierData,
        input_fingerprint: str,
        upstream: dict[str, dict[str, Any]],
        scan_records: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], dict[str, Any], str]:
        latest = self._latest_record(scan_records, stage, data.tier)
        upstream_fingerprints = {
            name: value["input_fingerprint"] for name, value in upstream.items()
        }
        if latest is not None and latest["input_fingerprint"] == input_fingerprint:
            artifact = self.store.load_artifact(latest)
            return latest, artifact, "reused"

        historical = self._matching_history(
            scan_records, stage, data.tier, input_fingerprint
        )
        if historical is not None:
            artifact_bytes = self.store.artifact_bytes(historical)
            action = "republished"
            artifact = self.store.load_artifact(historical)
        else:
            upstream_artifacts = {
                name: value["artifact"] for name, value in upstream.items()
            }
            payload = self._build_payload(stage, data, upstream_artifacts)
            stage_config_fingerprint = self._stage_config_fingerprints()[stage]
            artifact_bytes = make_stage_artifact(
                stage=stage,
                tier=data.tier,
                input_fingerprint=input_fingerprint,
                stage_config_fingerprint=stage_config_fingerprint,
                payload=payload,
            )
            artifact = json.loads(artifact_bytes.decode("utf-8"))
            action = "rebuilt"

        record = self.store.publish(
            stage=stage,
            tier=data.tier,
            input_fingerprint=input_fingerprint,
            config_version=config_version(self.config),
            artifact_bytes=artifact_bytes,
            upstream_fingerprints=upstream_fingerprints,
        )
        artifact["artifact_digest"] = record["artifact_digest"]
        return record, artifact, action

    def _resolve_tier(
        self,
        tier: str,
        *,
        stage_hook: Callable[[str, str, dict[str, Any]], None] | None = None,
        stop_after: set[tuple[str, str]] | None = None,
        verify_reference: bool = False,
        through_stage: str | None = None,
        actions: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        actions = actions if actions is not None else []
        data = load_tier(self.root, tier)
        stage_configs = self._stage_config_fingerprints()
        upstream: dict[str, dict[str, Any]] = {}
        reference = self._load_reference() if verify_reference else None
        for stage in STAGES:
            if stage == "preprocess":
                input_fingerprint = self._preprocess_input(tier, data, self.config)
            else:
                input_fingerprint = self._next_input(stage, tier, self.config, upstream)
            if input_fingerprint is None:
                raise WorkbenchError(
                    "E_INVALID_STAGE",
                    f"cannot execute {stage} without upstream artifacts",
                    stage=stage,
                    details={"tier": tier, "upstream": list(upstream)},
                )
            self._write_execution_state(
                "running",
                tiers=[tier],
                current_tier=tier,
                current_stage=stage,
                actions=actions,
            )
            scan_records = self.store.scan()["records"]
            record, artifact, action = self._ensure_stage(
                stage=stage,
                data=data,
                input_fingerprint=input_fingerprint,
                upstream=upstream,
                scan_records=scan_records,
            )
            if artifact.get("input_fingerprint") != input_fingerprint:
                raise WorkbenchError(
                    "E_INCREMENTAL_MISMATCH",
                    "artifact input fingerprint differs from requested stage input",
                    stage=stage,
                    details={
                        "artifact_input": artifact.get("input_fingerprint"),
                        "expected_input": input_fingerprint,
                    },
                )
            action_record = {
                "tier": tier,
                "stage": stage,
                "action": action,
                "seq": record["seq"],
                "artifact_digest": record["artifact_digest"],
                "input_fingerprint": input_fingerprint,
                "config_fingerprint": stage_configs[stage],
            }
            actions.append(action_record)
            upstream[stage] = {
                "record": record,
                "artifact": artifact,
                "artifact_digest": record["artifact_digest"],
                "input_fingerprint": input_fingerprint,
            }
            self._verify_reference_stage(reference, stage, tier, record)
            if stage_hook is not None:
                stage_hook(stage, tier, record)
            if stop_after is not None and (tier, stage) in stop_after:
                self.request_stop()
            if self.stop_requested():
                self._write_execution_state(
                    "stopped",
                    tiers=[tier],
                    current_tier=tier,
                    current_stage=stage,
                    actions=actions,
                )
                return {
                    "status": "stopped",
                    "stopped_after": {"tier": tier, "stage": stage},
                    "seq": record["seq"],
                    "actions": actions,
                }
            if through_stage is not None and stage == through_stage:
                return {
                    "status": "partial",
                    "tier": tier,
                    "completed_stage": stage,
                    "seq": record["seq"],
                    "actions": actions,
                }
        return {"status": "complete", "tier": tier, "actions": actions}

    def run(
        self,
        tiers: Iterable[str] | None = None,
        *,
        stage_hook: Callable[[str, str, dict[str, Any]], None] | None = None,
        stop_after: Iterable[tuple[str, str]] | None = None,
        verify_reference: bool = False,
        through_stage: str | None = None,
    ) -> dict[str, Any]:
        lock = file_lock(self.root / "locks" / "pipeline.lock", blocking=False)
        try:
            lock.acquire()
        except BlockingIOError as exc:
            raise WorkbenchError(
                "E_BUSY", "another pipeline execution already holds the workspace lock", details={}
            ) from exc
        try:
            self.refresh()
            selected = self._select_tiers(tiers)
            if through_stage is not None and through_stage not in STAGES:
                raise WorkbenchError(
                    "E_INVALID_STAGE",
                    f"unknown through_stage: {through_stage}",
                    stage=through_stage,
                    details={"valid_stages": list(STAGES)},
                )
            if through_stage == STAGES[-1]:
                through_stage = None
            stop_set = set(stop_after or [])
            actions: list[dict[str, Any]] = []
            partial_results: list[dict[str, Any]] = []
            self._write_execution_state(
                "running", tiers=selected, current_tier=None, current_stage=None, actions=actions
            )
            for tier in selected:
                result = self._resolve_tier(
                    tier,
                    stage_hook=stage_hook,
                    stop_after=stop_set,
                    verify_reference=verify_reference,
                    through_stage=through_stage,
                    actions=actions,
                )
                if result["status"] == "stopped":
                    return {
                        "status": "stopped",
                        "tiers": selected,
                        "stopped_after": result["stopped_after"],
                        "seq": result["seq"],
                        "actions": actions,
                        "checkpoints": self.store.status(include_records=False),
                    }
                if result["status"] == "partial":
                    partial_results.append(result)
                    continue
            if partial_results:
                self._write_execution_state(
                    "partial", tiers=selected, current_tier=None, current_stage=through_stage, actions=actions
                )
                return {
                    "status": "partial",
                    "tiers": selected,
                    "completed_through": through_stage,
                    "tier_results": partial_results,
                    "actions": actions,
                    "checkpoints": self.store.status(include_records=False),
                }
            self._write_execution_state(
                "complete", tiers=selected, current_tier=None, current_stage=None, actions=actions
            )
            return {
                "status": "complete",
                "tiers": selected,
                "actions": actions,
                "reused_stages": sum(1 for item in actions if item["action"] == "reused"),
                "rebuilt_stages": sum(1 for item in actions if item["action"] == "rebuilt"),
                "republished_stages": sum(1 for item in actions if item["action"] == "republished"),
                "checkpoints": self.store.status(include_records=False),
            }
        except WorkbenchError as exc:
            try:
                error_tiers = self._select_tiers(tiers)
            except WorkbenchError:
                error_tiers = []
            self._write_execution_state(
                "error",
                tiers=error_tiers,
                current_tier=None,
                current_stage=exc.stage,
                actions=[],
                error=exc.to_dict(),
            )
            raise
        finally:
            lock.release()

    def _select_tiers(self, tiers: Iterable[str] | None) -> list[str]:
        manifest_path = self.root / "manifest.json"
        if not manifest_path.exists():
            raise WorkbenchError("E_NOT_FOUND", "workspace manifest.json is missing", stage="import")
        available = list(read_json(manifest_path).get("tiers", {}).keys())
        if tiers is None:
            preferred = [value for value in ("small", "medium") if value in available]
            return preferred + sorted(value for value in available if value not in preferred)
        selected = list(tiers)
        missing = [value for value in selected if value not in available]
        if missing:
            raise WorkbenchError(
                "E_NOT_FOUND",
                "requested tiers are not imported",
                stage="import",
                details={"missing": missing, "available": sorted(available)},
            )
        return selected

    # ------------------------------------------------------------------
    # stop / resume and execution state
    # ------------------------------------------------------------------
    def request_stop(self) -> dict[str, Any]:
        atomic_write_json(
            self.stop_path,
            {"requested": True, "requested_at": _now(), "pid": os.getpid()},
        )
        return {"stop_requested": True, "path": str(self.stop_path)}

    def clear_stop(self) -> None:
        try:
            self.stop_path.unlink()
        except FileNotFoundError:
            pass

    def stop_requested(self) -> bool:
        if not self.stop_path.exists():
            return False
        try:
            return bool(read_json(self.stop_path).get("requested"))
        except Exception:
            return True

    def resume(self, tiers: Iterable[str] | None = None, **kwargs: Any) -> dict[str, Any]:
        self.clear_stop()
        result = self.run(tiers=tiers, **kwargs)
        return {**result, "resumed": True}

    def _write_execution_state(
        self,
        status: str,
        *,
        tiers: Iterable[str],
        current_tier: str | None,
        current_stage: str | None,
        actions: list[dict[str, Any]],
        error: dict[str, Any] | None = None,
    ) -> None:
        atomic_write_json(
            self.execution_path,
            {
                "schema_version": 1,
                "status": status,
                "tiers": list(tiers),
                "current_tier": current_tier,
                "current_stage": current_stage,
                "actions": actions,
                "stop_requested": self.stop_requested(),
                "updated_at": _now(),
                "pid": os.getpid(),
                "error": error,
            },
        )

    # ------------------------------------------------------------------
    # invalidation events and configuration changes
    # ------------------------------------------------------------------
    def _load_invalidation_state(self) -> dict[str, Any]:
        if not self.invalidation_path.exists():
            return {"schema_version": 1, "next_event": 1, "events": []}
        try:
            state = read_json(self.invalidation_path)
            if not isinstance(state, dict) or not isinstance(state.get("events"), list):
                raise ValueError("invalid invalidation state")
            return state
        except Exception:
            return {"schema_version": 1, "next_event": 1, "events": []}

    def invalidations(self, tier: str | None = None) -> list[dict[str, Any]]:
        events = self._load_invalidation_state()["events"]
        if tier is None:
            return events
        return [event for event in events if event.get("tier") == tier]

    def _append_invalidation(self, event: dict[str, Any]) -> dict[str, Any]:
        with file_lock(self.root / "locks" / "state.lock"):
            state = self._load_invalidation_state()
            sequence = int(state.get("next_event", 1))
            stored = {
                "schema_version": 1,
                "event_seq": sequence,
                **event,
            }
            state["events"].append(stored)
            state["next_event"] = sequence + 1
            atomic_write_json(self.invalidation_path, state)
        return stored

    def _checkpoint_snapshot(self, tier: str) -> dict[str, Any]:
        records = self.store.scan()["records"]
        result: dict[str, Any] = {}
        for stage in STAGES:
            record = self._latest_record(records, stage, tier)
            result[stage] = (
                None
                if record is None
                else {
                    "seq": record["seq"],
                    "input_fingerprint": record["input_fingerprint"],
                    "artifact_digest": record["artifact_digest"],
                }
            )
        return result

    def update_config(self, patch: dict[str, Any]) -> dict[str, Any]:
        pipeline_lock = self._with_pipeline_lock()
        try:
            with file_lock(self.root / "locks" / "config.lock"):
                self.refresh()
                old_config = copy.deepcopy(self.config)
                new_config = validate_config(merge_config(old_config, patch))
                earliest = earliest_stage_for_config_change(old_config, new_config)
                if earliest is None:
                    return {"changed": False, "affected_stages": [], "events": []}
                changed_paths = changed_leaf_paths(old_config, new_config)
                atomic_write_json(self.config_path, new_config)
                self.config = new_config
                old_fps = stage_config_fingerprints(old_config)
                new_fps = stage_config_fingerprints(new_config)
                affected = list(STAGES[STAGES.index(earliest) :])
                events: list[dict[str, Any]] = []
                for tier in self._select_tiers(None):
                    store_snapshot = self._checkpoint_snapshot(tier)
                    events.append(
                        self._append_invalidation(
                            {
                                "kind": "config_change",
                                "reason": "configuration changed",
                                "tier": tier,
                                "changed_paths": changed_paths,
                                "earliest_stage": earliest,
                                "affected_stages": affected,
                                "reused_stages": list(STAGES[: STAGES.index(earliest)]),
                                "old_config_version": config_version(old_config),
                                "new_config_version": config_version(new_config),
                                "old_stage_config_fingerprints": old_fps,
                                "new_stage_config_fingerprints": new_fps,
                                "checkpoint_snapshot_before": store_snapshot,
                                "checkpoint_snapshot_after": store_snapshot,
                            }
                        )
                    )
                return {
                    "changed": True,
                    "earliest_stage": earliest,
                    "affected_stages": affected,
                    "reused_stages": list(STAGES[: STAGES.index(earliest)]),
                    "old_config_version": config_version(old_config),
                    "new_config_version": config_version(new_config),
                    "changed_paths": changed_paths,
                    "events": events,
                }
        finally:
            pipeline_lock.release()

    # ------------------------------------------------------------------
    # import and replacement wrappers with invalidation observations
    # ------------------------------------------------------------------
    def _with_pipeline_lock(self) -> Any:
        lock = file_lock(self.root / "locks" / "pipeline.lock", blocking=False)
        try:
            lock.acquire()
        except BlockingIOError as exc:
            raise WorkbenchError(
                "E_BUSY", "pipeline is executing; data/configuration mutation is not allowed", details={}
            ) from exc
        return lock

    def import_dataset(
        self,
        tier: str,
        declaration_source: str | os.PathLike[str],
        shard_sources: Iterable[str | os.PathLike[str]],
        *,
        replace: bool = True,
    ) -> dict[str, Any]:
        old_fingerprint: str | None = None
        try:
            old_fingerprint = load_tier(self.root, tier).data_fingerprint
        except WorkbenchError:
            pass
        lock = self._with_pipeline_lock()
        try:
            result = data_import_dataset(
                self.root, tier, declaration_source, shard_sources, replace=replace
            )
            new_fingerprint = result["observation"]["data_fingerprint"]
            event = None
            if old_fingerprint != new_fingerprint:
                event = self._append_invalidation(
                    {
                        "kind": "dataset_import",
                        "reason": "sample shard contents or declaration changed",
                        "tier": tier,
                        "changed_paths": [f"data.{tier}"],
                        "earliest_stage": "preprocess",
                        "affected_stages": list(STAGES),
                        "reused_stages": [],
                        "old_data_fingerprint": old_fingerprint,
                        "new_data_fingerprint": new_fingerprint,
                        "old_stage_config_fingerprints": self._stage_config_fingerprints(),
                        "new_stage_config_fingerprints": self._stage_config_fingerprints(),
                        "checkpoint_snapshot_before": self._checkpoint_snapshot(tier),
                        "checkpoint_snapshot_after": self._checkpoint_snapshot(tier),
                    }
                )
            return {**result, "invalidation": event}
        finally:
            lock.release()

    def replace_shard(
        self,
        tier: str,
        shard_source: str | os.PathLike[str],
        declaration_source: str | os.PathLike[str] | None = None,
    ) -> dict[str, Any]:
        old_fingerprint = load_tier(self.root, tier).data_fingerprint
        old_snapshot = self._checkpoint_snapshot(tier)
        lock = self._with_pipeline_lock()
        try:
            result = data_replace_shard(
                self.root, tier, shard_source, declaration_source=declaration_source
            )
            new_fingerprint = result["observation"]["data_fingerprint"]
            event = None
            if old_fingerprint != new_fingerprint:
                event = self._append_invalidation(
                    {
                        "kind": "shard_replacement",
                        "reason": "single batch shard was replaced",
                        "tier": tier,
                        "changed_paths": [f"data.{tier}.batch.{result['replaced_batch']}"],
                        "earliest_stage": "preprocess",
                        "affected_stages": list(STAGES),
                        "reused_stages": [],
                        "old_data_fingerprint": old_fingerprint,
                        "new_data_fingerprint": new_fingerprint,
                        "old_stage_config_fingerprints": self._stage_config_fingerprints(),
                        "new_stage_config_fingerprints": self._stage_config_fingerprints(),
                        "checkpoint_snapshot_before": old_snapshot,
                        "checkpoint_snapshot_after": self._checkpoint_snapshot(tier),
                    }
                )
            return {**result, "invalidation": event}
        finally:
            lock.release()

    # ------------------------------------------------------------------
    # status, artifacts, diagnostics and exports
    # ------------------------------------------------------------------
    def _actual_chain(self, tier: str) -> dict[str, Any]:
        self.refresh()
        data = load_tier(self.root, tier)
        records = self.store.scan()["records"]
        configs = self._stage_config_fingerprints()
        result: dict[str, Any] = {
            "data": data.observation(),
            "config_version": config_version(self.config),
            "stages": {},
        }
        upstream: dict[str, dict[str, Any]] = {}
        for stage in STAGES:
            expected_input = (
                self._preprocess_input(tier, data, self.config)
                if stage == "preprocess"
                else self._next_input(stage, tier, self.config, upstream)
            )
            latest = self._latest_record(records, stage, tier)
            if expected_input is None:
                state = "blocked"
            elif latest is None:
                state = "missing"
            elif latest["input_fingerprint"] == expected_input:
                state = "complete"
            else:
                state = "stale"
            stage_info = {
                "status": state,
                "config_fingerprint": configs[stage],
                "expected_input_fingerprint": expected_input,
                "latest_checkpoint": (
                    None
                    if latest is None
                    else {
                        "seq": latest["seq"],
                        "input_fingerprint": latest["input_fingerprint"],
                        "artifact_digest": latest["artifact_digest"],
                        "artifact_path": latest["artifact_path"],
                        "config_version": latest["config_version"],
                    }
                ),
            }
            result["stages"][stage] = stage_info
            if state == "complete":
                upstream[stage] = {
                    "input_fingerprint": expected_input,
                    "artifact_digest": latest["artifact_digest"],
                }
        result["fingerprints"] = {
            stage: {
                "config": configs[stage],
                "input": result["stages"][stage]["expected_input_fingerprint"],
                "artifact": (
                    None
                    if result["stages"][stage]["latest_checkpoint"] is None
                    else result["stages"][stage]["latest_checkpoint"]["artifact_digest"]
                ),
            }
            for stage in STAGES
        }
        return result

    def status(self, *, include_records: bool = False) -> dict[str, Any]:
        self.refresh()
        manifest_path = self.root / "manifest.json"
        tiers: dict[str, Any] = {}
        if manifest_path.exists():
            for tier in read_json(manifest_path).get("tiers", {}):
                try:
                    chain = self._actual_chain(tier)
                    chain["invalidations"] = self.invalidations(tier)
                    tiers[tier] = chain
                except WorkbenchError as exc:
                    tiers[tier] = {"error": exc.to_dict(), "invalidations": self.invalidations(tier)}
        execution: Any = None
        if self.execution_path.exists():
            try:
                execution = read_json(self.execution_path)
            except Exception:
                execution = {"status": "unreadable"}
        reference: dict[str, Any] = {"available": self.reference_path.exists()}
        if self.reference_path.exists():
            try:
                reference_data = read_json(self.reference_path)
                reference["config_version"] = reference_data.get("config_version")
                reference["matches_current_config"] = (
                    reference_data.get("config_version") == config_version(self.config)
                )
            except Exception:
                reference["available"] = False
        return {
            "schema_version": 1,
            "config_version": config_version(self.config),
            "config": self.config,
            "tiers": tiers,
            "checkpoints": self.store.status(include_records=include_records),
            "control": {
                "stop_requested": self.stop_requested(),
                "execution": execution,
            },
            "reference": reference,
        }

    def current_artifacts(self, tier: str) -> dict[str, dict[str, Any]]:
        self.refresh()
        data = load_tier(self.root, tier)
        records = self.store.scan()["records"]
        upstream: dict[str, dict[str, Any]] = {}
        artifacts: dict[str, dict[str, Any]] = {}
        for stage in STAGES:
            expected_input = (
                self._preprocess_input(tier, data, self.config)
                if stage == "preprocess"
                else self._next_input(stage, tier, self.config, upstream)
            )
            if expected_input is None:
                raise WorkbenchError(
                    "E_INVALID_STAGE",
                    f"cannot load current {stage} artifact without valid upstream stages",
                    stage=stage,
                    details={"tier": tier},
                )
            record = self._latest_record(records, stage, tier)
            if record is None or record["input_fingerprint"] != expected_input:
                raise WorkbenchError(
                    "E_INVALID_STAGE",
                    f"no current valid checkpoint for stage {stage}",
                    stage=stage,
                    details={
                        "tier": tier,
                        "expected_input_fingerprint": expected_input,
                        "latest": None
                        if record is None
                        else {
                            "seq": record["seq"],
                            "input_fingerprint": record["input_fingerprint"],
                        },
                    },
                )
            artifact = self.store.load_artifact(record)
            artifacts[stage] = artifact
            upstream[stage] = {
                "input_fingerprint": expected_input,
                "artifact_digest": record["artifact_digest"],
            }
        return artifacts

    def sensitivity(self, tier: str) -> dict[str, Any]:
        artifacts = self.current_artifacts(tier)
        payload = artifacts["diagnostics"]["payload"]
        return {
            "tier": tier,
            "threshold": payload["threshold"],
            "sensitivity": payload["sensitivity"],
            "by_source": payload["by_source"],
            "sample_size_error_model": payload["sample_size_error_model"],
            "diagnostics_artifact_digest": artifacts["diagnostics"]["artifact_digest"],
        }

    def summary(self) -> dict[str, Any]:
        """Small deterministic two-tier overview for dashboards and CLI use."""

        self.refresh()
        tiers: dict[str, Any] = {}
        for tier in self._select_tiers(None):
            try:
                artifacts = self.current_artifacts(tier)
                diagnostics = artifacts["diagnostics"]["payload"]
                score_payload = artifacts["score"]["payload"]
                tiers[tier] = {
                    "rows": artifacts["preprocess"]["payload"]["row_count"],
                    "columns": artifacts["preprocess"]["payload"]["column_count"],
                    "missing_column_indices": artifacts["preprocess"]["payload"][
                        "missing_column_indices"
                    ],
                    "heavy_tail_column_indices": artifacts["preprocess"]["payload"][
                        "heavy_tail_column_indices"
                    ],
                    "threshold": score_payload["threshold"],
                    "detected_count": score_payload["detected_count"],
                    "detection_rate": score_payload["detection_rate"],
                    "score_quantiles": diagnostics["score_distribution"]["quantiles"],
                    "sample_size_error_band": diagnostics["sample_size_error_model"],
                    "by_source": diagnostics["by_source"],
                    "artifact_digests": {
                        stage: artifacts[stage]["artifact_digest"] for stage in STAGES
                    },
                }
            except WorkbenchError as exc:
                tiers[tier] = {
                    "status": "unavailable",
                    "error": exc.to_dict(),
                    "invalidations": self.invalidations(tier),
                }
        return {
            "schema_version": 1,
            "config_version": config_version(self.config),
            "tiers": tiers,
            "checkpoints": self.store.status(include_records=False),
        }

    def export_tier(
        self, tier: str, output_dir: str | os.PathLike[str] | None = None
    ) -> dict[str, Any]:
        artifacts = self.current_artifacts(tier)
        destination = Path(output_dir) if output_dir is not None else self.exports_dir / tier
        destination.mkdir(parents=True, exist_ok=True)
        preprocess = artifacts["preprocess"]
        preprocess_payload = preprocess["payload"]
        preprocess_summary = {
            key: value
            for key, value in preprocess_payload.items()
            if key not in {"values"}
        }
        fit_summary = {
            "tier": tier,
            "artifact_digest": artifacts["fit"]["artifact_digest"],
            "covariance": artifacts["fit"]["payload"]["covariance"],
            "forest": {
                key: value
                for key, value in artifacts["fit"]["payload"]["forest"].items()
                if key != "trees"
            },
        }
        score_payload = artifacts["score"]["payload"]
        scores = {
            "schema_version": 1,
            "tier": tier,
            "source_ids": score_payload["source_ids"],
            "batch_ids": score_payload["batch_ids"],
            "raw_scores": score_payload["raw_scores"],
            "normalized_scores": score_payload["normalized_scores"],
            "weights": score_payload["weights"],
            "combined_scores": score_payload["combined_scores"],
            "threshold": score_payload["threshold"],
            "decisions": score_payload["decisions"],
            "detected_count": score_payload["detected_count"],
            "detection_rate": score_payload["detection_rate"],
            "artifact_digest": artifacts["score"]["artifact_digest"],
        }
        diagnostics = {
            "schema_version": 1,
            **artifacts["diagnostics"]["payload"],
            "artifact_digest": artifacts["diagnostics"]["artifact_digest"],
        }
        manifest = {
            "schema_version": 1,
            "tier": tier,
            "config_version": config_version(self.config),
            "stages": {
                stage: {
                    "input_fingerprint": artifacts[stage]["input_fingerprint"],
                    "stage_config_fingerprint": artifacts[stage]["stage_config_fingerprint"],
                    "artifact_digest": artifacts[stage]["artifact_digest"],
                }
                for stage in STAGES
            },
        }
        files = {
            "preprocess-summary.json": preprocess_summary,
            "model-summary.json": fit_summary,
            "scores.json": scores,
            "diagnostics.json": diagnostics,
            "manifest.json": manifest,
        }
        for filename, value in files.items():
            atomic_write_json(destination / filename, value)
        return {
            "tier": tier,
            "directory": str(destination),
            "files": {name: str(destination / name) for name in files},
            "artifact_digests": {
                stage: artifacts[stage]["artifact_digest"] for stage in STAGES
            },
        }

    # ------------------------------------------------------------------
    # reference and full-build equivalence
    # ------------------------------------------------------------------
    def _load_reference(self) -> dict[str, Any] | None:
        if not self.reference_path.exists():
            return None
        try:
            return read_json(self.reference_path)
        except Exception:
            return None

    def _verify_reference_stage(
        self,
        reference: dict[str, Any] | None,
        stage: str,
        tier: str,
        record: dict[str, Any],
    ) -> None:
        if not reference:
            return
        if reference.get("config_version") != config_version(self.config):
            return
        expected = (
            reference.get("tiers", {})
            .get(tier, {})
            .get("stages", {})
            .get(stage, {})
            .get("artifact_digest")
        )
        if expected and expected != record["artifact_digest"]:
            raise WorkbenchError(
                "E_REFERENCE_MISMATCH",
                "stage artifact differs from the independent local reference",
                stage=stage,
                details={
                    "tier": tier,
                    "expected_digest": expected,
                    "actual_digest": record["artifact_digest"],
                },
            )

    def verify_full_equivalence(self, tier: str) -> dict[str, Any]:
        """Build a shadow workspace from zero and compare every stage byte."""

        lock = self._with_pipeline_lock()
        try:
            self.refresh()
            self.current_artifacts(tier)
            with tempfile.TemporaryDirectory(prefix="awb-full-verify-") as temporary:
                shadow_root = Path(temporary)
                shutil.copytree(self.root / "data", shadow_root / "data")
                shutil.copyfile(self.root / "manifest.json", shadow_root / "manifest.json")
                atomic_write_json(shadow_root / "config.json", self.config)
                shadow = Pipeline(shadow_root)
                run_result = shadow.run(tiers=[tier])
                shadow.current_artifacts(tier)
                comparisons: dict[str, Any] = {}
                for stage in STAGES:
                    main_record = self._latest_record(
                        self.store.scan()["records"], stage, tier
                    )
                    shadow_record = shadow._latest_record(
                        shadow.store.scan()["records"], stage, tier
                    )
                    if main_record is None or shadow_record is None:
                        raise WorkbenchError(
                            "E_INCREMENTAL_MISMATCH",
                            f"missing {stage} artifact during full-build verification",
                            stage=stage,
                            details={"tier": tier},
                        )
                    main_bytes = self.store.artifact_bytes(main_record)
                    shadow_bytes = shadow.store.artifact_bytes(shadow_record)
                    byte_identical = main_bytes == shadow_bytes
                    comparisons[stage] = {
                        "byte_identical": byte_identical,
                        "main_digest": main_record["artifact_digest"],
                        "full_digest": shadow_record["artifact_digest"],
                        "size": len(main_bytes),
                    }
                    if not byte_identical:
                        offset = next(
                            (
                                index
                                for index, (left, right) in enumerate(zip(main_bytes, shadow_bytes))
                                if left != right
                            ),
                            min(len(main_bytes), len(shadow_bytes)),
                        )
                        raise WorkbenchError(
                            "E_INCREMENTAL_MISMATCH",
                            "incremental artifact differs from a from-zero full build",
                            stage=stage,
                            details={
                                "tier": tier,
                                "stage": stage,
                                "first_difference_offset": offset,
                                "main_digest": main_record["artifact_digest"],
                                "full_digest": shadow_record["artifact_digest"],
                                "comparisons": comparisons,
                            },
                        )
                return {
                    "tier": tier,
                    "byte_identical": True,
                    "stages": comparisons,
                    "full_build": {
                        "status": run_result["status"],
                        "actions": run_result["actions"],
                    },
                }
        finally:
            lock.release()
