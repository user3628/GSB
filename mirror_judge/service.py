from __future__ import annotations

import os
import uuid
from typing import Any

from .batching import build_batches
from .bias import build_bias
from .config import Settings
from .errors import BusinessError
from .models import MirrorRecord, parse_record_batch, records_hash, sha256_json
from .policy import (
    affected_artifact_types,
    apply_policy_patch,
    artifact_rule_dependencies,
    diff_policy_rules,
    validate_patch,
    validate_policy,
)
from .replay import execute_batches
from .sampling import select_records, selected_from_selection
from .store import Store


class MirrorJudgeService:
    def __init__(self, store: Store, settings: Settings) -> None:
        self.store = store
        self.settings = settings

    def _crash_if_requested(self, point: str) -> None:
        if self.settings.test_hooks_enabled and self.settings.crash_point == point:
            os._exit(97)

    @staticmethod
    def _snapshot_hash(snapshot_version: str, records: list[MirrorRecord], policy: dict[str, Any]) -> str:
        return sha256_json(
            {
                "snapshot_version": snapshot_version,
                "records_hash": records_hash(records),
                "policy": policy,
            }
        )

    @staticmethod
    def _build_artifacts(records: list[MirrorRecord], policy: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        selection = select_records(records, policy)
        selected = selected_from_selection(records, selection)
        batches = build_batches(selected, policy)
        bias = build_bias(records, selection, batches)
        return selection, batches, bias

    @staticmethod
    def _cascade(
        *,
        previous: dict[str, Any] | None,
        policy: dict[str, Any],
        changed_rules: list[str],
        rebuilt: dict[str, Any],
        reason: str,
    ) -> tuple[dict[str, Any], dict[str, list[str]]]:
        dependencies = artifact_rule_dependencies(policy)
        affected = affected_artifact_types(changed_rules, policy) if changed_rules else ["selection", "batches", "bias"]
        invalidated = []
        if previous is not None:
            for artifact_type in affected:
                artifact = previous["artifacts"].get(artifact_type)
                if artifact is not None:
                    invalidated.append(
                        {
                            "artifact_type": artifact_type,
                            "artifact_id": artifact["artifact_id"],
                            "rule_keys": previous["dependencies"].get(artifact_type, {}).get("rule_keys", []),
                        }
                    )
        cascade = {
            "atomic_rebuild": True,
            "reason": reason,
            "changed_rules": changed_rules,
            "affected_artifact_types": affected,
            "invalidated_previous_artifacts": invalidated,
            "rebuilt_artifacts": [
                {"artifact_type": artifact_type, "artifact_id": rebuilt[artifact_type]["artifact_id"]}
                for artifact_type in ("selection", "batches", "bias")
            ],
        }
        return cascade, dependencies

    def _assert_snapshot_base(self, base_version: int | None) -> tuple[int | None, dict[str, Any] | None]:
        current = self.store.current_version()
        if current is None:
            if base_version not in (None, 0):
                raise BusinessError("E_STALE_BASE", "No committed version exists", {"base_version": base_version})
            return None, None
        if base_version != current:
            raise BusinessError(
                "E_STALE_BASE",
                "base_version does not match the current committed version",
                {"base_version": base_version, "current_version": current},
            )
        return current, self.store.read_snapshot(current)

    def commit_snapshot(
        self,
        *,
        base_version: int | None,
        snapshot_version: str,
        batch_id: str,
        raw_records: list[Any],
        raw_policy: dict[str, Any],
    ) -> dict[str, Any]:
        records = parse_record_batch(batch_id, raw_records)
        policy = validate_policy(raw_policy)
        if not isinstance(snapshot_version, str) or not snapshot_version.strip():
            raise BusinessError("E_SNAPSHOT_INVALID", "snapshot_version must be a non-empty string")
        snapshot_version = snapshot_version.strip()
        with self.store.writer():
            current, previous = self._assert_snapshot_base(base_version)
            if previous is not None and previous["commit"]["snapshot_version"] == snapshot_version:
                raise BusinessError(
                    "E_SNAPSHOT_INVALID",
                    "Snapshot replacement requires a new snapshot_version",
                    {
                        "snapshot_version": snapshot_version,
                        "current_snapshot_version": previous["commit"]["snapshot_version"],
                    },
                )
            for record in records:
                existing = self.store.record_with_key(record.record_key)
                if existing is not None and existing != record.stored_dict():
                    raise BusinessError(
                        "E_CONFLICTING_RECORD",
                        "Snapshot reuses a record identity with different content; first submission is preserved",
                        {"record_key": record.record_key},
                    )
            changed_rules = diff_policy_rules(previous["policy"] if previous else None, policy)
            intent = self.store.create_intent(
                "snapshot",
                current,
                {"snapshot_version": snapshot_version, "batch_id": batch_id, "record_count": len(records)},
            )
            try:
                selection, batches, bias = self._build_artifacts(records, policy)
                cascade, dependencies = self._cascade(
                    previous=previous,
                    policy=policy,
                    changed_rules=changed_rules,
                    rebuilt={"selection": selection, "batches": batches, "bias": bias},
                    reason="initial_snapshot" if previous is None else "snapshot_replacement",
                )
                snapshot_hash = self._snapshot_hash(snapshot_version, records, policy)
                self._crash_if_requested("before_commit")
                version = self.store.commit_snapshot(
                    base_version=base_version,
                    snapshot_version=snapshot_version,
                    records=records,
                    policy=policy,
                    snapshot_hash=snapshot_hash,
                    selection=selection,
                    batches=batches,
                    bias=bias,
                    cascade=cascade,
                    dependency_map=dependencies,
                    intent_id=intent,
                )
            except Exception as exc:
                self.store.abort_intent(intent, str(exc))
                raise
            return self.state(version=version)

    def _exact_batch(self, batch_id: str, records: list[MirrorRecord]) -> bool:
        existing = self.store.get_records_for_batch(batch_id)
        if not existing:
            return False
        if len(existing) != len(records):
            raise BusinessError(
                "E_CONFLICTING_RECORD",
                "batch_id was already committed with a different number of records",
                {"batch_id": batch_id, "existing_count": len(existing), "submitted_count": len(records)},
            )
        for left, right in zip(existing, records):
            if left.stored_dict() != right.stored_dict():
                raise BusinessError(
                    "E_CONFLICTING_RECORD",
                    "Same batch and line identity was submitted with different content; first submission is preserved",
                    {"batch_id": batch_id, "line_no": right.line_no, "record_key": right.record_key},
                )
        return True

    def append_sample_batch(
        self,
        *,
        base_version: int,
        batch_id: str,
        raw_records: list[Any],
    ) -> dict[str, Any]:
        new_records = parse_record_batch(batch_id, raw_records)
        with self.store.writer():
            if self._exact_batch(batch_id, new_records):
                current = self.store.current_version()
                if current is None:
                    raise BusinessError("E_STALE_BASE", "No committed version exists", {"base_version": base_version})
                return {
                    "idempotent": True,
                    "version": current,
                    "batch_id": batch_id,
                    "committed_records": len(new_records),
                    "message": "Identical sample batch already committed; no state changed",
                }
            current = self.store.current_version()
            if current is None or base_version != current:
                raise BusinessError(
                    "E_STALE_BASE",
                    "base_version does not match the current committed version",
                    {"base_version": base_version, "current_version": current},
                )
            snapshot = self.store.read_snapshot(current)
            if snapshot is None:
                raise BusinessError("E_STALE_BASE", "No committed version exists")
            policy = snapshot["policy"]
            all_records = [*snapshot["records"], *new_records]
            intent = self.store.create_intent(
                "sample_batch",
                current,
                {"batch_id": batch_id, "record_count": len(new_records)},
            )
            try:
                selection, batches, bias = self._build_artifacts(all_records, policy)
                cascade, dependencies = self._cascade(
                    previous=snapshot,
                    policy=policy,
                    changed_rules=[],
                    rebuilt={"selection": selection, "batches": batches, "bias": bias},
                    reason="sample_delta",
                )
                snapshot_version = snapshot["commit"]["snapshot_version"]
                snapshot_hash = self._snapshot_hash(snapshot_version, all_records, policy)
                self._crash_if_requested("before_commit")
                version = self.store.commit_sample_batch(
                    base_version=base_version,
                    new_records=new_records,
                    all_records=all_records,
                    policy=policy,
                    snapshot_version=snapshot_version,
                    snapshot_hash=snapshot_hash,
                    selection=selection,
                    batches=batches,
                    bias=bias,
                    cascade=cascade,
                    dependency_map=dependencies,
                    intent_id=intent,
                )
            except Exception as exc:
                self.store.abort_intent(intent, str(exc))
                raise
            return self.state(version=version)

    def apply_policy_increment(
        self,
        *,
        base_version: int,
        base_policy_version: str,
        policy_version: str,
        patch: dict[str, Any],
    ) -> dict[str, Any]:
        patch = validate_patch(patch)
        if not isinstance(policy_version, str) or not policy_version.strip():
            raise BusinessError("E_SNAPSHOT_INVALID", "policy_version must be a non-empty string")
        policy_version = policy_version.strip()
        with self.store.writer():
            current = self.store.current_version()
            if current is None or base_version != current:
                raise BusinessError(
                    "E_STALE_BASE",
                    "base_version does not match the current committed version",
                    {"base_version": base_version, "current_version": current},
                )
            snapshot = self.store.read_snapshot(current)
            assert snapshot is not None
            if snapshot["policy"]["policy_version"] != base_policy_version:
                raise BusinessError(
                    "E_STALE_BASE",
                    "base_policy_version does not match the current policy snapshot",
                    {
                        "base_policy_version": base_policy_version,
                        "current_policy_version": snapshot["policy"]["policy_version"],
                    },
                )
            if policy_version == base_policy_version:
                raise BusinessError(
                    "E_SNAPSHOT_INVALID",
                    "A policy increment must publish a new policy_version",
                    {"policy_version": policy_version},
                )
            policy = apply_policy_patch(snapshot["policy"], patch, policy_version)
            changed_rules = diff_policy_rules(snapshot["policy"], policy)
            if not changed_rules:
                raise BusinessError(
                    "E_SNAPSHOT_INVALID",
                    "Policy increment does not change any mirror rule",
                    {"policy_version": policy_version},
                )
            records = snapshot["records"]
            intent = self.store.create_intent(
                "policy_increment",
                current,
                {
                    "base_policy_version": base_policy_version,
                    "policy_version": policy_version,
                    "changed_rules": changed_rules,
                },
            )
            try:
                selection, batches, bias = self._build_artifacts(records, policy)
                cascade, dependencies = self._cascade(
                    previous=snapshot,
                    policy=policy,
                    changed_rules=changed_rules,
                    rebuilt={"selection": selection, "batches": batches, "bias": bias},
                    reason="policy_rule_change",
                )
                snapshot_version = snapshot["commit"]["snapshot_version"]
                snapshot_hash = self._snapshot_hash(snapshot_version, records, policy)
                self._crash_if_requested("before_commit")
                version = self.store.commit_policy_increment(
                    base_version=base_version,
                    all_records=records,
                    policy=policy,
                    snapshot_version=snapshot_version,
                    snapshot_hash=snapshot_hash,
                    selection=selection,
                    batches=batches,
                    bias=bias,
                    cascade=cascade,
                    dependency_map=dependencies,
                    intent_id=intent,
                )
            except Exception as exc:
                self.store.abort_intent(intent, str(exc))
                raise
            return self.state(version=version)

    def replay(
        self,
        *,
        source_version: int,
        batch_id: str | None = None,
        timeout_ms: int | None = None,
        fault_mode: str = "mirror",
        fault_status: int = 200,
        hang_ms: int = 0,
        raise_on_timeout: bool = False,
    ) -> dict[str, Any]:
        with self.store.writer():
            current = self.store.current_version()
            if current is None or source_version != current:
                raise BusinessError(
                    "E_STALE_BASE",
                    "Replay source_version is not current",
                    {"source_version": source_version, "current_version": current},
                )
            snapshot = self.store.read_snapshot(source_version)
            assert snapshot is not None
            policy = snapshot["policy"]
            all_batches = snapshot["artifacts"]["batches"]["batches"]
            if batch_id is not None:
                batches = [batch for batch in all_batches if batch["batch_id"] == batch_id]
                if not batches:
                    raise BusinessError(
                        "E_SNAPSHOT_INVALID",
                        "Requested replay batch does not belong to the source version",
                        {"batch_id": batch_id, "source_version": source_version},
                    )
            else:
                batches = all_batches
            batch_ids = [batch["batch_id"] for batch in batches]
            timeout_ms = int(timeout_ms if timeout_ms is not None else policy["replay_timeout_ms"])
            if timeout_ms < 1 or timeout_ms > 600000:
                raise BusinessError("E_SNAPSHOT_INVALID", "timeout_ms must be between 1 and 600000")
            run_id = f"run-{uuid.uuid4().hex}"
            intent = self.store.create_intent(
                "replay",
                current,
                {"run_id": run_id, "source_version": source_version, "batch_ids": batch_ids},
            )
            committed = False
            try:
                self.store.start_replay_run(
                    run_id=run_id,
                    source_version=source_version,
                    batch_ids=batch_ids,
                    intent_id=intent,
                )
                self._crash_if_requested("replay_inflight")
                metrics = execute_batches(
                    batches,
                    target_url=self.settings.replay_target_url,
                    timeout_ms=timeout_ms,
                    concurrency_limit=policy["max_batch_concurrency"],
                    fault_mode=fault_mode,
                    fault_status=fault_status,
                    hang_ms=hang_ms,
                )
                replay_payload = {
                    "run_id": run_id,
                    "status": "completed",
                    "source_version": source_version,
                    "batch_ids": batch_ids,
                    **metrics,
                }
                bias = build_bias(
                    snapshot["records"],
                    snapshot["artifacts"]["selection"],
                    snapshot["artifacts"]["batches"],
                    replay_payload,
                )
                cascade, dependencies = self._cascade(
                    previous=snapshot,
                    policy=policy,
                    changed_rules=[],
                    rebuilt={
                        "selection": snapshot["artifacts"]["selection"],
                        "batches": snapshot["artifacts"]["batches"],
                        "bias": bias,
                    },
                    reason="replay_observation",
                )
                cascade["rebuilt_artifacts"] = [{"artifact_type": "bias", "artifact_id": bias["artifact_id"]}]
                self._crash_if_requested("before_replay_commit")
                result_version = self.store.finish_replay_run(
                    run_id=run_id,
                    source_version=source_version,
                    batch_ids=batch_ids,
                    all_records=snapshot["records"],
                    policy=policy,
                    snapshot_version=snapshot["commit"]["snapshot_version"],
                    snapshot_hash=snapshot["commit"]["snapshot_hash"],
                    selection=snapshot["artifacts"]["selection"],
                    batches=snapshot["artifacts"]["batches"],
                    bias=bias,
                    cascade=cascade,
                    dependency_map=dependencies,
                    metrics=replay_payload,
                    intent_id=intent,
                )
                committed = True
            except Exception as exc:
                if not committed:
                    self.store.abort_replay_run(run_id, str(exc))
                    self.store.abort_intent(intent, str(exc))
                raise
            result = {
                "run_id": run_id,
                "source_version": source_version,
                "result_version": result_version,
                "batch_ids": batch_ids,
                "timeout_ms": timeout_ms,
                "metrics": replay_payload,
                "bias": bias,
            }
            if raise_on_timeout and replay_payload["timeout_count"] > 0:
                raise BusinessError(
                    "E_REPLAY_TIMEOUT",
                    "At least one replay request timed out; each timed-out connection was closed and counted as failure",
                    {
                        "run_id": run_id,
                        "result_version": result_version,
                        "timeout_count": replay_payload["timeout_count"],
                        "failure_count": replay_payload["failure_count"],
                        "attribution": bias["attribution"],
                    },
                )
            return result

    def state(self, version: int | None = None) -> dict[str, Any] | None:
        snapshot = self.store.read_snapshot(version)
        if snapshot is None:
            return None
        selection = snapshot["artifacts"]["selection"]
        batches = snapshot["artifacts"]["batches"]
        bias = snapshot["artifacts"]["bias"]
        return {
            "commit": snapshot["commit"],
            "policy": snapshot["policy"],
            "selection_summary": {
                "artifact_id": selection["artifact_id"],
                "total_records": selection["total_records"],
                "eligible_records": selection["eligible_records"],
                "selected_count": selection["selected_count"],
                "coverage": selection["coverage"],
                "rejection_counts": selection["rejection_counts"],
                "by_service": selection["by_service"],
            },
            "batch_summary": {
                "artifact_id": batches["artifact_id"],
                "target_multiplier": batches["target_multiplier"],
                "effective_multiplier": batches["effective_multiplier"],
                "downshift": batches["downshift"],
                "batch_count": batches["batch_count"],
                "operation_count": batches["operation_count"],
                "split_connections": batches["split_connections"],
                "states": snapshot["batch_states"],
            },
            "bias_summary": {
                "artifact_id": bias["artifact_id"],
                "online_original": bias["online_original"],
                "sample": bias["sample"],
                "sampling_bias": bias["sampling_bias"],
                "replay_amplification_bias": bias["replay_amplification_bias"],
                "attribution": bias["attribution"],
            },
            "cascade": snapshot["commit"]["cascade"],
            "snapshot_hash": snapshot["commit"]["snapshot_hash"],
        }

    def selection(self, version: int | None = None) -> dict[str, Any] | None:
        snapshot = self.store.read_snapshot(version)
        if snapshot is None:
            return None
        return {"commit": snapshot["commit"], "selection": snapshot["artifacts"]["selection"]}

    def batches(self, version: int | None = None) -> dict[str, Any] | None:
        snapshot = self.store.read_snapshot(version)
        if snapshot is None:
            return None
        return {
            "commit": snapshot["commit"],
            "batches": snapshot["artifacts"]["batches"],
            "batch_states": snapshot["batch_states"],
        }

    def bias(self, version: int | None = None) -> dict[str, Any] | None:
        snapshot = self.store.read_snapshot(version)
        if snapshot is None:
            return None
        return {"commit": snapshot["commit"], "bias": snapshot["artifacts"]["bias"]}

    def records(self, version: int | None = None) -> dict[str, Any] | None:
        snapshot = self.store.read_snapshot(version)
        if snapshot is None:
            return None
        return {
            "commit": snapshot["commit"],
            "records": [record.public_dict() for record in snapshot["records"]],
        }
