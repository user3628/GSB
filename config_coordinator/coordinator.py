"""Configuration repository, impact analysis and distribution coordination."""

from __future__ import annotations

import asyncio
import copy
import json
import sqlite3
import time
from typing import Any

from .codec import canonical_json, compute_snapshot_checksum, digest_json, sha256_text, utc_now_iso
from .derived import (
    DerivedBuild,
    build_derived_artifacts,
    changed_refs_for,
)
from .errors import (
    DomainError,
    E_COMPONENT_NOT_FOUND,
    E_DEPENDENCY_CYCLE,
    E_DISPATCH_IN_PROGRESS,
    E_DISPATCH_NOT_FOUND,
    E_INTERNAL,
    E_SNAPSHOT_NOT_CURRENT,
    E_SNAPSHOT_NOT_FOUND,
    E_STALE_BASE,
    E_STALE_SNAPSHOT,
    E_VALIDATION,
)
from .models import (
    ComponentDeclaration,
    IncrementRequest,
    Snapshot,
    SnapshotSubmitRequest,
    TransitionRequest,
    validate_components,
)
from .push import PushResult, post_json
from .state_machine import (
    ALL_STATUSES,
    APPLIED,
    DISPATCHED,
    FAILED,
    PENDING,
    ROLLED_BACK,
    require_transition,
)
from .store import Store
from .topology import (
    affected_components,
    find_dependency_cycle,
    full_batch_indices,
    raise_for_cycle,
    topological_batches,
)


_MISSING = object()


def _changed_mapping_keys(old: dict[str, Any], new: dict[str, Any]) -> list[str]:
    changed: list[str] = []
    for key in sorted(set(old) | set(new)):
        if old.get(key, _MISSING) != new.get(key, _MISSING):
            changed.append(key)
    return changed


def _declaration_map(
    components: list[ComponentDeclaration],
) -> dict[str, ComponentDeclaration]:
    return {component.component_id: component for component in components}


def _changed_component_ids(
    old_components: list[ComponentDeclaration], new_components: list[ComponentDeclaration]
) -> list[str]:
    old_map = _declaration_map(old_components)
    new_map = _declaration_map(new_components)
    changed: list[str] = []
    for component_id in sorted(set(old_map) | set(new_map)):
        old = old_map.get(component_id)
        new = new_map.get(component_id)
        if old is None or new is None or old.stable_dict() != new.stable_dict():
            changed.append(component_id)
    return changed


def _utc_now() -> str:
    return utc_now_iso()


class Coordinator:
    """The embeddable domain component.

    Synchronous methods are used for database reads and atomic submissions;
    :meth:`dispatch` is async because it performs real HTTP pushes.  Every
    persisted mutation follows the same rule: a committed PENDING intent row
    is written first, then the business transaction commits (or rolls back).
    """

    def __init__(self, db_path: str, *, dispatch_wait_seconds: float = 60.0) -> None:
        self.store = Store(db_path)
        self.dispatch_wait_seconds = dispatch_wait_seconds
        self._dispatch_locks: dict[int, asyncio.Lock] = {}

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------
    def current_version(self) -> int:
        with self.store.read_transaction() as connection:
            return self.store.current_version(connection)

    def get_state(self, *, version: int | None = None) -> dict[str, Any]:
        with self.store.read_transaction() as connection:
            current = self.store.current_version(connection)
            selected = current if version is None else version
            snapshot = self.store.load_snapshot(connection, selected) if selected else None
            if selected and snapshot is None:
                raise DomainError(
                    E_SNAPSHOT_NOT_FOUND,
                    "snapshot version was not found",
                    details={"snapshot_version": selected},
                    http_status=404,
                )
            states = (
                self.store.load_component_states(connection, selected) if selected else {}
            )
            artifacts = (
                self.store.load_artifacts(connection, selected) if selected else []
            )
            events = (
                self.store.load_events(connection, version=selected) if selected else []
            )
            return {
                "current_version": current,
                "snapshot": self._snapshot_public(snapshot) if snapshot else None,
                "component_states": sorted(states.values(), key=lambda item: item["component_id"]),
                "derived_artifacts": artifacts,
                "state_events": events,
            }

    def get_derived(self, version: int | None = None) -> dict[str, Any]:
        with self.store.read_transaction() as connection:
            selected = self.store.current_version(connection) if version is None else version
            snapshot = self.store.load_snapshot(connection, selected) if selected else None
            if selected and snapshot is None:
                raise DomainError(
                    E_SNAPSHOT_NOT_FOUND,
                    "snapshot version was not found",
                    details={"snapshot_version": selected},
                    http_status=404,
                )
            return {
                "version": selected,
                "rebuild_scope": snapshot["rebuild_scope"] if snapshot else {},
                "artifacts": self.store.load_artifacts(connection, selected) if selected else [],
            }

    def get_component_state(self, component_id: str, version: int | None = None) -> dict[str, Any]:
        with self.store.read_transaction() as connection:
            selected = self.store.current_version(connection) if version is None else version
            states = self.store.load_component_states(connection, selected)
            state = states.get(component_id)
            if state is None:
                raise DomainError(
                    E_COMPONENT_NOT_FOUND,
                    "component is not present in the selected snapshot",
                    details={"component_id": component_id, "snapshot_version": selected},
                    http_status=404,
                )
            return {
                "snapshot_version": selected,
                "state": state,
                "events": self.store.load_events(
                    connection, version=selected, component_id=component_id
                ),
            }

    def get_snapshot(self, version: int) -> dict[str, Any]:
        with self.store.read_transaction() as connection:
            snapshot = self.store.load_snapshot(connection, version)
            if snapshot is None:
                raise DomainError(
                    E_SNAPSHOT_NOT_FOUND,
                    "snapshot version was not found",
                    details={"snapshot_version": version},
                    http_status=404,
                )
            return self._snapshot_public(snapshot)

    def get_impact(self, version: int | None = None) -> dict[str, Any]:
        with self.store.read_transaction() as connection:
            selected = self.store.current_version(connection) if version is None else version
            snapshot = self.store.load_snapshot(connection, selected) if selected else None
            if snapshot is None:
                raise DomainError(
                    E_SNAPSHOT_NOT_FOUND,
                    "snapshot version was not found",
                    details={"snapshot_version": selected},
                    http_status=404,
                )
            impact = self._impact_from_snapshot(snapshot)
            if impact.get("dependency_error"):
                error = impact["dependency_error"]
                raise DomainError(
                    E_DEPENDENCY_CYCLE,
                    "component dependency graph contains a cycle",
                    details={"cycle": error["cycle"], "cycle_ids": error["cycle_ids"]},
                )
            return impact

    def get_dispatch(self, version: int) -> dict[str, Any]:
        with self.store.read_transaction() as connection:
            row = self.store.get_dispatch_run(connection, version)
            if row is None:
                raise DomainError(
                    E_DISPATCH_NOT_FOUND,
                    "dispatch run was not found",
                    details={"snapshot_version": version},
                    http_status=404,
                )
            result = self.store.dispatch_result(row)
            return {
                "snapshot_version": version,
                "run_id": row["run_id"],
                "status": row["status"],
                "policy": row["policy"],
                "plan": json.loads(row["plan_json"]),
                "result": result,
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
                "error": row["error"],
            }

    def get_plan(self, version: int | None = None) -> dict[str, Any]:
        with self.store.read_transaction() as connection:
            selected = self.store.current_version(connection) if version is None else version
            snapshot = self.store.load_snapshot(connection, selected) if selected else None
            if snapshot is None:
                raise DomainError(
                    E_SNAPSHOT_NOT_FOUND,
                    "snapshot version was not found",
                    details={"snapshot_version": selected},
                    http_status=404,
                )
            artifacts = self.store.load_artifacts(connection, selected)
            states = self.store.load_component_states(connection, selected)
            return self._plan_from_snapshot(snapshot, artifacts, states)

    # ------------------------------------------------------------------
    # Snapshot and increment submission
    # ------------------------------------------------------------------
    def submit_snapshot(
        self,
        snapshot: Snapshot,
        components: list[ComponentDeclaration] | None = None,
    ) -> dict[str, Any]:
        # Preserve the requested precedence: stale version, then integrity.
        if snapshot.version <= self.current_version():
            raise DomainError(
                E_STALE_SNAPSHOT,
                "snapshot version must be greater than the current committed version",
                details={"current_version": self.current_version()},
            )
        snapshot.assert_checksum()

        with self.store.intent_scope(
            "SUBMIT_SNAPSHOT",
            snapshot.version,
            {
                "version": snapshot.version,
                "checksum": snapshot.checksum,
                "source": "FULL",
            },
        ) as (connection, intent_id):
            current = self.store.current_version(connection)
            if snapshot.version <= current:
                raise DomainError(
                    E_STALE_SNAPSHOT,
                    "snapshot version must be greater than the current committed version",
                    details={"current_version": current},
                )
            previous = self.store.load_snapshot(connection, current) if current else None
            selected_components = (
                components
                if components is not None
                else (previous["components"] if previous else [])
            )
            validated = validate_components(selected_components, snapshot.configs)
            report = self._publish_version(
                connection,
                intent_id=intent_id,
                version=snapshot.version,
                base_version=current,
                generated_at=snapshot.generated_at,
                configs=snapshot.configs,
                components=validated,
                checksum=snapshot.checksum,
                source="FULL",
                previous=previous,
            )
        return report

    def submit_increment(self, request: IncrementRequest) -> dict[str, Any]:
        target_version = request.base_version + 1
        with self.store.intent_scope(
            "SUBMIT_INCREMENT",
            target_version,
            {
                "base_version": request.base_version,
                "config_set_keys": sorted(request.config_set),
                "config_delete": request.config_delete,
                "upsert_ids": sorted(
                    component.component_id for component in request.components_upsert
                ),
                "remove_ids": request.components_remove,
            },
        ) as (connection, intent_id):
            current = self.store.current_version(connection)
            if request.base_version != current:
                raise DomainError(
                    E_STALE_BASE,
                    "increment base version is no longer current",
                    details={"base_version": request.base_version, "current_version": current},
                )
            previous = self.store.load_snapshot(connection, current) if current else None
            old_configs = dict(previous["configs"]) if previous else {}
            old_components = list(previous["components"]) if previous else []

            configs = copy.deepcopy(old_configs)
            for key, value in request.config_set.items():
                configs[key] = value
            for key in request.config_delete:
                configs.pop(key, None)
            if not configs:
                raise DomainError(
                    E_VALIDATION,
                    "an increment cannot leave the repository without configuration",
                    http_status=422,
                )

            component_map = _declaration_map(old_components)
            for component_id in request.components_remove:
                component_map.pop(component_id, None)
            for component in request.components_upsert:
                component_map[component.component_id] = component
            components = validate_components(list(component_map.values()), configs)

            generated_at = request.generated_timestamp()
            checksum = compute_snapshot_checksum(target_version, generated_at, configs)
            report = self._publish_version(
                connection,
                intent_id=intent_id,
                version=target_version,
                base_version=current,
                generated_at=generated_at,
                configs=configs,
                components=components,
                checksum=checksum,
                source="INCREMENT",
                previous=previous,
            )
        return report

    def _publish_version(
        self,
        connection: sqlite3.Connection,
        *,
        intent_id: str,
        version: int,
        base_version: int,
        generated_at: str,
        configs: dict[str, Any],
        components: list[ComponentDeclaration],
        checksum: str,
        source: str,
        previous: dict[str, Any] | None,
    ) -> dict[str, Any]:
        old_configs = previous["configs"] if previous else {}
        old_components = previous["components"] if previous else []
        changed_config_keys = _changed_mapping_keys(old_configs, configs)
        changed_component_ids = _changed_component_ids(old_components, components)
        affected = affected_components(components, changed_config_keys, changed_component_ids)

        cycle = find_dependency_cycle(components)
        affected_batches: list[list[str]] = []
        if cycle:
            batch_indices = {component.component_id: -1 for component in components}
            dependency_error = {
                "code": E_DEPENDENCY_CYCLE,
                "cycle": cycle,
                "cycle_ids": sorted(set(cycle)),
            }
        else:
            affected_batches = topological_batches(components, subset=affected)
            full_indices = full_batch_indices(components)
            prior_batch_indices = self._prior_batch_indices(connection, previous)
            batch_indices = {}
            for component in components:
                component_id = component.component_id
                if component_id in affected:
                    batch_indices[component_id] = next(
                        index
                        for index, batch in enumerate(affected_batches)
                        if component_id in batch
                    )
                else:
                    batch_indices[component_id] = prior_batch_indices.get(
                        component_id, full_indices.get(component_id, 0)
                    )
            dependency_error = None

        prior_states = self.store.load_component_states(connection, base_version) if base_version else {}
        changed_refs = changed_refs_for(changed_config_keys, changed_component_ids)
        state_input = {
            component.component_id: {
                "last_applied_digest": (prior_states.get(component.component_id) or {}).get(
                    "last_applied_digest"
                )
            }
            for component in components
        }
        derived = build_derived_artifacts(
            version=version,
            configs=configs,
            components=components,
            states=state_input,
            batch_indices=batch_indices,
            changed_refs=changed_refs,
        )

        new_states: list[dict[str, Any]] = []
        state_events: list[dict[str, Any]] = []
        for component in components:
            component_id = component.component_id
            prior = prior_states.get(component_id)
            if prior is None:
                status = PENDING
            elif component_id in affected:
                status = PENDING
            else:
                status = prior["status"]
            if status not in ALL_STATUSES:
                status = PENDING
            new_states.append(
                {
                    "component_id": component_id,
                    "status": status,
                    "desired_digest": derived.expected_digests[component_id],
                    "last_applied_digest": (prior or {}).get("last_applied_digest"),
                    "last_applied_version": (prior or {}).get("last_applied_version"),
                    "endpoint_url": component.endpoint_url,
                    "request_id": None,
                    "updated_at": _utc_now(),
                }
            )
            if prior is None:
                state_events.append(
                    {
                        "component_id": component_id,
                        "from_status": None,
                        "to_status": PENDING,
                        "reason": "INITIAL_STATE",
                    }
                )
            elif prior["status"] != status:
                state_events.append(
                    {
                        "component_id": component_id,
                        "from_status": prior["status"],
                        "to_status": status,
                        "reason": "NEW_SNAPSHOT_AFFECTED",
                    }
                )

        rebuild_scope = {
            "changed_refs": changed_refs,
            "changed_config_keys": changed_config_keys,
            "changed_component_ids": changed_component_ids,
            "affected_components": affected,
            "invalidated_artifacts": derived.invalidated_artifacts,
            "rebuilt_artifact_count": len(derived.artifacts),
            "invalidated_artifact_count": len(derived.invalidated_artifacts),
            "atomic_transaction": True,
        }
        record = {
            "version": version,
            "base_version": base_version,
            "generated_at": generated_at,
            "configs": configs,
            "components": [component.model_dump(mode="json") for component in components],
            "checksum": checksum,
            "source": source,
            "changed_config_keys": changed_config_keys,
            "changed_component_ids": changed_component_ids,
            "affected_components": affected,
            "rebuild_scope": rebuild_scope,
            "created_at": _utc_now(),
        }

        self.store.insert_snapshot(connection, record)
        self.store.set_current_version(connection, version)
        self.store.write_component_states(connection, version, new_states)
        self.store.write_artifacts(connection, version, derived.artifacts)
        for event in state_events:
            self.store.append_event(
                connection,
                version=version,
                component_id=event["component_id"],
                from_status=event["from_status"],
                to_status=event["to_status"],
                reason=event["reason"],
            )
        self.store.complete_intent(connection, intent_id)

        report = {
            "version": version,
            "base_version": base_version,
            "source": source,
            "checksum": checksum,
            "changed_config_keys": changed_config_keys,
            "changed_component_ids": changed_component_ids,
            "affected_components": affected,
            "batches": affected_batches,
            "rebuild_scope": rebuild_scope,
            "dependency_error": dependency_error,
        }
        return report

    def _prior_batch_indices(
        self,
        connection: sqlite3.Connection,
        previous: dict[str, Any] | None,
    ) -> dict[str, int]:
        if not previous:
            return {}
        artifacts = self.store.load_artifacts(connection, previous["version"])
        return {
            artifact["artifact_id"]: int(artifact["value"].get("batch_index", -1))
            for artifact in artifacts
            if artifact["artifact_type"] == "batch_index"
        }

    # ------------------------------------------------------------------
    # Planning
    # ------------------------------------------------------------------
    def _impact_from_snapshot(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        components = snapshot["components"]
        cycle = find_dependency_cycle(components)
        batches = [] if cycle else topological_batches(
            components, subset=snapshot["affected_components"]
        )
        return {
            "version": snapshot["version"],
            "base_version": snapshot["base_version"],
            "source": snapshot["source"],
            "changed_config_keys": snapshot["changed_config_keys"],
            "changed_component_ids": snapshot["changed_component_ids"],
            "affected_components": snapshot["affected_components"],
            "batches": batches,
            "rebuild_scope": snapshot["rebuild_scope"],
            "dependency_error": (
                {
                    "code": E_DEPENDENCY_CYCLE,
                    "cycle": cycle,
                    "cycle_ids": sorted(set(cycle)),
                }
                if cycle
                else None
            ),
        }

    def _plan_from_snapshot(
        self,
        snapshot: dict[str, Any],
        artifacts: list[dict[str, Any]],
        states: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        raise_for_cycle(snapshot["components"])
        batches = topological_batches(
            snapshot["components"], subset=snapshot["affected_components"]
        )
        batch_by_component = {
            component_id: index
            for index, batch in enumerate(batches)
            for component_id in batch
        }
        artifact_map = {artifact["artifact_key"]: artifact for artifact in artifacts}
        components = _declaration_map(snapshot["components"])
        planned_components = []
        for batch_index, batch in enumerate(batches):
            for component_id in batch:
                component = components[component_id]
                expected = artifact_map[f"expected_digest:{component_id}"]["value"]["digest"]
                consistency = artifact_map[f"consistency_score:{component_id}"]["value"]
                request_id = sha256_text(
                    canonical_json(
                        {
                            "snapshot_version": snapshot["version"],
                            "component_id": component_id,
                            "expected_digest": expected,
                            "batch_index": batch_index,
                        }
                    )
                )
                planned_components.append(
                    {
                        "component_id": component_id,
                        "cluster_id": component.cluster_id,
                        "batch_index": batch_by_component[component_id],
                        "endpoint_url": component.endpoint_url,
                        "config_keys": component.config_keys,
                        "expected_digest": expected,
                        "consistency_score": consistency["score"],
                        "request_id": request_id,
                        "state": states.get(component_id, {}).get("status", PENDING),
                    }
                )
        plan = {
            "snapshot_version": snapshot["version"],
            "source": snapshot["source"],
            "affected_components": snapshot["affected_components"],
            "batches": batches,
            "components": planned_components,
        }
        plan["plan_digest"] = digest_json(plan)
        return plan

    # ------------------------------------------------------------------
    # Dispatch and push
    # ------------------------------------------------------------------
    async def dispatch(
        self,
        version: int,
        *,
        policy: str = "CONTINUE",
        timeout_seconds: float = 1.0,
    ) -> dict[str, Any]:
        if policy not in {"CONTINUE", "ROLLBACK"}:
            raise DomainError(E_VALIDATION, "unknown dispatch policy", http_status=422)
        current = self.current_version()
        if not current:
            raise DomainError(
                E_SNAPSHOT_NOT_FOUND,
                "there is no committed snapshot to dispatch",
                http_status=404,
            )
        if version != current:
            raise DomainError(
                E_SNAPSHOT_NOT_CURRENT,
                "only the current committed snapshot can be dispatched",
                details={"requested_version": version, "current_version": current},
            )
        # A dependency cycle is rejected before a run row or component state is
        # touched, so the failed plan cannot mutate persisted state.
        plan = self.get_plan(version)
        run_id = "run:" + digest_json(
            {"version": version, "plan_digest": plan["plan_digest"], "policy": policy}
        ).split(":", 1)[1][:32]

        claim_intent = self.store.create_intent(
            "DISPATCH_CLAIM", version, {"run_id": run_id, "policy": policy}
        )
        claimed = False
        try:
            with self.store.transaction() as connection:
                existing = self.store.get_dispatch_run(connection, version)
                if existing is None:
                    if self.store.current_version(connection) != version:
                        raise DomainError(
                            E_SNAPSHOT_NOT_CURRENT,
                            "snapshot changed while claiming the dispatch",
                            details={"requested_version": version},
                        )
                    self.store.insert_dispatch_run(
                        connection,
                        version=version,
                        run_id=run_id,
                        policy=policy,
                        plan=plan,
                    )
                    self.store.complete_intent(connection, claim_intent)
                    claimed = True
        except sqlite3.IntegrityError:
            self.store.abort_intent(claim_intent, "concurrent dispatch claim")
            claimed = False
        except BaseException as exc:
            self.store.abort_intent(claim_intent, f"{type(exc).__name__}: {exc}")
            raise
        if not claimed:
            self.store.abort_intent(claim_intent, "reused existing dispatch")
            return await self._reuse_dispatch(version)

        return await self._execute_dispatch(
            version=version,
            run_id=run_id,
            plan=plan,
            policy=policy,
            timeout_seconds=timeout_seconds,
        )

    async def _reuse_dispatch(self, version: int) -> dict[str, Any]:
        deadline = time.monotonic() + self.dispatch_wait_seconds
        while True:
            with self.store.read_transaction() as connection:
                row = self.store.get_dispatch_run(connection, version)
                if row is None:
                    raise DomainError(
                        E_INTERNAL,
                        "dispatch claim disappeared during reuse wait",
                        http_status=500,
                    )
                if row["status"] != "RUNNING":
                    result = self.store.dispatch_result(row)
                    if result is None:
                        raise DomainError(
                            E_INTERNAL,
                            "finished dispatch has no persisted result",
                            http_status=500,
                        )
                    reused = copy.deepcopy(result)
                    reused["reused"] = True
                    reused["reused_from_run_id"] = row["run_id"]
                    return reused
            if time.monotonic() >= deadline:
                raise DomainError(
                    E_DISPATCH_IN_PROGRESS,
                    "another writer is still executing this snapshot dispatch",
                    details={"snapshot_version": version},
                    http_status=409,
                )
            await asyncio.sleep(0.05)

    async def _execute_dispatch(
        self,
        *,
        version: int,
        run_id: str,
        plan: dict[str, Any],
        policy: str,
        timeout_seconds: float,
    ) -> dict[str, Any]:
        component_results: list[dict[str, Any]] = []
        applied_in_run: list[str] = []
        failure_count = 0
        with self.store.read_transaction() as connection:
            snapshot = self.store.load_snapshot(connection, version)
            states = self.store.load_component_states(connection, version)
        if snapshot is None:
            raise DomainError(E_SNAPSHOT_NOT_FOUND, "snapshot disappeared", http_status=404)
        component_map = _declaration_map(snapshot["components"])

        for batch_index, batch in enumerate(plan["batches"]):
            for component_id in batch:
                if component_id not in component_map:
                    raise DomainError(
                        E_COMPONENT_NOT_FOUND,
                        "planned component is not present in the snapshot",
                        details={"component_id": component_id},
                    )
                state = states.get(component_id)
                if state and state["status"] == APPLIED and state.get("last_applied_digest") == state.get("desired_digest"):
                    component_results.append(
                        {
                            "component_id": component_id,
                            "batch_index": batch_index,
                            "status": APPLIED,
                            "ok": True,
                            "code": None,
                            "message": "already applied; push skipped",
                            "elapsed_ms": 0.0,
                            "request_id": state.get("request_id"),
                            "endpoint_url": component_map[component_id].endpoint_url,
                            "already_applied": True,
                        }
                    )
                    applied_in_run.append(component_id)
                    continue
                try:
                    result = await self._push_component(
                        version=version,
                        run_id=run_id,
                        component=component_map[component_id],
                        configs=snapshot["configs"],
                        desired_digest=state["desired_digest"] if state else None,
                        timeout_seconds=timeout_seconds,
                    )
                except DomainError as exc:
                    aborted_result = self._aborted_result(
                        version,
                        run_id,
                        plan,
                        policy,
                        component_results,
                        f"{exc.code}: {exc.message}",
                    )
                    self._finalize_with_intent(
                        version,
                        status="ABORTED",
                        result=aborted_result,
                        error=exc.code,
                    )
                    raise
                result["batch_index"] = batch_index
                component_results.append(result)
                if result["ok"]:
                    applied_in_run.append(component_id)
                else:
                    failure_count += 1

        rollback_results: list[dict[str, Any]] = []
        if policy == "ROLLBACK" and failure_count:
            for component_id in reversed(applied_in_run):
                rollback_results.append(
                    await self._rollback_component(
                        version=version,
                        run_id=run_id,
                        component=component_map[component_id],
                        timeout_seconds=timeout_seconds,
                    )
                )

        with self.store.read_transaction() as connection:
            final_states = self.store.load_component_states(connection, version)
            events = self.store.load_events(connection, version=version)
            artifacts = self.store.load_artifacts(connection, version)

        rolled_back = [item for item in rollback_results if item["ok"]]
        if failure_count == 0:
            conclusion = "ALL_APPLIED"
            run_status = "COMPLETED"
        elif policy == "ROLLBACK" and len(rolled_back) == len(applied_in_run):
            conclusion = "ROLLED_BACK"
            run_status = "ROLLED_BACK"
        elif policy == "ROLLBACK":
            conclusion = "ROLLBACK_INCOMPLETE"
            run_status = "COMPLETED"
        else:
            conclusion = "PARTIAL_FAILURE"
            run_status = "COMPLETED"

        consistency_scores = {
            artifact["artifact_id"]: artifact["value"]["score"]
            for artifact in artifacts
            if artifact["artifact_type"] == "consistency_score"
        }
        result = {
            "snapshot_version": version,
            "run_id": run_id,
            "status": run_status,
            "policy": policy,
            "conclusion": conclusion,
            "reused": False,
            "affected_components": plan["affected_components"],
            "batches": plan["batches"],
            "components": component_results,
            "rollback_results": rollback_results,
            "failure_count": failure_count,
            "final_states": sorted(final_states.values(), key=lambda item: item["component_id"]),
            "state_events": events,
            "consistency_scores": consistency_scores,
            "errors": [
                item["error"]
                for item in component_results
                if not item.get("ok") and item.get("error")
            ],
            "finished_at": _utc_now(),
        }
        if not plan["affected_components"]:
            result["conclusion"] = "NO_AFFECTED_COMPONENTS"
            result["status"] = "COMPLETED"
        self._finalize_with_intent(version, status=result["status"], result=result)
        return result

    async def _push_component(
        self,
        *,
        version: int,
        run_id: str,
        component: ComponentDeclaration,
        configs: dict[str, Any],
        desired_digest: str | None,
        timeout_seconds: float,
    ) -> dict[str, Any]:
        with self.store.read_transaction() as connection:
            state = self.store.load_component_states(connection, version).get(
                component.component_id
            )
        if state is None:
            raise DomainError(
                E_COMPONENT_NOT_FOUND,
                "component state is missing",
                details={"component_id": component.component_id},
                http_status=404,
            )
        if state["status"] != PENDING:
            raise DomainError(
                "E_INVALID_TRANSITION",
                "dispatch can only start from PENDING",
                details={
                    "component_id": component.component_id,
                    "from_status": state["status"],
                    "to_status": DISPATCHED,
                },
            )
        expected_digest = desired_digest or state["desired_digest"]
        request_id = sha256_text(
            canonical_json(
                {
                    "run_id": run_id,
                    "snapshot_version": version,
                    "component_id": component.component_id,
                    "expected_digest": expected_digest,
                }
            )
        )
        payload = {
            "request_id": request_id,
            "snapshot_version": version,
            "component_id": component.component_id,
            "config_keys": component.config_keys,
            "configs": {key: configs[key] for key in component.config_keys},
            "expected_config_version": component.expected_config_version,
            "expected_digest": expected_digest,
        }
        push_intent = self.store.create_intent(
            "PUSH",
            version,
            {
                "run_id": run_id,
                "component_id": component.component_id,
                "request_id": request_id,
                "endpoint_url": component.endpoint_url,
            },
        )
        try:
            with self.store.transaction() as connection:
                current_state = self.store.load_component_states(connection, version).get(
                    component.component_id
                )
                if current_state is None:
                    raise DomainError(
                        E_COMPONENT_NOT_FOUND,
                        "component state disappeared before dispatch",
                        details={"component_id": component.component_id},
                        http_status=404,
                    )
                require_transition(current_state["status"], DISPATCHED, reason="PUSH_STARTED")
                self.store.update_component_state(
                    connection,
                    version,
                    component.component_id,
                    status=DISPATCHED,
                    last_applied_digest=current_state.get("last_applied_digest"),
                    last_applied_version=current_state.get("last_applied_version"),
                    request_id=request_id,
                )
                self.store.append_event(
                    connection,
                    version=version,
                    component_id=component.component_id,
                    from_status=current_state["status"],
                    to_status=DISPATCHED,
                    reason="PUSH_STARTED",
                )
        except BaseException as exc:
            self.store.abort_intent(push_intent, f"{type(exc).__name__}: {exc}")
            raise

        push_result: PushResult = await post_json(
            component.endpoint_url, payload, timeout_seconds=timeout_seconds
        )
        final_status = APPLIED if push_result.ok else FAILED
        try:
            with self.store.transaction() as connection:
                current_state = self.store.load_component_states(connection, version).get(
                    component.component_id
                )
                if current_state is None or current_state["status"] != DISPATCHED:
                    raise DomainError(
                        E_INTERNAL,
                        "component left DISPATCHED while its HTTP request was in flight",
                        details={
                            "component_id": component.component_id,
                            "status": (current_state or {}).get("status"),
                        },
                        http_status=500,
                    )
                require_transition(
                    DISPATCHED,
                    final_status,
                    reason=push_result.code or "PUSH_ACCEPTED",
                )
                last_digest = expected_digest if push_result.ok else current_state.get("last_applied_digest")
                last_version = version if push_result.ok else current_state.get("last_applied_version")
                self.store.update_component_state(
                    connection,
                    version,
                    component.component_id,
                    status=final_status,
                    last_applied_digest=last_digest,
                    last_applied_version=last_version,
                    request_id=request_id,
                )
                self.store.append_event(
                    connection,
                    version=version,
                    component_id=component.component_id,
                    from_status=DISPATCHED,
                    to_status=final_status,
                    reason=push_result.code or "PUSH_ACCEPTED",
                )
                self._refresh_derived_in_connection(
                    connection, version, changed_state_ids=[component.component_id]
                )
                self.store.complete_intent(connection, push_intent)
        except BaseException as exc:
            self.store.abort_intent(push_intent, f"{type(exc).__name__}: {exc}")
            raise

        response = {
            "component_id": component.component_id,
            "status": final_status,
            "ok": push_result.ok,
            "code": push_result.code,
            "message": push_result.message,
            "http_status": push_result.status_code,
            "elapsed_ms": round(push_result.elapsed_ms, 3),
            "request_id": request_id,
            "endpoint_url": component.endpoint_url,
            "endpoint_response": push_result.response,
        }
        if not push_result.ok:
            response["error"] = {
                "code": push_result.code,
                "message": push_result.message,
                "details": {
                    "component_id": component.component_id,
                    "http_status": push_result.status_code,
                    "endpoint_response": push_result.response,
                },
            }
        return response

    async def _rollback_component(
        self,
        *,
        version: int,
        run_id: str,
        component: ComponentDeclaration,
        timeout_seconds: float,
    ) -> dict[str, Any]:
        rollback_url = (
            component.endpoint_url[: -len("/push")] + "/rollback"
            if component.endpoint_url.endswith("/push")
            else component.endpoint_url.rstrip("/") + "/rollback"
        )
        with self.store.read_transaction() as connection:
            state = self.store.load_component_states(connection, version).get(
                component.component_id
            )
        rollback_request_id = sha256_text(
            canonical_json(
                {
                    "kind": "rollback",
                    "run_id": run_id,
                    "snapshot_version": version,
                    "component_id": component.component_id,
                }
            )
        )
        rollback_intent = self.store.create_intent(
            "ROLLBACK",
            version,
            {
                "run_id": run_id,
                "component_id": component.component_id,
                "request_id": rollback_request_id,
                "endpoint_url": rollback_url,
            },
        )
        push_result = await post_json(
            rollback_url,
            {
                "request_id": rollback_request_id,
                "snapshot_version": version,
                "component_id": component.component_id,
            },
            timeout_seconds=timeout_seconds,
        )
        rollback_status = ROLLED_BACK if push_result.ok else (state or {}).get("status", APPLIED)
        try:
            with self.store.transaction() as connection:
                current_state = self.store.load_component_states(connection, version).get(
                    component.component_id
                )
                if current_state is None:
                    raise DomainError(
                        E_COMPONENT_NOT_FOUND,
                        "component state disappeared during rollback",
                        details={"component_id": component.component_id},
                        http_status=404,
                    )
                if push_result.ok:
                    require_transition(
                        current_state["status"], ROLLED_BACK, reason="ROLLBACK_POLICY"
                    )
                    self.store.update_component_state(
                        connection,
                        version,
                        component.component_id,
                        status=ROLLED_BACK,
                        last_applied_digest=None,
                        last_applied_version=None,
                        request_id=rollback_request_id,
                    )
                    self.store.append_event(
                        connection,
                        version=version,
                        component_id=component.component_id,
                        from_status=current_state["status"],
                        to_status=ROLLED_BACK,
                        reason="ROLLBACK_POLICY",
                    )
                    self._refresh_derived_in_connection(
                        connection, version, changed_state_ids=[component.component_id]
                    )
                else:
                    self.store.append_event(
                        connection,
                        version=version,
                        component_id=component.component_id,
                        from_status=current_state["status"],
                        to_status=current_state["status"],
                        reason=f"ROLLBACK_FAILED:{push_result.code}",
                    )
                self.store.complete_intent(connection, rollback_intent)
        except BaseException as exc:
            self.store.abort_intent(rollback_intent, f"{type(exc).__name__}: {exc}")
            raise
        return {
            "component_id": component.component_id,
            "ok": push_result.ok,
            "status": rollback_status,
            "code": push_result.code,
            "message": push_result.message,
            "elapsed_ms": round(push_result.elapsed_ms, 3),
            "request_id": rollback_request_id,
        }

    def _refresh_derived_in_connection(
        self,
        connection: sqlite3.Connection,
        version: int,
        *,
        changed_state_ids: list[str] | None = None,
    ) -> None:
        snapshot = self.store.load_snapshot(connection, version)
        if snapshot is None:
            raise DomainError(
                E_SNAPSHOT_NOT_FOUND,
                "cannot refresh derived artifacts for a missing snapshot",
                details={"snapshot_version": version},
                http_status=404,
            )
        states = self.store.load_component_states(connection, version)
        artifacts = self.store.load_artifacts(connection, version)
        batch_indices = {
            artifact["artifact_id"]: int(artifact["value"].get("batch_index", -1))
            for artifact in artifacts
            if artifact["artifact_type"] == "batch_index"
        }
        changed_refs = [f"state:{component_id}" for component_id in (changed_state_ids or [])]
        rebuilt = build_derived_artifacts(
            version=version,
            configs=snapshot["configs"],
            components=snapshot["components"],
            states=states,
            batch_indices=batch_indices,
            changed_refs=changed_refs,
        )
        self.store.write_artifacts(connection, version, rebuilt.artifacts)

    def _aborted_result(
        self,
        version: int,
        run_id: str,
        plan: dict[str, Any],
        policy: str,
        component_results: list[dict[str, Any]],
        error: str,
    ) -> dict[str, Any]:
        return {
            "snapshot_version": version,
            "run_id": run_id,
            "status": "ABORTED",
            "policy": policy,
            "conclusion": "ABORTED",
            "error": error,
            "affected_components": plan["affected_components"],
            "batches": plan["batches"],
            "components": component_results,
            "finished_at": _utc_now(),
        }

    def _finalize_with_intent(
        self,
        version: int,
        *,
        status: str,
        result: dict[str, Any],
        error: str | None = None,
    ) -> None:
        finalize_intent = self.store.create_intent(
            "DISPATCH_FINALIZE",
            version,
            {"status": status, "run_id": result.get("run_id")},
        )
        try:
            with self.store.transaction() as connection:
                self.store.finalize_dispatch_run(
                    connection,
                    version,
                    status=status,
                    result=result,
                    error=error,
                )
                self.store.complete_intent(connection, finalize_intent)
        except BaseException as exc:
            self.store.abort_intent(finalize_intent, f"{type(exc).__name__}: {exc}")
            raise

    # ------------------------------------------------------------------
    # Manual transitions
    # ------------------------------------------------------------------
    def transition_component(self, component_id: str, request: TransitionRequest) -> dict[str, Any]:
        with self.store.intent_scope(
            "STATE_TRANSITION",
            request.snapshot_version,
            {
                "component_id": component_id,
                "to_status": request.to_status,
                "reason": request.reason,
            },
        ) as (connection, intent_id):
            current = self.store.current_version(connection)
            if request.snapshot_version != current:
                raise DomainError(
                    E_SNAPSHOT_NOT_CURRENT,
                    "manual transitions must target the current committed snapshot",
                    details={
                        "requested_version": request.snapshot_version,
                        "current_version": current,
                    },
                )
            states = self.store.load_component_states(connection, request.snapshot_version)
            state = states.get(component_id)
            if state is None:
                raise DomainError(
                    E_COMPONENT_NOT_FOUND,
                    "component is not present in the current snapshot",
                    details={"component_id": component_id, "snapshot_version": current},
                    http_status=404,
                )
            require_transition(state["status"], request.to_status, reason=request.reason)
            last_digest = state.get("last_applied_digest")
            last_version = state.get("last_applied_version")
            if request.to_status == APPLIED:
                last_digest = state["desired_digest"]
                last_version = current
            elif request.to_status == ROLLED_BACK:
                last_digest = None
                last_version = None
            self.store.update_component_state(
                connection,
                current,
                component_id,
                status=request.to_status,
                last_applied_digest=last_digest,
                last_applied_version=last_version,
                request_id=state.get("request_id"),
            )
            self.store.append_event(
                connection,
                version=current,
                component_id=component_id,
                from_status=state["status"],
                to_status=request.to_status,
                reason=request.reason,
            )
            if request.to_status in {APPLIED, ROLLED_BACK}:
                self._refresh_derived_in_connection(
                    connection, current, changed_state_ids=[component_id]
                )
            self.store.complete_intent(connection, intent_id)
        return self.get_component_state(component_id, current)

    # ------------------------------------------------------------------
    # Recovery
    # ------------------------------------------------------------------
    def recover(self) -> dict[str, Any]:
        """Abort unfinished intents/runs using only persisted SQLite facts."""

        with self.store.read_transaction() as connection:
            current_before = self.store.current_version(connection)
            pending_before = self.store.pending_intents(connection)
            runs_before = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM dispatch_runs WHERE status='RUNNING' ORDER BY version"
                ).fetchall()
            ]
        recovery_intent = self.store.create_intent(
            "RECOVERY",
            current_before,
            {"pending_intent_count": len(pending_before), "running_dispatch_count": len(runs_before)},
        )
        aborted_intents: list[dict[str, Any]] = []
        aborted_runs: list[dict[str, Any]] = []
        recovered_transitions: list[dict[str, Any]] = []
        try:
            with self.store.transaction() as connection:
                pending = [
                    item
                    for item in self.store.pending_intents(connection)
                    if item["intent_id"] != recovery_intent
                ]
                for intent in pending:
                    self.store.abort_intent_in_transaction(
                        connection,
                        intent["intent_id"],
                        "RECOVERED_UNFINISHED_INTENT",
                    )
                    aborted_intents.append(
                        {
                            "intent_id": intent["intent_id"],
                            "kind": intent["kind"],
                            "snapshot_version": intent["snapshot_version"],
                            "action": "ABORTED",
                        }
                    )
                    payload = intent["payload"]
                    if intent["kind"] in {"PUSH", "ROLLBACK", "STATE_TRANSITION"}:
                        component_id = payload.get("component_id")
                        version = intent["snapshot_version"]
                        state = self.store.load_component_states(connection, version).get(
                            component_id
                        )
                        if state and state["status"] == DISPATCHED:
                            require_transition(
                                DISPATCHED, ROLLED_BACK, reason="RECOVERED_UNCONFIRMED_PUSH"
                            )
                            self.store.update_component_state(
                                connection,
                                version,
                                component_id,
                                status=ROLLED_BACK,
                                last_applied_digest=state.get("last_applied_digest"),
                                last_applied_version=state.get("last_applied_version"),
                                request_id=state.get("request_id"),
                            )
                            self.store.append_event(
                                connection,
                                version=version,
                                component_id=component_id,
                                from_status=DISPATCHED,
                                to_status=ROLLED_BACK,
                                reason="RECOVERED_UNCONFIRMED_PUSH",
                            )
                            recovered_transitions.append(
                                {
                                    "component_id": component_id,
                                    "snapshot_version": version,
                                    "from_status": DISPATCHED,
                                    "to_status": ROLLED_BACK,
                                    "reason": "RECOVERED_UNCONFIRMED_PUSH",
                                }
                            )
                        elif (
                            intent["kind"] == "ROLLBACK"
                            and state
                            and state["status"] == APPLIED
                        ):
                            require_transition(
                                APPLIED, ROLLED_BACK, reason="RECOVERED_ROLLBACK_INTENT"
                            )
                            self.store.update_component_state(
                                connection,
                                version,
                                component_id,
                                status=ROLLED_BACK,
                                last_applied_digest=None,
                                last_applied_version=None,
                                request_id=state.get("request_id"),
                            )
                            self.store.append_event(
                                connection,
                                version=version,
                                component_id=component_id,
                                from_status=APPLIED,
                                to_status=ROLLED_BACK,
                                reason="RECOVERED_ROLLBACK_INTENT",
                            )
                            recovered_transitions.append(
                                {
                                    "component_id": component_id,
                                    "snapshot_version": version,
                                    "from_status": APPLIED,
                                    "to_status": ROLLED_BACK,
                                    "reason": "RECOVERED_ROLLBACK_INTENT",
                                }
                            )

                running_rows = connection.execute(
                    "SELECT * FROM dispatch_runs WHERE status='RUNNING' ORDER BY version"
                ).fetchall()
                for row in running_rows:
                    version = row["version"]
                    states = self.store.load_component_states(connection, version)
                    result = {
                        "snapshot_version": version,
                        "run_id": row["run_id"],
                        "status": "ABORTED",
                        "policy": row["policy"],
                        "conclusion": "ABORTED_PROCESS_TERMINATION",
                        "recovered": True,
                        "facts": {
                            "component_states": sorted(
                                states.values(), key=lambda item: item["component_id"]
                            ),
                            "state_events": self.store.load_events(
                                connection, version=version
                            ),
                        },
                        "finished_at": _utc_now(),
                    }
                    self.store.finalize_dispatch_run(
                        connection,
                        version,
                        status="ABORTED",
                        result=result,
                        error="RECOVERED_PROCESS_TERMINATION",
                    )
                    aborted_runs.append(
                        {
                            "snapshot_version": version,
                            "run_id": row["run_id"],
                            "status": "ABORTED",
                            "conclusion": "ABORTED_PROCESS_TERMINATION",
                        }
                    )
                current_after = self.store.current_version(connection)
                self.store.complete_intent(connection, recovery_intent)
        except BaseException as exc:
            self.store.abort_intent(recovery_intent, f"{type(exc).__name__}: {exc}")
            raise
        return {
            "recovery_intent_id": recovery_intent,
            "last_complete_version": current_before,
            "current_version_after": current_after,
            "pending_before": [
                {
                    "intent_id": item["intent_id"],
                    "kind": item["kind"],
                    "snapshot_version": item["snapshot_version"],
                }
                for item in pending_before
            ],
            "running_dispatches_before": [
                {
                    "snapshot_version": row["version"],
                    "run_id": row["run_id"],
                    "status": row["status"],
                }
                for row in runs_before
            ],
            "aborted_intents": aborted_intents,
            "aborted_dispatches": aborted_runs,
            "recovered_state_transitions": recovered_transitions,
            "facts_basis": "Only committed SQLite intent, dispatch, state and event rows were used.",
        }

    # ------------------------------------------------------------------
    # Serialisation helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _snapshot_public(snapshot: dict[str, Any]) -> dict[str, Any]:
        return {
            "version": snapshot["version"],
            "base_version": snapshot["base_version"],
            "generated_at": snapshot["generated_at"],
            "configs": snapshot["configs"],
            "components": [
                component.model_dump(mode="json") for component in snapshot["components"]
            ],
            "checksum": snapshot["checksum"],
            "source": snapshot["source"],
            "changed_config_keys": snapshot["changed_config_keys"],
            "changed_component_ids": snapshot["changed_component_ids"],
            "affected_components": snapshot["affected_components"],
            "rebuild_scope": snapshot["rebuild_scope"],
            "created_at": snapshot["created_at"],
        }

