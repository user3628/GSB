from __future__ import annotations

import hashlib
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from . import validation
from .errors import BusinessError, spec_invalid
from .graph import CascadeResult, DependencyGraph
from .models import AppRuntime, AppState, Application, Cluster, SnapshotSpec
from .planning import BatchPlan, build_batch_plan
from .state_machine import ensure_transition, initial_state, transitions_as_dict
from .storage import Database, json_dumps, json_loads, utc_now


@dataclass(frozen=True)
class EngineConfig:
    switch_threshold: int = 1000
    max_batch_replicas: int = 512
    trigger_wait_seconds: float = 60.0
    default_query_limit: int = 5000
    crash_after: str | None = None


@dataclass
class SnapshotView:
    version: int
    base_version: int | None
    generated_at: str
    checksum: str
    clusters: dict[str, Cluster]
    applications: dict[str, Application]
    states: dict[str, AppRuntime]
    affected_app_ids: tuple[str, ...]
    affected_cluster_ids: tuple[str, ...]
    reevaluation_app_ids: tuple[str, ...]
    status: str

    @property
    def graph(self) -> DependencyGraph:
        return DependencyGraph(self.applications.values(), self.clusters.keys())


class Engine:
    def __init__(self, db_path: str | Path, config: EngineConfig | None = None) -> None:
        self.config = config or EngineConfig()
        self.db = Database(db_path)

    # ------------------------------------------------------------------
    # Snapshot submission
    # ------------------------------------------------------------------
    def submit_snapshot(self, payload: dict[str, Any]) -> dict[str, Any]:
        spec = validation.parse_snapshot_document(payload)
        intent_id = self._create_intent(
            "SNAPSHOT_COMMIT",
            spec.version,
            {"checksum": spec.checksum, "application_count": len(spec.applications)},
        )
        self._halt("snapshot_intent")
        try:
            with self.db.transaction() as connection:
                current = self._current_version_on(connection)
                if current is not None and spec.version <= current:
                    raise BusinessError(
                        "E_STALE_BASE",
                        "snapshot version must increase monotonically",
                        {"version": spec.version, "current_version": current},
                    )
                states = {
                    app.app_id: AppRuntime(
                        app_id=app.app_id,
                        state=initial_state(app.desired_version, app.effective_version),
                        effective_version=app.effective_version,
                        requires_reevaluation=False,
                    )
                    for app in spec.applications
                }
                affected = tuple(app.app_id for app in spec.applications)
                affected_clusters = tuple(sorted({app.cluster_id for app in spec.applications}))
                self._insert_snapshot_data(
                    connection,
                    spec,
                    states,
                    affected,
                    affected_clusters,
                    reevaluation_app_ids=(),
                )
                self._complete_intent(connection, intent_id)
        except BaseException as exc:
            self._abort_intent(intent_id, exc)
            raise
        return self.get_snapshot_summary(spec.version)

    def submit_increment(self, base_version: int, payload: dict[str, Any]) -> dict[str, Any]:
        increment = validation.parse_increment_document(
            payload, path_base_version=base_version
        )
        intent_id = self._create_intent(
            "INCREMENT_COMMIT",
            increment["version"],
            {
                "base_version": base_version,
                "changed_app_ids": [change["app_id"] for change in increment["changes"]],
            },
        )
        self._halt("increment_intent")
        try:
            with self.db.transaction() as connection:
                current = self._current_version_on(connection)
                if current != base_version:
                    raise BusinessError(
                        "E_STALE_BASE",
                        "increment base version is no longer current",
                        {"base_version": base_version, "current_version": current},
                    )
                if increment["version"] <= base_version:
                    raise BusinessError(
                        "E_STALE_BASE",
                        "increment version must be greater than the base version",
                        {
                            "base_version": base_version,
                            "version": increment["version"],
                        },
                    )
                base_view = self._load_snapshot_view_on(connection, base_version)
                if increment.get("base_checksum"):
                    if increment["base_checksum"].lower() != base_view.checksum:
                        raise BusinessError(
                            "E_STALE_BASE",
                            "increment base checksum does not match the persisted base snapshot",
                            {
                                "base_version": base_version,
                                "expected_base_checksum": base_view.checksum,
                                "actual_base_checksum": increment["base_checksum"].lower(),
                            },
                        )

                new_configs = dict(base_view.applications)
                changed_ids = {change["app_id"] for change in increment["changes"]}
                for change in increment["changes"]:
                    app_id = change["app_id"]
                    if app_id not in new_configs:
                        raise spec_invalid(
                            "increment references an unknown application",
                            app_id=app_id,
                        )
                    old = new_configs[app_id]
                    cluster_id = change.get("cluster_id", old.cluster_id)
                    if cluster_id not in base_view.clusters:
                        raise spec_invalid(
                            "increment references an unknown cluster",
                            app_id=app_id,
                            cluster_id=cluster_id,
                        )
                    new_configs[app_id] = Application(
                        app_id=app_id,
                        cluster_id=cluster_id,
                        desired_version=change.get("desired_version", old.desired_version),
                        effective_version=change.get(
                            "effective_version", old.effective_version
                        ),
                        replica_budget=change.get("replica_budget", old.replica_budget),
                        dependencies=tuple(
                            sorted(change.get("dependencies", old.dependencies))
                        ),
                    )

                for app in new_configs.values():
                    missing = set(app.dependencies) - set(new_configs)
                    if missing:
                        raise spec_invalid(
                            "increment creates a missing dependency reference",
                            app_id=app.app_id,
                            missing_dependencies=sorted(missing),
                        )

                snapshot_payload = {
                    "version": increment["version"],
                    "generated_at": increment["generated_at"],
                    "clusters": [
                        {
                            "cluster_id": cluster.cluster_id,
                            "replica_quota": cluster.replica_quota,
                        }
                        for cluster in base_view.clusters.values()
                    ],
                    "applications": [app.to_dict() for app in new_configs.values()],
                }
                if increment.get("checksum"):
                    snapshot_payload["checksum"] = increment["checksum"]
                new_spec = validation.parse_snapshot_document(
                    snapshot_payload, base_version=base_version
                )

                graph = DependencyGraph(
                    new_spec.applications, (cluster.cluster_id for cluster in new_spec.clusters)
                )
                changed = sorted(changed_ids)
                reevaluation: set[str] = set()
                for app_id in changed:
                    reevaluation.update(graph.downstream_reachable([app_id]))
                reevaluation.difference_update(changed)

                rollback_triggers = sorted(
                    change["app_id"]
                    for change in increment["changes"]
                    if change.get("rollback", False)
                )
                cascade: CascadeResult | None = None
                cascade_id: str | None = None
                if rollback_triggers:
                    cascade = graph.rollback_cascade(rollback_triggers)
                    cascade_digest = hashlib.sha256(
                        json_dumps(
                            {
                                "version": new_spec.version,
                                "triggers": rollback_triggers,
                            }
                        ).encode("utf-8")
                    ).hexdigest()[:12]
                    cascade_id = f"cascade-v{new_spec.version}-{cascade_digest}"

                changed_by_id = {change["app_id"]: change for change in increment["changes"]}
                cascade_apps = set(cascade.affected_app_ids) if cascade else set()
                new_states: dict[str, AppRuntime] = {}
                app_by_id = {app.app_id: app for app in new_spec.applications}
                for app_id, app in app_by_id.items():
                    previous = base_view.states[app_id]
                    effective_version = previous.effective_version
                    if "effective_version" in changed_by_id.get(app_id, {}):
                        effective_version = changed_by_id[app_id]["effective_version"]
                    requires_reevaluation = previous.requires_reevaluation
                    target = previous.state
                    new_cascade_id = previous.cascade_id

                    if app_id in cascade_apps:
                        target = AppState.ROLLING_BACK
                        requires_reevaluation = True
                        new_cascade_id = cascade_id
                    elif app_id in changed_by_id:
                        target = initial_state(app.desired_version, effective_version)
                        requires_reevaluation = False
                        new_cascade_id = None
                    elif app_id in reevaluation:
                        requires_reevaluation = True
                        if (
                            previous.state == AppState.CONVERGED
                            and app.desired_version == effective_version
                        ):
                            target = AppState.CONVERGED
                        elif app.desired_version != effective_version:
                            target = AppState.PENDING

                    if target != previous.state:
                        try:
                            ensure_transition(previous.state, target)
                        except BusinessError as exc:
                            exc.context.update({"app_id": app_id})
                            raise
                    new_states[app_id] = AppRuntime(
                        app_id=app_id,
                        state=target,
                        effective_version=effective_version,
                        requires_reevaluation=requires_reevaluation,
                        cascade_id=new_cascade_id,
                    )

                affected = tuple(
                    sorted(set(changed) | reevaluation | cascade_apps)
                )
                affected_clusters = tuple(graph.clusters_for(affected))
                self._insert_snapshot_data(
                    connection,
                    new_spec,
                    new_states,
                    affected,
                    affected_clusters,
                    reevaluation_app_ids=tuple(sorted(reevaluation)),
                )
                if cascade is not None and cascade_id is not None:
                    connection.execute(
                        "INSERT INTO rollback_cascades("
                        "cascade_id, snapshot_version, trigger_app_ids_json, "
                        "ordered_app_ids_json, affected_cluster_ids_json, status, created_at"
                        ") VALUES(?, ?, ?, ?, ?, 'APPLIED', ?)",
                        (
                            cascade_id,
                            new_spec.version,
                            json_dumps(list(cascade.trigger_app_ids)),
                            json_dumps(list(cascade.ordered_app_ids)),
                            json_dumps(list(cascade.affected_cluster_ids)),
                            utc_now(),
                        ),
                    )
                self._complete_intent(
                    connection,
                    intent_id,
                    {"affected_app_count": len(affected), "cascade_id": cascade_id},
                )
        except BaseException as exc:
            self._abort_intent(intent_id, exc)
            raise

        return {
            "version": new_spec.version,
            "base_version": base_version,
            "generated_at": new_spec.generated_at,
            "checksum": new_spec.checksum,
            "affected_app_count": len(affected),
            "affected_app_ids": list(affected),
            "affected_cluster_ids": list(affected_clusters),
            "reevaluation_app_ids": sorted(reevaluation),
            "rollback_cascade": (
                self._cascade_to_dict(cascade, cascade_id) if cascade and cascade_id else None
            ),
            "state_counts": self._state_counts(new_states.values()),
        }

    def _insert_snapshot_data(
        self,
        connection: sqlite3.Connection,
        spec: SnapshotSpec,
        states: Mapping[str, AppRuntime],
        affected_app_ids: Iterable[str],
        affected_cluster_ids: Iterable[str],
        *,
        reevaluation_app_ids: Iterable[str],
    ) -> None:
        now = utc_now()
        connection.execute(
            "INSERT INTO snapshot_headers("
            "version, base_version, generated_at, checksum, affected_app_ids_json, "
            "affected_cluster_ids_json, reevaluation_app_ids_json, status, created_at"
            ") VALUES(?, ?, ?, ?, ?, ?, ?, 'COMMITTED', ?)",
            (
                spec.version,
                spec.base_version,
                spec.generated_at,
                spec.checksum,
                json_dumps(sorted(affected_app_ids)),
                json_dumps(sorted(affected_cluster_ids)),
                json_dumps(sorted(reevaluation_app_ids)),
                now,
            ),
        )
        connection.executemany(
            "INSERT INTO clusters(version, cluster_id, replica_quota) VALUES(?, ?, ?)",
            [
                (spec.version, cluster.cluster_id, cluster.replica_quota)
                for cluster in spec.clusters
            ],
        )
        connection.executemany(
            "INSERT INTO app_configs("
            "version, app_id, cluster_id, desired_version, submitted_effective_version, "
            "replica_budget, dependencies_json"
            ") VALUES(?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    spec.version,
                    app.app_id,
                    app.cluster_id,
                    app.desired_version,
                    app.effective_version,
                    app.replica_budget,
                    json_dumps(list(app.dependencies)),
                )
                for app in spec.applications
            ],
        )
        connection.executemany(
            "INSERT INTO app_states("
            "version, app_id, state, effective_version, requires_reevaluation, "
            "cascade_id, last_transition_at"
            ") VALUES(?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    spec.version,
                    app.app_id,
                    runtime.state.value,
                    runtime.effective_version,
                    1 if runtime.requires_reevaluation else 0,
                    runtime.cascade_id,
                    now,
                )
                for app in spec.applications
                for runtime in (states[app.app_id],)
            ],
        )
        self.db.set_meta(connection, "current_version", str(spec.version))

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------
    def current_version(self) -> int | None:
        return self._current_version_on(None)

    def _current_version_on(self, connection: sqlite3.Connection | None) -> int | None:
        if connection is None:
            with self.db.transaction(immediate=False) as local:
                return self._current_version_on(local)
        row = connection.execute(
            "SELECT value FROM meta WHERE key = 'current_version'"
        ).fetchone()
        return None if row is None else int(row["value"])

    def _load_snapshot_view_on(
        self, connection: sqlite3.Connection, version: int
    ) -> SnapshotView:
        header = connection.execute(
            "SELECT * FROM snapshot_headers WHERE version = ? AND status = 'COMMITTED'",
            (version,),
        ).fetchone()
        if header is None:
            raise BusinessError(
                "E_NOT_FOUND", "snapshot version does not exist", {"version": version}
            )
        cluster_rows = connection.execute(
            "SELECT * FROM clusters WHERE version = ? ORDER BY cluster_id", (version,)
        ).fetchall()
        clusters = {
            row["cluster_id"]: Cluster(
                cluster_id=row["cluster_id"], replica_quota=int(row["replica_quota"])
            )
            for row in cluster_rows
        }
        config_rows = connection.execute(
            "SELECT * FROM app_configs WHERE version = ? ORDER BY app_id", (version,)
        ).fetchall()
        applications = {
            row["app_id"]: Application(
                app_id=row["app_id"],
                cluster_id=row["cluster_id"],
                desired_version=row["desired_version"],
                effective_version=row["submitted_effective_version"],
                replica_budget=int(row["replica_budget"]),
                dependencies=tuple(json_loads(row["dependencies_json"], [])),
            )
            for row in config_rows
        }
        state_rows = connection.execute(
            "SELECT * FROM app_states WHERE version = ? ORDER BY app_id", (version,)
        ).fetchall()
        states = {
            row["app_id"]: AppRuntime(
                app_id=row["app_id"],
                state=AppState(row["state"]),
                effective_version=row["effective_version"],
                requires_reevaluation=bool(row["requires_reevaluation"]),
                cascade_id=row["cascade_id"],
            )
            for row in state_rows
        }
        return SnapshotView(
            version=int(header["version"]),
            base_version=(
                None if header["base_version"] is None else int(header["base_version"])
            ),
            generated_at=header["generated_at"],
            checksum=header["checksum"],
            clusters=clusters,
            applications=applications,
            states=states,
            affected_app_ids=tuple(json_loads(header["affected_app_ids_json"], [])),
            affected_cluster_ids=tuple(json_loads(header["affected_cluster_ids_json"], [])),
            reevaluation_app_ids=tuple(
                json_loads(header["reevaluation_app_ids_json"], [])
            ),
            status=header["status"],
        )

    def get_snapshot_summary(self, version: int) -> dict[str, Any]:
        with self.db.transaction(immediate=False) as connection:
            view = self._load_snapshot_view_on(connection, version)
            run = connection.execute(
                "SELECT * FROM convergence_runs WHERE snapshot_version = ?", (version,)
            ).fetchone()
            active_batch_count = connection.execute(
                "SELECT COUNT(*) AS count FROM batches WHERE snapshot_version = ? "
                "AND status = 'ACTIVE'",
                (version,),
            ).fetchone()["count"]
            current = self._current_version_on(connection)
        return {
            "version": view.version,
            "base_version": view.base_version,
            "generated_at": view.generated_at,
            "checksum": view.checksum,
            "status": view.status,
            "is_current": current == view.version,
            "cluster_ids": sorted(view.clusters),
            "application_count": len(view.applications),
            "state_counts": self._state_counts(view.states.values()),
            "affected_app_count": len(view.affected_app_ids),
            "affected_app_ids": list(view.affected_app_ids),
            "affected_cluster_ids": list(view.affected_cluster_ids),
            "reevaluation_app_ids": list(view.reevaluation_app_ids),
            "convergence_run": (
                None
                if run is None
                else {
                    "status": run["status"],
                    "attempt": int(run["attempt"]),
                    "execution_count": int(run["execution_count"]),
                    "started_at": run["started_at"],
                    "completed_at": run["completed_at"],
                }
            ),
            "active_batch_count": int(active_batch_count),
        }

    def list_apps(
        self,
        version: int,
        *,
        state: str | None = None,
        cluster_id: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> dict[str, Any]:
        if state is not None:
            try:
                AppState(state)
            except ValueError as exc:
                raise spec_invalid("unknown application state", state=state) from exc
        effective_limit = max(1, min(limit or self.config.default_query_limit, 20000))
        if offset < 0:
            raise spec_invalid("offset must be non-negative")
        where = ["c.version = ?"]
        params: list[Any] = [version]
        if state is not None:
            where.append("s.state = ?")
            params.append(state)
        if cluster_id is not None:
            where.append("c.cluster_id = ?")
            params.append(cluster_id)
        where_sql = " AND ".join(where)
        with self.db.transaction(immediate=False) as connection:
            self._load_snapshot_view_on(connection, version)
            total = connection.execute(
                f"SELECT COUNT(*) AS count FROM app_configs c "
                f"JOIN app_states s ON s.version = c.version AND s.app_id = c.app_id "
                f"WHERE {where_sql}",
                tuple(params),
            ).fetchone()["count"]
            rows = connection.execute(
                f"SELECT c.*, s.state, s.effective_version, s.requires_reevaluation, "
                f"s.cascade_id "
                f"FROM app_configs c JOIN app_states s "
                f"ON s.version = c.version AND s.app_id = c.app_id "
                f"WHERE {where_sql} ORDER BY c.app_id LIMIT ? OFFSET ?",
                tuple(params + [effective_limit, offset]),
            ).fetchall()
        return {
            "version": version,
            "total": int(total),
            "limit": effective_limit,
            "offset": offset,
            "applications": [self._app_row_to_dict(row) for row in rows],
        }

    def get_app(self, version: int, app_id: str) -> dict[str, Any]:
        with self.db.transaction(immediate=False) as connection:
            self._load_snapshot_view_on(connection, version)
            row = connection.execute(
                "SELECT c.*, s.state, s.effective_version, s.requires_reevaluation, "
                "s.cascade_id FROM app_configs c JOIN app_states s "
                "ON s.version = c.version AND s.app_id = c.app_id "
                "WHERE c.version = ? AND c.app_id = ?",
                (version, app_id),
            ).fetchone()
        if row is None:
            raise BusinessError(
                "E_NOT_FOUND", "application does not exist in this snapshot",
                {"version": version, "app_id": app_id},
            )
        return self._app_row_to_dict(row)

    def _app_row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "app_id": row["app_id"],
            "cluster_id": row["cluster_id"],
            "desired_version": row["desired_version"],
            "submitted_effective_version": row["submitted_effective_version"],
            "effective_version": row["effective_version"],
            "replica_budget": int(row["replica_budget"]),
            "dependencies": json_loads(row["dependencies_json"], []),
            "state": row["state"],
            "requires_reevaluation": bool(row["requires_reevaluation"]),
            "cascade_id": row["cascade_id"],
        }

    def get_active_plan(self, version: int) -> dict[str, Any]:
        with self.db.transaction(immediate=False) as connection:
            view = self._load_snapshot_view_on(connection, version)
            run = connection.execute(
                "SELECT * FROM convergence_runs WHERE snapshot_version = ?", (version,)
            ).fetchone()
            rows = connection.execute(
                "SELECT * FROM batches WHERE snapshot_version = ? AND status = 'ACTIVE' "
                "ORDER BY batch_index",
                (version,),
            ).fetchall()
            app_states = {
                row["app_id"]: row
                for row in connection.execute(
                    "SELECT app_id, state, effective_version FROM app_states WHERE version = ?",
                    (version,),
                ).fetchall()
            }
        result_json = json_loads(run["result_json"], {}) if run is not None else {}
        return {
            "snapshot_version": version,
            "status": "ACTIVE" if rows else "NOT_GENERATED",
            "recompute": result_json.get("recompute"),
            "batches": [
                {
                    "batch_id": row["batch_id"],
                    "batch_index": int(row["batch_index"]),
                    "app_ids": json_loads(row["app_ids_json"], []),
                    "cluster_replica_usage": json_loads(row["cluster_usage_json"], {}),
                    "total_replicas": int(row["total_replicas"]),
                    "quota_exceeded": bool(row["quota_exceeded"]),
                    "apps": [
                        {
                            "app_id": app_id,
                            "state": app_states[app_id]["state"],
                            "effective_version": app_states[app_id]["effective_version"],
                        }
                        for app_id in json_loads(row["app_ids_json"], [])
                    ],
                }
                for row in rows
            ],
            "run": (
                None
                if run is None
                else {
                    "status": run["status"],
                    "attempt": int(run["attempt"]),
                    "execution_count": int(run["execution_count"]),
                }
            ),
            "snapshot_checksum": view.checksum,
        }

    def get_rollback_cascades(self, version: int) -> dict[str, Any]:
        with self.db.transaction(immediate=False) as connection:
            self._load_snapshot_view_on(connection, version)
            rows = connection.execute(
                "SELECT * FROM rollback_cascades WHERE snapshot_version = ? "
                "AND status = 'APPLIED' ORDER BY created_at, cascade_id",
                (version,),
            ).fetchall()
        return {
            "snapshot_version": version,
            "cascades": [
                {
                    "cascade_id": row["cascade_id"],
                    "trigger_app_ids": json_loads(row["trigger_app_ids_json"], []),
                    "ordered_app_ids": json_loads(row["ordered_app_ids_json"], []),
                    "affected_cluster_ids": json_loads(
                        row["affected_cluster_ids_json"], []
                    ),
                    "status": row["status"],
                    "created_at": row["created_at"],
                }
                for row in rows
            ],
        }

    def compute_cascade(self, version: int, trigger_app_ids: Iterable[str]) -> dict[str, Any]:
        with self.db.transaction(immediate=False) as connection:
            view = self._load_snapshot_view_on(connection, version)
        cascade = view.graph.rollback_cascade(trigger_app_ids)
        return self._cascade_to_dict(cascade, None)

    # ------------------------------------------------------------------
    # Plan preview and convergence
    # ------------------------------------------------------------------
    def preview_plan(self, version: int, options: dict[str, Any] | None = None) -> dict[str, Any]:
        normalized = self._normalize_convergence_options(options or {})
        with self.db.transaction(immediate=False) as connection:
            view = self._load_snapshot_view_on(connection, version)
            cascades = connection.execute(
                "SELECT * FROM rollback_cascades WHERE snapshot_version = ? "
                "AND status = 'APPLIED' ORDER BY created_at",
                (version,),
            ).fetchall()
        plan = self._build_plan(
            view,
            normalized["mode"],
            normalized["switch_threshold"],
            normalized["max_batch_replicas"],
        )
        result = plan.to_dict()
        result["snapshot_version"] = version
        result["reevaluation_app_ids"] = list(view.reevaluation_app_ids)
        result["rollback_cascades"] = [
            {
                "cascade_id": row["cascade_id"],
                "trigger_app_ids": json_loads(row["trigger_app_ids_json"], []),
                "ordered_app_ids": json_loads(row["ordered_app_ids_json"], []),
            }
            for row in cascades
        ]
        result["batches"] = [
            {
                "batch_id": f"preview-v{version}-b{batch.batch_index:04d}",
                **batch.to_dict(),
                "apps": [
                    {
                        "app_id": app_id,
                        "state": view.states[app_id].state.value,
                        "effective_version": view.states[app_id].effective_version,
                        "desired_version": view.applications[app_id].desired_version,
                    }
                    for app_id in batch.app_ids
                ],
            }
            for batch in plan.batches
        ]
        result["plan_hash"] = hashlib.sha256(
            json_dumps(
                {
                    "batches": [
                        {
                            "batch_index": batch.batch_index,
                            "app_ids": list(batch.app_ids),
                        }
                        for batch in plan.batches
                    ],
                    "final_states": {
                        app_id: "CONVERGED" for app_id in plan.actionable_app_ids
                    },
                }
            ).encode("utf-8")
        ).hexdigest()
        return result

    def trigger_convergence(
        self, version: int, options: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        normalized = self._normalize_convergence_options(options or {})
        request_hash = hashlib.sha256(json_dumps(normalized).encode("utf-8")).hexdigest()
        claim = self._claim_convergence_run(version, normalized, request_hash)
        if claim.get("cached") is not None:
            cached = dict(claim["cached"])
            cached["reused"] = True
            return cached
        if claim.get("stored_error") is not None:
            error = claim["stored_error"]
            raise BusinessError(
                error["code"],
                error["message"],
                {**error.get("context", {}), "reused": True},
            )

        intent_id = claim["intent_id"]
        attempt = claim["attempt"]
        self._halt("convergence_intent")
        try:
            with self.db.transaction(immediate=False) as connection:
                view = self._load_snapshot_view_on(connection, version)
            plan = self._build_plan(
                view,
                normalized["mode"],
                normalized["switch_threshold"],
                normalized["max_batch_replicas"],
            )
            self._persist_building_batches(view.version, plan, attempt, intent_id)
            self._halt("convergence_batches")
            result = self._execute_convergence(
                view, plan, intent_id, attempt, normalized, request_hash
            )
        except BaseException as exc:
            self._mark_run_failed(intent_id, version, exc)
            raise
        result["reused"] = False
        return result

    def _claim_convergence_run(
        self, version: int, normalized: dict[str, Any], request_hash: str
    ) -> dict[str, Any]:
        deadline = time.monotonic() + self.config.trigger_wait_seconds
        while True:
            with self.db.transaction() as connection:
                self._load_snapshot_view_on(connection, version)
                row = connection.execute(
                    "SELECT * FROM convergence_runs WHERE snapshot_version = ?", (version,)
                ).fetchone()
                if row is None:
                    intent_id = self._create_intent_on(
                        connection,
                        "CONVERGENCE",
                        version,
                        {"request_hash": request_hash, "options": normalized},
                    )
                    connection.execute(
                        "INSERT INTO convergence_runs("
                        "snapshot_version, request_hash, status, execution_count, attempt, "
                        "intent_id, started_at) VALUES(?, ?, 'RUNNING', 1, 1, ?, ?)",
                        (version, request_hash, intent_id, utc_now()),
                    )
                    return {"intent_id": intent_id, "attempt": 1}
                if str(row["request_hash"]) != request_hash:
                    raise BusinessError(
                        "E_CONFLICTING_TRIGGER",
                        "the snapshot version already has a different convergence trigger",
                        {
                            "snapshot_version": version,
                            "existing_request_hash": row["request_hash"],
                            "request_hash": request_hash,
                        },
                    )
                status = str(row["status"])
                if status == "COMPLETED":
                    return {"cached": json_loads(row["result_json"], {})}
                if status == "FAILED":
                    return {"stored_error": json_loads(row["error_json"], {})}
                if status == "RUNNING":
                    pass
                elif status == "ABORTED":
                    attempt = int(row["attempt"]) + 1
                    intent_id = self._create_intent_on(
                        connection,
                        "CONVERGENCE",
                        version,
                        {
                            "request_hash": request_hash,
                            "options": normalized,
                            "retry_after_recovery": True,
                        },
                    )
                    connection.execute(
                        "UPDATE convergence_runs SET status = 'RUNNING', attempt = ?, "
                        "execution_count = execution_count + 1, intent_id = ?, "
                        "started_at = ?, completed_at = NULL, result_json = NULL, "
                        "error_json = NULL WHERE snapshot_version = ?",
                        (attempt, intent_id, utc_now(), version),
                    )
                    return {"intent_id": intent_id, "attempt": attempt}
                else:  # pragma: no cover - schema guard
                    raise BusinessError(
                        "E_INTERNAL", "unknown convergence run status", {"status": status}
                    )
            if time.monotonic() >= deadline:
                raise BusinessError(
                    "E_CONFLICTING_TRIGGER",
                    "convergence trigger is still running in another writer",
                    {"snapshot_version": version, "request_hash": request_hash},
                )
            time.sleep(0.02)

    def _execute_convergence(
        self,
        view: SnapshotView,
        plan: BatchPlan,
        intent_id: str,
        attempt: int,
        normalized: dict[str, Any],
        request_hash: str,
    ) -> dict[str, Any]:
        now = utc_now()
        batch_results: list[dict[str, Any]] = []
        with self.db.transaction() as connection:
            run = connection.execute(
                "SELECT status, intent_id FROM convergence_runs WHERE snapshot_version = ?",
                (view.version,),
            ).fetchone()
            if (
                run is None
                or run["status"] != "RUNNING"
                or run["intent_id"] != intent_id
            ):
                raise BusinessError(
                    "E_CONFLICTING_TRIGGER",
                    "convergence claim is no longer active",
                    {"snapshot_version": view.version, "intent_id": intent_id},
                )
            stored_batches = connection.execute(
                "SELECT batch_id, batch_index, app_ids_json FROM batches "
                "WHERE snapshot_version = ? AND intent_id = ? AND status = 'BUILDING' "
                "ORDER BY batch_index",
                (view.version, intent_id),
            ).fetchall()
            expected_batches = [
                (batch.batch_index, list(batch.app_ids)) for batch in plan.batches
            ]
            stored_signature = [
                (int(row["batch_index"]), json_loads(row["app_ids_json"], []))
                for row in stored_batches
            ]
            if stored_signature != expected_batches:
                raise BusinessError(
                    "E_CONFLICTING_TRIGGER",
                    "persisted BUILDING batches do not match the convergence plan",
                    {
                        "snapshot_version": view.version,
                        "intent_id": intent_id,
                        "expected_batches": expected_batches,
                        "stored_batches": stored_signature,
                    },
                )

            # Publish state transitions and the active plan in this same
            # transaction. Readers therefore see neither a half-converged app
            # set nor a plan that references a partially updated state.
            for app_id in plan.actionable_app_ids:
                previous = view.states[app_id].state
                ensure_transition(previous, AppState.CONVERGED)
                connection.execute(
                    "UPDATE app_states SET state = 'CONVERGED', effective_version = ?, "
                    "requires_reevaluation = 0, last_transition_at = ? "
                    "WHERE version = ? AND app_id = ?",
                    (
                        view.applications[app_id].desired_version,
                        now,
                        view.version,
                        app_id,
                    ),
                )
            connection.execute(
                "UPDATE app_states SET requires_reevaluation = 0 WHERE version = ?",
                (view.version,),
            )
            connection.execute(
                "UPDATE batches SET status = 'SUPERSEDED' WHERE snapshot_version = ? "
                "AND status = 'ACTIVE' AND intent_id != ?",
                (view.version, intent_id),
            )
            connection.execute(
                "UPDATE batches SET status = 'ACTIVE' WHERE snapshot_version = ? "
                "AND intent_id = ? AND status = 'BUILDING'",
                (view.version, intent_id),
            )
            for batch in plan.batches:
                batch_id = f"v{view.version}-g{attempt}-b{batch.batch_index:04d}"
                batch_results.append(
                    {
                        "batch_id": batch_id,
                        "batch_index": batch.batch_index,
                        "app_ids": list(batch.app_ids),
                        "cluster_replica_usage": dict(batch.cluster_usage),
                        "total_replicas": batch.total_replicas,
                        "quota_exceeded": batch.quota_exceeded,
                        "apps": [
                            {
                                "app_id": app_id,
                                "state": "CONVERGED",
                                "desired_version": view.applications[app_id].desired_version,
                                "effective_version": view.applications[app_id].desired_version,
                            }
                            for app_id in batch.app_ids
                        ],
                    }
                )

            final_states = dict(self._state_counts(view.states.values()))
            for app_id in plan.actionable_app_ids:
                old_state = view.states[app_id].state.value
                final_states[old_state] = final_states.get(old_state, 0) - 1
                final_states["CONVERGED"] = final_states.get("CONVERGED", 0) + 1
            final_states = {key: value for key, value in final_states.items() if value}
            cascade_rows = connection.execute(
                "SELECT * FROM rollback_cascades WHERE snapshot_version = ? "
                "AND status = 'APPLIED' ORDER BY created_at",
                (view.version,),
            ).fetchall()
            result = {
                "snapshot_version": view.version,
                "request_hash": request_hash,
                "recompute": plan.to_dict()["recompute"],
                "actionable_app_count": len(plan.actionable_app_ids),
                "reevaluation_app_ids": list(view.reevaluation_app_ids),
                "rollback_cascades": [
                    {
                        "cascade_id": row["cascade_id"],
                        "trigger_app_ids": json_loads(row["trigger_app_ids_json"], []),
                        "ordered_app_ids": json_loads(row["ordered_app_ids_json"], []),
                        "affected_cluster_ids": json_loads(
                            row["affected_cluster_ids_json"], []
                        ),
                    }
                    for row in cascade_rows
                ],
                "batches": batch_results,
                "final_state_counts": final_states,
                "execution_count": int(
                    connection.execute(
                        "SELECT execution_count FROM convergence_runs "
                        "WHERE snapshot_version = ?",
                        (view.version,),
                    ).fetchone()["execution_count"]
                ),
                "completed_at": now,
            }
            connection.execute(
                "UPDATE convergence_runs SET status = 'COMPLETED', result_json = ?, "
                "completed_at = ? WHERE snapshot_version = ?",
                (json_dumps(result), now, view.version),
            )
            self._complete_intent(
                connection,
                intent_id,
                {
                    "path": plan.path,
                    "batch_count": len(plan.batches),
                    "options": normalized,
                },
            )
        return result

    def _persist_building_batches(
        self, version: int, plan: BatchPlan, attempt: int, intent_id: str
    ) -> None:
        """Persist an unobservable draft plan before any runtime state changes."""

        now = utc_now()
        with self.db.transaction() as connection:
            run = connection.execute(
                "SELECT status, intent_id FROM convergence_runs WHERE snapshot_version = ?",
                (version,),
            ).fetchone()
            if run is None or run["status"] != "RUNNING" or run["intent_id"] != intent_id:
                raise BusinessError(
                    "E_CONFLICTING_TRIGGER",
                    "convergence claim is no longer active",
                    {"snapshot_version": version, "intent_id": intent_id},
                )
            connection.execute(
                "UPDATE batches SET status = 'ABORTED' WHERE snapshot_version = ? "
                "AND status = 'BUILDING'",
                (version,),
            )
            for batch in plan.batches:
                batch_id = f"v{version}-g{attempt}-b{batch.batch_index:04d}"
                connection.execute(
                    "INSERT INTO batches("
                    "batch_id, snapshot_version, run_attempt, batch_index, app_ids_json, "
                    "cluster_usage_json, total_replicas, quota_exceeded, status, intent_id, "
                    "created_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'BUILDING', ?, ?)",
                    (
                        batch_id,
                        version,
                        attempt,
                        batch.batch_index,
                        json_dumps(list(batch.app_ids)),
                        json_dumps(batch.cluster_usage),
                        batch.total_replicas,
                        1 if batch.quota_exceeded else 0,
                        intent_id,
                        now,
                    ),
                )

    def _build_plan(
        self,
        view: SnapshotView,
        mode: str,
        switch_threshold: int,
        max_batch_replicas: int,
    ) -> BatchPlan:
        return build_batch_plan(
            view.graph,
            {app_id: state.state for app_id, state in view.states.items()},
            view.affected_app_ids,
            {cluster_id: cluster.replica_quota for cluster_id, cluster in view.clusters.items()},
            requested_mode=mode,
            switch_threshold=switch_threshold,
            max_batch_replicas=max_batch_replicas,
        )

    def _normalize_convergence_options(self, options: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(options, dict):
            raise spec_invalid("convergence options must be a JSON object")
        unknown = set(options) - {"mode", "switch_threshold", "max_batch_replicas"}
        if unknown:
            raise spec_invalid(
                "convergence options contain unsupported fields",
                unknown_fields=sorted(unknown),
            )
        mode = options.get("mode", "auto")
        if not isinstance(mode, str):
            raise spec_invalid("mode must be a string")
        mode = mode.lower()
        if mode not in {"auto", "full", "exact", "exact_full", "incremental", "local"}:
            raise spec_invalid(
                "mode must be auto, full, or incremental", mode=options.get("mode")
            )
        threshold = options.get("switch_threshold", self.config.switch_threshold)
        max_batch_replicas = options.get(
            "max_batch_replicas", self.config.max_batch_replicas
        )
        for value, name in (
            (threshold, "switch_threshold"),
            (max_batch_replicas, "max_batch_replicas"),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise spec_invalid(f"{name} must be a positive integer", field=name)
        return {
            "mode": mode,
            "switch_threshold": threshold,
            "max_batch_replicas": max_batch_replicas,
        }

    # ------------------------------------------------------------------
    # State transitions and recovery
    # ------------------------------------------------------------------
    def transition_app(
        self, version: int, app_id: str, target_state: str, reason: str | None = None
    ) -> dict[str, Any]:
        if not isinstance(target_state, str):
            raise spec_invalid("target_state must be a string")
        try:
            target = AppState(target_state)
        except (TypeError, ValueError) as exc:
            raise spec_invalid("unknown application state", state=target_state) from exc
        intent_id = self._create_intent(
            "STATE_TRANSITION",
            version,
            {"app_id": app_id, "target_state": target.value, "reason": reason},
        )
        try:
            with self.db.transaction() as connection:
                current = self._current_version_on(connection)
                if current != version:
                    raise BusinessError(
                        "E_INVALID_TRANSITION",
                        "state transitions are accepted only for the current snapshot",
                        {"version": version, "current_version": current},
                    )
                view = self._load_snapshot_view_on(connection, version)
                if app_id not in view.states:
                    raise BusinessError(
                        "E_NOT_FOUND",
                        "application does not exist in this snapshot",
                        {"version": version, "app_id": app_id},
                    )
                previous = view.states[app_id].state
                ensure_transition(previous, target)
                effective_version = view.states[app_id].effective_version
                if target == AppState.CONVERGED:
                    effective_version = view.applications[app_id].desired_version
                connection.execute(
                    "UPDATE app_states SET state = ?, effective_version = ?, "
                    "last_transition_at = ? WHERE version = ? AND app_id = ?",
                    (target.value, effective_version, utc_now(), version, app_id),
                )
                # A manually changed state invalidates the published plan rather
                # than leaving a plan that still includes newly converged apps.
                connection.execute(
                    "UPDATE batches SET status = 'SUPERSEDED' WHERE snapshot_version = ? "
                    "AND status = 'ACTIVE'",
                    (version,),
                )
                self._complete_intent(connection, intent_id, {"reason": reason})
        except BaseException as exc:
            self._abort_intent(intent_id, exc)
            raise
        return {
            "snapshot_version": version,
            "app_id": app_id,
            "from": previous.value,
            "to": target.value,
            "effective_version": effective_version,
        }

    def recover(self) -> dict[str, Any]:
        """Abort unfinished facts and report only what is durably present."""

        aborted_intents: list[dict[str, Any]] = []
        aborted_runs: list[int] = []
        aborted_batches: list[str] = []
        now = utc_now()
        with self.db.transaction() as connection:
            intents = connection.execute(
                "SELECT * FROM intents WHERE status = 'RUNNING' ORDER BY created_at"
            ).fetchall()
            for intent in intents:
                kind = str(intent["kind"])
                version = int(intent["snapshot_version"])
                detail: dict[str, Any] = {
                    "recovered_at": now,
                    "reason": "process terminated before durable commit",
                }
                if kind == "CONVERGENCE":
                    connection.execute(
                        "UPDATE convergence_runs SET status = 'ABORTED', completed_at = ? "
                        "WHERE snapshot_version = ? AND status = 'RUNNING'",
                        (now, version),
                    )
                    if connection.execute(
                        "SELECT changes() AS count"
                    ).fetchone()["count"]:
                        aborted_runs.append(version)
                    batch_rows = connection.execute(
                        "SELECT batch_id FROM batches WHERE snapshot_version = ? "
                        "AND status = 'BUILDING'",
                        (version,),
                    ).fetchall()
                    aborted_batches.extend(row["batch_id"] for row in batch_rows)
                    connection.execute(
                        "UPDATE batches SET status = 'ABORTED' WHERE snapshot_version = ? "
                        "AND status = 'BUILDING'",
                        (version,),
                    )
                elif kind in {"SNAPSHOT_COMMIT", "INCREMENT_COMMIT"}:
                    header = connection.execute(
                        "SELECT status FROM snapshot_headers WHERE version = ?", (version,)
                    ).fetchone()
                    detail["snapshot_present"] = header is not None
                    detail["partial_snapshot_discarded"] = header is None
                connection.execute(
                    "UPDATE intents SET status = 'ABORTED', updated_at = ?, detail_json = ? "
                    "WHERE intent_id = ?",
                    (now, json_dumps(detail), intent["intent_id"]),
                )
                aborted_intents.append(
                    {
                        "intent_id": intent["intent_id"],
                        "kind": kind,
                        "snapshot_version": version,
                        "detail": detail,
                    }
                )

            # Also cover an orphaned RUNNING run or BUILDING batch without a
            # readable intent (for example after a manual database interruption).
            run_rows = connection.execute(
                "SELECT snapshot_version FROM convergence_runs WHERE status = 'RUNNING'"
            ).fetchall()
            for row in run_rows:
                version = int(row["snapshot_version"])
                connection.execute(
                    "UPDATE convergence_runs SET status = 'ABORTED', completed_at = ? "
                    "WHERE snapshot_version = ? AND status = 'RUNNING'",
                    (now, version),
                )
                if version not in aborted_runs:
                    aborted_runs.append(version)
            orphan_batches = connection.execute(
                "SELECT batch_id FROM batches WHERE status = 'BUILDING'"
            ).fetchall()
            connection.execute("UPDATE batches SET status = 'ABORTED' WHERE status = 'BUILDING'")
            aborted_batches.extend(row["batch_id"] for row in orphan_batches)
            current = self._current_version_on(connection)
            state_counts: dict[str, int] = {}
            if current is not None:
                rows = connection.execute(
                    "SELECT state, COUNT(*) AS count FROM app_states WHERE version = ? "
                    "GROUP BY state ORDER BY state",
                    (current,),
                ).fetchall()
                state_counts = {row["state"]: int(row["count"]) for row in rows}
        return {
            "recovered_at": now,
            "last_complete_snapshot_version": current,
            "aborted_intents": aborted_intents,
            "aborted_convergence_versions": sorted(set(aborted_runs)),
            "aborted_batch_ids": sorted(set(aborted_batches)),
            "state_diff": {
                "changed_app_ids": [],
                "note": (
                    "no partial application state was visible; SQLite transactions kept the "
                    "last complete snapshot intact"
                ),
            },
            "persisted_state_counts": state_counts,
        }

    # ------------------------------------------------------------------
    # Intent and run helpers
    # ------------------------------------------------------------------
    def _new_intent_id(self) -> str:
        return f"intent-{uuid.uuid4().hex}"

    def _create_intent(
        self, kind: str, snapshot_version: int, payload: dict[str, Any]
    ) -> str:
        with self.db.transaction() as connection:
            return self._create_intent_on(connection, kind, snapshot_version, payload)

    def _create_intent_on(
        self,
        connection: sqlite3.Connection,
        kind: str,
        snapshot_version: int,
        payload: dict[str, Any],
    ) -> str:
        intent_id = self._new_intent_id()
        now = utc_now()
        connection.execute(
            "INSERT INTO intents("
            "intent_id, kind, snapshot_version, status, payload_json, created_at, updated_at"
            ") VALUES(?, ?, ?, 'RUNNING', ?, ?, ?)",
            (intent_id, kind, snapshot_version, json_dumps(payload), now, now),
        )
        return intent_id

    def _complete_intent(
        self,
        connection: sqlite3.Connection,
        intent_id: str,
        detail: dict[str, Any] | None = None,
    ) -> None:
        connection.execute(
            "UPDATE intents SET status = 'COMPLETED', updated_at = ?, detail_json = ? "
            "WHERE intent_id = ?",
            (utc_now(), json_dumps(detail or {}), intent_id),
        )

    def _abort_intent(self, intent_id: str, exc: BaseException) -> None:
        try:
            with self.db.transaction() as connection:
                detail = {
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
                if isinstance(exc, BusinessError):
                    detail["code"] = exc.code
                    detail["context"] = exc.context
                connection.execute(
                    "UPDATE intents SET status = 'ABORTED', updated_at = ?, "
                    "detail_json = ? WHERE intent_id = ? AND status = 'RUNNING'",
                    (utc_now(), json_dumps(detail), intent_id),
                )
        except sqlite3.Error:
            # Cancellation/termination can close the process while the best
            # effort audit update is running. Startup recovery scans RUNNING.
            pass

    def _mark_run_failed(
        self, intent_id: str, version: int, exc: BaseException
    ) -> None:
        now = utc_now()
        if isinstance(exc, BusinessError):
            error = exc.to_dict()
        else:
            error = {
                "code": "E_INTERNAL",
                "message": str(exc) or type(exc).__name__,
                "context": {},
            }
        try:
            with self.db.transaction() as connection:
                connection.execute(
                    "UPDATE convergence_runs SET status = 'FAILED', error_json = ?, "
                    "completed_at = ? WHERE snapshot_version = ? AND status = 'RUNNING'",
                    (json_dumps(error), now, version),
                )
                connection.execute(
                    "UPDATE batches SET status = 'ABORTED' WHERE snapshot_version = ? "
                    "AND status = 'BUILDING'",
                    (version,),
                )
                connection.execute(
                    "UPDATE intents SET status = 'ABORTED', updated_at = ?, "
                    "detail_json = ? WHERE intent_id = ? AND status = 'RUNNING'",
                    (now, json_dumps({"error": error}), intent_id),
                )
        except sqlite3.Error:
            pass

    def _halt(self, point: str) -> None:
        if self.config.crash_after == point:
            os._exit(137)

    def _state_counts(self, states: Iterable[AppRuntime]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for state in states:
            counts[state.state.value] = counts.get(state.state.value, 0) + 1
        return dict(sorted(counts.items()))

    def _cascade_to_dict(
        self, cascade: CascadeResult, cascade_id: str | None
    ) -> dict[str, Any]:
        return {
            "cascade_id": cascade_id,
            "trigger_app_ids": list(cascade.trigger_app_ids),
            "ordered_app_ids": list(cascade.ordered_app_ids),
            "affected_app_count": len(cascade.affected_app_ids),
            "affected_app_ids": list(cascade.affected_app_ids),
            "affected_cluster_ids": list(cascade.affected_cluster_ids),
        }

    def state_machine(self) -> dict[str, Any]:
        return {
            "states": [state.value for state in AppState],
            "transitions": transitions_as_dict(),
        }
