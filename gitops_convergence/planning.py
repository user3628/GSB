from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping

from .errors import BusinessError
from .graph import DependencyGraph
from .models import AppState, Application


ACTIONABLE_STATES = frozenset({AppState.PENDING, AppState.ROLLING_BACK, AppState.FAILED})


@dataclass(frozen=True)
class PlannedBatch:
    batch_index: int
    app_ids: tuple[str, ...]
    cluster_usage: dict[str, int]
    total_replicas: int
    quota_exceeded: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "batch_index": self.batch_index,
            "app_ids": list(self.app_ids),
            "cluster_replica_usage": dict(self.cluster_usage),
            "total_replicas": self.total_replicas,
            "quota_exceeded": self.quota_exceeded,
        }


@dataclass(frozen=True)
class BatchPlan:
    path: str
    reason: str
    switch_threshold: int
    total_app_count: int
    affected_app_count: int
    actionable_app_ids: tuple[str, ...]
    batches: tuple[PlannedBatch, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "recompute": {
                "path": self.path,
                "affected_app_count": self.affected_app_count,
                "total_app_count": self.total_app_count,
                "switch_threshold": self.switch_threshold,
                "reason": self.reason,
            },
            "actionable_app_ids": list(self.actionable_app_ids),
            "batches": [batch.to_dict() for batch in self.batches],
        }


def decide_recompute_path(
    total_app_count: int,
    affected_app_count: int,
    switch_threshold: int,
    requested_mode: str,
) -> tuple[str, str]:
    mode = requested_mode.lower()
    if mode in {"full", "exact", "exact_full"}:
        return "EXACT_FULL", "exact full recomputation explicitly requested"
    if mode in {"incremental", "local"}:
        return "INCREMENTAL", "incremental affected-set recomputation explicitly requested"
    if mode != "auto":
        raise BusinessError(
            "E_SPEC_INVALID",
            "recompute mode must be auto, full, or incremental",
            {"mode": requested_mode},
        )
    if total_app_count > switch_threshold:
        if affected_app_count < total_app_count:
            return (
                "INCREMENTAL",
                (
                    f"total applications {total_app_count} exceed threshold "
                    f"{switch_threshold}; only {affected_app_count} are affected"
                ),
            )
        return (
            "EXACT_FULL",
            (
                f"total applications {total_app_count} exceed threshold "
                f"{switch_threshold}, but the affected set covers the whole graph"
            ),
        )
    return (
        "EXACT_FULL",
        f"total applications {total_app_count} are at or below threshold {switch_threshold}",
    )


def _append_batch(
    batches: list[PlannedBatch],
    app_ids: list[str],
    usage: dict[str, int],
    quotas: Mapping[str, int],
    max_batch_replicas: int,
) -> None:
    if not app_ids:
        return
    ordered_apps = sorted(app_ids)
    cluster_usage = {cluster: usage[cluster] for cluster in sorted(usage)}
    exceeded = sum(cluster_usage.values()) > max_batch_replicas or any(
        cluster_usage.get(cluster, 0) > quota for cluster, quota in quotas.items()
    )
    batches.append(
        PlannedBatch(
            batch_index=len(batches) + 1,
            app_ids=tuple(ordered_apps),
            cluster_usage=cluster_usage,
            total_replicas=sum(cluster_usage.values()),
            quota_exceeded=exceeded,
        )
    )


def build_batch_plan(
    graph: DependencyGraph,
    runtime_states: Mapping[str, AppState],
    affected_app_ids: Iterable[str],
    cluster_quotas: Mapping[str, int],
    *,
    requested_mode: str,
    switch_threshold: int,
    max_batch_replicas: int,
) -> BatchPlan:
    affected = set(affected_app_ids)
    total_app_count = len(graph.apps)
    path, reason = decide_recompute_path(
        total_app_count, len(affected), switch_threshold, requested_mode
    )

    # Both paths intentionally apply the same final-state predicate. The full
    # path scans every app; the incremental path starts from the affected
    # closure and also includes any already-persisted actionable state outside
    # it, so a manual FAILED/PENDING state cannot make the conclusions diverge.
    candidates = set(graph.apps) if path == "EXACT_FULL" else set(affected)
    candidates.update(
        app_id for app_id, state in runtime_states.items() if state in ACTIONABLE_STATES
    )
    actionable = sorted(
        app_id
        for app_id in candidates
        if app_id in graph.apps and runtime_states.get(app_id) in ACTIONABLE_STATES
    )

    levels = graph.dependency_levels(actionable)
    batches: list[PlannedBatch] = []
    grouped: dict[int, list[str]] = {}
    for app_id in actionable:
        grouped.setdefault(levels[app_id], []).append(app_id)

    for level in sorted(grouped):
        current_apps: list[str] = []
        current_usage: dict[str, int] = {}
        for app_id in sorted(grouped[level], key=lambda item: (graph.apps[item].cluster_id, item)):
            app = graph.apps[app_id]
            prospective_cluster = current_usage.get(app.cluster_id, 0) + app.replica_budget
            prospective_total = sum(current_usage.values()) + app.replica_budget
            cluster_over = prospective_cluster > cluster_quotas[app.cluster_id]
            batch_over = prospective_total > max_batch_replicas
            if current_apps and (cluster_over or batch_over):
                _append_batch(
                    batches,
                    current_apps,
                    current_usage,
                    cluster_quotas,
                    max_batch_replicas,
                )
                current_apps = []
                current_usage = {}
            current_apps.append(app_id)
            current_usage[app.cluster_id] = (
                current_usage.get(app.cluster_id, 0) + app.replica_budget
            )
            # A single app can be larger than a quota. Keep it rather than
            # dropping the app, and mark the resulting batch explicitly.
            if app.replica_budget > cluster_quotas[app.cluster_id] or (
                app.replica_budget > max_batch_replicas
            ):
                _append_batch(
                    batches,
                    current_apps,
                    current_usage,
                    cluster_quotas,
                    max_batch_replicas,
                )
                current_apps = []
                current_usage = {}
        _append_batch(
            batches, current_apps, current_usage, cluster_quotas, max_batch_replicas
        )

    return BatchPlan(
        path=path,
        reason=reason,
        switch_threshold=switch_threshold,
        total_app_count=total_app_count,
        affected_app_count=len(affected),
        actionable_app_ids=tuple(actionable),
        batches=tuple(batches),
    )
