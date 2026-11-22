from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


MAX_SQLITE_INT = 2**63 - 1


class AppState(str, Enum):
    PENDING = "PENDING"
    ROLLING_BACK = "ROLLING_BACK"
    CONVERGED = "CONVERGED"
    FAILED = "FAILED"
    ABORTED = "ABORTED"


@dataclass(frozen=True)
class Cluster:
    cluster_id: str
    replica_quota: int


@dataclass(frozen=True)
class Application:
    app_id: str
    cluster_id: str
    desired_version: str
    effective_version: str
    replica_budget: int
    dependencies: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "app_id": self.app_id,
            "cluster_id": self.cluster_id,
            "desired_version": self.desired_version,
            "effective_version": self.effective_version,
            "replica_budget": self.replica_budget,
            "dependencies": list(self.dependencies),
        }


@dataclass(frozen=True)
class SnapshotSpec:
    version: int
    generated_at: str
    clusters: tuple[Cluster, ...]
    applications: tuple[Application, ...]
    checksum: str
    base_version: int | None = None


@dataclass(frozen=True)
class AppRuntime:
    """Runtime facts layered over an immutable application configuration."""

    app_id: str
    state: AppState
    effective_version: str
    requires_reevaluation: bool
    cascade_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "app_id": self.app_id,
            "state": self.state.value,
            "effective_version": self.effective_version,
            "requires_reevaluation": self.requires_reevaluation,
            "cascade_id": self.cascade_id,
        }
