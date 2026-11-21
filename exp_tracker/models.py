"""Core domain models for runs, metric points and artifacts.

Internal canonical conventions used throughout the component:

* metric point time is stored as integer **microseconds** since an epoch;
* parameter values are one of bool / int / float / str / None;
* every run carries a last-write timestamp (integer microseconds) used as
  the deterministic tie-breaker during import conflict resolution.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class MetricPoint:
    """One metric observation.

    ``step`` is the training step (integer), ``value`` the observed scalar
    (mixed precision is preserved) and ``time_us`` the step timestamp in
    microseconds (binary ms / JSON us inputs are normalized to this).
    """

    step: int
    value: float
    time_us: int

    def to_dict(self) -> Dict[str, Any]:
        return {"step": self.step, "value": self.value, "time_us": self.time_us}

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "MetricPoint":
        return cls(
            step=int(raw["step"]),
            value=float(raw["value"]),
            time_us=int(raw["time_us"]),
        )


@dataclass(frozen=True)
class Artifact:
    """Summary of a produced artifact (no bytes are stored)."""

    name: str
    size_bytes: int
    digest: str

    def to_dict(self) -> Dict[str, Any]:
        return {"name": self.name, "size_bytes": self.size_bytes, "digest": self.digest}

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "Artifact":
        return cls(
            name=str(raw["name"]),
            size_bytes=int(raw["size_bytes"]),
            digest=str(raw["digest"]),
        )


@dataclass
class Run:
    """A single training run record."""

    run_id: str
    experiment: str
    params: Dict[str, Any]
    metrics: List[MetricPoint]
    toolchain_version: str
    last_write_us: int
    base_time_us: Optional[int] = None
    retention_seconds: Optional[float] = None
    expires_at_us: Optional[int] = None
    artifacts: List[Artifact] = field(default_factory=list)
    downgrades: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "Run":
        return cls(
            run_id=str(raw["run_id"]),
            experiment=str(raw["experiment"]),
            params=dict(raw.get("params") or {}),
            metrics=[MetricPoint.from_dict(m) for m in raw.get("metrics", [])],
            toolchain_version=str(raw["toolchain_version"]),
            last_write_us=int(raw["last_write_us"]),
            base_time_us=raw.get("base_time_us"),
            retention_seconds=raw.get("retention_seconds"),
            expires_at_us=raw.get("expires_at_us"),
            artifacts=[Artifact.from_dict(a) for a in raw.get("artifacts", [])],
            downgrades=list(raw.get("downgrades", [])),
        )

    def metric_signature(self) -> str:
        """Deterministic summary of the metric sequence for conflict checks."""
        parts = [f"{p.step}:{p.value!r}:{p.time_us}" for p in self.metrics]
        return "|".join(parts)
