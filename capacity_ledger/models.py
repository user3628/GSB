"""Validated snapshot and increment models.

Validation is deliberately explicit instead of relying on Pydantic's default
error envelope.  Every failure is translated to E_SNAPSHOT_INVALID.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .checksum import compute_payload_checksum, checksum_matches
from .errors import snapshot_invalid


SNAPSHOT_ROOT_KEYS = {
    "kind",
    "commit_id",
    "version",
    "generated_at",
    "total_budget_cents_per_hour",
    "tolerance_ppm",
    "services",
    "checksum",
}
INCREMENT_ROOT_KEYS = {
    "kind",
    "commit_id",
    "base_version",
    "version",
    "generated_at",
    "services",
    "checksum",
}
SERVICE_KEYS = {
    "replicas",
    "cores_per_replica",
    "memory_gb_per_replica",
    "min_cores",
    "max_cores",
    "min_memory_gb",
    "max_memory_gb",
    "price_core_cents_per_core_hour",
    "price_memory_cents_per_gb_hour",
    "budget_cents_per_hour",
    "max_cost_share_ppm",
    "slo_target_ppm",
}
RESOURCE_FIELDS = {
    "replicas",
    "cores_per_replica",
    "memory_gb_per_replica",
    "min_cores",
    "max_cores",
    "min_memory_gb",
    "max_memory_gb",
}
PRICE_FIELDS = {
    "price_core_cents_per_core_hour",
    "price_memory_cents_per_gb_hour",
}
BUDGET_FIELDS = {"budget_cents_per_hour", "max_cost_share_ppm"}
SLO_FIELDS = {"slo_target_ppm"}


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _require_int(
    data: dict[str, Any],
    key: str,
    path: str,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    value = data.get(key)
    if not _is_int(value):
        raise snapshot_invalid(f"{path}.{key}", "must be an integer")
    if minimum is not None and value < minimum:
        raise snapshot_invalid(f"{path}.{key}", f"must be >= {minimum}")
    if maximum is not None and value > maximum:
        raise snapshot_invalid(f"{path}.{key}", f"must be <= {maximum}")
    return value


def _require_text(data: dict[str, Any], key: str, path: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise snapshot_invalid(f"{path}.{key}", "must be a non-empty string")
    return value


def _check_root_keys(data: dict[str, Any], allowed: set[str], path: str) -> None:
    if not isinstance(data, dict):
        raise snapshot_invalid(path, "must be a JSON object")
    unknown = sorted(set(data) - allowed)
    if unknown:
        raise snapshot_invalid(path, "contains unknown fields", fields=unknown)
    missing = sorted(allowed - set(data))
    if missing:
        raise snapshot_invalid(path, "is missing required fields", fields=missing)


def _validate_timestamp(value: str, path: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise snapshot_invalid(path, "must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise snapshot_invalid(path, "must include a timezone")


def _validate_checksum(data: dict[str, Any], path: str) -> str:
    try:
        matches = checksum_matches(data)
        expected = compute_payload_checksum(data)
    except (TypeError, ValueError) as exc:
        raise snapshot_invalid(
            f"{path}.checksum", "payload is not canonicalizable JSON"
        ) from exc
    if not matches:
        raise snapshot_invalid(
            f"{path}.checksum",
            "checksum mismatch",
            supplied=data.get("checksum"),
            expected=expected,
        )
    return str(data["checksum"])


@dataclass(frozen=True)
class ServiceCapacity:
    name: str
    replicas: int
    cores_per_replica: int
    memory_gb_per_replica: int
    min_cores: int
    max_cores: int
    min_memory_gb: int
    max_memory_gb: int
    price_core_cents_per_core_hour: int
    price_memory_cents_per_gb_hour: int
    budget_cents_per_hour: int
    max_cost_share_ppm: int
    slo_target_ppm: int

    @classmethod
    def from_dict(cls, name: str, data: Any, path: str) -> "ServiceCapacity":
        if not isinstance(name, str) or not name or len(name) > 128:
            raise snapshot_invalid(f"{path}.<name>", "service name must be 1..128 characters")
        if not isinstance(data, dict):
            raise snapshot_invalid(path, "service record must be a JSON object")
        unknown = sorted(set(data) - SERVICE_KEYS)
        if unknown:
            raise snapshot_invalid(path, "contains unknown fields", fields=unknown)
        missing = sorted(SERVICE_KEYS - set(data))
        if missing:
            raise snapshot_invalid(path, "is missing required fields", fields=missing)

        min_cores = _require_int(data, "min_cores", path, minimum=1, maximum=1_000_000)
        max_cores = _require_int(data, "max_cores", path, minimum=1, maximum=1_000_000)
        min_memory = _require_int(data, "min_memory_gb", path, minimum=1, maximum=1_000_000)
        max_memory = _require_int(data, "max_memory_gb", path, minimum=1, maximum=1_000_000)
        if min_cores > max_cores:
            raise snapshot_invalid(f"{path}.min_cores", "must be <= max_cores")
        if min_memory > max_memory:
            raise snapshot_invalid(f"{path}.min_memory_gb", "must be <= max_memory_gb")

        cores = _require_int(data, "cores_per_replica", path, minimum=1, maximum=1_000_000)
        memory = _require_int(
            data, "memory_gb_per_replica", path, minimum=1, maximum=1_000_000
        )
        if not min_cores <= cores <= max_cores:
            raise snapshot_invalid(f"{path}.cores_per_replica", "outside declared min/max cores")
        if not min_memory <= memory <= max_memory:
            raise snapshot_invalid(
                f"{path}.memory_gb_per_replica", "outside declared min/max memory"
            )

        return cls(
            name=name,
            replicas=_require_int(data, "replicas", path, minimum=1, maximum=1_000_000),
            cores_per_replica=cores,
            memory_gb_per_replica=memory,
            min_cores=min_cores,
            max_cores=max_cores,
            min_memory_gb=min_memory,
            max_memory_gb=max_memory,
            price_core_cents_per_core_hour=_require_int(
                data, "price_core_cents_per_core_hour", path, minimum=0
            ),
            price_memory_cents_per_gb_hour=_require_int(
                data, "price_memory_cents_per_gb_hour", path, minimum=0
            ),
            budget_cents_per_hour=_require_int(
                data, "budget_cents_per_hour", path, minimum=0
            ),
            max_cost_share_ppm=_require_int(
                data, "max_cost_share_ppm", path, minimum=0, maximum=1_000_000
            ),
            slo_target_ppm=_require_int(
                data, "slo_target_ppm", path, minimum=1, maximum=1_000_000
            ),
        )

    def to_dict(self) -> dict[str, int]:
        return {
            "replicas": self.replicas,
            "cores_per_replica": self.cores_per_replica,
            "memory_gb_per_replica": self.memory_gb_per_replica,
            "min_cores": self.min_cores,
            "max_cores": self.max_cores,
            "min_memory_gb": self.min_memory_gb,
            "max_memory_gb": self.max_memory_gb,
            "price_core_cents_per_core_hour": self.price_core_cents_per_core_hour,
            "price_memory_cents_per_gb_hour": self.price_memory_cents_per_gb_hour,
            "budget_cents_per_hour": self.budget_cents_per_hour,
            "max_cost_share_ppm": self.max_cost_share_ppm,
            "slo_target_ppm": self.slo_target_ppm,
        }

    def changed_fields(self, other: "ServiceCapacity") -> tuple[str, ...]:
        current = self.to_dict()
        previous = other.to_dict()
        return tuple(sorted(key for key in current if current[key] != previous[key]))


@dataclass(frozen=True)
class CapacitySnapshot:
    commit_id: str
    version: int
    generated_at: str
    total_budget_cents_per_hour: int
    tolerance_ppm: int
    services: dict[str, ServiceCapacity]
    checksum: str
    kind: str = field(default="snapshot", init=False)

    @classmethod
    def from_payload(cls, payload: Any) -> "CapacitySnapshot":
        _check_root_keys(payload, SNAPSHOT_ROOT_KEYS, "$")
        assert isinstance(payload, dict)
        if payload.get("kind") != "snapshot":
            raise snapshot_invalid("$.kind", "must be 'snapshot'")
        _validate_checksum(payload, "$")
        version = _require_int(payload, "version", "$", minimum=1)
        generated_at = _require_text(payload, "generated_at", "$")
        _validate_timestamp(generated_at, "$.generated_at")
        total_budget = _require_int(
            payload, "total_budget_cents_per_hour", "$", minimum=0
        )
        tolerance = _require_int(payload, "tolerance_ppm", "$", minimum=0)
        services_raw = payload.get("services")
        if not isinstance(services_raw, dict) or not services_raw:
            raise snapshot_invalid("$.services", "must be a non-empty object")
        services = {
            name: ServiceCapacity.from_dict(name, raw, f"$.services.{name}")
            for name, raw in services_raw.items()
        }
        return cls(
            commit_id=_require_text(payload, "commit_id", "$"),
            version=version,
            generated_at=generated_at,
            total_budget_cents_per_hour=total_budget,
            tolerance_ppm=tolerance,
            services=services,
            checksum=str(payload["checksum"]),
        )

    def to_payload(self, *, include_checksum: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "kind": "snapshot",
            "commit_id": self.commit_id,
            "version": self.version,
            "generated_at": self.generated_at,
            "total_budget_cents_per_hour": self.total_budget_cents_per_hour,
            "tolerance_ppm": self.tolerance_ppm,
            "services": {name: svc.to_dict() for name, svc in sorted(self.services.items())},
        }
        if include_checksum:
            payload["checksum"] = self.checksum
        return payload

    def unsigned_payload(self) -> dict[str, Any]:
        return self.to_payload(include_checksum=False)

    def with_services(
        self,
        services: dict[str, ServiceCapacity],
        *,
        version: int,
        generated_at: str,
        commit_id: str,
    ) -> "CapacitySnapshot":
        payload = {
            "kind": "snapshot",
            "commit_id": commit_id,
            "version": version,
            "generated_at": generated_at,
            "total_budget_cents_per_hour": self.total_budget_cents_per_hour,
            "tolerance_ppm": self.tolerance_ppm,
            "services": {name: svc.to_dict() for name, svc in sorted(services.items())},
        }
        payload["checksum"] = compute_payload_checksum(payload)
        return CapacitySnapshot.from_payload(payload)


@dataclass(frozen=True)
class CapacityIncrement:
    commit_id: str
    base_version: int
    version: int
    generated_at: str
    services: dict[str, ServiceCapacity]
    checksum: str
    kind: str = field(default="increment", init=False)

    @classmethod
    def from_payload(cls, payload: Any) -> "CapacityIncrement":
        _check_root_keys(payload, INCREMENT_ROOT_KEYS, "$")
        assert isinstance(payload, dict)
        if payload.get("kind") != "increment":
            raise snapshot_invalid("$.kind", "must be 'increment'")
        _validate_checksum(payload, "$")
        base_version = _require_int(payload, "base_version", "$", minimum=1)
        version = _require_int(payload, "version", "$", minimum=2)
        if version != base_version + 1:
            raise snapshot_invalid("$.version", "must equal base_version + 1")
        generated_at = _require_text(payload, "generated_at", "$")
        _validate_timestamp(generated_at, "$.generated_at")
        services_raw = payload.get("services")
        if not isinstance(services_raw, dict) or not services_raw:
            raise snapshot_invalid("$.services", "must contain only affected services")
        services = {
            name: ServiceCapacity.from_dict(name, raw, f"$.services.{name}")
            for name, raw in services_raw.items()
        }
        return cls(
            commit_id=_require_text(payload, "commit_id", "$"),
            base_version=base_version,
            version=version,
            generated_at=generated_at,
            services=services,
            checksum=str(payload["checksum"]),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "kind": "increment",
            "commit_id": self.commit_id,
            "base_version": self.base_version,
            "version": self.version,
            "generated_at": self.generated_at,
            "services": {name: svc.to_dict() for name, svc in sorted(self.services.items())},
            "checksum": self.checksum,
        }

    def changed_fields(self, current: CapacitySnapshot) -> dict[str, tuple[str, ...]]:
        changes: dict[str, tuple[str, ...]] = {}
        for name, proposed in sorted(self.services.items()):
            previous = current.services.get(name)
            if previous is None:
                raise snapshot_invalid(
                    f"$.services.{name}", "increments may not add services"
                )
            changed = proposed.changed_fields(previous)
            if not changed:
                raise snapshot_invalid(
                    f"$.services.{name}", "increment does not change the service"
                )
            changes[name] = changed
        return changes
