from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from typing import Any, Iterable

from app.errors import DomainError, spec_invalid
from app.models import TenantSpec, TenantStatus

UNIT_SCALE = 1_000_000
UNIT = Decimal(UNIT_SCALE)
ROUNDING_NOTE = (
    "All byte, token, request-weight and rate inputs are converted to 1e-6 fixed-point "
    "integer units with ROUND_FLOOR. Elapsed time is integer milliseconds. Token refill uses "
    "floor((refill_remainder + elapsed_ms * rate_units_per_second) / 1000); the integer "
    "remainder is carried to the next refill. A refill capped at burst clears the remainder. "
    "Sub-micro-unit input fractions and sub-micro-token refill fractions are therefore truncated."
)


def to_units(value: Decimal | int | str | float, field: str) -> int:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise spec_invalid(f"{field} must be a finite decimal", field=field, value=str(value))
    if not parsed.is_finite():
        raise spec_invalid(f"{field} must be finite", field=field, value=str(value))
    if parsed < 0:
        raise spec_invalid(f"{field} must be non-negative", field=field, value=str(parsed))
    return int((parsed * UNIT).to_integral_value(rounding=ROUND_FLOOR))




class CompensatedSum:
    """Neumaier/Kahan compensated accumulator over fixed-point integer facts.

    The domain values are already 1e-6 integers, so ordinary integer addition is
    exact.  Decimal compensation is retained anyway so batch and subtree aggregate
    code follows the compensated-summation contract explicitly and remains safe if
    future code feeds large/small adjustments.
    """

    def __init__(self, initial: int = 0) -> None:
        self.total = Decimal(initial)
        self.compensation = Decimal(0)

    def add(self, value: int | Decimal) -> None:
        amount = Decimal(value)
        adjusted = amount - self.compensation
        new_total = self.total + adjusted
        self.compensation = (new_total - self.total) - adjusted
        self.total = new_total

    @property
    def value(self) -> int:
        return int(self.total)


def compensated_sum(values: Iterable[int | Decimal]) -> int:
    accumulator = CompensatedSum()
    for value in values:
        accumulator.add(value)
    return accumulator.value


def units_to_str(units: int) -> str:
    sign = "-" if units < 0 else ""
    absolute = abs(units)
    whole, fraction = divmod(absolute, UNIT_SCALE)
    return f"{sign}{whole}.{fraction:06d}"


@dataclass(frozen=True)
class TenantRecord:
    tenant_id: str
    capacity_units: int
    rps_units: int
    burst_units: int
    weight_units: int
    parent_id: str | None

    def as_policy_dict(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "capacity_bytes": units_to_str(self.capacity_units),
            "requests_per_second": units_to_str(self.rps_units),
            "burst_capacity": units_to_str(self.burst_units),
            "weight": units_to_str(self.weight_units),
            "parent_id": self.parent_id,
        }


@dataclass(frozen=True)
class TenantInput:
    tenant_id: str
    capacity_units: int
    rps_units: int
    burst_units: int
    weight_units: int
    parent_id: str | None


def _decimal_is_negative(value: Decimal) -> bool:
    try:
        return Decimal(str(value)) < 0
    except (InvalidOperation, ValueError):
        return False


def parse_tenant_values(specs: Iterable[TenantSpec]) -> dict[str, TenantInput]:
    """Validate scalar tenant fields without inspecting parent hierarchy."""

    records: dict[str, TenantInput] = {}
    for spec in specs:
        if not spec.tenant_id or not spec.tenant_id.strip():
            raise spec_invalid("tenant_id must not be empty", field="tenant_id")
        if spec.tenant_id in records:
            raise spec_invalid(
                "tenant_id must be unique inside a policy snapshot",
                tenant_id=spec.tenant_id,
            )
        if spec.parent_id is not None and not spec.parent_id.strip():
            raise spec_invalid(
                "parent_id must be null or a non-empty tenant id",
                tenant_id=spec.tenant_id,
            )
        if any(
            _decimal_is_negative(value)
            for value in (
                spec.capacity_bytes,
                spec.requests_per_second,
                spec.burst_capacity,
                spec.weight,
            )
        ):
            raise spec_invalid(
                "capacity, rate, burst and weight must be non-negative",
                tenant_id=spec.tenant_id,
            )
        records[spec.tenant_id] = TenantInput(
            tenant_id=spec.tenant_id,
            capacity_units=to_units(spec.capacity_bytes, f"tenants[{spec.tenant_id}].capacity_bytes"),
            rps_units=to_units(spec.requests_per_second, f"tenants[{spec.tenant_id}].requests_per_second"),
            burst_units=to_units(spec.burst_capacity, f"tenants[{spec.tenant_id}].burst_capacity"),
            weight_units=to_units(spec.weight, f"tenants[{spec.tenant_id}].weight"),
            parent_id=spec.parent_id,
        )

    if not records:
        raise spec_invalid("a policy snapshot must contain at least one tenant")
    return records


def as_tenant_records(records: dict[str, TenantInput]) -> dict[str, TenantRecord]:
    return {
        tenant_id: TenantRecord(
            tenant_id=tenant_id,
            capacity_units=item.capacity_units,
            rps_units=item.rps_units,
            burst_units=item.burst_units,
            weight_units=item.weight_units,
            parent_id=item.parent_id,
        )
        for tenant_id, item in records.items()
    }


def validate_tenant_hierarchy(records: dict[str, TenantRecord]) -> dict[str, TenantRecord]:
    missing = sorted(
        {
            record.parent_id
            for record in records.values()
            if record.parent_id is not None and record.parent_id not in records
        }
    )
    if missing:
        raise DomainError(
            "E_TENANT_CYCLE",
            "a tenant references a parent that is not present",
            {
                "code_detail": "MISSING_PARENT",
                "missing_parent_ids": missing,
                "cycle_ids": [],
            },
            409,
        )

    cycle = find_cycle(records)
    if cycle:
        raise DomainError(
            "E_TENANT_CYCLE",
            "tenant parent references form a cycle",
            {"cycle_ids": cycle, "cycle": " -> ".join(cycle + [cycle[0]])},
            409,
        )
    return records


def parse_tenant_specs(specs: Iterable[TenantSpec]) -> dict[str, TenantRecord]:
    values = parse_tenant_values(specs)
    return validate_tenant_hierarchy(as_tenant_records(values))

def find_cycle(records: dict[str, TenantInput] | dict[str, TenantRecord]) -> list[str] | None:
    """Return a deterministic cycle path, excluding the repeated closing node."""

    visiting: set[str] = set()
    visited: set[str] = set()
    stack: list[str] = []

    def visit(tenant_id: str) -> list[str] | None:
        if tenant_id in visiting:
            start = stack.index(tenant_id)
            return stack[start:]
        if tenant_id in visited:
            return None
        visiting.add(tenant_id)
        stack.append(tenant_id)
        parent = records[tenant_id].parent_id
        if parent is not None and parent in records:
            found = visit(parent)
            if found:
                return found
        stack.pop()
        visiting.remove(tenant_id)
        visited.add(tenant_id)
        return None

    for tenant_id in sorted(records):
        found = visit(tenant_id)
        if found:
            return found
    return None


def lineage(tenant_id: str, tenants: dict[str, TenantRecord]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    current: str | None = tenant_id
    while current is not None:
        if current in seen:
            raise DomainError(
                "E_TENANT_CYCLE",
                "tenant hierarchy changed into a cycle",
                {"cycle_ids": list(seen)},
                409,
            )
        seen.add(current)
        result.append(current)
        current = tenants[current].parent_id if current in tenants else None
    return result


def descendants(tenant_id: str, tenants: dict[str, TenantRecord]) -> set[str]:
    result: set[str] = set()
    frontier = [tenant_id]
    while frontier:
        current = frontier.pop()
        for child_id, tenant in tenants.items():
            if tenant.parent_id == current and child_id not in result:
                result.add(child_id)
                frontier.append(child_id)
    return result


def policy_checksum(version: int, generated_at_ms: int, tenants: list[dict[str, Any]]) -> str:
    canonical = json.dumps(
        {
            "version": version,
            "generated_at_ms": generated_at_ms,
            "tenants": sorted(tenants, key=lambda item: item["tenant_id"]),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def policy_document(
    version: int,
    generated_at_ms: int,
    tenants: dict[str, TenantRecord],
    checksum: str | None = None,
) -> dict[str, Any]:
    tenant_docs = [tenants[key].as_policy_dict() for key in sorted(tenants)]
    computed = policy_checksum(version, generated_at_ms, tenant_docs)
    return {
        "version": version,
        "generated_at_ms": generated_at_ms,
        "tenants": tenant_docs,
        "checksum": computed,
        "checksum_algorithm": "sha256(canonical-json)",
        **( {"checksum_verified": checksum == computed} if checksum is not None else {}),
    }


LEGAL_TRANSITIONS: dict[TenantStatus, set[TenantStatus]] = {
    TenantStatus.ACTIVE: {
        TenantStatus.THROTTLED,
        TenantStatus.QUOTA_BLOCKED,
        TenantStatus.SUSPENDED,
    },
    TenantStatus.THROTTLED: {
        TenantStatus.ACTIVE,
        TenantStatus.QUOTA_BLOCKED,
        TenantStatus.SUSPENDED,
    },
    TenantStatus.QUOTA_BLOCKED: {
        TenantStatus.ACTIVE,
        TenantStatus.THROTTLED,
        TenantStatus.SUSPENDED,
    },
    TenantStatus.SUSPENDED: {TenantStatus.ACTIVE},
}


def transition_allowed(source: TenantStatus | str, target: TenantStatus | str) -> bool:
    return TenantStatus(target) in LEGAL_TRANSITIONS[TenantStatus(source)]
