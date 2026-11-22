"""Policy parsing, validation and deterministic checksums.

The checksum is computed over a semantic normalization of a policy document:
top-level object keys are sorted and the tenant list is sorted by tenant_id.
Whitespace and tenant order therefore do not affect the digest, while any
business-field change does.
"""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Iterable

from .errors import AppError, ErrorCode

SCHEMA_V1 = "governance.policy/v1-flat"
SCHEMA_V2 = "governance.policy/v2"
MAX_PARENT_DEPTH = 4
TOKEN_SCALE = 1000  # one request token == 1000 persisted token units


def _invalid(message: str, details: dict[str, Any] | None = None) -> AppError:
    return AppError(ErrorCode.E_SPEC_INVALID, message, details or {})


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def require_int(
    value: Any,
    field_name: str,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
    tenant_id: str | None = None,
) -> int:
    if not _is_int(value):
        raise _invalid(
            f"{field_name} must be an integer",
            {"field": field_name, "tenant_id": tenant_id, "actual": value},
        )
    if minimum is not None and value < minimum:
        raise _invalid(
            f"{field_name} must be >= {minimum}",
            {"field": field_name, "tenant_id": tenant_id, "minimum": minimum, "actual": value},
        )
    if maximum is not None and value > maximum:
        raise _invalid(
            f"{field_name} must be <= {maximum}",
            {"field": field_name, "tenant_id": tenant_id, "maximum": maximum, "actual": value},
        )
    if value > 2**63 - 1:
        raise _invalid(
            f"{field_name} exceeds SQLite INTEGER range",
            {"field": field_name, "tenant_id": tenant_id, "actual": value},
        )
    return value


def require_str(value: Any, field_name: str, *, tenant_id: str | None = None) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _invalid(
            f"{field_name} must be a non-empty string",
            {"field": field_name, "tenant_id": tenant_id, "actual": value},
        )
    return value


def _canonical_document(document: dict[str, Any]) -> dict[str, Any]:
    """Return the semantic form covered by a checksum."""
    result = copy.deepcopy(document)
    result.pop("checksum", None)
    tenants = result.get("tenants")
    if isinstance(tenants, list):
        result["tenants"] = sorted(
            tenants,
            key=lambda item: item.get("tenant_id", "") if isinstance(item, dict) else "",
        )
    return result


def compute_checksum(document: dict[str, Any]) -> str:
    canonical = _canonical_document(document)
    payload = json.dumps(
        canonical,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def verify_checksum(document: dict[str, Any], *, required: bool = True) -> str:
    supplied = document.get("checksum")
    if supplied is None:
        if required:
            raise _invalid("policy checksum is required", {"field": "checksum"})
        supplied = compute_checksum(document)
    if not isinstance(supplied, str) or not supplied.startswith("sha256:"):
        raise _invalid("checksum must have the form sha256:<hex>", {"field": "checksum"})
    actual = compute_checksum(document)
    if supplied != actual:
        raise _invalid(
            "policy checksum does not match the canonical document",
            {"field": "checksum", "supplied": supplied, "actual": actual},
        )
    return supplied


def with_checksum(document: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(document)
    result.pop("checksum", None)
    result["checksum"] = compute_checksum(result)
    return result


@dataclass(frozen=True)
class Quota:
    cpu_millicores: int
    memory_bytes: int
    burst_cpu_millicores: int = 0
    burst_memory_bytes: int = 0

    @property
    def cpu_limit(self) -> int:
        return self.cpu_millicores + self.burst_cpu_millicores

    @property
    def memory_limit(self) -> int:
        return self.memory_bytes + self.burst_memory_bytes

    def to_dict(self) -> dict[str, Any]:
        return {
            "cpu_millicores": self.cpu_millicores,
            "memory_bytes": self.memory_bytes,
            "burst": {
                "cpu_millicores": self.burst_cpu_millicores,
                "memory_bytes": self.burst_memory_bytes,
            },
        }


@dataclass(frozen=True)
class TokenBucketPolicy:
    refill_per_second: int
    capacity: int
    initial_tokens: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "refill_per_second": self.refill_per_second,
            "capacity": self.capacity,
            "initial_tokens": self.initial_tokens,
        }


@dataclass(frozen=True)
class Lifecycle:
    start_ms: int
    end_ms: int

    def contains(self, timestamp_ms: int) -> bool:
        return self.start_ms <= timestamp_ms < self.end_ms

    def overlaps(self, other: "Lifecycle") -> bool:
        return self.start_ms < other.end_ms and other.start_ms < self.end_ms

    def to_dict(self) -> dict[str, int]:
        return {"start_ms": self.start_ms, "end_ms": self.end_ms}


@dataclass(frozen=True)
class TenantPolicy:
    tenant_id: str
    parent_id: str | None
    quota: Quota
    rate_limit: TokenBucketPolicy
    lifecycle: Lifecycle
    deprecated_fields: dict[str, Any] = field(default_factory=dict, compare=False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "parent_id": self.parent_id,
            "quota": self.quota.to_dict(),
            "rate_limit": self.rate_limit.to_dict(),
            "lifecycle": self.lifecycle.to_dict(),
        }


@dataclass(frozen=True)
class PolicySnapshot:
    schema_version: str
    version: int
    generated_at_ms: int
    tenants: dict[str, TenantPolicy]
    checksum: str
    source_schema: str

    @property
    def tenant_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self.tenants))

    def chain(self, tenant_id: str) -> tuple[str, ...]:
        """Return [tenant, parent, ..., root]."""
        if tenant_id not in self.tenants:
            raise AppError(
                ErrorCode.E_TENANT_NOT_FOUND,
                f"tenant {tenant_id!r} is not present in snapshot {self.version}",
                {"tenant_id": tenant_id, "snapshot_version": self.version},
            )
        result: list[str] = []
        seen: set[str] = set()
        current: str | None = tenant_id
        while current is not None:
            if current in seen:
                raise _invalid("tenant hierarchy contains a cycle", {"tenant_id": current})
            seen.add(current)
            result.append(current)
            current = self.tenants[current].parent_id
        return tuple(result)

    def ancestors(self, tenant_id: str) -> tuple[str, ...]:
        return self.chain(tenant_id)[1:]

    def depth(self, tenant_id: str) -> int:
        return len(self.chain(tenant_id)) - 1

    def to_dict(self, *, include_checksum: bool = True) -> dict[str, Any]:
        document: dict[str, Any] = {
            "schema_version": SCHEMA_V2,
            "version": self.version,
            "generated_at_ms": self.generated_at_ms,
            "tenants": [self.tenants[tid].to_dict() for tid in self.tenant_ids],
        }
        if include_checksum:
            document["checksum"] = self.checksum
        return document


def _parse_lifecycle(raw: Any, *, tenant_id: str) -> Lifecycle:
    if not isinstance(raw, dict):
        raise _invalid(
            "lifecycle must be an object with start_ms and end_ms",
            {"field": "lifecycle", "tenant_id": tenant_id},
        )
    start = require_int(raw.get("start_ms"), "lifecycle.start_ms", minimum=0, tenant_id=tenant_id)
    end = require_int(raw.get("end_ms"), "lifecycle.end_ms", minimum=1, tenant_id=tenant_id)
    if end <= start:
        raise _invalid(
            "lifecycle.end_ms must be greater than lifecycle.start_ms",
            {"field": "lifecycle", "tenant_id": tenant_id, "start_ms": start, "end_ms": end},
        )
    return Lifecycle(start, end)


def parse_v2_tenant(raw: Any) -> TenantPolicy:
    if not isinstance(raw, dict):
        raise _invalid("tenant entry must be an object")
    tenant_id = require_str(raw.get("tenant_id"), "tenant_id")
    parent_id = raw.get("parent_id")
    if parent_id is not None:
        parent_id = require_str(parent_id, "parent_id", tenant_id=tenant_id)
        if parent_id == tenant_id:
            raise _invalid("tenant cannot be its own parent", {"tenant_id": tenant_id})

    quota_raw = raw.get("quota")
    if not isinstance(quota_raw, dict):
        raise _invalid(
            "quota must be an object",
            {"field": "quota", "tenant_id": tenant_id},
        )
    cpu = require_int(quota_raw.get("cpu_millicores"), "quota.cpu_millicores", minimum=1, tenant_id=tenant_id)
    memory = require_int(quota_raw.get("memory_bytes"), "quota.memory_bytes", minimum=1, tenant_id=tenant_id)
    burst_raw = quota_raw.get("burst", {})
    if burst_raw is None:
        burst_raw = {}
    if not isinstance(burst_raw, dict):
        raise _invalid(
            "quota.burst must be an object",
            {"field": "quota.burst", "tenant_id": tenant_id},
        )
    burst_cpu = require_int(
        burst_raw.get("cpu_millicores", 0),
        "quota.burst.cpu_millicores",
        minimum=0,
        tenant_id=tenant_id,
    )
    burst_memory = require_int(
        burst_raw.get("memory_bytes", 0),
        "quota.burst.memory_bytes",
        minimum=0,
        tenant_id=tenant_id,
    )

    rate_raw = raw.get("rate_limit")
    if not isinstance(rate_raw, dict):
        raise _invalid(
            "rate_limit must be an object",
            {"field": "rate_limit", "tenant_id": tenant_id},
        )
    refill = require_int(
        rate_raw.get("refill_per_second"),
        "rate_limit.refill_per_second",
        minimum=1,
        tenant_id=tenant_id,
    )
    capacity = require_int(
        rate_raw.get("capacity"),
        "rate_limit.capacity",
        minimum=1,
        tenant_id=tenant_id,
    )
    initial = require_int(
        rate_raw.get("initial_tokens"),
        "rate_limit.initial_tokens",
        minimum=0,
        tenant_id=tenant_id,
    )
    if initial > capacity:
        raise _invalid(
            "rate_limit.initial_tokens cannot exceed rate_limit.capacity",
            {"tenant_id": tenant_id, "initial_tokens": initial, "capacity": capacity},
        )

    lifecycle = _parse_lifecycle(raw.get("lifecycle"), tenant_id=tenant_id)
    return TenantPolicy(
        tenant_id=tenant_id,
        parent_id=parent_id,
        quota=Quota(cpu, memory, burst_cpu, burst_memory),
        rate_limit=TokenBucketPolicy(refill, capacity, initial),
        lifecycle=lifecycle,
    )


def validate_hierarchy(
    tenants: dict[str, TenantPolicy],
    *,
    max_depth: int = MAX_PARENT_DEPTH,
) -> dict[str, int]:
    depths: dict[str, int] = {}
    for tenant_id in tenants:
        chain: list[str] = []
        seen: set[str] = set()
        current: str | None = tenant_id
        while current is not None:
            if current in seen:
                raise _invalid(
                    "tenant hierarchy contains a cycle",
                    {"tenant_id": tenant_id, "cycle_at": current},
                )
            seen.add(current)
            chain.append(current)
            if current not in tenants:
                raise _invalid(
                    "parent tenant reference does not exist",
                    {"tenant_id": tenant_id, "missing_parent_id": current},
                )
            current = tenants[current].parent_id
        depth = len(chain) - 1
        if depth > max_depth:
            raise _invalid(
                "tenant hierarchy exceeds maximum depth",
                {
                    "tenant_id": tenant_id,
                    "depth": depth,
                    "max_depth": max_depth,
                    "chain": list(reversed(chain)),
                },
            )
        depths[tenant_id] = depth
    return depths


def build_snapshot(
    tenants: Iterable[TenantPolicy],
    *,
    version: int,
    generated_at_ms: int,
    source_schema: str = SCHEMA_V2,
) -> PolicySnapshot:
    tenant_map: dict[str, TenantPolicy] = {}
    for tenant in tenants:
        if tenant.tenant_id in tenant_map:
            raise _invalid(
                "duplicate tenant_id",
                {"tenant_id": tenant.tenant_id},
            )
        tenant_map[tenant.tenant_id] = tenant
    validate_hierarchy(tenant_map)
    document = {
        "schema_version": SCHEMA_V2,
        "version": version,
        "generated_at_ms": generated_at_ms,
        "tenants": [tenant_map[tid].to_dict() for tid in sorted(tenant_map)],
    }
    document = with_checksum(document)
    return PolicySnapshot(
        schema_version=SCHEMA_V2,
        version=version,
        generated_at_ms=generated_at_ms,
        tenants=tenant_map,
        checksum=document["checksum"],
        source_schema=source_schema,
    )


def parse_hierarchical_document(document: dict[str, Any], *, verify: bool = True) -> PolicySnapshot:
    if not isinstance(document, dict):
        raise _invalid("snapshot must be a JSON object")
    if document.get("schema_version") != SCHEMA_V2:
        raise _invalid(
            "unsupported hierarchical schema_version",
            {"expected": SCHEMA_V2, "actual": document.get("schema_version")},
        )
    version = require_int(document.get("version"), "version", minimum=1)
    generated_at_ms = require_int(document.get("generated_at_ms"), "generated_at_ms", minimum=0)
    raw_tenants = document.get("tenants")
    if not isinstance(raw_tenants, list) or not raw_tenants:
        raise _invalid("tenants must be a non-empty array", {"field": "tenants"})

    tenants = [parse_v2_tenant(item) for item in raw_tenants]
    tenant_map: dict[str, TenantPolicy] = {}
    for tenant in tenants:
        if tenant.tenant_id in tenant_map:
            raise _invalid("duplicate tenant_id", {"tenant_id": tenant.tenant_id})
        tenant_map[tenant.tenant_id] = tenant
    validate_hierarchy(tenant_map)
    checksum = verify_checksum(document, required=verify)
    return PolicySnapshot(
        schema_version=SCHEMA_V2,
        version=version,
        generated_at_ms=generated_at_ms,
        tenants=tenant_map,
        checksum=checksum,
        source_schema=SCHEMA_V2,
    )


_LEGACY_ALIASES: dict[str, tuple[str, ...]] = {
    "capacity": ("deprecated_burst_tokens", "legacy_burst_tokens"),
    "initial_tokens": ("deprecated_initial_tokens", "legacy_initial_tokens"),
    "burst_cpu": ("deprecated_burst_cpu_millicores", "legacy_burst_cpu_millicores"),
    "burst_memory": ("deprecated_burst_memory_bytes", "legacy_burst_memory_bytes"),
    "parent_id": ("deprecated_parent_tenant_id", "legacy_parent_tenant_id"),
}


def _legacy_alias_value(raw: dict[str, Any], semantic: str) -> tuple[Any, list[str]]:
    present = [key for key in _LEGACY_ALIASES[semantic] if key in raw]
    if not present:
        return None, []
    values = [raw[key] for key in present]
    first = values[0]
    if any(value != first for value in values[1:]):
        raise _invalid(
            "conflicting legacy aliases require explicit field resolution",
            {
                "field": semantic,
                "aliases": {key: raw[key] for key in present},
            },
        )
    return first, present


def parse_legacy_tenant(raw: Any, *, parent_map: dict[str, str | None] | None = None) -> TenantPolicy:
    if not isinstance(raw, dict):
        raise _invalid("tenant entry must be an object")
    tenant_id = require_str(raw.get("tenant_id"), "tenant_id")
    cpu = require_int(
        raw.get("cpu_millicores_limit"),
        "cpu_millicores_limit",
        minimum=1,
        tenant_id=tenant_id,
    )
    memory = require_int(
        raw.get("memory_bytes_limit"),
        "memory_bytes_limit",
        minimum=1,
        tenant_id=tenant_id,
    )
    rate = require_int(
        raw.get("requests_per_second_limit"),
        "requests_per_second_limit",
        minimum=1,
        tenant_id=tenant_id,
    )
    lifecycle = _parse_lifecycle(raw.get("lifecycle"), tenant_id=tenant_id)

    parent_candidates: list[Any] = []
    if "parent_id" in raw:
        parent_candidates.append(raw.get("parent_id"))
    alias_parent, _ = _legacy_alias_value(raw, "parent_id")
    if alias_parent is not None:
        parent_candidates.append(alias_parent)
    if parent_map and tenant_id in parent_map:
        parent_candidates.append(parent_map[tenant_id])
    if len({value for value in parent_candidates}) > 1:
        raise _invalid(
            "conflicting parent references",
            {"tenant_id": tenant_id, "parent_candidates": parent_candidates},
        )
    parent_id = parent_candidates[0] if parent_candidates else None
    if parent_id is not None:
        parent_id = require_str(parent_id, "parent_id", tenant_id=tenant_id)

    capacity_alias, _ = _legacy_alias_value(raw, "capacity")
    initial_alias, _ = _legacy_alias_value(raw, "initial_tokens")
    burst_cpu_alias, _ = _legacy_alias_value(raw, "burst_cpu")
    burst_mem_alias, _ = _legacy_alias_value(raw, "burst_memory")

    capacity = rate if capacity_alias is None else require_int(
        capacity_alias, "deprecated_burst_tokens", minimum=1, tenant_id=tenant_id
    )
    initial = min(rate, capacity) if initial_alias is None else require_int(
        initial_alias, "deprecated_initial_tokens", minimum=0, tenant_id=tenant_id
    )
    if initial > capacity:
        raise _invalid(
            "deprecated_initial_tokens cannot exceed burst capacity",
            {"tenant_id": tenant_id, "initial_tokens": initial, "capacity": capacity},
        )
    burst_cpu = 0 if burst_cpu_alias is None else require_int(
        burst_cpu_alias, "deprecated_burst_cpu_millicores", minimum=0, tenant_id=tenant_id
    )
    burst_mem = 0 if burst_mem_alias is None else require_int(
        burst_mem_alias, "deprecated_burst_memory_bytes", minimum=0, tenant_id=tenant_id
    )

    deprecated_fields = {
        key: value
        for key, value in raw.items()
        if key.startswith("deprecated_") or key.startswith("legacy_")
    }
    return TenantPolicy(
        tenant_id=tenant_id,
        parent_id=parent_id,
        quota=Quota(cpu, memory, burst_cpu, burst_mem),
        rate_limit=TokenBucketPolicy(rate, capacity, initial),
        lifecycle=lifecycle,
        deprecated_fields=deprecated_fields,
    )


def parse_legacy_document(
    document: dict[str, Any],
    *,
    parent_map: dict[str, str | None] | None = None,
    verify: bool = True,
) -> PolicySnapshot:
    if not isinstance(document, dict):
        raise _invalid("legacy snapshot must be a JSON object")
    if document.get("schema_version") != SCHEMA_V1:
        raise _invalid(
            "unsupported legacy schema_version",
            {"expected": SCHEMA_V1, "actual": document.get("schema_version")},
        )
    version = require_int(document.get("version"), "version", minimum=1)
    generated_at_ms = require_int(document.get("generated_at_ms"), "generated_at_ms", minimum=0)
    raw_tenants = document.get("tenants")
    if not isinstance(raw_tenants, list) or not raw_tenants:
        raise _invalid("tenants must be a non-empty array", {"field": "tenants"})
    tenants: dict[str, TenantPolicy] = {}
    for raw in raw_tenants:
        tenant = parse_legacy_tenant(raw, parent_map=parent_map)
        if tenant.tenant_id in tenants:
            raise _invalid("duplicate tenant_id", {"tenant_id": tenant.tenant_id})
        tenants[tenant.tenant_id] = tenant
    validate_hierarchy(tenants)
    checksum = verify_checksum(document, required=verify)
    return PolicySnapshot(
        schema_version=SCHEMA_V2,
        version=version,
        generated_at_ms=generated_at_ms,
        tenants=tenants,
        checksum=checksum,
        source_schema=SCHEMA_V1,
    )


def parse_policy_document(
    document: dict[str, Any],
    *,
    parent_map: dict[str, str | None] | None = None,
    verify: bool = True,
) -> PolicySnapshot:
    if not isinstance(document, dict):
        raise _invalid("policy document must be a JSON object")
    schema = document.get("schema_version")
    if schema == SCHEMA_V2:
        return parse_hierarchical_document(document, verify=verify)
    if schema == SCHEMA_V1:
        return parse_legacy_document(document, parent_map=parent_map, verify=verify)
    raise _invalid(
        "unsupported policy schema_version",
        {"expected": [SCHEMA_V1, SCHEMA_V2], "actual": schema},
    )


def deep_merge(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def legacy_compatibility_issues(raw: dict[str, Any]) -> list[dict[str, Any]]:
    """Return deprecated fields that cannot be mapped without human review."""
    known = {
        key
        for aliases in _LEGACY_ALIASES.values()
        for key in aliases
    }
    issues: list[dict[str, Any]] = []
    for key, value in raw.items():
        if (key.startswith("deprecated_") or key.startswith("legacy_")) and key not in known:
            issues.append(
                {
                    "code": "DEPRECATED_FIELD_UNMAPPED",
                    "field": key,
                    "value": value,
                }
            )
    for semantic, aliases in _LEGACY_ALIASES.items():
        present = [key for key in aliases if key in raw]
        if len(present) > 1 and len({json.dumps(raw[key], sort_keys=True) for key in present}) > 1:
            issues.append(
                {
                    "code": "DEPRECATED_ALIAS_CONFLICT",
                    "field": semantic,
                    "aliases": {key: raw[key] for key in present},
                }
            )
    return issues
