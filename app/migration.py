"""Pure planning logic for v1-flat -> v2-hierarchical migration."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from .errors import AppError, ErrorCode
from .policy import (
    MAX_PARENT_DEPTH,
    SCHEMA_V1,
    TenantPolicy,
    build_snapshot,
    legacy_compatibility_issues,
    parse_legacy_tenant,
    require_int,
    require_str,
    verify_checksum,
)


def checksum_value(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


@dataclass
class MigrationPlan:
    code: str
    status: str
    report: dict[str, Any]
    target_tenants: dict[str, TenantPolicy] | None
    output_version: int | None


def _item(
    tenant_id: str | None,
    status: str,
    code: str,
    *,
    reasons: list[str] | None = None,
    mapping_basis: list[dict[str, Any]] | None = None,
    mapped_policy: dict[str, Any] | None = None,
    deprecated_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "tenant_id": tenant_id,
        "status": status,
        "code": code,
        "reasons": reasons or [],
        "mapping_basis": mapping_basis or [],
        "mapped_policy": mapped_policy,
        "deprecated_fields": deprecated_fields or {},
    }


def _legacy_basis(raw: dict[str, Any]) -> list[dict[str, Any]]:
    basis = [
        {"source": "tenant_id", "target": "tenant_id", "value": raw.get("tenant_id")},
        {
            "source": "cpu_millicores_limit",
            "target": "quota.cpu_millicores",
            "value": raw.get("cpu_millicores_limit"),
        },
        {
            "source": "memory_bytes_limit",
            "target": "quota.memory_bytes",
            "value": raw.get("memory_bytes_limit"),
        },
        {
            "source": "requests_per_second_limit",
            "target": "rate_limit.refill_per_second",
            "value": raw.get("requests_per_second_limit"),
        },
        {"source": "lifecycle", "target": "lifecycle", "value": raw.get("lifecycle")},
    ]
    field_map = {
        "deprecated_burst_tokens": "rate_limit.capacity",
        "legacy_burst_tokens": "rate_limit.capacity",
        "deprecated_initial_tokens": "rate_limit.initial_tokens",
        "legacy_initial_tokens": "rate_limit.initial_tokens",
        "deprecated_burst_cpu_millicores": "quota.burst.cpu_millicores",
        "legacy_burst_cpu_millicores": "quota.burst.cpu_millicores",
        "deprecated_burst_memory_bytes": "quota.burst.memory_bytes",
        "legacy_burst_memory_bytes": "quota.burst.memory_bytes",
        "deprecated_parent_tenant_id": "parent_id",
        "legacy_parent_tenant_id": "parent_id",
        "parent_id": "parent_id",
    }
    for source, target in field_map.items():
        if source in raw:
            basis.append({"source": source, "target": target, "value": raw[source]})
    if raw.get("parent_id") is None and "parent_id" not in raw:
        basis.append(
            {
                "source": "(default)",
                "target": "parent_id",
                "value": None,
                "note": "root tenant when no parent is supplied",
            }
        )
    if "deprecated_burst_tokens" not in raw and "legacy_burst_tokens" not in raw:
        basis.append(
            {
                "source": "(default)",
                "target": "rate_limit.capacity",
                "value": raw.get("requests_per_second_limit"),
                "note": "one-second burst capacity",
            }
        )
    return basis


def plan_migration(
    legacy_document: dict[str, Any],
    *,
    current_snapshot,
    parent_map: dict[str, str | None] | None,
    confirm_lifecycle_overlap: bool,
    generated_at_ms: int,
) -> MigrationPlan:
    if not isinstance(legacy_document, dict):
        raise AppError(ErrorCode.E_SPEC_INVALID, "legacy migration input must be an object")
    if legacy_document.get("schema_version") != SCHEMA_V1:
        raise AppError(
            ErrorCode.E_SPEC_INVALID,
            "migration input must use the v1-flat schema",
            {"expected": SCHEMA_V1, "actual": legacy_document.get("schema_version")},
        )
    require_int(legacy_document.get("version"), "version", minimum=1)
    require_int(legacy_document.get("generated_at_ms"), "generated_at_ms", minimum=0)
    raw_tenants = legacy_document.get("tenants")
    if not isinstance(raw_tenants, list) or not raw_tenants:
        raise AppError(ErrorCode.E_SPEC_INVALID, "migration tenants must be a non-empty array")
    verify_checksum(legacy_document, required=True)

    ids: list[str] = []
    for raw in raw_tenants:
        if not isinstance(raw, dict):
            raise AppError(ErrorCode.E_SPEC_INVALID, "migration tenant entry must be an object")
        ids.append(require_str(raw.get("tenant_id"), "tenant_id"))
    if len(set(ids)) != len(ids):
        raise AppError(
            ErrorCode.E_SPEC_INVALID,
            "migration batch contains duplicate tenant_id values",
            {"duplicate_ids": sorted({x for x in ids if ids.count(x) > 1})},
        )

    current_tenants: dict[str, TenantPolicy] = (
        dict(current_snapshot.tenants) if current_snapshot is not None else {}
    )
    items: list[dict[str, Any]] = []
    parsed: dict[str, TenantPolicy] = {}

    for raw in raw_tenants:
        tenant_id = raw["tenant_id"]
        issues = legacy_compatibility_issues(raw)
        deprecated = {
            key: raw[key]
            for key in raw
            if key.startswith("deprecated_") or key.startswith("legacy_")
        }
        if issues:
            items.append(
                _item(
                    tenant_id,
                    "PENDING_REVIEW",
                    "E_MIGRATION_REVIEW",
                    reasons=[issue["code"] for issue in issues],
                    deprecated_fields=deprecated,
                    mapping_basis=[
                        {
                            "source": "compatibility_check",
                            "target": "manual_review",
                            "value": issues,
                        }
                    ],
                )
            )
            continue
        try:
            tenant = parse_legacy_tenant(raw, parent_map=parent_map)
        except AppError as exc:
            items.append(
                _item(
                    tenant_id,
                    "REJECTED",
                    exc.code,
                    reasons=[exc.message],
                    deprecated_fields=deprecated,
                )
            )
            continue
        parsed[tenant_id] = tenant
        items.append(
            _item(
                tenant_id,
                "ACCEPTED",
                "OK",
                mapping_basis=_legacy_basis(raw),
                mapped_policy=tenant.to_dict(),
                deprecated_fields=deprecated,
            )
        )

    item_by_id = {item["tenant_id"]: item for item in items}
    available_ids = set(current_tenants) | set(parsed)

    for tenant_id, tenant in list(parsed.items()):
        item = item_by_id[tenant_id]
        if tenant.parent_id is not None and tenant.parent_id not in available_ids:
            item.update(
                status="REJECTED",
                code="E_MIGRATION_REJECTED",
                reasons=[
                    f"parent tenant {tenant.parent_id!r} does not exist in the target hierarchy"
                ],
            )
            parsed.pop(tenant_id, None)

    # Re-validate after parent rejection so children of rejected parents cannot slip through.
    changed = True
    while changed:
        changed = False
        available_ids = set(current_tenants) | set(parsed)
        for tenant_id, tenant in list(parsed.items()):
            item = item_by_id[tenant_id]
            if tenant.parent_id is not None and tenant.parent_id not in available_ids:
                item.update(
                    status="REJECTED",
                    code="E_MIGRATION_REJECTED",
                    reasons=[
                        f"parent tenant {tenant.parent_id!r} does not exist in the target hierarchy"
                    ],
                )
                parsed.pop(tenant_id, None)
                changed = True

    current_by_id = current_tenants
    for tenant_id, tenant in list(parsed.items()):
        item = item_by_id[tenant_id]
        previous = current_by_id.get(tenant_id)
        if previous is not None and tenant.lifecycle.overlaps(previous.lifecycle):
            if not confirm_lifecycle_overlap:
                item.update(
                    status="PENDING_REVIEW",
                    code="E_MIGRATION_REVIEW",
                    reasons=[
                        "lifecycle interval overlaps the active policy; explicit confirmation is required"
                    ],
                )
            else:
                item["mapping_basis"].append(
                    {
                        "source": "lifecycle_overlap_confirmation",
                        "target": "lifecycle",
                        "value": True,
                        "note": "overlap explicitly confirmed",
                    }
                )

    # Validate parent depth over the proposed target set, when applicable.
    target_preview = dict(current_tenants)
    target_preview.update(parsed)
    if parsed:
        try:
            build_snapshot(
                target_preview.values(),
                version=(current_snapshot.version + 1 if current_snapshot else 1),
                generated_at_ms=generated_at_ms,
                source_schema=SCHEMA_V1,
            )
        except AppError as exc:
            if exc.details.get("tenant_id") in item_by_id:
                item_by_id[exc.details["tenant_id"]].update(
                    status="REJECTED",
                    code="E_MIGRATION_REJECTED",
                    reasons=[exc.message],
                )
                parsed.pop(exc.details["tenant_id"], None)
            else:
                for tenant_id in list(parsed):
                    item_by_id[tenant_id].update(
                        status="REJECTED",
                        code="E_MIGRATION_REJECTED",
                        reasons=[exc.message],
                    )
                parsed.clear()

    statuses = [item["status"] for item in items]
    if "REJECTED" in statuses:
        code = ErrorCode.E_MIGRATION_REJECTED
        status = "ROLLED_BACK"
    elif "PENDING_REVIEW" in statuses:
        code = ErrorCode.E_MIGRATION_REVIEW
        status = "PENDING_REVIEW"
    else:
        code = "OK"
        status = "APPLIED"

    output_version: int | None = None
    target_tenants: dict[str, TenantPolicy] | None = None
    if status == "APPLIED":
        target_tenants = dict(current_tenants)
        target_tenants.update(parsed)
        output_version = current_snapshot.version + 1 if current_snapshot is not None else 1
    else:
        # Keep the rollback preview in the report, but never activate it.
        target_tenants = None

    summary = {
        "total": len(items),
        "accepted": sum(item["status"] == "ACCEPTED" for item in items),
        "pending_review": sum(item["status"] == "PENDING_REVIEW" for item in items),
        "rejected": sum(item["status"] == "REJECTED" for item in items),
    }
    report: dict[str, Any] = {
        "migration_id": None,
        "status": status,
        "code": code,
        "base_version": current_snapshot.version if current_snapshot is not None else 0,
        "output_version": output_version,
        "generated_at_ms": generated_at_ms,
        "max_parent_depth": MAX_PARENT_DEPTH,
        "summary": summary,
        "items": items,
        "atomic": True,
        "rollback": status != "APPLIED",
    }
    report["report_checksum"] = checksum_value(report)
    return MigrationPlan(
        code=code,
        status=status,
        report=report,
        target_tenants=target_tenants,
        output_version=output_version,
    )
