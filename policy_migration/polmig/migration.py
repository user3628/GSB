"""Cross-version semantic migration, mapping rules and adjudication."""

from .errors import (
    ParentCycle,
    ParentDepthExceeded,
    LifetimeOverlap,
)
from .model import (
    DEFAULT_LEGACY_TTL_MS,
    EPOCH_MS,
    NewPolicy,
    parse_legacy_document,
)

ACCEPT = "ACCEPT"
DEGRADED = "DEGRADED"
REJECT = "REJECT"

DEPRECATED_FIELD_RULES = {
    "legacy_queue_hint": {
        "mappable": True,
        "target": "extensions.queue_hint",
        "note": "copied into free-form extension, no scheduling semantics",
    },
    "legacy_retry_class": {
        "mappable": True,
        "target": "extensions.retry_class",
        "note": "retained for observability only",
    },
    "legacy_magic_token": {
        "mappable": False,
        "target": None,
        "note": "retired token has no equivalent in the layered model",
    },
    "legacy_node_affinity": {
        "mappable": False,
        "target": None,
        "note": "node affinity was removed when placement became platform-managed",
    },
}

PREEMPT_TIER_ENABLED = 60
PREEMPT_TIER_DISABLED = 10


def _map_deprecated_fields(deprecated):
    extensions = {}
    mappable = []
    unmappable = []
    for name, value in (deprecated or {}).items():
        rule = DEPRECATED_FIELD_RULES.get(name)
        if rule is None or not rule["mappable"]:
            unmappable.append({
                "field": "deprecated_fields.%s" % name,
                "reason": rule["note"] if rule else "unknown deprecated field has no mapping",
            })
            continue
        target_name = rule["target"].split(".", 1)[1]
        extensions[target_name] = value
        mappable.append({"field": name, "target": rule["target"], "note": rule["note"]})
    return extensions, mappable, unmappable


def map_legacy_policy(policy, now_ms):
    extensions, mapped, unmappable = _map_deprecated_fields(policy.deprecated_fields)
    filled = [
        {"field": "parent_ref", "filled": None,
         "rule": "missing in flat model; default null root inheritance"},
        {"field": "preempt_tier",
         "filled": PREEMPT_TIER_ENABLED if policy.preempt_enabled else PREEMPT_TIER_DISABLED,
         "rule": "boolean preempt_enabled promoted to weighted tier (true->60, false->10)"},
        {"field": "lifetime.start_ms", "filled": EPOCH_MS,
         "rule": "flat model had no explicit lifetime; start anchored at declared epoch"},
        {"field": "lifetime.end_ms", "filled": EPOCH_MS + DEFAULT_LEGACY_TTL_MS,
         "rule": "default 365-day lifetime synthesized"},
    ]
    new_policy = NewPolicy(
        tenant_id=policy.tenant_id,
        parent_ref=None,
        concurrency_limit=policy.concurrency_limit,
        preempt_tier=PREEMPT_TIER_ENABLED if policy.preempt_enabled else PREEMPT_TIER_DISABLED,
        lifetime_start=EPOCH_MS,
        lifetime_end=EPOCH_MS + DEFAULT_LEGACY_TTL_MS,
        priority_min=policy.priority_min,
        priority_max=policy.priority_max,
        extensions=extensions,
    )
    mapping_basis = [
        {"from": "tenant_id", "to": "tenant_id"},
        {"from": "concurrency_limit", "to": "concurrency_limit"},
        {"from": "priority_min/priority_max", "to": "priority_min/priority_max (optional)"},
        {"from": "preempt_enabled", "to": "preempt_tier"},
    ] + [{"from": item["field"], "to": item["target"]} for item in mapped]
    return {
        "policy": new_policy,
        "filled_fields": filled,
        "mapping_basis": mapping_basis,
        "mapped_deprecated": mapped,
        "unmappable_deprecated": unmappable,
    }


def build_legacy_decisions(policies, now_ms):
    decisions = []
    mapped_policies = []
    for index, legacy in enumerate(policies):
        result = map_legacy_policy(legacy, now_ms)
        reasons = list(result["unmappable_deprecated"])
        decision = {
            "tenant_id": legacy.tenant_id,
            "record_index": index,
            "source": "legacy-json",
            "verdict": REJECT if reasons else DEGRADED,
            "filled_fields": result["filled_fields"],
            "mapping_basis": result["mapping_basis"],
            "mapped_deprecated": result["mapped_deprecated"],
            "rejection_reasons": reasons,
            "canonical_digest": result["policy"].canonical_digest(),
        }
        decisions.append(decision)
        mapped_policies.append(result["policy"])
    return decisions, mapped_policies


def build_new_decisions(policies):
    decisions = []
    for index, policy in enumerate(policies):
        decisions.append({
            "tenant_id": policy.tenant_id,
            "record_index": index,
            "source": "new-model",
            "verdict": ACCEPT,
            "filled_fields": [],
            "mapping_basis": [
                {"from": "record", "to": "tenant_id"},
                {"from": "record", "to": "parent_ref"},
                {"from": "record", "to": "concurrency_limit"},
                {"from": "record", "to": "preempt_tier"},
                {"from": "record", "to": "lifetime"},
            ],
            "mapped_deprecated": [],
            "rejection_reasons": [],
            "canonical_digest": policy.canonical_digest(),
        })
    return decisions


def detect_parent_chain(tenant_id, batch_by_tenant, stored_parent_resolver, max_depth):
    """Return chain ids (root -> tenant) or raise a parent-chain error."""
    chain = []
    seen = {}
    current = tenant_id
    depth = 0
    while current is not None:
        if current in seen:
            cycle = chain[seen[current]:] + [current]
            raise ParentCycle(
                "parent reference cycle detected",
                {"cycle_tenant_ids": cycle},
            )
        seen[current] = len(chain)
        chain.append(current)
        parent = batch_by_tenant.get(current).parent_ref if current in batch_by_tenant \
            else stored_parent_resolver(current)
        if parent is None:
            break
        current = parent
        depth += 1
        if depth > max_depth:
            raise ParentDepthExceeded(
                "parent chain depth %d exceeds configured maximum %d"
                % (depth, max_depth),
                {"tenant_id": tenant_id, "depth": depth, "max_depth": max_depth},
            )
    return chain


def _intervals_overlap(start_a, end_a, start_b, end_b):
    return start_a < end_b and start_b < end_a


def check_lifetime_overlaps(batch_policies, stored_policy_resolver):
    for policy in batch_policies:
        for stored in stored_policy_resolver(policy.tenant_id):
            if _intervals_overlap(
                policy.lifetime_start, policy.lifetime_end,
                stored.lifetime_start, stored.lifetime_end,
            ):
                raise LifetimeOverlap(
                    "lifetime overlap for tenant %s" % policy.tenant_id,
                    {
                        "tenant_id": policy.tenant_id,
                        "incoming": {"start_ms": policy.lifetime_start,
                                     "end_ms": policy.lifetime_end},
                        "existing": {"start_ms": stored.lifetime_start,
                                     "end_ms": stored.lifetime_end},
                    },
                )
    by_tenant = {}
    for policy in batch_policies:
        earlier = by_tenant.setdefault(policy.tenant_id, [])
        for other in earlier:
            if _intervals_overlap(
                policy.lifetime_start, policy.lifetime_end,
                other.lifetime_start, other.lifetime_end,
            ):
                raise LifetimeOverlap(
                    "lifetime overlap within batch for tenant %s" % policy.tenant_id,
                    {
                        "tenant_id": policy.tenant_id,
                        "incoming": {"start_ms": policy.lifetime_start,
                                     "end_ms": policy.lifetime_end},
                        "existing": {"start_ms": other.lifetime_start,
                                     "end_ms": other.lifetime_end},
                    },
                )
        earlier.append(policy)


def adjudicate_legacy(raw_document, now_ms):
    policies = parse_legacy_document(raw_document)
    decisions, mapped = build_legacy_decisions(policies, now_ms)
    return decisions, mapped
