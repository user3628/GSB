"""Legacy flat policy and new layered policy domain models."""

import hashlib
import json

EPOCH_MS = 1_700_000_000_000
DEFAULT_LEGACY_TTL_MS = 365 * 24 * 3600 * 1000


def _is_positive_int(value):
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


class LegacyPolicy:
    __slots__ = (
        "tenant_id",
        "concurrency_limit",
        "priority_min",
        "priority_max",
        "preempt_enabled",
        "deprecated_fields",
    )

    def __init__(self, tenant_id, concurrency_limit, priority_min, priority_max,
                 preempt_enabled, deprecated_fields=None):
        self.tenant_id = tenant_id
        self.concurrency_limit = concurrency_limit
        self.priority_min = priority_min
        self.priority_max = priority_max
        self.preempt_enabled = preempt_enabled
        self.deprecated_fields = dict(deprecated_fields or {})

    @classmethod
    def from_dict(cls, raw):
        if not isinstance(raw, dict):
            raise TypeError("legacy policy record must be a JSON object")
        return cls(
            raw.get("tenant_id"),
            raw.get("concurrency_limit"),
            raw.get("priority_min"),
            raw.get("priority_max"),
            raw.get("preempt_enabled"),
            raw.get("deprecated_fields"),
        )

    def validate(self):
        errors = []
        if not isinstance(self.tenant_id, str) or not self.tenant_id:
            errors.append("tenant_id: must be a non-empty string")
        if not _is_positive_int(self.concurrency_limit):
            errors.append("concurrency_limit: must be a positive integer")
        if not (_is_int(self.priority_min) and _is_int(self.priority_max)):
            errors.append("priority_min/priority_max: must be integers")
        elif self.priority_min > self.priority_max:
            errors.append(
                "priority_min/priority_max: lower bound %d exceeds upper bound %d"
                % (self.priority_min, self.priority_max)
            )
        if not isinstance(self.preempt_enabled, bool):
            errors.append("preempt_enabled: must be a boolean")
        if not isinstance(self.deprecated_fields, dict):
            errors.append("deprecated_fields: must be an object when present")
        return errors


class NewPolicy:
    __slots__ = (
        "tenant_id",
        "parent_ref",
        "concurrency_limit",
        "preempt_tier",
        "lifetime_start",
        "lifetime_end",
        "priority_min",
        "priority_max",
        "extensions",
    )

    def __init__(self, tenant_id, parent_ref, concurrency_limit, preempt_tier,
                 lifetime_start, lifetime_end, priority_min=None, priority_max=None,
                 extensions=None):
        self.tenant_id = tenant_id
        self.parent_ref = parent_ref
        self.concurrency_limit = concurrency_limit
        self.preempt_tier = preempt_tier
        self.lifetime_start = lifetime_start
        self.lifetime_end = lifetime_end
        self.priority_min = priority_min
        self.priority_max = priority_max
        self.extensions = dict(extensions or {})

    @classmethod
    def from_dict(cls, raw):
        if not isinstance(raw, dict):
            raise TypeError("new policy record must be a JSON object")
        lifetime = raw.get("lifetime", {})
        return cls(
            raw.get("tenant_id"),
            raw.get("parent_ref"),
            raw.get("concurrency_limit"),
            raw.get("preempt_tier"),
            lifetime.get("start_ms") if isinstance(lifetime, dict) else None,
            lifetime.get("end_ms") if isinstance(lifetime, dict) else None,
            raw.get("priority_min"),
            raw.get("priority_max"),
            raw.get("extensions"),
        )

    def to_dict(self):
        data = {
            "tenant_id": self.tenant_id,
            "parent_ref": self.parent_ref,
            "concurrency_limit": self.concurrency_limit,
            "preempt_tier": self.preempt_tier,
            "lifetime": {
                "start_ms": self.lifetime_start,
                "end_ms": self.lifetime_end,
            },
        }
        if self.priority_min is not None:
            data["priority_min"] = self.priority_min
        if self.priority_max is not None:
            data["priority_max"] = self.priority_max
        if self.extensions:
            data["extensions"] = dict(self.extensions)
        return data

    def validate(self):
        errors = []
        if not isinstance(self.tenant_id, str) or not self.tenant_id:
            errors.append("tenant_id: must be a non-empty string")
        if self.parent_ref is not None and (
            not isinstance(self.parent_ref, str) or not self.parent_ref
        ):
            errors.append("parent_ref: must be a non-empty string or null")
        if not _is_positive_int(self.concurrency_limit):
            errors.append("concurrency_limit: must be a positive integer")
        if not _is_positive_int(self.preempt_tier) or not 1 <= self.preempt_tier <= 100:
            errors.append("preempt_tier: must be an integer in [1, 100]")
        if not (_is_int(self.lifetime_start) and _is_int(self.lifetime_end)):
            errors.append("lifetime.start_ms/lifetime.end_ms: must be integers")
        else:
            if self.lifetime_start < EPOCH_MS:
                errors.append(
                    "lifetime.start_ms: %d is earlier than declared epoch %d"
                    % (self.lifetime_start, EPOCH_MS)
                )
            if self.lifetime_start >= self.lifetime_end:
                errors.append(
                    "lifetime.start_ms/lifetime.end_ms: start %d must be earlier than end %d"
                    % (self.lifetime_start, self.lifetime_end)
                )
        if self.priority_min is not None or self.priority_max is not None:
            if not (_is_int(self.priority_min) and _is_int(self.priority_max)):
                errors.append("priority_min/priority_max: must be integers when present")
            elif self.priority_min > self.priority_max:
                errors.append(
                    "priority_min/priority_max: lower bound %d exceeds upper bound %d"
                    % (self.priority_min, self.priority_max)
                )
        if not isinstance(self.extensions, dict):
            errors.append("extensions: must be an object when present")
        return errors

    def canonical_digest(self):
        canonical = json.dumps(
            self.to_dict(), sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def parse_legacy_document(raw):
    """Parse a legacy flat JSON document and validate every record.

    Raises PolicyInvalid carrying field paths, including duplicate tenant ids.
    """
    if isinstance(raw, dict) and "policies" in raw:
        records = raw["policies"]
    else:
        records = raw
    if not isinstance(records, list):
        raise TypeError("legacy document must be a list of records or {'policies': [...]}")

    policies = []
    seen = set()
    for index, item in enumerate(records):
        policy = LegacyPolicy.from_dict(item)
        errors = policy.validate()
        if isinstance(policy.tenant_id, str) and policy.tenant_id:
            if policy.tenant_id in seen:
                errors.append("tenant_id: duplicate tenant identifier %s" % policy.tenant_id)
            seen.add(policy.tenant_id)
        if errors:
            from .errors import PolicyInvalid
            raise PolicyInvalid(
                "legacy policy record %d failed validation" % index,
                {"record_index": index, "field_paths": errors},
            )
        policies.append(policy)
    return policies


def parse_new_records(records):
    """Validate new-model records, enforcing unique tenant ids within the batch."""
    if not isinstance(records, list):
        raise TypeError("new policy payload must be a list of records")
    policies = []
    seen = set()
    for index, item in enumerate(records):
        if isinstance(item, NewPolicy):
            policy = item
        else:
            policy = NewPolicy.from_dict(item)
        errors = policy.validate()
        if isinstance(policy.tenant_id, str) and policy.tenant_id:
            if policy.tenant_id in seen:
                errors.append("tenant_id: duplicate tenant identifier %s" % policy.tenant_id)
            seen.add(policy.tenant_id)
        if errors:
            from .errors import PolicyInvalid
            raise PolicyInvalid(
                "new policy record %d failed validation" % index,
                {"record_index": index, "field_paths": errors},
            )
        policies.append(policy)
    return policies
