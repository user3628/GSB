from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from app.domain import CompensatedSum, TenantRecord, lineage, to_units, units_to_str
from app.errors import DomainError
from app.models import EventIn, MeterMode, TenantStatus


@dataclass
class RuntimeState:
    tenant_id: str
    tokens_units: int
    last_refill_ms: int
    refill_remainder: int
    direct_used_bytes_units: int
    aggregate_used_bytes_units: int
    direct_used_request_units: int
    aggregate_used_request_units: int
    consecutive_rejections: int
    status: TenantStatus
    last_reason: str | None
    suspended_reason: str | None
    updated_at_ms: int

    @classmethod
    def initial(cls, tenant: TenantRecord, at_ms: int) -> "RuntimeState":
        return cls(
            tenant_id=tenant.tenant_id,
            tokens_units=tenant.burst_units,
            last_refill_ms=at_ms,
            refill_remainder=0,
            direct_used_bytes_units=0,
            aggregate_used_bytes_units=0,
            direct_used_request_units=0,
            aggregate_used_request_units=0,
            consecutive_rejections=0,
            status=TenantStatus.ACTIVE,
            last_reason=None,
            suspended_reason=None,
            updated_at_ms=at_ms,
        )

    def copy(self) -> "RuntimeState":
        return replace(self)

    def as_row_dict(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "tokens_units": self.tokens_units,
            "last_refill_ms": self.last_refill_ms,
            "refill_remainder": self.refill_remainder,
            "direct_used_bytes_units": self.direct_used_bytes_units,
            "aggregate_used_bytes_units": self.aggregate_used_bytes_units,
            "direct_used_request_units": self.direct_used_request_units,
            "aggregate_used_request_units": self.aggregate_used_request_units,
            "consecutive_rejections": self.consecutive_rejections,
            "status": str(self.status),
            "last_reason": self.last_reason,
            "suspended_reason": self.suspended_reason,
            "updated_at_ms": self.updated_at_ms,
        }

    def as_public(self, tenant: TenantRecord) -> dict[str, Any]:
        remaining = max(0, tenant.capacity_units - self.aggregate_used_bytes_units)
        return {
            "tenant_id": self.tenant_id,
            "status": str(self.status),
            "tokens_units": self.tokens_units,
            "tokens": units_to_str(self.tokens_units),
            "tokens_burst_units": tenant.burst_units,
            "tokens_burst": units_to_str(tenant.burst_units),
            "last_refill_ms": self.last_refill_ms,
            "refill_remainder": self.refill_remainder,
            "used_bytes_units": self.aggregate_used_bytes_units,
            "used_bytes": units_to_str(self.aggregate_used_bytes_units),
            "direct_used_bytes_units": self.direct_used_bytes_units,
            "direct_used_bytes": units_to_str(self.direct_used_bytes_units),
            "remaining_bytes_units": remaining,
            "remaining_bytes": units_to_str(remaining),
            "used_request_units": self.aggregate_used_request_units,
            "used_requests": units_to_str(self.aggregate_used_request_units),
            "consecutive_rejections": self.consecutive_rejections,
            "last_reason": self.last_reason,
            "suspended_reason": self.suspended_reason,
            "updated_at_ms": self.updated_at_ms,
            "policy_version": None,
        }


def refill(state: RuntimeState, tenant: TenantRecord, at_ms: int) -> None:
    if at_ms < state.last_refill_ms:
        raise DomainError(
            "E_CLOCK_REWIND",
            "event time is earlier than the tenant's last refill time",
            {
                "tenant_id": state.tenant_id,
                "event_time_ms": at_ms,
                "last_refill_ms": state.last_refill_ms,
            },
            409,
        )
    elapsed_ms = at_ms - state.last_refill_ms
    if elapsed_ms == 0:
        return
    numerator = state.refill_remainder + elapsed_ms * tenant.rps_units
    added = numerator // 1000
    remainder = numerator % 1000
    if state.tokens_units + added >= tenant.burst_units:
        state.tokens_units = tenant.burst_units
        state.refill_remainder = 0
    else:
        state.tokens_units += added
        state.refill_remainder = remainder
    state.last_refill_ms = at_ms


def next_available(state: RuntimeState, tenant: TenantRecord, required_units: int) -> int | None:
    shortage = required_units - state.tokens_units
    if shortage <= 0:
        return state.last_refill_ms
    if tenant.rps_units <= 0:
        return None
    numerator_needed = shortage * 1000 - state.refill_remainder
    delta_ms = max(0, -(-numerator_needed // tenant.rps_units))
    return state.last_refill_ms + delta_ms


def _status_after_success(
    state: RuntimeState,
    tenant: TenantRecord,
    capacity_charged: bool,
) -> None:
    if state.status == TenantStatus.SUSPENDED:
        return
    if state.aggregate_used_bytes_units >= tenant.capacity_units:
        state.status = TenantStatus.QUOTA_BLOCKED
        return
    if state.status == TenantStatus.THROTTLED:
        state.status = TenantStatus.ACTIVE
    elif state.status == TenantStatus.QUOTA_BLOCKED and capacity_charged:
        state.status = TenantStatus.ACTIVE


def _status_after_quota_rejection(state: RuntimeState) -> None:
    if state.status == TenantStatus.SUSPENDED:
        return
    state.status = TenantStatus.QUOTA_BLOCKED


def _status_after_rate_rejection(
    state: RuntimeState,
    threshold: int,
) -> None:
    if state.status == TenantStatus.SUSPENDED:
        return
    state.consecutive_rejections += 1
    if state.consecutive_rejections >= threshold:
        state.status = TenantStatus.THROTTLED


def _lineage_public(
    lineage_ids: list[str],
    states: dict[str, RuntimeState],
    tenants: dict[str, TenantRecord],
) -> list[dict[str, Any]]:
    output = []
    for tenant_id in lineage_ids:
        state = states[tenant_id]
        tenant = tenants[tenant_id]
        output.append(
            {
                "tenant_id": tenant_id,
                "parent_id": tenant.parent_id,
                "status": str(state.status),
                "used_bytes_units": state.aggregate_used_bytes_units,
                "used_bytes": units_to_str(state.aggregate_used_bytes_units),
                "remaining_bytes_units": max(
                    0, tenant.capacity_units - state.aggregate_used_bytes_units
                ),
                "remaining_bytes": units_to_str(
                    max(0, tenant.capacity_units - state.aggregate_used_bytes_units)
                ),
                "tokens_units": state.tokens_units,
                "tokens": units_to_str(state.tokens_units),
                "capacity_units": tenant.capacity_units,
                "capacity_bytes": units_to_str(tenant.capacity_units),
            }
        )
    return output


def evaluate_batch(
    states: dict[str, RuntimeState],
    tenants: dict[str, TenantRecord],
    events: list[EventIn],
    last_event_ms: int,
    throttle_threshold: int,
    persist: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Evaluate a prevalidated, time-monotonic batch.

    Mutates ``states`` in place. Capacity is checked before token availability for
    COMPOSITE events; lineage is checked self-to-root. No deduction occurs unless
    every required resource on the full lineage is available.
    """

    results: list[dict[str, Any]] = []
    accepted_count = 0
    rejected_count = 0
    previous_time = last_event_ms
    aggregate_byte_sums = {
        tenant_id: CompensatedSum(state.aggregate_used_bytes_units)
        for tenant_id, state in states.items()
    }
    aggregate_request_sums = {
        tenant_id: CompensatedSum(state.aggregate_used_request_units)
        for tenant_id, state in states.items()
    }
    direct_byte_sums = {
        tenant_id: CompensatedSum(state.direct_used_bytes_units)
        for tenant_id, state in states.items()
    }
    direct_request_sums = {
        tenant_id: CompensatedSum(state.direct_used_request_units)
        for tenant_id, state in states.items()
    }

    for index, event in enumerate(events):
        if event.event_time_ms < previous_time:
            raise DomainError(
                "E_CLOCK_REWIND",
                "batch event time moved backwards",
                {
                    "event_index": index,
                    "event_time_ms": event.event_time_ms,
                    "previous_time_ms": previous_time,
                },
                409,
            )
        previous_time = event.event_time_ms
        chain = lineage(event.tenant_id, tenants)
        for account_id in chain:
            refill(states[account_id], tenants[account_id], event.event_time_ms)

        payload_units = to_units(event.payload_bytes, "payload_bytes")
        weight_units = to_units(event.request_weight, "request_weight")
        capacity_required = (
            payload_units if event.meter_mode in {MeterMode.BYTES, MeterMode.COMPOSITE} else 0
        )
        request_required = (
            weight_units
            if event.meter_mode in {MeterMode.REQUESTS, MeterMode.COMPOSITE}
            else 0
        )

        rejection: DomainError | None = None

        suspended_account = next(
            (account_id for account_id in chain if states[account_id].status == TenantStatus.SUSPENDED),
            None,
        )
        if suspended_account is not None:
            code = "E_TENANT_SUSPENDED" if suspended_account == event.tenant_id else "E_PARENT_SUSPENDED"
            rejection = DomainError(
                code,
                "a suspended tenant in the ancestry chain cannot admit events",
                {
                    "tenant_id": event.tenant_id,
                    "suspended_tenant_id": suspended_account,
                    "ancestor_chain": chain,
                },
                423,
            )

        if rejection is None and capacity_required > 0:
            for account_id in chain:
                account_state = states[account_id]
                account_tenant = tenants[account_id]
                remaining = max(
                    0,
                    account_tenant.capacity_units - account_state.aggregate_used_bytes_units,
                )
                if capacity_required > remaining:
                    difference = capacity_required - remaining
                    if account_id == event.tenant_id:
                        rejection = DomainError(
                            "E_QUOTA_EXCEEDED",
                            "tenant capacity quota is insufficient",
                            {
                                "tenant_id": event.tenant_id,
                                "resource": "capacity_bytes",
                                "required_bytes": units_to_str(capacity_required),
                                "required_units": capacity_required,
                                "remaining_bytes": units_to_str(remaining),
                                "remaining_units": remaining,
                                "shortage_bytes": units_to_str(difference),
                                "shortage_units": difference,
                            },
                            429,
                        )
                    else:
                        rejection = DomainError(
                            "E_PARENT_QUOTA_EXCEEDED",
                            "an ancestor capacity quota is insufficient",
                            {
                                "tenant_id": event.tenant_id,
                                "resource": "capacity_bytes",
                                "ancestor_chain": chain,
                                "insufficient_tenant_id": account_id,
                                "required_bytes": units_to_str(capacity_required),
                                "required_units": capacity_required,
                                "remaining_bytes": units_to_str(remaining),
                                "remaining_units": remaining,
                                "difference_bytes": units_to_str(difference),
                                "difference_units": difference,
                            },
                            429,
                        )
                    break

        if rejection is None and request_required > 0:
            for account_id in chain:
                account_state = states[account_id]
                account_tenant = tenants[account_id]
                if request_required > account_state.tokens_units:
                    shortage = request_required - account_state.tokens_units
                    available_at = next_available(account_state, account_tenant, request_required)
                    context = {
                        "tenant_id": event.tenant_id,
                        "resource": "request_tokens",
                        "ancestor_chain": chain,
                        "required_tokens": units_to_str(request_required),
                        "required_units": request_required,
                        "tokens_remaining": units_to_str(account_state.tokens_units),
                        "tokens_remaining_units": account_state.tokens_units,
                        "shortage_tokens": units_to_str(shortage),
                        "shortage_units": shortage,
                        "difference_tokens": units_to_str(shortage),
                        "difference_units": shortage,
                        "next_available_at_ms": available_at,
                        "insufficient_tenant_id": account_id,
                    }
                    if account_id == event.tenant_id:
                        rejection = DomainError(
                            "E_RATE_LIMITED",
                            "tenant token bucket is empty",
                            context,
                            429,
                        )
                    else:
                        rejection = DomainError(
                            "E_PARENT_QUOTA_EXCEEDED",
                            "an ancestor token bucket is empty",
                            context,
                            429,
                        )
                    break

        if rejection is None:
            accepted_count += 1
            if capacity_required:
                for account_id in chain:
                    aggregate_byte_sums[account_id].add(capacity_required)
                    states[account_id].aggregate_used_bytes_units = aggregate_byte_sums[
                        account_id
                    ].value
                direct_byte_sums[event.tenant_id].add(capacity_required)
                states[event.tenant_id].direct_used_bytes_units = direct_byte_sums[
                    event.tenant_id
                ].value
            if request_required:
                for account_id in chain:
                    account_state = states[account_id]
                    account_state.tokens_units -= request_required
                    aggregate_request_sums[account_id].add(request_required)
                    account_state.aggregate_used_request_units = aggregate_request_sums[
                        account_id
                    ].value
                direct_request_sums[event.tenant_id].add(request_required)
                states[event.tenant_id].direct_used_request_units = direct_request_sums[
                    event.tenant_id
                ].value
            states[event.tenant_id].consecutive_rejections = 0
            for account_id in chain:
                account_state = states[account_id]
                account_state.updated_at_ms = event.event_time_ms
                account_state.last_reason = None if account_id == event.tenant_id else account_state.last_reason
                _status_after_success(
                    account_state,
                    tenants[account_id],
                    capacity_charged=capacity_required > 0,
                )
            code = "OK"
            context: dict[str, Any] = {}
            status = "ACCEPTED"
        else:
            rejected_count += 1
            code = rejection.code
            context = dict(rejection.context)
            status = "REJECTED"
            states[event.tenant_id].last_reason = code
            states[event.tenant_id].updated_at_ms = event.event_time_ms
            insufficient = context.get("insufficient_tenant_id")
            if code in {"E_QUOTA_EXCEEDED", "E_PARENT_QUOTA_EXCEEDED"} and context.get(
                "resource"
            ) == "capacity_bytes":
                states[event.tenant_id].consecutive_rejections += 1
                _status_after_quota_rejection(states[event.tenant_id])
                if isinstance(insufficient, str) and insufficient in states:
                    _status_after_quota_rejection(states[insufficient])
                    states[insufficient].last_reason = code
                    states[insufficient].updated_at_ms = event.event_time_ms
            elif code in {"E_RATE_LIMITED", "E_PARENT_QUOTA_EXCEEDED"}:
                _status_after_rate_rejection(states[event.tenant_id], throttle_threshold)
                if (
                    isinstance(insufficient, str)
                    and insufficient in states
                    and insufficient != event.tenant_id
                ):
                    _status_after_rate_rejection(states[insufficient], throttle_threshold)
                    states[insufficient].last_reason = code
                    states[insufficient].updated_at_ms = event.event_time_ms
            elif code in {"E_TENANT_SUSPENDED", "E_PARENT_SUSPENDED"}:
                states[event.tenant_id].consecutive_rejections += 1

        public_lineage = _lineage_public(chain, states, tenants)
        result = {
            "index": index,
            "event_time_ms": event.event_time_ms,
            "tenant_id": event.tenant_id,
            "meter_mode": str(event.meter_mode),
            "io_direction": str(event.io_direction),
            "payload_bytes": units_to_str(payload_units),
            "request_weight": units_to_str(weight_units),
            "status": status,
            "code": code,
            "accepted": status == "ACCEPTED",
            "charged": {
                "capacity_bytes_units": capacity_required if status == "ACCEPTED" else 0,
                "capacity_bytes": units_to_str(capacity_required if status == "ACCEPTED" else 0),
                "request_tokens_units": request_required if status == "ACCEPTED" else 0,
                "request_tokens": units_to_str(request_required if status == "ACCEPTED" else 0),
            },
            "context": context,
            "tenant_status": str(states[event.tenant_id].status),
            "lineage": public_lineage,
        }
        results.append(result)

    summary = {
        "event_count": len(events),
        "accepted_count": accepted_count,
        "rejected_count": rejected_count,
        "first_event_ms": events[0].event_time_ms if events else None,
        "last_event_ms": events[-1].event_time_ms if events else None,
        "policy_version": None,
    }
    return results, summary
