"""Deterministic multi-window SLI, error-budget and burn-rate evaluation."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable

from .errors import SloError
from .events import EventWindowCounts, count_events_in_window
from .models import RequestEvent, WindowRecord, sorted_records
from .policy import BoundaryPolicy, CalculationPolicy, fixed_decimal


@dataclass(frozen=True)
class WindowRollup:
    window_ms: int
    window_start_ms: int
    window_end_ms: int
    good_events: int
    total_events: int
    late_count: int
    record_count: int
    coverage_ms: int
    source: str
    source_snapshot_sequences: tuple[int, ...]
    bucket_sli: tuple[Decimal, ...]
    expired_late_events: int = 0

    @property
    def bad_events(self) -> int:
        return self.total_events - self.good_events

    @property
    def complete_coverage(self) -> bool:
        return self.coverage_ms >= self.window_ms

    @property
    def zero_total(self) -> bool:
        return self.total_events == 0

    @property
    def sli(self) -> Decimal:
        if self.total_events == 0:
            return Decimal(1)
        return Decimal(self.good_events) / Decimal(self.total_events)

    @property
    def bad_event_ratio(self) -> Decimal:
        if self.total_events == 0:
            return Decimal(0)
        return Decimal(self.bad_events) / Decimal(self.total_events)

    def burn_rate(self, slo_target: Decimal) -> Decimal:
        if self.total_events == 0:
            return Decimal(0)
        return self.bad_event_ratio / (Decimal(1) - slo_target)


def _validate_counts(*, good: int, total: int, late: int, context: dict[str, Any]) -> None:
    if good < 0 or total < 0 or late < 0:
        raise SloError(
            "E_COUNT_INVALID",
            "event counts must be non-negative",
            details={**context, "good": good, "total": total, "late": late},
        )
    if good > total:
        raise SloError(
            "E_COUNT_INVALID",
            "good_events must not exceed total_events",
            details={**context, "good_events": good, "total_events": total},
        )
    if late > total:
        raise SloError(
            "E_COUNT_INVALID",
            "late_count must not exceed total_events",
            details={**context, "late_count": late, "total_events": total},
        )


def _decimal_quantile(values: list[Decimal], q: Decimal) -> Decimal:
    if not values:
        return Decimal(1)
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = Decimal(len(ordered) - 1) * q
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    fraction = position - Decimal(lower_index)
    return ordered[lower_index] + (ordered[upper_index] - ordered[lower_index]) * fraction


def quantiles(
    values: Iterable[Decimal],
    *,
    places: int,
) -> dict[str, str]:
    materialized = list(values)
    return {
        name: fixed_decimal(_decimal_quantile(materialized, q), places)
        for name, q in (
            ("p50", Decimal("0.50")),
            ("p95", Decimal("0.95")),
            ("p99", Decimal("0.99")),
        )
    }


def rollup_records(
    records: Iterable[WindowRecord],
    *,
    service: str,
    sli_type: str,
    window_ms: int,
    as_of_ms: int,
) -> WindowRollup:
    window_start = as_of_ms - window_ms
    selected: list[WindowRecord] = []
    for record in sorted_records(records):
        if record.service != service or record.sli_type != sli_type:
            continue
        if record.window_start_ms >= window_start and record.window_end_ms <= as_of_ms:
            selected.append(record)
    if not selected:
        return WindowRollup(
            window_ms=window_ms,
            window_start_ms=window_start,
            window_end_ms=as_of_ms,
            good_events=0,
            total_events=0,
            late_count=0,
            record_count=0,
            coverage_ms=0,
            source="snapshot",
            source_snapshot_sequences=(),
            bucket_sli=(),
        )

    previous_end: int | None = None
    coverage_intervals: list[tuple[int, int]] = []
    for record in selected:
        if previous_end is not None and record.window_start_ms < previous_end:
            raise SloError(
                "E_INCONSISTENT_READINGS",
                "overlapping snapshot windows cannot be attributed deterministically",
                details={
                    "service": service,
                    "sli_type": sli_type,
                    "window_ms": window_ms,
                    "overlap": {
                        "previous_end_ms": previous_end,
                        "next_start_ms": record.window_start_ms,
                        "next_offset": record.source_offset,
                    },
                },
            )
        previous_end = max(previous_end or record.window_end_ms, record.window_end_ms)
        clipped_start = max(record.window_start_ms, window_start)
        clipped_end = min(record.window_end_ms, as_of_ms)
        if clipped_end > clipped_start:
            coverage_intervals.append((clipped_start, clipped_end))

    good = sum(record.good_events for record in selected)
    total = sum(record.total_events for record in selected)
    late = sum(record.late_count for record in selected)
    _validate_counts(
        good=good,
        total=total,
        late=late,
        context={"service": service, "sli_type": sli_type, "window_ms": window_ms},
    )
    merged: list[list[int]] = []
    for start, end in sorted(coverage_intervals):
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)
    coverage_ms = sum(end - start for start, end in merged)
    bucket_sli = tuple(
        Decimal(record.good_events) / Decimal(record.total_events)
        if record.total_events
        else Decimal(1)
        for record in selected
    )
    sequences = tuple(sorted({record.snapshot_sequence for record in selected if record.snapshot_sequence}))
    return WindowRollup(
        window_ms=window_ms,
        window_start_ms=window_start,
        window_end_ms=as_of_ms,
        good_events=good,
        total_events=total,
        late_count=late,
        record_count=len(selected),
        coverage_ms=coverage_ms,
        source="snapshot",
        source_snapshot_sequences=sequences,
        bucket_sli=bucket_sli,
    )


def rollup_events(
    events: Iterable[RequestEvent],
    *,
    service: str,
    sli_type: str,
    window_ms: int,
    as_of_ms: int,
    policy: CalculationPolicy,
    boundaries: BoundaryPolicy,
) -> WindowRollup:
    window_start = as_of_ms - window_ms
    counts = count_events_in_window(
        events,
        service=service,
        sli_type=sli_type,
        window_start_ms=window_start,
        window_end_ms=as_of_ms,
        as_of_ms=as_of_ms,
        policy=policy,
        boundaries=boundaries,
    )
    _validate_counts(
        good=counts.good_events,
        total=counts.total_events,
        late=counts.late_count,
        context={"service": service, "sli_type": sli_type, "window_ms": window_ms},
    )
    bucket_ms = min(policy.window_lengths_ms)
    buckets: dict[int, list[int]] = {}
    for event in events:
        if event.service != service or event.sli_type != sli_type:
            continue
        if not (window_start <= event.timestamp_ms < as_of_ms):
            continue
        if event.late and not policy.include_late_events:
            continue
        bucket_start = (event.timestamp_ms // bucket_ms) * bucket_ms
        counter = buckets.setdefault(bucket_start, [0, 0])
        counter[0] += 1
        if event.good:
            counter[1] += 1
    bucket_sli = tuple(
        Decimal(counter[1]) / Decimal(counter[0])
        for _bucket_start, counter in sorted(buckets.items())
    )
    return WindowRollup(
        window_ms=window_ms,
        window_start_ms=window_start,
        window_end_ms=as_of_ms,
        good_events=counts.good_events,
        total_events=counts.total_events,
        late_count=counts.late_count,
        record_count=counts.total_events,
        coverage_ms=window_ms,
        source="events",
        source_snapshot_sequences=(),
        bucket_sli=bucket_sli,
        expired_late_events=counts.expired_late_events,
    )


def _window_payload(
    rollup: WindowRollup,
    *,
    policy: CalculationPolicy,
    boundaries: BoundaryPolicy,
    source: str,
) -> tuple[dict[str, Any], Decimal, bool]:
    good = rollup.good_events
    total = rollup.total_events
    late = rollup.late_count
    if total == 0:
        sli = Decimal(1)
        bad_ratio = Decimal(0)
        burn = Decimal(0)
        allowed_bad_events = Decimal(0)
    else:
        sli = Decimal(good) / Decimal(total)
        bad_ratio = Decimal(total - good) / Decimal(total)
        budget_fraction = Decimal(1) - policy.slo_target
        burn = bad_ratio / budget_fraction
        allowed_bad_events = Decimal(total) * budget_fraction
    sufficient = total >= policy.min_samples
    samples_code = None if sufficient else "E_INSUFFICIENT_SAMPLES"
    sample_status = "OK" if sufficient else ("NO_DATA" if total == 0 else "INSUFFICIENT")
    payload = {
        "window_ms": rollup.window_ms,
        "window_start_ms": rollup.window_start_ms,
        "window_end_ms": rollup.window_end_ms,
        "source": source,
        "record_count": rollup.record_count,
        "coverage_ms": rollup.coverage_ms,
        "coverage_complete": rollup.complete_coverage,
        "counts": {
            "good_events": good,
            "bad_events": total - good,
            "total_events": total,
            "late_count": late,
            "expired_late_events": rollup.expired_late_events,
        },
        "sample_gate": {
            "minimum": policy.min_samples,
            "observed": total,
            "passed": sufficient,
            "status": sample_status,
            "business_code": samples_code,
        },
        "sli": fixed_decimal(sli, policy.fixed_places),
        "bad_event_ratio": fixed_decimal(bad_ratio, policy.fixed_places),
        "error_budget": {
            "slo_target": fixed_decimal(policy.slo_target, policy.fixed_places),
            "allowed_bad_events": fixed_decimal(allowed_bad_events, policy.fixed_places),
            "burn_rate": fixed_decimal(burn, policy.fixed_places),
            "consumed_ratio": fixed_decimal(burn, policy.fixed_places),
            "remaining_ratio": fixed_decimal(Decimal(1) - burn, policy.fixed_places),
        },
        "bucket_sli_quantiles": {
            **quantiles(rollup.bucket_sli, places=policy.fixed_places),
            "method": "linear_interpolation",
            "rounding_mode": policy.rounding_mode,
        },
        "presentation": {
            "fixed_places": policy.fixed_places,
            "rounding_mode": policy.rounding_mode,
            "zero_total_rule": "sli=1, burn_rate=0, sample_status=NO_DATA",
        },
        "boundary_policy": boundaries.to_dict(),
    }
    return payload, burn, sufficient


def _event_counts_payload(counts: EventWindowCounts) -> dict[str, Any]:
    return {
        "good_events": counts.good_events,
        "bad_events": counts.bad_events,
        "total_events": counts.total_events,
        "late_count": counts.late_count,
        "expired_late_events": counts.expired_late_events,
    }


def calculate(
    records: Iterable[WindowRecord],
    *,
    service: str,
    sli_type: str,
    as_of_ms: int,
    policy: CalculationPolicy | None = None,
    boundaries: BoundaryPolicy | None = None,
    events: Iterable[RequestEvent] | None = None,
) -> dict[str, Any]:
    policy = policy or CalculationPolicy()
    boundaries = boundaries or BoundaryPolicy()
    policy.validate()
    boundaries.validate()
    if isinstance(as_of_ms, bool) or not isinstance(as_of_ms, int) or as_of_ms < 0:
        raise SloError(
            "E_REQUEST_INVALID",
            "as_of_ms must be a non-negative integer",
            details={"as_of_ms": as_of_ms},
        )
    record_materialized = tuple(sorted_records(records))
    event_materialized = tuple(events) if events is not None else None

    rollups: list[WindowRollup] = []
    windows: list[dict[str, Any]] = []
    burns: dict[int, Decimal] = {}
    sufficiency: dict[int, bool] = {}
    consistency_checks: list[dict[str, Any]] = []

    for window_ms in policy.window_lengths_ms:
        rollup = rollup_records(
            record_materialized,
            service=service,
            sli_type=sli_type,
            window_ms=window_ms,
            as_of_ms=as_of_ms,
        )
        if policy.require_complete_coverage and rollup.total_events > 0 and not rollup.complete_coverage:
            raise SloError(
                "E_INCONSISTENT_READINGS",
                "snapshot coverage is incomplete and cannot be attributed",
                details={
                    "service": service,
                    "sli_type": sli_type,
                    "window_ms": window_ms,
                    "window_start_ms": rollup.window_start_ms,
                    "window_end_ms": rollup.window_end_ms,
                    "coverage_ms": rollup.coverage_ms,
                    "missing_ms": window_ms - rollup.coverage_ms,
                },
            )
        payload, burn, sufficient = _window_payload(
            rollup,
            policy=policy,
            boundaries=boundaries,
            source="snapshot",
        )
        if event_materialized is not None:
            event_rollup = rollup_events(
                event_materialized,
                service=service,
                sli_type=sli_type,
                window_ms=window_ms,
                as_of_ms=as_of_ms,
                policy=policy,
                boundaries=boundaries,
            )
            delta_total = event_rollup.total_events - rollup.total_events
            delta_good = event_rollup.good_events - rollup.good_events
            if delta_total == 0 and delta_good == 0:
                attribution = "exact_match"
            elif (
                abs(delta_total) <= rollup.late_count
                and abs(delta_good) <= rollup.late_count
            ):
                attribution = "late_event_policy"
            else:
                raise SloError(
                    "E_INCONSISTENT_READINGS",
                    "event and snapshot readings differ without a deterministic attribution",
                    details={
                        "service": service,
                        "sli_type": sli_type,
                        "window_ms": window_ms,
                        "snapshot": {
                            "good_events": rollup.good_events,
                            "total_events": rollup.total_events,
                            "late_count": rollup.late_count,
                        },
                        "events": _event_counts_payload(
                            EventWindowCounts(
                                good_events=event_rollup.good_events,
                                total_events=event_rollup.total_events,
                                late_count=event_rollup.late_count,
                                expired_late_events=event_rollup.expired_late_events,
                            )
                        ),
                        "delta": {"good_events": delta_good, "total_events": delta_total},
                        "max_attributable_late": rollup.late_count,
                    },
                )
            consistency_checks.append(
                {
                    "window_ms": window_ms,
                    "attribution": attribution,
                    "snapshot": {
                        "good_events": rollup.good_events,
                        "total_events": rollup.total_events,
                    },
                    "events": {
                        "good_events": event_rollup.good_events,
                        "total_events": event_rollup.total_events,
                    },
                    "delta": {"good_events": delta_good, "total_events": delta_total},
                }
            )
        rollups.append(rollup)
        windows.append(payload)
        burns[window_ms] = burn
        sufficiency[window_ms] = sufficient

    short = next(item for item in rollups if item.window_ms == policy.short_window_ms)
    long = next(item for item in rollups if item.window_ms == policy.long_window_ms)
    short_burn = burns[policy.short_window_ms]
    long_burn = burns[policy.long_window_ms]
    short_sufficient = sufficiency[policy.short_window_ms]
    long_sufficient = sufficiency[policy.long_window_ms]

    if not short_sufficient or not long_sufficient:
        alert_grade = "UNKNOWN"
        alert_code = "E_INSUFFICIENT_SAMPLES"
        trigger = None
        insufficient_windows = [
            item.window_ms
            for item in rollups
            if not sufficiency[item.window_ms]
        ]
    else:
        alert_grade = "OK"
        alert_code = None
        trigger = None
        insufficient_windows = []
        for name, threshold in policy.thresholds:
            if short_burn >= threshold and long_burn >= threshold:
                alert_grade = name
                trigger = threshold
                break

    baseline = policy.thresholds[-1][1]
    short_above = short_burn >= baseline
    long_above = long_burn >= baseline
    if short_above and long_above:
        divergence_class = "CONSISTENT_ABOVE"
        attribution = "both_windows_show_the_same_direction"
    elif short_above and not long_above:
        divergence_class = "SHORT_ONLY"
        attribution = "recent_regression_inside_short_window"
    elif long_above and not short_above:
        divergence_class = "LONG_ONLY"
        attribution = "older_errors_remain_inside_long_window"
    else:
        divergence_class = "CONSISTENT_BELOW"
        attribution = "neither_window_crosses_the_lowest_threshold"

    source_sequences = sorted(
        {sequence for rollup in rollups for sequence in rollup.source_snapshot_sequences}
    )
    if len(source_sequences) > 1:
        raise SloError(
            "E_INCONSISTENT_READINGS",
            "one query mixes records from multiple snapshot sequences",
            details={
                "service": service,
                "sli_type": sli_type,
                "snapshot_sequences": source_sequences,
            },
        )

    result: dict[str, Any] = {
        "ok": alert_code is None,
        "business_code": alert_code,
        "query": {
            "service": service,
            "sli_type": sli_type,
            "as_of_ms": as_of_ms,
        },
        "policy": policy.to_dict(),
        "boundary_policy": boundaries.to_dict(),
        "windows": windows,
        "alert": {
            "grade": alert_grade,
            "business_code": alert_code,
            "requires_both_windows": True,
            "pair": {
                "short_window_ms": policy.short_window_ms,
                "long_window_ms": policy.long_window_ms,
                "short_burn_rate": fixed_decimal(short_burn, policy.fixed_places),
                "long_burn_rate": fixed_decimal(long_burn, policy.fixed_places),
                "trigger_threshold": (
                    fixed_decimal(trigger, policy.fixed_places) if trigger is not None else None
                ),
            },
            "insufficient_windows": insufficient_windows,
            "divergence": {
                "class": divergence_class,
                "attribution": attribution,
                "short_above_lowest_threshold": short_above,
                "long_above_lowest_threshold": long_above,
                "delta_burn_rate": fixed_decimal(long_burn - short_burn, policy.fixed_places),
            },
        },
        "consistency": {
            "snapshot_sequences": source_sequences,
            "cross_source": consistency_checks if event_materialized is not None else None,
            "unattributed_difference": False,
        },
        "presentation": {
            "fixed_places": policy.fixed_places,
            "rounding_mode": policy.rounding_mode,
            "decimal_output": "JSON strings with fixed fractional places",
        },
    }
    return result
