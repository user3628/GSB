"""Pure deterministic sampling, deduplication, counters and bias estimation."""

from __future__ import annotations

import hashlib
import math
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable

from .policy import Policy

PENDING = "PENDING"
SAMPLED = "SAMPLED"
DROPPED = "DROPPED"
EMITTED = "EMITTED"

HEAD = "HEAD"
TAIL_RETAINED = "TAIL_RETAINED"
TAIL_PROMOTED = "TAIL_PROMOTED"
DEDUP_WINDOW = "DEDUP_WINDOW"

KIND_ORDER = {"span": 0, "log": 1}
Z_95 = 1.959963984540054


@dataclass(frozen=True)
class Record:
    record_uid: str
    batch_id: str
    kind: str
    source_index: int
    timestamp_ms: int
    service: str
    level: str
    trace_id: str
    span_id: str
    parent_span_id: str | None
    message_fingerprint: str | None
    latency_us: int
    error: bool
    sample_weight: float

    @property
    def order_key(self) -> tuple[int, int, str, int, str]:
        return (
            self.timestamp_ms,
            KIND_ORDER[self.kind],
            self.batch_id,
            self.source_index,
            self.record_uid,
        )


@dataclass
class Decision:
    record_uid: str
    policy_version: int
    state: str
    path: str
    reason: str
    head_sampled: bool
    tail_triggered: bool
    tail_sampled: bool
    dedup_leader_uid: str | None
    duplicate: bool
    inclusion_probability: float
    upstream_weight: float
    effective_weight: float
    state_history: list[str]
    transitions: list[dict[str, str]] = field(default_factory=list)

    def digest_line(self) -> str:
        return "|".join(
            (
                self.record_uid,
                str(self.policy_version),
                self.state,
                self.path,
                f"{self.inclusion_probability:.17g}",
                f"{self.effective_weight:.17g}",
            )
        )


@dataclass(frozen=True)
class DedupWindow:
    kind: str
    trace_id: str
    window_start_ms: int
    leader_uid: str
    member_uids: tuple[str, ...]


@dataclass
class KahanAccumulator:
    total: float = 0.0
    compensation: float = 0.0
    absolute_sum: float = 0.0
    count: int = 0

    def add(self, value: float) -> None:
        term = float(value)
        corrected = term - self.compensation
        updated = self.total + corrected
        self.compensation = (updated - self.total) - corrected
        self.total = updated
        self.absolute_sum += abs(term)
        self.count += 1

    @property
    def numeric_error_bound(self) -> float:
        machine_epsilon = sys.float_info.epsilon
        return 2.0 * machine_epsilon * self.absolute_sum + abs(self.compensation)


@dataclass
class StratumCounter:
    service: str
    level: str
    population_count: int = 0
    log_leaders: int = 0
    span_leaders: int = 0
    emitted_count: int = 0
    population_weight: KahanAccumulator = field(default_factory=KahanAccumulator)
    emitted_weight: KahanAccumulator = field(default_factory=KahanAccumulator)
    variance_sum: KahanAccumulator = field(default_factory=KahanAccumulator)
    max_inclusion_probability: float = 0.0


def unit_interval(*parts: object) -> float:
    material = "|".join(str(part) for part in parts).encode("utf-8")
    integer = int.from_bytes(hashlib.blake2b(material, digest_size=8).digest(), "big")
    return integer / float(1 << 64)


def decision_unit(policy: Policy, record: Record) -> float:
    return unit_interval(
        "head",
        policy.version,
        policy.seed,
        record.batch_id,
        record.kind,
        record.source_index,
    )


def tail_unit(policy: Policy, record: Record) -> float:
    return unit_interval("tail", policy.version, policy.seed, record.record_uid)


def dedup_key(record: Record, policy: Policy) -> tuple[str, str]:
    if policy.dedup_scope == "trace_per_kind":
        # Logs and spans are independent observation signals. The supplied demo
        # uses this mode so each side contributes its own deduplicated view.
        return (record.kind, record.trace_id)
    if policy.dedup_scope == "trace_global":
        # A stricter mode in which one canonical record wins for a trace window
        # across both logs and spans.
        return ("all", record.trace_id)
    raise AssertionError(f"unsupported dedup scope {policy.dedup_scope}")


def deduplicate(
    records: Iterable[Record], policy: Policy
) -> tuple[list[Record], dict[str, str], list[DedupWindow]]:
    """Return leaders, duplicate->leader map, and inclusive dedup windows.

    The window starts at the leader timestamp and ends at
    ``leader_timestamp_ms + dedup_window_ms``. Both boundaries are inclusive.
    Timestamps are integer milliseconds, so no floating-point rounding occurs.
    """

    leaders: list[Record] = []
    duplicate_to_leader: dict[str, str] = {}
    windows: list[dict[str, Any]] = []
    active: dict[tuple[str, str], dict[str, Any]] = {}

    for record in sorted(records, key=lambda item: item.order_key):
        key = dedup_key(record, policy)
        current = active.get(key)
        if current is not None:
            delta = record.timestamp_ms - current["window_start_ms"]
            if 0 <= delta <= policy.dedup_window_ms:
                duplicate_to_leader[record.record_uid] = current["leader_uid"]
                current["member_uids"].append(record.record_uid)
                continue

        current = {
            "kind": record.kind,
            "trace_id": record.trace_id,
            "window_start_ms": record.timestamp_ms,
            "leader_uid": record.record_uid,
            "member_uids": [record.record_uid],
        }
        active[key] = current
        windows.append(current)
        leaders.append(record)

    dedup_windows = [
        DedupWindow(
            kind=window["kind"],
            trace_id=window["trace_id"],
            window_start_ms=window["window_start_ms"],
            leader_uid=window["leader_uid"],
            member_uids=tuple(window["member_uids"]),
        )
        for window in windows
    ]
    return leaders, duplicate_to_leader, dedup_windows


def decide_records(records: Iterable[Record], policy: Policy) -> tuple[list[Decision], list[DedupWindow]]:
    ordered = sorted(records, key=lambda item: item.order_key)
    leaders, duplicate_to_leader, windows = deduplicate(ordered, policy)
    leader_by_uid = {record.record_uid: record for record in leaders}
    decisions: list[Decision] = []

    for record in ordered:
        leader_uid = duplicate_to_leader.get(record.record_uid)
        if leader_uid is not None:
            decisions.append(
                Decision(
                    record_uid=record.record_uid,
                    policy_version=policy.version,
                    state=DROPPED,
                    path=DEDUP_WINDOW,
                    reason="trace occurrence already counted inside inclusive dedup window",
                    head_sampled=False,
                    tail_triggered=False,
                    tail_sampled=False,
                    dedup_leader_uid=leader_uid,
                    duplicate=True,
                    inclusion_probability=0.0,
                    upstream_weight=record.sample_weight,
                    effective_weight=0.0,
                    state_history=[PENDING, DROPPED],
                    transitions=[
                        {"from": PENDING, "to": DROPPED, "path": DEDUP_WINDOW}
                    ],
                )
            )
            continue

        assert leader_by_uid[record.record_uid] is record
        decisions.append(_decide_leader(record, policy))

    # A record may theoretically be a leader in an older window and a duplicate
    # in a newer one; the duplicate map takes precedence because it points to a
    # still-active window. Leaders are guaranteed unique by UID.
    return decisions, windows


def _decide_leader(record: Record, policy: Policy) -> Decision:
    head_rate = policy.head_rate(record.service, record.level)
    head_sampled = decision_unit(policy, record) < head_rate

    history = [PENDING]
    transitions: list[dict[str, str]] = []
    if head_sampled:
        history.append(SAMPLED)
        transitions.append({"from": PENDING, "to": SAMPLED, "path": HEAD})
    else:
        history.append(DROPPED)
        transitions.append({"from": PENDING, "to": DROPPED, "path": HEAD})

    tail_triggered = False
    tail_sampled = False
    tail_probability = 0.0
    if record.kind == "span":
        latency_trigger = record.latency_us >= policy.tail.latency_threshold_us
        error_trigger = bool(record.error)
        tail_triggered = latency_trigger or error_trigger
        if tail_triggered:
            tail_probability = policy.tail.probability(error=error_trigger)
            tail_sampled = tail_unit(policy, record) < tail_probability

    if tail_triggered:
        inclusion_probability = head_rate + (1.0 - head_rate) * tail_probability
    else:
        inclusion_probability = head_rate
    inclusion_probability = min(1.0, max(0.0, inclusion_probability))

    if tail_triggered and tail_sampled:
        # Tail has conflict priority for classification and weight. It never
        # demotes a head-sampled record; it either retains it or promotes a
        # head-dropped one.
        state = EMITTED
        path = TAIL_RETAINED if head_sampled else TAIL_PROMOTED
        reason = "tail rule selected record with combined head+tail inclusion probability"
        history.append(EMITTED)
        transitions.append({"from": history[-2], "to": EMITTED, "path": path})
    elif head_sampled:
        state = EMITTED
        path = HEAD
        reason = "head stratum rate selected record"
        history.append(EMITTED)
        transitions.append({"from": SAMPLED, "to": EMITTED, "path": HEAD})
    else:
        state = DROPPED
        path = "HEAD_DROPPED"
        reason = "head stratum rate did not select record and tail did not promote it"

    if state == EMITTED:
        if policy.bias_correction:
            if inclusion_probability <= 0.0:
                # A zero-rate stratum cannot emit unless a tail rule makes it
                # eligible, in which case the combined probability is > 0.
                raise AssertionError("emitted record must have positive inclusion probability")
            effective_weight = record.sample_weight / inclusion_probability
        else:
            effective_weight = record.sample_weight
    else:
        effective_weight = 0.0

    return Decision(
        record_uid=record.record_uid,
        policy_version=policy.version,
        state=state,
        path=path,
        reason=reason,
        head_sampled=head_sampled,
        tail_triggered=tail_triggered,
        tail_sampled=tail_sampled,
        dedup_leader_uid=None,
        duplicate=False,
        inclusion_probability=inclusion_probability,
        upstream_weight=record.sample_weight,
        effective_weight=effective_weight,
        state_history=history,
        transitions=transitions,
    )


def aggregate(records: Iterable[Record], decisions: Iterable[Decision], policy: Policy) -> dict[tuple[str, str], StratumCounter]:
    record_by_uid = {record.record_uid: record for record in records}
    counters: dict[tuple[str, str], StratumCounter] = defaultdict(
        lambda: StratumCounter(service="", level="")
    )
    for decision in decisions:
        if decision.duplicate:
            continue
        record = record_by_uid[decision.record_uid]
        key = (record.service, record.level)
        counter = counters[key]
        counter.service = record.service
        counter.level = record.level
        counter.population_count += 1
        if record.kind == "span":
            counter.span_leaders += 1
        else:
            counter.log_leaders += 1
        counter.population_weight.add(record.sample_weight)
        counter.max_inclusion_probability = max(
            counter.max_inclusion_probability, decision.inclusion_probability
        )
        if decision.state == EMITTED:
            counter.emitted_count += 1
            counter.emitted_weight.add(decision.effective_weight)
            p = decision.inclusion_probability
            if p > 0.0:
                variance = ((1.0 - p) / (p * p)) * (record.sample_weight**2)
                counter.variance_sum.add(variance)
    return dict(counters)


def build_report_rows(
    counters: dict[tuple[str, str], StratumCounter], policy: Policy
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (service, level), counter in sorted(counters.items()):
        population_weight = counter.population_weight.total
        insufficient_reason: str | None = None
        if counter.population_count == 0:
            insufficient_reason = "NO_POPULATION"
        elif counter.max_inclusion_probability <= 0.0 and counter.emitted_count == 0:
            insufficient_reason = "UNSAMPLED_STRATUM"
        elif counter.emitted_count < policy.min_samples:
            insufficient_reason = "SAMPLE_SIZE_BELOW_MINIMUM"

        if insufficient_reason is not None:
            weighted_total = None
            absolute_bias = None
            relative_bias = None
            standard_error = None
            confidence_95_bound = None
            relative_error_bound = None
            within_error_bound = None
            status = "INSUFFICIENT"
        else:
            weighted_total = counter.emitted_weight.total
            absolute_bias = weighted_total - population_weight
            relative_bias = (
                absolute_bias / population_weight if population_weight != 0.0 else None
            )
            variance_sum = max(0.0, counter.variance_sum.total)
            standard_error = math.sqrt(variance_sum)
            confidence_95_bound = Z_95 * standard_error
            relative_error_bound = (
                confidence_95_bound / population_weight if population_weight != 0.0 else None
            )
            within_error_bound = (
                relative_error_bound is not None
                and abs(relative_bias or 0.0) <= policy.error_bound
                and relative_error_bound <= policy.error_bound
            )
            status = "OK" if within_error_bound else "ERROR_BOUND_EXCEEDED"

        rows.append(
            {
                "service": service,
                "level": level,
                "head_rate": policy.head_rate(service, level),
                "population_count": counter.population_count,
                "log_leaders": counter.log_leaders,
                "span_leaders": counter.span_leaders,
                "emitted_count": counter.emitted_count,
                "population_weight": population_weight,
                "population_weight_compensation": counter.population_weight.compensation,
                "weighted_total": weighted_total,
                "weighted_total_compensation": counter.emitted_weight.compensation,
                "numeric_error_bound": (
                    counter.emitted_weight.numeric_error_bound
                    + counter.population_weight.numeric_error_bound
                ),
                "absolute_bias": absolute_bias,
                "relative_bias": relative_bias,
                "variance_sum": counter.variance_sum.total,
                "standard_error": standard_error,
                "confidence_95_bound": confidence_95_bound,
                "relative_error_bound": relative_error_bound,
                "configured_error_bound": policy.error_bound,
                "within_error_bound": within_error_bound,
                "min_samples": policy.min_samples,
                "max_inclusion_probability": counter.max_inclusion_probability,
                "insufficient_reason": insufficient_reason,
                "status": status,
            }
        )
    return rows


def decision_digest(decisions: Iterable[Decision]) -> str:
    digest = hashlib.sha256()
    for decision in sorted(decisions, key=lambda item: item.record_uid):
        digest.update(decision.digest_line().encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def counts_digest(rows: Iterable[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for row in sorted(rows, key=lambda item: (item["service"], item["level"])):
        material = "|".join(
            (
                row["service"],
                row["level"],
                str(row["population_count"]),
                str(row["emitted_count"]),
                f"{row['population_weight']:.17g}",
                "NULL" if row["weighted_total"] is None else f"{row['weighted_total']:.17g}",
                str(row["status"]),
            )
        )
        digest.update(material.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()
