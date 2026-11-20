from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.models import PolicySnapshot, TraceRequest, TraceSample


DECISION_TYPES = (
    "SUCCESS",
    "RATE_LIMITED",
    "CIRCUIT_OPEN",
    "TIMEOUT",
    "UPSTREAM_ERROR",
    "UPSTREAM_CONNECTION_ERROR",
)

DIFFERENCE_WEIGHTS = {
    "TIMEOUT_BREAKER_CONFLICT": 10,
    "CIRCUIT_FREQUENT_TRANSITION": 8,
    "RATE_LIMIT_HIGH_REJECTION": 7,
    "ROUTE_MATCH_AMBIGUITY": 5,
}

ROUTE_RESOLUTION_STRATEGY = (
    "priority_desc,path_prefix_length_desc,required_header_count_desc,rule_id_asc"
)


@dataclass
class BreakerState:
    state: str = "CLOSED"
    consecutive_failures: int = 0
    open_until_ms: int | None = None
    transitions: int = 0
    events: list[dict[str, Any]] = field(default_factory=list)
    affected_request_ids: set[str] = field(default_factory=set)


def _candidate_view(rule: Any) -> dict[str, Any]:
    precedence = (rule.priority, len(rule.path_prefix), len(rule.headers))
    return {
        "rule_id": rule.rule_id,
        "route_id": rule.route_id,
        "priority": rule.priority,
        "path_prefix_length": len(rule.path_prefix),
        "required_header_count": len(rule.headers),
        "precedence": list(precedence),
    }


def match_route(
    snapshot: PolicySnapshot,
    path: str,
    headers: dict[str, str],
) -> dict[str, Any]:
    """Resolve one route with a total, integer-only precedence order."""

    normalized_headers = {key.lower(): value for key, value in headers.items()}
    candidates = []
    for rule in snapshot.virtual_service.routes:
        if not path.startswith(rule.path_prefix):
            continue
        if any(normalized_headers.get(key) != value for key, value in rule.headers.items()):
            continue
        candidates.append(rule)

    candidates.sort(
        key=lambda rule: (
            -rule.priority,
            -len(rule.path_prefix),
            -len(rule.headers),
            rule.rule_id,
        )
    )

    if not candidates:
        return {
            "route_id": snapshot.virtual_service.default_route,
            "is_default_route": True,
            "matched_rule_ids": [],
            "match_count": 0,
            "winning_rule_id": None,
            "tied_at_top_precedence": False,
            "strategy": ROUTE_RESOLUTION_STRATEGY,
            "candidates": [],
        }

    winner = candidates[0]
    winner_precedence = (winner.priority, len(winner.path_prefix), len(winner.headers))
    top_ties = [
        rule
        for rule in candidates
        if (rule.priority, len(rule.path_prefix), len(rule.headers)) == winner_precedence
    ]
    return {
        "route_id": winner.route_id,
        "is_default_route": False,
        "matched_rule_ids": [rule.rule_id for rule in candidates],
        "match_count": len(candidates),
        "winning_rule_id": winner.rule_id,
        "tied_at_top_precedence": len(top_ties) > 1,
        "strategy": ROUTE_RESOLUTION_STRATEGY,
        "candidates": [_candidate_view(rule) for rule in candidates],
    }


def detect_route_conflicts(snapshot: PolicySnapshot) -> list[dict[str, Any]]:
    conflicts = []
    for route_id in sorted(snapshot.target_rules):
        target = snapshot.target_rules[route_id]
        if target.timeout_ms > target.circuit_breaker.window_ms:
            conflicts.append(
                {
                    "route_id": route_id,
                    "difference_type": "TIMEOUT_BREAKER_CONFLICT",
                    "timeout_ms": target.timeout_ms,
                    "breaker_window_ms": target.circuit_breaker.window_ms,
                    "condition": "timeout_ms > circuit_breaker.window_ms",
                    "over_by_ms": target.timeout_ms - target.circuit_breaker.window_ms,
                }
            )
    return conflicts


def _breaker_failure_reason(request: TraceRequest, timeout_ms: int) -> str | None:
    if request.upstream.connection_error:
        return "CONNECTION_ERROR"
    if request.upstream.latency_ms > timeout_ms:
        return "TIMEOUT"
    if request.upstream.status_code >= 500:
        return "UPSTREAM_5XX"
    return None


def _transition(
    breaker: BreakerState,
    new_state: str,
    *,
    timestamp_ms: int,
    request_id: str,
    reason: str,
) -> dict[str, Any]:
    previous = breaker.state
    breaker.state = new_state
    breaker.transitions += 1
    breaker.affected_request_ids.add(request_id)
    event = {
        "timestamp_ms": timestamp_ms,
        "request_id": request_id,
        "from": previous,
        "to": new_state,
        "reason": reason,
    }
    breaker.events.append(event)
    return event


def _priority_band(score: int) -> str:
    if score >= 50_000:
        return "P1"
    if score >= 20_000:
        return "P2"
    return "P3"


def _build_orders(
    decisions: list[dict[str, Any]],
    conflicts: list[dict[str, Any]],
    route_match_counts: dict[str, int],
    breaker_states: dict[str, BreakerState],
    total_requests: int,
) -> list[dict[str, Any]]:
    aggregate: dict[tuple[str, str], dict[str, Any]] = {}

    def add(
        route_id: str,
        difference_type: str,
        trigger_condition: str,
        affected_requests: int,
        evidence: dict[str, Any],
    ) -> None:
        key = (route_id, difference_type)
        if key in aggregate:
            return
        ratio_bp = (affected_requests * 10_000) // total_requests if total_requests else 0
        weight = DIFFERENCE_WEIGHTS[difference_type]
        score = ratio_bp * weight
        aggregate[key] = {
            "route_id": route_id,
            "difference_type": difference_type,
            "trigger_condition": trigger_condition,
            "affected_requests": affected_requests,
            "total_requests": total_requests,
            "affected_ratio_bp": ratio_bp,
            "type_weight": weight,
            "priority_score": score,
            "priority": _priority_band(score),
            "metrics": evidence,
        }

    rate_rejections: dict[str, int] = {}
    rate_rejection_ids: dict[str, list[str]] = {}
    rate_rejection_refs: dict[str, list[dict[str, Any]]] = {}
    ambiguity_requests: dict[str, int] = {}
    ambiguity_request_ids: dict[str, list[str]] = {}
    ambiguity_evidence: dict[str, dict[str, Any]] = {}
    route_request_ids: dict[str, list[str]] = {}
    route_decision_refs: dict[str, list[dict[str, Any]]] = {}
    for decision in decisions:
        route_id = decision["route_id"]
        route_request_ids.setdefault(route_id, []).append(decision["request_id"])
        route_decision_refs.setdefault(route_id, []).append(
            {
                "request_id": decision["request_id"],
                "decision_type": decision["decision_type"],
                "response_status": decision["response_status"],
                "trigger_parameters": decision["trigger_parameters"],
            }
        )
        if decision["decision_type"] == "RATE_LIMITED":
            rate_rejections[route_id] = rate_rejections.get(route_id, 0) + 1
            rate_rejection_ids.setdefault(route_id, []).append(decision["request_id"])
            rate_rejection_refs.setdefault(route_id, []).append(
                {
                    "request_id": decision["request_id"],
                    "trigger_parameters": decision["trigger_parameters"],
                }
            )
        resolution = decision["route_resolution"]
        if decision["match_count"] > 1 and resolution["tied_at_top_precedence"]:
            ambiguity_requests[route_id] = ambiguity_requests.get(route_id, 0) + 1
            ambiguity_request_ids.setdefault(route_id, []).append(decision["request_id"])
            entry = ambiguity_evidence.setdefault(
                route_id,
                {
                    "winner": resolution["winning_rule_id"],
                    "strategy": resolution["strategy"],
                    "candidate_sets": [],
                },
            )
            candidate_set = {
                "matched_rule_ids": decision["matched_rule_ids"],
                "candidates": resolution["candidates"],
            }
            if candidate_set not in entry["candidate_sets"]:
                entry["candidate_sets"].append(candidate_set)

    for route_id, rejected in sorted(rate_rejections.items()):
        matched = route_match_counts.get(route_id, 0)
        if rejected * 5 >= matched:
            add(
                route_id,
                "RATE_LIMIT_HIGH_REJECTION",
                "rate_limited_requests / route_matched_requests >= 20%",
                rejected,
                {
                    "rate_limited_requests": rejected,
                    "route_matched_requests": matched,
                    "route_rejection_ratio_bp": (rejected * 10_000) // matched,
                    "affected_request_ids": rate_rejection_ids[route_id],
                    "decision_refs": rate_rejection_refs[route_id],
                },
            )

    for route_id, breaker in sorted(breaker_states.items()):
        if breaker.transitions >= 2:
            affected = len(breaker.affected_request_ids)
            add(
                route_id,
                "CIRCUIT_FREQUENT_TRANSITION",
                "breaker_transitions >= 2",
                affected,
                {
                    "breaker_transitions": breaker.transitions,
                    "affected_request_ids": sorted(breaker.affected_request_ids),
                    "transition_events": breaker.events,
                },
            )

    conflict_evidence: dict[str, list[dict[str, Any]]] = {}
    for conflict in conflicts:
        conflict_evidence.setdefault(conflict["route_id"], []).append(conflict)
    for route_id, route_conflicts in sorted(conflict_evidence.items()):
        affected = route_match_counts.get(route_id, 0)
        add(
            route_id,
            "TIMEOUT_BREAKER_CONFLICT",
            "timeout_ms > circuit_breaker.window_ms",
            affected,
            {
                "route_matched_requests": affected,
                "affected_request_ids": route_request_ids.get(route_id, []),
                "conflicts": route_conflicts,
                "decision_refs": route_decision_refs.get(route_id, []),
            },
        )

    for route_id, affected in sorted(ambiguity_requests.items()):
        add(
            route_id,
            "ROUTE_MATCH_AMBIGUITY",
            "multiple route rules tie at top precedence; rule_id resolves deterministically",
            affected,
            {
                **ambiguity_evidence[route_id],
                "affected_request_ids": ambiguity_request_ids[route_id],
            },
        )

    ordered = sorted(
        aggregate.values(),
        key=lambda order: (
            -order["priority_score"],
            -order["type_weight"],
            -order["affected_requests"],
            order["route_id"],
            order["difference_type"],
        ),
    )
    for index, order in enumerate(ordered, start=1):
        order["rank"] = index
    return ordered


def simulate(
    snapshot: PolicySnapshot,
    trace: TraceSample,
    *,
    detect_conflicts: bool = True,
) -> dict[str, Any]:
    """Run a deterministic, offline simulation for one immutable snapshot."""

    requests = sorted(trace.requests, key=lambda item: (item.timestamp_ms, item.request_id))
    conflicts = detect_route_conflicts(snapshot) if detect_conflicts else []
    conflicts_by_route: dict[str, list[dict[str, Any]]] = {}
    for conflict in conflicts:
        conflicts_by_route.setdefault(conflict["route_id"], []).append(conflict)

    rate_counts: dict[tuple[str, int], int] = {}
    breaker_states: dict[str, BreakerState] = {}
    route_match_counts: dict[str, int] = {}
    by_decision_type = {name: 0 for name in DECISION_TYPES}
    by_route: dict[str, int] = {}
    decisions: list[dict[str, Any]] = []

    for sequence_no, request in enumerate(requests):
        resolution = match_route(snapshot, request.path, request.headers)
        route_id = resolution["route_id"]
        target = snapshot.target_rules[route_id]
        breaker = breaker_states.setdefault(route_id, BreakerState())
        route_match_counts[route_id] = route_match_counts.get(route_id, 0) + 1

        rate_rule = target.rate_limit
        window_start_ms = (request.timestamp_ms // rate_rule.window_ms) * rate_rule.window_ms
        window_end_ms = window_start_ms + rate_rule.window_ms
        rate_key = (route_id, window_start_ms)
        rate_counts[rate_key] = rate_counts.get(rate_key, 0) + 1
        observed_count = rate_counts[rate_key]
        rate_info = {
            "limit_threshold": rate_rule.threshold,
            "window_ms": rate_rule.window_ms,
            "window_start_ms": window_start_ms,
            "window_end_ms": window_end_ms,
            "window_interval": "[window_start_ms, window_end_ms)",
            "observed_count": observed_count,
            "exceeded": observed_count > rate_rule.threshold,
        }

        state_before = breaker.state
        consecutive_failures_before = breaker.consecutive_failures
        probe = False
        upstream_attempted = False
        breaker_failure = False

        if observed_count > rate_rule.threshold:
            decision_type = "RATE_LIMITED"
            response_status = 429
            response_latency_ms = 0
            trigger_parameters = {
                "limit_threshold": rate_rule.threshold,
                "observed_count": observed_count,
                "window_ms": rate_rule.window_ms,
                "window_start_ms": window_start_ms,
                "window_end_ms": window_end_ms,
            }
        elif breaker.state == "OPEN" and breaker.open_until_ms is not None and request.timestamp_ms < breaker.open_until_ms:
            decision_type = "CIRCUIT_OPEN"
            response_status = 503
            response_latency_ms = 0
            breaker.affected_request_ids.add(request.request_id)
            trigger_parameters = {
                "breaker_state": "OPEN",
                "consecutive_failures": breaker.consecutive_failures,
                "consecutive_failure_threshold": target.circuit_breaker.consecutive_failures,
                "breaker_window_ms": target.circuit_breaker.window_ms,
                "open_until_ms": breaker.open_until_ms,
                "request_timestamp_ms": request.timestamp_ms,
            }
        else:
            if breaker.state == "OPEN" and breaker.open_until_ms is not None:
                probe = True
                _transition(
                    breaker,
                    "HALF_OPEN",
                    timestamp_ms=request.timestamp_ms,
                    request_id=request.request_id,
                    reason="OPEN_WINDOW_ELAPSED",
                )
            upstream_attempted = True
            failure_reason = _breaker_failure_reason(request, target.timeout_ms)
            breaker_failure = failure_reason is not None

            if failure_reason == "CONNECTION_ERROR":
                decision_type = "UPSTREAM_CONNECTION_ERROR"
                response_status = 502
                response_latency_ms = request.upstream.latency_ms
            elif failure_reason == "TIMEOUT":
                decision_type = "TIMEOUT"
                response_status = 504
                response_latency_ms = target.timeout_ms
            elif request.upstream.status_code >= 400:
                decision_type = "UPSTREAM_ERROR"
                response_status = request.upstream.status_code
                response_latency_ms = request.upstream.latency_ms
            else:
                decision_type = "SUCCESS"
                response_status = request.upstream.status_code
                response_latency_ms = request.upstream.latency_ms

            if breaker_failure:
                breaker.consecutive_failures += 1
                if breaker.state == "HALF_OPEN":
                    breaker.open_until_ms = request.timestamp_ms + target.circuit_breaker.window_ms
                    _transition(
                        breaker,
                        "OPEN",
                        timestamp_ms=request.timestamp_ms,
                        request_id=request.request_id,
                        reason="HALF_OPEN_PROBE_FAILED",
                    )
                elif (
                    breaker.state == "CLOSED"
                    and breaker.consecutive_failures >= target.circuit_breaker.consecutive_failures
                ):
                    breaker.open_until_ms = request.timestamp_ms + target.circuit_breaker.window_ms
                    _transition(
                        breaker,
                        "OPEN",
                        timestamp_ms=request.timestamp_ms,
                        request_id=request.request_id,
                        reason="CONSECUTIVE_FAILURE_THRESHOLD_REACHED",
                    )
            else:
                breaker.consecutive_failures = 0
                breaker.open_until_ms = None
                if breaker.state == "HALF_OPEN":
                    _transition(
                        breaker,
                        "CLOSED",
                        timestamp_ms=request.timestamp_ms,
                        request_id=request.request_id,
                        reason="HALF_OPEN_PROBE_SUCCEEDED",
                    )

            trigger_parameters = {
                "timeout_ms": target.timeout_ms,
                "upstream_status_code": request.upstream.status_code,
                "upstream_latency_ms": request.upstream.latency_ms,
                "connection_error": request.upstream.connection_error,
                "breaker_failure": breaker_failure,
                "breaker_failure_reason": failure_reason,
                "consecutive_failures_before": consecutive_failures_before,
                "consecutive_failure_threshold": target.circuit_breaker.consecutive_failures,
                "breaker_window_ms": target.circuit_breaker.window_ms,
                "half_open_probe": probe,
            }

        by_decision_type[decision_type] += 1
        by_route[route_id] = by_route.get(route_id, 0) + 1
        decisions.append(
            {
                "request_id": request.request_id,
                "sequence_no": sequence_no,
                "timestamp_ms": request.timestamp_ms,
                "method": request.method,
                "path": request.path,
                "route_id": route_id,
                "is_default_route": resolution["is_default_route"],
                "matched_rule_ids": resolution["matched_rule_ids"],
                "match_count": resolution["match_count"],
                "route_resolution": {
                    "winning_rule_id": resolution["winning_rule_id"],
                    "strategy": resolution["strategy"],
                    "tied_at_top_precedence": resolution["tied_at_top_precedence"],
                    "candidates": resolution["candidates"],
                },
                "decision_type": decision_type,
                "trigger_parameters": trigger_parameters,
                "response_status": response_status,
                "response_latency_ms": response_latency_ms,
                "rate_limit": rate_info,
                "upstream_attempted": upstream_attempted,
                "upstream": request.upstream.model_dump(mode="json"),
                "breaker": {
                    "state_before": state_before,
                    "state_after": breaker.state,
                    "consecutive_failures": breaker.consecutive_failures,
                    "open_until_ms": breaker.open_until_ms,
                    "half_open_probe": probe,
                },
                "parameter_conflicts": [dict(item) for item in conflicts_by_route.get(route_id, [])],
            }
        )

    stats = {
        "total_requests": len(requests),
        "by_decision_type": by_decision_type,
        "by_route": dict(sorted(by_route.items())),
        "rate_limit_rejections": by_decision_type["RATE_LIMITED"],
        "circuit_open_rejections": by_decision_type["CIRCUIT_OPEN"],
        "timeouts": by_decision_type["TIMEOUT"],
        "upstream_errors": by_decision_type["UPSTREAM_ERROR"],
        "upstream_connection_errors": by_decision_type["UPSTREAM_CONNECTION_ERROR"],
        "successes": by_decision_type["SUCCESS"],
        "route_ambiguity_requests": sum(
            1 for decision in decisions if decision["match_count"] > 1
        ),
        "route_ambiguity_ties": sum(
            1 for decision in decisions if decision["route_resolution"]["tied_at_top_precedence"]
        ),
        "breaker_transitions": {
            route_id: state.transitions for route_id, state in sorted(breaker_states.items())
        },
        "breaker_transition_events": [
            {"route_id": route_id, **event}
            for route_id, state in sorted(breaker_states.items())
            for event in state.events
        ],
        "conflict_count": len(conflicts),
    }
    orders = _build_orders(
        decisions,
        conflicts,
        route_match_counts,
        breaker_states,
        len(requests),
    )
    return {
        "trace_id": trace.trace_id,
        "snapshot_version": snapshot.version,
        "decisions": decisions,
        "orders": orders,
        "conflicts": conflicts,
        "stats": stats,
    }
