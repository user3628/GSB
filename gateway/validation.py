from __future__ import annotations

import argparse
import json
from collections import defaultdict
from typing import Any

from .config import GatewayConfig
from .domain import LEGAL_TRANSITIONS, RequestStatus, compute_route_checksum
from .store import Store, utc_now


def _check(
    checks: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    message: str,
    *,
    severity: str = "ERROR",
    details: dict[str, Any] | None = None,
) -> None:
    checks.append(
        {
            "id": check_id,
            "status": "PASS" if passed else ("WARN" if severity == "WARN" else "FAIL"),
            "message": message,
            "details": details or {},
        }
    )


def validate_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    requests = snapshot["requests"]
    runtime = snapshot["runtime"]
    transitions = snapshot["transitions"]
    rounds = snapshot["rounds"]
    routes = snapshot["routes"]
    config = snapshot["config"]
    request_by_id = {item["request_id"]: item for item in requests}
    runtime_by_key = {(item["route_version"], item["backend_id"]): item for item in runtime}
    transitions_by_request: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for transition in transitions:
        transitions_by_request[transition["request_id"]].append(transition)

    _check(
        checks,
        "route_present",
        bool(routes) and snapshot["current_route_version"] is not None,
        "A committed route version exists",
    )
    checksum_failures = []
    for route in routes:
        try:
            payload = json.loads(route["payload"])
            source_payload = json.loads(route.get("source_payload") or route["payload"])
            checksum = payload.get("checksum")
            expected = compute_route_checksum(source_payload)
            if checksum != expected or route["checksum"] != expected:
                checksum_failures.append(
                    {
                        "version": route["version"],
                        "stored_column": route["checksum"],
                        "payload_checksum": checksum,
                        "expected": expected,
                    }
                )
        except Exception as exc:
            checksum_failures.append({"version": route.get("version"), "error": type(exc).__name__})
    _check(
        checks,
        "route_checksums",
        not checksum_failures,
        "Every persisted route payload matches its checksum",
        details={"failures": checksum_failures},
    )

    state_errors: list[dict[str, Any]] = []
    valid_statuses = {status.value for status in RequestStatus}
    for request in requests:
        request_id = request["request_id"]
        history = transitions_by_request.get(request_id, [])
        if request["status"] not in valid_statuses:
            state_errors.append({"request_id": request_id, "error": "unknown_status", "status": request["status"]})
        if not history:
            state_errors.append({"request_id": request_id, "error": "missing_transition_history"})
            continue
        first = history[0]
        if first["from_status"] is not None or first["to_status"] != "PENDING":
            state_errors.append(
                {
                    "request_id": request_id,
                    "error": "invalid_initial_transition",
                    "transition": {"from": first["from_status"], "to": first["to_status"]},
                }
            )
        for index in range(1, len(history)):
            previous = history[index - 1]
            current = history[index]
            if current["from_status"] != previous["to_status"]:
                state_errors.append(
                    {
                        "request_id": request_id,
                        "error": "broken_transition_chain",
                        "index": index,
                        "expected_from": previous["to_status"],
                        "actual_from": current["from_status"],
                    }
                )
                continue
            try:
                source = RequestStatus(current["from_status"])
                target = RequestStatus(current["to_status"])
            except ValueError:
                state_errors.append(
                    {"request_id": request_id, "error": "unknown_transition_status", "transition": current}
                )
                continue
            if target not in LEGAL_TRANSITIONS[source]:
                state_errors.append(
                    {
                        "request_id": request_id,
                        "error": "illegal_transition",
                        "from": source.value,
                        "to": target.value,
                    }
                )
            if source is RequestStatus.DISPATCHED and target in {
                RequestStatus.QUEUED,
                RequestStatus.ABORTED,
            } and current["reason_code"] not in {"RECOVERY_REQUEUE", "E_RECOVERY_ABORTED"}:
                state_errors.append(
                    {
                        "request_id": request_id,
                        "error": "recovery_transition_without_recovery_reason",
                        "from": source.value,
                        "to": target.value,
                        "reason": current["reason_code"],
                    }
                )
        if history[-1]["to_status"] != request["status"]:
            state_errors.append(
                {
                    "request_id": request_id,
                    "error": "latest_transition_mismatch",
                    "latest": history[-1]["to_status"],
                    "status": request["status"],
                }
            )
    _check(
        checks,
        "state_machine",
        not state_errors,
        "All persisted state transitions form one legal chain ending at the request status",
        details={"failures": state_errors},
    )

    dispatched_by_backend: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    for request in requests:
        if request["status"] == "DISPATCHED":
            dispatched_by_backend[(request["route_version"], request["backend_id"])].append(request)

    counter_errors: list[dict[str, Any]] = []
    budget_errors: list[dict[str, Any]] = []
    for key, runtime_row in runtime_by_key.items():
        active = dispatched_by_backend.get(key, [])
        expected_in_flight = len(active)
        expected_vram = round(sum(float(item["vram_reserved_mb"]) for item in active), 6)
        actual_in_flight = int(runtime_row["in_flight"])
        actual_vram = round(float(runtime_row["vram_reserved_mb"]), 6)
        if expected_in_flight != actual_in_flight:
            counter_errors.append(
                {
                    "route_version": key[0],
                    "backend_id": key[1],
                    "kind": "in_flight",
                    "expected": expected_in_flight,
                    "actual": actual_in_flight,
                }
            )
        if abs(expected_vram - actual_vram) > 1e-6:
            counter_errors.append(
                {
                    "route_version": key[0],
                    "backend_id": key[1],
                    "kind": "vram",
                    "expected": expected_vram,
                    "actual": actual_vram,
                }
            )
        if actual_in_flight > int(runtime_row["concurrency_limit"]):
            budget_errors.append(
                {
                    "route_version": key[0],
                    "backend_id": key[1],
                    "kind": "concurrency_limit",
                    "actual": actual_in_flight,
                    "limit": int(runtime_row["concurrency_limit"]),
                }
            )
        if actual_vram > float(runtime_row["vram_limit_mb"]) + 1e-6:
            budget_errors.append(
                {
                    "route_version": key[0],
                    "backend_id": key[1],
                    "kind": "vram_limit",
                    "actual": actual_vram,
                    "limit": float(runtime_row["vram_limit_mb"]),
                }
            )
    for request in requests:
        if request["status"] == "DISPATCHED":
            key = (request["route_version"], request["backend_id"])
            if key not in runtime_by_key:
                counter_errors.append(
                    {
                        "request_id": request["request_id"],
                        "kind": "dispatched_without_runtime",
                        "route_version": key[0],
                        "backend_id": key[1],
                    }
                )
        elif abs(float(request["vram_reserved_mb"])) > 1e-9:
            counter_errors.append(
                {
                    "request_id": request["request_id"],
                    "kind": "reservation_on_non_dispatched",
                    "status": request["status"],
                    "vram_reserved_mb": float(request["vram_reserved_mb"]),
                }
            )
    _check(
        checks,
        "reservation_closure",
        not counter_errors,
        "DISPATCHED requests exactly match backend in-flight counts and VRAM reservations",
        details={"failures": counter_errors},
    )

    global_in_flight = sum(1 for request in requests if request["status"] == "DISPATCHED")
    current_global_limit = int(config["global_concurrency_limit"])
    current_route = next(
        (route for route in routes if int(route["version"]) == snapshot["current_route_version"]),
        None,
    )
    if current_route is not None:
        current_payload = json.loads(current_route["payload"])
        if current_payload.get("global_concurrency_limit") is not None:
            current_global_limit = min(
                current_global_limit, int(current_payload["global_concurrency_limit"])
            )
    if global_in_flight > current_global_limit:
        budget_errors.append(
            {
                "kind": "global_concurrency",
                "actual": global_in_flight,
                "limit": current_global_limit,
            }
        )
    _check(
        checks,
        "resource_limits",
        not budget_errors,
        "Global concurrency, per-backend concurrency and VRAM stay within budgets",
        details={"failures": budget_errors},
    )

    binding_errors: list[dict[str, Any]] = []
    routes_by_version = {int(route["version"]): json.loads(route["payload"]) for route in routes}
    for request in requests:
        route = routes_by_version.get(int(request["route_version"]))
        if route is None:
            binding_errors.append({"request_id": request["request_id"], "error": "missing_route"})
            continue
        backend = route.get("backends", {}).get(request["backend_id"])
        if backend is None:
            binding_errors.append({"request_id": request["request_id"], "error": "missing_backend"})
            continue
        if backend.get("endpoint") != request["endpoint"]:
            binding_errors.append(
                {
                    "request_id": request["request_id"],
                    "error": "endpoint_binding_mismatch",
                    "expected": backend.get("endpoint"),
                    "actual": request["endpoint"],
                }
            )
        expected_demand = round(
            float(request["sample_count"]) * float(backend["vram_per_sample_mb"]), 6
        )
        if abs(expected_demand - float(request["vram_demand_mb"])) > 1e-6:
            binding_errors.append(
                {
                    "request_id": request["request_id"],
                    "error": "vram_demand_mismatch",
                    "expected": expected_demand,
                    "actual": float(request["vram_demand_mb"]),
                }
            )
    _check(
        checks,
        "route_binding",
        not binding_errors,
        "Each request uses one immutable route/backend/endpoint specification snapshot",
        details={"failures": binding_errors},
    )

    round_errors: list[dict[str, Any]] = []
    for round_row in rounds:
        trace = round_row.get("trace")
        if round_row["status"] != "COMPLETED":
            continue
        if trace is None:
            round_errors.append({"round_id": round_row["id"], "error": "missing_trace"})
            continue
        summary = trace.get("summary", {})
        if int(summary.get("estimated_used_ms", 0)) > int(round_row["budget_ms"]):
            round_errors.append(
                {
                    "round_id": round_row["id"],
                    "error": "time_budget_exceeded",
                    "used_ms": summary.get("estimated_used_ms"),
                    "budget_ms": round_row["budget_ms"],
                }
            )
        ids = [item["request_id"] for item in trace.get("dispatch_order", [])]
        if len(ids) != len(set(ids)):
            round_errors.append({"round_id": round_row["id"], "error": "duplicate_dispatch_in_round"})
        if int(summary.get("dispatched_count", 0)) != len(ids):
            round_errors.append(
                {"round_id": round_row["id"], "error": "dispatch_count_mismatch"}
            )
        for request_id in ids:
            request = request_by_id.get(request_id)
            if request is None:
                round_errors.append(
                    {"round_id": round_row["id"], "request_id": request_id, "error": "dispatch_unknown_request"}
                )
                continue
            history = transitions_by_request.get(request_id, [])
            if not any(
                transition["to_status"] == "DISPATCHED"
                and transition["from_status"] == "QUEUED"
                and transition["reason_code"] == "DISPATCHED"
                and json.loads(transition["detail_json"]).get("round_id") == round_row["id"]
                for transition in history
            ):
                round_errors.append(
                    {"round_id": round_row["id"], "request_id": request_id, "error": "trace_without_durable_dispatch"}
                )
    _check(
        checks,
        "round_traces",
        not round_errors,
        "Round traces are within budget, contain no duplicate dispatch, and match durable transitions",
        details={"failures": round_errors},
    )

    _check(
        checks,
        "intent_journal",
        not snapshot["open_intents"],
        "No operation intent is left open in a stable persisted snapshot",
        details={"open_intents": snapshot["open_intents"]},
    )
    recovery_errors = []
    for request in requests:
        if request["status"] == "DISPATCHED":
            history = transitions_by_request.get(request["request_id"], [])
            if not history or history[-1]["to_status"] != "DISPATCHED":
                recovery_errors.append({"request_id": request["request_id"]})
    _check(
        checks,
        "recovery_consistency",
        not recovery_errors,
        "No request is left in an un-owned DISPATCHED state",
        details={"failures": recovery_errors},
    )

    failures = [item for item in checks if item["status"] == "FAIL"]
    warnings = [item for item in checks if item["status"] == "WARN"]
    status_counts: dict[str, int] = defaultdict(int)
    for request in requests:
        status_counts[request["status"]] += 1
    return {
        "verdict": "PASS" if not failures else "FAIL",
        "generated_at": utc_now(),
        "checks": checks,
        "failures": failures,
        "warnings": warnings,
        "summary": {
            "route_versions": len(routes),
            "current_route_version": snapshot["current_route_version"],
            "requests": len(requests),
            "request_status_counts": dict(sorted(status_counts.items())),
            "rounds": len(rounds),
            "backends": len(runtime),
            "global_in_flight": global_in_flight,
            "latest_recovery_report_id": (
                None
                if snapshot["latest_recovery"] is None
                else snapshot["latest_recovery"]["id"]
            ),
        },
    }


def validate_database(db_path: str, config: GatewayConfig | None = None) -> dict[str, Any]:
    cfg = config or GatewayConfig(db_path=db_path)
    cfg.db_path = db_path
    store = Store(cfg)
    try:
        return validate_snapshot(store.validation_snapshot())
    finally:
        store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate persisted inference gateway state")
    parser.add_argument("--db", default="gateway.db", help="SQLite database path")
    parser.add_argument("--output", help="Optional JSON output path")
    args = parser.parse_args()
    config = GatewayConfig.from_env()
    config.db_path = args.db
    report = validate_database(args.db, config)
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            handle.write(text + "\n")
    print(text)
    raise SystemExit(0 if report["verdict"] == "PASS" else 1)


if __name__ == "__main__":
    main()

