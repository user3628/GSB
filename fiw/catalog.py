"""Parsing and normalization of the endpoint inventory and the injection plan.

Any parse failure, reference to an unknown endpoint, or dependency cycle
aborts the whole load with E_PLAN_INVALID; nothing is partially applied.
"""

from __future__ import annotations

import json

from .errors import BizError, E_PLAN_INVALID
from .models import (
    FAULT_TYPES,
    PROFILES,
    Catalog,
    EndpointSpec,
    InjectionItem,
    PlanSpec,
    SchedulerConfig,
)


def _invalid(message, **details):
    return BizError(E_PLAN_INVALID, message, details)


def _read_json(path, what):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        raise _invalid(f"{what} file not found", path=str(path))
    except json.JSONDecodeError as exc:
        raise _invalid(f"{what} is not valid JSON", path=str(path), error=str(exc))


def _require(cond, message, **details):
    if not cond:
        raise _invalid(message, **details)


def parse_catalog(data) -> Catalog:
    _require(isinstance(data, dict) and isinstance(data.get("endpoints"), list),
             "catalog must contain an 'endpoints' list")
    endpoints = []
    seen = set()
    for raw in data["endpoints"]:
        _require(isinstance(raw, dict), "endpoint entry must be an object", entry=raw)
        eid = raw.get("id")
        _require(isinstance(eid, str) and eid, "endpoint requires a non-empty 'id'", entry=raw)
        _require(eid not in seen, "duplicate endpoint id", endpoint=eid)
        seen.add(eid)
        node = raw.get("node")
        _require(isinstance(node, str) and node, "endpoint requires 'node'", endpoint=eid)
        weight = raw.get("weight")
        _require(isinstance(weight, (int, float)) and not isinstance(weight, bool)
                 and 0.0 <= float(weight) <= 1.0,
                 "endpoint weight must be a number in [0, 1]", endpoint=eid, weight=weight)
        upstreams = raw.get("upstreams", [])
        _require(isinstance(upstreams, list) and all(isinstance(u, str) for u in upstreams),
                 "endpoint upstreams must be a list of endpoint ids", endpoint=eid)
        _require(eid not in upstreams, "endpoint cannot depend on itself", endpoint=eid)
        host = raw.get("host", "127.0.0.1")
        port = raw.get("port")
        _require(isinstance(port, int) and not isinstance(port, bool) and 0 < port < 65536,
                 "endpoint requires a valid 'port'", endpoint=eid, port=port)
        profile = raw.get("profile")
        _require(profile in PROFILES, "endpoint profile must be one of healthy/blackhole/unlistened",
                 endpoint=eid, profile=profile)
        endpoints.append(EndpointSpec(
            endpoint_id=eid, node=node, weight=float(weight),
            upstreams=tuple(upstreams), host=str(host), port=port, profile=profile,
        ))
    by_id = {e.endpoint_id: e for e in endpoints}
    for ep in endpoints:
        for up in ep.upstreams:
            _require(up in by_id, "endpoint references unknown upstream",
                     endpoint=ep.endpoint_id, upstream=up)
    _check_cycles(endpoints)
    return Catalog(endpoints=tuple(endpoints))


def _check_cycles(endpoints):
    by_id = {e.endpoint_id: e for e in endpoints}
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {e.endpoint_id: WHITE for e in endpoints}
    stack = []

    def visit(eid):
        color[eid] = GRAY
        stack.append(eid)
        for up in by_id[eid].upstreams:
            if color[up] == GRAY:
                cycle = stack[stack.index(up):] + [up]
                raise _invalid("dependency cycle detected", cycle=cycle)
            if color[up] == WHITE:
                visit(up)
        stack.pop()
        color[eid] = BLACK

    for ep in endpoints:
        if color[ep.endpoint_id] == WHITE:
            visit(ep.endpoint_id)


def topo_order(catalog: Catalog):
    """Endpoint ids ordered so upstreams always come before dependents."""
    by_id = catalog.by_id
    done = set()
    order = []

    def visit(eid):
        if eid in done:
            return
        for up in by_id[eid].upstreams:
            visit(up)
        done.add(eid)
        order.append(eid)

    for ep in catalog.endpoints:
        visit(ep.endpoint_id)
    return order


def parse_plan(data, catalog: Catalog) -> PlanSpec:
    _require(isinstance(data, dict), "plan must be a JSON object")
    plan_id = data.get("plan_id")
    _require(isinstance(plan_id, str) and plan_id, "plan requires a non-empty 'plan_id'")
    version = data.get("version")
    _require(isinstance(version, int) and not isinstance(version, bool) and version >= 1,
             "plan version must be an integer >= 1", version=version)
    rounds = data.get("rounds")
    _require(isinstance(rounds, int) and not isinstance(rounds, bool) and rounds >= 1,
             "plan rounds must be an integer >= 1", rounds=rounds)

    switches = data.get("fault_switches", {})
    _require(isinstance(switches, dict), "fault_switches must be an object")
    fault_switches = {ft: bool(switches.get(ft, True)) for ft in FAULT_TYPES}

    sched_raw = data.get("scheduler", {})
    _require(isinstance(sched_raw, dict), "scheduler must be an object")
    scheduler = SchedulerConfig(
        global_concurrency=int(sched_raw.get("global_concurrency", 2)),
        queue_capacity=int(sched_raw.get("queue_capacity", 4)),
        round_time_budget_ms=float(sched_raw.get("round_time_budget_ms", 2000.0)),
        connect_timeout_ms=float(sched_raw.get("connect_timeout_ms", 300.0)),
        cost_estimates_ms=dict(sched_raw.get("cost_estimates_ms", {})),
    )
    _require(scheduler.global_concurrency >= 1, "global_concurrency must be >= 1")
    _require(scheduler.queue_capacity >= 0, "queue_capacity must be >= 0")
    _require(scheduler.round_time_budget_ms > 0, "round_time_budget_ms must be > 0")
    _require(scheduler.connect_timeout_ms > 0, "connect_timeout_ms must be > 0")

    known = set(catalog.by_id)
    items = []
    seen_items = set()
    for raw in data.get("items", []):
        _require(isinstance(raw, dict), "injection item must be an object", entry=raw)
        iid = raw.get("id")
        _require(isinstance(iid, str) and iid, "injection item requires a non-empty 'id'", entry=raw)
        _require(iid not in seen_items, "duplicate injection item id", item=iid)
        seen_items.add(iid)
        target = raw.get("target")
        _require(target in known, "injection item references unknown endpoint",
                 item=iid, target=target)
        fault_type = raw.get("fault_type")
        _require(fault_type in FAULT_TYPES, "unknown fault_type",
                 item=iid, fault_type=fault_type, supported=sorted(FAULT_TYPES))
        duration = raw.get("duration_rounds")
        _require(isinstance(duration, int) and not isinstance(duration, bool) and duration >= 1,
                 "duration_rounds must be an integer >= 1", item=iid, duration=duration)
        priority = raw.get("priority", 0)
        _require(isinstance(priority, int) and not isinstance(priority, bool),
                 "priority must be an integer", item=iid, priority=priority)
        items.append(InjectionItem(
            item_id=iid, target=target, fault_type=fault_type,
            duration_rounds=duration, priority=priority,
        ))
    return PlanSpec(
        plan_id=plan_id, version=version, rounds=rounds,
        fault_switches=fault_switches, scheduler=scheduler, items=tuple(items),
    )


def load_samples(endpoints_path, plan_path):
    """Parse both sample files; raises BizError(E_PLAN_INVALID) on any problem."""
    catalog = parse_catalog(_read_json(endpoints_path, "endpoint inventory"))
    plan = parse_plan(_read_json(plan_path, "injection plan"), catalog)
    return catalog, plan
