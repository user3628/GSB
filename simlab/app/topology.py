"""Topology & injection-plan parsing, normalization and validation."""
from __future__ import annotations

import hashlib
import json
import time
import uuid

from .db import Database
from .errors import BizError, E_PLAN_INVALID, E_TOPOLOGY_INVALID

FAULT_TYPES = {"node_down", "link_timeout", "replica_jitter"}
FAILOVER_STRATEGIES = {"cross_domain", "prefer_local", "none"}

DEFAULT_BUDGETS = {
    "injection_concurrency": 2,
    "injection_queue_capacity": 4,
    "round_time_budget_ms": 1500,
    "failover_ops_per_round": 3,
}


def _canonical(payload: dict) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def content_hash(payload: dict) -> str:
    return hashlib.sha256(_canonical(payload).encode("utf-8")).hexdigest()


def _fail(msg: str, details: dict | None = None):
    raise BizError(E_TOPOLOGY_INVALID, msg, details or {})


def normalize_topology(raw: dict) -> dict:
    """Validate + normalize a topology document.  Raises E_TOPOLOGY_INVALID."""
    if not isinstance(raw, dict):
        _fail("topology must be a JSON object")
    name = raw.get("name")
    if not name or not isinstance(name, str):
        _fail("topology.name is required")

    # fault domains ------------------------------------------------------------
    fd_raw = raw.get("fault_domains")
    if not isinstance(fd_raw, list) or not fd_raw:
        _fail("topology.fault_domains must be a non-empty list")
    domains: dict[str, dict] = {}
    for d in fd_raw:
        dname = d.get("name")
        quota = d.get("isolation_quota")
        if not dname or not isinstance(quota, int) or quota < 0:
            _fail("each fault domain needs name and non-negative integer isolation_quota",
                  {"fault_domain": d})
        if dname in domains:
            _fail("duplicate fault domain", {"fault_domain": dname})
        domains[dname] = {"name": dname, "isolation_quota": quota}

    # services -----------------------------------------------------------------
    svc_raw = raw.get("services")
    if not isinstance(svc_raw, list) or not svc_raw:
        _fail("topology.services must be a non-empty list")
    services: dict[str, dict] = {}
    for s in svc_raw:
        sname = s.get("name")
        replicas = s.get("replicas", 1)
        if not sname or not isinstance(replicas, int) or replicas < 1:
            _fail("each service needs name and replicas>=1", {"service": s})
        if sname in services:
            _fail("duplicate service", {"service": sname})
        placement = s.get("fault_domains")
        if not isinstance(placement, list) or not placement:
            _fail("service.fault_domains must list replica placement", {"service": sname})
        unknown = [d for d in placement if d not in domains]
        if unknown:
            _fail("service placed in unknown fault domain",
                  {"service": sname, "unknown_domains": unknown})
        strategy = s.get("failover_strategy", "cross_domain")
        if strategy not in FAILOVER_STRATEGIES:
            _fail("unknown failover_strategy", {"service": sname, "strategy": strategy})
        services[sname] = {
            "name": sname,
            "replicas": replicas,
            "fault_domains": list(placement),
            "failover_strategy": strategy,
        }

    # dependencies -------------------------------------------------------------
    deps_raw = raw.get("dependencies", [])
    if not isinstance(deps_raw, list):
        _fail("topology.dependencies must be a list")
    edges: list[dict] = []
    for e in deps_raw:
        src, dst = e.get("from"), e.get("to")
        if src not in services or dst not in services:
            _fail("dependency references unknown service", {"edge": e})
        if src == dst:
            _fail("self dependency not allowed", {"service": src})
        timeout_ms = e.get("timeout_ms", 500)
        if not isinstance(timeout_ms, int) or timeout_ms <= 0:
            _fail("dependency timeout_ms must be positive int", {"edge": e})
        edges.append({"from": src, "to": dst, "timeout_ms": timeout_ms})

    # cycle detection (Kahn) -----------------------------------------------------
    indeg = {s: 0 for s in services}
    adj: dict[str, list[str]] = {s: [] for s in services}
    for e in edges:
        adj[e["from"]].append(e["to"])
        indeg[e["to"]] += 1
    queue = [s for s, d in indeg.items() if d == 0]
    seen = 0
    while queue:
        node = queue.pop()
        seen += 1
        for nxt in adj[node]:
            indeg[nxt] -= 1
            if indeg[nxt] == 0:
                queue.append(nxt)
    if seen != len(services):
        cyclic = sorted(s for s, d in indeg.items() if d > 0)
        _fail("dependency graph contains a cycle", {"cyclic_services": cyclic})

    budgets = dict(DEFAULT_BUDGETS)
    for k, v in (raw.get("budgets") or {}).items():
        if k not in budgets or not isinstance(v, int) or v < 0:
            _fail("invalid budget override", {"key": k, "value": v})
        budgets[k] = v

    return {
        "name": name,
        "fault_domains": domains,
        "services": services,
        "dependencies": edges,
        "budgets": budgets,
    }


def load_topology(db: Database, raw: dict) -> dict:
    """Normalize + persist.  Identical content is reused idempotently."""
    normalized = normalize_topology(raw)
    digest = content_hash(normalized)
    existing = db.query_one("SELECT * FROM topologies WHERE content_hash=?", (digest,))
    if existing:
        return {"topology_id": existing["topology_id"], "name": existing["name"],
                "reused": True, "topology": normalized}
    topology_id = "topo-" + uuid.uuid4().hex[:12]
    def _do(conn):
        conn.execute(
            "INSERT INTO topologies(topology_id,name,content_hash,payload,created_at)"
            " VALUES(?,?,?,?,?)",
            (topology_id, normalized["name"], digest, _canonical(normalized), time.time()))
    db.write(_do)
    return {"topology_id": topology_id, "name": normalized["name"],
            "reused": False, "topology": normalized}


def get_topology(db: Database, topology_id: str) -> dict:
    row = db.query_one("SELECT payload FROM topologies WHERE topology_id=?", (topology_id,))
    if not row:
        from .errors import E_NOT_FOUND
        raise BizError(E_NOT_FOUND, "topology not found", {"topology_id": topology_id})
    return json.loads(row["payload"])


def normalize_plan(raw: dict, topology: dict) -> dict:
    """Validate + normalize an injection plan against a loaded topology."""
    if not isinstance(raw, dict):
        raise BizError(E_PLAN_INVALID, "plan must be a JSON object")
    rounds = raw.get("rounds", 1)
    if not isinstance(rounds, int) or rounds < 1:
        raise BizError(E_PLAN_INVALID, "plan.rounds must be a positive integer")
    items_raw = raw.get("items")
    if not isinstance(items_raw, list) or not items_raw:
        raise BizError(E_PLAN_INVALID, "plan.items must be a non-empty list")
    services = topology["services"]
    items: list[dict] = []
    seen_keys: set[str] = set()
    for i, it in enumerate(items_raw):
        key = it.get("id") or f"inj-{i}"
        if key in seen_keys:
            raise BizError(E_PLAN_INVALID, "duplicate injection item id", {"id": key})
        seen_keys.add(key)
        target = it.get("target")
        if target not in services:
            raise BizError(E_PLAN_INVALID, "injection targets unknown service",
                           {"id": key, "target": target})
        fault_type = it.get("fault_type")
        if fault_type not in FAULT_TYPES:
            raise BizError(E_PLAN_INVALID, "unknown fault_type",
                           {"id": key, "fault_type": fault_type})
        duration = it.get("duration_rounds", 1)
        priority = it.get("priority", 0)
        if not isinstance(duration, int) or duration < 1 or not isinstance(priority, int):
            raise BizError(E_PLAN_INVALID, "duration_rounds/priority must be integers",
                           {"id": key})
        endpoint = it.get("endpoint")
        if not isinstance(endpoint, str) or not endpoint.startswith("http://127.0.0.1"):
            raise BizError(E_PLAN_INVALID, "endpoint must be a loopback http URL",
                           {"id": key, "endpoint": endpoint})
        items.append({
            "item_key": key,
            "service": target,
            "fault_domain": services[target]["fault_domains"][0],
            "fault_type": fault_type,
            "duration_rounds": duration,
            "priority": priority,
            "endpoint": endpoint,
            "timeout_ms": it.get("timeout_ms", 800),
            "start_round": it.get("start_round", 1),
        })
    return {"name": raw.get("name", "plan"), "rounds": rounds, "items": items}


def load_plan(db: Database, topology_id: str, raw: dict) -> dict:
    topology = get_topology(db, topology_id)
    normalized = normalize_plan(raw, topology)
    digest = content_hash({"topology_id": topology_id, **normalized})
    existing = db.query_one("SELECT * FROM plans WHERE content_hash=?", (digest,))
    if existing:
        return {"plan_id": existing["plan_id"], "reused": True, "plan": normalized,
                "topology": topology}
    plan_id = "plan-" + uuid.uuid4().hex[:12]
    def _do(conn):
        conn.execute(
            "INSERT INTO plans(plan_id,topology_id,name,content_hash,payload,created_at)"
            " VALUES(?,?,?,?,?,?)",
            (plan_id, topology_id, normalized["name"], digest,
             _canonical(normalized), time.time()))
        for it in normalized["items"]:
            conn.execute(
                "INSERT INTO injections(injection_id,plan_id,item_key,service,fault_domain,"
                "fault_type,priority,duration_rounds,status) VALUES(?,?,?,?,?,?,?,?,?)",
                (f"{plan_id}:{it['item_key']}", plan_id, it["item_key"], it["service"],
                 it["fault_domain"], it["fault_type"], it["priority"],
                 it["duration_rounds"], "PENDING"))
    db.write(_do)
    return {"plan_id": plan_id, "reused": False, "plan": normalized, "topology": topology}
