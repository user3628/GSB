"""Service dependency topology: checksum, versioning, snapshots and increments."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from collections import deque

from .errors import ChecksumMismatch, StaleBase, TopologyInvalid


def canonical_checksum(services: list[str], edges: list[tuple[str, str]]) -> str:
    body = json.dumps(
        {"services": sorted(services), "edges": sorted((s, d) for s, d in edges)},
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def current_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT MAX(version) AS v FROM topology_versions").fetchone()
    return int(row["v"]) if row and row["v"] is not None else 0


def declared_services(conn: sqlite3.Connection) -> set[str]:
    return {row["name"] for row in conn.execute("SELECT name FROM services")}


def active_edges(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    rows = conn.execute(
        "SELECT src, dst FROM edges WHERE removed_version IS NULL ORDER BY src, dst"
    )
    return [(row["src"], row["dst"]) for row in rows]


def hop_distances(conn: sqlite3.Connection, start: str) -> dict[str, int]:
    """BFS over downstream dependency edges; returns service -> hop count.

    ``start`` itself maps to hop 0.  Edges are traversed in deterministic
    (src, dst) order so tie-breaking downstream is stable.
    """
    adjacency: dict[str, list[str]] = {}
    for src, dst in active_edges(conn):
        adjacency.setdefault(src, []).append(dst)
    for targets in adjacency.values():
        targets.sort()

    distances = {start: 0}
    queue = deque([start])
    while queue:
        node = queue.popleft()
        for neighbour in adjacency.get(node, ()):
            if neighbour not in distances:
                distances[neighbour] = distances[node] + 1
                queue.append(neighbour)
    return distances


def _validate(services: list[str], edges: list[tuple[str, str]]) -> None:
    if not services or len(set(services)) != len(services):
        raise TopologyInvalid("services must be a non-empty unique list", {"services": services})
    known = set(services)
    for edge in edges:
        if len(edge) != 2 or not edge[0] or not edge[1]:
            raise TopologyInvalid("each edge must be a [source, target] pair", {"edge": edge})
        src, dst = edge
        if src not in known or dst not in known:
            raise TopologyInvalid(
                "edge references an undeclared service",
                {"edge": [src, dst], "known_services": sorted(known)},
            )


def apply_topology(conn: sqlite3.Connection, payload: dict) -> tuple[int, set[str]]:
    """Apply a snapshot or increment inside the caller's open write transaction.

    Returns ``(new_version, affected_services)``.  Raises a business error
    without mutating anything when validation or the base version check fails.
    """
    kind = payload.get("kind")
    if kind not in ("snapshot", "increment"):
        raise TopologyInvalid("kind must be 'snapshot' or 'increment'", {"kind": kind})

    try:
        new_version = int(payload["version"])
    except (KeyError, TypeError, ValueError):
        raise TopologyInvalid("version must be an integer", {"version": payload.get("version")})

    services = list(payload.get("services", []))
    edges = [tuple(edge) for edge in payload.get("edges", [])]
    _validate(services, edges)

    checksum = canonical_checksum(services, edges)
    declared = payload.get("checksum")
    if declared is not None and declared != checksum:
        raise ChecksumMismatch(
            "topology checksum does not match canonical content",
            {"declared": declared, "computed": checksum},
        )

    version = current_version(conn)
    based_on = payload.get("base_version")
    if kind == "increment":
        if based_on is None or int(based_on) != version:
            raise StaleBase(
                "increment base_version is not the current topology version",
                {"base_version": based_on, "current_version": version},
            )
        if new_version <= version:
            raise TopologyInvalid(
                "increment version must advance",
                {"version": new_version, "current_version": version},
            )
    else:
        if new_version <= version:
            raise TopologyInvalid(
                "snapshot version must advance beyond the current version",
                {"version": new_version, "current_version": version},
            )

    now_ms = int(time.time() * 1000)
    affected: set[str] = set(services)
    affected.update(src for src, _ in edges)
    affected.update(dst for _, dst in edges)

    if kind == "snapshot":
        # Whole-snapshot replacement: retire every active edge first so old
        # and new content can never be read mixed together.
        conn.execute(
            "UPDATE edges SET removed_version=? WHERE removed_version IS NULL", (new_version,)
        )
        for name in services:
            conn.execute(
                "INSERT INTO services(name, first_version, latest_version) VALUES(?, ?, ?) "
                "ON CONFLICT(name) DO UPDATE SET latest_version=excluded.latest_version",
                (name, new_version, new_version),
            )
        conn.execute("DELETE FROM services WHERE name NOT IN (%s)" % ",".join("?" * len(services)),
                     services)
    else:
        for name in services:
            conn.execute(
                "INSERT INTO services(name, first_version, latest_version) VALUES(?, ?, ?) "
                "ON CONFLICT(name) DO UPDATE SET latest_version=excluded.latest_version",
                (name, new_version, new_version),
            )

    for src, dst in edges:
        existing = conn.execute(
            "SELECT id, removed_version FROM edges WHERE src=? AND dst=?", (src, dst)
        ).fetchone()
        if existing is None:
            conn.execute(
                "INSERT INTO edges(src, dst, added_version) VALUES(?, ?, ?)",
                (src, dst, new_version),
            )
        elif existing["removed_version"] is not None:
            conn.execute(
                "UPDATE edges SET added_version=?, removed_version=NULL WHERE id=?",
                (new_version, existing["id"]),
            )

    conn.execute(
        "INSERT INTO topology_versions(version, kind, checksum, declared_checksum, "
        "based_on_version, commit_id, created_at_ms) VALUES(?, ?, ?, ?, ?, ?, ?)",
        (new_version, kind, checksum, declared, based_on, payload.get("commit_id"), now_ms),
    )
    return new_version, affected
