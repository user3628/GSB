"""Dependency invalidation and atomic cascade rebuild orchestration.

A rebuild always runs inside the caller's single write transaction.  The
invalidation phase removes every derived artifact that could have gone stale
(quantile index, suppression index, rca scores, service references) before any
new row is written, so readers can never observe a half-rebuilt state such as
"fresh quantile index but old rca score".
"""
from __future__ import annotations

import sqlite3
import time

from . import aggregates, rca
from .config import Settings
from .topology import declared_services


def invalidate_services(conn: sqlite3.Connection, services: set[str]) -> dict:
    if not services:
        return {"invalidated": {"quantile": 0, "suppression": 0, "rca": 0, "refs": 0}}
    markers = ",".join("?" * len(services))
    params = sorted(services)

    quantile = conn.execute(
        f"DELETE FROM quantile_index WHERE service IN ({markers})", params
    ).rowcount
    # The suppression index is an accumulating fingerprint-level denoise
    # artifact: it survives a rebuild and is updated by alert evaluation.
    suppression = 0
    alert_ids = [
        row["id"]
        for row in conn.execute(
            f"SELECT id FROM alerts WHERE service IN ({markers})", params
        )
    ]
    rca_rows = conn.execute(
        f"DELETE FROM rca_scores WHERE candidate IN ({markers})"
        + (f" OR alert_id IN ({','.join('?' * len(alert_ids))})" if alert_ids else ""),
        params + alert_ids,
    ).rowcount if (params or alert_ids) else 0
    refs = conn.execute(
        f"DELETE FROM service_refs WHERE service IN ({markers})", params
    ).rowcount
    return {
        "invalidated": {
            "quantile": quantile,
            "suppression": suppression,
            "rca": rca_rows,
            "refs": refs,
        }
    }


def invalidate_all_derived(conn: sqlite3.Connection) -> dict:
    counts = {
        "quantile": conn.execute("DELETE FROM quantile_index").rowcount,
        "suppression": conn.execute("DELETE FROM suppression_index").rowcount,
        "rca": conn.execute("DELETE FROM rca_scores").rowcount,
        "refs": conn.execute("DELETE FROM service_refs").rowcount,
    }
    return {"invalidated": counts}


def cascade_rebuild(conn: sqlite3.Connection, settings: Settings, services: set[str]) -> dict:
    """Invalidate then rebuild all affected artifacts within one transaction."""
    started = int(time.time() * 1000)
    invalidation = invalidate_services(conn, services)

    live_services = declared_services(conn)
    rebuilt_services = []
    for service in sorted(services):
        if service not in live_services:
            # Removed by a full snapshot replacement: invalidated, never rebuilt.
            continue
        rebuilt_services.append(aggregates.rebuild_service_derived(conn, settings, service))

    # Any rca row that referenced an invalidated candidate was removed above;
    # rebuild the complete rca table so no downstream score is ever left stale.
    rca_summary = rca.rebuild_all_rca(conn, settings)
    return {
        "started_at_ms": started,
        "finished_at_ms": int(time.time() * 1000),
        **invalidation,
        "rebuilt_services": rebuilt_services,
        "rca": rca_summary,
    }


def full_rebuild(conn: sqlite3.Connection, settings: Settings) -> dict:
    """Rebuild every derived artifact from durable samples + topology.

    Used by crash recovery: base facts (samples, edges, alerts history) are
    committed durably, and the derived tables are simply regenerated.
    """
    invalidate_all_derived(conn)
    rebuilt = []
    for service in sorted(declared_services(conn)):
        rebuilt.append(aggregates.rebuild_service_derived(conn, settings, service))
    rca_summary = rca.rebuild_all_rca(conn, settings)
    return {
        "rebuilt_services": rebuilt,
        "rca": rca_summary,
        "finished_at_ms": int(time.time() * 1000),
    }
