"""Derived layer one/two: quantile aggregation index, alerts and suppression.

All functions in this module only touch the database through the caller's open
write transaction, so cascade invalidation and rebuild are atomic together.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass

from .config import Settings
from .errors import InvalidTransition
from .fixedpoint import SCALE, from_float, to_string
from .stats import good_rate_fixed, quantile_map

RULE_GOOD_RATE = "GOOD_RATE"
RULE_LATENCY_P99 = "LATENCY_P99"

# Legal alert state machine.  Anything not listed is rejected with
# E_INVALID_TRANSITION and leaves persisted state untouched.
LEGAL_TRANSITIONS = {
    "FIRING": {"SUPPRESSED", "RESOLVED"},
    "SUPPRESSED": {"FIRING", "RESOLVED"},
    "RESOLVED": {"FIRING"},
}


def alert_fingerprint(service: str, metric: str, rule: str) -> str:
    return hashlib.sha256(f"{service}|{metric}|{rule}".encode("utf-8")).hexdigest()[:16]


def alert_id(service: str, metric: str, rule: str) -> str:
    return hashlib.sha256(f"alert:{service}|{metric}|{rule}".encode("utf-8")).hexdigest()[:16]


@dataclass
class WindowAggregate:
    service: str
    metric: str
    sample_lo: int
    sample_hi: int
    sample_count: int
    good_rate: int
    quantiles: dict[str, int]
    window_max_ts: int


def _window_rows(conn: sqlite3.Connection, service: str, metric: str, window_size: int):
    return conn.execute(
        "SELECT sample_version, ts_ms, is_good, latency_us FROM samples "
        "WHERE service=? AND metric=? ORDER BY ts_ms DESC, id DESC LIMIT ?",
        (service, metric, window_size),
    ).fetchall()


def build_window_aggregate(conn, settings: Settings, service: str, metric: str) -> WindowAggregate | None:
    rows = _window_rows(conn, service, metric, settings.window_size)
    if len(rows) < settings.min_samples:
        return None
    ordered = list(reversed(rows))  # ascending for quantile selection
    latencies = sorted(row["latency_us"] for row in ordered)
    good_flags = [row["is_good"] for row in ordered]
    versions = [row["sample_version"] for row in ordered]
    return WindowAggregate(
        service=service,
        metric=metric,
        sample_lo=min(versions),
        sample_hi=max(versions),
        sample_count=len(ordered),
        good_rate=good_rate_fixed(good_flags),
        quantiles=quantile_map(latencies, settings.quantile_points),
        window_max_ts=max(row["ts_ms"] for row in ordered),
    )


def _record_event(conn, alert_id_value: str, transition: str, from_status, to_status, at_ms, detail=None):
    conn.execute(
        "INSERT INTO alert_events(alert_id, transition, from_status, to_status, at_ms, detail_json) "
        "VALUES(?, ?, ?, ?, ?, ?)",
        (alert_id_value, transition, from_status, to_status, at_ms,
         json.dumps(detail, sort_keys=True) if detail else None),
    )


def _upsert_suppression(conn, fingerprint, service, level, alert_id_value, at_ms, window_end, count):
    conn.execute(
        "INSERT INTO suppression_index(fingerprint, service, level, alert_id, suppressed_count, "
        "window_start_ms, window_end_ms, built_at_ms) VALUES(?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(fingerprint, service, level) DO UPDATE SET alert_id=excluded.alert_id, "
        "suppressed_count=excluded.suppressed_count, window_start_ms=excluded.window_start_ms, "
        "window_end_ms=excluded.window_end_ms, built_at_ms=excluded.built_at_ms",
        (fingerprint, service, level, alert_id_value, count, at_ms, window_end, int(time.time() * 1000)),
    )


def _evaluate_rule(conn, settings: Settings, agg: WindowAggregate, rule: str):
    threshold = settings.threshold_for(agg.service, agg.metric)
    breach = False
    if rule == RULE_LATENCY_P99:
        breach = agg.quantiles["p99"] > threshold.latency_max_us
    else:
        breach = agg.good_rate < from_float(threshold.good_rate_min)
    if not breach:
        return

    fingerprint = alert_fingerprint(agg.service, agg.metric, rule)
    alert_id_value = alert_id(agg.service, agg.metric, rule)
    at_ms = agg.window_max_ts
    existing = conn.execute(
        "SELECT * FROM alerts WHERE fingerprint=? AND service=? AND level=?",
        (fingerprint, agg.service, threshold.level),
    ).fetchone()

    if existing is None:
        conn.execute(
            "INSERT INTO alerts(id, fingerprint, service, metric, level, rule, status, "
            "first_seen_ms, last_seen_ms, sample_hi) VALUES(?, ?, ?, ?, ?, ?, 'FIRING', ?, ?, ?)",
            (alert_id_value, fingerprint, agg.service, agg.metric, threshold.level, rule,
             at_ms, at_ms, agg.sample_hi),
        )
        _record_event(conn, alert_id_value, "OPEN", None, "FIRING", at_ms)
        return

    current = existing["status"]
    if current == "RESOLVED":
        conn.execute(
            "UPDATE alerts SET status='FIRING', first_seen_ms=?, last_seen_ms=?, sample_hi=? WHERE id=?",
            (at_ms, at_ms, agg.sample_hi, alert_id_value),
        )
        conn.execute("DELETE FROM suppression_index WHERE fingerprint=? AND service=? AND level=?",
                     (fingerprint, agg.service, threshold.level))
        _record_event(conn, alert_id_value, "REOPEN", "RESOLVED", "FIRING", at_ms)
    elif current == "FIRING":
        delta = at_ms - existing["last_seen_ms"]
        if delta <= settings.suppression_window_ms:
            conn.execute(
                "UPDATE alerts SET status='SUPPRESSED', last_seen_ms=?, sample_hi=? WHERE id=?",
                (at_ms, agg.sample_hi, alert_id_value),
            )
            _upsert_suppression(conn, fingerprint, agg.service, threshold.level,
                                alert_id_value, existing["first_seen_ms"], at_ms, 1)
            _record_event(conn, alert_id_value, "SUPPRESS", "FIRING", "SUPPRESSED", at_ms,
                          {"suppressed_count": 1})
        else:
            conn.execute(
                "UPDATE alerts SET last_seen_ms=?, sample_hi=? WHERE id=?",
                (at_ms, agg.sample_hi, alert_id_value),
            )
    else:  # SUPPRESSED
        delta = at_ms - existing["last_seen_ms"]
        if delta <= settings.suppression_window_ms:
            row = conn.execute(
                "SELECT suppressed_count FROM suppression_index WHERE fingerprint=? AND service=? AND level=?",
                (fingerprint, agg.service, threshold.level),
            ).fetchone()
            count = (row["suppressed_count"] if row else 0) + 1
            _upsert_suppression(conn, fingerprint, agg.service, threshold.level,
                                alert_id_value, existing["first_seen_ms"], at_ms, count)
            conn.execute(
                "UPDATE alerts SET last_seen_ms=?, sample_hi=? WHERE id=?",
                (at_ms, agg.sample_hi, alert_id_value),
            )
            _record_event(conn, alert_id_value, "SUPPRESS", "SUPPRESSED", "SUPPRESSED", at_ms,
                          {"suppressed_count": count})
        else:
            conn.execute(
                "UPDATE alerts SET status='FIRING', first_seen_ms=?, last_seen_ms=?, sample_hi=? WHERE id=?",
                (at_ms, at_ms, agg.sample_hi, alert_id_value),
            )
            conn.execute("DELETE FROM suppression_index WHERE fingerprint=? AND service=? AND level=?",
                         (fingerprint, agg.service, threshold.level))
            _record_event(conn, alert_id_value, "REALERT", "SUPPRESSED", "FIRING", at_ms)


def _resolve_cleared_alerts(conn, service: str, metric: str, at_ms: int):
    rows = conn.execute(
        "SELECT id, status FROM alerts WHERE service=? AND metric=? AND status IN ('FIRING','SUPPRESSED')",
        (service, metric),
    ).fetchall()
    for row in rows:
        conn.execute("UPDATE alerts SET status='RESOLVED', last_seen_ms=? WHERE id=?", (at_ms, row["id"]))
        _record_event(conn, row["id"], "RESOLVE", row["status"], "RESOLVED", at_ms)


def _resolve_if_healthy(conn, settings: Settings, agg: WindowAggregate) -> None:
    """Resolve open alerts when the current window breaches neither rule."""
    threshold = settings.threshold_for(agg.service, agg.metric)
    healthy = (
        agg.good_rate >= from_float(threshold.good_rate_min)
        and agg.quantiles["p99"] <= threshold.latency_max_us
    )
    if not healthy:
        return
    rows = conn.execute(
        "SELECT id, status FROM alerts WHERE service=? AND metric=? "
        "AND status IN ('FIRING','SUPPRESSED')",
        (agg.service, agg.metric),
    ).fetchall()
    for row in rows:
        conn.execute(
            "UPDATE alerts SET status='RESOLVED', last_seen_ms=? WHERE id=?",
            (agg.window_max_ts, row["id"]),
        )
        _record_event(conn, row["id"], "RESOLVE", row["status"], "RESOLVED",
                      agg.window_max_ts, {"reason": "window_healthy"})


def rebuild_service_derived(conn, settings: Settings, service: str) -> dict:
    """Rebuild quantile + alert artifacts for one service (same transaction)."""
    now_ms = int(time.time() * 1000)
    metrics = [
        row["metric"]
        for row in conn.execute(
            "SELECT DISTINCT metric FROM samples WHERE service=? ORDER BY metric", (service,)
        )
    ]
    quantile_entries = 0
    alert_evaluations = 0
    for metric in metrics:
        agg = build_window_aggregate(conn, settings, service, metric)
        if agg is None:
            _resolve_cleared_alerts(conn, service, metric, now_ms)
            continue
        conn.execute(
            "INSERT INTO quantile_index(service, metric, sample_lo, sample_hi, window_size, "
            "sample_count, good_rate, quantiles_json, built_at_ms) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(service, metric) DO UPDATE SET sample_lo=excluded.sample_lo, "
            "sample_hi=excluded.sample_hi, window_size=excluded.window_size, "
            "sample_count=excluded.sample_count, good_rate=excluded.good_rate, "
            "quantiles_json=excluded.quantiles_json, built_at_ms=excluded.built_at_ms",
            (service, metric, agg.sample_lo, agg.sample_hi, settings.window_size,
             agg.sample_count, agg.good_rate,
             json.dumps(agg.quantiles, sort_keys=True), now_ms),
        )
        quantile_entries += 1
        conn.execute(
            "INSERT INTO service_refs(service, artifact, artifact_id, depends_on, updated_at_ms) "
            "VALUES(?, 'quantile', ?, NULL, ?) "
            "ON CONFLICT(service, artifact, artifact_id) DO UPDATE SET updated_at_ms=excluded.updated_at_ms",
            (service, metric, now_ms),
        )
        _evaluate_rule(conn, settings, agg, RULE_GOOD_RATE)
        _evaluate_rule(conn, settings, agg, RULE_LATENCY_P99)
        alert_evaluations += 2
        _resolve_if_healthy(conn, settings, agg)

    for row in conn.execute(
        "SELECT fingerprint, alert_id, level FROM suppression_index WHERE service=?", (service,)
    ):
        conn.execute(
            "INSERT INTO service_refs(service, artifact, artifact_id, depends_on, updated_at_ms) "
            "VALUES(?, 'suppression', ?, ?, ?) "
            "ON CONFLICT(service, artifact, artifact_id) DO UPDATE SET updated_at_ms=excluded.updated_at_ms",
            (service, row["fingerprint"], row["alert_id"], now_ms),
        )
    return {"service": service, "quantile_entries": quantile_entries, "rule_evaluations": alert_evaluations}


def manual_transition(conn, alert_id_value: str, to_status: str) -> dict:
    row = conn.execute("SELECT id, status FROM alerts WHERE id=?", (alert_id_value,)).fetchone()
    if row is None:
        from .errors import NotFound

        raise NotFound("alert not found", {"alert_id": alert_id_value})
    current = row["status"]
    if to_status not in LEGAL_TRANSITIONS.get(current, set()):
        raise InvalidTransition(
            f"illegal transition {current} -> {to_status}",
            {"alert_id": alert_id_value, "from": current, "to": to_status,
             "allowed": sorted(LEGAL_TRANSITIONS.get(current, set()))},
        )
    at_ms = int(time.time() * 1000)
    conn.execute("UPDATE alerts SET status=? WHERE id=?", (to_status, alert_id_value))
    _record_event(conn, alert_id_value, "MANUAL", current, to_status, at_ms)
    return {"alert_id": alert_id_value, "from": current, "to": to_status, "at_ms": at_ms}


def numeric_contract_note() -> dict:
    return {
        "good_rate_scale": SCALE,
        "good_rate_rendering": to_string(0),
        "quantile_method": "nearest_rank",
        "quantile_rule": "rank=ceil(p*n), value=sorted_ascending[rank-1]",
    }
