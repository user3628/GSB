"""Root-cause candidate association scoring with a strict fixed-point contract.

Score composition (every value a fixed-point integer, scale 10**6)::

    hop_score(h)  = 1 / (1 + h)                 # half-to-even integer division
    time_score(d) = clamp(1 - d / halo, 0, 1)   # halo = RCA_TIME_HALO_MS
    score         = W_HOPS * hop_score + W_TIME * time_score

All arithmetic goes through :mod:`fixedpoint`, which checks an explicit bound
on each step.  Ranking uses a relative-tolerance comparison before applying the
stable tie-break order (ascending hops, then service name), so equal inputs
produce byte-identical result tables on every run.
"""
from __future__ import annotations

import json
import sqlite3
import time

from . import fixedpoint as fp
from .config import MAX_HOPS, MAX_TIME_DELTA_MS, RCA_TIME_HALO_MS, Settings
from .errors import RebuildFailed
from .fixedpoint import FixedPointError, SCALE
from .topology import hop_distances

# Relative tolerance for "equal score" grouping during ranking: 1e-4.
SCORE_REL_TOL = 100


def _hop_score(hops: int) -> int:
    if hops < 0 or hops > MAX_HOPS:
        raise FixedPointError(f"hop count {hops} outside [0, {MAX_HOPS}]")
    return fp.divide_small(SCALE, hops + 1)


def _time_score(delta_ms: int) -> int:
    if delta_ms < 0 or delta_ms > MAX_TIME_DELTA_MS:
        raise FixedPointError(f"time delta {delta_ms} outside [0, {MAX_TIME_DELTA_MS}]")
    consumed = fp.divide_small(fp.from_int(delta_ms), RCA_TIME_HALO_MS)
    return fp.clamp(fp.sub(SCALE, consumed))


def score_candidate(hops: int, time_delta_ms: int, weights) -> dict:
    """Pure function: deterministic scoring, reused by tests and persistence."""
    hop_score = _hop_score(hops)
    time_score = _time_score(time_delta_ms)
    weighted_hops = fp.mul(weights[0], hop_score)
    weighted_time = fp.mul(weights[1], time_score)
    score = fp.add(weighted_hops, weighted_time)
    return {
        "hops": hops,
        "time_delta_ms": time_delta_ms,
        "hop_score": hop_score,
        "time_score": time_score,
        "score": score,
    }


def _candidate_latest_ts(conn: sqlite3.Connection, service: str) -> int | None:
    row = conn.execute(
        "SELECT MAX(ts_ms) AS ts FROM samples WHERE service=?", (service,)
    ).fetchone()
    return row["ts"] if row and row["ts"] is not None else None


def _stable_ranked(candidates: list[dict]) -> list[dict]:
    """Rank with relative tolerance then deterministic tie-break keys."""
    ordered = sorted(candidates, key=lambda item: (-item["score"], item["hops"], item["candidate"]))
    ranked: list[dict] = []
    rank = 0
    group_anchor: int | None = None
    for item in ordered:
        if group_anchor is None or not fp.nearly_equal(group_anchor, item["score"], SCORE_REL_TOL):
            rank += 1
            group_anchor = item["score"]
        item["rank"] = rank
        ranked.append(item)
    return ranked


def rebuild_alert_rca(conn: sqlite3.Connection, settings: Settings, alert: sqlite3.Row) -> int:
    service = alert["service"]
    distances = hop_distances(conn, service)
    from .config import W_HOPS, W_TIME

    rows_out: list[dict] = []
    for candidate, hops in distances.items():
        latest = _candidate_latest_ts(conn, candidate)
        if latest is None:
            continue
        delta = abs(int(alert["last_seen_ms"]) - int(latest))
        try:
            scored = score_candidate(hops, delta, (W_HOPS, W_TIME))
        except FixedPointError as exc:
            raise RebuildFailed("fixed-point bound violated while scoring candidate",
                                {"alert_id": alert["id"], "candidate": candidate, "detail": str(exc)})
        scored["candidate"] = candidate
        rows_out.append(scored)

    ranked = _stable_ranked(rows_out)
    now_ms = int(time.time() * 1000)
    conn.execute("DELETE FROM rca_scores WHERE alert_id=?", (alert["id"],))
    for item in ranked:
        conn.execute(
            "INSERT INTO rca_scores(alert_id, candidate, hops, time_delta_ms, hop_score, "
            "time_score, score, rank, built_at_ms) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (alert["id"], item["candidate"], item["hops"], item["time_delta_ms"],
             item["hop_score"], item["time_score"], item["score"], item["rank"], now_ms),
        )
        conn.execute(
            "INSERT INTO service_refs(service, artifact, artifact_id, depends_on, updated_at_ms) "
            "VALUES(?, 'rca', ?, ?, ?) "
            "ON CONFLICT(service, artifact, artifact_id) DO UPDATE SET "
            "depends_on=excluded.depends_on, updated_at_ms=excluded.updated_at_ms",
            (item["candidate"], alert["id"], service, now_ms),
        )
    return len(ranked)


def rebuild_all_rca(conn: sqlite3.Connection, settings: Settings) -> dict:
    alerts = conn.execute(
        "SELECT * FROM alerts ORDER BY id"
    ).fetchall()
    total_candidates = 0
    for alert in alerts:
        total_candidates += rebuild_alert_rca(conn, settings, alert)
    return {"alerts_scored": len(alerts), "candidate_rows": total_candidates}


def numeric_contract_note() -> dict:
    """Documented rounding/error note attached to every score response."""
    return {
        "scale": SCALE,
        "representation": "signed integer fixed-point, 6 fractional digits",
        "rounding": "round-half-to-even on rescale; final render truncates at 6 decimals (lossless)",
        "max_cumulative_error": fp.to_string(2),  # <= 2 ulp across the two weighted products
        "ulp": fp.to_string(1),
        "comparison": f"relative tolerance {fp.to_string(SCORE_REL_TOL)}, ties broken by hops then name",
        "determinism": "pure integer arithmetic; repeated inputs yield bit-identical outputs",
        "bounds": {"max_hops": MAX_HOPS, "max_time_delta_ms": MAX_TIME_DELTA_MS},
    }


def render_score(row: sqlite3.Row) -> dict:
    return {
        "candidate": row["candidate"],
        "rank": row["rank"],
        "hops": row["hops"],
        "time_delta_ms": row["time_delta_ms"],
        "hop_score": fp.to_string(row["hop_score"]),
        "time_score": fp.to_string(row["time_score"]),
        "score": fp.to_string(row["score"]),
    }
