"""SLI sample parsing, deterministic in-batch de-duplication and persistence."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from typing import Any

from .errors import SampleInvalid, StaleBase, TopologyInvalid
from .topology import current_version as topo_current_version
from .topology import declared_services

REQUIRED_FIELDS = ("ts_ms", "service", "metric", "is_good", "latency_us")


def canonical_payload_hash(payload: dict[str, Any]) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def parse_and_dedup(raw_records: list[dict], known_services: set[str]):
    """Validate every record and de-duplicate on (service, metric, ts_ms).

    The first occurrence in input order wins; later duplicates are counted.
    Raises :class:`SampleInvalid` on the first malformed record — the caller
    guarantees that nothing is persisted in that case.
    """
    if not isinstance(raw_records, list) or not raw_records:
        raise SampleInvalid("records must be a non-empty list", {"received": type(raw_records).__name__})

    kept: list[dict] = []
    seen: set[tuple[str, str, int]] = set()
    duplicate_count = 0
    for index, record in enumerate(raw_records):
        if not isinstance(record, dict):
            raise SampleInvalid("record must be an object", {"index": index})
        missing = [field for field in REQUIRED_FIELDS if field not in record]
        if missing:
            raise SampleInvalid("record is missing required fields", {"index": index, "missing": missing})
        try:
            ts_ms = int(record["ts_ms"])
            latency_us = int(record["latency_us"])
        except (TypeError, ValueError):
            raise SampleInvalid(
                "ts_ms and latency_us must be integers",
                {"index": index, "ts_ms": record.get("ts_ms"), "latency_us": record.get("latency_us")},
            )
        service = record["service"]
        metric = record["metric"]
        if not isinstance(service, str) or not service or not isinstance(metric, str) or not metric:
            raise SampleInvalid("service and metric must be non-empty strings", {"index": index})
        is_good = record["is_good"]
        if not isinstance(is_good, bool):
            raise SampleInvalid("is_good must be a boolean", {"index": index, "is_good": is_good})
        if latency_us < 0:
            raise SampleInvalid("latency_us must be non-negative", {"index": index, "latency_us": latency_us})
        if service not in known_services:
            raise SampleInvalid(
                "service is not declared in the current topology",
                {"index": index, "service": service, "known_services": sorted(known_services)},
            )
        key = (service, metric, ts_ms)
        if key in seen:
            duplicate_count += 1
            continue
        seen.add(key)
        kept.append(
            {
                "ts_ms": ts_ms,
                "service": service,
                "metric": metric,
                "is_good": 1 if is_good else 0,
                "latency_us": latency_us,
            }
        )
    return kept, duplicate_count


def current_sample_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COALESCE(MAX(new_version), 0) AS v FROM sample_batches").fetchone()
    return int(row["v"])


def affected_services(records: list[dict]) -> set[str]:
    return {record["service"] for record in records}


def insert_batch(
    conn: sqlite3.Connection,
    payload: dict,
    records: list[dict],
    duplicate_count: int,
    payload_hash: str,
) -> tuple[int, int]:
    """Persist a validated batch inside the caller's open write transaction."""
    base_version = payload.get("base_version")
    version = current_sample_version(conn)
    if base_version is None or int(base_version) != version:
        raise StaleBase(
            "sample batch base_version is not the current sample version",
            {"base_version": base_version, "current_version": version},
        )

    commit_id = payload["commit_id"]
    new_version = version + 1
    now_ms = int(time.time() * 1000)

    # Guard against a key that already exists across the whole table.
    clash = conn.execute(
        "SELECT 1 FROM samples WHERE (service, metric, ts_ms) IN "
        "(VALUES " + ",".join("(?, ?, ?)" for _ in records) + ")",
        [value for record in records for value in (record["service"], record["metric"], record["ts_ms"])],
    ).fetchall() if records else []
    if clash:
        raise SampleInvalid(
            "one or more records already exist at (service, metric, ts_ms)",
            {"clashing_keys": len(clash)},
        )

    conn.execute(
        "INSERT INTO sample_batches(commit_id, base_version, new_version, payload_hash, "
        "record_count, duplicate_count, created_at_ms) VALUES(?, ?, ?, ?, ?, ?, ?)",
        (commit_id, version, new_version, payload_hash, len(records), duplicate_count, now_ms),
    )
    conn.executemany(
        "INSERT INTO samples(sample_version, batch_commit, ts_ms, service, metric, "
        "is_good, latency_us) VALUES(?, ?, ?, ?, ?, ?, ?)",
        [
            (new_version, commit_id, r["ts_ms"], r["service"], r["metric"], r["is_good"], r["latency_us"])
            for r in records
        ],
    )
    return version, new_version


def require_topology(conn: sqlite3.Connection) -> set[str]:
    if topo_current_version(conn) == 0:
        raise TopologyInvalid("a topology snapshot must be submitted before samples")
    return declared_services(conn)
