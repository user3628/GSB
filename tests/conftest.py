from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from alert_sieve.domain import (  # noqa: E402
    Alert,
    FingerprintMeta,
    alert_increment_checksum,
    alert_snapshot_checksum,
)
from alert_sieve.samples import load_alerts_jsonl, load_json  # noqa: E402
from alert_sieve.store import Store  # noqa: E402

SAMPLES = ROOT / "samples"


@pytest.fixture
def store(tmp_path: Path) -> Store:
    return Store(tmp_path / "test.db")


def load_baseline(store: Store) -> dict:
    alert_result = store.submit_alert_snapshot(load_alerts_jsonl(SAMPLES / "alerts_v1.jsonl"))
    topology_result = store.submit_topology_snapshot(load_json(SAMPLES / "topology_v1.json"))
    rules_result = store.submit_rules_snapshot(load_json(SAMPLES / "rules_v1.json"))
    return {
        "alerts": alert_result,
        "topology": topology_result,
        "rules": rules_result,
    }


def make_alert_increment(
    batch_id: str,
    base_version: int,
    version: int,
    alert_rows: list[dict],
    affected_fingerprints: list[dict] | None = None,
) -> dict:
    alerts = [
        Alert(
            batch_id=batch_id,
            line_no=index + 1,
            timestamp_ms=row["timestamp_ms"],
            fingerprint=row["fingerprint"],
            service=row["service"],
            resource=row["resource"],
            severity=row["severity"],
            labels=row.get("labels", {}),
        )
        for index, row in enumerate(alert_rows)
    ]
    fps = [
        FingerprintMeta(row["fingerprint"], row["service"], row.get("labels", {}))
        for row in (affected_fingerprints or [])
    ]
    return {
        "kind": "alert_increment",
        "batch_id": batch_id,
        "base_version": base_version,
        "version": version,
        "checksum": alert_increment_checksum(batch_id, base_version, version, alerts, fps),
        "alerts": [{key: value for key, value in row.items()} for row in alert_rows],
        "affected_fingerprints": affected_fingerprints or [],
    }


def make_alert_snapshot(version: int, batch_id: str, alert_rows: list[dict]) -> dict:
    alerts = [
        Alert(
            batch_id=batch_id,
            line_no=index + 1,
            timestamp_ms=row["timestamp_ms"],
            fingerprint=row["fingerprint"],
            service=row["service"],
            resource=row["resource"],
            severity=row["severity"],
            labels=row.get("labels", {}),
        )
        for index, row in enumerate(alert_rows)
    ]
    return {
        "kind": "alert_snapshot",
        "version": version,
        "batch_id": batch_id,
        "checksum": alert_snapshot_checksum(version, alerts),
        "alerts": [{"line_no": alert.line_no, **alert.content_payload()} for alert in alerts],
    }


def alert_row(
    ts: int,
    fingerprint: str,
    service: str,
    *,
    severity: str = "CRITICAL",
    resource: str | None = None,
    labels: dict | None = None,
) -> dict:
    return {
        "timestamp_ms": ts,
        "fingerprint": fingerprint,
        "service": service,
        "resource": resource or f"{service}-resource",
        "severity": severity,
        "labels": labels or {"env": "prod"},
    }
