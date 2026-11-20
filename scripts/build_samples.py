from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from alert_sieve.domain import (  # noqa: E402
    Alert,
    FingerprintMeta,
    SuppressionRule,
    alert_increment_checksum,
    alert_snapshot_checksum,
    rules_increment_checksum,
    rules_snapshot_checksum,
    topology_increment_checksum,
    topology_snapshot_checksum,
)
from alert_sieve.util import canonical_json  # noqa: E402

SAMPLES = ROOT / "samples"
BASE_TS = 1_735_689_600_000  # 2025-01-01T00:00:00.000Z


def write_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def alert_record(
    ts: int,
    fingerprint: str,
    service: str,
    resource: str,
    severity: str,
    labels: dict[str, str],
) -> dict:
    return {
        "timestamp_ms": ts,
        "fingerprint": fingerprint,
        "service": service,
        "resource": resource,
        "severity": severity,
        "labels": labels,
    }


ALERT_RECORDS = [
    alert_record(BASE_TS + 0, "ledger-db-connection", "ledger", "ledger-primary", "CRITICAL", {"env": "prod", "kind": "database", "region": "cn-east"}),
    alert_record(BASE_TS + 30_000, "ledger-db-connection", "ledger", "ledger-primary", "CRITICAL", {"env": "prod", "kind": "database", "region": "cn-east"}),
    alert_record(BASE_TS + 5_000, "payment-latency", "payment", "payment-api", "ERROR", {"env": "prod", "kind": "latency", "region": "cn-east"}),
    alert_record(BASE_TS + 20_000, "payment-latency", "payment", "payment-api", "ERROR", {"env": "prod", "kind": "latency", "region": "cn-east"}),
    alert_record(BASE_TS + 10_000, "checkout-5xx", "checkout", "checkout-api", "CRITICAL", {"env": "prod", "kind": "http", "region": "cn-east"}),
    alert_record(BASE_TS + 40_000, "checkout-5xx", "checkout", "checkout-api", "CRITICAL", {"env": "prod", "kind": "http", "region": "cn-east"}),
    alert_record(BASE_TS + 15_000, "gateway-5xx", "edge-gateway", "gateway-api", "CRITICAL", {"env": "prod", "kind": "http", "region": "cn-east"}),
    alert_record(BASE_TS + 20_000, "gateway-5xx", "edge-gateway", "gateway-api", "CRITICAL", {"env": "prod", "kind": "http", "region": "cn-east"}),
    alert_record(BASE_TS + 7_000, "inventory-lag", "inventory", "inventory-queue", "WARNING", {"env": "prod", "kind": "queue", "region": "cn-east"}),
    alert_record(BASE_TS + 20_000, "warehouse-lag", "warehouse", "warehouse-queue", "WARNING", {"env": "prod", "kind": "queue", "region": "cn-east"}),
]


def make_alerts() -> list[Alert]:
    batch = "seed-alerts-v1"
    return [
        Alert(
            batch_id=batch,
            line_no=index + 1,
            timestamp_ms=record["timestamp_ms"],
            fingerprint=record["fingerprint"],
            service=record["service"],
            resource=record["resource"],
            severity=record["severity"],
            labels=record["labels"],
        )
        for index, record in enumerate(ALERT_RECORDS)
    ]


def main() -> None:
    SAMPLES.mkdir(parents=True, exist_ok=True)
    alerts = make_alerts()
    snapshot_checksum = alert_snapshot_checksum(1, alerts)
    header = {
        "record_type": "header",
        "kind": "alert_snapshot",
        "version": 1,
        "batch_id": "seed-alerts-v1",
        "checksum": snapshot_checksum,
    }
    lines = [canonical_json(header)]
    for alert in alerts:
        lines.append(canonical_json({"record_type": "alert", **alert.content_payload()}))
    (SAMPLES / "alerts_v1.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    write_json(
        SAMPLES / "alerts_v1_snapshot.json",
        {
            "kind": "alert_snapshot",
            "version": 1,
            "batch_id": "seed-alerts-v1",
            "checksum": snapshot_checksum,
            "alerts": [{"line_no": alert.line_no, **alert.content_payload()} for alert in alerts],
        },
    )

    increment_batch = "seed-alerts-inc-v2"
    inc_raw = alert_record(
        BASE_TS + 12_000,
        "inventory-lag",
        "inventory",
        "inventory-queue",
        "WARNING",
        {"env": "prod", "kind": "queue", "region": "cn-east"},
    )
    inc_alert = Alert(
        batch_id=increment_batch,
        line_no=1,
        timestamp_ms=inc_raw["timestamp_ms"],
        fingerprint=inc_raw["fingerprint"],
        service=inc_raw["service"],
        resource=inc_raw["resource"],
        severity=inc_raw["severity"],
        labels=inc_raw["labels"],
    )
    inc_fp = FingerprintMeta("inventory-lag", "inventory", inc_alert.labels)
    inc_checksum = alert_increment_checksum(increment_batch, 1, 2, [inc_alert], [inc_fp])
    write_json(
        SAMPLES / "alerts_increment_v2.json",
        {
            "kind": "alert_increment",
            "batch_id": increment_batch,
            "base_version": 1,
            "version": 2,
            "checksum": inc_checksum,
            "alerts": [inc_raw],
            "affected_fingerprints": [inc_fp.payload()],
        },
    )

    edges_v1 = [
        ("edge-gateway", "checkout"),
        ("checkout", "payment"),
        ("checkout", "inventory"),
        ("payment", "ledger"),
        ("inventory", "warehouse"),
    ]
    nodes_v1 = ["edge-gateway", "checkout", "payment", "ledger", "inventory", "warehouse"]
    topo_checksum = topology_snapshot_checksum(1, nodes_v1, edges_v1)
    write_json(
        SAMPLES / "topology_v1.json",
        {
            "kind": "topology_snapshot",
            "version": 1,
            "checksum": topo_checksum,
            "nodes": nodes_v1,
            "edges": [{"source": source, "target": target} for source, target in edges_v1],
        },
    )

    rules_v1 = [
        SuppressionRule("r-ledger-db", "ledger", "CRITICAL", {"kind": "database"}, 60_000, True),
        SuppressionRule("r-payment-latency", "payment", "ERROR", {"kind": "latency"}, 120_000, True),
        SuppressionRule("r-checkout-5xx", "checkout", "CRITICAL", {"env": "prod"}, 30_000, True),
        SuppressionRule("r-gateway-5xx", "edge-gateway", "CRITICAL", {"kind": "http"}, 10_000, True),
        SuppressionRule("r-warning-global", "*", "WARNING", {"env": "prod"}, 15_000, True),
    ]
    rules_checksum = rules_snapshot_checksum(1, 60_000, rules_v1)
    write_json(
        SAMPLES / "rules_v1.json",
        {
            "kind": "rules_snapshot",
            "version": 1,
            "checksum": rules_checksum,
            "default_window_ms": 60_000,
            "rules": [rule.payload() for rule in rules_v1],
        },
    )

    topo_upserts_v2 = [("checkout", "notifications")]
    topo_removes_v2 = [("checkout", "payment")]
    topo_checksum_v2 = topology_increment_checksum(1, 2, topo_upserts_v2, topo_removes_v2, [], [])
    write_json(
        SAMPLES / "topology_increment_v2.json",
        {
            "kind": "topology_increment",
            "base_version": 1,
            "version": 2,
            "checksum": topo_checksum_v2,
            "upsert_edges": [{"source": source, "target": target} for source, target in topo_upserts_v2],
            "remove_edges": [{"source": source, "target": target} for source, target in topo_removes_v2],
            "upsert_nodes": [],
            "remove_nodes": [],
        },
    )

    checkout_rule_v2 = SuppressionRule("r-checkout-5xx", "checkout", "CRITICAL", {"env": "prod"}, 1_000, True)
    rules_checksum_v2 = rules_increment_checksum(1, 2, None, [checkout_rule_v2], [])
    write_json(
        SAMPLES / "rules_increment_v2.json",
        {
            "kind": "rules_increment",
            "base_version": 1,
            "version": 2,
            "checksum": rules_checksum_v2,
            "default_window_ms": None,
            "upsert_rules": [checkout_rule_v2.payload()],
            "remove_rules": [],
        },
    )

    topo_upserts_v3 = [("checkout", "payment")]
    topo_removes_v3 = [("checkout", "notifications")]
    topo_checksum_v3 = topology_increment_checksum(2, 3, topo_upserts_v3, topo_removes_v3, [], [])
    write_json(
        SAMPLES / "topology_increment_v3_crash.json",
        {
            "kind": "topology_increment",
            "base_version": 2,
            "version": 3,
            "checksum": topo_checksum_v3,
            "upsert_edges": [{"source": source, "target": target} for source, target in topo_upserts_v3],
            "remove_edges": [{"source": source, "target": target} for source, target in topo_removes_v3],
            "upsert_nodes": [],
            "remove_nodes": [],
        },
    )

    print("wrote samples to", SAMPLES)


if __name__ == "__main__":
    main()
