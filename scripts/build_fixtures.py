from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from slowquery.util import sha256_canonical  # noqa: E402


STATUSES = ["paid", "pending", "refunded", "cancelled"]
REGIONS = ["north", "south", "east", "west"]
TIERS = ["gold", "silver", "bronze"]
BASE_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)


def mixer(value: int, salt: int) -> int:
    x = (value * 0x9E3779B1 + salt * 0x85EBCA77) & 0xFFFFFFFF
    x ^= x >> 16
    x = (x * 0x7FEB352D) & 0xFFFFFFFF
    x ^= x >> 15
    x = (x * 0x846CA68B) & 0xFFFFFFFF
    x ^= x >> 16
    return x


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def build_sample_database(path: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(path) + suffix)
        if candidate.exists():
            candidate.unlink()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(
            """
            PRAGMA page_size=4096;
            PRAGMA journal_mode=DELETE;
            CREATE TABLE customers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                tier TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                region TEXT NOT NULL,
                amount REAL NOT NULL,
                note TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX idx_customers_tier ON customers(tier);
            CREATE INDEX idx_orders_status ON orders(status);
            CREATE INDEX idx_orders_created_at ON orders(created_at);
            CREATE INDEX idx_orders_region_status ON orders(region, status);
            CREATE INDEX idx_orders_note_status ON orders(note, status);
            """
        )
        customers = []
        for customer_id in range(1, 51):
            customers.append(
                (
                    customer_id,
                    f"customer-{customer_id:03d}",
                    TIERS[mixer(customer_id, 11) % len(TIERS)],
                    iso(BASE_TIME - timedelta(days=customer_id * 3)),
                )
            )
        conn.executemany(
            "INSERT INTO customers(id, name, tier, created_at) VALUES (?, ?, ?, ?)", customers
        )
        orders = []
        for order_id in range(1, 1001):
            booking_id = order_id - 1
            customer_id = mixer(booking_id, 21) % 50 + 1
            status = STATUSES[mixer(booking_id, 22) % 4]
            region = REGIONS[mixer(booking_id, 23) % 4]
            amount = round(((mixer(booking_id, 24) % 50000) + 1) / 100.0, 2)
            note = f"note-{order_id:04d}"
            created_at = iso(BASE_TIME + timedelta(minutes=order_id * 7))
            orders.append(
                (order_id, customer_id, status, region, amount, note, created_at)
            )
        conn.executemany(
            """
            INSERT INTO orders(id, customer_id, status, region, amount, note, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            orders,
        )
        conn.commit()
        conn.execute("VACUUM")
    finally:
        conn.close()


def snapshot_payload(version: int, generated_at: str) -> dict[str, Any]:
    tables = {
        "orders": {
            "row_count": 1000,
            "columns": {
                "id": {"ndv": 1000},
                "customer_id": {"ndv": 50},
                "status": {"ndv": 4},
                "region": {"ndv": 4},
                "amount": {"ndv": 1000},
                "note": {"ndv": 1000},
                "created_at": {"ndv": 1000},
            },
            "indexes": [
                {"name": "idx_orders_status", "columns": ["status"], "unique": False},
                {"name": "idx_orders_created_at", "columns": ["created_at"], "unique": False},
                {
                    "name": "idx_orders_region_status",
                    "columns": ["region", "status"],
                    "unique": False,
                },
                {
                    "name": "idx_orders_note_status",
                    "columns": ["note", "status"],
                    "unique": False,
                },
            ],
        },
        "customers": {
            "row_count": 50,
            "columns": {
                "id": {"ndv": 50},
                "name": {"ndv": 50},
                "tier": {"ndv": 3},
                "created_at": {"ndv": 50},
            },
            "indexes": [
                {"name": "idx_customers_tier", "columns": ["tier"], "unique": False}
            ],
        },
    }
    summary = {
        "table_count": len(tables),
        "total_rows": sum(table["row_count"] for table in tables.values()),
        "index_count": sum(len(table["indexes"]) for table in tables.values()),
    }
    payload: dict[str, Any] = {
        "version": version,
        "generated_at": generated_at,
        "summary": summary,
        "tables": tables,
    }
    payload["digest"] = "sha256:" + sha256_canonical(payload)
    return payload


def samples_payload() -> list[dict[str, Any]]:
    return [
        {
            "sample_id": "sq-missing-index",
            "sql": "SELECT * FROM orders WHERE customer_id = 7 AND status = 'paid'",
            "duration_ms": 860,
            "scanned_rows": 1000,
            "lock_wait_ms": 3,
            "occurred_at": "2026-01-15T08:00:00Z",
        },
        {
            "sample_id": "sq-selectivity",
            "sql": "SELECT * FROM orders WHERE status = 'paid' AND note = 'note-0042'",
            "duration_ms": 740,
            "scanned_rows": 1000,
            "lock_wait_ms": 0,
            "occurred_at": "2026-01-15T08:05:00Z",
        },
        {
            "sample_id": "sq-lock-wait",
            "sql": "SELECT * FROM orders WHERE status = 'pending'",
            "duration_ms": 900,
            "scanned_rows": 250,
            "lock_wait_ms": 430,
            "occurred_at": "2026-01-15T08:10:00Z",
        },
        {
            "sample_id": "sq-or-split",
            "sql": "SELECT * FROM orders WHERE region = 'north' OR status = 'refunded'",
            "duration_ms": 950,
            "scanned_rows": 1000,
            "lock_wait_ms": 0,
            "occurred_at": "2026-01-15T08:15:00Z",
        },
        {
            "sample_id": "sq-many-rows",
            "sql": "SELECT * FROM orders WHERE status = 'paid'",
            "duration_ms": 630,
            "scanned_rows": 250,
            "lock_wait_ms": 0,
            "occurred_at": "2026-01-15T08:20:00Z",
        },
        {
            "sample_id": "sq-full-scan",
            "sql": "SELECT * FROM orders",
            "duration_ms": 1200,
            "scanned_rows": 1000,
            "lock_wait_ms": 0,
            "occurred_at": "2026-01-15T08:25:00Z",
        },
        {
            "sample_id": "sq-fast",
            "sql": "SELECT id, status FROM orders WHERE id = 5",
            "duration_ms": 2,
            "scanned_rows": 1,
            "lock_wait_ms": 0,
            "occurred_at": "2026-01-15T08:30:00Z",
        },
    ]


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=False)
        handle.write("\n")


def build_all(data_dir: Path) -> None:
    build_sample_database(data_dir / "slowquery_sample.sqlite3")
    write_json(data_dir / "samples.json", samples_payload())
    write_json(data_dir / "snapshot_v1.json", snapshot_payload(1, "2026-01-01T00:00:00Z"))
    write_json(data_dir / "snapshot_v2.json", snapshot_payload(2, "2026-02-01T00:00:00Z"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build deterministic local SQLite fixtures")
    parser.add_argument("--data-dir", default=str(PROJECT_ROOT / "data"))
    args = parser.parse_args(argv)
    data_dir = Path(args.data_dir).resolve()
    build_all(data_dir)
    print(f"Fixtures written to {data_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
