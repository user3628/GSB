from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from join_planner.snapshot import checksum_for_snapshot  # noqa: E402


SNAPSHOT_DIR = ROOT / "data" / "snapshots"
QUERY_DIR = ROOT / "data" / "queries"


BASE_RELATIONS: dict[str, Any] = {
    "customers": {
        "rows": 12000,
        "columns": {
            "customer_id": {"ndv": 12000},
            "country": {"ndv": 12},
            "segment": {"ndv": 4},
            "signup_year": {"ndv": 10, "min": 2015, "max": 2026},
        },
        "correlations": [
            {"columns": ["country", "segment"], "coefficient": 1.15},
        ],
    },
    "orders": {
        "rows": 100000,
        "columns": {
            "order_id": {"ndv": 100000},
            "customer_id": {"ndv": 9000},
            "amount": {"ndv": 2400, "min": 1, "max": 5000},
            "priority": {"ndv": 5},
            "status": {"ndv": 5},
            "order_day": {"ndv": 365, "min": 1, "max": 365},
        },
        "correlations": [
            {"columns": ["priority", "status"], "coefficient": 0.82},
        ],
    },
    "order_items": {
        "rows": 350000,
        "columns": {
            "order_item_id": {"ndv": 350000},
            "order_id": {"ndv": 98000},
            "product_id": {"ndv": 4000},
            "quantity": {"ndv": 10, "min": 1, "max": 10},
            "unit_price": {"ndv": 800, "min": 1, "max": 800},
        },
        "correlations": [],
    },
    "products": {
        "rows": 4000,
        "columns": {
            "product_id": {"ndv": 4000},
            "category_id": {"ndv": 40},
            "price": {"ndv": 1000, "min": 1, "max": 1000},
            "weight": {"ndv": 500, "min": 0.1, "max": 100},
        },
        "correlations": [],
    },
    "employees": {
        "rows": 5000,
        "columns": {
            "employee_id": {"ndv": 5000},
            "manager_id": {"ndv": 650},
            "department_id": {"ndv": 30},
            "salary": {"ndv": 2200, "min": 30000, "max": 250000},
        },
        "correlations": [],
    },
}


def _version_payload(version: int, generated_at: str) -> dict[str, Any]:
    relations = copy.deepcopy(BASE_RELATIONS)
    if version == 2:
        relations["customers"]["rows"] = 13000
        relations["customers"]["columns"]["customer_id"]["ndv"] = 13000
        relations["orders"]["rows"] = 125000
        relations["orders"]["columns"]["order_id"]["ndv"] = 125000
        relations["orders"]["columns"]["customer_id"]["ndv"] = 9800
        relations["order_items"]["rows"] = 430000
        relations["order_items"]["columns"]["order_item_id"]["ndv"] = 430000
        relations["order_items"]["columns"]["order_id"]["ndv"] = 123000
    elif version == 3:
        relations["customers"]["rows"] = 14000
        relations["customers"]["columns"]["customer_id"]["ndv"] = 14000
        relations["orders"]["rows"] = 140000
        relations["orders"]["columns"]["order_id"]["ndv"] = 140000
        relations["orders"]["columns"]["customer_id"]["ndv"] = 10500
        relations["orders"]["columns"]["amount"]["ndv"] = 2600
        relations["order_items"]["rows"] = 520000
        relations["order_items"]["columns"]["order_item_id"]["ndv"] = 520000
        relations["order_items"]["columns"]["order_id"]["ndv"] = 138000
        relations["products"]["rows"] = 4200
        relations["products"]["columns"]["product_id"]["ndv"] = 4200
    payload = {
        "version": version,
        "generated_at": generated_at,
        "checksum_algorithm": "sha256",
        "relations": relations,
    }
    payload["checksum"] = checksum_for_snapshot(payload)
    return payload


QUERIES: dict[str, Any] = {
    "queries": [
        {
            "query_id": "q_customer_revenue",
            "relations": ["customers", "orders", "order_items"],
            "predicates": [
                {
                    "id": "cr_customer",
                    "type": "equality",
                    "left": {"relation": "orders", "column": "customer_id"},
                    "right": {"relation": "customers", "column": "customer_id"},
                },
                {
                    "id": "cr_order",
                    "type": "equality",
                    "left": {"relation": "order_items", "column": "order_id"},
                    "right": {"relation": "orders", "column": "order_id"},
                },
                {
                    "id": "cr_amount",
                    "type": "range",
                    "left": {"relation": "orders", "column": "amount"},
                    "lower": 100,
                    "upper": 1500,
                },
                {
                    "id": "cr_quantity",
                    "type": "range",
                    "left": {"relation": "order_items", "column": "quantity"},
                    "lower": 2,
                    "upper": 8,
                },
            ],
        },
        {
            "query_id": "q_product_priority",
            "relations": ["customers", "orders", "order_items", "products"],
            "predicates": [
                {
                    "id": "pp_customer",
                    "type": "equality",
                    "left": {"relation": "orders", "column": "customer_id"},
                    "right": {"relation": "customers", "column": "customer_id"},
                },
                {
                    "id": "pp_order",
                    "type": "equality",
                    "left": {"relation": "order_items", "column": "order_id"},
                    "right": {"relation": "orders", "column": "order_id"},
                },
                {
                    "id": "pp_product",
                    "type": "equality",
                    "left": {"relation": "order_items", "column": "product_id"},
                    "right": {"relation": "products", "column": "product_id"},
                },
                {
                    "id": "pp_status",
                    "type": "equality",
                    "left": {"relation": "orders", "column": "status"},
                    "literal": "paid",
                },
                {
                    "id": "pp_priority",
                    "type": "equality",
                    "left": {"relation": "orders", "column": "priority"},
                    "literal": 1,
                },
                {
                    "id": "pp_price",
                    "type": "range",
                    "left": {"relation": "products", "column": "price"},
                    "lower": 50,
                    "upper": 600,
                },
            ],
        },
        {
            "query_id": "q_employee_manager",
            "relations": [
                {"alias": "e1", "table": "employees"},
                {"alias": "e2", "table": "employees"},
            ],
            "predicates": [
                {
                    "id": "em_manager",
                    "type": "self_join",
                    "left": {"relation": "e1", "column": "employee_id"},
                    "right": {"relation": "e2", "column": "manager_id"},
                },
                {
                    "id": "em_senior_salary",
                    "type": "range",
                    "left": {"relation": "e2", "column": "salary"},
                    "lower": 80000,
                },
                {
                    "id": "em_manager_salary",
                    "type": "range",
                    "left": {"relation": "e1", "column": "salary"},
                    "lower": 50000,
                },
            ],
        },
    ]
}


def main() -> None:
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    QUERY_DIR.mkdir(parents=True, exist_ok=True)
    snapshots = [
        _version_payload(1, "2026-01-10T00:00:00Z"),
        _version_payload(2, "2026-01-11T00:00:00Z"),
        _version_payload(3, "2026-01-12T00:00:00Z"),
    ]
    for snapshot in snapshots:
        path = SNAPSHOT_DIR / f"snapshot_v{snapshot['version']}.json"
        path.write_text(
            json.dumps(snapshot, sort_keys=True, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        print(f"wrote {path.relative_to(ROOT)} checksum={snapshot['checksum']}")
    query_path = QUERY_DIR / "queries.json"
    query_path.write_text(
        json.dumps(QUERIES, sort_keys=True, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"wrote {query_path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()

