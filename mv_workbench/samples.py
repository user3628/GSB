from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .util import digest_json, pretty_json


FIXED_TIME = "2026-01-15T09:00:00.000Z"
FIXED_BATCH_TIME = "2026-01-16T10:30:00.000Z"


def _columns(rows: list[dict[str, Any]], columns: list[dict[str, Any]]) -> dict[str, Any]:
    data = {column["name"]: [row.get(column["name"]) for row in rows] for column in columns}
    null_bitmap = {
        column["name"]: [row.get(column["name"]) is None for row in rows]
        for column in columns
    }
    row_versions = [int(row.get("_row_version", 1)) for row in rows]
    deleted = [bool(row.get("_deleted", False)) for row in rows]
    digests = [
        digest_json(
            {
                "data": {column["name"]: row.get(column["name"]) for column in columns},
                "deleted": bool(row.get("_deleted", False)),
                "row_version": int(row.get("_row_version", 1)),
            }
        )
        for row in rows
    ]
    return {
        "columns_data": data,
        "row_versions": row_versions,
        "deleted": deleted,
        "null_bitmap": null_bitmap,
        "row_digests": digests,
    }


def base_tables_sample() -> dict[str, Any]:
    sales_columns = [
        {"name": "id", "type": "integer", "nullable": False},
        {"name": "region", "type": "string", "nullable": False},
        {"name": "channel", "type": "string", "nullable": False},
        {"name": "amount", "type": "number", "nullable": False},
        {"name": "qty", "type": "integer", "nullable": False},
        {"name": "status", "type": "string", "nullable": False},
        {"name": "note", "type": "string", "nullable": True},
    ]
    sales_rows = [
        {"id": 1, "region": "east", "channel": "web", "amount": 120.5, "qty": 2, "status": "active", "note": "priority"},
        {"id": 2, "region": "east", "channel": "store", "amount": 80.0, "qty": 1, "status": "active", "note": ""},
        {"id": 3, "region": "west", "channel": "web", "amount": 200.0, "qty": 3, "status": "active", "note": "vip"},
        {"id": 4, "region": "west", "channel": "store", "amount": 50.0, "qty": 1, "status": "active", "note": None},
        {"id": 5, "region": "north", "channel": "web", "amount": 75.0, "qty": 1, "status": "active", "note": ""},
        {"id": 6, "region": "north", "channel": "store", "amount": 45.0, "qty": 2, "status": "active", "note": ""},
        {"id": 7, "region": "east", "channel": "web", "amount": 10.0, "qty": 1, "status": "inactive", "note": "archived"},
        {"id": 8, "region": "south", "channel": "store", "amount": 95.0, "qty": 2, "status": "active", "note": ""},
    ]
    returns_columns = [
        {"name": "id", "type": "integer", "nullable": False},
        {"name": "sale_id", "type": "integer", "nullable": False},
        {"name": "reason", "type": "string", "nullable": False},
        {"name": "refund", "type": "number", "nullable": False},
        {"name": "note", "type": "string", "nullable": True},
    ]
    returns_rows = [
        {"id": 101, "sale_id": 2, "reason": "price", "refund": 10.0, "note": None},
        {"id": 102, "sale_id": 4, "reason": "damaged", "refund": 5.0, "note": ""},
        {"id": 103, "sale_id": 5, "reason": "late", "refund": 7.5, "note": ""},
    ]

    def table(name: str, columns: list[dict[str, Any]], rows: list[dict[str, Any]]) -> dict[str, Any]:
        encoded = _columns(rows, columns)
        current_version = 1
        digest = digest_json(
            {
                "columns": columns,
                "rows": sorted(
                    encoded["columns_data"]["id"],
                ),
                "data": encoded["columns_data"],
                "row_versions": encoded["row_versions"],
                "deleted": encoded["deleted"],
            }
        )
        return {
            "name": name,
            "version": current_version,
            "generated_at": FIXED_TIME,
            "columns": columns,
            **encoded,
            "snapshot_digest": digest,
            "row_count": len(rows),
            "live_count": sum(not value for value in encoded["deleted"]),
        }

    return {
        "format": "mv-workbench/base-tables-v1",
        "generated_at": FIXED_TIME,
        "tables": [table("sales", sales_columns, sales_rows), table("returns", returns_columns, returns_rows)],
    }


def view_definitions_v1() -> dict[str, Any]:
    return {
        "format": "mv-workbench/view-definitions-v1",
        "generated_at": FIXED_TIME,
        "views": [
            {
                "name": "v_sales_active",
                "description": "Active sales projected to analysis columns.",
                "source": {"kind": "table", "name": "sales"},
                "operators": [
                    {
                        "op": "filter",
                        "where": [{"column": "status", "operator": "eq", "value": "active"}],
                    },
                    {
                        "op": "project",
                        "columns": {
                            "id": "id",
                            "region": "region",
                            "channel": "channel",
                            "amount": "amount",
                            "qty": "qty",
                        },
                    },
                ],
            },
            {
                "name": "v_region_revenue",
                "description": "Region revenue using distributive sum/count aggregates.",
                "source": {"kind": "table", "name": "sales"},
                "operators": [
                    {
                        "op": "filter",
                        "where": [{"column": "status", "operator": "eq", "value": "active"}],
                    },
                    {
                        "op": "group_by",
                        "keys": ["region"],
                        "aggregates": [
                            {"name": "revenue", "func": "sum", "column": "amount"},
                            {"name": "orders", "func": "count", "column": "id"},
                        ],
                    },
                ],
            },
            {
                "name": "v_region_peak",
                "description": "Region max and median; safe correction requires a full rebuild.",
                "source": {"kind": "table", "name": "sales"},
                "operators": [
                    {
                        "op": "filter",
                        "where": [{"column": "status", "operator": "eq", "value": "active"}],
                    },
                    {
                        "op": "group_by",
                        "keys": ["region"],
                        "aggregates": [
                            {"name": "peak_amount", "func": "max", "column": "amount"},
                            {"name": "median_amount", "func": "quantile", "column": "amount", "q": 0.5},
                        ],
                    },
                ],
            },
            {
                "name": "v_channel_refund",
                "description": "Join a dependent view with returns, then aggregate by channel.",
                "source": {
                    "kind": "join",
                    "type": "inner",
                    "left": {"kind": "view", "name": "v_sales_active"},
                    "right": {"kind": "table", "name": "returns"},
                    "on": [{"left": "left.id", "right": "right.sale_id"}],
                    "select": {"channel": "left.channel", "refund": "right.refund"},
                },
                "operators": [
                    {
                        "op": "group_by",
                        "keys": ["channel"],
                        "aggregates": [
                            {"name": "refund_total", "func": "sum", "column": "refund"},
                            {"name": "refund_count", "func": "count", "column": "refund"},
                        ],
                    }
                ],
            },
            {
                "name": "v_revenue_audit",
                "description": "Revenue groups above an audit threshold (depends on a view).",
                "source": {"kind": "view", "name": "v_region_revenue"},
                "operators": [
                    {
                        "op": "filter",
                        "where": [{"column": "revenue", "operator": "gte", "value": 100}],
                    },
                    {"op": "project", "columns": {"region": "region", "revenue": "revenue"}},
                ],
            },
            {
                "name": "v_high_peak_regions",
                "description": "A second-level dependency used to prove definition cascade propagation.",
                "source": {"kind": "view", "name": "v_region_peak"},
                "operators": [
                    {
                        "op": "filter",
                        "where": [{"column": "peak_amount", "operator": "gte", "value": 100}],
                    },
                    {"op": "project", "columns": {"region": "region", "peak_amount": "peak_amount"}},
                ],
            },
            {
                "name": "v_return_reasons",
                "description": "An independent base-table branch that must stay FRESH after sales changes.",
                "source": {"kind": "table", "name": "returns"},
                "operators": [
                    {
                        "op": "group_by",
                        "keys": ["reason"],
                        "aggregates": [
                            {"name": "refund_total", "func": "sum", "column": "refund"},
                            {"name": "returns", "func": "count", "column": "id"},
                        ],
                    }
                ],
            },
        ],
    }


def view_definitions_v2() -> dict[str, Any]:
    definitions = view_definitions_v1()
    definitions["format"] = "mv-workbench/view-definitions-v2"
    definitions["generated_at"] = "2026-01-17T08:00:00.000Z"
    views = {view["name"]: view for view in definitions["views"]}
    views["v_region_peak"]["operators"][1]["aggregates"].append(
        {"name": "avg_amount", "func": "avg", "column": "amount"}
    )
    views["v_region_peak"]["description"] = (
        "Changed definition: max, median and average all require a full rebuild."
    )
    views["v_revenue_audit"]["operators"][0]["where"][0]["value"] = 200
    views["v_region_order_bands"] = {
        "name": "v_region_order_bands",
        "description": "New dependent view introduced by graph version 2.",
        "source": {"kind": "view", "name": "v_region_revenue"},
        "operators": [
            {"op": "filter", "where": [{"column": "orders", "operator": "gte", "value": 2}]},
            {"op": "project", "columns": {"region": "region", "orders": "orders"}},
        ],
    }
    definitions["views"] = list(views.values())
    return definitions


def batches_sample() -> dict[str, Any]:
    return {
        "format": "mv-workbench/incremental-batches-v1",
        "generated_at": FIXED_BATCH_TIME,
        "batches": [
            {
                "batch_id": "b_correct_east_north_south",
                "table": "sales",
                "base_version": 1,
                "generated_at": FIXED_BATCH_TIME,
                "description": "Correct east sale 1, delete north sale 5, append south sale 9.",
                "rows": [
                    {
                        "id": 1,
                        "region": "east",
                        "channel": "web",
                        "amount": 180.0,
                        "qty": 3,
                        "status": "active",
                        "note": "priority",
                    },
                    {"id": 5, "_deleted": True},
                    {
                        "id": 9,
                        "region": "south",
                        "channel": "web",
                        "amount": 60.0,
                        "qty": 1,
                        "status": "active",
                        "note": "",
                    },
                ],
            },
            {
                "batch_id": "b_stale_probe",
                "table": "sales",
                "base_version": 1,
                "generated_at": "2026-01-16T11:00:00.000Z",
                "description": "Deliberately stale after the correction batch has committed.",
                "rows": [{"id": 2, "amount": 1.0}],
            },
        ],
    }


def all_samples() -> dict[str, Any]:
    return {
        "base_tables.json": base_tables_sample(),
        "view_definitions.json": view_definitions_v1(),
        "view_definitions_v2.json": view_definitions_v2(),
        "batches.json": batches_sample(),
    }


def write_samples(destination: str | Path) -> dict[str, str]:
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    written: dict[str, str] = {}
    for filename, payload in all_samples().items():
        text = pretty_json(payload) + "\n"
        (destination / filename).write_text(text, encoding="utf-8", newline="\n")
        written[filename] = digest_json(payload)
    manifest = {
        "format": "mv-workbench/sample-manifest-v1",
        "generated_at": FIXED_TIME,
        "files": [
            {"name": name, "sha256": written[name]} for name in sorted(written)
        ],
    }
    (destination / "manifest.json").write_text(
        pretty_json(manifest) + "\n", encoding="utf-8", newline="\n"
    )
    return written


def load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def sample_paths(samples_dir: str | Path) -> dict[str, Path]:
    root = Path(samples_dir)
    return {
        "base_tables": root / "base_tables.json",
        "view_definitions": root / "view_definitions.json",
        "view_definitions_v2": root / "view_definitions_v2.json",
        "batches": root / "batches.json",
        "manifest": root / "manifest.json",
    }
